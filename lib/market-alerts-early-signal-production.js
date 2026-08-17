/**
 * Early Signal production fetch + Airtable sync (V1.2).
 * Only EARLY_SIGNAL_PRODUCTION_FAMILIES — weak families remain disabled.
 */
import { fetchSingleRssFeed } from "../api/market-alerts-news.js";
import {
  canonicalizeSourceUrl,
  normalizeAlertTitle,
  resolveGoogleNewsArticleUrl,
} from "./market-alerts-dedupe.js";
import {
  MAP_ALERT,
  loadExistingDedupeIndex,
  mapRssItemToAirtableFields,
  validateAlertFields,
  syncRssItemsToAirtable,
} from "../api/lib/market-alerts-rss-airtable.js";
import {
  buildGoogleNewsRssUrl,
  EARLY_SIGNAL_FAMILY_TAGS,
  googleNewsSourceLabel,
  listEarlySignalQueries,
} from "./market-alerts-early-signal-queries.js";
import {
  EARLY_SIGNAL_PRODUCTION_INSERT_LIMIT,
  EARLY_SIGNAL_PRODUCTION_MAX_AGE_DAYS,
  EARLY_SIGNAL_PRODUCTION_PER_QUERY,
  EARLY_SIGNAL_PRODUCTION_WHEN,
  isEarlySignalProductionEnabled,
} from "./market-alerts-early-signal-config.js";
import {
  assessEarlySignalProductionReady,
  classifyEarlySignalCandidate,
  summarizeEarlySignalPilot,
} from "./market-alerts-early-signals.js";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isWithinRecency(pubDate, maxAgeDays) {
  if (!pubDate) return true;
  const t = new Date(pubDate).getTime();
  if (!Number.isFinite(t)) return true;
  const ageDays = (Date.now() - t) / (24 * 60 * 60 * 1000);
  return ageDays <= maxAgeDays;
}

function mapEarlySignalToAirtableFields(item, classified) {
  const familyTag = EARLY_SIGNAL_FAMILY_TAGS[item.family] || "EARLY_SIGNAL";
  const fields = mapRssItemToAirtableFields(item);
  fields[MAP_ALERT.tags] = item.airtableTags || ["RSS", "EARLY_SIGNAL", familyTag];
  if (classified.region || item.regionGroup) {
    fields[MAP_ALERT.regionGroup] = classified.region || item.regionGroup;
  }
  return fields;
}

/**
 * Fetch + classify production early signals (no writes).
 */
export async function fetchProductionEarlySignalCandidates(opts = {}) {
  const perQuery = opts.perQuery ?? EARLY_SIGNAL_PRODUCTION_PER_QUERY;
  const maxAgeDays = opts.maxAgeDays ?? EARLY_SIGNAL_PRODUCTION_MAX_AGE_DAYS;
  const when = opts.when || EARLY_SIGNAL_PRODUCTION_WHEN;
  const queries = listEarlySignalQueries(null, { productionOnly: true });

  const raw = [];
  const fetchErrors = [];

  for (const q of queries) {
    const url = buildGoogleNewsRssUrl(q.query, { when });
    const source = googleNewsSourceLabel(q.family);
    try {
      const items = await fetchSingleRssFeed(url, source);
      for (const item of items.slice(0, perQuery)) {
        if (!isWithinRecency(item.pubDate, maxAgeDays)) continue;
        raw.push({
          ...item,
          family: q.family,
          queryId: q.id,
          queryLabel: q.label,
          cala: !!q.cala,
          requireTitleHospitality: !!q.requireTitleHospitality,
        });
      }
    } catch (err) {
      fetchErrors.push({ id: q.id, error: err.message || String(err) });
    }
    await sleep(350);
  }

  const seenUrls = new Set();
  const seenTitles = new Set();
  let duplicateCount = 0;
  const unique = [];

  for (const item of raw) {
    const urlKey = canonicalizeSourceUrl(item.link || "");
    const titleKey = normalizeAlertTitle(item.title || "");
    if (urlKey && seenUrls.has(urlKey)) {
      duplicateCount += 1;
      continue;
    }
    if (titleKey && titleKey.length >= 24 && seenTitles.has(titleKey)) {
      duplicateCount += 1;
      continue;
    }
    if (urlKey) seenUrls.add(urlKey);
    if (titleKey && titleKey.length >= 24) seenTitles.add(titleKey);
    unique.push(item);
  }

  const classified = unique.map((item) => classifyEarlySignalCandidate(item));
  const productionReady = [];
  const rejectedProduction = [];

  for (let i = 0; i < unique.length; i += 1) {
    const row = classified[i];
    const gate = assessEarlySignalProductionReady(row);
    if (gate.ok) {
      productionReady.push({ item: unique[i], classified: row });
    } else {
      rejectedProduction.push({ title: row.title, reason: gate.reason, family: row.family });
    }
  }

  return {
    rawCount: raw.length,
    dedupedCount: unique.length,
    duplicateCount,
    classified,
    productionReady,
    rejectedProduction,
    fetchErrors,
    summary: summarizeEarlySignalPilot(classified),
  };
}

async function resolveGoogleNewsLinks(items, { concurrency = 3 } = {}) {
  const out = items.map((item) => ({ ...item }));
  const idxs = out
    .map((item, i) => (/news\.google\.com/i.test(item.link || "") ? i : -1))
    .filter((i) => i >= 0);

  for (let start = 0; start < idxs.length; start += concurrency) {
    const batch = idxs.slice(start, start + concurrency);
    const results = await Promise.all(
      batch.map(async (i) => {
        const next = await resolveGoogleNewsArticleUrl(out[i].link);
        return { i, next };
      })
    );
    for (const { i, next } of results) {
      if (next && next !== out[i].link && !/news\.google\.com/i.test(next)) {
        out[i].link = next;
      }
    }
  }
  return out;
}

/**
 * Production Early Signal sync — inserts qualified rows into MarketAlerts.
 * @param {{ dryRun?: boolean, limit?: number, tableName?: string }} [opts]
 */
export async function runEarlySignalProductionSync(opts = {}) {
  const dryRun = opts.dryRun === true || process.env.DRY_RUN === "true";
  const insertLimit = Math.min(
    opts.limit ?? EARLY_SIGNAL_PRODUCTION_INSERT_LIMIT,
    EARLY_SIGNAL_PRODUCTION_INSERT_LIMIT
  );

  if (!dryRun && !isEarlySignalProductionEnabled() && !opts.force) {
    return {
      ok: true,
      skipped: true,
      reason: "MARKET_ALERTS_EARLY_SIGNALS_ENABLED is not true",
    };
  }

  const fetched = await fetchProductionEarlySignalCandidates(opts);
  let candidates = fetched.productionReady.slice(0, insertLimit);

  const items = candidates.map((c) => c.item);
  const withLinks = await resolveGoogleNewsLinks(items);
  candidates = candidates.map((c, i) => ({
    ...c,
    item: withLinks[i] || c.item,
  }));

  const table = opts.tableName || process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const existing = await loadExistingDedupeIndex(table);

  const toSync = [];
  let existingDuplicates = 0;

  for (const row of candidates) {
    const familyTag = EARLY_SIGNAL_FAMILY_TAGS[row.item.family] || "EARLY_SIGNAL";
    const probeItem = {
      ...row.item,
      airtableTags: ["RSS", "EARLY_SIGNAL", familyTag],
      regionGroup: row.classified.region || undefined,
    };
    const fields = mapEarlySignalToAirtableFields(probeItem, row.classified);
    const validation = validateAlertFields(fields);
    if (!validation.ok) continue;

    const dedupeId = fields[MAP_ALERT.dedupeId];
    const titleKey = normalizeAlertTitle(fields[MAP_ALERT.title] || "");
    const urlKey = canonicalizeSourceUrl(fields[MAP_ALERT.sourceUrl] || "");

    if (
      existing.dedupeIds.has(dedupeId) ||
      (urlKey && existing.urlKeys.has(urlKey)) ||
      (titleKey && titleKey.length >= 24 && existing.titleKeys.has(titleKey))
    ) {
      existingDuplicates += 1;
      continue;
    }

    existing.dedupeIds.add(dedupeId);
    if (urlKey) existing.urlKeys.add(urlKey);
    if (titleKey && titleKey.length >= 24) existing.titleKeys.add(titleKey);
    toSync.push(probeItem);
  }

  const audit = {
    raw: fetched.rawCount,
    deduped: fetched.dedupedCount,
    classified: fetched.classified.length,
    valid: fetched.summary.validHospitalitySignals,
    productionReady: fetched.productionReady.length,
    rejectedProduction: fetched.rejectedProduction.length,
    existingDuplicates,
    review: fetched.summary.review,
    standard: fetched.summary.standard,
    rejected: fetched.summary.rejected,
    byFamily: fetched.summary.byFamily,
    byTiming: fetched.summary.byTiming,
    byDirection: fetched.summary.byDirection,
    audience: fetched.summary.audience,
    rejectionReasons: fetched.summary.rejectionReasons,
    offTopic: fetched.summary.rejectionReasons["off-topic"] || 0,
    fetchErrors: fetched.fetchErrors.length,
  };

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      audit,
      wouldInsert: toSync.length,
      qualifiedNotInserted: Math.max(0, candidates.length - toSync.length),
      preview: candidates.slice(0, 8).map((c) => ({
        title: c.classified.title,
        family: c.classified.family,
        eventType: c.classified.eventType,
        signalTiming: c.classified.signalTiming,
        projectDirection: c.classified.projectDirection,
        treatment: c.classified.treatment,
      })),
    };
  }

  const syncResult = await syncRssItemsToAirtable({
    items: toSync,
    dryRun: false,
    tableName: table,
  });

  return {
    ok: syncResult.ok,
    dryRun: false,
    audit,
    inserted: syncResult.created || 0,
    qualifiedNotInserted: Math.max(0, toSync.length - (syncResult.created || 0)),
    skipped: syncResult.skipped || 0,
    invalid: syncResult.invalid || 0,
    createErrors: syncResult.createErrors || [],
    intelligenceEnriched: syncResult.intelligenceEnriched || 0,
    intelligenceErrors: syncResult.intelligenceErrors || 0,
    createdSample: syncResult.createdSample || [],
  };
}
