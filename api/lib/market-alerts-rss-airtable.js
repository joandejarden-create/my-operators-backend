import crypto from "crypto";
import Airtable from "airtable";
import { inferCategoryFromText } from "../../lib/market-alerts-category-infer.js";
import {
  canonicalizeSourceUrl,
  normalizeAlertTitle,
} from "../../lib/market-alerts-dedupe.js";
import { sanitizeMarketAlertPlainText } from "../../lib/market-alerts-plain-text.js";
import { assessMarketAlertPublishReady } from "../../lib/market-alerts-publish-gate.js";
import { REGION_GEO_REGEX } from "../../lib/market-alerts-geo-keywords.js";

/** @typedef {import("../market-alerts-news.js").fetchMarketAlertsRssItems} fetchMarketAlertsRssItems */

export const MAP_ALERT = {
  title: "Title",
  dedupeId: "Dedupe ID",
  summary: "Summary",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  publishedAt: "Published At",
  category: "Category",
  regionGroup: "Region Group",
  priority: "Priority",
  tags: "Tags",
};

export const ALLOWED_CATEGORIES = [
  "Deals",
  "Capital",
  "Brand",
  "Supply",
  "Demand",
  "Loyalty",
  "Risk",
];

export const ALLOWED_REGION_GROUPS = [
  "Global",
  "North America",
  "Europe",
  "Asia Pacific",
  "Caribbean",
  "Latin America",
  "Other",
];

export const ALLOWED_PRIORITIES = ["Low", "Medium", "High"];

/** Reserve Global for explicitly worldwide / multi-region industry coverage. */
const GLOBAL_SIGNAL_RE =
  /\b(global (?:hotel|hospitality|travel|tourism|market|trend|outlook|industry|revpar|demand|supply|pipeline|shift|survey|report|study|index|workforce)|world(?:'s|wide)|world-wide|around the world|across (?:the )?(?:world|regions|markets)|all regions|multi-?region|pan-?regional|cross-?border|international (?:hotel|hospitality|chain|group)|industry-?wide|world hotel|hotel industry(?: as a whole)?|regions besides|every region|no single market|world's boutique|world's hotel)\b/i;

/** SHA-256 of canonical source URL (stable Airtable Dedupe ID). */
export function rssItemDedupeId(item) {
  const url = canonicalizeSourceUrl(item.link || item.sourceUrl || "");
  if (url) return crypto.createHash("sha256").update(url).digest("hex");
  const title = normalizeAlertTitle(item.title || "");
  return crypto.createHash("sha256").update(title || "untitled").digest("hex");
}

export function inferCategory(item) {
  const text = `${item.title || ""} ${item.summary || ""}`.trim();
  return inferCategoryFromText(text, item.source || item.sourceName || "");
}

export function inferRegionGroup(item) {
  const source = (item.source || item.sourceName || "").trim();
  if (/\b(usa\s*&\s*canada|north america)\b/i.test(source)) {
    return "North America";
  }

  const text = `${item.title || ""} ${item.summary || ""} ${item.link || ""}`.trim();
  for (const { region, re } of REGION_GEO_REGEX) {
    if (re.test(text)) return region;
  }
  if (GLOBAL_SIGNAL_RE.test(text)) return "Global";

  // Source-based hints only after text/geo fail.
  if (/\b(middle east|africa|mena)\b/i.test(source)) return "Other";
  if (/\b(asia|hospitalityworld|et hospitality)\b/i.test(source)) return "Asia Pacific";
  if (/\b(hotel dive|hotel management|lodging magazine)\b/i.test(source)) {
    return "North America";
  }

  return "Global";
}

/** Infer category from Airtable MarketAlerts field shape (for backfill scripts). */
export function inferCategoryFromFields(fields) {
  const text = `${fields[MAP_ALERT.title] || ""} ${fields[MAP_ALERT.summary] || ""}`.trim();
  return inferCategoryFromText(text, fields[MAP_ALERT.sourceName] || "");
}

/** Infer region from Airtable MarketAlerts field shape (for backfill scripts). */
export function inferRegionGroupFromFields(fields) {
  return inferRegionGroup({
    title: fields[MAP_ALERT.title],
    summary: fields[MAP_ALERT.summary],
    link: fields[MAP_ALERT.sourceUrl],
    source: fields[MAP_ALERT.sourceName],
  });
}

/** Full classification patch (region + category) for backfill. */
export function inferClassificationPatch(fields) {
  const patch = {};
  const region = inferRegionGroupFromFields(fields);
  const category = inferCategoryFromFields(fields);
  const currentRegion = fields[MAP_ALERT.regionGroup] || "Global";
  const currentCategory = fields[MAP_ALERT.category] || "Demand";
  if (region !== currentRegion) patch[MAP_ALERT.regionGroup] = region;
  if (category !== currentCategory) patch[MAP_ALERT.category] = category;
  return { patch, region, category, currentRegion, currentCategory };
}

export function parsePublishedAtIso(pubDate) {
  if (!pubDate) return new Date().toISOString();
  const d = new Date(pubDate);
  if (Number.isNaN(d.getTime())) return new Date().toISOString();
  return d.toISOString();
}

/** Normalize display text before Airtable write or publish. */
export function sanitizeMarketAlertText(text, { preserveWhitespace = false } = {}) {
  return sanitizeMarketAlertPlainText(text, {
    preserveWhitespace,
    maxLen: preserveWhitespace ? 10000 : 2000,
  });
}

export function patchMarketAlertTextFields(fields) {
  const patch = {};
  for (const key of [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName]) {
    const val = fields[key];
    if (typeof val !== "string" || !val) continue;
    const preserveWhitespace = key === MAP_ALERT.summary;
    const cleaned = sanitizeMarketAlertText(val, { preserveWhitespace });
    if (cleaned !== val) patch[key] = cleaned;
  }
  return patch;
}

export function mapRssItemToAirtableFields(item) {
  const sourceUrl = (item.link || "").trim();
  const fields = {
    [MAP_ALERT.title]: sanitizeMarketAlertText(item.title || "(No title)").slice(0, 500),
    [MAP_ALERT.dedupeId]: rssItemDedupeId(item),
    [MAP_ALERT.summary]: sanitizeMarketAlertText(item.summary, { preserveWhitespace: true }).slice(
      0,
      10000
    ),
    [MAP_ALERT.sourceName]: sanitizeMarketAlertText(item.source || "RSS").slice(0, 200),
    [MAP_ALERT.sourceUrl]: sourceUrl.slice(0, 1000),
    [MAP_ALERT.publishedAt]: parsePublishedAtIso(item.pubDate),
    [MAP_ALERT.category]: inferCategory(item),
    [MAP_ALERT.regionGroup]: item.regionGroup || inferRegionGroup(item),
    [MAP_ALERT.priority]: "Medium",
    [MAP_ALERT.tags]: Array.isArray(item.airtableTags) && item.airtableTags.length
      ? item.airtableTags
      : ["RSS"],
  };
  return fields;
}

export function validateAlertFields(fields) {
  const errors = [];
  if (!fields[MAP_ALERT.title]) errors.push("Title is required");
  if (!fields[MAP_ALERT.dedupeId]) errors.push("Dedupe ID is required");
  if (!ALLOWED_CATEGORIES.includes(fields[MAP_ALERT.category])) {
    errors.push(`Invalid Category: ${fields[MAP_ALERT.category]}`);
  }
  if (!ALLOWED_REGION_GROUPS.includes(fields[MAP_ALERT.regionGroup])) {
    errors.push(`Invalid Region Group: ${fields[MAP_ALERT.regionGroup]}`);
  }
  if (!ALLOWED_PRIORITIES.includes(fields[MAP_ALERT.priority])) {
    errors.push(`Invalid Priority: ${fields[MAP_ALERT.priority]}`);
  }
  const pub = fields[MAP_ALERT.publishedAt];
  if (pub && Number.isNaN(new Date(pub).getTime())) {
    errors.push("Published At is not a valid date");
  }
  return { ok: errors.length === 0, errors };
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) return null;
  return new Airtable({ apiKey }).base(baseId);
}

/** @deprecated Prefer loadExistingDedupeIndex — kept for scripts that only need IDs. */
export async function loadExistingDedupeIds(tableName) {
  const index = await loadExistingDedupeIndex(tableName);
  return index.dedupeIds;
}

/**
 * Load existing URL dedupe IDs + soft title keys for cross-feed duplicate suppression.
 * @returns {Promise<{ dedupeIds: Set<string>, titleKeys: Set<string>, urlKeys: Set<string> }>}
 */
export async function loadExistingDedupeIndex(tableName) {
  const base = getBase();
  if (!base) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required");

  const dedupeIds = new Set();
  const titleKeys = new Set();
  const urlKeys = new Set();

  await base(tableName)
    .select({
      fields: [MAP_ALERT.dedupeId, MAP_ALERT.title, MAP_ALERT.sourceUrl],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      records.forEach((r) => {
        const id = r.fields[MAP_ALERT.dedupeId];
        if (id) dedupeIds.add(String(id));
        const titleKey = normalizeAlertTitle(r.fields[MAP_ALERT.title] || "");
        if (titleKey && titleKey.length >= 24) titleKeys.add(titleKey);
        const urlKey = canonicalizeSourceUrl(r.fields[MAP_ALERT.sourceUrl] || "");
        if (urlKey) urlKeys.add(urlKey);
      });
      next();
    });

  return { dedupeIds, titleKeys, urlKeys };
}

const AIRTABLE_CREATE_BATCH = 10;

async function loadKnownMarketAlertsFieldNames(baseId, token, tableName) {
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) return null;
    const table = (json.tables || []).find((t) => t.name === tableName);
    if (!table) return null;
    return new Set((table.fields || []).map((f) => f.name));
  } catch (err) {
    console.warn("[market-alerts-rss-sync] could not load field schema:", err.message || err);
    return null;
  }
}

/**
 * @param {object} opts
 * @param {Array<object>} opts.items RSS items from fetchMarketAlertsRssItems
 * @param {boolean} [opts.dryRun]
 * @param {string} [opts.tableName]
 */
export async function syncRssItemsToAirtable({ items, dryRun = false, tableName }) {
  const table = tableName || process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const base = getBase();
  if (!base) {
    return {
      ok: false,
      error: "AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required",
      validation: { ok: false, errors: ["Airtable not configured"] },
    };
  }

  const existing = await loadExistingDedupeIndex(table);
  const toCreate = [];
  const skipped = [];
  const invalid = [];
  let skippedByTitle = 0;
  let skippedByUrl = 0;
  let skippedIrrelevant = 0;
  let skippedPublishGate = 0;

  for (const item of items) {
    const gate = assessMarketAlertPublishReady(item);
    if (!gate.ok) {
      skippedPublishGate += 1;
      if (gate.reasons.some((r) => r.startsWith("irrelevant:"))) skippedIrrelevant += 1;
      skipped.push({
        title: item.title,
        reason: "publish_gate",
        details: gate.reasons,
      });
      continue;
    }
    const cleanedItem = {
      ...item,
      title: gate.cleaned.title,
      summary: gate.cleaned.summary,
      source: gate.cleaned.source || item.source,
      link: gate.cleaned.link || item.link,
    };
    const fields = mapRssItemToAirtableFields(cleanedItem);
    const validation = validateAlertFields(fields);
    if (!validation.ok) {
      invalid.push({ title: fields[MAP_ALERT.title], errors: validation.errors });
      continue;
    }

    const dedupeId = fields[MAP_ALERT.dedupeId];
    const titleKey = normalizeAlertTitle(fields[MAP_ALERT.title] || "");
    const urlKey = canonicalizeSourceUrl(fields[MAP_ALERT.sourceUrl] || "");

    if (existing.dedupeIds.has(dedupeId) || (urlKey && existing.urlKeys.has(urlKey))) {
      skippedByUrl += 1;
      skipped.push({ title: fields[MAP_ALERT.title], dedupeId, reason: "url" });
      continue;
    }
    if (titleKey && titleKey.length >= 24 && existing.titleKeys.has(titleKey)) {
      skippedByTitle += 1;
      skipped.push({ title: fields[MAP_ALERT.title], dedupeId, reason: "title" });
      continue;
    }

    existing.dedupeIds.add(dedupeId);
    if (urlKey) existing.urlKeys.add(urlKey);
    if (titleKey && titleKey.length >= 24) existing.titleKeys.add(titleKey);
    toCreate.push(fields);
  }

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      table,
      fetched: items.length,
      wouldCreate: toCreate.length,
      skipped: skipped.length,
      skippedByUrl,
      skippedByTitle,
      skippedIrrelevant,
      skippedPublishGate,
      invalid: invalid.length,
      preview: toCreate.slice(0, 8).map((f) => ({
        title: f[MAP_ALERT.title],
        category: f[MAP_ALERT.category],
        regionGroup: f[MAP_ALERT.regionGroup],
        sourceName: f[MAP_ALERT.sourceName],
        publishedAt: f[MAP_ALERT.publishedAt],
        sourceUrl: f[MAP_ALERT.sourceUrl],
      })),
      fieldMapping: MAP_ALERT,
    };
  }

  const created = [];
  const createErrors = [];
  let intelligenceEnriched = 0;
  let intelligenceErrors = 0;

  const knownFieldNames = await loadKnownMarketAlertsFieldNames(
    process.env.AIRTABLE_BASE_ID,
    process.env.AIRTABLE_API_KEY,
    table
  );

  let enrichMarketAlertIntelligence = null;
  try {
    ({ enrichMarketAlertIntelligence } = await import(
      "../../lib/market-alerts-intelligence.js"
    ));
  } catch (err) {
    console.warn(
      "[market-alerts-rss-sync] intelligence module unavailable:",
      err.message || err
    );
  }

  for (let i = 0; i < toCreate.length; i += AIRTABLE_CREATE_BATCH) {
    const batchFields = toCreate.slice(i, i + AIRTABLE_CREATE_BATCH);
    const batch = batchFields.map((fields) => ({ fields }));
    try {
      const records = await base(table).create(batch, { typecast: true });
      for (let j = 0; j < records.length; j++) {
        const r = records[j];
        created.push({ id: r.id, title: r.fields[MAP_ALERT.title] });
        if (typeof enrichMarketAlertIntelligence === "function") {
          try {
            const enrichResult = await enrichMarketAlertIntelligence(
              r.id,
              batchFields[j] || r.fields,
              { tableName: table, knownFieldNames }
            );
            if (enrichResult?.ok) intelligenceEnriched += 1;
            else intelligenceErrors += 1;
          } catch (enrichErr) {
            intelligenceErrors += 1;
            console.error(
              "[market-alerts-rss-sync] intelligence enrich failed (non-fatal):",
              enrichErr.message || enrichErr
            );
          }
        }
      }
    } catch (err) {
      createErrors.push({
        batchStart: i,
        message: err.message || String(err),
      });
      if (process.env.NODE_ENV !== "production") {
        console.error("[market-alerts-rss-sync] batch create failed:", err.message);
      }
    }
  }

  return {
    ok: createErrors.length === 0,
    dryRun: false,
    table,
    fetched: items.length,
    created: created.length,
    skipped: skipped.length,
    skippedByUrl,
    skippedByTitle,
    skippedIrrelevant,
    skippedPublishGate,
    invalid: invalid.length,
    createErrors,
    createdSample: created.slice(0, 5),
    intelligenceEnriched,
    intelligenceErrors,
    fieldMapping: MAP_ALERT,
  };
}
