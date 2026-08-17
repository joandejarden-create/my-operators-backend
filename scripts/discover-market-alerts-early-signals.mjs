#!/usr/bin/env node
/**
 * Early Signal discovery V1.1 — dry-run only.
 *
 *   npm run market-alerts:early-signals:discover -- --dry-run --limit 280
 *
 * Does NOT write to Airtable. Refuses --apply.
 */
import fs from "fs";
import path from "path";
import "../load-env.js";
import {
  canonicalizeSourceUrl,
  normalizeAlertTitle,
} from "../lib/market-alerts-dedupe.js";
import {
  buildGoogleNewsRssUrl,
  EARLY_SIGNAL_FAMILIES,
  EARLY_SIGNAL_FAMILY_LABELS,
  googleNewsSourceLabel,
  listEarlySignalQueries,
} from "../lib/market-alerts-early-signal-queries.js";
import {
  classifyEarlySignalCandidate,
  summarizeEarlySignalPilot,
} from "../lib/market-alerts-early-signals.js";
import { fetchSingleRssFeed } from "../api/market-alerts-news.js";

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  if (i === -1) return null;
  return process.argv[i + 1] || null;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function recommendFamily(stats, rawCount) {
  const valid = stats?.valid || 0;
  const review = stats?.review || 0;
  const raw = rawCount || 0;
  if (raw === 0 && valid === 0) return "TUNE_AGAIN";
  if (valid === 0) return raw >= 8 ? "DO_NOT_ACTIVATE" : "TUNE_AGAIN";
  const hitRate = valid / Math.max(raw, 1);
  if (review >= 6 && hitRate >= 0.15) return "READY";
  if (review >= 2 && valid >= 2) return "READY_WITH_LIMITS";
  if (valid >= 1) return "TUNE_AGAIN";
  return "DO_NOT_ACTIVATE";
}

async function main() {
  if (process.argv.includes("--apply")) {
    console.error("Refusing --apply. Early Signal discovery V1.1 is dry-run / pilot only.");
    process.exit(1);
  }

  const family = argValue("--family");
  const productionOnly = process.argv.includes("--production");
  const limit = Math.min(Math.max(parseInt(argValue("--limit") || "280", 10) || 280, 1), 400);
  const perQuery = Math.min(Math.max(parseInt(argValue("--per-query") || "25", 10) || 25, 1), 80);
  const sinceRaw = argValue("--since");
  const since = sinceRaw ? new Date(sinceRaw) : null;
  if (sinceRaw && Number.isNaN(since.getTime())) {
    throw new Error(`Invalid --since date: ${sinceRaw}`);
  }

  const queries = listEarlySignalQueries(family, { productionOnly });
  if (!queries.length) {
    throw new Error(`No queries for family="${family || ""}"`);
  }

  console.log(
    `Mode: dry-run family=${family || "all"} productionOnly=${productionOnly} queries=${queries.length} limit=${limit} perQuery=${perQuery}${sinceRaw ? ` since=${sinceRaw}` : ""}`
  );

  const raw = [];
  const fetchErrors = [];
  const rawByFamily = {};

  for (const q of queries) {
    const url = buildGoogleNewsRssUrl(q.query);
    const source = googleNewsSourceLabel(q.family);
    try {
      const items = await fetchSingleRssFeed(url, source);
      const sliced = items.slice(0, perQuery);
      for (const item of sliced) {
        raw.push({
          ...item,
          family: q.family,
          queryId: q.id,
          queryLabel: q.label,
          cala: !!q.cala,
          requireTitleHospitality: !!q.requireTitleHospitality,
        });
      }
      rawByFamily[q.family] = (rawByFamily[q.family] || 0) + sliced.length;
      console.log(`  ${q.id}: ${items.length} fetched / ${sliced.length} kept`);
    } catch (err) {
      fetchErrors.push({ id: q.id, error: err.message || String(err) });
      console.warn(`  ${q.id}: FETCH FAIL ${err.message || err}`);
    }
    await sleep(400);
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
    if (since && item.pubDate) {
      const t = new Date(item.pubDate).getTime();
      if (Number.isFinite(t) && t < since.getTime()) {
        continue;
      }
    }
    if (urlKey) seenUrls.add(urlKey);
    if (titleKey && titleKey.length >= 24) seenTitles.add(titleKey);
    unique.push(item);
  }

  const slice = unique.slice(0, limit);
  const classified = slice.map((item) => classifyEarlySignalCandidate(item));

  const summary = summarizeEarlySignalPilot(classified);
  summary.rawResults = raw.length;
  summary.deduped = unique.length;
  summary.duplicatesDropped = duplicateCount;
  summary.fetchErrors = fetchErrors;
  summary.rejectionReasons.duplicate = duplicateCount;
  summary.rawByFamily = rawByFamily;

  const familyReport = {};
  for (const fam of EARLY_SIGNAL_FAMILIES) {
    const stats = summary.byFamily[fam] || {
      raw: 0,
      valid: 0,
      review: 0,
      rejected: 0,
      preDecision: 0,
      decisionForming: 0,
      topRejectionReason: null,
    };
    familyReport[fam] = {
      label: EARLY_SIGNAL_FAMILY_LABELS[fam],
      raw: rawByFamily[fam] || 0,
      deduped: stats.raw || 0,
      valid: stats.valid || 0,
      review: stats.review || 0,
      rejected: stats.rejected || 0,
      preDecision: stats.preDecision || 0,
      decisionForming: stats.decisionForming || 0,
      topRejectionReason: stats.topRejectionReason || null,
      recommendation: recommendFamily(stats, rawByFamily[fam] || 0),
    };
  }

  const qualifying = classified.filter((r) => r.earlyWr).slice(0, 25);
  const rejectedOrStandard = classified
    .filter((r) => r.treatment !== "REVIEW" || r.rejection)
    .slice(0, 20);
  const challengedOrBlocked = classified
    .filter((r) => r.projectDirection === "Challenged" || r.projectDirection === "Rejected / Blocked")
    .slice(0, 12);

  const out = {
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    version: "v1.1",
    family: family || "all",
    limit,
    perQuery,
    summary,
    familyReport,
    qualifying,
    rejectedOrStandard,
    challengedOrBlocked,
    classified: classified.map((r) => ({
      title: r.title,
      source: r.source,
      family: r.family,
      eventType: r.eventType,
      signalTiming: r.signalTiming,
      projectDirection: r.projectDirection,
      treatment: r.treatment,
      region: r.region,
      hotelProject: r.hotelProject,
      projectLabel: r.projectLabel,
      rejection: r.rejection,
      link: r.link,
      ownerWr: !!r.audience?.owner?.worthReviewing,
      brandWr: !!r.audience?.brand?.worthReviewing,
      operatorWr: !!r.audience?.operator?.worthReviewing,
      brandSignal: r.audience?.brand?.signalType || null,
      operatorSignal: r.audience?.operator?.signalType || null,
      ownerSignal: r.audience?.owner?.signalType || null,
    })),
  };

  const outPath = path.join(process.cwd(), "data", "market-alerts-early-signal-pilot-v1_1.json");
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));

  console.log("\nPILOT SUMMARY");
  console.log(JSON.stringify({
    raw: summary.rawResults,
    deduped: summary.deduped,
    classified: summary.classified,
    valid: summary.validHospitalitySignals,
    review: summary.review,
    standard: summary.standard,
    rejected: summary.rejected,
    timing: summary.byTiming,
    direction: summary.byDirection,
    timingXDirection: summary.timingXDirection,
    audience: summary.audience,
    regions: summary.byRegion,
    rejections: summary.rejectionReasons,
    familyReport,
    fetchErrors: fetchErrors.length,
  }, null, 2));
  console.log(`\nWrote ${outPath}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
