#!/usr/bin/env node
/**
 * Backfill empty MarketAlerts summaries from article pages + fix HTML entities in text fields.
 *
 * Usage:
 *   node scripts/backfill-market-alerts-summaries.mjs --dry-run
 *   node scripts/backfill-market-alerts-summaries.mjs
 *   node scripts/backfill-market-alerts-summaries.mjs --entities-only
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MAP_ALERT,
  patchMarketAlertTextFields,
  sanitizeMarketAlertText,
} from "../api/lib/market-alerts-rss-airtable.js";
import { fetchArticleSummary } from "../lib/rss-article-summary-enrich.js";
import { hasHtmlEntities } from "../lib/decode-html-entities.js";

const TEXT_FIELDS = [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName];
const BATCH = 10;
const FETCH_CONCURRENCY = 4;

function parseArgs(argv) {
  let dryRun = false;
  let entitiesOnly = false;
  let limit = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--dry-run") dryRun = true;
    if (argv[i] === "--entities-only") entitiesOnly = true;
    if (argv[i] === "--limit" && argv[i + 1]) limit = parseInt(argv[i + 1], 10);
  }
  return { dryRun, entitiesOnly, limit };
}

function needsSummaryBackfill(fields) {
  return !(fields[MAP_ALERT.summary] || "").trim() && !!(fields[MAP_ALERT.sourceUrl] || "").trim();
}

function needsAnyWork(fields, entitiesOnly) {
  const textPatch = patchMarketAlertTextFields(fields);
  if (Object.keys(textPatch).length) return true;
  if (!entitiesOnly && needsSummaryBackfill(fields)) return true;
  return false;
}

async function enrichSummariesForRows(rows, entitiesOnly) {
  if (entitiesOnly) return rows;

  const out = rows.map((r) => ({ ...r, patch: { ...r.patch } }));
  const todo = out
    .map((r, i) => ({ i, url: (r.before[MAP_ALERT.sourceUrl] || "").trim() }))
    .filter((x) => !(out[x.i].patch[MAP_ALERT.summary] || out[x.i].before[MAP_ALERT.summary] || "").trim() && x.url);

  for (let start = 0; start < todo.length; start += FETCH_CONCURRENCY) {
    const batch = todo.slice(start, start + FETCH_CONCURRENCY);
    const results = await Promise.all(
      batch.map(async ({ i, url }) => {
        const summary = await fetchArticleSummary(url);
        return { i, summary };
      })
    );
    for (const { i, summary } of results) {
      if (summary) {
        out[i].patch[MAP_ALERT.summary] = sanitizeMarketAlertText(summary, { preserveWhitespace: true });
      }
    }
  }
  return out;
}

async function main() {
  const { dryRun, entitiesOnly, limit } = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";

  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const candidates = [];
  let scanned = 0;

  await base(table)
    .select({
      fields: [...TEXT_FIELDS, MAP_ALERT.sourceUrl],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const rec of records) {
        scanned += 1;
        if (limit != null && scanned > limit) return;
        const before = rec.fields;
        const patch = patchMarketAlertTextFields(before);
        if (!needsAnyWork(before, entitiesOnly) && !needsSummaryBackfill(before)) continue;
        candidates.push({ id: rec.id, before, patch });
      }
      if (limit != null && scanned >= limit) return;
      next();
    });

  const prepared = await enrichSummariesForRows(candidates, entitiesOnly);
  const toUpdate = prepared
    .map((r) => ({
      id: r.id,
      before: r.before,
      fields: r.patch,
    }))
    .filter((r) => Object.keys(r.fields).length > 0);

  const summaryFilled = toUpdate.filter((r) => r.fields[MAP_ALERT.summary] && !r.before[MAP_ALERT.summary]).length;
  const textCleaned = toUpdate.filter((r) =>
    TEXT_FIELDS.some((k) => r.fields[k] && r.fields[k] !== (r.before[k] || ""))
  ).length;

  console.log(
    JSON.stringify(
      {
        table,
        dryRun,
        entitiesOnly,
        scanned,
        toUpdate: toUpdate.length,
        summaryFilled,
        textCleaned,
        samples: toUpdate.slice(0, 5).map((u) => ({
          id: u.id,
          titleBefore: u.before[MAP_ALERT.title],
          titleAfter: u.fields[MAP_ALERT.title],
          summaryAfter: (u.fields[MAP_ALERT.summary] || u.before[MAP_ALERT.summary] || "").slice(0, 120),
        })),
      },
      null,
      2
    )
  );

  if (dryRun || toUpdate.length === 0) process.exit(0);

  let updated = 0;
  const errors = [];
  for (let i = 0; i < toUpdate.length; i += BATCH) {
    const batch = toUpdate.slice(i, i + BATCH).map(({ id, fields }) => ({ id, fields }));
    try {
      await base(table).update(batch, { typecast: true });
      updated += batch.length;
    } catch (err) {
      errors.push({ batchStart: i, message: err.message || String(err) });
      console.error("[backfill-market-alerts-summaries] batch failed:", err.message);
    }
  }

  console.log(JSON.stringify({ updated, errors }, null, 2));
  process.exit(errors.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
