#!/usr/bin/env node
/**
 * Fix HTML entity artifacts in MarketAlerts (Title, Summary, Source Name).
 *
 * Usage:
 *   node scripts/cleanup-market-alerts-html-entities.mjs --dry-run
 *   node scripts/cleanup-market-alerts-html-entities.mjs
 *   node scripts/cleanup-market-alerts-html-entities.mjs --limit 50
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MAP_ALERT,
  sanitizeMarketAlertText,
} from "../api/lib/market-alerts-rss-airtable.js";
import { hasHtmlEntities } from "../lib/decode-html-entities.js";

const TEXT_FIELDS = [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName];
const BATCH = 10;

function parseArgs(argv) {
  let dryRun = false;
  let limit = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--dry-run") dryRun = true;
    if (argv[i] === "--limit" && argv[i + 1]) limit = parseInt(argv[i + 1], 10);
    if (argv[i].startsWith("--limit=")) limit = parseInt(argv[i].slice("--limit=".length), 10);
  }
  return { dryRun, limit };
}

function buildPatch(fields) {
  const patch = {};
  for (const key of TEXT_FIELDS) {
    const val = fields[key];
    if (typeof val !== "string" || !val) continue;
    if (!hasHtmlEntities(val)) continue;
    const preserveWhitespace = key === MAP_ALERT.summary;
    const cleaned = sanitizeMarketAlertText(val, { preserveWhitespace });
    if (cleaned !== val) patch[key] = cleaned;
  }
  return patch;
}

async function main() {
  const { dryRun, limit } = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";

  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const toUpdate = [];
  let scanned = 0;

  await base(table)
    .select({ fields: TEXT_FIELDS, pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        scanned += 1;
        if (limit != null && scanned > limit) {
          next();
          return;
        }
        const patch = buildPatch(rec.fields);
        if (Object.keys(patch).length) {
          toUpdate.push({ id: rec.id, fields: patch, before: rec.fields });
        }
      }
      if (limit != null && scanned >= limit) return;
      next();
    });

  console.log(
    JSON.stringify(
      {
        table,
        dryRun,
        scanned,
        needsUpdate: toUpdate.length,
        samples: toUpdate.slice(0, 5).map((u) => ({
          id: u.id,
          titleBefore: u.before[MAP_ALERT.title],
          titleAfter: u.fields[MAP_ALERT.title],
        })),
      },
      null,
      2
    )
  );

  if (dryRun || toUpdate.length === 0) {
    process.exit(0);
  }

  let updated = 0;
  const errors = [];

  for (let i = 0; i < toUpdate.length; i += BATCH) {
    const batch = toUpdate.slice(i, i + BATCH).map(({ id, fields }) => ({ id, fields }));
    try {
      await base(table).update(batch, { typecast: true });
      updated += batch.length;
    } catch (err) {
      errors.push({ batchStart: i, message: err.message || String(err) });
      console.error("[cleanup-market-alerts-html-entities] batch failed:", err.message);
    }
  }

  console.log(JSON.stringify({ updated, errors }, null, 2));
  process.exit(errors.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
