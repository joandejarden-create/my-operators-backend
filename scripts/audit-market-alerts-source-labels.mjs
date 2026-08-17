#!/usr/bin/env node
/**
 * Dry-run audit: MarketAlerts Source Name values that leak internal family/system suffixes.
 *
 * Usage: node scripts/audit-market-alerts-source-labels.mjs --dry-run
 *
 * Does not write to Airtable. Presentation sanitization is the repair path.
 */
import "../load-env.js";
import Airtable from "airtable";
import { getUserFacingSourceName } from "../lib/market-alerts-user-facing.js";

const TABLE = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
const SOURCE_FIELD = "Source Name";
const TITLE_FIELD = "Title";

const PATTERNS = [
  { key: "EARLY_SIGNAL_paren", re: /\(EARLY_SIGNAL(?:_[A-Z0-9]+)?\)/ },
  { key: "EARLY_SIGNAL_token", re: /\bEARLY_SIGNAL(?:_[A-Z0-9]+)?\b/ },
  { key: "RSS_paren", re: /\(\s*RSS\s*\)/i },
  { key: "GOOGLE_NEWS_family", re: /\bGOOGLE_NEWS(?:_[A-Z0-9]+)?\b/ },
];

function classify(source) {
  const hits = [];
  for (const p of PATTERNS) {
    if (p.re.test(source)) hits.push(p.key);
  }
  return hits;
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Airtable not configured");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const records = [];
  await base(TABLE)
    .select({ fields: [SOURCE_FIELD, TITLE_FIELD] })
    .eachPage((page, next) => {
      records.push(...page);
      next();
    });

  const affected = [];
  const byPattern = {};
  for (const rec of records) {
    const source = String(rec.fields[SOURCE_FIELD] || "");
    const hits = classify(source);
    if (!hits.length) continue;
    affected.push({
      id: rec.id,
      title: rec.fields[TITLE_FIELD] || "",
      sourceName: source,
      displaySourceName: getUserFacingSourceName(source),
      patterns: hits,
    });
    for (const h of hits) byPattern[h] = (byPattern[h] || 0) + 1;
  }

  const report = {
    dryRun: true,
    table: TABLE,
    totalRows: records.length,
    affectedRows: affected.length,
    byPattern,
    sampleRecords: affected.slice(0, 12),
    historicalMutation: "NOT recommended — sanitize at API presentation time",
  };

  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
