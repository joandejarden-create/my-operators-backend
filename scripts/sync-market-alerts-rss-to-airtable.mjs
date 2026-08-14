#!/usr/bin/env node
/**
 * Fetch hospitality RSS headlines and upsert new rows into MarketAlerts (by Dedupe ID).
 *
 * Usage:
 *   node scripts/sync-market-alerts-rss-to-airtable.mjs
 *   node scripts/sync-market-alerts-rss-to-airtable.mjs --limit 100
 *   node scripts/sync-market-alerts-rss-to-airtable.mjs --dry-run --limit 50
 */
import "../load-env.js";
import { runMarketAlertsRssSync } from "../api/run-market-alerts-rss-sync.js";

function parseArgs(argv) {
  let limit = parseInt(process.env.RSS_TOTAL_TARGET || "100", 10);
  let dryRun = process.env.DRY_RUN === "true";

  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--dry-run") dryRun = true;
    if (argv[i] === "--limit" && argv[i + 1]) limit = parseInt(argv[i + 1], 10);
    if (argv[i].startsWith("--limit=")) limit = parseInt(argv[i].slice("--limit=".length), 10);
  }

  return { limit, dryRun };
}

const { limit, dryRun } = parseArgs(process.argv.slice(2));

console.log(`Fetching up to ${limit} RSS items…`);
const result = await runMarketAlertsRssSync({ limit, dryRun });

if (!result.ok && result.error) {
  console.error(result.error);
  process.exit(1);
}

console.log(JSON.stringify(result, null, 2));

if (result.createErrors?.length) {
  process.exit(1);
}
