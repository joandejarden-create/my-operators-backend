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
import { fetchMarketAlertsRssItems } from "../api/market-alerts-news.js";
import { syncRssItemsToAirtable } from "../api/lib/market-alerts-rss-airtable.js";

function parseArgs(argv) {
  let limit = parseInt(process.env.RSS_TOTAL_TARGET || "100", 10);
  let dryRun = process.env.DRY_RUN === "true";

  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--dry-run") dryRun = true;
    if (argv[i] === "--limit" && argv[i + 1]) limit = parseInt(argv[i + 1], 10);
    if (argv[i].startsWith("--limit=")) limit = parseInt(argv[i].slice("--limit=".length), 10);
  }

  limit = Math.min(Math.max(limit || 100, 1), 250);
  return { limit, dryRun };
}

const { limit, dryRun } = parseArgs(process.argv.slice(2));

console.log(`Fetching up to ${limit} RSS items…`);
const items = await fetchMarketAlertsRssItems({ limit });
console.log(`Fetched ${items.length} unique headline(s). Syncing to Airtable${dryRun ? " (dry run)" : ""}…`);

const result = await syncRssItemsToAirtable({ items, dryRun });

if (!result.ok && result.error) {
  console.error(result.error);
  process.exit(1);
}

console.log(JSON.stringify(result, null, 2));

if (result.createErrors?.length) {
  process.exit(1);
}
