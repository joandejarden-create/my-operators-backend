#!/usr/bin/env node
/**
 * Pull hospitality RSS headlines (same feeds as GET /api/market-alerts/news).
 *
 * Usage:
 *   node scripts/fetch-market-alerts-rss.mjs
 *   node scripts/fetch-market-alerts-rss.mjs --limit 10
 *   node scripts/fetch-market-alerts-rss.mjs --json > reports/rss-sample.json
 */
import "dotenv/config";
import { fetchMarketAlertsRssItems } from "../api/market-alerts-news.js";

function parseLimit(argv) {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--limit" && argv[i + 1]) {
      return parseInt(argv[i + 1], 10);
    }
    if (argv[i].startsWith("--limit=")) {
      return parseInt(argv[i].slice("--limit=".length), 10);
    }
  }
  return 20;
}

const limit = parseLimit(process.argv.slice(2));
const jsonOnly = process.argv.includes("--json");

const items = await fetchMarketAlertsRssItems({ limit });

if (jsonOnly) {
  console.log(JSON.stringify({ success: true, count: items.length, items }, null, 2));
  process.exit(0);
}

console.log(`Fetched ${items.length} RSS item(s) (limit ${limit}):\n`);
for (const item of items) {
  console.log(`— ${item.source} · ${item.pubDate || "no date"}`);
  console.log(`  ${item.title}`);
  if (item.summary) {
    const blurb = item.summary.replace(/\s+/g, " ").trim().slice(0, 160);
    console.log(`  ${blurb}${item.summary.length > 160 ? "…" : ""}`);
  }
  if (item.link) console.log(`  ${item.link}`);
  console.log("");
}
