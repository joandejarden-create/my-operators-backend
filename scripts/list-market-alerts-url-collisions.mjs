#!/usr/bin/env node
/** Find MarketAlerts sharing the same Source URL but different titles (RSS link errors). */
import "../load-env.js";
import Airtable from "airtable";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const byUrl = new Map();
  await base(table)
    .select({ fields: [MAP_ALERT.title, MAP_ALERT.sourceUrl, MAP_ALERT.publishedAt], pageSize: 100 })
    .eachPage((recs, next) => {
      recs.forEach((r) => {
        const url = (r.fields[MAP_ALERT.sourceUrl] || "").trim();
        if (!url) return;
        if (!byUrl.has(url)) byUrl.set(url, []);
        byUrl.get(url).push({ id: r.id, title: r.fields[MAP_ALERT.title], publishedAt: r.fields[MAP_ALERT.publishedAt] });
      });
      next();
    });

  const collisions = [...byUrl.entries()].filter(([, g]) => g.length > 1);
  console.log(`URL collisions: ${collisions.length} URLs, ${collisions.reduce((n, [, g]) => n + g.length, 0)} records`);
  for (const [url, group] of collisions.slice(0, 20)) {
    console.log(`\n${url.slice(0, 90)}`);
    group.forEach((r) => console.log(`  ${r.id} | ${r.title?.slice(0, 70)}`));
  }
}

main().catch(console.error);
