#!/usr/bin/env node
/** List duplicate MarketAlerts by title and dedupe ID. */
import "../load-env.js";
import Airtable from "airtable";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";

const FIELDS = [MAP_ALERT.title, MAP_ALERT.dedupeId, MAP_ALERT.sourceUrl, MAP_ALERT.publishedAt, MAP_ALERT.regionGroup];

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const rows = [];
  await base(table)
    .select({ fields: FIELDS, pageSize: 100 })
    .eachPage((recs, next) => {
      recs.forEach((r) => rows.push({ id: r.id, ...r.fields }));
      next();
    });

  const byTitle = new Map();
  const byDedupe = new Map();
  for (const r of rows) {
    const t = (r[MAP_ALERT.title] || "").trim().toLowerCase();
    if (t) {
      if (!byTitle.has(t)) byTitle.set(t, []);
      byTitle.get(t).push(r);
    }
    const d = r[MAP_ALERT.dedupeId];
    if (d) {
      if (!byDedupe.has(d)) byDedupe.set(d, []);
      byDedupe.get(d).push(r);
    }
  }

  console.log("=== Duplicate titles ===");
  for (const [t, group] of [...byTitle.entries()].filter(([, g]) => g.length > 1)) {
    console.log(`\n"${t.slice(0, 70)}" (${group.length})`);
    group.forEach((r) =>
      console.log(`  ${r.id} | ${r[MAP_ALERT.publishedAt]} | ${r[MAP_ALERT.sourceUrl]?.slice(0, 60)}`)
    );
  }

  console.log("\n=== Duplicate dedupe IDs ===");
  for (const [d, group] of [...byDedupe.entries()].filter(([, g]) => g.length > 1)) {
    console.log(`\nDedupe: ${d.slice(0, 16)}... (${group.length})`);
    group.forEach((r) => console.log(`  ${r.id} | ${r[MAP_ALERT.title]?.slice(0, 60)}`));
  }
}

main().catch(console.error);
