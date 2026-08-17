#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
const rows = [];

await base(table)
  .select({
    fields: [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName],
    pageSize: 100,
  })
  .eachPage((recs, next) => {
    for (const r of recs) {
      const summary = r.fields[MAP_ALERT.summary] || "";
      if (/<[^>]+>|&lt;[^&]+&gt;|field--name-|data-pm-slice|class="field/.test(summary)) {
        rows.push({
          id: r.id,
          title: r.fields[MAP_ALERT.title],
          source: r.fields[MAP_ALERT.sourceName],
          summary: summary.slice(0, 280),
        });
      }
    }
    next();
  });

console.log(JSON.stringify({ count: rows.length, samples: rows.slice(0, 20) }, null, 2));
