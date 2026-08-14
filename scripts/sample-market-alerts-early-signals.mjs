#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";

function familyFromTags(tags) {
  const t = tags || [];
  if (t.includes("EARLY_SIGNAL_PLANNING")) return "Planning";
  if (t.includes("EARLY_SIGNAL_DEVELOPMENT")) return "Early Development";
  if (t.includes("EARLY_SIGNAL_MIXED_USE")) return "Mixed Use";
  if (t.includes("EARLY_SIGNAL_ADAPTIVE_REUSE")) return "Adaptive Reuse";
  return "Early Signal";
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";

  const records = await base(table)
    .select({
      filterByFormula: 'FIND("EARLY_SIGNAL", ARRAYJOIN({Tags}))',
      sort: [{ field: MAP_ALERT.publishedAt, direction: "desc" }],
      maxRecords: 20,
    })
    .all();

  const rows = records.map((r, i) => {
    const f = r.fields;
    const intel = computeMarketAlertIntelligence({
      title: f[MAP_ALERT.title] || "",
      summary: f[MAP_ALERT.summary] || "",
      sourceName: f[MAP_ALERT.sourceName] || "",
    });
    return {
      n: i + 1,
      id: r.id,
      title: f[MAP_ALERT.title],
      source: f[MAP_ALERT.sourceName],
      queryFamily: familyFromTags(f[MAP_ALERT.tags]),
      eventType: f[MAP_INTEL.eventType] || intel.meta.event?.eventType,
      signalTiming: intel.meta.signalTiming,
      projectDirection: intel.meta.projectDirection,
      hotelProject: f[MAP_INTEL.hotelProject] || "",
      projectLabel: intel.meta.projectLabel || "",
      location: f[MAP_ALERT.regionGroup],
      brand: f[MAP_INTEL.brandInvolved] || "",
      operator: f[MAP_INTEL.operatorInvolved] || "",
      ownerWr: !!f[MAP_INTEL.worthReviewingOwner],
      brandWr: !!f[MAP_INTEL.worthReviewingBrand],
      operatorWr: !!f[MAP_INTEL.worthReviewingOperator],
      whyItMatters: f[MAP_INTEL.whyItMattersBrand] || f[MAP_INTEL.whyItMattersOwner] || "",
      recommendedAction:
        f[MAP_INTEL.recommendedActionBrand] || f[MAP_INTEL.recommendedActionOwner] || "",
      sourceUrl: f[MAP_ALERT.sourceUrl],
    };
  });

  const outPath = path.join(process.cwd(), "data", "market-alerts-early-signal-first-20.json");
  fs.writeFileSync(outPath, JSON.stringify(rows, null, 2));
  console.log(JSON.stringify(rows, null, 2));
  console.log(`\nWrote ${outPath}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
