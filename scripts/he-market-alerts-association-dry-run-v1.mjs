#!/usr/bin/env node
/**
 * Dry-run Market Alerts ↔ HPC association plans (no writes).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planMarketAlertsHotelAssociations } from "../lib/hotel-intelligence/market-alerts-hotel-association-sync-v1.js";
import { proposedHpcHotelRecordIdsFieldSchema } from "../lib/hotel-intelligence/market-alerts-evidence-citations-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const hotels = [
  {
    id: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    brand: "Marriott Hotels",
    city: "Bethesda",
    aliases: ["Bethesda Marriott Hotel", "Marriott Bethesda"],
  },
];

const out = {
  generatedAt: new Date().toISOString(),
  applyAuthorized: false,
  marketAlertsHotelLinkApply: String(process.env.MARKET_ALERTS_HOTEL_LINK_APPLY || "") === "1",
  proposedFieldSchema: proposedHpcHotelRecordIdsFieldSchema(),
  assocFieldExists: false,
  liveArticlesSample: [],
  sync: null,
  notes: [],
};

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";

if (!apiKey || !baseId) {
  out.notes.push("Missing AIRTABLE env — fixture-only dry-run skipped for live rows");
} else {
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    await base(table)
      .select({ maxRecords: 1, fields: ["HPC Hotel Record IDs"] })
      .firstPage();
    out.assocFieldExists = true;
  } catch (_err) {
    out.assocFieldExists = false;
    out.notes.push("HPC Hotel Record IDs field not present — Tag-only association path");
  }

  const formula =
    "OR(SEARCH('loews nashville', LOWER({Title})), SEARCH('plaza hotel', LOWER({Title})), SEARCH('bethesda marriott', LOWER({Title})), SEARCH('bethesda marriott', LOWER({Hotel / Project})))";
  try {
    const rows = await base(table)
      .select({
        maxRecords: 20,
        filterByFormula: formula,
        fields: [
          "Title",
          "Hotel / Project",
          "Tags",
          "Source URL",
          "Source Name",
          "Published At",
          "Category",
          "Dedupe ID",
        ],
      })
      .firstPage();
    out.liveArticlesSample = rows.map((r) => ({ id: r.id, fields: r.fields }));
    out.sync = planMarketAlertsHotelAssociations(rows, hotels, {
      apply: false,
      assocFieldAvailable: out.assocFieldExists,
    });
    out.notes.push(
      `Live sample ${rows.length} article(s); wouldWriteCount=${out.sync.wouldWriteCount} (apply blocked)`
    );
  } catch (err) {
    out.notes.push(`Live select failed: ${String(err?.message || err)}`);
  }
}

const outPath = path.join(
  ROOT,
  "reports/hotel-explorer/HE-MARKET-ALERTS-ASSOCIATION-DRY-RUN-V1.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ ok: true, outPath, applyAuthorized: false }, null, 2));
