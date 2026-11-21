#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inv = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const find = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];
const keys = [
  "disc_cb5_westin_den_airport_mct_2026",
  "find_cb5_westin_den_airport_mct_2026",
  "disc_cb5_bryce_canyon_lodge_veia_2026",
  "find_cb5_bryce_canyon_lodge_veia_2026",
  "disc_cb4_ahip_remaining_portfolio_mct_2026",
  "find_cb4_ahip_aimbridge_mct_2026",
];

async function findBy(tableId, field, value) {
  const qs = new URLSearchParams({ filterByFormula: `{${field}}="${value}"`, maxRecords: "1" });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return json.records?.[0] || null;
}

const out = { asOf: new Date().toISOString(), inventory: {}, findings: {}, missing: [] };
for (const k of keys) {
  const isFinding = k.startsWith("find_");
  const rec = await findBy(
    isFinding ? find : inv,
    isFinding ? MAP_DISCOVERY_FINDINGS.findingId : MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    k,
  );
  if (!rec) out.missing.push(k);
  else if (isFinding) out.findings[k] = rec.id;
  else out.inventory[k] = rec.id;
  await new Promise((r) => setTimeout(r, 150));
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb5-trigger-first-portfolio");
fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "AIRTABLE_READBACK_IDS.json"), JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
if (out.missing.filter((k) => k.startsWith("disc_cb5_") || k.startsWith("find_cb5_")).length) process.exit(1);
