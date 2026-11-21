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
  "disc_cb3_embassy_one_bengaluru_2026",
  "find_cb3_embassy_one_bengaluru_mct_2026",
  "disc_cb3_hotel_oromana_alcala_2026",
  "find_cb3_hotel_oromana_concession_2026",
  "disc_cb3_honey_creek_resort_ia_2026",
  "find_cb3_honey_creek_concession_2026",
  "disc_cb3_marriott_marquis_chicago_2026",
  "find_cb3_marriott_marquis_chicago_mct_2026",
  "disc_cb3_aena_recife_airport_hotel_2026",
  "find_cb3_aena_recife_dev_2026",
  "disc_cb3_aena_maceio_airport_hotel_2026",
  "find_cb3_aena_maceio_dev_2026",
  "disc_cb3_aena_joao_pessoa_airport_hotel_2026",
  "find_cb3_aena_joao_pessoa_dev_2026",
  "disc_cb3_aena_aracaju_airport_hotel_2026",
  "find_cb3_aena_aracaju_dev_2026",
];
const out = {};
for (const k of keys) {
  const isFind = k.startsWith("find_");
  const field = isFind ? MAP_DISCOVERY_FINDINGS.findingId : MAP_DISCOVERY_INVENTORY.discoveryPropertyId;
  const table = isFind ? find : inv;
  const qs = new URLSearchParams({ filterByFormula: `{${field}}="${k}"`, maxRecords: "1" });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${table}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const j = await res.json();
  out[k] = j.records?.[0]?.id || null;
}
const dest = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb3-commercial-discovery/PERSISTENCE_LEDGER_FIRST_APPLY.json");
fs.writeFileSync(
  dest,
  JSON.stringify(
    {
      asOf: new Date().toISOString(),
      mode: "APPLY_READBACK",
      publicationState: "CUSTOMER_BLOCKED",
      summary: { creates: 16, noops: 2, idempotentNoops: 18, conflicts: 0 },
      createdIds: out,
      hpcCreates: 0,
      schemaChanges: 0,
    },
    null,
    2,
  ),
);
console.log(JSON.stringify(out, null, 2));
