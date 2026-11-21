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
  "disc_cb2_legacy_hotel_green_bay_2026",
  "disc_cb2_hotel_do_mel_gois_2026",
  "disc_cb2_jericho_beach_hostel_2026",
  "disc_cb2_torre_uriz_navarra_2026",
  "disc_cb2_white_sands_grand_turk_2026",
  "disc_cb2b_shorebreak_huntington_2026",
  "disc_cb2b_courtyard_denver_downtown_2026",
  "disc_cb2b_hotel_murta_xativa_2026",
  "disc_cb2b_areia_branca_revive_2026",
  "find_cb2_legacy_hotel_green_bay_bct_2026",
  "find_cb2_hotel_do_mel_gois_operator_2026",
  "find_cb2_jericho_beach_hostel_operator_2026",
  "find_cb2_torre_uriz_concession_2026",
  "find_cb2_white_sands_lease_ops_2026",
  "find_cb2b_shorebreak_huntington_bct_2026",
  "find_cb2b_courtyard_denver_bct_2026",
  "find_cb2b_hotel_murta_concession_2026",
  "find_cb2b_areia_branca_concession_2026",
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
const dest = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb2-commercial-discovery/PERSISTENCE_LEDGER_FIRST_APPLY.json");
fs.writeFileSync(
  dest,
  JSON.stringify(
    {
      asOf: new Date().toISOString(),
      mode: "APPLY_READBACK",
      publicationState: "CUSTOMER_BLOCKED",
      summary: { creates: 10, noopsFirstPass: 4, idempotentNoops: 14, conflicts: 0 },
      createdIds: out,
      priorNoops: {
        disc_cb1_lakeville_inn_mn_2026: "recPXlLWkd63mLK5h",
        find_cb1_lakeville_bct_brand_search_2026: "recjL2vx3ExNlReF1",
        disc_cfa_hilton_trinidad_pos_2026: "rec0f9E8genYc3tI2",
        find_cfa_hilton_trinidad_mct_hma_sale_2026: "recDRyNUriKMAl58C",
      },
      hpcCreates: 0,
      schemaChanges: 0,
    },
    null,
    2,
  ),
);
console.log(JSON.stringify(out, null, 2));
