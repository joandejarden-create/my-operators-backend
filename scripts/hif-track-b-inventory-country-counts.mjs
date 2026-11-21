#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
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
const inv = ids.tableIds["HI Hotel Discovery Inventory"];
const findings = ids.tableIds["HI Discovery Findings"];

async function count(tableId, formula) {
  let n = 0;
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    n += (json.records || []).length;
    offset = json.offset;
  } while (offset);
  return n;
}

async function searchHpc(nameContains) {
  const formula = `FIND(LOWER("${nameContains.toLowerCase()}"), LOWER({Hotel Name}&""))`;
  const qs = new URLSearchParams({
    pageSize: "5",
    filterByFormula: formula,
    "fields[]": "Hotel Name",
  });
  qs.append("fields[]", "Country");
  qs.append("fields[]", "City");
  qs.append("fields[]", "Market");
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/tbl9aY5ijiuIzzWam?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return (json.records || []).map((r) => ({ id: r.id, ...r.fields }));
}

const labels = ["US", "United States", "CA", "Canada", "MX", "Mexico", "BR", "Brazil", "GB", "United Kingdom", "UK"];
const inventoryByCountry = {};
for (const c of labels) {
  inventoryByCountry[c] = await count(inv, `{Country}="${c}"`);
}

const totalInv = await count(inv, "TRUE()");
const totalFindings = await count(findings, "TRUE()");

const hpcSearches = {};
for (const q of ["Yorkville", "Rebouças", "Reboccas", "Helbor", "Thornhill", "Lakeville"]) {
  try {
    hpcSearches[q] = await searchHpc(q);
  } catch (e) {
    hpcSearches[q] = { error: String(e.message || e) };
  }
}

const out = {
  asOf: new Date().toISOString(),
  totalInventory: totalInv,
  totalFindings,
  inventoryByCountry,
  hpcSearches,
  trackAReserved: ["disc_cb1_lakeville_inn_mn_2026"],
};

const outDir = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-foundation");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "INVENTORY_COUNTRY_AND_HPC_SEARCH.json"), JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
