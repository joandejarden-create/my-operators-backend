#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

async function count(country) {
  let offset;
  let n = 0;
  do {
    const qs = new URLSearchParams({
      pageSize: "100",
      filterByFormula: `{Country}="${country}"`,
    });
    qs.append("fields[]", "Country");
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const j = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(j));
    n += (j.records || []).length;
    offset = j.offset;
  } while (offset);
  return n;
}

const out = {
  asOf: new Date().toISOString(),
  UnitedStates: await count("United States"),
  Mexico: await count("Mexico"),
  Brazil: await count("Brazil"),
  UnitedKingdom: await count("United Kingdom"),
  Canada: await count("Canada"),
};
fs.mkdirSync(path.join(ROOT, "reports/sec-edgar-hif-track-c-universe-expansion"), { recursive: true });
fs.writeFileSync(
  path.join(ROOT, "reports/sec-edgar-hif-track-c-universe-expansion/LIVE_PRIORITY_RECOUNT.json"),
  `${JSON.stringify(out, null, 2)}\n`,
);
console.log(JSON.stringify(out, null, 2));
