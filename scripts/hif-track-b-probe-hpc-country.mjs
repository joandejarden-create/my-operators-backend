#!/usr/bin/env node
/**
 * Probe HPC Country / Market distribution (read-only).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

async function fetchPage(offset) {
  const qs = new URLSearchParams({
    pageSize: "100",
    "fields[]": "Country",
  });
  // Airtable needs multiple fields[] — append Market
  qs.append("fields[]", "Market");
  if (offset) qs.set("offset", offset);
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
}

async function main() {
  const countryDist = new Map();
  const marketDist = new Map();
  let scanned = 0;
  let offset;
  do {
    const json = await fetchPage(offset);
    for (const r of json.records || []) {
      const c = r.fields?.Country || "(empty)";
      const m = r.fields?.Market || "(empty)";
      countryDist.set(c, (countryDist.get(c) || 0) + 1);
      marketDist.set(m, (marketDist.get(m) || 0) + 1);
      scanned += 1;
    }
    offset = json.offset;
    if (scanned % 2000 === 0) process.stderr.write(`scanned ${scanned}...\n`);
  } while (offset);

  const countries = [...countryDist.entries()].sort((a, b) => b[1] - a[1]);
  const markets = [...marketDist.entries()].sort((a, b) => b[1] - a[1]);
  const out = {
    ok: true,
    scanned,
    countries,
    marketsTop40: markets.slice(0, 40),
    marketCount: markets.length,
  };
  const outDir = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-foundation");
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, "hpc-country-probe.json"), JSON.stringify(out, null, 2));
  console.log(JSON.stringify({ ok: true, scanned, countryCount: countries.length, topCountries: countries.slice(0, 25) }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
