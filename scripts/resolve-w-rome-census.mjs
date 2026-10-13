/**
 * Search Hotel Census for W Rome (read-only).
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;

async function searchTable(tableName) {
  const base = new Airtable({ apiKey }).base(baseId);
  const hits = [];
  const nameField = CENSUS_FIELDS.name;
  await base(tableName)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) {
        const f = r.fields || {};
        const name = String(f[nameField] || f.Name || f.name || "").toLowerCase();
        const city = String(f[CENSUS_FIELDS.city] || f.City || f.city || "").toLowerCase();
        const country = String(f[CENSUS_FIELDS.country] || f.Country || f.country || "").toLowerCase();
        const aff = String(f[CENSUS_FIELDS.affiliation] || "").toLowerCase();
        const blob = `${name} ${city} ${country} ${aff}`;
        if (!blob.includes("rome")) continue;
        const isW =
          /\bw rome\b/.test(name) ||
          /\bw hotels?\b/.test(name) ||
          /^w\s/.test(name) ||
          aff.includes("w hotels") ||
          aff.includes("w hotel");
        if (isW || name.includes("w rome")) {
          hits.push({
            id: r.id,
            name: f[nameField] || f.Name || f.name,
            affiliation: f[CENSUS_FIELDS.affiliation],
            parentCompany: f[CENSUS_FIELDS.parentCompany],
            city: f[CENSUS_FIELDS.city] || f.City,
            country: f[CENSUS_FIELDS.country] || f.Country,
            rooms: f[CENSUS_FIELDS.rooms] || f.rooms,
            market: f[CENSUS_FIELDS.market],
            chainScale: f[CENSUS_FIELDS.chainScale],
            serviceModel: f[CENSUS_FIELDS.hotelServiceModel],
            status: f[CENSUS_FIELDS.status],
          });
        }
      }
      next();
    });
  return hits;
}

async function main() {
  const tables = [HOTEL_CENSUS_TABLE, "Hotel Property Census", "Hotel Census"];
  const uniqueTables = [...new Set(tables)];
  const out = { baseId, tables: {} };
  for (const t of uniqueTables) {
    try {
      const hits = await searchTable(t);
      out.tables[t] = { ok: true, count: hits.length, hits };
      console.log(JSON.stringify({ table: t, count: hits.length, hits }, null, 2));
    } catch (e) {
      out.tables[t] = { ok: false, error: String(e.message || e).slice(0, 200) };
      console.log(JSON.stringify({ table: t, error: out.tables[t].error }, null, 2));
    }
  }
  const outDir = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "../reports/group-demand-intelligence/cross-market-replication-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, "PHASE0_W_ROME_CENSUS_SEARCH.json"), JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
