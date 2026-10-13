/**
 * Search Hotel Property Census for Hilton New York Times Square (read-only).
 * Identity: NYCTSHH / 234 West 42nd Street / Hilton Times Square.
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;

const NEEDLES = [
  "hilton times square",
  "hilton new york times square",
  "nyctshh",
  "234 west 42",
  "234 w 42",
  "234 w. 42",
];

function scoreHit(f) {
  const name = String(f[CENSUS_FIELDS.name] || f.Name || f.name || "").toLowerCase();
  const addr = String(
    f[CENSUS_FIELDS.address] || f.Address || f.address || f["Street Address"] || ""
  ).toLowerCase();
  const code = String(
    f[CENSUS_FIELDS.hotelCode] ||
      f["Hotel Code"] ||
      f.hotelCode ||
      f["Property Code"] ||
      f.MARSHA ||
      ""
  ).toLowerCase();
  const url = String(
    f[CENSUS_FIELDS.website] || f.Website || f.website || f["Official Website"] || ""
  ).toLowerCase();
  const aff = String(f[CENSUS_FIELDS.affiliation] || "").toLowerCase();
  const blob = `${name} ${addr} ${code} ${url} ${aff}`;

  let score = 0;
  const reasons = [];
  if (code.includes("nyctshh") || url.includes("nyctshh")) {
    score += 100;
    reasons.push("HOTEL_CODE_NYCTSHH");
  }
  if (/234\s*w(est)?\.?\s*42/.test(addr)) {
    score += 80;
    reasons.push("ADDRESS_234_W_42");
  }
  if (/hilton/.test(name) && /times\s*square/.test(name)) {
    score += 70;
    reasons.push("NAME_HILTON_TIMES_SQUARE");
  }
  if (/hilton/.test(aff) && /times\s*square/.test(name)) {
    score += 40;
    reasons.push("AFFILIATION_HILTON_TS_NAME");
  }
  if (url.includes("hilton.com") && url.includes("times-square")) {
    score += 50;
    reasons.push("OFFICIAL_URL");
  }
  if (/hilton/.test(name) && /new york|nyc|manhattan/.test(blob) && /42/.test(blob)) {
    score += 20;
    reasons.push("NYC_42_HILTON");
  }
  return { score, reasons, blob };
}

function classify(score) {
  if (score >= 100) return "EXACT_CANONICAL_MATCH";
  if (score >= 70) return "LIKELY_DUPLICATE";
  if (score >= 30) return "AMBIGUOUS_MATCH";
  return "WEAK_CANDIDATE";
}

async function searchTable(tableName) {
  const base = new Airtable({ apiKey }).base(baseId);
  const hits = [];
  await base(tableName)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) {
        const f = r.fields || {};
        const name = String(f[CENSUS_FIELDS.name] || f.Name || f.name || "").toLowerCase();
        const city = String(f[CENSUS_FIELDS.city] || f.City || "").toLowerCase();
        const aff = String(f[CENSUS_FIELDS.affiliation] || "").toLowerCase();
        const addr = String(
          f[CENSUS_FIELDS.address] || f.Address || f.address || ""
        ).toLowerCase();
        const url = String(f[CENSUS_FIELDS.website] || f.Website || "").toLowerCase();
        const code = String(f[CENSUS_FIELDS.hotelCode] || f["Hotel Code"] || "").toLowerCase();
        const blob = `${name} ${city} ${aff} ${addr} ${url} ${code}`;

        const relevant =
          NEEDLES.some((n) => blob.includes(n)) ||
          (blob.includes("hilton") &&
            (blob.includes("times square") ||
              blob.includes("42nd") ||
              code.includes("nyct") ||
              url.includes("nyctshh")));

        if (!relevant) continue;

        const scored = scoreHit(f);
        hits.push({
          id: r.id,
          name: f[CENSUS_FIELDS.name] || f.Name || f.name,
          affiliation: f[CENSUS_FIELDS.affiliation],
          parentCompany: f[CENSUS_FIELDS.parentCompany],
          city: f[CENSUS_FIELDS.city] || f.City,
          state: f[CENSUS_FIELDS.state] || f.State,
          country: f[CENSUS_FIELDS.country] || f.Country,
          address: f[CENSUS_FIELDS.address] || f.Address || f.address,
          rooms: f[CENSUS_FIELDS.rooms] || f.rooms,
          website: f[CENSUS_FIELDS.website] || f.Website,
          hotelCode: f[CENSUS_FIELDS.hotelCode] || f["Hotel Code"] || null,
          market: f[CENSUS_FIELDS.market],
          status: f[CENSUS_FIELDS.status],
          score: scored.score,
          matchClass: classify(scored.score),
          reasons: scored.reasons,
        });
      }
      next();
    });
  hits.sort((a, b) => b.score - a.score);
  return hits;
}

async function main() {
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
    process.exit(1);
  }
  const tables = [HOTEL_CENSUS_TABLE, "Hotel Property Census", "Hotel Census"];
  const uniqueTables = [...new Set(tables.filter(Boolean))];
  const out = {
    at: new Date().toISOString(),
    baseId,
    target: {
      officialName: "Hilton New York Times Square",
      hotelCode: "NYCTSHH",
      address: "234 West 42nd Street, New York, NY 10036",
      url: "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/",
    },
    tables: {},
  };

  for (const t of uniqueTables) {
    try {
      const hits = await searchTable(t);
      out.tables[t] = { ok: true, count: hits.length, hits };
      console.log(JSON.stringify({ table: t, count: hits.length, top: hits.slice(0, 10) }, null, 2));
    } catch (e) {
      out.tables[t] = { ok: false, error: String(e.message || e).slice(0, 300) };
      console.log(JSON.stringify({ table: t, error: out.tables[t].error }, null, 2));
    }
  }

  const allHits = Object.values(out.tables)
    .flatMap((t) => t.hits || []);
  const best = allHits[0] || null;
  out.decision = best
    ? {
        classification: best.matchClass,
        bestId: best.id,
        bestName: best.name,
        score: best.score,
        reasons: best.reasons,
      }
    : { classification: "NO_EXISTING_CANONICAL_MATCH", bestId: null };

  const outDir = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "../reports/group-demand-intelligence/hotel4-hilton-times-square-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "PHASE0_1_HPC_SEARCH.json"),
    JSON.stringify(out, null, 2)
  );
  console.log("\nDECISION:", JSON.stringify(out.decision, null, 2));
  console.log("Wrote", path.join(outDir, "PHASE0_1_HPC_SEARCH.json"));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
