#!/usr/bin/env node
/**
 * Before/after hotel population comparison — Legacy Census vs HPC.
 * Explains material variance; does not force parity.
 * Read-only. Legacy reads require DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  fetchAllLegacyHotelCensusRecords,
} from "../lib/hotel-census/dealality-legacy-hpc-reconciliation-v1.js";
import { LEGACY_CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
  HOTEL_PROPERTY_CENSUS_TABLE_ID,
  getPlatformBase,
} from "../lib/hotel-census/platform-base.js";
import { migrationReadAllowed } from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
const HPC_COMPARE_FIELDS = [
  "Property Name",
  "Current Brand",
  "Brand Family",
  "Radar Status",
  "Rooms / Keys",
  "Country",
  "City",
  "Market",
  "Submarket",
];

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.join(__dirname, "..", "reports/hotel-explorer/global-census-retirement");
fs.mkdirSync(outDir, { recursive: true });

function argValue(name) {
  const hit = process.argv.find((a) => a.startsWith(`${name}=`));
  return hit ? hit.slice(name.length + 1) : null;
}

const maxRecords = argValue("--max-records") ? Number(argValue("--max-records")) : 500;

function bucketKey(parts) {
  return parts.map((p) => String(p || "").trim() || "(blank)").join(" | ");
}

function inc(map, key) {
  map[key] = (map[key] || 0) + 1;
}

function topN(map, n = 15) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([k, v]) => ({ key: k, count: v }));
}

function text(fields, keys) {
  for (const k of keys) {
    const v = fields?.[k];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

async function fetchHpcSample(limit) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing platform Airtable config");
  const records = await base(HOTEL_PROPERTY_CENSUS_TABLE)
    .select({
      fields: [...HPC_COMPARE_FIELDS],
      pageSize: 100,
      maxRecords: limit,
    })
    .all();
  return records;
}

const hpcRecords = await fetchHpcSample(maxRecords);
const hpcBy = {
  country: {},
  market: {},
  brand: {},
  status: {},
};
let hpcRooms = 0;
for (const rec of hpcRecords) {
  const f = rec.fields || {};
  inc(hpcBy.country, text(f, ["Country"]));
  inc(hpcBy.market, text(f, ["Market"]));
  inc(hpcBy.brand, text(f, ["Current Brand", "Brand Family"]));
  inc(hpcBy.status, text(f, ["Radar Status"]));
  const rooms = Number(f["Rooms / Keys"]);
  if (Number.isFinite(rooms)) hpcRooms += rooms;
}

let legacy = {
  attempted: false,
  count: 0,
  by: { country: {}, market: {}, brand: {}, status: {} },
  rooms: 0,
  note: null,
};

if (migrationReadAllowed({ allowLegacyHotelCensusRead: true })) {
  process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ = "1";
  const legacyRecords = await fetchAllLegacyHotelCensusRecords({
    maxRecords,
    workflow: "compare-legacy-hpc-population-v1",
  });
  legacy.attempted = true;
  legacy.count = legacyRecords.length;
  for (const rec of legacyRecords) {
    const f = rec.fields || {};
    inc(legacy.by.country, text(f, [LEGACY_CENSUS_FIELDS.country, "country"]));
    inc(legacy.by.market, text(f, [LEGACY_CENSUS_FIELDS.market, "Market"]));
    inc(legacy.by.brand, text(f, [LEGACY_CENSUS_FIELDS.affiliation, "Affiliation"]));
    inc(legacy.by.status, text(f, [LEGACY_CENSUS_FIELDS.status, "status"]));
    const rooms = Number(f[LEGACY_CENSUS_FIELDS.rooms] ?? f.rooms);
    if (Number.isFinite(rooms)) legacy.rooms += rooms;
  }
} else {
  legacy.note =
    "Legacy sample skipped — set DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1 for before/after Census counts";
}

const varianceExplanations = [
  {
    type: "DENOMINATOR_DEFINITION",
    detail:
      "Legacy Census status uses Open/Pipeline on `status`; HPC uses Radar Status Open|Pipeline|Hidden — counts are not forced to parity.",
  },
  {
    type: "COVERAGE_DIFFERENCE",
    detail:
      "HPC may exclude unverified / incomplete properties that remain in legacy Census; conversely HPC may include new CREATE hotels absent from legacy.",
  },
  {
    type: "DUPLICATE_REMOVAL",
    detail:
      "Multiple legacy rows can map to one HPC id (see DUPLICATE_LEGACY_RECORD disposition) — hotel count can fall without disappearance of a real property.",
  },
  {
    type: "PILOT_OVERLAY",
    detail:
      "Secrets Papagayo: legacy recyX1wLsHBJYRPw7 → HPC recFQ0WvGLbfK5Gpa (HE 5.0 CREATE). Not a disappearance.",
  },
];

const report = {
  contract: "DEALALITY_LEGACY_HPC_POPULATION_COMPARE_V1",
  generatedAt: new Date().toISOString(),
  sampleLimit: maxRecords,
  hpc: {
    tableId: HOTEL_PROPERTY_CENSUS_TABLE_ID,
    sampleCount: hpcRecords.length,
    roomsSumSample: hpcRooms,
    topCountry: topN(hpcBy.country),
    topMarket: topN(hpcBy.market),
    topBrand: topN(hpcBy.brand),
    byStatus: hpcBy.status,
  },
  legacy,
  varianceExplanations,
  rule: "Do not force artificial numerical parity. Explain material variance with record-level dispositions.",
  products: {
    Scout: "HPC via census-read-cache",
    Radar: "HPC Open|Pipeline",
    BrandExplorer: "HPC Current Brand footprint",
    BrandPresence: "HPC",
    OperatorsByBrandRegion: "HPC",
    BrandFit: "HPC",
  },
};

fs.writeFileSync(
  path.join(outDir, "LEGACY-HPC-POPULATION-COMPARE-V1.json"),
  JSON.stringify(report, null, 2)
);

const md = [];
md.push("# Legacy vs HPC population comparison v1");
md.push("");
md.push(`Generated: ${report.generatedAt}`);
md.push(`Sample limit: ${maxRecords}`);
md.push("");
md.push(`HPC sample count: **${hpcRecords.length}**`);
md.push(
  legacy.attempted
    ? `Legacy sample count: **${legacy.count}**`
    : `Legacy sample: skipped (${legacy.note})`
);
md.push("");
md.push("## Variance explanations (not forced parity)");
md.push("");
for (const v of varianceExplanations) {
  md.push(`- **${v.type}**: ${v.detail}`);
}
md.push("");
md.push("## HPC status buckets (sample)");
md.push("");
md.push("```json");
md.push(JSON.stringify(hpcBy.status, null, 2));
md.push("```");

fs.writeFileSync(path.join(outDir, "LEGACY-HPC-POPULATION-COMPARE-V1.md"), md.join("\n"));
console.log(
  JSON.stringify(
    {
      hpcSample: hpcRecords.length,
      legacySample: legacy.count,
      legacyAttempted: legacy.attempted,
    },
    null,
    2
  )
);
console.log("Wrote LEGACY-HPC-POPULATION-COMPARE-V1.*");
