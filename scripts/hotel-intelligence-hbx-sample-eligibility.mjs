#!/usr/bin/env node
/**
 * Read-only: count HBX-linked hotels in frozen CALA sample.
 * No Hotelbeds LIVE calls. No Airtable writes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/hotelbeds-live-rooms-validation-v1"
);
const FROZEN = path.join(
  ROOT,
  "reports/hotel-intelligence/cala-validation-v1/01-sample-definition.json"
);

const def = JSON.parse(fs.readFileSync(FROZEN, "utf8"));
const ids = def.record_ids;
const pat = (
  process.env.AIRTABLE_PAT ||
  process.env.AIRTABLE_TOKEN ||
  process.env.AIRTABLE_API_KEY ||
  ""
).trim();
const baseId = (
  process.env.AIRTABLE_BASE_ID_ALT ||
  process.env.AIRTABLE_BASE_ID ||
  ""
).trim();
if (!pat || !baseId) throw new Error("Airtable read credentials missing");

const table = new Airtable({ apiKey: pat }).base(baseId)(
  MAP_HOTEL_PROPERTY_CENSUS.tableId
);
const fields = [
  MAP_CENSUS_FIELDS.propertyName,
  MAP_CENSUS_FIELDS.officialName,
  MAP_CENSUS_FIELDS.country,
  MAP_CENSUS_FIELDS.city,
  MAP_CENSUS_FIELDS.roomCount,
  MAP_CENSUS_FIELDS.brandName,
  MAP_CENSUS_FIELDS.hbxHotelCode,
  MAP_CENSUS_FIELDS.latitude,
  MAP_CENSUS_FIELDS.longitude,
  MAP_CENSUS_FIELDS.propertyType,
];

const byId = new Map();
for (let i = 0; i < ids.length; i += 40) {
  const chunk = ids.slice(i, i + 40);
  const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
  await table
    .select({ filterByFormula: formula, fields, pageSize: 100 })
    .eachPage((recs, next) => {
      for (const r of recs) byId.set(r.id, r);
      next();
    });
}

function hasRooms(f) {
  const n = Number(f[MAP_CENSUS_FIELDS.roomCount]);
  return Number.isFinite(n) && n > 0;
}

const rows = ids.map((id) => {
  const r = byId.get(id);
  if (!r) throw new Error(`missing frozen record ${id}`);
  const f = r.fields || {};
  return {
    census_record_id: id,
    name:
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      null,
    country: f[MAP_CENSUS_FIELDS.country] || null,
    city: f[MAP_CENSUS_FIELDS.city] || null,
    brand: f[MAP_CENSUS_FIELDS.brandName] || null,
    property_type: f[MAP_CENSUS_FIELDS.propertyType] || null,
    hbx_hotel_code: String(f[MAP_CENSUS_FIELDS.hbxHotelCode] || "").trim() || null,
    rooms: hasRooms(f) ? Number(f[MAP_CENSUS_FIELDS.roomCount]) : null,
    has_coords: Number.isFinite(Number(f[MAP_CENSUS_FIELDS.latitude])),
  };
});

const hbx = rows.filter((r) => r.hbx_hotel_code);
const hbxMissing = hbx.filter((r) => r.rooms == null);
const hbxWithRooms = hbx.filter((r) => r.rooms != null);

const countryCounts = {};
for (const r of hbx) {
  countryCounts[r.country || "unknown"] =
    (countryCounts[r.country || "unknown"] || 0) + 1;
}

fs.mkdirSync(OUT_DIR, { recursive: true });
const eligibility = {
  seed: def.seed,
  total: rows.length,
  hbx_linked: hbx.length,
  hbx_missing_rooms: hbxMissing.length,
  hbx_with_rooms: hbxWithRooms.length,
  country_counts: countryCounts,
  hbx_record_ids: hbx.map((r) => r.census_record_id),
  hbx_missing_room_ids: hbxMissing.map((r) => r.census_record_id),
  rows_hbx: hbx,
};
fs.writeFileSync(
  path.join(OUT_DIR, "01-eligibility.json"),
  `${JSON.stringify(eligibility, null, 2)}\n`
);
console.log(
  JSON.stringify(
    {
      total: eligibility.total,
      hbx_linked: eligibility.hbx_linked,
      hbx_missing_rooms: eligibility.hbx_missing_rooms,
      hbx_with_rooms: eligibility.hbx_with_rooms,
      country_counts: eligibility.country_counts,
    },
    null,
    2
  )
);
