#!/usr/bin/env node
/**
 * Tripadvisor Apify benchmark — READ ONLY sample builder.
 * No Airtable writes.
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
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/tripadvisor-apify-benchmark-v1"
);

function diversify(pool, n) {
  const byCountry = new Map();
  for (const h of pool) {
    if (!byCountry.has(h.country)) byCountry.set(h.country, []);
    byCountry.get(h.country).push(h);
  }
  const countries = [...byCountry.keys()].sort(
    (a, b) => byCountry.get(b).length - byCountry.get(a).length
  );
  const picked = [];
  const used = new Set();
  let guard = 0;
  while (picked.length < n && guard < n * 30) {
    guard += 1;
    let progressed = false;
    for (const c of countries) {
      if (picked.length >= n) break;
      const arr = byCountry.get(c);
      while (arr.length) {
        const h = arr.shift();
        if (used.has(h.record_id)) continue;
        used.add(h.record_id);
        picked.push(h);
        progressed = true;
        break;
      }
    }
    if (!progressed) break;
  }
  return picked;
}

async function main() {
  const token = (
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
  if (!token || !baseId) throw new Error("Airtable credentials missing");

  const base = new Airtable({ apiKey: token }).base(baseId);
  const fields = [
    MAP_CENSUS_FIELDS.propertyName,
    MAP_CENSUS_FIELDS.officialName,
    MAP_CENSUS_FIELDS.city,
    MAP_CENSUS_FIELDS.country,
    MAP_CENSUS_FIELDS.roomCount,
    MAP_CENSUS_FIELDS.brandName,
    MAP_CENSUS_FIELDS.latitude,
    MAP_CENSUS_FIELDS.longitude,
    MAP_CENSUS_FIELDS.website,
    MAP_CENSUS_FIELDS.identityConfidence,
    MAP_CENSUS_FIELDS.dataConfidenceTier,
    MAP_CENSUS_FIELDS.status,
  ];

  const withRooms = [];
  const missing = [];

  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({ pageSize: 100, fields })
    .eachPage((page, next) => {
      for (const rec of page) {
        const f = rec.fields || {};
        const name = String(
          f[MAP_CENSUS_FIELDS.officialName] ||
            f[MAP_CENSUS_FIELDS.propertyName] ||
            ""
        ).trim();
        const country = String(f[MAP_CENSUS_FIELDS.country] || "").trim();
        if (!name || !country) continue;
        const roomsRaw = f[MAP_CENSUS_FIELDS.roomCount];
        const rooms =
          roomsRaw != null && roomsRaw !== "" ? Number(roomsRaw) : null;
        const tier = String(
          f[MAP_CENSUS_FIELDS.dataConfidenceTier] ||
            f[MAP_CENSUS_FIELDS.identityConfidence] ||
            ""
        );
        const row = {
          record_id: rec.id,
          name,
          city: String(f[MAP_CENSUS_FIELDS.city] || "").trim() || null,
          country,
          rooms: Number.isFinite(rooms) ? rooms : null,
          brand: String(f[MAP_CENSUS_FIELDS.brandName] || "").trim() || null,
          lat: f[MAP_CENSUS_FIELDS.latitude] ?? null,
          lng: f[MAP_CENSUS_FIELDS.longitude] ?? null,
          website: String(f[MAP_CENSUS_FIELDS.website] || "").trim() || null,
          identity_confidence: f[MAP_CENSUS_FIELDS.identityConfidence] ?? null,
          data_confidence_tier: f[MAP_CENSUS_FIELDS.dataConfidenceTier] ?? null,
          status: f[MAP_CENSUS_FIELDS.status] ?? null,
          _high: /high/i.test(tier),
        };
        if (row.rooms != null && row.rooms > 0) withRooms.push(row);
        else missing.push(row);
      }
      next();
    });

  const knownHigh = withRooms.filter((h) => h._high);
  const knownRest = withRooms.filter((h) => !h._high);
  const known = diversify(
    [...knownHigh, ...knownRest],
    Math.min(100, withRooms.length)
  );

  const missRanked = [...missing].sort((a, b) => {
    const sa = (a.website ? 2 : 0) + (a.lat != null ? 1 : 0) + (a.brand ? 1 : 0);
    const sb = (b.website ? 2 : 0) + (b.lat != null ? 1 : 0) + (b.brand ? 1 : 0);
    return sb - sa;
  });
  const miss = diversify(missRanked, 100);

  const strip = (h) => {
    const { _high, ...rest } = h;
    return rest;
  };

  const destCounts = new Map();
  for (const h of [...known, ...miss]) {
    const key = `${h.city || "Unknown"}|${h.country}`;
    if (!destCounts.has(key)) {
      destCounts.set(key, {
        city: h.city,
        country: h.country,
        query: h.city ? `${h.city}, ${h.country}` : h.country,
        n: 0,
      });
    }
    destCounts.get(key).n += 1;
  }
  const destinations = [...destCounts.values()]
    .sort((a, b) => b.n - a.n)
    .slice(0, 30);

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "samples.json"),
    JSON.stringify(
      {
        created_at: new Date().toISOString(),
        production_writes: false,
        known_room_universe: withRooms.length,
        known_high_confidence: knownHigh.length,
        missing_universe: missing.length,
        phase2_known: known.map(strip),
        phase3_missing: miss.map(strip),
      },
      null,
      2
    )
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "destinations.json"),
    JSON.stringify({ destinations }, null, 2)
  );

  console.log(
    JSON.stringify(
      {
        known_selected: known.length,
        missing_selected: miss.length,
        known_universe: withRooms.length,
        known_high: knownHigh.length,
        destinations: destinations.length,
        top_dest: destinations.slice(0, 12),
        known_countries: new Set(known.map((h) => h.country)).size,
        miss_countries: new Set(miss.map((h) => h.country)).size,
        room_size_bands: {
          small: known.filter((h) => h.rooms < 50).length,
          mid: known.filter((h) => h.rooms >= 50 && h.rooms < 150).length,
          large: known.filter((h) => h.rooms >= 150).length,
        },
        branded_known: known.filter((h) => h.brand).length,
        independent_known: known.filter((h) => !h.brand).length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(String(err?.message || err));
  process.exitCode = 1;
});
