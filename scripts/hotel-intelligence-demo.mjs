#!/usr/bin/env node
/**
 * Read-only Hotel Intelligence MCP demo (fixture census; no secrets; no Airtable writes).
 *
 * Demonstrates:
 *  - 3 known existing census hotels
 *  - 2 likely new hotels
 *  - 1 deliberate ambiguous/duplicate case
 *  - 1 nearby-hotels query
 */
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import {
  createLocalStore,
  createHotelIntelligenceService,
} from "../lib/hotel-intelligence/index.js";
import { MAP_CENSUS_FIELDS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "hi-demo-"));
const store = createLocalStore({ root: tmpRoot });

const FIXTURES = [
  {
    id: "recDemo1",
    fields: {
      [MAP_CENSUS_FIELDS.propertyName]: "Courtyard Mexico City Airport",
      [MAP_CENSUS_FIELDS.officialName]: "Courtyard Mexico City Airport",
      [MAP_CENSUS_FIELDS.address]: "Blvd Puerto Aereo 502",
      [MAP_CENSUS_FIELDS.city]: "Mexico City",
      [MAP_CENSUS_FIELDS.country]: "Mexico",
      [MAP_CENSUS_FIELDS.latitude]: 19.4361,
      [MAP_CENSUS_FIELDS.longitude]: -99.0719,
      [MAP_CENSUS_FIELDS.roomCount]: 300,
      [MAP_CENSUS_FIELDS.brandName]: "Courtyard",
      [MAP_CENSUS_FIELDS.parentCompanyName]: "Marriott International",
      [MAP_CENSUS_FIELDS.hbxHotelCode]: "HBX111",
      [MAP_CENSUS_FIELDS.website]: "https://www.marriott.com/example-mexcy",
      [MAP_CENSUS_FIELDS.phone]: "+52 55 0000 0001",
      [MAP_CENSUS_FIELDS.identityConfidence]: "High",
      [MAP_CENSUS_FIELDS.chainScale]: "Upscale",
      [MAP_CENSUS_FIELDS.status]: "Census Only / Not Owner-Facing",
    },
  },
  {
    id: "recDemo2",
    fields: {
      [MAP_CENSUS_FIELDS.propertyName]: "Kimpton Aluna Tulum",
      [MAP_CENSUS_FIELDS.officialName]: "Kimpton Aluna Tulum",
      [MAP_CENSUS_FIELDS.city]: "Tulum",
      [MAP_CENSUS_FIELDS.country]: "Mexico",
      [MAP_CENSUS_FIELDS.latitude]: 20.211,
      [MAP_CENSUS_FIELDS.longitude]: -87.465,
      [MAP_CENSUS_FIELDS.roomCount]: 120,
      [MAP_CENSUS_FIELDS.brandName]: "Kimpton",
      [MAP_CENSUS_FIELDS.parentCompanyName]: "IHG",
      [MAP_CENSUS_FIELDS.identityConfidence]: "High",
    },
  },
  {
    id: "recDemo3",
    fields: {
      [MAP_CENSUS_FIELDS.propertyName]: "Hilton Santo Domingo",
      [MAP_CENSUS_FIELDS.officialName]: "Hilton Santo Domingo",
      [MAP_CENSUS_FIELDS.city]: "Santo Domingo",
      [MAP_CENSUS_FIELDS.country]: "Dominican Republic",
      [MAP_CENSUS_FIELDS.latitude]: 18.467,
      [MAP_CENSUS_FIELDS.longitude]: -69.93,
      [MAP_CENSUS_FIELDS.roomCount]: 256,
      [MAP_CENSUS_FIELDS.brandName]: "Hilton",
      [MAP_CENSUS_FIELDS.parentCompanyName]: "Hilton",
      [MAP_CENSUS_FIELDS.identityConfidence]: "Medium",
    },
  },
  {
    id: "recAmb1",
    fields: {
      [MAP_CENSUS_FIELDS.propertyName]: "Hotel Ambiguo Centro",
      [MAP_CENSUS_FIELDS.officialName]: "Hotel Ambiguo Centro",
      [MAP_CENSUS_FIELDS.city]: "Lima",
      [MAP_CENSUS_FIELDS.country]: "Peru",
      [MAP_CENSUS_FIELDS.latitude]: -12.05,
      [MAP_CENSUS_FIELDS.longitude]: -77.04,
    },
  },
  {
    id: "recAmb2",
    fields: {
      [MAP_CENSUS_FIELDS.propertyName]: "Hotel Ambiguo Centro",
      [MAP_CENSUS_FIELDS.officialName]: "Hotel Ambiguo Centro",
      [MAP_CENSUS_FIELDS.city]: "Lima",
      [MAP_CENSUS_FIELDS.country]: "Peru",
      [MAP_CENSUS_FIELDS.latitude]: -12.0501,
      [MAP_CENSUS_FIELDS.longitude]: -77.0401,
    },
  },
];

const service = createHotelIntelligenceService({
  store,
  censusRecords: FIXTURES,
  env: {
    ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
    ENABLE_HBX_CONTENT_API: "0",
  },
});

function section(title, payload) {
  console.log(`\n=== ${title} ===`);
  console.log(JSON.stringify(payload, null, 2));
}

const existing = [];
for (const q of [
  { name: "Courtyard Mexico City Airport", city: "Mexico City", country: "Mexico" },
  { name: "Kimpton Aluna Tulum", city: "Tulum", country: "Mexico" },
  { name: "Hilton Santo Domingo", city: "Santo Domingo", country: "Dominican Republic" },
]) {
  existing.push(await service.hotelResolve(q));
}
section("3 known existing census hotels (resolve)", existing);

const news = await service.hotelCensusIngest({
  enrich: false,
  records: [
    { name: "Selva Verde Eco Lodge Demo", city: "Sarapiqui", country: "Costa Rica" },
    { name: "Andes Cloud Hotel Demo", city: "Cusco", country: "Peru" },
  ],
});
section("2 likely new hotels (ingest stage-only)", {
  batch_status: news.batch_status,
  airtable_writes_made: news.airtable_writes_made,
  results: news.results,
});

const ambiguous = await service.hotelResolve({
  name: "Hotel Ambiguo Centro",
  city: "Lima",
  country: "Peru",
  latitude: -12.05005,
  longitude: -77.04005,
});
section("1 deliberate ambiguous/duplicate case", ambiguous);

const nearby = await service.hotelNearby({
  latitude: 19.436,
  longitude: -99.072,
  radius_km: 5,
  limit: 10,
});
section("1 nearby-hotels query (CDMX airport area)", {
  ok: nearby.ok,
  count: nearby.count,
  hotels: nearby.hotels.map((h) => ({
    hotel_id: h.hotel_id,
    hotel_name: h.hotel_name,
    distance_km: h.distance_km,
    rooms: h.rooms,
    brand: h.brand,
    parent_company: h.parent_company,
    confidence: h.confidence,
  })),
});

section("Safety", {
  airtable_writes_enabled: service.airtableWritesEnabled(),
  production_writes_made: 0,
  secrets_exposed: false,
  staging_dir: tmpRoot,
});

console.log("\nDEALALITY_HOTEL_INTELLIGENCE_MCP_DEMO_COMPLETE");
