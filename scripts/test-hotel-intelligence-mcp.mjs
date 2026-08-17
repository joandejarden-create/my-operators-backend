#!/usr/bin/env node
/**
 * Hotel Intelligence MCP V1 unit tests (no Airtable / no live HBX required).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  generateDealalityHotelId,
  isDealalityHotelId,
  createLocalStore,
  createExternalIdRegistry,
  createEvidenceStore,
  createReviewQueue,
  scoreFieldConfidence,
  preferCanonicalValue,
  resolveHotelIdentity,
  MATCH_STATUS,
  findNearbyHotels,
  createHotelbedsProvider,
  createHotelIntelligenceService,
  ISSUE_TYPES,
} from "../lib/hotel-intelligence/index.js";
import { MAP_CENSUS_FIELDS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { providerStatus } from "../lib/hotel-intelligence/providers/types.js";

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "hi-mcp-"));
const store = createLocalStore({ root: tmpRoot });
const idRegistry = createExternalIdRegistry(store);

function censusRec(id, fields) {
  return { id, fields };
}

const FIXTURES = [
  censusRec("recExact1", {
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
    [MAP_CENSUS_FIELDS.propertyIdentityKey]: "mx_cdmx_courtyard_airport",
    [MAP_CENSUS_FIELDS.website]: "https://www.marriott.com/mexcy",
    [MAP_CENSUS_FIELDS.phone]: "+52 55 1234 5678",
    [MAP_CENSUS_FIELDS.status]: "Census Only / Not Owner-Facing",
    [MAP_CENSUS_FIELDS.identityConfidence]: "High",
    [MAP_CENSUS_FIELDS.chainScale]: "Upscale",
  }),
  censusRec("recDowntown", {
    [MAP_CENSUS_FIELDS.propertyName]: "Courtyard Mexico City Downtown",
    [MAP_CENSUS_FIELDS.officialName]: "Courtyard Mexico City Downtown",
    [MAP_CENSUS_FIELDS.address]: "Calle Liverpool 45",
    [MAP_CENSUS_FIELDS.city]: "Mexico City",
    [MAP_CENSUS_FIELDS.country]: "Mexico",
    [MAP_CENSUS_FIELDS.latitude]: 19.4260,
    [MAP_CENSUS_FIELDS.longitude]: -99.1677,
    [MAP_CENSUS_FIELDS.roomCount]: 280,
    [MAP_CENSUS_FIELDS.brandName]: "Courtyard",
    [MAP_CENSUS_FIELDS.parentCompanyName]: "Marriott International",
    [MAP_CENSUS_FIELDS.propertyIdentityKey]: "mx_cdmx_courtyard_downtown",
  }),
  censusRec("recKimpton", {
    [MAP_CENSUS_FIELDS.propertyName]: "Kimpton Aluna Tulum",
    [MAP_CENSUS_FIELDS.officialName]: "Kimpton Aluna Tulum",
    [MAP_CENSUS_FIELDS.address]: "Av. Coba Sur",
    [MAP_CENSUS_FIELDS.city]: "Tulum",
    [MAP_CENSUS_FIELDS.country]: "Mexico",
    [MAP_CENSUS_FIELDS.latitude]: 20.2110,
    [MAP_CENSUS_FIELDS.longitude]: -87.4650,
    [MAP_CENSUS_FIELDS.roomCount]: 120,
    [MAP_CENSUS_FIELDS.brandName]: "Kimpton",
    [MAP_CENSUS_FIELDS.parentCompanyName]: "IHG",
    [MAP_CENSUS_FIELDS.hbxHotelCode]: "HBX222",
  }),
];

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`ok - ${name}`);
  } catch (err) {
    console.error(`FAIL - ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`ok - ${name}`);
  } catch (err) {
    console.error(`FAIL - ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

// --- Identity ---
test("dhl_ hotel id format", () => {
  const id = generateDealalityHotelId();
  assert.ok(isDealalityHotelId(id), id);
});

test("exact external-ID match via HBX code on census", () => {
  const r = resolveHotelIdentity(
    { name: "Anything", external_ids: { hotelbeds: "HBX111" } },
    FIXTURES,
    { idRegistry, store }
  );
  assert.equal(r.match_status, MATCH_STATUS.EXACT);
  assert.ok(r.hotel_id);
  assert.equal(r.review_required, false);
});

test("exact address + normalized name", () => {
  const r = resolveHotelIdentity(
    {
      name: "Courtyard Mexico City Airport",
      address: "Blvd Puerto Aereo 502",
      city: "Mexico City",
      country: "Mexico",
    },
    FIXTURES,
    { idRegistry, store }
  );
  assert.equal(r.match_status, MATCH_STATUS.EXACT);
});

test("same-brand hotels in same city remain distinct (airport vs downtown)", () => {
  const airport = resolveHotelIdentity(
    {
      name: "Courtyard Mexico City Airport",
      city: "Mexico City",
      country: "Mexico",
      latitude: 19.4361,
      longitude: -99.0719,
      brand: "Courtyard",
    },
    FIXTURES,
    { idRegistry, store }
  );
  const downtown = resolveHotelIdentity(
    {
      name: "Courtyard Mexico City Downtown",
      city: "Mexico City",
      country: "Mexico",
      latitude: 19.426,
      longitude: -99.1677,
      brand: "Courtyard",
    },
    FIXTURES,
    { idRegistry, store }
  );
  assert.notEqual(airport.hotel_id, downtown.hotel_id);
  assert.ok(
    [MATCH_STATUS.EXACT, MATCH_STATUS.STRONG, MATCH_STATUS.PROBABLE].includes(
      airport.match_status
    )
  );
});

test("rebrand / alternate name still strong or probable", () => {
  const r = resolveHotelIdentity(
    {
      name: "Kimpton Aluna",
      city: "Tulum",
      country: "Mexico",
      latitude: 20.211,
      longitude: -87.465,
      brand: "Kimpton",
    },
    FIXTURES,
    { idRegistry, store }
  );
  assert.ok(
    [MATCH_STATUS.STRONG, MATCH_STATUS.PROBABLE, MATCH_STATUS.EXACT].includes(
      r.match_status
    ),
    r.match_status
  );
});

test("ambiguous duplicate when two near-equal candidates", () => {
  const twins = [
    censusRec("recA", {
      [MAP_CENSUS_FIELDS.propertyName]: "Hotel Plaza Central",
      [MAP_CENSUS_FIELDS.officialName]: "Hotel Plaza Central",
      [MAP_CENSUS_FIELDS.city]: "Bogota",
      [MAP_CENSUS_FIELDS.country]: "Colombia",
      [MAP_CENSUS_FIELDS.latitude]: 4.71,
      [MAP_CENSUS_FIELDS.longitude]: -74.07,
    }),
    censusRec("recB", {
      [MAP_CENSUS_FIELDS.propertyName]: "Hotel Plaza Central",
      [MAP_CENSUS_FIELDS.officialName]: "Hotel Plaza Central",
      [MAP_CENSUS_FIELDS.city]: "Bogota",
      [MAP_CENSUS_FIELDS.country]: "Colombia",
      [MAP_CENSUS_FIELDS.latitude]: 4.7102,
      [MAP_CENSUS_FIELDS.longitude]: -74.0702,
    }),
  ];
  const r = resolveHotelIdentity(
    {
      name: "Hotel Plaza Central",
      city: "Bogota",
      country: "Colombia",
      latitude: 4.7101,
      longitude: -74.0701,
    },
    twins,
    { idRegistry: createExternalIdRegistry(createLocalStore({ root: path.join(tmpRoot, "amb") })), store }
  );
  assert.ok(
    r.match_status === MATCH_STATUS.AMBIGUOUS || r.review_required === true,
    JSON.stringify(r)
  );
});

test("new hotel staged with dhl_ id", () => {
  const r = resolveHotelIdentity(
    {
      name: "Completely Unknown Boutique ZYX",
      city: "Quito",
      country: "Ecuador",
    },
    FIXTURES,
    { idRegistry, store }
  );
  assert.equal(r.match_status, MATCH_STATUS.NEW);
  assert.ok(isDealalityHotelId(r.hotel_id));
});

// --- Evidence / confidence ---
test("one source evidence + confidence", () => {
  const ev = createEvidenceStore(store);
  const hotelId = generateDealalityHotelId();
  const row = ev.addEvidence({
    hotel_id: hotelId,
    field: "room_count",
    value: 184,
    source: "hotelbeds",
    source_record_id: "123456",
  });
  assert.ok(row.confidence >= 0.7);
  assert.equal(ev.listForHotel(hotelId).length, 1);
});

test("two agreeing sources", () => {
  const root = path.join(tmpRoot, "agree");
  const s = createLocalStore({ root });
  const ev = createEvidenceStore(s);
  const hotelId = generateDealalityHotelId();
  ev.addEvidence({ hotel_id: hotelId, field: "room_count", value: 184, source: "hotelbeds" });
  ev.addEvidence({ hotel_id: hotelId, field: "room_count", value: 184, source: "official_site" });
  const summary = ev.summarizeHotel(hotelId);
  assert.equal(summary.conflicts.length, 0);
  assert.equal(summary.fields[0].preferred.value, 184);
});

test("conflicting room counts preserved", () => {
  const root = path.join(tmpRoot, "conflict-rooms");
  const s = createLocalStore({ root });
  const ev = createEvidenceStore(s);
  const hotelId = generateDealalityHotelId();
  ev.addEvidence({ hotel_id: hotelId, field: "room_count", value: 184, source: "hotelbeds" });
  ev.addEvidence({ hotel_id: hotelId, field: "room_count", value: 180, source: "official_site" });
  const summary = ev.summarizeHotel(hotelId);
  assert.ok(summary.conflicts.length >= 1);
  assert.equal(summary.fields[0].preferred.value, 180); // official_site higher for rooms
});

test("conflicting brands preserved", () => {
  const pref = preferCanonicalValue([
    { field: "brand_name", value: "Courtyard", source: "hotelbeds" },
    { field: "brand_name", value: "Courtyard by Marriott", source: "brand_directory" },
  ]);
  assert.ok(pref.conflicts.length >= 1);
  assert.equal(pref.preferred.source, "brand_directory");
});

test("confidence tiers documented", () => {
  const c = scoreFieldConfidence("room_count", "official_site");
  assert.ok(c.confidence >= 0.95);
  assert.equal(c.auto_accept, true);
  const low = scoreFieldConfidence("room_count", "google_places");
  assert.ok(low.confidence < 0.5);
  assert.equal(low.auto_accept, false);
});

// --- Provider failure ---
test("Hotelbeds quota exhaustion is retryable provider status", () => {
  const p = createHotelbedsProvider({
    env: {},
    forceEnabled: true,
    cfg: {
      ok: true,
      apiKey: "test",
      apiSecret: "secret",
      contentBase: "https://example.test",
      apiKeyFingerprint: "test…",
    },
  });
  const status = p._classifyFetch({
    ok: false,
    status: 403,
    error_message: "Quota exceeded",
    error_code: "QUOTA_EXCEEDED",
    quota_exceeded: true,
  });
  assert.equal(status.status, "quota_exhausted");
  assert.equal(status.retryable, true);
});

test("Hotelbeds timeout classified", () => {
  const p = createHotelbedsProvider({ env: {}, forceEnabled: true, cfg: { ok: true } });
  const status = p._classifyFetch({
    ok: false,
    status: 0,
    error_code: "AbortError",
    error_message: "The operation was aborted",
  });
  assert.equal(status.status, "timeout");
  assert.equal(status.retryable, true);
});

test("malformed / empty provider result normalize", () => {
  const p = createHotelbedsProvider({ env: {} });
  const h = p.normalizeHotel({});
  assert.equal(h.provider, "hotelbeds");
  assert.equal(h.external_id, null);
});

await testAsync("zero provider results does not throw", async () => {
  const service = createHotelIntelligenceService({
    store,
    idRegistry,
    censusRecords: FIXTURES,
    env: { ENABLE_HBX_CONTENT_API: "0" },
  });
  const res = await service.hotelSearch({ name: "NoSuchHotelZZZ", city: "Nowhere" });
  assert.equal(res.ok, true);
  assert.ok(Array.isArray(res.hotels));
});

// --- Census staging ---
await testAsync("existing record enriched (staged evidence only)", async () => {
  const service = createHotelIntelligenceService({
    store: createLocalStore({ root: path.join(tmpRoot, "enrich") }),
    censusRecords: FIXTURES,
    env: { ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0" },
  });
  const resolved = await service.hotelResolve({
    name: "Courtyard Mexico City Airport",
    city: "Mexico City",
    country: "Mexico",
    external_ids: { hotelbeds: "HBX111" },
  });
  assert.ok(resolved.hotel_id);
  const ev = service.evidence.addEvidence({
    hotel_id: resolved.hotel_id,
    field: "phone",
    value: "+52 55 1234 5678",
    source: "dealality_census",
  });
  assert.ok(ev.confidence > 0);
  assert.equal(service.airtableWritesEnabled(), false);
});

await testAsync("new record staged via ingest", async () => {
  const service = createHotelIntelligenceService({
    store: createLocalStore({ root: path.join(tmpRoot, "ingest") }),
    censusRecords: FIXTURES,
    env: { ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0" },
  });
  const res = await service.hotelCensusIngest({
    enrich: false,
    records: [
      {
        name: "Brand New Jungle Lodge ABC",
        city: "Puerto Viejo",
        country: "Costa Rica",
      },
    ],
  });
  assert.equal(res.airtable_writes_made, 0);
  assert.equal(res.results[0].status === "enriched" || res.results[0].status === "completed" || res.results[0].status === "review_required", true);
  assert.ok(isDealalityHotelId(res.results[0].hotel_id));
});

await testAsync("suspected duplicate held for review", async () => {
  const service = createHotelIntelligenceService({
    store: createLocalStore({ root: path.join(tmpRoot, "dup") }),
    censusRecords: [
      censusRec("recD1", {
        [MAP_CENSUS_FIELDS.propertyName]: "Hotel Ambiguo",
        [MAP_CENSUS_FIELDS.officialName]: "Hotel Ambiguo",
        [MAP_CENSUS_FIELDS.city]: "Lima",
        [MAP_CENSUS_FIELDS.country]: "Peru",
        [MAP_CENSUS_FIELDS.latitude]: -12.05,
        [MAP_CENSUS_FIELDS.longitude]: -77.04,
      }),
      censusRec("recD2", {
        [MAP_CENSUS_FIELDS.propertyName]: "Hotel Ambiguo",
        [MAP_CENSUS_FIELDS.officialName]: "Hotel Ambiguo",
        [MAP_CENSUS_FIELDS.city]: "Lima",
        [MAP_CENSUS_FIELDS.country]: "Peru",
        [MAP_CENSUS_FIELDS.latitude]: -12.0501,
        [MAP_CENSUS_FIELDS.longitude]: -77.0401,
      }),
    ],
    env: {},
  });
  const res = await service.hotelCensusIngest({
    enrich: false,
    records: [
      {
        name: "Hotel Ambiguo",
        city: "Lima",
        country: "Peru",
        latitude: -12.05005,
        longitude: -77.04005,
      },
    ],
  });
  const rq = await service.hotelReviewQueue({ status: "open" });
  assert.ok(
    res.results[0].review_required ||
      res.results[0].status === "review_required" ||
      rq.items.length > 0
  );
});

await testAsync("failed provider does not corrupt staged record", async () => {
  const service = createHotelIntelligenceService({
    store: createLocalStore({ root: path.join(tmpRoot, "nocrash") }),
    censusRecords: FIXTURES,
    env: { ENABLE_HBX_CONTENT_API: "0", HOTEL_INTELLIGENCE_HOTELBEDS: "0" },
  });
  const resolved = await service.hotelResolve({
    external_ids: { hotelbeds: "HBX111" },
    name: "Courtyard Mexico City Airport",
    city: "Mexico City",
    country: "Mexico",
  });
  const enrich = await service.hotelEnrich({
    hotel_id: resolved.hotel_id,
    providers: ["hotelbeds"],
  });
  assert.equal(enrich.ok, true);
  const got = await service.hotelGet({ hotel_id: resolved.hotel_id });
  assert.equal(got.ok, true);
});

// --- Geography ---
test("nearby search + coordinate tolerance", () => {
  const near = findNearbyHotels(
    FIXTURES,
    { latitude: 19.436, longitude: -99.072, radius_km: 2, limit: 10 },
    { idRegistry, store }
  );
  assert.equal(near.ok, true);
  assert.ok(near.hotels.some((h) => h.hotel_name.includes("Airport")));
  assert.ok(!near.hotels.some((h) => (h.hotel_name || "").includes("Tulum")));
});

test("review queue issue types", () => {
  const rq = createReviewQueue(store);
  const item = rq.enqueue({
    hotel_id: generateDealalityHotelId(),
    issue_type: ISSUE_TYPES.ROOM_COUNT_CONFLICT,
    current_value: 180,
    candidate_value: 184,
    sources: ["official_site", "hotelbeds"],
  });
  assert.equal(item.issue_type, "room_count_conflict");
  assert.ok(rq.list({ status: "open" }).length >= 1);
});

test("providerStatus helper", () => {
  const s = providerStatus("hotelbeds", "quota_exhausted", { retryable: true });
  assert.equal(s.provider, "hotelbeds");
});

console.log(`\n${passed} tests passed`);
if (process.exitCode) {
  console.error("Some tests failed");
  process.exit(1);
}
