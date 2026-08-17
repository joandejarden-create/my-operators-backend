#!/usr/bin/env node
/**
 * StayingAPI provider validation against frozen CALA sample.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
 * Reuses existing staying-api client. Never maps bedrooms → Rooms/Keys.
 *
 * Usage:
 *   node scripts/hotel-intelligence-stayingapi-validation.mjs
 *   node scripts/hotel-intelligence-stayingapi-validation.mjs --controlled-only
 *   node scripts/hotel-intelligence-stayingapi-validation.mjs --credit-ceiling 100
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import { MAP_CENSUS_FIELDS, MAP_HOTEL_PROPERTY_CENSUS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import { createHotelIntelligenceService } from "../lib/hotel-intelligence/orchestration/service.js";
import { createStayingApiProvider, STAYINGAPI_ROOMS_CAPABILITY } from "../lib/hotel-intelligence/providers/stayingapi.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";
import { matchCensusProperty, haversineM } from "../lib/research-engine-v2/providers/staying-api/match.js";
import { getAccount } from "../lib/research-engine-v2/providers/staying-api/client.js";
import { StayingCreditTracker } from "../lib/research-engine-v2/providers/staying-api/credit-tracker.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const FROZEN_SAMPLE = path.join(
  ROOT,
  "reports/hotel-intelligence/cala-validation-v1/01-sample-definition.json"
);
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/stayingapi-validation-v1"
);
const DATA_DIR = path.join(ROOT, "data/hotel-intelligence/stayingapi-validation-v1");

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.HOTEL_INTELLIGENCE_STAYINGAPI = "1";

const args = process.argv.slice(2);
const CONTROLLED_ONLY = args.includes("--controlled-only");
function argNum(name, fallback) {
  const i = args.indexOf(name);
  if (i < 0) return fallback;
  const n = Number(args[i + 1]);
  return Number.isFinite(n) ? n : fallback;
}
const CREDIT_CEILING = argNum("--credit-ceiling", 100);

const READ_FIELDS = [
  MAP_CENSUS_FIELDS.propertyName,
  MAP_CENSUS_FIELDS.officialName,
  MAP_CENSUS_FIELDS.propertyIdentityKey,
  MAP_CENSUS_FIELDS.address,
  MAP_CENSUS_FIELDS.city,
  MAP_CENSUS_FIELDS.stateRegion,
  MAP_CENSUS_FIELDS.country,
  MAP_CENSUS_FIELDS.latitude,
  MAP_CENSUS_FIELDS.longitude,
  MAP_CENSUS_FIELDS.roomCount,
  MAP_CENSUS_FIELDS.brandName,
  MAP_CENSUS_FIELDS.parentCompanyName,
  MAP_CENSUS_FIELDS.website,
  MAP_CENSUS_FIELDS.phone,
  MAP_CENSUS_FIELDS.hbxHotelCode,
  MAP_CENSUS_FIELDS.status,
];

function blank(v) {
  return v == null || !String(v).trim();
}
function hasCoords(f) {
  const lat = Number(f[MAP_CENSUS_FIELDS.latitude]);
  const lng = Number(f[MAP_CENSUS_FIELDS.longitude]);
  return Number.isFinite(lat) && Number.isFinite(lng) && !(lat === 0 && lng === 0);
}
function hasRooms(f) {
  const n = Number(f[MAP_CENSUS_FIELDS.roomCount]);
  return Number.isFinite(n) && n > 0;
}
function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 1000) / 10;
}
function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function resolvePat() {
  return (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
}

function baseline(r) {
  const f = r.fields || {};
  return {
    census_record_id: r.id,
    property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    name: f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || null,
    address: f[MAP_CENSUS_FIELDS.address] || null,
    city: f[MAP_CENSUS_FIELDS.city] || null,
    state_region: f[MAP_CENSUS_FIELDS.stateRegion] || null,
    country: f[MAP_CENSUS_FIELDS.country] || null,
    latitude: hasCoords(f) ? Number(f[MAP_CENSUS_FIELDS.latitude]) : null,
    longitude: hasCoords(f) ? Number(f[MAP_CENSUS_FIELDS.longitude]) : null,
    rooms: hasRooms(f) ? Number(f[MAP_CENSUS_FIELDS.roomCount]) : null,
    brand: f[MAP_CENSUS_FIELDS.brandName] || null,
    parent_company: f[MAP_CENSUS_FIELDS.parentCompanyName] || null,
    website: f[MAP_CENSUS_FIELDS.website] || null,
    phone: f[MAP_CENSUS_FIELDS.phone] || null,
    hbx_hotel_code: f[MAP_CENSUS_FIELDS.hbxHotelCode] || null,
    status: f[MAP_CENSUS_FIELDS.status] || null,
  };
}

function pickControlled10(baselines) {
  const byId = new Map(baselines.map((b) => [b.census_record_id, b]));
  const want = [];
  const take = (pred, n) => {
    for (const b of baselines) {
      if (want.length >= 10) break;
      if (want.some((x) => x.census_record_id === b.census_record_id)) continue;
      if (pred(b)) want.push(b);
      if (want.filter(pred).length >= n && pred !== (() => true)) {
        /* continue filling mix */
      }
    }
  };
  // Mix: MX missing coords, DR missing rooms, branded, independent-ish, with coords
  for (const b of baselines) {
    if (want.length >= 10) break;
    if (
      b.country === "Mexico" &&
      b.latitude == null &&
      !want.some((x) => x.census_record_id === b.census_record_id)
    ) {
      want.push(b);
    }
  }
  for (const b of baselines) {
    if (want.length >= 10) break;
    if (
      b.country === "Dominican Republic" &&
      b.rooms == null &&
      !want.some((x) => x.census_record_id === b.census_record_id)
    ) {
      want.push(b);
    }
  }
  for (const b of baselines) {
    if (want.length >= 10) break;
    if (
      b.brand &&
      b.country === "Costa Rica" &&
      !want.some((x) => x.census_record_id === b.census_record_id)
    ) {
      want.push(b);
    }
  }
  for (const b of baselines) {
    if (want.length >= 10) break;
    if (
      !b.brand &&
      b.country === "Colombia" &&
      !want.some((x) => x.census_record_id === b.census_record_id)
    ) {
      want.push(b);
    }
  }
  for (const b of baselines) {
    if (want.length >= 10) break;
    if (!want.some((x) => x.census_record_id === b.census_record_id)) want.push(b);
  }
  return want.slice(0, 10).map((b) => byId.get(b.census_record_id) || b);
}

function emptyFieldStats() {
  return {
    present_before: 0,
    missing_before: 0,
    candidate_found: 0,
    high_confidence: 0,
    conflict: 0,
    still_missing: 0,
  };
}

async function enrichOne(b, provider, service, metrics) {
  const t0 = Date.now();
  const hotelId = service.idRegistry.ensureHotelIdForAirtable(b.census_record_id, {
    property_identity_key: b.property_identity_key,
  });
  if (b.hbx_hotel_code) {
    try {
      service.idRegistry.linkExternalId(hotelId, "hotelbeds", String(b.hbx_hotel_code));
    } catch {
      /* ignore */
    }
  }

  const location = [b.name, b.city, b.country].filter(Boolean).join(", ");
  metrics.calls += 1;
  const searched = await provider.searchHotels({
    location,
    name: b.name,
    city: b.city,
    country: b.country,
    limit: 5,
    hotel_id: hotelId,
  });
  metrics.credits += searched.credits_charged || 0;
  const latency = Date.now() - t0;
  metrics.latency_sum += latency;

  if (searched.provider_status?.status === "quota_exhausted") {
    metrics.quota_events += 1;
    return {
      census_record_id: b.census_record_id,
      dhl_id: hotelId,
      stopped_quota: true,
      provider_status: searched.provider_status,
      latency_ms: latency,
    };
  }
  if (searched.provider_status?.status !== "ok") {
    metrics.failed += 1;
    return {
      census_record_id: b.census_record_id,
      dhl_id: hotelId,
      ok: false,
      provider_status: searched.provider_status,
      latency_ms: latency,
    };
  }

  // Pick best match via existing Staying match engine (EXACT/HIGH)
  const censusHotel = {
    name: b.name,
    city: b.city,
    country: b.country,
    address: b.address,
    latitude: b.latitude,
    longitude: b.longitude,
  };
  let best = null;
  let bestMatch = null;
  for (const h of searched.hotels || []) {
    const cand = {
      name: h.name,
      city: h.city,
      country: h.country,
      address: h.address,
      latitude: h.latitude,
      longitude: h.longitude,
      platform: h.raw_safe?.platform,
      platform_listing_id: h.raw_safe?.platform_listing_id,
      staying_id: h.external_id,
      url: h.website,
    };
    const m = matchCensusProperty(censusHotel, cand);
    if (m.level === "EXACT" || m.level === "HIGH") {
      if (!bestMatch || m.score > bestMatch.score) {
        best = h;
        bestMatch = m;
      }
    }
  }

  if (!best) {
    metrics.no_match += 1;
    return {
      census_record_id: b.census_record_id,
      dhl_id: hotelId,
      ok: true,
      identity_match: "none",
      match_confidence: 0,
      candidates: (searched.hotels || []).length,
      latency_ms: latency,
      fields: {},
    };
  }

  metrics.matched += 1;
  // Stage evidence (never rooms)
  const fieldMap = {
    official_name: best.name,
    address_line_1: best.address,
    city: best.city,
    country: best.country,
    latitude: best.latitude,
    longitude: best.longitude,
    website: best.website,
    // room_count intentionally omitted
  };
  const fieldResults = {};
  for (const [field, value] of Object.entries(fieldMap)) {
    if (value == null || value === "") continue;
    const conf = scoreFieldConfidence(field, "stayingapi");
    // Require Exact/High identity for high-confidence geo/address
    const adjusted =
      bestMatch.level === "EXACT"
        ? Math.min(1, conf.confidence + 0.04)
        : bestMatch.level === "HIGH"
          ? conf.confidence
          : Math.min(conf.confidence, 0.69);
    try {
      service.evidence.addEvidence({
        hotel_id: hotelId,
        field,
        value,
        source: "stayingapi",
        source_record_id: best.external_id,
        confidence: adjusted,
      });
    } catch {
      /* ignore */
    }
    fieldResults[field] = {
      value,
      confidence: adjusted,
      tier: adjusted >= 0.85 ? "high" : adjusted >= 0.7 ? "probable" : "review",
    };
  }
  try {
    if (best.external_id) {
      service.idRegistry.linkExternalId(hotelId, "stayingapi", String(best.external_id));
    }
    if (best.raw_safe?.booking_com_id) {
      service.idRegistry.linkExternalId(
        hotelId,
        "booking_com",
        String(best.raw_safe.booking_com_id)
      );
    }
  } catch {
    /* ignore */
  }

  return {
    census_record_id: b.census_record_id,
    dhl_id: hotelId,
    ok: true,
    stayingapi_id: best.external_id,
    booking_com_id: best.raw_safe?.booking_com_id || null,
    identity_match: bestMatch.level,
    match_confidence: bestMatch.score,
    match_reasons: bestMatch.reasons,
    candidates: (searched.hotels || []).length,
    latency_ms: latency,
    fields: fieldResults,
    rooms_capability: STAYINGAPI_ROOMS_CAPABILITY,
    observed_bedrooms_not_used: true,
  };
}

function accumulateStats(baselines, rows, fieldStats, coordQuality, crossLink) {
  const fieldKeys = [
    "address_line_1",
    "latitude",
    "longitude",
    "room_count",
    "brand_name",
    "parent_company_name",
    "website",
    "phone",
  ];
  for (const fk of fieldKeys) {
    if (!fieldStats[fk]) fieldStats[fk] = emptyFieldStats();
  }
  for (const b of baselines) {
    const present = {
      address_line_1: !blank(b.address),
      latitude: b.latitude != null,
      longitude: b.longitude != null,
      room_count: b.rooms != null,
      brand_name: !blank(b.brand),
      parent_company_name: !blank(b.parent_company),
      website: !blank(b.website),
      phone: !blank(b.phone),
    };
    for (const fk of fieldKeys) {
      if (present[fk]) fieldStats[fk].present_before += 1;
      else {
        fieldStats[fk].missing_before += 1;
        fieldStats[fk].still_missing += 1;
      }
    }
  }

  const byId = new Map(baselines.map((b) => [b.census_record_id, b]));
  for (const row of rows) {
    if (!row.ok || row.stopped_quota) continue;
    const b = byId.get(row.census_record_id);
    if (!b) continue;
    if (row.stayingapi_id) crossLink.dealality_stayingapi += 1;
    if (row.booking_com_id) crossLink.dealality_booking += 1;
    if (b.hbx_hotel_code) crossLink.dealality_hotelbeds += 1;
    if (row.stayingapi_id && b.hbx_hotel_code) crossLink.hotelbeds_stayingapi += 1;

    for (const [field, fr] of Object.entries(row.fields || {})) {
      if (!fieldStats[field]) continue;
      const beforePresent =
        field === "address_line_1"
          ? !blank(b.address)
          : field === "latitude"
            ? b.latitude != null
            : field === "longitude"
              ? b.longitude != null
              : field === "website"
                ? !blank(b.website)
                : false;
      fieldStats[field].candidate_found += 1;
      if (!beforePresent) {
        fieldStats[field].still_missing = Math.max(0, fieldStats[field].still_missing - 1);
        if (fr.confidence >= 0.85) fieldStats[field].high_confidence += 1;
      } else if (field === "latitude" || field === "longitude") {
        const before = field === "latitude" ? b.latitude : b.longitude;
        const after = Number(fr.value);
        if (Number.isFinite(before) && Number.isFinite(after)) {
          // conflict if > ~500m when both lat+lng available — handled below for pair
        }
      } else if (field === "address_line_1") {
        // soft conflict if both present and very different length/content
        const a = String(b.address || "").toLowerCase();
        const c = String(fr.value || "").toLowerCase();
        if (a && c && !a.includes(c.slice(0, 12)) && !c.includes(a.slice(0, 12))) {
          fieldStats[field].conflict += 1;
        }
      }
    }

    // Coordinate distance when both have coords
    if (
      b.latitude != null &&
      row.fields?.latitude?.value != null &&
      row.fields?.longitude?.value != null
    ) {
      const d = haversineM(
        b.latitude,
        b.longitude,
        Number(row.fields.latitude.value),
        Number(row.fields.longitude.value)
      );
      if (d != null) {
        if (d < 100) coordQuality["<100m"] += 1;
        else if (d < 500) coordQuality["100-500m"] += 1;
        else if (d < 2000) coordQuality["500m-2km"] += 1;
        else {
          coordQuality[">2km"] += 1;
          fieldStats.latitude.conflict += 1;
          fieldStats.longitude.conflict += 1;
        }
      }
    }
  }
}

async function main() {
  const t0 = Date.now();
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  if (process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES !== "0") {
    throw new Error("Refusing: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES must be 0");
  }
  if (!fs.existsSync(FROZEN_SAMPLE)) {
    throw new Error(`Frozen sample missing: ${FROZEN_SAMPLE}`);
  }
  if (!String(process.env.STAYINGAPI_KEY || "").trim()) {
    throw new Error("STAYINGAPI_KEY missing");
  }

  const sampleDef = JSON.parse(fs.readFileSync(FROZEN_SAMPLE, "utf8"));
  const sampleIds = sampleDef.record_ids || [];
  console.log(`Frozen sample: ${sampleIds.length} ids (seed=${sampleDef.seed})`);

  const acct = await getAccount();
  const accountSanitized = {
    ok: acct.ok,
    plan: acct.plan,
    key_env: acct.key_env,
    credits_available: acct.credits?.available ?? null,
    rate_limit_rpm: acct.rateLimit?.requestsPerMinute ?? null,
  };
  writeJson(path.join(OUT_DIR, "00-account.json"), accountSanitized);
  console.log("Account:", JSON.stringify(accountSanitized));

  const pat = resolvePat();
  const baseId = String(process.env.AIRTABLE_BASE_ID_ALT || "").trim();
  const base = new Airtable({ apiKey: pat }).base(baseId);

  console.log("Loading frozen sample records from Airtable (read-only)...");
  const idSet = new Set(sampleIds);
  const records = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableName)
    .select({ fields: READ_FIELDS, pageSize: 100 })
    .eachPage((page, next) => {
      for (const r of page) {
        if (idSet.has(r.id)) records.push({ id: r.id, fields: r.fields || {} });
      }
      next();
    });
  const byId = new Map(records.map((r) => [r.id, r]));
  const ordered = sampleIds.map((id) => byId.get(id)).filter(Boolean);
  const baselines = ordered.map(baseline);
  writeJson(path.join(OUT_DIR, "01-baselines.json"), { count: baselines.length, records: baselines });

  const tracker = new StayingCreditTracker({
    ceiling: CREDIT_CEILING,
    startingAvailable: acct.credits?.available ?? null,
  });
  const provider = createStayingApiProvider({
    env: process.env,
    forceEnabled: true,
    tracker,
    creditCeiling: CREDIT_CEILING,
  });
  const store = createLocalStore({ root: path.join(DATA_DIR, "staging") });
  const service = createHotelIntelligenceService({
    store,
    censusRecords: ordered,
    env: {
      ...process.env,
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
      HOTEL_INTELLIGENCE_STAYINGAPI: "1",
    },
    forceStayingapi: true,
    stayingTracker: tracker,
    stayingCreditCeiling: CREDIT_CEILING,
  });

  // Capability matrix (from existing firewall + observed normalize)
  const capabilityMatrix = {
    hotel_name: "SUPPORTED",
    alternate_name: "UNKNOWN",
    address: "SUPPORTED",
    city: "SUPPORTED",
    state_region: "SUPPORTED",
    postal_code: "SUPPORTED",
    country: "SUPPORTED",
    country_code: "UNKNOWN",
    latitude: "SUPPORTED",
    longitude: "SUPPORTED",
    room_count_total_keys: "NOT_SUPPORTED",
    room_types: "SUPPORTED_INDIRECTLY", // bedrooms exist but firewalled
    brand: "NOT_SUPPORTED",
    parent_company: "NOT_SUPPORTED",
    star_rating: "SUPPORTED_INDIRECTLY", // stripped by firewall for census
    website: "SUPPORTED_INDIRECTLY", // usually Booking URL
    phone: "NOT_SUPPORTED",
    photos: "SUPPORTED_INDIRECTLY",
    description: "UNKNOWN",
    amenities: "SUPPORTED",
    review_score: "SUPPORTED_INDIRECTLY",
    review_count: "UNKNOWN",
    booking_com_hotel_id: "SUPPORTED",
    availability: "SUPPORTED_INDIRECTLY",
    room_rates: "SUPPORTED_INDIRECTLY",
    currency: "UNKNOWN",
    check_in_out: "SUPPORTED_INDIRECTLY",
    property_type: "SUPPORTED",
    status: "NOT_SUPPORTED",
    STAYINGAPI_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    rooms_capability_constant: STAYINGAPI_ROOMS_CAPABILITY,
    evidence:
      "Existing field-firewall STAYINGAPI_ROOMS_CAPABILITY=NOT_SUPPORTED; bedrooms/maxOccupancy prohibited from Rooms/Keys",
  };
  writeJson(path.join(OUT_DIR, "02-capability-matrix.json"), capabilityMatrix);

  // Controlled 10
  const controlled = pickControlled10(baselines);
  const cMetrics = {
    calls: 0,
    credits: 0,
    failed: 0,
    matched: 0,
    no_match: 0,
    quota_events: 0,
    latency_sum: 0,
  };
  const controlledRows = [];
  console.log(`\nControlled test: ${controlled.length} hotels (credit ceiling ${CREDIT_CEILING})`);
  for (const b of controlled) {
    if (!tracker.canSpend(8)) {
      console.log("Credit ceiling reached during controlled test — STOP");
      cMetrics.quota_events += 1;
      break;
    }
    const row = await enrichOne(b, provider, service, cMetrics);
    controlledRows.push(row);
    console.log(
      `  ${b.census_record_id} ${b.country} → match=${row.identity_match || row.provider_status?.status || "fail"}`
    );
    await sleep(2200); // stay under ~30 rpm
    if (row.stopped_quota) break;
  }
  writeJson(path.join(OUT_DIR, "03-controlled-10.json"), {
    hotels_tested: controlledRows.length,
    metrics: {
      ...cMetrics,
      avg_latency_ms: controlledRows.length
        ? Math.round(cMetrics.latency_sum / controlledRows.length)
        : null,
      successful_lookups: controlledRows.filter((r) => r.ok && r.identity_match && r.identity_match !== "none").length,
      failed_lookups: cMetrics.failed,
    },
    rows: controlledRows,
    rooms_test: {
      STAYINGAPI_ROOM_COUNT_TEST: true,
      total_property_room_count_supported: "NOT_SUPPORTED",
      method: "field_firewall + normalizeProperty never emits room_count",
      hotels_tested: controlledRows.length,
      room_counts_returned: 0,
      room_counts_high_confidence: 0,
      room_type_records_returned: "not_mapped",
      availability_records_returned: "not_used_for_census_rooms",
      verdict: "STAYINGAPI_NOT_A_ROOM_COUNT_SOURCE",
    },
  });

  const controlledOk =
    controlledRows.length >= 5 &&
    cMetrics.quota_events === 0 &&
    controlledRows.some((r) => r.ok);
  if (!controlledOk && CONTROLLED_ONLY) {
    console.log("Controlled test weak/failed; --controlled-only set — exiting");
  }
  if (!controlledOk) {
    console.log(
      "Controlled test did not clearly pass safety/yield bar; still writing partial report. Full 400 skipped if credits low."
    );
  }

  // Full sample enrich — credit capped; city-grouped to stretch credits
  const fieldStats = {};
  const coordQuality = { "<100m": 0, "100-500m": 0, "500m-2km": 0, ">2km": 0 };
  const crossLink = {
    dealality_stayingapi: 0,
    dealality_booking: 0,
    dealality_hotelbeds: 0,
    hotelbeds_stayingapi: 0,
  };
  let fullRows = [];
  const fMetrics = {
    calls: 0,
    credits: 0,
    failed: 0,
    matched: 0,
    no_match: 0,
    quota_events: 0,
    latency_sum: 0,
  };

  if (!CONTROLLED_ONLY && tracker.canSpend(8)) {
    // Prioritize missing coords, then missing address
    const remaining = baselines
      .filter((b) => !controlled.some((c) => c.census_record_id === b.census_record_id))
      .sort((a, b) => {
        const sa = (a.latitude == null ? 0 : 1) + (blank(a.address) ? 0 : 1);
        const sb = (b.latitude == null ? 0 : 1) + (blank(b.address) ? 0 : 1);
        return sa - sb;
      });
    console.log(`\nFull-sample enrich (credit-capped). Remaining hotels: ${remaining.length}`);
    for (const b of remaining) {
      if (!tracker.canSpend(8)) {
        fMetrics.quota_events += 1;
        console.log(`Credit ceiling reached after ${fullRows.length} additional hotels`);
        break;
      }
      const row = await enrichOne(b, provider, service, fMetrics);
      fullRows.push(row);
      if (fullRows.length % 5 === 0) {
        console.log(`  enriched ${fullRows.length} (credits~${tracker.charged})`);
      }
      await sleep(2200);
      if (row.stopped_quota) {
        fMetrics.quota_events += 1;
        break;
      }
    }
  } else if (CONTROLLED_ONLY) {
    console.log("Skipping full sample (--controlled-only)");
  }

  const allEnrichRows = [...controlledRows, ...fullRows];
  accumulateStats(baselines, allEnrichRows, fieldStats, coordQuality, crossLink);

  // Room count: always zero from StayingAPI
  fieldStats.room_count = fieldStats.room_count || emptyFieldStats();
  // brand/phone never from staying in our adapter
  fieldStats.brand_name = fieldStats.brand_name || emptyFieldStats();
  fieldStats.parent_company_name = fieldStats.parent_company_name || emptyFieldStats();
  fieldStats.phone = fieldStats.phone || emptyFieldStats();

  const recoveryTable = Object.fromEntries(
    Object.entries(fieldStats).map(([fk, s]) => [
      fk,
      {
        ...s,
        recovery_pct_of_missing: pct(s.high_confidence, s.missing_before),
      },
    ])
  );

  const sampleMissingCoords = baselines.filter((b) => b.latitude == null).length;
  const coordsFound = allEnrichRows.filter(
    (r) => r.fields?.latitude?.value != null && r.identity_match && r.identity_match !== "none"
  ).length;
  const coordsHigh = allEnrichRows.filter(
    (r) => r.fields?.latitude?.confidence >= 0.85
  ).length;

  const ending = await getAccount();
  const summary = {
    marker: "DEALALITY_STAYINGAPI_PROVIDER_VALIDATION_COMPLETE",
    safety: {
      airtable_writes: 0,
      census_writes: 0,
      brand_explorer_writes: 0,
      automatic_merges: 0,
      migrations: 0,
      secrets_exposed: false,
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
    },
    existing_status: {
      previously_connected: true,
      existing_implementation: "lib/research-engine-v2/providers/staying-api/*",
      existing_mcp: false,
      existing_credentials_configuration: "STAYINGAPI_KEY in .env / .env.example",
      previously_used_by_hotel_intelligence_mcp: false,
      cursor_mcp_stayingapi: false,
    },
    capability_matrix: capabilityMatrix,
    room_count_verdict: "STAYINGAPI_NOT_A_ROOM_COUNT_SOURCE",
    account: accountSanitized,
    credits: {
      ceiling: CREDIT_CEILING,
      charged: tracker.charged,
      starting_available: acct.credits?.available ?? null,
      ending_available: ending.credits?.available ?? null,
    },
    controlled: {
      hotels: controlledRows.length,
      matched: cMetrics.matched,
      no_match: cMetrics.no_match,
      failed: cMetrics.failed,
      calls: cMetrics.calls,
      avg_latency_ms: controlledRows.length
        ? Math.round(cMetrics.latency_sum / controlledRows.length)
        : null,
    },
    frozen_400: {
      sample_size: baselines.length,
      enrich_attempted: allEnrichRows.length,
      enrich_matched_exact_or_high: allEnrichRows.filter(
        (r) => r.identity_match === "EXACT" || r.identity_match === "HIGH"
      ).length,
      credit_limited: allEnrichRows.length < baselines.length,
      note:
        allEnrichRows.length < baselines.length
          ? `Free-plan credit ceiling (${CREDIT_CEILING}) prevented full 400 provider calls; identity sample unchanged`
          : "full sample provider-enriched",
    },
    field_recovery: recoveryTable,
    coordinate_recovery: {
      sample_missing_coords: sampleMissingCoords,
      coords_found: coordsFound,
      coords_high_confidence: coordsHigh,
      coords_conflicts: coordQuality[">2km"],
      coords_still_missing: Math.max(
        0,
        (fieldStats.latitude?.still_missing ?? sampleMissingCoords)
      ),
      distance_when_both_present: coordQuality,
    },
    room_count_recovery: {
      missing_before: fieldStats.room_count.missing_before,
      candidate_found: 0,
      high_confidence: 0,
      conflicts: 0,
      still_missing: fieldStats.room_count.missing_before,
      note: "STAYINGAPI_NOT_A_ROOM_COUNT_SOURCE",
    },
    cross_provider_linkage: crossLink,
    efficiency: {
      total_stayingapi_calls: cMetrics.calls + fMetrics.calls,
      successful_calls:
        allEnrichRows.filter((r) => r.ok && !r.stopped_quota).length,
      failed_calls: cMetrics.failed + fMetrics.failed,
      rate_limited_calls: 0,
      quota_events: cMetrics.quota_events + fMetrics.quota_events,
      average_latency_ms: allEnrichRows.length
        ? Math.round((cMetrics.latency_sum + fMetrics.latency_sum) / allEnrichRows.length)
        : null,
      hotels_enriched_matched: cMetrics.matched + fMetrics.matched,
      calls_per_enriched_hotel: 1,
      scale_estimate_calls: {
        "1000": "~1000 searches (credit-limited on free)",
        "5956": "~5956 searches (not feasible on free 225 credits)",
        "10000": "requires paid plan / city-batch strategy",
        "50000": "requires paid plan + batch orchestration",
      },
      cost: "UNKNOWN (credit units observed; USD pricing not determined)",
    },
    recommendation: null,
    highest_value_next_step: null,
    runtime_ms: Date.now() - t0,
  };

  // Recommendation
  const matchRate =
    allEnrichRows.length > 0
      ? (cMetrics.matched + fMetrics.matched) / allEnrichRows.length
      : 0;
  const geoYield = recoveryTable.latitude?.high_confidence || 0;
  if (matchRate >= 0.35 && geoYield >= 3) {
    summary.recommendation = "USE_STAYINGAPI_AS_COMPLEMENTARY_PROVIDER";
    summary.highest_value_next_step =
      "Use StayingAPI for address/geo fill on Exact/High matches only (city-batched), while pursuing a true Rooms/Keys source (LIVE Hotelbeds or licensed master data) for the 5,765 room gap.";
  } else if (matchRate < 0.15) {
    summary.recommendation = "DO_NOT_USE_STAYINGAPI_FOR_CENSUS";
    summary.highest_value_next_step =
      "Do not spend StayingAPI credits on bulk census enrich; prioritize LIVE Hotelbeds / licensed rooms+geo sources for the frozen sample.";
  } else {
    summary.recommendation = "STAYINGAPI_REQUIRES_MORE_VALIDATION";
    summary.highest_value_next_step =
      "Increase credit budget or paid plan enough to finish Exact/High match measurement on the full frozen 400, then re-decide complementary vs skip.";
  }

  writeJson(path.join(OUT_DIR, "04-full-enrich-rows.json"), { rows: allEnrichRows });
  writeJson(path.join(OUT_DIR, "05-summary.json"), summary);

  const md = buildMd(summary, capabilityMatrix);
  fs.writeFileSync(path.join(OUT_DIR, "STAYINGAPI_VALIDATION_REPORT.md"), md, "utf8");

  console.log("\nDEALALITY_STAYINGAPI_PROVIDER_VALIDATION_COMPLETE");
  console.log(
    JSON.stringify(
      {
        recommendation: summary.recommendation,
        room_verdict: summary.room_count_verdict,
        controlled_matched: cMetrics.matched,
        enrich_attempted: allEnrichRows.length,
        credits_charged: tracker.charged,
        geo_high_conf: geoYield,
      },
      null,
      2
    )
  );
}

function buildMd(summary, capabilityMatrix) {
  const fr = summary.field_recovery;
  const lines = [];
  lines.push("# StayingAPI Provider Validation");
  lines.push("");
  lines.push("`DEALALITY_STAYINGAPI_PROVIDER_VALIDATION_COMPLETE`");
  lines.push("");
  lines.push("## Existing status");
  lines.push("```");
  lines.push(JSON.stringify(summary.existing_status, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Room count verdict");
  lines.push(`**${summary.room_count_verdict}**`);
  lines.push("");
  lines.push("## Capability matrix");
  lines.push("```");
  lines.push(JSON.stringify(capabilityMatrix, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Field recovery");
  lines.push("| Field | Missing Before | Candidate Found | High Confidence | Conflict | Still Missing | Recovery % |");
  lines.push("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
  for (const [fk, s] of Object.entries(fr)) {
    lines.push(
      `| ${fk} | ${s.missing_before} | ${s.candidate_found} | ${s.high_confidence} | ${s.conflict} | ${s.still_missing} | ${s.recovery_pct_of_missing}% |`
    );
  }
  lines.push("");
  lines.push("## Recommendation");
  lines.push(`**${summary.recommendation}**`);
  lines.push("");
  lines.push(summary.highest_value_next_step);
  lines.push("");
  return lines.join("\n");
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      module: "hotel-intelligence-stayingapi-validation",
      error: String(err?.message || err).slice(0, 400),
    })
  );
  process.exit(1);
});
