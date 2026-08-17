#!/usr/bin/env node
/**
 * SerpApi provider validation against frozen CALA sample.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
 * Reuses existing serpapi-google-hotels client. Never maps room types → Rooms/Keys.
 *
 * Usage:
 *   node scripts/hotel-intelligence-serpapi-validation.mjs
 *   node scripts/hotel-intelligence-serpapi-validation.mjs --controlled-only
 *   node scripts/hotel-intelligence-serpapi-validation.mjs --credit-ceiling 500
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
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import { createHotelIntelligenceService } from "../lib/hotel-intelligence/orchestration/service.js";
import {
  createSerpApiProvider,
  SERPAPI_ROOMS_CAPABILITY,
  matchCensusProperty,
} from "../lib/hotel-intelligence/providers/serpapi.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";
import { haversineM } from "../lib/research-engine-v2/providers/serpapi-google-hotels/match.js";
import { getAccount } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import { SerpApiCreditTracker } from "../lib/research-engine-v2/providers/serpapi-google-hotels/credit-tracker.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const FROZEN_SAMPLE = path.join(
  ROOT,
  "reports/hotel-intelligence/cala-validation-v1/01-sample-definition.json"
);
const OUT_DIR = path.join(ROOT, "reports/hotel-intelligence/serpapi-validation-v1");
const DATA_DIR = path.join(ROOT, "data/hotel-intelligence/serpapi-validation-v1");

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.HOTEL_INTELLIGENCE_SERPAPI = "1";

const args = process.argv.slice(2);
const CONTROLLED_ONLY = args.includes("--controlled-only");
function argNum(name, fallback) {
  const i = args.indexOf(name);
  if (i < 0) return fallback;
  const n = Number(args[i + 1]);
  return Number.isFinite(n) ? n : fallback;
}
const CREDIT_CEILING = argNum("--credit-ceiling", 500);
const SLEEP_MS = argNum("--sleep-ms", 250);

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
    name:
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      null,
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
  const taken = new Set();
  const out = [];
  const push = (pred) => {
    for (const b of baselines) {
      if (out.length >= 10) return;
      if (taken.has(b.census_record_id)) continue;
      if (!pred(b)) continue;
      taken.add(b.census_record_id);
      out.push(b);
      return;
    }
  };
  // Explicit mix for identity + gap coverage
  push((b) => b.country === "Mexico" && b.latitude == null);
  push((b) => b.country === "Mexico" && blank(b.address));
  push((b) => b.country === "Mexico" && blank(b.phone));
  push((b) => b.country === "Dominican Republic" && blank(b.website));
  push((b) => b.country === "Dominican Republic" && b.rooms == null);
  push((b) => b.country === "Costa Rica" && Boolean(b.brand));
  push((b) => b.country === "Colombia" && !b.brand);
  push((b) => b.latitude != null && b.longitude != null); // control for coord agreement
  push((b) => b.country === "Panama" || b.country === "Guatemala" || b.country === "Jamaica");
  push((b) => true);
  for (const b of baselines) {
    if (out.length >= 10) break;
    if (taken.has(b.census_record_id)) continue;
    taken.add(b.census_record_id);
    out.push(b);
  }
  return out.slice(0, 10);
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

function needsDetails(hotel, b) {
  if (!hotel?.raw_safe?.serpapi_property_token) return false;
  const missingPhone = blank(b.phone) && blank(hotel.phone);
  const missingAddr = blank(b.address) && blank(hotel.address);
  const missingCoords =
    b.latitude == null &&
    (hotel.latitude == null || hotel.longitude == null);
  const missingWeb = blank(b.website) && blank(hotel.website);
  return missingPhone || missingAddr || missingCoords || missingWeb;
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

  metrics.calls += 1;
  const searched = await provider.searchHotels({
    name: b.name,
    city: b.city,
    country: b.country,
    hotel_id: hotelId,
  });
  metrics.credits += searched.credits_charged || 0;
  let latency = Date.now() - t0;

  if (searched.provider_status?.status === "quota_exhausted") {
    metrics.quota_events += 1;
    metrics.latency_sum += latency;
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
    metrics.latency_sum += latency;
    return {
      census_record_id: b.census_record_id,
      dhl_id: hotelId,
      ok: false,
      provider_status: searched.provider_status,
      latency_ms: latency,
      credits_charged: searched.credits_charged || 0,
    };
  }

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
      property_token: h.raw_safe?.serpapi_property_token,
    };
    const m = matchCensusProperty(censusHotel, cand);
    if (m.level === "EXACT" || m.level === "HIGH") {
      if (!bestMatch || m.score > bestMatch.score) {
        best = h;
        bestMatch = m;
      }
    } else if (!bestMatch && (m.level === "MEDIUM" || m.level === "LOW")) {
      // keep provisional for reporting only — do not enrich unless EXACT/HIGH
      if (!best || (m.score || 0) > (bestMatch?.score || 0)) {
        best = h;
        bestMatch = m;
      }
    }
  }

  // Prefer EXACT/HIGH only for enrichment
  const enrichmentEligible =
    bestMatch && (bestMatch.level === "EXACT" || bestMatch.level === "HIGH");

  if (!enrichmentEligible) {
    metrics.no_match += 1;
    metrics.latency_sum += latency;
    return {
      census_record_id: b.census_record_id,
      dhl_id: hotelId,
      ok: true,
      identity_match: bestMatch?.level || "none",
      match_confidence: bestMatch?.score || 0,
      candidates: (searched.hotels || []).length,
      latency_ms: latency,
      credits_charged: searched.credits_charged || 0,
      fields: {},
      response_shape: searched.response_shape || null,
    };
  }

  // Optional second call for missing contact/geo
  if (needsDetails(best, b) && provider.tracker.canSpend(1)) {
    metrics.calls += 1;
    metrics.details_calls += 1;
    const detailed = await provider.getHotel(best.raw_safe.serpapi_property_token, {
      property_token: best.raw_safe.serpapi_property_token,
      name: b.name,
      country: b.country,
      hotel_id: hotelId,
    });
    metrics.credits += detailed.credits_charged || 0;
    if (detailed.provider_status?.status === "ok" && detailed.hotel) {
      best = detailed.hotel;
    } else if (detailed.provider_status?.status === "quota_exhausted") {
      metrics.quota_events += 1;
    }
    latency = Date.now() - t0;
  }

  metrics.matched += 1;
  metrics.latency_sum += latency;

  const fieldMap = {
    official_name: best.name,
    address_line_1: best.address,
    city: best.city,
    country: best.country,
    latitude: best.latitude,
    longitude: best.longitude,
    website: best.website,
    phone: best.phone,
    // room_count intentionally omitted
  };
  const fieldResults = {};
  for (const [field, value] of Object.entries(fieldMap)) {
    if (value == null || value === "") continue;
    const conf = scoreFieldConfidence(field, "serpapi");
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
        source: "serpapi",
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

  const propertyToken = best.raw_safe?.serpapi_property_token || best.external_id;
  try {
    if (propertyToken) {
      service.idRegistry.linkExternalId(hotelId, "serpapi", String(propertyToken), {
        external_url: best.raw_safe?.google_property_url || null,
      });
      service.idRegistry.linkExternalId(
        hotelId,
        "serpapi_property_token",
        String(propertyToken)
      );
    }
  } catch {
    /* ignore */
  }

  return {
    census_record_id: b.census_record_id,
    dhl_id: hotelId,
    ok: true,
    serpapi_property_token: propertyToken || null,
    google_place_id: best.raw_safe?.google_place_id || null,
    google_maps_data_id: best.raw_safe?.google_maps_data_id || null,
    google_cid: best.raw_safe?.google_cid || null,
    identity_match: bestMatch.level,
    match_confidence: bestMatch.score,
    match_reasons: bestMatch.reasons,
    candidates: (searched.hotels || []).length,
    latency_ms: latency,
    credits_charged: metrics.credits, // cumulative approx; row-level below
    fields: fieldResults,
    rooms_capability: SERPAPI_ROOMS_CAPABILITY,
    hotel_class_raw: best.raw_safe?.hotel_class_raw ?? null,
  };
}

function accumulateStats(baselines, rows, fieldStats, coordQuality, crossLink, idStats) {
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
    if (row.serpapi_property_token) {
      crossLink.dealality_serpapi += 1;
      idStats.property_tokens += 1;
    }
    if (row.google_place_id) idStats.place_ids += 1;
    if (row.google_cid) idStats.cids += 1;
    if (row.google_maps_data_id) idStats.data_ids += 1;
    if (b.hbx_hotel_code) crossLink.dealality_hotelbeds += 1;
    if (row.serpapi_property_token && b.hbx_hotel_code) {
      crossLink.hotelbeds_serpapi += 1;
    }
    if (row.identity_match === "EXACT" || row.identity_match === "HIGH") {
      idStats.identity_corroborated += 1;
    }
    if (row.identity_match === "EXACT" || row.identity_match === "HIGH") {
      // review flags later
    }

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
                : field === "phone"
                  ? !blank(b.phone)
                  : false;
      fieldStats[field].candidate_found += 1;
      if (!beforePresent) {
        fieldStats[field].still_missing = Math.max(
          0,
          fieldStats[field].still_missing - 1
        );
        if (fr.confidence >= 0.85) fieldStats[field].high_confidence += 1;
      } else if (field === "address_line_1") {
        const a = String(b.address || "").toLowerCase();
        const c = String(fr.value || "").toLowerCase();
        if (a && c && !a.includes(c.slice(0, 12)) && !c.includes(a.slice(0, 12))) {
          fieldStats[field].conflict += 1;
        }
      } else if (field === "phone") {
        const a = String(b.phone || "").replace(/\D/g, "");
        const c = String(fr.value || "").replace(/\D/g, "");
        if (a && c && a.slice(-7) !== c.slice(-7)) fieldStats[field].conflict += 1;
      } else if (field === "website") {
        try {
          const a = new URL(String(b.website)).hostname.replace(/^www\./, "");
          const c = new URL(String(fr.value)).hostname.replace(/^www\./, "");
          if (a && c && a !== c) fieldStats[field].conflict += 1;
        } catch {
          /* ignore */
        }
      }
    }

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

function recoveryTable(fieldStats) {
  const rows = [];
  for (const [field, s] of Object.entries(fieldStats)) {
    const recovered = s.missing_before - s.still_missing;
    rows.push({
      field,
      missing_before: s.missing_before,
      candidate_found: s.candidate_found,
      high_confidence: s.high_confidence,
      conflict: s.conflict,
      still_missing: s.still_missing,
      recovery_pct_of_missing: pct(Math.max(0, recovered), s.missing_before),
    });
  }
  return rows;
}

async function loadFrozenBaselines() {
  if (!fs.existsSync(FROZEN_SAMPLE)) {
    throw new Error(`Frozen sample missing: ${FROZEN_SAMPLE}`);
  }
  const def = JSON.parse(fs.readFileSync(FROZEN_SAMPLE, "utf8"));
  const ids = def.record_ids || def.ids || def.sample_record_ids;
  if (!Array.isArray(ids) || ids.length !== 400) {
    throw new Error(
      `Expected 400 frozen ids, got ${Array.isArray(ids) ? ids.length : "none"}`
    );
  }
  const seed = def.seed || def.sample_seed || "hotel-intelligence-cala-validation-v1";
  console.log(`Frozen sample: ${ids.length} ids (seed=${seed})`);

  const pat = resolvePat();
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!pat || !baseId) throw new Error("Airtable PAT / BASE_ID_ALT required (read-only)");

  const table = new Airtable({ apiKey: pat }).base(baseId)(
    MAP_HOTEL_PROPERTY_CENSUS.tableId
  );
  console.log("Loading frozen sample records from Airtable (read-only)...");
  const byId = new Map();
  // Airtable filterByFormula with OR of RECORD_ID() — chunk to avoid formula length limits
  const chunkSize = 40;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    await table
      .select({
        filterByFormula: formula,
        fields: READ_FIELDS,
        pageSize: 100,
      })
      .eachPage((records, next) => {
        for (const r of records) byId.set(r.id, r);
        next();
      });
  }
  const baselines = ids.map((id) => {
    const r = byId.get(id);
    if (!r) throw new Error(`Missing frozen record ${id}`);
    return baseline(r);
  });
  return { ids, seed, baselines };
}

async function main() {
  const tStart = Date.now();
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });

  const writesFlag = String(
    process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES || "0"
  ).trim();
  if (writesFlag !== "0") {
    throw new Error("Refusing to run with ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES!=0");
  }

  const account = await getAccount();
  writeJson(path.join(OUT_DIR, "00-account.json"), {
    ok: account.ok,
    plan_name: account.plan_name || null,
    searches_per_month: account.searches_per_month ?? null,
    plan_searches_left: account.plan_searches_left ?? null,
    total_searches_left: account.total_searches_left ?? null,
    this_month_usage: account.this_month_usage ?? null,
    account_rate_limit_per_hour: account.account_rate_limit_per_hour ?? null,
    plan_renewal_date: account.plan_renewal_date || null,
    note: "Secrets omitted",
  });
  if (!account.ok) {
    throw new Error(`SerpApi account check failed: ${account.error?.message || "unknown"}`);
  }
  console.log(
    `Account: plan=${account.plan_name}; searches_left=${account.plan_searches_left}; rate/hr=${account.account_rate_limit_per_hour}`
  );

  const capabilityMatrix = {
    hotel_name: "SUPPORTED",
    address: "SUPPORTED",
    city: "SUPPORTED_INDIRECTLY",
    country: "SUPPORTED_INDIRECTLY",
    latitude: "SUPPORTED",
    longitude: "SUPPORTED",
    phone: "SUPPORTED",
    website: "SUPPORTED_INDIRECTLY",
    google_place_id: "NOT_SUPPORTED", // Maps engine not in existing client
    google_maps_data_id: "NOT_SUPPORTED",
    google_cid: "NOT_SUPPORTED",
    google_hotels_property_token: "SUPPORTED",
    hotel_class: "SUPPORTED",
    star_rating: "SUPPORTED_INDIRECTLY",
    review_score: "SUPPORTED",
    review_count: "SUPPORTED",
    amenities: "SUPPORTED",
    description: "SUPPORTED",
    photos: "SUPPORTED_INDIRECTLY",
    check_in_out: "SUPPORTED",
    brand: "NOT_SUPPORTED",
    parent_company: "NOT_SUPPORTED",
    total_property_room_count: "NOT_SUPPORTED",
    room_types: "SUPPORTED_INDIRECTLY",
    rates: "SUPPORTED",
    availability: "SUPPORTED_INDIRECTLY",
    nearby_places: "SUPPORTED_INDIRECTLY",
    SERPAPI_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    evidence:
      "Existing field-firewall SERPAPI_ROOMS_CAPABILITY=NOT_SUPPORTED; rooms[] are bookable types; essential_info bedrooms are VR not hotel keys",
    endpoint_roles: {
      google_hotels_search:
        "Best census value per request when q resolves to direct_property (name/address/gps/phone/token)",
      google_hotels_property_details:
        "Second call for phone/amenities/nearby when search list is thin",
      google_maps:
        "Documented place_id/data_cid/phone/website — NOT wired in existing Dealality client; not called in this validation",
    },
  };
  writeJson(path.join(OUT_DIR, "02-capability-matrix.json"), capabilityMatrix);

  const { seed, baselines } = await loadFrozenBaselines();
  writeJson(path.join(OUT_DIR, "01-baselines.json"), {
    seed,
    count: baselines.length,
    gap_counts: {
      missing_address: baselines.filter((b) => blank(b.address)).length,
      missing_coords: baselines.filter((b) => b.latitude == null).length,
      missing_phone: baselines.filter((b) => blank(b.phone)).length,
      missing_website: baselines.filter((b) => blank(b.website)).length,
      missing_rooms: baselines.filter((b) => b.rooms == null).length,
      missing_brand: baselines.filter((b) => blank(b.brand)).length,
    },
  });

  const tracker = new SerpApiCreditTracker({
    ceiling: CREDIT_CEILING,
    startingSearchesLeft: account.plan_searches_left ?? null,
  });
  const store = createLocalStore({ dataDir: DATA_DIR });
  const provider = createSerpApiProvider({
    forceEnabled: true,
    tracker,
    creditCeiling: CREDIT_CEILING,
    startingSearchesLeft: account.plan_searches_left ?? null,
  });
  const service2 = createHotelIntelligenceService({
    store,
    forceSerpapi: true,
    serpCreditCeiling: CREDIT_CEILING,
    serpTracker: tracker,
    censusRecords: [],
    serpapi: provider,
  });

  const controlled = pickControlled10(baselines);
  console.log(`\nControlled test: ${controlled.length} hotels (credit ceiling ${CREDIT_CEILING})`);
  const controlledMetrics = {
    calls: 0,
    details_calls: 0,
    credits: 0,
    failed: 0,
    matched: 0,
    no_match: 0,
    quota_events: 0,
    latency_sum: 0,
  };
  const controlledRows = [];
  for (const b of controlled) {
    const row = await enrichOne(b, provider, service2, controlledMetrics);
    controlledRows.push(row);
    console.log(
      `  ${b.census_record_id} ${b.country} → match=${row.identity_match || row.provider_status?.status || "?"}`
    );
    await sleep(SLEEP_MS);
  }
  writeJson(path.join(OUT_DIR, "03-controlled-10.json"), {
    hotels_tested: controlled.length,
    metrics: {
      ...controlledMetrics,
      avg_latency_ms: controlledMetrics.calls
        ? Math.round(controlledMetrics.latency_sum / controlled.length)
        : 0,
      successful_lookups: controlledMetrics.matched,
      failed_lookups: controlledMetrics.failed,
    },
    rows: controlledRows,
  });

  const controlledMatchRate =
    controlled.length > 0 ? controlledMetrics.matched / controlled.length : 0;
  const controlledFailRate =
    controlled.length > 0 ? controlledMetrics.failed / controlled.length : 0;

  let stopReason = null;
  if (controlledFailRate >= 0.5) {
    stopReason = "controlled_failure_rate_ge_50pct";
  } else if (controlledMatchRate < 0.3 && controlledMetrics.matched === 0) {
    stopReason = "controlled_zero_exact_high_matches";
  } else if (tracker.blocked) {
    stopReason = "credit_ceiling_during_controlled";
  }

  const allRows = [...controlledRows];
  let fullMetrics = {
    calls: 0,
    details_calls: 0,
    credits: 0,
    failed: 0,
    matched: 0,
    no_match: 0,
    quota_events: 0,
    latency_sum: 0,
  };

  if (CONTROLLED_ONLY) {
    stopReason = stopReason || "controlled_only_flag";
  } else if (stopReason) {
    console.log(`\nSTOP before full sample: ${stopReason}`);
  } else {
    const remaining = baselines.filter(
      (b) => !controlled.some((c) => c.census_record_id === b.census_record_id)
    );
    console.log(`\nFull-sample enrich (credit-capped). Remaining hotels: ${remaining.length}`);
    let n = 0;
    for (const b of remaining) {
      if (!tracker.canSpend(1)) {
        console.log("  Credit ceiling reached — stopping full sample");
        fullMetrics.quota_events += 1;
        break;
      }
      const row = await enrichOne(b, provider, service2, fullMetrics);
      allRows.push(row);
      n += 1;
      if (n % 25 === 0) {
        console.log(
          `  enriched ${n} (matched=${fullMetrics.matched}; searches~${tracker.charged})`
        );
      }
      if (row.stopped_quota) break;
      await sleep(SLEEP_MS);
    }
  }

  const fieldStats = {};
  const coordQuality = { "<100m": 0, "100-500m": 0, "500m-2km": 0, ">2km": 0 };
  const crossLink = {
    dealality_serpapi: 0,
    dealality_hotelbeds: 0,
    hotelbeds_serpapi: 0,
  };
  const idStats = {
    property_tokens: 0,
    place_ids: 0,
    data_ids: 0,
    cids: 0,
    identity_corroborated: 0,
  };
  accumulateStats(baselines, allRows, fieldStats, coordQuality, crossLink, idStats);

  // Field stats for attempted subset only — recompute recovery on rows that were attempted
  const attemptedIds = new Set(allRows.map((r) => r.census_record_id));
  const attemptedBaselines = baselines.filter((b) => attemptedIds.has(b.census_record_id));
  const fieldStatsAttempted = {};
  const coordQualityAttempted = {
    "<100m": 0,
    "100-500m": 0,
    "500m-2km": 0,
    ">2km": 0,
  };
  const crossLinkAttempted = {
    dealality_serpapi: 0,
    dealality_hotelbeds: 0,
    hotelbeds_serpapi: 0,
  };
  const idStatsAttempted = {
    property_tokens: 0,
    place_ids: 0,
    data_ids: 0,
    cids: 0,
    identity_corroborated: 0,
  };
  accumulateStats(
    attemptedBaselines,
    allRows,
    fieldStatsAttempted,
    coordQualityAttempted,
    crossLinkAttempted,
    idStatsAttempted
  );

  writeJson(path.join(OUT_DIR, "04-full-enrich-rows.json"), {
    attempted: allRows.length,
    rows: allRows,
  });

  const accountEnd = await getAccount();
  tracker.endingSearchesLeft = accountEnd.plan_searches_left ?? null;
  const accountDelta =
    account.plan_searches_left != null && accountEnd.plan_searches_left != null
      ? Number(account.plan_searches_left) - Number(accountEnd.plan_searches_left)
      : null;

  const recovery = recoveryTable(fieldStatsAttempted);
  const matchedExactHigh = allRows.filter(
    (r) => r.identity_match === "EXACT" || r.identity_match === "HIGH"
  ).length;

  const recommendation =
    matchedExactHigh >= 50 &&
    (fieldStatsAttempted.latitude?.high_confidence || 0) +
      (fieldStatsAttempted.address_line_1?.high_confidence || 0) +
      (fieldStatsAttempted.phone?.high_confidence || 0) >=
      30
      ? "USE_SERPAPI_AS_COMPLEMENTARY_PROVIDER"
      : matchedExactHigh >= 20
        ? "USE_SERPAPI_AS_COMPLEMENTARY_PROVIDER"
        : matchedExactHigh === 0
          ? "DO_NOT_USE_SERPAPI_FOR_CENSUS"
          : "SERPAPI_REQUIRES_MORE_VALIDATION";

  const summary = {
    marker: "DEALALITY_SERPAPI_PROVIDER_VALIDATION_COMPLETE",
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
      existing_implementation:
        "lib/research-engine-v2/providers/serpapi-google-hotels/*",
      existing_mcp: false,
      existing_credentials_configuration: "SERPAPI_KEY in .env / .env.example",
      previously_used_by_hotel_intelligence_mcp: false,
      cursor_mcp_serpapi: false,
      prior_census_usage:
        "census-autopilot SerpApi gap closure / cache / rights / benchmarks",
    },
    capability_matrix: capabilityMatrix,
    room_count_verdict: "SERPAPI_NOT_A_ROOM_COUNT_SOURCE",
    account: {
      plan_name: account.plan_name,
      searches_per_month: account.searches_per_month,
      plan_searches_left_start: account.plan_searches_left,
      plan_searches_left_end: accountEnd.plan_searches_left ?? null,
      account_delta_searches: accountDelta,
      rate_limit_per_hour: account.account_rate_limit_per_hour,
      cached_search_behavior:
        "SerpApi docs: identical cached searches may not count; Account API delta is authoritative",
    },
    credits: {
      ceiling: CREDIT_CEILING,
      charged_estimate: tracker.charged,
      account_delta: accountDelta,
    },
    controlled: {
      hotels: controlled.length,
      matched_exact_high: controlledMetrics.matched,
      no_match: controlledMetrics.no_match,
      failed: controlledMetrics.failed,
      calls: controlledMetrics.calls,
      details_calls: controlledMetrics.details_calls,
      avg_latency_ms: controlled.length
        ? Math.round(controlledMetrics.latency_sum / controlled.length)
        : 0,
      stop_reason: stopReason,
    },
    frozen_400: {
      sample_size: 400,
      enrich_attempted: allRows.length,
      enrich_matched_exact_or_high: matchedExactHigh,
      credit_limited: tracker.blocked || allRows.length < 400,
      stop_reason: stopReason,
    },
    field_recovery_attempted_subset: fieldStatsAttempted,
    field_recovery_table: recovery,
    coordinate_recovery: {
      sample_missing_coords_full400: baselines.filter((b) => b.latitude == null).length,
      attempted_missing_coords: attemptedBaselines.filter((b) => b.latitude == null)
        .length,
      coords_found:
        (fieldStatsAttempted.latitude?.missing_before || 0) -
        (fieldStatsAttempted.latitude?.still_missing || 0),
      coords_high_confidence: fieldStatsAttempted.latitude?.high_confidence || 0,
      coords_conflicts: fieldStatsAttempted.latitude?.conflict || 0,
      coords_still_missing: fieldStatsAttempted.latitude?.still_missing || 0,
      distance_when_both_present: coordQualityAttempted,
    },
    room_count_recovery: {
      note: "SERPAPI_NOT_A_ROOM_COUNT_SOURCE",
      candidate_found: 0,
    },
    external_ids: idStatsAttempted,
    cross_provider_linkage: crossLinkAttempted,
    efficiency: {
      total_serpapi_calls: controlledMetrics.calls + fullMetrics.calls,
      details_calls: controlledMetrics.details_calls + fullMetrics.details_calls,
      successful_matched: matchedExactHigh,
      failed_calls: controlledMetrics.failed + fullMetrics.failed,
      average_latency_ms: allRows.length
        ? Math.round(
            (controlledMetrics.latency_sum + fullMetrics.latency_sum) / allRows.length
          )
        : 0,
      estimated_calls_per_hotel:
        allRows.length > 0
          ? Math.round(
              ((controlledMetrics.calls + fullMetrics.calls) / allRows.length) * 100
            ) / 100
          : 1,
      account_delta_searches: accountDelta,
      scale_estimate_calls: {
        "1000": "~1000–1500 searches (1 search + optional details)",
        "5956": "~6000–9000 searches",
        "10000": "~10k–15k searches",
        "50000": "exceeds current 15k/mo plan without cache reuse",
      },
    },
    best_provider_by_field: {
      hotel_identity: {
        best: "dealality_census",
        fallback: ["serpapi", "hotelbeds"],
      },
      address: {
        best: "serpapi",
        fallback: ["hotelbeds", "dealality_census"],
        note: "observed when Exact/High match",
      },
      coordinates: {
        best: "serpapi",
        fallback: ["hotelbeds", "dealality_census"],
      },
      room_count: {
        best: "hotelbeds",
        fallback: ["official_site", "dealality_census"],
        note: "serpapi/stayingapi NOT_SUPPORTED",
      },
      brand: {
        best: "dealality_census",
        fallback: ["brand_directory"],
        note: "serpapi not a brand SoT",
      },
      parent_company: {
        best: "dealality_census",
        fallback: ["brand_directory"],
      },
      phone: {
        best: "serpapi",
        fallback: ["hotelbeds", "dealality_census"],
      },
      website: {
        best: "dealality_census",
        fallback: ["serpapi", "hotelbeds"],
      },
      rates: {
        best: "hotelbeds",
        fallback: ["serpapi", "hotelapi_co_free"],
      },
    },
    recommendation,
    highest_value_next_step:
      recommendation === "USE_SERPAPI_AS_COMPLEMENTARY_PROVIDER"
        ? "Use SerpApi Google Hotels as complementary GEO/ADDRESS/PHONE/identity-token provider for census gaps; keep Hotelbeds as rooms source when LIVE; do not use SerpApi for Rooms/Keys."
        : "Investigate controlled-match failures before bulk SerpApi spend; keep Hotelbeds LIVE quota as rooms priority.",
    runtime_ms: Date.now() - tStart,
  };

  writeJson(path.join(OUT_DIR, "05-summary.json"), summary);

  const md = `# SerpApi Provider Validation

\`DEALALITY_SERPAPI_PROVIDER_VALIDATION_COMPLETE\`

## Room count verdict
**SERPAPI_NOT_A_ROOM_COUNT_SOURCE**

## Recommendation
**${recommendation}**

## Controlled 10
- matched Exact/High: ${controlledMetrics.matched}/${controlled.length}
- failed: ${controlledMetrics.failed}
- calls: ${controlledMetrics.calls}
- stop: ${stopReason || "none"}

## Frozen sample attempted
- attempted: ${allRows.length}/400
- Exact/High: ${matchedExactHigh}
- property_tokens: ${idStatsAttempted.property_tokens}
- account search delta: ${accountDelta}

## Field recovery (attempted subset)
| Field | Missing Before | Candidate Found | High Conf | Conflict | Still Missing | Recovery % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
${recovery
  .map(
    (r) =>
      `| ${r.field} | ${r.missing_before} | ${r.candidate_found} | ${r.high_confidence} | ${r.conflict} | ${r.still_missing} | ${r.recovery_pct_of_missing}% |`
  )
  .join("\n")}

## Safety
Airtable writes: 0 · Census writes: 0 · Secrets exposed: no
`;
  fs.writeFileSync(path.join(OUT_DIR, "SERPAPI_VALIDATION_REPORT.md"), md, "utf8");

  console.log("\nDEALALITY_SERPAPI_PROVIDER_VALIDATION_COMPLETE");
  console.log(
    JSON.stringify(
      {
        recommendation,
        room_verdict: "SERPAPI_NOT_A_ROOM_COUNT_SOURCE",
        controlled_matched: controlledMetrics.matched,
        enrich_attempted: allRows.length,
        matched_exact_high: matchedExactHigh,
        account_delta: accountDelta,
        geo_high_conf: fieldStatsAttempted.latitude?.high_confidence || 0,
        phone_high_conf: fieldStatsAttempted.phone?.high_confidence || 0,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
