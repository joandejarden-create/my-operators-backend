#!/usr/bin/env node
/**
 * Dealality Hotel Intelligence MCP — Live CALA read-only validation.
 *
 * SAFETY: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES forced to 0.
 * No Airtable creates/updates/deletes. Local reports only.
 *
 * Usage:
 *   node scripts/hotel-intelligence-cala-validation.mjs
 *   node scripts/hotel-intelligence-cala-validation.mjs --max-hbx-enrich 40
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import { MAP_CENSUS_FIELDS, MAP_HOTEL_PROPERTY_CENSUS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import { createHotelIntelligenceService } from "../lib/hotel-intelligence/orchestration/service.js";
import { MATCH_STATUS } from "../lib/hotel-intelligence/identity-resolve.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";
import { createHotelbedsProvider } from "../lib/hotel-intelligence/providers/hotelbeds.js";
import {
  DEALALITY_CALA_GEOGRAPHIES,
  normalizeGeographyLabel,
} from "../lib/research-engine-v2/dealality-cala-geography-registry-v1.js";
import { normName } from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";
import { normalizeKey } from "../lib/independent-census/match-current-census.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const RUN_ID = "cala-validation-v1";
const SAMPLE_TARGET = 400;
const SAMPLE_SEED = "hotel-intelligence-cala-validation-v1";

// --- SAFETY LOCK ---
process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";

const args = process.argv.slice(2);
function argNum(name, fallback) {
  const i = args.indexOf(name);
  if (i < 0) return fallback;
  const n = Number(args[i + 1]);
  return Number.isFinite(n) ? n : fallback;
}
const MAX_HBX_ENRICH = argNum("--max-hbx-enrich", 50);

const OUT_DIR = path.join(ROOT, "reports", "hotel-intelligence", RUN_ID);
const DATA_DIR = path.join(ROOT, "data", "hotel-intelligence", RUN_ID);

/** Live-probed fields only (Chain Scale / Postal Code absent on HPC as of validation). */
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
  MAP_CENSUS_FIELDS.market,
  MAP_CENSUS_FIELDS.submarket,
  MAP_CENSUS_FIELDS.roomCount,
  MAP_CENSUS_FIELDS.propertyType,
  MAP_CENSUS_FIELDS.status,
  MAP_CENSUS_FIELDS.brandName,
  MAP_CENSUS_FIELDS.parentCompanyName,
  MAP_CENSUS_FIELDS.affiliationStatus,
  MAP_CENSUS_FIELDS.website,
  MAP_CENSUS_FIELDS.phone,
  MAP_CENSUS_FIELDS.identityConfidence,
  MAP_CENSUS_FIELDS.hbxHotelCode,
  MAP_CENSUS_FIELDS.hbxSourceStatus,
];

const CALA_NAMES = new Set(
  DEALALITY_CALA_GEOGRAPHIES.filter(
    (g) => g.scope !== "out_of_scope" && g.scope !== "excluded"
  ).map((g) => normalizeGeographyLabel(g.name))
);

function blank(v) {
  return v == null || !String(v).trim();
}

function hasVal(v) {
  if (blank(v)) return false;
  if (typeof v === "number") return Number.isFinite(v) && !(v === 0 && arguments[1] === "coord");
  return true;
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

function resolvePat() {
  return (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
}

function ensureDirs() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function writeJson(file, data) {
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function stableHash(s) {
  return crypto.createHash("sha256").update(`${SAMPLE_SEED}:${s}`).digest("hex");
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 1000) / 10;
}

function confBucket(score) {
  const n = Number(score);
  if (!Number.isFinite(n)) return "unknown";
  if (n >= 0.95) return "0.95-1.00";
  if (n >= 0.85) return "0.85-0.94";
  if (n >= 0.7) return "0.70-0.84";
  if (n >= 0.5) return "0.50-0.69";
  return "<0.50";
}

function isCalaCountry(country) {
  const n = normalizeGeographyLabel(country);
  if (!n) return false;
  if (CALA_NAMES.has(n)) return true;
  // alias soft match against registry
  for (const g of DEALALITY_CALA_GEOGRAPHIES) {
    if (normalizeGeographyLabel(g.name) === n) return true;
    for (const a of g.aliases || []) {
      if (normalizeGeographyLabel(a) === n) return true;
    }
  }
  return false;
}

async function loadAllCensusRecords(base) {
  const table = MAP_HOTEL_PROPERTY_CENSUS.tableName;
  const records = [];
  await base(table)
    .select({ fields: READ_FIELDS, pageSize: 100 })
    .eachPage((page, next) => {
      for (const r of page) records.push({ id: r.id, fields: r.fields || {} });
      next();
    });
  return records;
}

function fieldPresence(records) {
  const total = records.length;
  const count = (pred) => records.filter((r) => pred(r.fields || {})).length;
  return {
    total,
    with_rooms: count(hasRooms),
    missing_rooms: count((f) => !hasRooms(f)),
    with_brand: count((f) => !blank(f[MAP_CENSUS_FIELDS.brandName])),
    missing_brand: count((f) => blank(f[MAP_CENSUS_FIELDS.brandName])),
    with_parent: count((f) => !blank(f[MAP_CENSUS_FIELDS.parentCompanyName])),
    missing_parent: count((f) => blank(f[MAP_CENSUS_FIELDS.parentCompanyName])),
    with_coords: count(hasCoords),
    missing_coords: count((f) => !hasCoords(f)),
    with_website: count((f) => !blank(f[MAP_CENSUS_FIELDS.website])),
    missing_website: count((f) => blank(f[MAP_CENSUS_FIELDS.website])),
    with_phone: count((f) => !blank(f[MAP_CENSUS_FIELDS.phone])),
    missing_phone: count((f) => blank(f[MAP_CENSUS_FIELDS.phone])),
    with_address: count((f) => !blank(f[MAP_CENSUS_FIELDS.address])),
    with_hbx_code: count((f) => !blank(f[MAP_CENSUS_FIELDS.hbxHotelCode])),
  };
}

function distribution(records, field) {
  const m = {};
  for (const r of records) {
    const k = String(r.fields?.[field] || "(blank)").trim() || "(blank)";
    m[k] = (m[k] || 0) + 1;
  }
  return Object.entries(m)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 40)
    .map(([name, count]) => ({ name, count }));
}

function qualityBucket(f) {
  const gaps = [];
  if (!hasRooms(f)) gaps.push("missing_rooms");
  if (!hasCoords(f)) gaps.push("missing_coords");
  if (blank(f[MAP_CENSUS_FIELDS.brandName])) gaps.push("missing_brand");
  if (blank(f[MAP_CENSUS_FIELDS.website])) gaps.push("missing_website");
  if (!gaps.length) return "complete";
  if (gaps.length >= 3) return "sparse";
  return gaps.sort().join("+");
}

function buildDeterministicSample(calaRecords, target = SAMPLE_TARGET) {
  const byCountry = new Map();
  for (const r of calaRecords) {
    const c = String(r.fields[MAP_CENSUS_FIELDS.country] || "Unknown").trim();
    if (!byCountry.has(c)) byCountry.set(c, []);
    byCountry.get(c).push(r);
  }
  for (const [, arr] of byCountry) {
    arr.sort((a, b) => stableHash(a.id).localeCompare(stableHash(b.id)));
  }

  const countries = [...byCountry.keys()].sort();
  const selected = [];
  const selectedIds = new Set();
  const perCountryFloor = Math.max(15, Math.floor(target / Math.max(1, countries.length)));

  // Pass 1: stratified by country + quality
  for (const country of countries) {
    const pool = byCountry.get(country) || [];
    const buckets = new Map();
    for (const r of pool) {
      const qb = qualityBucket(r.fields);
      if (!buckets.has(qb)) buckets.set(qb, []);
      buckets.get(qb).push(r);
    }
    const bucketKeys = [...buckets.keys()].sort();
    let taken = 0;
    let guard = 0;
    while (taken < perCountryFloor && guard < pool.length * 2) {
      for (const bk of bucketKeys) {
        const arr = buckets.get(bk);
        if (!arr?.length) continue;
        const r = arr.shift();
        if (selectedIds.has(r.id)) continue;
        selected.push(r);
        selectedIds.add(r.id);
        taken += 1;
        if (taken >= perCountryFloor || selected.length >= target) break;
      }
      guard += 1;
      if (selected.length >= target) break;
    }
  }

  // Pass 2: fill with hashed remainder prioritizing gaps
  if (selected.length < target) {
    const rest = calaRecords
      .filter((r) => !selectedIds.has(r.id))
      .sort((a, b) => {
        const ga = qualityBucket(a.fields) === "complete" ? 1 : 0;
        const gb = qualityBucket(b.fields) === "complete" ? 1 : 0;
        if (ga !== gb) return ga - gb; // prefer incomplete
        return stableHash(a.id).localeCompare(stableHash(b.id));
      });
    for (const r of rest) {
      selected.push(r);
      selectedIds.add(r.id);
      if (selected.length >= target) break;
    }
  }

  return selected.slice(0, target);
}

function baselineFromRecord(r) {
  const f = r.fields || {};
  return {
    census_record_id: r.id,
    property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    name: f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || null,
    property_name: f[MAP_CENSUS_FIELDS.propertyName] || null,
    address: f[MAP_CENSUS_FIELDS.address] || null,
    city: f[MAP_CENSUS_FIELDS.city] || null,
    country: f[MAP_CENSUS_FIELDS.country] || null,
    market: f[MAP_CENSUS_FIELDS.market] || null,
    latitude: hasCoords(f) ? Number(f[MAP_CENSUS_FIELDS.latitude]) : null,
    longitude: hasCoords(f) ? Number(f[MAP_CENSUS_FIELDS.longitude]) : null,
    rooms: hasRooms(f) ? Number(f[MAP_CENSUS_FIELDS.roomCount]) : null,
    brand: f[MAP_CENSUS_FIELDS.brandName] || null,
    parent_company: f[MAP_CENSUS_FIELDS.parentCompanyName] || null,
    website: f[MAP_CENSUS_FIELDS.website] || null,
    phone: f[MAP_CENSUS_FIELDS.phone] || null,
    status: f[MAP_CENSUS_FIELDS.status] || null,
    property_type: f[MAP_CENSUS_FIELDS.propertyType] || null,
    chain_scale: f[MAP_CENSUS_FIELDS.chainScale] || null,
    hbx_hotel_code: f[MAP_CENSUS_FIELDS.hbxHotelCode] || null,
    identity_confidence: f[MAP_CENSUS_FIELDS.identityConfidence] || null,
    completeness: {
      rooms: hasRooms(f),
      coords: hasCoords(f),
      brand: !blank(f[MAP_CENSUS_FIELDS.brandName]),
      parent: !blank(f[MAP_CENSUS_FIELDS.parentCompanyName]),
      website: !blank(f[MAP_CENSUS_FIELDS.website]),
      phone: !blank(f[MAP_CENSUS_FIELDS.phone]),
      address: !blank(f[MAP_CENSUS_FIELDS.address]),
    },
  };
}

function resolveInputFromBaseline(b) {
  return {
    name: b.name,
    address: b.address,
    city: b.city,
    country: b.country,
    latitude: b.latitude,
    longitude: b.longitude,
    brand: b.brand,
    website: b.website,
    phone: b.phone,
    external_ids: b.hbx_hotel_code ? { hotelbeds: String(b.hbx_hotel_code) } : undefined,
  };
}

function mapResolveClass(status) {
  if (status === MATCH_STATUS.EXACT) return "exact";
  if (status === MATCH_STATUS.STRONG) return "strong";
  if (status === MATCH_STATUS.PROBABLE) return "probable";
  if (status === MATCH_STATUS.AMBIGUOUS) return "ambiguous";
  if (status === MATCH_STATUS.NEW || status === MATCH_STATUS.INSUFFICIENT) return "no_match";
  return "error";
}

function roomsConflictMagnitude(a, b) {
  const d = Math.abs(Number(a) - Number(b));
  if (!Number.isFinite(d)) return null;
  if (d <= 2) return "1-2";
  if (d <= 5) return "3-5";
  if (d <= 10) return "6-10";
  return ">10";
}

function brandNormOnly(a, b) {
  const na = normalizeKey(a || "").replace(/\b(hotels?|resorts?|by|the)\b/g, " ").replace(/\s+/g, " ").trim();
  const nb = normalizeKey(b || "").replace(/\b(hotels?|resorts?|by|the)\b/g, " ").replace(/\s+/g, " ").trim();
  if (!na || !nb) return false;
  return na === nb || na.includes(nb) || nb.includes(na);
}

async function main() {
  const t0 = Date.now();
  ensureDirs();

  const writeFlag = process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES;
  if (writeFlag !== "0") {
    throw new Error(`Refusing to run: ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=${writeFlag}`);
  }

  const pat = resolvePat();
  const baseId = String(process.env.AIRTABLE_BASE_ID_ALT || "").trim();
  if (!pat || !baseId) throw new Error("Missing AIRTABLE_PAT/API_KEY or AIRTABLE_BASE_ID_ALT");

  const base = new Airtable({ apiKey: pat }).base(baseId);

  console.log("Loading live Hotel Property Census (read-only)...");
  const allRecords = await loadAllCensusRecords(base);
  const calaRecords = allRecords.filter((r) =>
    isCalaCountry(r.fields?.[MAP_CENSUS_FIELDS.country])
  );

  const universePresence = fieldPresence(allRecords);
  const calaPresence = fieldPresence(calaRecords);
  const countryDist = distribution(allRecords, MAP_CENSUS_FIELDS.country);
  const marketDist = distribution(calaRecords, MAP_CENSUS_FIELDS.market);

  // HBX status (single probe; do not crash on quota)
  const hbx = createHotelbedsProvider({
    env: process.env,
    forceEnabled: String(process.env.ENABLE_HBX_CONTENT_API || "0") === "1",
    maxRequestsPerRun: Math.max(MAX_HBX_ENRICH + 5, 20),
  });
  let hbxStatus;
  try {
    hbxStatus = await hbx.getAvailabilityStatus();
  } catch (err) {
    hbxStatus = {
      provider: "hotelbeds",
      status: "unavailable",
      retryable: true,
      message: String(err?.message || err).slice(0, 160),
    };
  }

  const preflight = {
    marker: "HOTEL_INTELLIGENCE_CALA_VALIDATION_PREFLIGHT",
    safety: {
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: writeFlag,
      airtable_writes: "disabled",
      migrations: "none",
      schema_changes: "none",
    },
    table: MAP_HOTEL_PROPERTY_CENSUS,
    live_census_record_count: allRecords.length,
    cala_record_count: calaRecords.length,
    completeness_universe: universePresence,
    completeness_cala: calaPresence,
    country_distribution: countryDist,
    major_market_distribution_cala: marketDist,
    provider_status: {
      dealality_census: { status: "ok", retryable: false },
      hotelbeds: hbxStatus,
    },
    hbx_quota_api_status: hbxStatus,
    write_flag_status: writeFlag,
    generated_at: new Date().toISOString(),
  };
  writeJson(path.join(OUT_DIR, "00-preflight.json"), preflight);
  console.log("\nHOTEL_INTELLIGENCE_CALA_VALIDATION_PREFLIGHT");
  console.log(
    JSON.stringify(
      {
        live_census_record_count: preflight.live_census_record_count,
        cala_record_count: preflight.cala_record_count,
        rooms: `${calaPresence.with_rooms} present / ${calaPresence.missing_rooms} missing`,
        brand: `${calaPresence.with_brand} present / ${calaPresence.missing_brand} missing`,
        coords: `${calaPresence.with_coords} present / ${calaPresence.missing_coords} missing`,
        website: `${calaPresence.with_website} present / ${calaPresence.missing_website} missing`,
        hbx: hbxStatus.status,
        write_flag: writeFlag,
      },
      null,
      2
    )
  );

  // Sample
  const sample = buildDeterministicSample(calaRecords, SAMPLE_TARGET);
  const sampleDef = {
    seed: SAMPLE_SEED,
    target: SAMPLE_TARGET,
    actual: sample.length,
    record_ids: sample.map((r) => r.id),
    country_counts: (() => {
      const m = {};
      for (const r of sample) {
        const c = r.fields[MAP_CENSUS_FIELDS.country] || "Unknown";
        m[c] = (m[c] || 0) + 1;
      }
      return m;
    })(),
    quality_counts: (() => {
      const m = {};
      for (const r of sample) {
        const q = qualityBucket(r.fields);
        m[q] = (m[q] || 0) + 1;
      }
      return m;
    })(),
  };
  writeJson(path.join(OUT_DIR, "01-sample-definition.json"), sampleDef);
  writeJson(path.join(DATA_DIR, "sample-definition.json"), sampleDef);

  const baselines = sample.map(baselineFromRecord);
  writeJson(path.join(OUT_DIR, "02-baseline.json"), {
    sample_size: baselines.length,
    records: baselines,
  });

  // Service with injected census (no live re-fetch; no writes)
  const store = createLocalStore({ root: path.join(DATA_DIR, "staging") });
  const service = createHotelIntelligenceService({
    store,
    censusRecords: allRecords,
    env: {
      ...process.env,
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
      HOTEL_INTELLIGENCE_HOTELBEDS:
        hbxStatus.status === "ok" ? "1" : "0",
      ENABLE_HBX_CONTENT_API: hbxStatus.status === "ok" ? "1" : "0",
    },
  });

  // Resolution
  const resolveRows = [];
  let resolveLatencySum = 0;
  const byClass = {
    exact: 0,
    strong: 0,
    probable: 0,
    ambiguous: 0,
    no_match: 0,
    error: 0,
  };

  console.log(`\nResolving ${baselines.length} sample hotels...`);
  for (let i = 0; i < baselines.length; i += 1) {
    const b = baselines[i];
    const t = Date.now();
    try {
      // Country-scoped pool for speed; fall back to full if tiny
      const countryPool = allRecords.filter(
        (r) =>
          normalizeGeographyLabel(r.fields?.[MAP_CENSUS_FIELDS.country]) ===
          normalizeGeographyLabel(b.country)
      );
      const pool = countryPool.length >= 5 ? countryPool : allRecords;
      // Temporarily swap service census via resolveHotelIdentity through service
      // Use service.hotelResolve but it loads all — override by calling resolve on filtered:
      const { resolveHotelIdentity } = await import(
        "../lib/hotel-intelligence/identity-resolve.js"
      );
      const result = resolveHotelIdentity(resolveInputFromBaseline(b), pool, {
        idRegistry: service.idRegistry,
        store,
      });
      // also enqueue review via service path for ambiguous
      if (result.review_required || result.match_status === MATCH_STATUS.AMBIGUOUS) {
        await service.hotelResolve(resolveInputFromBaseline(b));
      }
      const cls = mapResolveClass(result.match_status);
      byClass[cls] = (byClass[cls] || 0) + 1;
      const latency = Date.now() - t;
      resolveLatencySum += latency;
      const top = result.candidate_matches?.[0];
      const selfHit =
        (result.candidate_matches || []).some((c) => c.airtable_record_id === b.census_record_id) ||
        top?.airtable_record_id === b.census_record_id;
      resolveRows.push({
        census_record_id: b.census_record_id,
        dhl_id: result.hotel_id,
        resolution_class: cls,
        match_status: result.match_status,
        match_score: result.match_score,
        match_reasons: result.matching_reasons,
        candidate_count: (result.candidate_matches || []).length,
        review_required: Boolean(result.review_required),
        self_matched: selfHit,
        top_candidate_census_id: top?.airtable_record_id || null,
        latency_ms: latency,
        baseline: b,
      });
    } catch (err) {
      byClass.error += 1;
      resolveRows.push({
        census_record_id: b.census_record_id,
        dhl_id: null,
        resolution_class: "error",
        match_score: 0,
        match_reasons: [String(err?.message || err).slice(0, 200)],
        candidate_count: 0,
        review_required: true,
        self_matched: false,
        latency_ms: Date.now() - t,
        baseline: b,
      });
    }
    if ((i + 1) % 50 === 0) console.log(`  resolved ${i + 1}/${baselines.length}`);
  }
  writeJson(path.join(OUT_DIR, "03-resolution-rows.json"), { byClass, rows: resolveRows });

  // Duplicate risk
  const dhlToCensus = new Map();
  const duplicateRisk = [];
  for (const row of resolveRows) {
    if (!row.dhl_id) continue;
    if (!dhlToCensus.has(row.dhl_id)) dhlToCensus.set(row.dhl_id, []);
    dhlToCensus.get(row.dhl_id).push(row.census_record_id);
  }
  const nameCity = new Map();
  for (const b of baselines) {
    const k = `${normName(b.name)}|${normalizeKey(b.city)}|${normalizeGeographyLabel(b.country)}`;
    if (!nameCity.has(k)) nameCity.set(k, []);
    nameCity.get(k).push(b.census_record_id);
  }

  let safe_unique = 0;
  let possible_duplicate = 0;
  let probable_duplicate = 0;
  let ambiguous_identity = 0;
  for (const row of resolveRows) {
    let risk = "safe_unique";
    if (row.resolution_class === "ambiguous") {
      risk = "ambiguous_identity";
      ambiguous_identity += 1;
    } else if (row.dhl_id && (dhlToCensus.get(row.dhl_id) || []).length > 1) {
      risk = "probable_duplicate";
      probable_duplicate += 1;
    } else {
      const k = `${normName(row.baseline.name)}|${normalizeKey(row.baseline.city)}|${normalizeGeographyLabel(row.baseline.country)}`;
      const siblings = nameCity.get(k) || [];
      if (siblings.length > 1) {
        risk = "possible_duplicate";
        possible_duplicate += 1;
      } else if (!row.self_matched && ["exact", "strong", "probable"].includes(row.resolution_class)) {
        risk = "possible_duplicate";
        possible_duplicate += 1;
      } else {
        safe_unique += 1;
      }
    }
    duplicateRisk.push({
      census_record_id: row.census_record_id,
      dhl_id: row.dhl_id,
      risk,
      resolution_class: row.resolution_class,
      self_matched: row.self_matched,
      shared_dhl_with: row.dhl_id
        ? (dhlToCensus.get(row.dhl_id) || []).filter((id) => id !== row.census_record_id)
        : [],
    });
  }
  writeJson(path.join(OUT_DIR, "04-duplicate-risk.json"), {
    counts: { safe_unique, possible_duplicate, probable_duplicate, ambiguous_identity },
    rows: duplicateRisk,
  });

  // Enrichment (external) — only for exact/strong self-matched, HBX when available
  const enrichEligible = resolveRows.filter(
    (r) =>
      (r.resolution_class === "exact" || r.resolution_class === "strong") &&
      r.self_matched &&
      r.dhl_id &&
      r.baseline.hbx_hotel_code
  );
  const hbxAvailable = hbxStatus.status === "ok";
  const enrichTargets = hbxAvailable
    ? enrichEligible.slice(0, MAX_HBX_ENRICH)
    : [];

  const enrichRows = [];
  let enrichLatencySum = 0;
  let hbxCalls = 0;
  let quotaEvents = 0;
  let failedCalls = 0;
  const fieldStats = {};
  const fieldKeys = [
    "official_name",
    "address_line_1",
    "city",
    "country",
    "latitude",
    "longitude",
    "room_count",
    "brand_name",
    "parent_company_name",
    "website",
    "phone",
    "status",
  ];
  for (const fk of fieldKeys) {
    fieldStats[fk] = {
      present_before: 0,
      missing_before: 0,
      candidate_found: 0,
      high_confidence: 0,
      agreement: 0,
      conflict: 0,
      still_missing: 0,
    };
  }

  // Baseline field presence for whole sample (resolution_validation independent of HBX)
  for (const b of baselines) {
    const map = {
      official_name: b.name,
      address_line_1: b.address,
      city: b.city,
      country: b.country,
      latitude: b.latitude,
      longitude: b.longitude,
      room_count: b.rooms,
      brand_name: b.brand,
      parent_company_name: b.parent_company,
      website: b.website,
      phone: b.phone,
      status: b.status,
    };
    for (const fk of fieldKeys) {
      const present =
        fk === "latitude" || fk === "longitude"
          ? b.latitude != null && b.longitude != null
          : fk === "room_count"
            ? b.rooms != null
            : !blank(map[fk]);
      if (present) fieldStats[fk].present_before += 1;
      else {
        fieldStats[fk].missing_before += 1;
        fieldStats[fk].still_missing += 1;
      }
    }
  }

  const roomConflicts = [];
  const brandAnalysis = {
    brand_present_before: baselines.filter((b) => !blank(b.brand)).length,
    brand_missing_before: baselines.filter((b) => blank(b.brand)).length,
    brand_candidate_found: 0,
    brand_exact_agreement: 0,
    brand_normalization_only: 0,
    brand_conflicts: 0,
    parent_company_candidate_found: 0,
    unresolved_source_labels: [],
  };

  const confidenceDist = {
    identity: { "0.95-1.00": 0, "0.85-0.94": 0, "0.70-0.84": 0, "0.50-0.69": 0, "<0.50": 0, unknown: 0 },
    room_count: { "0.95-1.00": 0, "0.85-0.94": 0, "0.70-0.84": 0, "0.50-0.69": 0, "<0.50": 0, unknown: 0 },
    brand: { "0.95-1.00": 0, "0.85-0.94": 0, "0.70-0.84": 0, "0.50-0.69": 0, "<0.50": 0, unknown: 0 },
    coordinates: { "0.95-1.00": 0, "0.85-0.94": 0, "0.70-0.84": 0, "0.50-0.69": 0, "<0.50": 0, unknown: 0 },
  };

  for (const row of resolveRows) {
    const score =
      row.resolution_class === "exact"
        ? 0.99
        : row.resolution_class === "strong"
          ? 0.9
          : row.resolution_class === "probable"
            ? 0.75
            : row.resolution_class === "ambiguous"
              ? 0.55
              : 0.3;
    confidenceDist.identity[confBucket(score)] += 1;
  }

  console.log(
    `\nEnrichment: hbx_available=${hbxAvailable} targets=${enrichTargets.length} (eligible_with_hbx_code=${enrichEligible.length})`
  );

  for (const row of enrichTargets) {
    const t = Date.now();
    try {
      service.idRegistry.linkExternalId(row.dhl_id, "hotelbeds", String(row.baseline.hbx_hotel_code));
    } catch {
      /* ignore */
    }

    let providerHotel = null;
    let providerStatus = null;
    try {
      const got = await hbx.getHotel(String(row.baseline.hbx_hotel_code));
      hbxCalls += 1;
      providerStatus = got.provider_status;
      if (got.provider_status.status === "quota_exhausted") {
        quotaEvents += 1;
        enrichLatencySum += Date.now() - t;
        console.log("HBX quota exhausted — stopping further enrich calls");
        enrichRows.push({
          census_record_id: row.census_record_id,
          stopped_quota: true,
          provider_status: got.provider_status,
        });
        break;
      }
      if (got.provider_status.status !== "ok") {
        failedCalls += 1;
        enrichLatencySum += Date.now() - t;
        enrichRows.push({
          census_record_id: row.census_record_id,
          provider_status: got.provider_status,
          fields: {},
        });
        continue;
      }
      providerHotel = got.hotel;
      // Stage local evidence only (no Airtable writes)
      if (providerHotel) {
        for (const [field, value] of [
          ["official_name", providerHotel.name],
          ["address_line_1", providerHotel.address],
          ["room_count", providerHotel.room_count],
          ["brand_name", providerHotel.brand_name],
          ["website", providerHotel.website],
          ["phone", providerHotel.phone],
          ["latitude", providerHotel.latitude],
          ["longitude", providerHotel.longitude],
        ]) {
          if (value == null || value === "") continue;
          try {
            service.evidence.addEvidence({
              hotel_id: row.dhl_id,
              field,
              value,
              source: "hotelbeds",
              source_record_id: providerHotel.external_id,
            });
          } catch {
            /* ignore */
          }
        }
      }
    } catch (err) {
      failedCalls += 1;
      enrichLatencySum += Date.now() - t;
      enrichRows.push({
        census_record_id: row.census_record_id,
        error: String(err?.message || err).slice(0, 200),
        fields: {},
      });
      continue;
    }
    enrichLatencySum += Date.now() - t;

    const b = row.baseline;
    const providerMap = providerHotel
      ? {
          official_name: providerHotel.name,
          address_line_1: providerHotel.address,
          city: providerHotel.city,
          country: providerHotel.country,
          latitude: providerHotel.latitude,
          longitude: providerHotel.longitude,
          room_count: providerHotel.room_count,
          brand_name: providerHotel.brand_name,
          parent_company_name: providerHotel.parent_company_name,
          website: providerHotel.website,
          phone: providerHotel.phone,
          status: providerHotel.status,
        }
      : {};

    const fieldResults = {};
    for (const fk of fieldKeys) {
      const before =
        fk === "official_name"
          ? b.name
          : fk === "address_line_1"
            ? b.address
            : fk === "brand_name"
              ? b.brand
              : fk === "parent_company_name"
                ? b.parent_company
                : fk === "room_count"
                  ? b.rooms
                  : b[fk === "latitude" ? "latitude" : fk === "longitude" ? "longitude" : fk];
      const beforePresent =
        fk === "latitude" || fk === "longitude"
          ? b.latitude != null
          : fk === "room_count"
            ? b.rooms != null
            : !blank(before);
      const candidate = providerMap[fk];
      const candidatePresent =
        candidate != null && candidate !== "" && !(typeof candidate === "number" && !Number.isFinite(candidate));
      const src = "hotelbeds";
      const conf = candidatePresent
        ? scoreFieldConfidence(
            fk === "brand_name"
              ? "brand_name"
              : fk === "room_count"
                ? "room_count"
                : fk === "latitude" || fk === "longitude"
                  ? "latitude"
                  : fk === "website"
                    ? "website"
                    : fk === "phone"
                      ? "phone"
                      : fk === "official_name"
                        ? "official_name"
                        : fk === "address_line_1"
                          ? "address_line_1"
                          : "default",
            src
          )
        : null;

      if (candidatePresent) {
        fieldStats[fk].candidate_found += 1;
        if (!beforePresent) {
          fieldStats[fk].still_missing = Math.max(0, fieldStats[fk].still_missing - 1);
          if (conf?.confidence >= 0.85) fieldStats[fk].high_confidence += 1;
        }
        if (beforePresent) {
          const same =
            fk === "room_count"
              ? Number(before) === Number(candidate)
              : fk === "latitude" || fk === "longitude"
                ? Math.abs(Number(before) - Number(candidate)) < 0.01
                : brandNormOnly(String(before), String(candidate)) ||
                  normalizeKey(String(before)) === normalizeKey(String(candidate));
          if (same) fieldStats[fk].agreement += 1;
          else {
            fieldStats[fk].conflict += 1;
            if (fk === "room_count") {
              roomConflicts.push({
                census_record_id: b.census_record_id,
                census_rooms: before,
                provider_rooms: candidate,
                magnitude: roomsConflictMagnitude(before, candidate),
                source: "hotelbeds",
                source_record_id: providerHotel?.external_id || b.hbx_hotel_code,
                confidence: conf?.confidence,
              });
            }
            if (fk === "brand_name") {
              brandAnalysis.brand_conflicts += 1;
              if (!brandNormOnly(String(before), String(candidate))) {
                brandAnalysis.unresolved_source_labels.push({
                  census_record_id: b.census_record_id,
                  census_brand: before,
                  source_label: candidate,
                });
              } else {
                brandAnalysis.brand_normalization_only += 1;
                brandAnalysis.brand_conflicts -= 1;
              }
            }
          }
          if (fk === "brand_name" && same) brandAnalysis.brand_exact_agreement += 1;
        } else {
          if (fk === "brand_name") brandAnalysis.brand_candidate_found += 1;
          if (fk === "parent_company_name") brandAnalysis.parent_company_candidate_found += 1;
        }

        if (fk === "room_count" && conf) {
          confidenceDist.room_count[confBucket(conf.confidence)] += 1;
        }
        if (fk === "brand_name" && conf) {
          confidenceDist.brand[confBucket(conf.confidence)] += 1;
        }
        if ((fk === "latitude" || fk === "longitude") && conf) {
          confidenceDist.coordinates[confBucket(conf.confidence)] += 1;
        }
      }

      fieldResults[fk] = {
        before,
        before_present: beforePresent,
        candidate: candidatePresent ? candidate : null,
        confidence: conf?.confidence ?? null,
      };
    }

    // Stage evidence for missing room via service already done in hotelEnrich
    enrichRows.push({
      census_record_id: row.census_record_id,
      dhl_id: row.dhl_id,
      hbx_hotel_code: row.baseline.hbx_hotel_code,
      latency_ms: Date.now() - t,
      enrich_ok: Boolean(providerHotel),
      provider_status: providerStatus,
      fields: fieldResults,
    });
  }

  writeJson(path.join(OUT_DIR, "05-enrichment-rows.json"), {
    hbx_available: hbxAvailable,
    enrich_targets: enrichTargets.length,
    enrich_eligible_with_hbx: enrichEligible.length,
    hbx_calls: hbxCalls,
    quota_events: quotaEvents,
    failed_calls: failedCalls,
    rows: enrichRows,
  });

  // Room count dedicated
  const roomMissing = baselines.filter((b) => b.rooms == null).length;
  const roomPresent = baselines.filter((b) => b.rooms != null).length;
  let roomFoundForMissing = 0;
  let roomHighConf = 0;
  for (const er of enrichRows) {
    const fr = er.fields?.room_count;
    if (!fr) continue;
    if (!fr.before_present && fr.candidate != null) {
      roomFoundForMissing += 1;
      if (fr.confidence >= 0.85) roomHighConf += 1;
    }
  }
  const roomReport = {
    sample_records: baselines.length,
    room_count_present_before: roomPresent,
    room_count_missing_before: roomMissing,
    room_count_found_for_missing: roomFoundForMissing,
    room_count_high_confidence: roomHighConf,
    room_count_conflicts: roomConflicts.length,
    room_count_unresolved: Math.max(0, roomMissing - roomFoundForMissing),
    conflict_magnitude: {
      "1-2": roomConflicts.filter((c) => c.magnitude === "1-2").length,
      "3-5": roomConflicts.filter((c) => c.magnitude === "3-5").length,
      "6-10": roomConflicts.filter((c) => c.magnitude === "6-10").length,
      ">10": roomConflicts.filter((c) => c.magnitude === ">10").length,
    },
    conflicts: roomConflicts,
    note: hbxAvailable
      ? "External room recovery measured on HBX-linked enrich subset only"
      : "HBX unavailable — room recovery from external provider not measured; resolution_validation complete",
  };
  writeJson(path.join(OUT_DIR, "06-room-count.json"), roomReport);
  writeJson(path.join(OUT_DIR, "07-brand-analysis.json"), brandAnalysis);
  writeJson(path.join(OUT_DIR, "08-confidence.json"), confidenceDist);
  writeJson(path.join(OUT_DIR, "09-field-stats.json"), fieldStats);

  // Review queue
  const rq = await service.hotelReviewQueue({ status: "open" });
  const issueCounts = {};
  for (const item of rq.items || []) {
    issueCounts[item.issue_type] = (issueCounts[item.issue_type] || 0) + 1;
  }
  // synthesize high-value review examples from validation rows
  const topReviews = [];
  for (const r of resolveRows.filter((x) => x.resolution_class === "ambiguous").slice(0, 8)) {
    topReviews.push({
      issue_type: "identity_ambiguous",
      census_record_id: r.census_record_id,
      name: r.baseline.name,
      city: r.baseline.city,
      country: r.baseline.country,
      reasons: r.match_reasons,
      recommended_action: "manual_identity_review_do_not_merge",
      value: "high",
    });
  }
  for (const d of duplicateRisk.filter((x) => x.risk === "probable_duplicate").slice(0, 5)) {
    topReviews.push({
      issue_type: "possible_duplicate",
      census_record_id: d.census_record_id,
      shared_dhl_with: d.shared_dhl_with,
      recommended_action: "steward_dedupe_review",
      value: "high",
    });
  }
  for (const c of roomConflicts.slice(0, 5)) {
    topReviews.push({
      issue_type: "room_count_conflict",
      ...c,
      recommended_action: "compare_official_source_before_accept",
      value: "high",
    });
  }
  // missing rooms among exact/strong with hbx
  for (const r of resolveRows
    .filter(
      (x) =>
        (x.resolution_class === "exact" || x.resolution_class === "strong") &&
        x.baseline.rooms == null
    )
    .slice(0, 5)) {
    topReviews.push({
      issue_type: "missing_room_count",
      census_record_id: r.census_record_id,
      name: r.baseline.name,
      hbx_hotel_code: r.baseline.hbx_hotel_code,
      recommended_action: r.baseline.hbx_hotel_code
        ? "enrich_via_hotelbeds_then_official_confirm"
        : "queue_official_rooms_research",
      value: "high",
    });
  }
  writeJson(path.join(OUT_DIR, "10-review-queue.json"), {
    issue_counts: issueCounts,
    open_items: (rq.items || []).length,
    top_examples: topReviews.slice(0, 20),
  });

  // Spot checks
  function pick(cls, n) {
    return resolveRows.filter((r) => r.resolution_class === cls).slice(0, n);
  }
  const spot = {
    exact: pick("exact", 5).map(spotShape),
    strong: pick("strong", 5).map(spotShape),
    probable: pick("probable", 5).map(spotShape),
    ambiguous: pick("ambiguous", 5).map(spotShape),
    room_count_conflicts: roomConflicts.slice(0, 5),
  };
  function spotShape(r) {
    return {
      census_record_id: r.census_record_id,
      census: {
        name: r.baseline.name,
        city: r.baseline.city,
        country: r.baseline.country,
        rooms: r.baseline.rooms,
        brand: r.baseline.brand,
      },
      canonical_candidate: {
        dhl_id: r.dhl_id,
        top_candidate_census_id: r.top_candidate_census_id,
        self_matched: r.self_matched,
      },
      match_reason: r.match_reasons,
      confidence: r.match_score,
      recommended_action: r.review_required
        ? "hold_for_review"
        : r.self_matched
          ? "safe_identity_link"
          : "verify_candidate_not_collision",
    };
  }
  writeJson(path.join(OUT_DIR, "11-spot-checks.json"), spot);

  // Auto-accept potential (estimate, no writes)
  const autoAccept = {};
  for (const fk of fieldKeys) {
    autoAccept[fk] = {
      missing_before: fieldStats[fk].missing_before,
      high_confidence_candidates: fieldStats[fk].high_confidence,
      rate_on_missing:
        fieldStats[fk].missing_before > 0
          ? pct(fieldStats[fk].high_confidence, fieldStats[fk].missing_before)
          : 0,
    };
  }

  const n = baselines.length;
  const resolutionRate = pct(byClass.exact + byClass.strong, n);
  const probableRate = pct(byClass.probable, n);
  const reviewRate = pct(
    resolveRows.filter((r) => r.review_required || r.resolution_class === "ambiguous").length,
    n
  );
  const noMatchRate = pct(byClass.no_match, n);
  const selfMatchRate = pct(resolveRows.filter((r) => r.self_matched).length, n);

  const elapsedMs = Date.now() - t0;
  const recordsPerMin = n / (elapsedMs / 60000);

  const projectionFactor = 10000 / n;
  const projection = {
    label: "extrapolation_from_cala_validation_sample",
    sample_size: n,
    target: 10000,
    exact_strong: Math.round(((byClass.exact + byClass.strong) / n) * 10000),
    probable: Math.round((byClass.probable / n) * 10000),
    review_required: Math.round(
      (resolveRows.filter((r) => r.review_required || r.resolution_class === "ambiguous").length /
        n) *
        10000
    ),
    ambiguous: Math.round((byClass.ambiguous / n) * 10000),
    missing_rooms_in_sample: roomMissing,
    projected_missing_rooms: Math.round((roomMissing / n) * 10000),
    room_recovery_note: hbxAvailable
      ? `Observed high-conf room fills on enrich subset: ${roomHighConf}/${enrichTargets.length} enriched rows; do not scale HBX yield beyond observed provider availability`
      : "HBX unavailable — room recovery not extrapolated from external provider",
    brand_gaps: Math.round((brandAnalysis.brand_missing_before / n) * 10000),
    human_review_cases: Math.round(
      (resolveRows.filter((r) => r.review_required || r.resolution_class === "ambiguous").length /
        n) *
        10000
    ),
    caveat:
      "Identity rates may scale; external enrichment yield is capped by HBX quota/coverage and must not be linearly extrapolated from TEST quota runs.",
  };

  // Go/No-Go
  let recommendation = "GO_WITH_MATCH_REMEDIATION_FIRST";
  let recommendationWhy = "";
  if (selfMatchRate < 70 || resolutionRate < 60) {
    recommendation = "NO_GO_ARCHITECTURE_REWORK_REQUIRED";
    recommendationWhy =
      "Self-match / exact+strong rates too low for trusted census automation.";
  } else if (!hbxAvailable && roomMissing / n > 0.3) {
    recommendation = "GO_WITH_PROVIDER_EXPANSION_FIRST";
    recommendationWhy =
      "Identity layer is usable, but external enrichment (esp. rooms) blocked by provider availability; expand/entitle sources before auto-accept design.";
  } else if (possible_duplicate + probable_duplicate + byClass.ambiguous > n * 0.15) {
    recommendation = "GO_WITH_MATCH_REMEDIATION_FIRST";
    recommendationWhy =
      "Resolution works, but duplicate/ambiguity burden is high enough that auto-accept would be unsafe without remediation.";
  } else if (
    hbxAvailable &&
    roomHighConf / Math.max(1, roomMissing) >= 0.25 &&
    resolutionRate >= 80
  ) {
    recommendation = "GO_AUTO_ACCEPT_DESIGN";
    recommendationWhy =
      "Strong identity + meaningful high-confidence enrichment observed; design gated auto-accept next (still no blind writes).";
  } else {
    recommendation = "GO_WITH_MATCH_REMEDIATION_FIRST";
    recommendationWhy =
      "Solid foundation; tighten self-match/dedupe and conflict handling before designing auto-accept.";
  }

  const summary = {
    marker: "DEALALITY_HOTEL_INTELLIGENCE_CALA_VALIDATION_COMPLETE",
    safety: {
      airtable_writes: 0,
      migrations: 0,
      schema_changes: 0,
      brand_explorer_writes: 0,
      secrets_exposed: false,
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
    },
    live_baseline: {
      universe: universePresence,
      cala: calaPresence,
      country_distribution: countryDist.slice(0, 20),
    },
    sample: sampleDef,
    resolution: {
      counts: byClass,
      percentages: Object.fromEntries(
        Object.entries(byClass).map(([k, v]) => [k, pct(v, n)])
      ),
      resolution_rate_exact_plus_strong: resolutionRate,
      probable_rate: probableRate,
      review_rate: reviewRate,
      no_match_rate: noMatchRate,
      self_match_rate: selfMatchRate,
    },
    duplicate_risk: { safe_unique, possible_duplicate, probable_duplicate, ambiguous_identity },
    field_stats: fieldStats,
    room_report: roomReport,
    brand_analysis: brandAnalysis,
    confidence: confidenceDist,
    review: { issue_counts: issueCounts, top_examples: topReviews.slice(0, 20) },
    hotelbeds: {
      status: hbxStatus,
      available: hbxAvailable,
      calls: hbxCalls,
      quota_events: quotaEvents,
      failed_calls: failedCalls,
      enrich_targets: enrichTargets.length,
      incremental_value: hbxAvailable
        ? {
            room_candidates_for_missing: roomFoundForMissing,
            room_high_confidence: roomHighConf,
            field_candidates: Object.fromEntries(
              fieldKeys.map((fk) => [fk, fieldStats[fk].candidate_found])
            ),
          }
        : {
            note: "Provider unavailable/quota — external enrichment validation limited; resolution_validation still valid",
          },
    },
    performance: {
      sample_size: n,
      total_runtime_ms: elapsedMs,
      total_runtime_min: Math.round((elapsedMs / 60000) * 100) / 100,
      records_per_minute: Math.round(recordsPerMin * 10) / 10,
      provider_calls: hbxCalls,
      hbx_calls: hbxCalls,
      quota_events: quotaEvents,
      failed_calls: failedCalls,
      retry_events: 0,
      avg_resolve_latency_ms: Math.round(resolveLatencySum / Math.max(1, n)),
      avg_enrich_latency_ms: enrichRows.length
        ? Math.round(enrichLatencySum / enrichRows.length)
        : null,
      scale_notes: {
        "1000": `~${Math.round(1000 / recordsPerMin)} min at observed resolve throughput (enrich extra)`,
        "10000": `~${Math.round(10000 / recordsPerMin)} min resolve-only at observed rate`,
        "50000": `~${Math.round(50000 / recordsPerMin)} min resolve-only; requires batching + provider budgets`,
      },
    },
    auto_accept_potential: autoAccept,
    projection_10k: projection,
    recommendation,
    recommendation_why: recommendationWhy,
    highest_value_next_step: null, // filled below
  };

  // Highest-value next step (exactly one)
  if (recommendation === "GO_WITH_PROVIDER_EXPANSION_FIRST") {
    summary.highest_value_next_step =
      "Entitle LIVE Hotelbeds Content API (or add one licensed rooms/geo source) and re-run the same CALA sample enrich slice — identity is ahead of external fill yield.";
  } else if (recommendation === "GO_WITH_MATCH_REMEDIATION_FIRST") {
    summary.highest_value_next_step =
      "Add a self-match / same-record identity lock + census-internal duplicate detector (same name+city+geo clusters) and re-measure ambiguous/possible_duplicate rates on this frozen sample.";
  } else if (recommendation === "GO_AUTO_ACCEPT_DESIGN") {
    summary.highest_value_next_step =
      "Design a gated auto-accept policy for high-confidence non-conflicting fields (rooms/website/phone) with dry-run apply plans only — still no production writes until steward sign-off.";
  } else {
    summary.highest_value_next_step =
      "Rework resolve scoring to require self-identification against the source census_record_id before treating external candidates as matches; re-run this validation.";
  }

  writeJson(path.join(OUT_DIR, "12-summary.json"), summary);

  // Markdown report
  const md = buildMarkdown(summary, spot, preflight);
  fs.writeFileSync(path.join(OUT_DIR, "CALA_VALIDATION_REPORT.md"), md, "utf8");

  console.log("\nDEALALITY_HOTEL_INTELLIGENCE_CALA_VALIDATION_COMPLETE");
  console.log(`Report: ${path.join(OUT_DIR, "CALA_VALIDATION_REPORT.md")}`);
  console.log(
    JSON.stringify(
      {
        sample: n,
        exact: byClass.exact,
        strong: byClass.strong,
        probable: byClass.probable,
        ambiguous: byClass.ambiguous,
        no_match: byClass.no_match,
        self_match_rate: selfMatchRate,
        hbx: hbxStatus.status,
        recommendation,
      },
      null,
      2
    )
  );
}

function buildMarkdown(summary, spot, preflight) {
  const r = summary.resolution;
  const f = summary.field_stats;
  const lines = [];
  lines.push(`# Dealality Hotel Intelligence — CALA Validation Report`);
  lines.push("");
  lines.push(`**Marker:** \`DEALALITY_HOTEL_INTELLIGENCE_CALA_VALIDATION_COMPLETE\``);
  lines.push(`**Generated:** ${new Date().toISOString()}`);
  lines.push("");
  lines.push(`## 1. Safety`);
  lines.push("");
  lines.push("```");
  lines.push(`Airtable writes: ${summary.safety.airtable_writes}`);
  lines.push(`Migrations: ${summary.safety.migrations}`);
  lines.push(`Schema changes: ${summary.safety.schema_changes}`);
  lines.push(`Brand Explorer writes: ${summary.safety.brand_explorer_writes}`);
  lines.push(`Secrets exposed: ${summary.safety.secrets_exposed ? "yes" : "no"}`);
  lines.push("```");
  lines.push("");
  lines.push(`## 2. Live Census Baseline`);
  lines.push("");
  lines.push(`- Universe records: **${summary.live_baseline.universe.total}**`);
  lines.push(`- CALA records: **${summary.live_baseline.cala.total}**`);
  lines.push(
    `- CALA rooms: ${summary.live_baseline.cala.with_rooms} present / ${summary.live_baseline.cala.missing_rooms} missing`
  );
  lines.push(
    `- CALA brand: ${summary.live_baseline.cala.with_brand} present / ${summary.live_baseline.cala.missing_brand} missing`
  );
  lines.push(
    `- CALA coords: ${summary.live_baseline.cala.with_coords} present / ${summary.live_baseline.cala.missing_coords} missing`
  );
  lines.push(
    `- CALA website: ${summary.live_baseline.cala.with_website} present / ${summary.live_baseline.cala.missing_website} missing`
  );
  lines.push("");
  lines.push(`Top countries: ${summary.live_baseline.country_distribution
    .slice(0, 10)
    .map((x) => `${x.name} (${x.count})`)
    .join(", ")}`);
  lines.push("");
  lines.push(`## 3. Validation Sample`);
  lines.push("");
  lines.push(`- Size: **${summary.sample.actual}** (target ${summary.sample.target})`);
  lines.push(`- Seed: \`${summary.sample.seed}\``);
  lines.push(`- Countries: ${JSON.stringify(summary.sample.country_counts)}`);
  lines.push(`- Quality buckets: ${JSON.stringify(summary.sample.quality_counts)}`);
  lines.push("");
  lines.push(`## 4. Resolution Results`);
  lines.push("");
  lines.push(`| Class | Count | % |`);
  lines.push(`| --- | ---: | ---: |`);
  for (const k of ["exact", "strong", "probable", "ambiguous", "no_match", "error"]) {
    lines.push(`| ${k} | ${r.counts[k]} | ${r.percentages[k]}% |`);
  }
  lines.push("");
  lines.push(`- Exact+Strong rate: **${r.resolution_rate_exact_plus_strong}%**`);
  lines.push(`- Self-match rate: **${r.self_match_rate}%**`);
  lines.push(`- Probable: ${r.probable_rate}% · Review: ${r.review_rate}% · No match: ${r.no_match_rate}%`);
  lines.push("");
  lines.push(`## 5. Duplicate Risk`);
  lines.push("");
  lines.push(`| Risk | Count |`);
  lines.push(`| --- | ---: |`);
  lines.push(`| safe_unique | ${summary.duplicate_risk.safe_unique} |`);
  lines.push(`| possible_duplicate | ${summary.duplicate_risk.possible_duplicate} |`);
  lines.push(`| probable_duplicate | ${summary.duplicate_risk.probable_duplicate} |`);
  lines.push(`| ambiguous_identity | ${summary.duplicate_risk.ambiguous_identity} |`);
  lines.push("");
  lines.push(`## 6. Enrichment Yield`);
  lines.push("");
  lines.push(
    `_External candidate metrics are measured on the HBX enrich subset when provider available; present/missing before cover the full sample._`
  );
  lines.push("");
  lines.push(
    `| Field | Present Before | Missing Before | Candidate Found | High Confidence | Conflict | Still Missing |`
  );
  lines.push(`| --- | ---: | ---: | ---: | ---: | ---: | ---: |`);
  for (const [fk, s] of Object.entries(f)) {
    lines.push(
      `| ${fk} | ${s.present_before} | ${s.missing_before} | ${s.candidate_found} | ${s.high_confidence} | ${s.conflict} | ${s.still_missing} |`
    );
  }
  lines.push("");
  lines.push(`## 7. Room Count`);
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(summary.room_report, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(`## 8. Brand / Parent`);
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(summary.brand_analysis, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(`## 9. Confidence`);
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(summary.confidence, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(`## 10. Review Queue`);
  lines.push("");
  lines.push(`Issue counts: ${JSON.stringify(summary.review.issue_counts)}`);
  lines.push("");
  lines.push(`Top examples: see \`10-review-queue.json\` (${summary.review.top_examples.length} listed).`);
  lines.push("");
  lines.push(`## 11. Hotelbeds`);
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(summary.hotelbeds, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(`## 12. Performance`);
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(summary.performance, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(`## 13. 10,000-Hotel Projection`);
  lines.push("");
  lines.push("```");
  lines.push(JSON.stringify(summary.projection_10k, null, 2));
  lines.push("```");
  lines.push("");
  lines.push(`## 14. Go / No-Go Recommendation`);
  lines.push("");
  lines.push(`**${summary.recommendation}**`);
  lines.push("");
  lines.push(summary.recommendation_why);
  lines.push("");
  lines.push(`## 15. Highest-Value Next Step`);
  lines.push("");
  lines.push(summary.highest_value_next_step);
  lines.push("");
  lines.push(`## Spot checks (summary)`);
  lines.push("");
  lines.push(`Exact: ${spot.exact.length}, Strong: ${spot.strong.length}, Probable: ${spot.probable.length}, Ambiguous: ${spot.ambiguous.length}, Room conflicts: ${spot.room_count_conflicts.length}`);
  lines.push("");
  lines.push(`Artifacts under \`reports/hotel-intelligence/${RUN_ID}/\`.`);
  lines.push("");
  return lines.join("\n");
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      module: "hotel-intelligence-cala-validation",
      error: String(err?.message || err).slice(0, 500),
      stack: String(err?.stack || "").split("\n").slice(0, 5),
    })
  );
  process.exit(1);
});
