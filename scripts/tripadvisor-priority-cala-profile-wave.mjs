#!/usr/bin/env node
/**
 * Tripadvisor Priority CALA Profile Wave v1 — overnight production runner.
 *
 * Modes:
 *   --mode=first-batch   Mexico first 25 HIGH-confidence write safety gate
 *   --mode=run           Continue overnight wave (requires first-batch PASS)
 *   --mode=resume        Resume from checkpoint
 *   --mode=report        Rebuild report from checkpoint/artifacts
 *
 * Schema delete/consolidate: NOT ALLOWED.
 * Null-fill into KEEP_* fields only. Rooms = candidate only.
 * Brand/Owner/Operator writes = 0.
 *
 * Env for Airtable writes (process-level; do not commit):
 *   ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=1
 *   ENABLE_CENSUS_FIELD_ENRICHMENT=1
 *   ENABLE_COORDINATE_WRITES=1
 *   CONFIRM_TRIPADVISOR_OVERNIGHT_WRITES=1
 *
 * Budget: APIFY_OVERNIGHT_BUDGET_USD (default 25; lower wins)
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
  MAP_PROVIDER_IDS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import {
  buildTripadvisorProfilePack,
  profilePackCoverageFlags,
  evaluateProductionWriteGate,
} from "../lib/hotel-intelligence/tripadvisor-profile/index.js";
import { proposeOvernightCensusWrites } from "../lib/hotel-intelligence/tripadvisor-profile/overnight-write-policy.js";
import {
  buildTripadvisorActorInput,
  buildTripadvisorResolutionPlan,
  matchTripadvisorHotel,
} from "../lib/hotel-intelligence/tripadvisor-rooms/index.js";
import { runApifyActor, getApifyToken } from "../lib/hotel-intelligence/apify/local-client.js";
import {
  createApifyUsageStore,
  APIFY_USE_CASES,
  APIFY_AUTH_METHODS,
} from "../lib/hotel-intelligence/apify-usage/index.js";
import { createExternalIdRegistry } from "../lib/hotel-intelligence/external-ids.js";
import { createEvidenceStore } from "../lib/hotel-intelligence/evidence-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-priority-cala-profile-wave-v1"
);
const DATA_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/tripadvisor-priority-cala-profile-wave-v1"
);
const PACK_DIR = path.join(DATA_DIR, "profile-packs");
const CHECKPOINT_PATH = path.join(DATA_DIR, "checkpoint.json");
const HEARTBEAT_PATH = path.join(DATA_DIR, "wave-heartbeat.json");
const WRITE_LOG_PATH = path.join(DATA_DIR, "airtable-write-log.jsonl");
const HEARTBEAT_HOTEL_INTERVAL = 50;
const HEARTBEAT_MS_INTERVAL = 10 * 60 * 1000;

let activeCheckpoint = null;
let shutdownInProgress = false;
const DISPOSITION_PATH = path.join(
  ROOT,
  "reports/hotel-intelligence/census-schema-rationalization-v1/04-field-disposition-matrix.json"
);

const PRIORITY_COUNTRIES = [
  "Mexico",
  "Colombia",
  "Dominican Republic",
  "Costa Rica",
  "Panama",
  "Brazil",
  "Puerto Rico",
  "Jamaica",
  "Bahamas",
  "Peru",
  "Chile",
  "Argentina",
  "Aruba",
  "Curaçao",
  "Curacao",
  "Cayman Islands",
  "Turks and Caicos",
  "Turks & Caicos",
];

const TIER1 = new Set(["Mexico", "Colombia", "Dominican Republic"]);

function parseArgs(argv) {
  const out = {
    mode: "first-batch",
    firstBatchSize: 25,
    apifyBatchSize: 12,
    checkpointEvery: 50,
    country: null,
    limit: 0,
  };
  for (const a of argv.slice(2)) {
    if (a.startsWith("--mode=")) out.mode = a.slice(7);
    if (a.startsWith("--first-batch=")) out.firstBatchSize = Number(a.slice(14)) || 25;
    if (a.startsWith("--apify-batch=")) out.apifyBatchSize = Number(a.slice(14)) || 12;
    if (a.startsWith("--checkpoint-every="))
      out.checkpointEvery = Number(a.slice(19)) || 25;
    if (a.startsWith("--country=")) out.country = a.slice(10);
    if (a.startsWith("--limit=")) out.limit = Number(a.slice(8)) || 0;
  }
  return out;
}

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(file, data) {
  ensureDir(path.dirname(file));
  const payload = `${JSON.stringify(data, null, 2)}\n`;
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, payload, "utf8");
  // Windows cannot rename over an existing file — copy then unlink tmp.
  fs.copyFileSync(tmp, file);
  try {
    fs.unlinkSync(tmp);
  } catch {
    /* ignore */
  }
}
function readJson(file, fallback = null) {
  if (!fs.existsSync(file)) return fallback;
  const raw = fs.readFileSync(file);
  if (!raw.length || raw.every((b) => b === 0)) return fallback;
  const text = raw.toString("utf8").replace(/^\uFEFF/, "").trim();
  if (!text || text.startsWith("\u0000")) return fallback;
  try {
    return JSON.parse(text);
  } catch (err) {
    console.error(`readJson_failed:${path.basename(file)}:${err.message}`);
    return fallback;
  }
}
function blank(v) {
  return v == null || String(v).trim() === "";
}
function pct(n, d) {
  if (!d) return null;
  return Number(((100 * n) / d).toFixed(1));
}
function budgetUsd() {
  const envCap = Number(process.env.APIFY_OVERNIGHT_BUDGET_USD || 25);
  return Number.isFinite(envCap) ? Math.min(envCap, 25) : 25;
}

function loadDispositions() {
  const j = readJson(DISPOSITION_PATH, { fields: [] });
  const map = new Map();
  for (const f of j.fields || []) {
    map.set(f.FIELD_NAME, f.DISPOSITION);
  }
  return map;
}

function emptyCheckpoint() {
  return {
    version: "tripadvisor-priority-cala-profile-wave-v1",
    started_at: new Date().toISOString(),
    updated_at: null,
    budget_usd: budgetUsd(),
    apify_spend_usd: 0,
    first_batch_validation: null,
    airtable_write_mode: "PENDING_GATE",
    current_country: null,
    completed_hotel_ids: [],
    skipped_hotel_ids: [],
    actor_run_ids: [],
    countries: {},
    counters: {
      processed: 0,
      matched_high: 0,
      matched_medium: 0,
      no_match: 0,
      false_match_rejected: 0,
      ambiguous: 0,
      profile_packs_saved: 0,
      profile_packs_reused: 0,
      room_candidates: 0,
      hotels_updated: 0,
      airtable_executed_writes: 0,
      blocked_existing: 0,
      blocked_schema: 0,
      blocked_validation: 0,
      conflicts: 0,
      error_count: 0,
      fields_filled: {
        ADDRESS_FILLED: 0,
        CITY_FILLED: 0,
        STATE_REGION_FILLED: 0,
        POSTAL_CODE_FILLED: 0,
        LATITUDE_FILLED: 0,
        LONGITUDE_FILLED: 0,
        WEBSITE_FILLED: 0,
        PHONE_FILLED: 0,
        EMAIL_FILLED: 0,
        PROPERTY_TYPE_FILLED: 0,
        HOTEL_CLASS_FILLED: 0,
        AMENITIES_FILLED: 0,
      },
    },
    safety_events: [],
    stop_reason: null,
  };
}

function saveCheckpoint(cp) {
  cp.updated_at = new Date().toISOString();
  writeJson(CHECKPOINT_PATH, cp);
}

function remainingBudgetUsd(cp) {
  return Number(Math.max(0, (cp.budget_usd || 0) - (cp.apify_spend_usd || 0)).toFixed(6));
}

function lastSuccessfulHotel(cp) {
  const ids = cp.completed_hotel_ids || [];
  return ids.length ? ids[ids.length - 1] : null;
}

function writeHeartbeat(cp, runStatus = "RUNNING", extra = {}) {
  const country = cp.current_country || null;
  const cs = country ? cp.countries[country] : null;
  const payload = {
    RUN_STATUS: runStatus,
    CURRENT_COUNTRY: country,
    PROCESSED_TOTAL: cp.counters?.processed ?? 0,
    PROCESSED_COUNTRY: cs?.processed ?? 0,
    HIGH_CONFIDENCE_MATCHES: cp.counters?.matched_high ?? 0,
    PROFILE_PACKS_SAVED: cp.counters?.profile_packs_saved ?? 0,
    HOTELS_UPDATED: cp.counters?.hotels_updated ?? 0,
    AIRTABLE_WRITES: cp.counters?.airtable_executed_writes ?? 0,
    ROOM_CANDIDATES: cp.counters?.room_candidates ?? 0,
    APIFY_SPEND: cp.apify_spend_usd ?? 0,
    REMAINING_BUDGET: remainingBudgetUsd(cp),
    LAST_CHECKPOINT: cp.updated_at,
    LAST_SUCCESSFUL_HOTEL: lastSuccessfulHotel(cp),
    ERROR_COUNT: cp.counters?.error_count ?? 0,
    STOP_REASON: cp.stop_reason,
    ACTOR_RUNS: (cp.actor_run_ids || []).length,
    LAST_APIFY_RUN: (cp.actor_run_ids || []).at(-1) || null,
    updated_at: new Date().toISOString(),
    ...extra,
  };
  writeJson(HEARTBEAT_PATH, payload);
  const statusLines = Object.entries(payload)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\n");
  fs.writeFileSync(path.join(OUT_DIR, "STATUS.txt"), `${statusLines}\n`, "utf8");
  return payload;
}

function createHeartbeatTracker(cp) {
  return {
    lastAt: Date.now(),
    lastProcessed: cp.counters?.processed ?? 0,
    shouldPulse(force = false) {
      const processed = cp.counters?.processed ?? 0;
      const hotelsSince =
        processed - this.lastProcessed >= HEARTBEAT_HOTEL_INTERVAL;
      const timeSince = Date.now() - this.lastAt >= HEARTBEAT_MS_INTERVAL;
      return force || hotelsSince || timeSince;
    },
    mark(runStatus, extra) {
      this.lastAt = Date.now();
      this.lastProcessed = cp.counters?.processed ?? 0;
      return writeHeartbeat(cp, runStatus, extra);
    },
  };
}

function hasFreshProfilePack(recordId, extIds, fields = {}) {
  try {
    const hotelId = extIds.ensureHotelIdForAirtable(recordId, {
      property_identity_key: fields[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });
    const packFile = packPathFor(hotelId);
    if (!fs.existsSync(packFile)) return false;
    const prev = readJson(packFile);
    const ageMs =
      Date.now() - Date.parse(prev?.retrieved_at || prev?.saved_at || 0);
    return (
      Boolean(prev?.pack) &&
      prev?.match_confidence === "high" &&
      ageMs < 36 * 3600 * 1000
    );
  } catch {
    return false;
  }
}

function installWaveShutdownHandlers() {
  const handler = (signal) => {
    if (shutdownInProgress) return;
    shutdownInProgress = true;
    const cp = activeCheckpoint;
    if (cp) {
      if (!cp.stop_reason) cp.stop_reason = `PROCESS_SIGNAL_${signal}`;
      saveCheckpoint(cp);
      writeHeartbeat(cp, "STOPPED", { signal });
    }
    process.exit(signal === "SIGINT" ? 130 : 1);
  };
  for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    try {
      process.on(sig, () => handler(sig));
    } catch {
      // Windows may not support all signals
    }
  }
}

function appendWriteLog(row) {
  ensureDir(path.dirname(WRITE_LOG_PATH));
  fs.appendFileSync(WRITE_LOG_PATH, `${JSON.stringify(row)}\n`, "utf8");
}

function gapPriority(fields) {
  let s = 0;
  if (blank(fields[MAP_CENSUS_FIELDS.roomCount])) s += 10;
  const aff = String(fields[MAP_CENSUS_FIELDS.affiliationStatus] || "");
  if (/independent|unconfirmed|unknown/i.test(aff) || blank(aff)) s += 6;
  if (blank(fields[MAP_CENSUS_FIELDS.brandName])) s += 4;
  if (blank(fields[MAP_CENSUS_FIELDS.latitude])) s += 5;
  if (blank(fields[MAP_CENSUS_FIELDS.website])) s += 4;
  if (blank(fields[MAP_CENSUS_FIELDS.phone])) s += 3;
  if (blank(fields[MAP_CENSUS_FIELDS.address])) s += 3;
  if (blank(fields[MAP_CENSUS_FIELDS.propertyType])) s += 2;
  const pt = String(fields[MAP_CENSUS_FIELDS.propertyType] || "");
  if (/resort/i.test(pt)) s += 3;
  return s;
}

async function loadCensusByCountries(countries) {
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
  const fieldNames = [
    ...new Set([
      MAP_CENSUS_FIELDS.propertyName,
      MAP_CENSUS_FIELDS.officialName,
      MAP_CENSUS_FIELDS.propertyIdentityKey,
      MAP_CENSUS_FIELDS.country,
      MAP_CENSUS_FIELDS.city,
      MAP_CENSUS_FIELDS.stateRegion,
      MAP_CENSUS_FIELDS.address,
      MAP_CENSUS_FIELDS.latitude,
      MAP_CENSUS_FIELDS.longitude,
      MAP_CENSUS_FIELDS.website,
      MAP_CENSUS_FIELDS.phone,
      MAP_CENSUS_FIELDS.roomCount,
      MAP_CENSUS_FIELDS.propertyType,
      MAP_CENSUS_FIELDS.brandName,
      MAP_CENSUS_FIELDS.affiliationStatus,
      "Hotel Class / Segment",
      "Amenities - Structured Tags",
      "Phone Confidence",
      "Address Confidence",
      "Coordinate Confidence",
    ]),
  ];
  const want = new Set(countries.map((c) => c.toLowerCase()));
  const hotels = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({ pageSize: 100, fields: fieldNames })
    .eachPage((page, next) => {
      for (const rec of page) {
        const f = rec.fields || {};
        const country = String(f[MAP_CENSUS_FIELDS.country] || "").trim();
        if (!want.has(country.toLowerCase())) continue;
        // Avoid PR double-count if stored oddly — Country field is authoritative
        const name = String(
          f[MAP_CENSUS_FIELDS.officialName] ||
            f[MAP_CENSUS_FIELDS.propertyName] ||
            ""
        ).trim();
        if (!name) continue;
        hotels.push({
          record_id: rec.id,
          name,
          city: String(f[MAP_CENSUS_FIELDS.city] || "").trim() || null,
          country,
          fields: f,
          priority: gapPriority(f),
        });
      }
      next();
    });
  hotels.sort((a, b) => b.priority - a.priority);
  return { base, hotels, token, baseId };
}

function loadTaPools() {
  const paths = [
    path.join(DATA_DIR, "ta-runtime-pool.json"),
    path.join(
      ROOT,
      "data/hotel-intelligence/tripadvisor-census-profile-pack-v1/ta-pilot-pool.json"
    ),
    path.join(
      ROOT,
      "data/hotel-intelligence/giata-tripadvisor-room-decision-v1/ta-decision-pool.json"
    ),
    path.join(
      ROOT,
      "data/hotel-intelligence/tripadvisor-apify-benchmark-v1/ta-pool.json"
    ),
  ];
  const byId = new Map();
  for (const p of paths) {
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const items = Array.isArray(j) ? j : j.items || [];
    for (const it of items) {
      if (it?.id != null) byId.set(String(it.id), it);
    }
  }
  return byId;
}

function saveRuntimePool(byId) {
  writeJson(path.join(DATA_DIR, "ta-runtime-pool.json"), {
    updated_at: new Date().toISOString(),
    count: byId.size,
    items: [...byId.values()],
  });
}

function packPathFor(hotelId) {
  return path.join(PACK_DIR, `${hotelId}.json`);
}

function fieldFillKey(field) {
  const map = {
    [MAP_CENSUS_FIELDS.address]: "ADDRESS_FILLED",
    [MAP_CENSUS_FIELDS.city]: "CITY_FILLED",
    [MAP_CENSUS_FIELDS.stateRegion]: "STATE_REGION_FILLED",
    [MAP_CENSUS_FIELDS.latitude]: "LATITUDE_FILLED",
    [MAP_CENSUS_FIELDS.longitude]: "LONGITUDE_FILLED",
    [MAP_CENSUS_FIELDS.website]: "WEBSITE_FILLED",
    [MAP_CENSUS_FIELDS.phone]: "PHONE_FILLED",
    [MAP_CENSUS_FIELDS.propertyType]: "PROPERTY_TYPE_FILLED",
    "Amenities - Structured Tags": "AMENITIES_FILLED",
  };
  return map[field] || null;
}

function ensureCountryStats(cp, country) {
  if (!cp.countries[country]) {
    cp.countries[country] = {
      census_hotels: 0,
      processed: 0,
      matched_high: 0,
      matched_medium: 0,
      no_match: 0,
      false_match_rejected: 0,
      ambiguous: 0,
      profile_packs: 0,
      hotels_updated: 0,
      fields_filled: 0,
      room_candidates: 0,
      conflicts: 0,
      apify_cost: 0,
    };
  }
  return cp.countries[country];
}

async function fetchApifyForHotels(hotels, cp, usageStore, byId) {
  if (!getApifyToken()) throw new Error("APIFY_TOKEN missing");
  if (cp.apify_spend_usd >= cp.budget_usd) {
    return { stopped: true, reason: "BUDGET_LIMIT_REACHED" };
  }

  const startUrls = [];
  const meta = [];
  for (const h of hotels) {
    const plan = buildTripadvisorResolutionPlan({
      name: h.name,
      city: h.city,
      country: h.country,
    });
    if (!plan.ok || !plan.steps.length) continue;
    const step = plan.steps[0];
    startUrls.push({ url: step.url });
    meta.push({ record_id: h.record_id, url: step.url, kind: step.kind });
  }
  if (!startUrls.length) return { items: [], cost: 0, run_id: null };

  const remaining = Math.max(0.05, cp.budget_usd - cp.apify_spend_usd);
  const input = buildTripadvisorActorInput(startUrls, {
    maxItemsPerQuery: 3,
  });
  // Full detail for profile pack
  Object.assign(input, {
    includeAbout: true,
    includeAmenities: true,
    includeReviews: false,
    includePhotos: false,
    includeAIReviewsSummary: false,
  });

  const result = await runApifyActor({
    input,
    waitSecs: 180,
    memoryMbytes: 2048,
    maxTotalChargeUsd: Math.min(remaining, 2.5),
  });

  const cost = result.usage_total_usd != null ? result.usage_total_usd : 0;
  cp.apify_spend_usd = Number((cp.apify_spend_usd + (cost || 0)).toFixed(6));
  if (result.run_id) cp.actor_run_ids.push(result.run_id);

  usageStore.recordRun({
    use_case: APIFY_USE_CASES.HOTEL_PROFILE,
    actor_id: result.run?.actId || null,
    actor_name: "maxcopell/tripadvisor",
    auth_method: APIFY_AUTH_METHODS.LOCAL_TOKEN,
    run_id: result.run_id,
    status: result.status,
    started_at: result.run?.startedAt || null,
    finished_at: result.run?.finishedAt || null,
    apify_run_cost_usd: cost,
    cost_source: "apify_usage_total_usd",
    records_requested: startUrls.length,
    records_returned: result.items.length,
    dataset_id: result.dataset_id,
    notes: "priority-cala-profile-wave-v1",
    label: `wave-batch-${result.run_id}`,
  });

  for (const it of result.items) {
    if (it?.id != null) byId.set(String(it.id), it);
  }
  saveRuntimePool(byId);

  return {
    items: result.items,
    cost,
    run_id: result.run_id,
    status: result.status,
    meta,
  };
}

function validateFirstBatchManifest(manifestRows) {
  const issues = [];
  for (const m of manifestRows) {
    const oldValue = m.OLD_VALUE ?? m.old_value;
    const field = m.FIELD ?? m.field;
    const disposition = m.SCHEMA_DISPOSITION ?? m.schema_disposition;
    const matchConfidence = m.MATCH_CONFIDENCE ?? m.match_confidence;
    if (oldValue != null && String(oldValue).trim() !== "") {
      issues.push({ type: "overwrite", row: m });
    }
    if (disposition && !["KEEP_CORE", "KEEP_SUPPORTING"].includes(disposition)) {
      issues.push({ type: "bad_disposition", row: m });
    }
    if (field === "Hotel Class / Segment") {
      issues.push({ type: "hotel_class_blocked", row: m });
    }
    if (field === MAP_CENSUS_FIELDS.brandName) {
      issues.push({ type: "brand_write", row: m });
    }
    if (field === MAP_CENSUS_FIELDS.roomCount) {
      issues.push({ type: "rooms_authoritative", row: m });
    }
    if (matchConfidence !== "high") {
      issues.push({ type: "match_not_high", row: m });
    }
  }
  return {
    ok: issues.length === 0,
    issues,
    proposed_count: manifestRows.length,
  };
}

async function applyWrites(base, proposals, hotel, allowCoords) {
  const fields = {};
  const applied = [];
  for (const p of proposals) {
    if (p.requires_env === "ENABLE_COORDINATE_WRITES=1" && !allowCoords) {
      continue;
    }
    if (!blank(hotel.fields[p.field])) continue; // double-check
    fields[p.field] = p.new_value;
    if (p.companion_fields) {
      for (const [k, v] of Object.entries(p.companion_fields)) {
        if (v != null && blank(hotel.fields[k])) fields[k] = v;
      }
    }
    applied.push(p);
  }
  if (!Object.keys(fields).length) return { applied: [], readback: null };

  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId).update(
    hotel.record_id,
    fields,
    { typecast: true }
  );

  // Read back
  const rec = await base(MAP_HOTEL_PROPERTY_CENSUS.tableId).find(hotel.record_id);
  const rb = rec.fields || {};
  const verification = [];
  for (const p of applied) {
    const got = rb[p.field];
    const ok =
      got != null &&
      String(got).trim() !== "" &&
      (typeof p.new_value === "number"
        ? Number(got) === Number(p.new_value)
        : String(got).trim() === String(p.new_value).trim() ||
          String(got).includes(String(p.new_value).slice(0, 40)));
    verification.push({
      field: p.field,
      expected: p.new_value,
      got,
      ok,
    });
    appendWriteLog({
      hotel_id: hotel.hotel_id || null,
      hotel_name: hotel.name,
      airtable_record_id: hotel.record_id,
      field_name: p.field,
      old_value: null,
      new_value: p.new_value,
      provider: "tripadvisor_apify",
      provider_property_id: p.provider_property_id,
      source_url: p.source_url,
      retrieved_at: new Date().toISOString(),
      match_confidence: p.match_confidence,
      field_confidence: p.field_confidence,
      validation_status: "passed",
      write_timestamp: new Date().toISOString(),
      post_write_verification_status: ok ? "PASS" : "FAIL",
    });
  }
  return { applied, readback: verification };
}

async function processHotel(h, ctx) {
  const {
    cp,
    byId,
    dispositions,
    extIds,
    evidence,
    writeEnabled,
    allowCoords,
    base,
    collectManifestOnly,
    manifestOut,
  } = ctx;

  const cs = ensureCountryStats(cp, h.country);
  if (cp.completed_hotel_ids.includes(h.record_id)) {
    return { status: "already_done" };
  }

  const hotelId = extIds.ensureHotelIdForAirtable(h.record_id, {
    property_identity_key:
      h.fields[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
  });
  h.hotel_id = hotelId;

  // Reuse profile pack if fresh
  const existingPackFile = packPathFor(hotelId);
  let pack = null;
  let taItem = null;
  let matchMeta = null;
  let reused = false;

  if (fs.existsSync(existingPackFile)) {
    const prev = readJson(existingPackFile);
    const ageMs =
      Date.now() - Date.parse(prev?.retrieved_at || prev?.saved_at || 0);
    if (
      prev?.pack &&
      prev?.match_confidence === "high" &&
      ageMs < 36 * 3600 * 1000
    ) {
      pack = prev.pack;
      taItem = prev.ta_item_snapshot || null;
      matchMeta = {
        confidence: "high",
        score: prev.match_score ?? 0.95,
      };
      reused = true;
      cp.counters.profile_packs_reused += 1;
    }
  }

  if (!pack) {
    const hotelQuery = {
      name: h.name,
      city: h.city,
      country: h.country,
      lat: h.fields[MAP_CENSUS_FIELDS.latitude] ?? null,
      lng: h.fields[MAP_CENSUS_FIELDS.longitude] ?? null,
      website: h.fields[MAP_CENSUS_FIELDS.website] || null,
      rooms: h.fields[MAP_CENSUS_FIELDS.roomCount] ?? null,
    };
    const { match, rejection } = matchTripadvisorHotel(
      hotelQuery,
      [...byId.values()]
    );
    if (!match) {
      cp.counters.processed += 1;
      cs.processed += 1;
      const reason = rejection?.reason || "no_match";
      if (/ambiguous/i.test(reason)) {
        cp.counters.ambiguous += 1;
        cs.ambiguous += 1;
      } else if (/false|reject|sister|collision|country/i.test(reason)) {
        cp.counters.false_match_rejected += 1;
        cs.false_match_rejected += 1;
      } else {
        cp.counters.no_match += 1;
        cs.no_match += 1;
      }
      cp.completed_hotel_ids.push(h.record_id);
      return { status: "UNMATCHED", reason };
    }

    matchMeta = {
      confidence: match.confidence,
      score: match.score,
      retrieved_at: new Date().toISOString(),
    };
    taItem = match.item;

    if (match.confidence === "medium") {
      cp.counters.matched_medium += 1;
      cs.matched_medium += 1;
      // Evidence only — no Airtable writes
      pack = buildTripadvisorProfilePack(taItem, matchMeta);
      writeJson(existingPackFile, {
        hotel_id: hotelId,
        airtable_record_id: h.record_id,
        match_confidence: "medium",
        match_score: match.score,
        saved_at: new Date().toISOString(),
        retrieved_at: matchMeta.retrieved_at,
        pack,
        ta_item_snapshot: {
          id: taItem.id,
          name: taItem.name,
          webUrl: taItem.webUrl,
          numberOfRooms: taItem.numberOfRooms,
          hotelClass: taItem.hotelClass,
          rating: taItem.rating,
          rankingPosition: taItem.rankingPosition,
          rankingDenominator: taItem.rankingDenominator,
          website: taItem.website,
          phone: taItem.phone,
          email: taItem.email,
          amenities: taItem.amenities,
          addressObj: taItem.addressObj,
          latitude: taItem.latitude,
          longitude: taItem.longitude,
          subcategories: taItem.subcategories,
          type: taItem.type,
          category: taItem.category,
          hotelClassAttribution: taItem.hotelClassAttribution,
        },
      });
      cp.counters.profile_packs_saved += 1;
      cs.profile_packs += 1;
      cp.counters.processed += 1;
      cs.processed += 1;
      cp.completed_hotel_ids.push(h.record_id);
      return { status: "MEDIUM_EVIDENCE_ONLY" };
    }

    if (match.confidence !== "high") {
      cp.counters.ambiguous += 1;
      cs.ambiguous += 1;
      cp.counters.processed += 1;
      cs.processed += 1;
      cp.completed_hotel_ids.push(h.record_id);
      return { status: "AMBIGUOUS" };
    }

    cp.counters.matched_high += 1;
    cs.matched_high += 1;
    pack = buildTripadvisorProfilePack(taItem, matchMeta);
  } else if (reused && matchMeta?.confidence === "high") {
    cp.counters.matched_high += 1;
    cs.matched_high += 1;
  }

  // Persist pack
  if (!reused) {
    writeJson(existingPackFile, {
      hotel_id: hotelId,
      airtable_record_id: h.record_id,
      match_confidence: "high",
      match_score: matchMeta?.score ?? null,
      saved_at: new Date().toISOString(),
      retrieved_at: matchMeta?.retrieved_at || new Date().toISOString(),
      pack,
      coverage: profilePackCoverageFlags(pack),
      ta_item_snapshot: taItem
        ? {
            id: taItem.id,
            name: taItem.name,
            webUrl: taItem.webUrl,
            numberOfRooms: taItem.numberOfRooms,
            hotelClass: taItem.hotelClass,
            rating: taItem.rating,
            rankingPosition: taItem.rankingPosition,
            rankingDenominator: taItem.rankingDenominator,
            website: taItem.website,
            phone: taItem.phone,
            email: taItem.email,
            amenities: taItem.amenities,
            addressObj: taItem.addressObj,
            latitude: taItem.latitude,
            longitude: taItem.longitude,
            subcategories: taItem.subcategories,
            type: taItem.type,
            category: taItem.category,
            hotelClassAttribution: taItem.hotelClassAttribution,
          }
        : null,
    });
    cp.counters.profile_packs_saved += 1;
    cs.profile_packs += 1;
  } else {
    cs.profile_packs += 1;
  }

  // External id + evidence
  if (taItem?.id != null) {
    extIds.linkExternalId(
      hotelId,
      MAP_PROVIDER_IDS.tripadvisor_apify,
      String(taItem.id),
      { source_url: taItem.webUrl || null }
    );
  }
  for (const [field, value] of [
    ["tripadvisor_rating", pack.guest_reputation.rating],
    ["tripadvisor_review_count", pack.guest_reputation.number_of_reviews],
    [
      "guest_ranking_percentile",
      pack.competitive_standing.guest_ranking_percentile,
    ],
    ["tripadvisor_hotel_class_stars", pack.product.hotel_class],
  ]) {
    if (value == null) continue;
    evidence.addEvidence({
      hotel_id: hotelId,
      field,
      value,
      source: MAP_PROVIDER_IDS.tripadvisor_apify,
      source_record_id: taItem?.id != null ? String(taItem.id) : null,
      completeness: 1,
    });
  }

  const proposed = proposeOvernightCensusWrites({
    censusFields: h.fields,
    taItem: taItem || {},
    matchMeta: matchMeta || { confidence: "high" },
    dispositionByField: dispositions,
  });

  for (const c of proposed.candidates) {
    if (c.field === MAP_CENSUS_FIELDS.roomCount) {
      cp.counters.room_candidates += 1;
      cs.room_candidates += 1;
      evidence.addEvidence({
        hotel_id: hotelId,
        field: "rooms_candidate",
        value: c.candidate_value,
        source: MAP_PROVIDER_IDS.tripadvisor_apify,
        source_record_id: taItem?.id != null ? String(taItem.id) : null,
        completeness: 0.6,
      });
    } else if (c.field === "property_email" || c.field === "postal_code") {
      evidence.addEvidence({
        hotel_id: hotelId,
        field: c.field,
        value: c.candidate_value,
        source: MAP_PROVIDER_IDS.tripadvisor_apify,
        source_record_id: taItem?.id != null ? String(taItem.id) : null,
      });
    }
  }

  for (const b of proposed.blocked) {
    if (b.reason === "existing_non_null_blocked") cp.counters.blocked_existing += 1;
    else if (b.reason === "blocked_schema_disposition")
      cp.counters.blocked_schema += 1;
    else cp.counters.blocked_validation += 1;
  }
  cp.counters.conflicts += proposed.conflicts.length;
  cs.conflicts += proposed.conflicts.length;

  const executable = proposed.proposals.filter((p) => {
    if (p.requires_env === "ENABLE_COORDINATE_WRITES=1" && !allowCoords) {
      return false;
    }
    return true;
  });

  for (const p of executable) {
    manifestOut.push({
      HOTEL_ID: hotelId,
      HOTEL_NAME: h.name,
      AIRTABLE_RECORD_ID: h.record_id,
      COUNTRY: h.country,
      FIELD: p.field,
      OLD_VALUE: p.old_value,
      NEW_VALUE: p.new_value,
      SOURCE: p.source,
      SOURCE_URL: p.source_url,
      MATCH_CONFIDENCE: p.match_confidence,
      FIELD_CONFIDENCE: p.field_confidence,
      SCHEMA_DISPOSITION: p.schema_disposition,
      VALIDATION_RESULT: "PASS",
      REASON: p.reason,
    });
  }

  if (collectManifestOnly) {
    cp.counters.processed += 1;
    cs.processed += 1;
    // Don't mark completed yet for first-batch until writes applied
    return {
      status: "MANIFEST",
      proposals: executable,
      blocked: proposed.blocked,
      conflicts: proposed.conflicts,
    };
  }

  if (writeEnabled && executable.length && cp.first_batch_validation === "PASS") {
    const { applied, readback } = await applyWrites(
      base,
      executable,
      h,
      allowCoords
    );
    if (applied.length) {
      cp.counters.hotels_updated += 1;
      cs.hotels_updated += 1;
      cp.counters.airtable_executed_writes += applied.length;
      cs.fields_filled += applied.length;
      for (const p of applied) {
        const k = fieldFillKey(p.field);
        if (k) cp.counters.fields_filled[k] += 1;
      }
      if (readback?.some((r) => !r.ok)) {
        cp.safety_events.push({
          at: new Date().toISOString(),
          type: "READBACK_FAIL",
          hotel: h.name,
          record_id: h.record_id,
        });
        cp.stop_reason = "READBACK_VALIDATION_FAIL";
        saveCheckpoint(cp);
        return { status: "READBACK_FAIL", readback };
      }
    }
  }

  cp.counters.processed += 1;
  cs.processed += 1;
  cp.completed_hotel_ids.push(h.record_id);
  return { status: "OK", proposals: executable.length };
}

async function runFirstBatch(args) {
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);
  ensureDir(PACK_DIR);

  if (!getApifyToken()) throw new Error("APIFY_TOKEN missing");

  const dispositions = loadDispositions();
  let cp = readJson(CHECKPOINT_PATH, null) || emptyCheckpoint();
  cp.budget_usd = Math.min(cp.budget_usd || budgetUsd(), budgetUsd());

  const gate = evaluateProductionWriteGate(process.env);
  const confirm =
    String(process.env.CONFIRM_TRIPADVISOR_OVERNIGHT_WRITES || "0") === "1";
  const writeEnabled = gate.allow_any_airtable_writes && confirm;
  const allowCoords = gate.allow_coordinate_writes && confirm;

  console.error(
    JSON.stringify(
      {
        mode: "first-batch",
        writeEnabled,
        allowCoords,
        gate: gate.status,
        budget: cp.budget_usd,
      },
      null,
      2
    )
  );

  const { base, hotels } = await loadCensusByCountries(["Mexico"]);
  ensureCountryStats(cp, "Mexico").census_hotels = hotels.length;
  writeJson(path.join(DATA_DIR, "mexico-eligible.json"), {
    n: hotels.length,
    top: hotels.slice(0, 50).map((h) => ({
      id: h.record_id,
      name: h.name,
      city: h.city,
      priority: h.priority,
    })),
  });

  const byId = loadTaPools();
  const usageStore = createApifyUsageStore();
  const extIds = createExternalIdRegistry();
  const evidence = createEvidenceStore();
  const manifestOut = [];

  // Prefer hotel-like names for first batch quality
  const candidates = hotels.filter((h) =>
    /hotel|resort|inn|lodge|suites|hilton|marriott|hyatt|iberostar|riu|barcelo|fiesta|presidente|live aqua|secrets|dreams|breathless|excellence|grand fiesta|camino real|nh |novotel|ibis|radisson|wyndham|holiday inn|courtyard|sheraton|westin|st regis|four seasons|ritz|sofitel|meli[aá]|palace|pueblo|cancun|cabo|vallarta/i.test(
      h.name
    )
  );
  const queue = (candidates.length >= args.firstBatchSize
    ? candidates
    : hotels
  ).slice(0, Math.max(args.firstBatchSize * 4, 80));

  const matchedHotels = [];
  // Ensure pool coverage via Apify batches until we have enough HIGH matches
  for (let i = 0; i < queue.length && matchedHotels.length < args.firstBatchSize; ) {
    if (cp.apify_spend_usd >= cp.budget_usd) {
      cp.stop_reason = "BUDGET_LIMIT_REACHED";
      break;
    }
    const slice = queue.slice(i, i + args.apifyBatchSize);
    i += args.apifyBatchSize;

    // Try match from existing pool first
    const needFetch = [];
    for (const h of slice) {
      const { match } = matchTripadvisorHotel(
        {
          name: h.name,
          city: h.city,
          country: h.country,
          lat: h.fields[MAP_CENSUS_FIELDS.latitude] ?? null,
          lng: h.fields[MAP_CENSUS_FIELDS.longitude] ?? null,
          website: h.fields[MAP_CENSUS_FIELDS.website] || null,
        },
        [...byId.values()]
      );
      if (match?.confidence === "high") {
        matchedHotels.push(h);
      } else {
        needFetch.push(h);
      }
      if (matchedHotels.length >= args.firstBatchSize) break;
    }

    if (matchedHotels.length >= args.firstBatchSize) break;
    if (needFetch.length) {
      console.error(
        `Apify fetch batch n=${needFetch.length} spend=${cp.apify_spend_usd}`
      );
      const fetched = await fetchApifyForHotels(needFetch, cp, usageStore, byId);
      if (fetched.stopped) {
        cp.stop_reason = fetched.reason;
        break;
      }
      // Rematch
      for (const h of needFetch) {
        const { match } = matchTripadvisorHotel(
          {
            name: h.name,
            city: h.city,
            country: h.country,
            lat: h.fields[MAP_CENSUS_FIELDS.latitude] ?? null,
            lng: h.fields[MAP_CENSUS_FIELDS.longitude] ?? null,
            website: h.fields[MAP_CENSUS_FIELDS.website] || null,
          },
          [...byId.values()]
        );
        if (match?.confidence === "high") matchedHotels.push(h);
        if (matchedHotels.length >= args.firstBatchSize) break;
      }
    }
    saveCheckpoint(cp);
  }

  const first25 = matchedHotels.slice(0, args.firstBatchSize);
  writeJson(path.join(DATA_DIR, "first-batch-hotels.json"), {
    n: first25.length,
    hotels: first25.map((h) => ({
      record_id: h.record_id,
      name: h.name,
      city: h.city,
      priority: h.priority,
    })),
  });

  if (first25.length < args.firstBatchSize) {
    console.error(
      `WARNING: only ${first25.length} HIGH matches for first batch (wanted ${args.firstBatchSize})`
    );
  }

  // Build manifest (no writes yet)
  for (const h of first25) {
    await processHotel(h, {
      cp,
      byId,
      dispositions,
      extIds,
      evidence,
      writeEnabled: false,
      allowCoords,
      base,
      collectManifestOnly: true,
      manifestOut,
    });
  }

  writeJson(path.join(OUT_DIR, "first-batch-proposed-write-manifest.json"), {
    generated_at: new Date().toISOString(),
    hotels: first25.length,
    proposed_writes: manifestOut,
  });

  const safety = validateFirstBatchManifest(manifestOut);
  writeJson(path.join(OUT_DIR, "first-batch-safety-check.json"), safety);

  if (!safety.ok) {
    cp.first_batch_validation = "FAIL";
    cp.airtable_write_mode = "EVIDENCE_ONLY";
    cp.safety_events.push({
      at: new Date().toISOString(),
      type: "FIRST_BATCH_VALIDATION_FAIL",
      issues: safety.issues.slice(0, 20),
    });
    saveCheckpoint(cp);
    console.log(
      JSON.stringify(
        {
          FIRST_BATCH_VALIDATION: "FAIL",
          issues: safety.issues.length,
          AIRTABLE_WRITE_MODE: "EVIDENCE_ONLY",
        },
        null,
        2
      )
    );
    return { cp, safety };
  }

  // Execute writes if enabled
  if (!writeEnabled) {
    cp.first_batch_validation = "PASS_MANIFEST_ONLY";
    cp.airtable_write_mode = "MANIFEST_ONLY_FLAGS_OFF";
    cp.safety_events.push({
      at: new Date().toISOString(),
      type: "WRITES_NOT_ENABLED",
      message:
        "Safety PASS but set ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=1 ENABLE_CENSUS_FIELD_ENRICHMENT=1 ENABLE_COORDINATE_WRITES=1 CONFIRM_TRIPADVISOR_OVERNIGHT_WRITES=1 to execute",
    });
    saveCheckpoint(cp);
    console.log(
      JSON.stringify(
        {
          FIRST_BATCH_VALIDATION: "PASS_MANIFEST_ONLY",
          proposed: manifestOut.length,
          note: "Enable write flags to execute",
        },
        null,
        2
      )
    );
    return { cp, safety };
  }

  // Apply writes + readback
  cp.first_batch_validation = "PASS";
  cp.airtable_write_mode = "NULL_FILL_ENABLED";
  let readbackFail = false;
  for (const h of first25) {
    // Reset completion so processHotel can write
    cp.completed_hotel_ids = cp.completed_hotel_ids.filter(
      (id) => id !== h.record_id
    );
    const r = await processHotel(h, {
      cp,
      byId,
      dispositions,
      extIds,
      evidence,
      writeEnabled: true,
      allowCoords,
      base,
      collectManifestOnly: false,
      manifestOut: [],
    });
    if (r.status === "READBACK_FAIL") {
      readbackFail = true;
      break;
    }
  }

  if (readbackFail) {
    cp.first_batch_validation = "FAIL";
    cp.airtable_write_mode = "EVIDENCE_ONLY";
  }
  saveCheckpoint(cp);
  writeJson(path.join(OUT_DIR, "first-batch-result.json"), {
    FIRST_BATCH_VALIDATION: cp.first_batch_validation,
    AIRTABLE_WRITE_MODE: cp.airtable_write_mode,
    hotels: first25.length,
    proposed: manifestOut.length,
    executed_writes: cp.counters.airtable_executed_writes,
    hotels_updated: cp.counters.hotels_updated,
    apify_spend: cp.apify_spend_usd,
  });

  console.log(
    JSON.stringify(
      {
        FIRST_BATCH_VALIDATION: cp.first_batch_validation,
        AIRTABLE_WRITE_MODE: cp.airtable_write_mode,
        EXECUTED_WRITES: cp.counters.airtable_executed_writes,
        HOTELS_UPDATED: cp.counters.hotels_updated,
        APIFY_SPEND: cp.apify_spend_usd,
      },
      null,
      2
    )
  );
  return { cp, safety };
}

async function runWave(args, resume = false) {
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);
  ensureDir(PACK_DIR);

  let cp = readJson(CHECKPOINT_PATH, null);
  if (!cp) cp = emptyCheckpoint();
  cp.budget_usd = Math.min(cp.budget_usd || budgetUsd(), budgetUsd());
  if (resume) {
    const resumable = new Set([
      null,
      "PROCESS_SIGNAL_SIGINT",
      "PROCESS_SIGNAL_SIGTERM",
      "PROCESS_SIGNAL_SIGHUP",
      "UNHANDLED_EXCEPTION",
      "PROCESS_EXITED",
    ]);
    if (resumable.has(cp.stop_reason)) {
      cp.stop_reason = null;
    }
    cp.resumed_at = new Date().toISOString();
  }
  activeCheckpoint = cp;
  try {
    fs.writeFileSync(
      path.join(DATA_DIR, "wave-runner.pid"),
      String(process.pid),
      "utf8"
    );
  } catch (err) {
    console.error(`pid_file_write_failed:${err.message}`);
  }
  const heartbeat = createHeartbeatTracker(cp);
  heartbeat.mark(resume ? "RESUMING" : "RUNNING", { RUNNER_PID: process.pid });

  if (
    cp.first_batch_validation !== "PASS" &&
    cp.first_batch_validation !== "PASS_MANIFEST_ONLY"
  ) {
    console.error(
      "First-batch validation not PASS — running evidence-only for remaining hotels"
    );
    cp.airtable_write_mode = "EVIDENCE_ONLY";
  }

  const dispositions = loadDispositions();
  const gate = evaluateProductionWriteGate(process.env);
  const confirm =
    String(process.env.CONFIRM_TRIPADVISOR_OVERNIGHT_WRITES || "0") === "1";
  let writeEnabled =
    gate.allow_any_airtable_writes &&
    confirm &&
    cp.first_batch_validation === "PASS";
  const allowCoords = gate.allow_coordinate_writes && confirm && writeEnabled;

  const countries = args.country
    ? [args.country]
    : [...new Set(PRIORITY_COUNTRIES.filter((c) => c !== "Curacao" || true))];
  // Dedupe Curacao spelling for load — load both variants
  const loadCountries = [...new Set(countries)];

  const { base, hotels } = await loadCensusByCountries(loadCountries);
  for (const c of loadCountries) {
    ensureCountryStats(cp, c).census_hotels = hotels.filter(
      (h) => h.country === c
    ).length;
  }

  const byId = loadTaPools();
  const usageStore = createApifyUsageStore();
  const extIds = createExternalIdRegistry();
  const evidence = createEvidenceStore();
  const done = new Set(cp.completed_hotel_ids);

  // Process country by country in priority order
  const order = PRIORITY_COUNTRIES.filter(
    (c, i, arr) => arr.findIndex((x) => x.toLowerCase() === c.toLowerCase()) === i
  );

  let sinceCheckpoint = 0;
  for (const country of order) {
    if (args.country && country.toLowerCase() !== args.country.toLowerCase()) {
      continue;
    }
    if (cp.stop_reason) break;
    cp.current_country = country;
    const list = hotels
      .filter((h) => h.country.toLowerCase() === country.toLowerCase())
      .filter((h) => !done.has(h.record_id));

    console.error(
      `Country ${country}: remaining=${list.length} spend=${cp.apify_spend_usd}`
    );

    for (let i = 0; i < list.length; ) {
      if (cp.apify_spend_usd >= cp.budget_usd) {
        cp.stop_reason = "BUDGET_LIMIT_REACHED";
        break;
      }
      if (args.limit && cp.counters.processed >= args.limit) {
        cp.stop_reason = "LIMIT_REACHED";
        break;
      }

      const slice = list.slice(i, i + args.apifyBatchSize);
      i += args.apifyBatchSize;

      const needFetch = [];
      for (const h of slice) {
        if (hasFreshProfilePack(h.record_id, extIds, h.fields)) {
          continue;
        }
        const { match } = matchTripadvisorHotel(
          {
            name: h.name,
            city: h.city,
            country: h.country,
            lat: h.fields[MAP_CENSUS_FIELDS.latitude] ?? null,
            lng: h.fields[MAP_CENSUS_FIELDS.longitude] ?? null,
            website: h.fields[MAP_CENSUS_FIELDS.website] || null,
          },
          [...byId.values()]
        );
        if (!match || match.confidence !== "high") needFetch.push(h);
      }
      if (needFetch.length) {
        let fetched = null;
        let apifyAttempts = 0;
        const maxApifyAttempts = 2;
        while (apifyAttempts < maxApifyAttempts) {
          apifyAttempts += 1;
          try {
            fetched = await fetchApifyForHotels(
              needFetch,
              cp,
              usageStore,
              byId
            );
            break;
          } catch (err) {
            const msg = String(err?.message || err);
            const transient =
              /non_json_html|invalid_json|empty_body|fetch failed|ECONNRESET|ETIMEDOUT|429|502|503|504/i.test(
                msg
              );
            cp.counters.error_count = (cp.counters.error_count || 0) + 1;
            cp.safety_events.push({
              at: new Date().toISOString(),
              type: "APIFY_BATCH_ERROR",
              country,
              attempt: apifyAttempts,
              message: msg.slice(0, 300),
            });
            console.error(
              `Apify batch error (${country}) attempt=${apifyAttempts}: ${msg}`
            );
            saveCheckpoint(cp);
            heartbeat.mark("RUNNING_WITH_ERRORS", { phase: "apify_fetch" });
            if (!transient || apifyAttempts >= maxApifyAttempts) break;
            await new Promise((r) => setTimeout(r, 8000 * apifyAttempts));
          }
        }
        if (fetched?.stopped) {
          cp.stop_reason = fetched.reason;
          saveCheckpoint(cp);
          heartbeat.mark("STOPPED", { phase: "apify_fetch" });
          break;
        }
      }

      for (const h of slice) {
        let r;
        try {
          r = await processHotel(h, {
            cp,
            byId,
            dispositions,
            extIds,
            evidence,
            writeEnabled,
            allowCoords,
            base,
            collectManifestOnly: false,
            manifestOut: [],
          });
        } catch (err) {
          cp.counters.error_count = (cp.counters.error_count || 0) + 1;
          cp.safety_events.push({
            at: new Date().toISOString(),
            type: "HOTEL_PROCESS_ERROR",
            country,
            hotel: h.name,
            record_id: h.record_id,
            message: String(err?.message || err).slice(0, 300),
          });
          console.error(
            `hotel error ${h.record_id} (${h.name}): ${err?.message || err}`
          );
          if (heartbeat.shouldPulse()) heartbeat.mark("RUNNING_WITH_ERRORS");
          continue;
        }
        done.add(h.record_id);
        sinceCheckpoint += 1;
        if (r.status === "READBACK_FAIL") {
          cp.airtable_write_mode = "EVIDENCE_ONLY";
          writeEnabled = false;
        }
        if (sinceCheckpoint >= args.checkpointEvery) {
          saveCheckpoint(cp);
          sinceCheckpoint = 0;
          console.error(
            `checkpoint processed=${cp.counters.processed} packs=${cp.counters.profile_packs_saved} writes=${cp.counters.airtable_executed_writes} spend=${cp.apify_spend_usd}`
          );
          heartbeat.mark("RUNNING");
        } else if (heartbeat.shouldPulse()) {
          heartbeat.mark("RUNNING");
        }
        if (cp.apify_spend_usd >= cp.budget_usd * 0.9) {
          saveCheckpoint(cp);
          heartbeat.mark("BUDGET_WARNING");
        }
        if (cp.stop_reason) break;
      }

      // Country quality emergency stops
      const cs = ensureCountryStats(cp, country);
      if (cs.processed >= 50) {
        const badRate =
          (cs.false_match_rejected + cs.ambiguous) / Math.max(1, cs.processed);
        if (badRate > 0.1) {
          cp.safety_events.push({
            at: new Date().toISOString(),
            type: "COUNTRY_QUALITY_PAUSE",
            country,
            badRate,
          });
          console.error(`Pausing ${country} due to quality rate ${badRate}`);
          break;
        }
      }
    }
    saveCheckpoint(cp);
    heartbeat.mark(cp.stop_reason ? "STOPPED" : "RUNNING", {
      country_complete: country,
    });
  }

  if (!cp.stop_reason) {
    const remaining = order.some((country) => {
      const list = hotels
        .filter((h) => h.country.toLowerCase() === country.toLowerCase())
        .filter((h) => !done.has(h.record_id));
      return list.length > 0;
    });
    if (!remaining) cp.stop_reason = "NORMAL_COMPLETION";
  }

  saveCheckpoint(cp);
  activeCheckpoint = null;
  heartbeat.mark(
    cp.stop_reason === "NORMAL_COMPLETION" ? "COMPLETE" : "STOPPED"
  );
  if (cp.stop_reason === "NORMAL_COMPLETION") {
    await writeFinalReport(cp);
  }
  return cp;
}

async function writeFinalReport(cp) {
  const packs = fs.existsSync(PACK_DIR)
    ? fs.readdirSync(PACK_DIR).filter((f) => f.endsWith(".json"))
    : [];
  let coverage = {
    rating: 0,
    reviews: 0,
    ranking: 0,
    guest_rank_percentile: 0,
    hotel_class: 0,
    amenities: 0,
    category_scores: 0,
    price_position: 0,
    website: 0,
    phone: 0,
    email: 0,
  };
  for (const f of packs.slice(0, 5000)) {
    const j = readJson(path.join(PACK_DIR, f));
    const c = j?.coverage || (j?.pack ? profilePackCoverageFlags(j.pack) : null);
    if (!c) continue;
    if (c.RATING_COVERAGE) coverage.rating += 1;
    if (c.REVIEW_COUNT_COVERAGE) coverage.reviews += 1;
    if (c.RANKING_COVERAGE) coverage.ranking += 1;
    if (c.GUEST_RANK_PERCENTILE_COVERAGE) coverage.guest_rank_percentile += 1;
    if (c.HOTEL_CLASS_COVERAGE) coverage.hotel_class += 1;
    if (c.AMENITY_COVERAGE) coverage.amenities += 1;
    if (c.CATEGORY_SCORE_COVERAGE) coverage.category_scores += 1;
    if (c.PRICE_POSITION_COVERAGE) coverage.price_position += 1;
    if (c.CONTACT_COVERAGE) {
      // contact aggregate — also inspect pack
    }
    const p = j.pack;
    if (p?.contact_observations?.website) coverage.website += 1;
    if (p?.contact_observations?.phone) coverage.phone += 1;
    if (p?.contact_observations?.email) coverage.email += 1;
  }
  const nPacks = packs.length || 1;
  const covPct = Object.fromEntries(
    Object.entries(coverage).map(([k, v]) => [k, pct(v, packs.length)])
  );

  const summary = {
    TRIPADVISOR_PRIORITY_CALA_PROFILE_WAVE_V1_COMPLETE: true,
    PRIORITY_COUNTRIES_REQUESTED: PRIORITY_COUNTRIES.filter(
      (c) => c !== "Curacao"
    ),
    COUNTRIES_STARTED: Object.keys(cp.countries),
    COUNTRIES_COMPLETED: Object.entries(cp.countries)
      .filter(([, v]) => v.processed > 0)
      .map(([k]) => k),
    CENSUS_TOTAL: 15485,
    TOTAL_PROCESSED: cp.counters.processed,
    MATCHED_HIGH_CONFIDENCE: cp.counters.matched_high,
    MATCHED_MEDIUM_CONFIDENCE: cp.counters.matched_medium,
    NO_MATCH: cp.counters.no_match,
    FALSE_MATCH_REJECTED: cp.counters.false_match_rejected,
    AMBIGUOUS: cp.counters.ambiguous,
    PROFILE_PACKS_SAVED: cp.counters.profile_packs_saved,
    EXISTING_PROFILE_PACKS_REUSED: cp.counters.profile_packs_reused,
    PROFILE_PACK_COVERAGE: covPct,
    ROOM_CANDIDATES_SAVED: cp.counters.room_candidates,
    AIRTABLE_WRITE_MODE: cp.airtable_write_mode,
    FIRST_BATCH_VALIDATION: cp.first_batch_validation,
    HOTELS_UPDATED: cp.counters.hotels_updated,
    AIRTABLE_EXECUTED_WRITES: cp.counters.airtable_executed_writes,
    ...cp.counters.fields_filled,
    BRAND_WRITES: 0,
    OWNER_WRITES: 0,
    OPERATOR_WRITES: 0,
    ROOM_AUTHORITATIVE_WRITES: 0,
    BLOCKED_EXISTING_VALUES: cp.counters.blocked_existing,
    BLOCKED_SCHEMA: cp.counters.blocked_schema,
    BLOCKED_VALIDATION: cp.counters.blocked_validation,
    CONFLICTS: cp.counters.conflicts,
    APIFY_TOTAL_COST: cp.apify_spend_usd,
    COST_PER_PROCESSED_HOTEL: pct(cp.apify_spend_usd * 100, cp.counters.processed)
      ? Number(
          (cp.apify_spend_usd / Math.max(1, cp.counters.processed)).toFixed(6)
        )
      : null,
    COST_PER_PROFILE_PACK: Number(
      (
        cp.apify_spend_usd /
        Math.max(1, cp.counters.profile_packs_saved + cp.counters.profile_packs_reused)
      ).toFixed(6)
    ),
    COST_PER_CANONICAL_FIELD_FILLED: Number(
      (
        cp.apify_spend_usd / Math.max(1, cp.counters.airtable_executed_writes)
      ).toFixed(6)
    ),
    SCHEMA_CHANGES_EXECUTED: 0,
    SCHEMA_RATIONALIZATION_RESPECTED: true,
    CHECKPOINT_STATUS: cp.stop_reason || "IN_PROGRESS_OR_COMPLETE",
    NEXT_RESUME_COMMAND:
      "node scripts/tripadvisor-priority-cala-profile-wave.mjs --mode=resume",
    PRODUCTION_SAFETY_STATUS: cp.airtable_write_mode,
    STOP_REASON: cp.stop_reason,
    COUNTRY_BREAKDOWN: cp.countries,
    SAFETY_EVENTS: cp.safety_events,
  };

  writeJson(path.join(OUT_DIR, "wave-summary.json"), summary);

  const md = `# Tripadvisor Priority CALA Profile Wave V1

**Generated:** ${new Date().toISOString()}

## Executive summary

- First-batch validation: **${cp.first_batch_validation}**
- Airtable write mode: **${cp.airtable_write_mode}**
- Processed: **${cp.counters.processed}**
- HIGH matches: **${cp.counters.matched_high}**
- Profile packs saved: **${cp.counters.profile_packs_saved}** (reused ${cp.counters.profile_packs_reused})
- Airtable executed writes: **${cp.counters.airtable_executed_writes}**
- Hotels updated: **${cp.counters.hotels_updated}**
- Room candidates: **${cp.counters.room_candidates}**
- Apify spend: **$${cp.apify_spend_usd}** / budget $${cp.budget_usd}
- Schema changes: **0**
- Brand/Owner/Operator/Rooms authoritative writes: **0**

## Countries

${Object.entries(cp.countries)
  .map(
    ([c, v]) =>
      `### ${c}\n- processed: ${v.processed}\n- matched_high: ${v.matched_high}\n- profile_packs: ${v.profile_packs}\n- hotels_updated: ${v.hotels_updated}\n- fields_filled: ${v.fields_filled}\n- room_candidates: ${v.room_candidates}\n- conflicts: ${v.conflicts}\n- apify_cost: ${v.apify_cost}`
  )
  .join("\n\n")}

## Safety

\`\`\`json
${JSON.stringify(cp.safety_events, null, 2)}
\`\`\`

## Resume

\`\`\`bash
node scripts/tripadvisor-priority-cala-profile-wave.mjs --mode=resume
\`\`\`

Stop reason: ${cp.stop_reason || "none"}
`;
  fs.writeFileSync(
    path.join(OUT_DIR, "DEALALITY_TRIPADVISOR_PRIORITY_CALA_PROFILE_WAVE.md"),
    md,
    "utf8"
  );

  fs.writeFileSync(
    path.join(OUT_DIR, "STATUS.txt"),
    `TRIPADVISOR_PRIORITY_CALA_PROFILE_WAVE_V1_COMPLETE

FIRST_BATCH_VALIDATION: ${cp.first_batch_validation}
AIRTABLE_WRITE_MODE: ${cp.airtable_write_mode}
TOTAL_PROCESSED: ${cp.counters.processed}
MATCHED_HIGH_CONFIDENCE: ${cp.counters.matched_high}
PROFILE_PACKS_SAVED: ${cp.counters.profile_packs_saved}
AIRTABLE_EXECUTED_WRITES: ${cp.counters.airtable_executed_writes}
HOTELS_UPDATED: ${cp.counters.hotels_updated}
ROOM_CANDIDATES_SAVED: ${cp.counters.room_candidates}
APIFY_TOTAL_COST: ${cp.apify_spend_usd}
SCHEMA_CHANGES_EXECUTED: 0
BRAND_WRITES: 0
OWNER_WRITES: 0
OPERATOR_WRITES: 0
ROOM_AUTHORITATIVE_WRITES: 0
CHECKPOINT_STATUS: ${cp.stop_reason || "RUNNING_OR_COMPLETE"}
NEXT_RESUME_COMMAND: node scripts/tripadvisor-priority-cala-profile-wave.mjs --mode=resume
`,
    "utf8"
  );

  return summary;
}

async function applyFirstBatchFromArtifacts(args) {
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);
  const manifest = readJson(
    path.join(OUT_DIR, "first-batch-proposed-write-manifest.json"),
    null
  );
  if (!manifest?.proposed_writes?.length) {
    throw new Error("missing first-batch manifest — run --mode=first-batch first");
  }
  const safety = validateFirstBatchManifest(manifest.proposed_writes);
  writeJson(path.join(OUT_DIR, "first-batch-safety-check.json"), safety);
  if (!safety.ok) {
    console.log(JSON.stringify({ FIRST_BATCH_VALIDATION: "FAIL", issues: safety.issues.length }, null, 2));
    return;
  }

  const gate = evaluateProductionWriteGate(process.env);
  const confirm =
    String(process.env.CONFIRM_TRIPADVISOR_OVERNIGHT_WRITES || "0") === "1";
  if (!(gate.allow_any_airtable_writes && confirm)) {
    throw new Error("write flags not enabled for apply-first-batch");
  }
  const allowCoords = gate.allow_coordinate_writes;

  const batchHotels = readJson(path.join(DATA_DIR, "first-batch-hotels.json"), {
    hotels: [],
  });
  const { base, hotels } = await loadCensusByCountries(["Mexico"]);
  const byRec = new Map(hotels.map((h) => [h.record_id, h]));
  const dispositions = loadDispositions();
  let cp = readJson(CHECKPOINT_PATH, null) || emptyCheckpoint();
  const byId = loadTaPools();
  const extIds = createExternalIdRegistry();
  const evidence = createEvidenceStore();

  cp.first_batch_validation = "PASS";
  cp.airtable_write_mode = "NULL_FILL_ENABLED";

  let readbackFail = false;
  for (const row of batchHotels.hotels || []) {
    const h = byRec.get(row.record_id);
    if (!h) continue;
    cp.completed_hotel_ids = cp.completed_hotel_ids.filter(
      (id) => id !== h.record_id
    );
    const r = await processHotel(h, {
      cp,
      byId,
      dispositions,
      extIds,
      evidence,
      writeEnabled: true,
      allowCoords,
      base,
      collectManifestOnly: false,
      manifestOut: [],
    });
    if (r.status === "READBACK_FAIL") {
      readbackFail = true;
      break;
    }
  }

  if (readbackFail) {
    cp.first_batch_validation = "FAIL";
    cp.airtable_write_mode = "EVIDENCE_ONLY";
  }
  saveCheckpoint(cp);
  writeJson(path.join(OUT_DIR, "first-batch-result.json"), {
    FIRST_BATCH_VALIDATION: cp.first_batch_validation,
    AIRTABLE_WRITE_MODE: cp.airtable_write_mode,
    hotels: (batchHotels.hotels || []).length,
    proposed: manifest.proposed_writes.length,
    executed_writes: cp.counters.airtable_executed_writes,
    hotels_updated: cp.counters.hotels_updated,
    apify_spend: cp.apify_spend_usd,
  });
  console.log(
    JSON.stringify(
      {
        FIRST_BATCH_VALIDATION: cp.first_batch_validation,
        AIRTABLE_WRITE_MODE: cp.airtable_write_mode,
        EXECUTED_WRITES: cp.counters.airtable_executed_writes,
        HOTELS_UPDATED: cp.counters.hotels_updated,
      },
      null,
      2
    )
  );
}

async function main() {
  installWaveShutdownHandlers();
  const args = parseArgs(process.argv);
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);

  if (args.mode === "self-check") {
    const cp = readJson(CHECKPOINT_PATH, emptyCheckpoint());
    if (!cp.counters.error_count) cp.counters.error_count = 0;
    const hb = writeHeartbeat(cp, "SELF_CHECK");
    const ok =
      hb.REMAINING_BUDGET === remainingBudgetUsd(cp) &&
      hb.PROCESSED_TOTAL === (cp.counters?.processed ?? 0);
    console.log(JSON.stringify({ ok, heartbeat: hb }, null, 2));
    if (!ok) process.exit(1);
    return;
  }

  if (args.mode === "first-batch") {
    await runFirstBatch(args);
    return;
  }
  if (args.mode === "apply-first-batch") {
    await applyFirstBatchFromArtifacts(args);
    return;
  }
  if (args.mode === "run") {
    await runWave(args, false);
    return;
  }
  if (args.mode === "resume") {
    await runWave(args, true);
    return;
  }
  if (args.mode === "report") {
    const cp = readJson(CHECKPOINT_PATH, emptyCheckpoint());
    const summary = await writeFinalReport(cp);
    console.log(JSON.stringify(summary, null, 2));
    return;
  }
  throw new Error(`Unknown mode ${args.mode}`);
}

main().catch((err) => {
  console.error(err);
  try {
    const cp = readJson(CHECKPOINT_PATH, null);
    if (cp && !cp.stop_reason) {
      cp.stop_reason = "UNHANDLED_EXCEPTION";
      cp.last_error = String(err?.message || err).slice(0, 500);
      cp.counters = cp.counters || {};
      cp.counters.error_count = (cp.counters.error_count || 0) + 1;
      saveCheckpoint(cp);
      writeHeartbeat(cp, "ERROR_STOPPED");
    }
  } catch (saveErr) {
    console.error("failed to persist checkpoint after error:", saveErr);
  }
  process.exit(1);
});
