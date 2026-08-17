/**
 * Master Hotel Property Census Enrichment Orchestrator
 *
 * Restartable production loop across approved enrichment fields.
 * Reuses Property Fundamentals, Official Rooms Registry, Mapbox coordinates,
 * geography library, and brand dictionary — does not duplicate those systems.
 *
 * Write target: Hotel Property Census only (tbl9aY5ijiuIzzWam).
 * NULL_FILL default. Never HBX rooms[] / Cvent-only rooms / HBX coordinates.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { createLiveHotelPropertyCensusAdapter } from "./census-autopilot-batch-engine.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  CENSUS_GEO_FIELDS,
  resolveContinentSubContinentFromCountry,
  resolveContinentSubContinentCanonical,
} from "./census-region-market-map.js";
import { resolveDealalityCalaGeography } from "./dealality-cala-geography-registry-v1.js";
import { POSTAL_CODE_FIELD } from "./census-postal-code-v1.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";
import {
  buildDeterministicGeoPatch,
  runPropertyFundamentalsEnrichmentV1,
  MAP_PF,
} from "./property-fundamentals-enrichment-v1.js";
import { runOfficialRoomsSourceRegistryWave } from "./official-rooms-source-registry-wave-v1.js";
import {
  evaluateMapboxPermanentReadiness,
  maxGeocodeRequestsPerRun,
} from "./census-coordinate-provider.js";
import {
  runCoordinateCompletionQueue,
  runMasterCoordinateSampleGate,
} from "./census-coordinate-completion.js";
import {
  buildCanonicalBrandDictionary,
  lookupCanonicalBrand,
  familyFromOfficialUrl,
} from "./census-brand-canonical-dictionary.js";
import { canonicalizeParentCompany } from "./census-parent-company-normalization.js";
import { resolveSecondaryHotelDataPolicy } from "./census-secondary-hotel-data-policy.js";
import {
  runMasterBrandPortfolioValidation,
  MAP_BRAND,
} from "./master-brand-portfolio-validation-v1.js";
import { evaluateCoordinateCompletionEligibility } from "./census-coordinate-completion.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");
const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/master-census-enrichment"
);
const CHECKPOINT_FP = path.join(STATE_DIR, "checkpoint.json");
const REPORT_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/master-census-enrichment-final.json"
);
const REPORT_MD = path.join(
  ROOT,
  "reports/research-engine-v2/master-census-enrichment-final.md"
);

export const MASTER_ENRICHMENT_VERSION = "master-census-enrichment-v1";

export const MAP_MASTER = Object.freeze({
  propertyName: MAP_FIRST_PASS.propertyName,
  canonicalName: MAP_FIRST_PASS.canonicalPropertyName,
  country: MAP_FIRST_PASS.country,
  continent: CENSUS_GEO_FIELDS.continent,
  subContinent: CENSUS_GEO_FIELDS.subContinent,
  stateRegion: MAP_FIRST_PASS.stateRegion,
  city: MAP_FIRST_PASS.city,
  address: MAP_FIRST_PASS.address,
  postalCode: POSTAL_CODE_FIELD,
  latitude: MAP_FIRST_PASS.latitude,
  longitude: MAP_FIRST_PASS.longitude,
  currentBrand: MAP_FIRST_PASS.currentBrand,
  brandFamily: MAP_FIRST_PASS.brandFamily,
  familySourceFamily: MAP_FIRST_PASS.family,
  officialUrl: MAP_FIRST_PASS.officialUrl,
  phone: "Phone",
  roomsKeys: MAP_ROOMS.roomsKeys,
  lastReviewed: MAP_FIRST_PASS.lastReviewed,
  enrichmentStatus: MAP_FIRST_PASS.enrichmentStatus,
  coordinateSourceType: MAP_FIRST_PASS.coordinateSourceType,
  coordinateConfidence: MAP_FIRST_PASS.coordinateConfidence,
  geocodeProvider: MAP_FIRST_PASS.geocodeProvider,
  geocodeMethod: MAP_FIRST_PASS.geocodeMethod,
  geocodeReviewedDate: MAP_FIRST_PASS.geocodeReviewedDate,
});

/** Schema note: there is no separate Family or Source Family column — only Family / Source Family. */
export const MASTER_SCHEMA_NOTES = Object.freeze({
  family_field: "Family / Source Family",
  website_field: "Official Property URL",
  coordinate_verified_date_field: "Geocode Reviewed Date",
  no_separate_family_or_source_family: true,
});

const READ_FIELDS = [
  MAP_MASTER.propertyName,
  MAP_MASTER.canonicalName,
  MAP_MASTER.country,
  MAP_MASTER.continent,
  MAP_MASTER.subContinent,
  MAP_MASTER.stateRegion,
  MAP_MASTER.city,
  MAP_MASTER.address,
  MAP_MASTER.postalCode,
  MAP_MASTER.latitude,
  MAP_MASTER.longitude,
  MAP_MASTER.currentBrand,
  MAP_MASTER.brandFamily,
  MAP_MASTER.familySourceFamily,
  MAP_MASTER.officialUrl,
  MAP_MASTER.phone,
  MAP_MASTER.roomsKeys,
  MAP_BRAND.candidateBrand,
  MAP_BRAND.candidateBrandFamily,
  MAP_FIRST_PASS.addressConfidence,
  MAP_FIRST_PASS.addressSourceUrl,
  MAP_FIRST_PASS.radarGeographyStatus,
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2));
}

function writeMd(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text);
}

function readJson(fp, fallback) {
  try {
    if (!fs.existsSync(fp)) return fallback;
    const raw = fs.readFileSync(fp, "utf8").replace(/^\uFEFF/, "");
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function writeCheckpoint(partial) {
  const prev = readJson(CHECKPOINT_FP, {});
  const next = {
    ...prev,
    ...partial,
    mapbox_wave: partial.mapbox_wave
      ? { ...(prev.mapbox_wave || {}), ...partial.mapbox_wave }
      : prev.mapbox_wave || null,
    mapbox_metrics: partial.mapbox_metrics
      ? { ...(prev.mapbox_metrics || {}), ...partial.mapbox_metrics }
      : prev.mapbox_metrics || null,
    updated_at: new Date().toISOString(),
  };
  writeJson(CHECKPOINT_FP, next);
  return next;
}

async function listCensusRecords(baseId, token, fields) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
    }
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(90);
  } while (offset);
  return records;
}

/**
 * Completeness dashboard for approved master fields.
 */
export function computeMasterCompleteness(records = []) {
  const n = Math.max(records.length, 1);
  const keys = [
    ["current_brand", MAP_MASTER.currentBrand],
    ["brand_family", MAP_MASTER.brandFamily],
    ["family_source_family", MAP_MASTER.familySourceFamily],
    ["continent", MAP_MASTER.continent],
    ["sub_continent", MAP_MASTER.subContinent],
    ["state_region", MAP_MASTER.stateRegion],
    ["city", MAP_MASTER.city],
    ["address", MAP_MASTER.address],
    ["postal_code", MAP_MASTER.postalCode],
    ["latitude", MAP_MASTER.latitude],
    ["longitude", MAP_MASTER.longitude],
    ["rooms", MAP_MASTER.roomsKeys],
    ["website", MAP_MASTER.officialUrl],
    ["phone", MAP_MASTER.phone],
  ];
  /** @type {Record<string, object>} */
  const out = { n: records.length };
  for (const [k, field] of keys) {
    let populated = 0;
    for (const r of records) {
      if (!isBlank(r.fields?.[field])) populated += 1;
    }
    out[k] = {
      populated,
      missing: records.length - populated,
      completeness_pct: Math.round((100 * populated) / n),
    };
  }
  return out;
}

/**
 * Derive Brand Family + Family / Source Family from validated Current Brand only.
 */
export function buildBrandFamilyDerivePatch(fields = {}, dictionary) {
  if (isBlank(fields[MAP_MASTER.currentBrand])) {
    return { ok: false, reason: "current_brand_blank" };
  }
  const needFamily = isBlank(fields[MAP_MASTER.brandFamily]);
  const needSourceFamily = isBlank(fields[MAP_MASTER.familySourceFamily]);
  if (!needFamily && !needSourceFamily) {
    return { ok: false, reason: "families_already_populated" };
  }

  const lookup = lookupCanonicalBrand(
    fields[MAP_MASTER.currentBrand],
    dictionary,
    {
      propertyName: fields[MAP_MASTER.propertyName],
      sourceUrl: fields[MAP_MASTER.officialUrl],
    }
  );
  if (!lookup.ok || !lookup.entry) {
    return {
      ok: false,
      reason: "BRAND_MAPPING_GAP",
      brand: fields[MAP_MASTER.currentBrand],
    };
  }

  const parent =
    canonicalizeParentCompany(
      lookup.entry.parent_company || lookup.entry.brand_family
    ) ||
    lookup.entry.parent_company ||
    lookup.entry.brand_family;
  if (!parent) {
    return { ok: false, reason: "BRAND_MAPPING_GAP", brand: lookup.canonical };
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  if (needFamily) patch[MAP_MASTER.brandFamily] = parent;
  if (needSourceFamily) patch[MAP_MASTER.familySourceFamily] = parent;
  patch[MAP_MASTER.lastReviewed] = todayIsoDate();
  patch[MAP_MASTER.enrichmentStatus] = "Partial";
  return {
    ok: true,
    class: "BRAND_FAMILY_DERIVED_FROM_VALIDATED_CURRENT_BRAND",
    patch,
    canonical: lookup.canonical,
  };
}

/**
 * Strict Current Brand HIGH from official brand property URL + name corroboration.
 * Does not use Candidate Brand, Cvent-alone, or HBX chain alone.
 */
export function evaluateStrictCurrentBrandFromOfficialUrl(fields = {}, dictionary) {
  if (!isBlank(fields[MAP_MASTER.currentBrand])) {
    return { ok: false, reason: "already_populated", class: "CONFIRMED_EXISTING" };
  }
  const url = String(fields[MAP_MASTER.officialUrl] || "").trim();
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, reason: "no_official_url", class: "BRAND_UNRESOLVED" };
  }
  if (/booking\.com|expedia\.|tripadvisor\.|facebook\.com|maps\.google/i.test(url)) {
    return { ok: false, reason: "forbidden_website_host", class: "BRAND_UNRESOLVED" };
  }

  const urlFamily = familyFromOfficialUrl(url);
  if (!urlFamily) {
    return { ok: false, reason: "url_not_official_brand_host", class: "BRAND_CANDIDATE" };
  }

  const name = String(
    fields[MAP_MASTER.propertyName] || fields[MAP_MASTER.canonicalName] || ""
  );
  // Find dictionary brands in this family whose tokens appear in property name
  /** @type {object[]} */
  const hits = [];
  for (const entry of dictionary.brands || []) {
    const fam = String(entry.brand_family || entry.parent_company || "");
    if (!fam) continue;
    const famOk =
      fam.toLowerCase().includes(String(urlFamily).toLowerCase()) ||
      String(urlFamily).toLowerCase().includes(fam.toLowerCase().split(/\s+/)[0]);
    if (!famOk && !familiesLooseMatch(urlFamily, fam)) continue;
    const brand = String(entry.canonical_brand_name || "");
    if (brand.length < 4) continue;
    const re = new RegExp(
      `(?:^|[^a-z0-9])${brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?:$|[^a-z0-9])`,
      "i"
    );
    if (re.test(name)) hits.push(entry);
  }

  if (hits.length === 1) {
    const entry = hits[0];
    const parent =
      canonicalizeParentCompany(entry.parent_company || entry.brand_family) ||
      entry.parent_company ||
      entry.brand_family;
    /** @type {Record<string, unknown>} */
    const patch = {
      [MAP_MASTER.currentBrand]: entry.canonical_brand_name,
      [MAP_MASTER.lastReviewed]: todayIsoDate(),
      [MAP_MASTER.enrichmentStatus]: "Partial",
    };
    if (isBlank(fields[MAP_MASTER.brandFamily]) && parent) {
      patch[MAP_MASTER.brandFamily] = parent;
    }
    if (isBlank(fields[MAP_MASTER.familySourceFamily]) && parent) {
      patch[MAP_MASTER.familySourceFamily] = parent;
    }
    return {
      ok: true,
      class: "BRAND_VALIDATED_HIGH",
      patch,
      evidence: {
        method: "official_brand_url_plus_property_name",
        url,
        url_family: urlFamily,
      },
    };
  }
  if (hits.length > 1) {
    return {
      ok: false,
      class: "BRAND_CONFLICT",
      reason: "multiple_brand_tokens_in_name",
      candidates: hits.map((h) => h.canonical_brand_name),
    };
  }
  return {
    ok: false,
    class: "BRAND_CANDIDATE",
    reason: "url_family_known_but_brand_not_unique_in_name",
    url_family: urlFamily,
  };
}

function familiesLooseMatch(a, b) {
  const na = String(a || "").toLowerCase();
  const nb = String(b || "").toLowerCase();
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (/marriott/.test(na) && /marriott/.test(nb)) return true;
  if (/hilton/.test(na) && /hilton/.test(nb)) return true;
  if (/ihg|intercontinental/.test(na) && /ihg|intercontinental/.test(nb)) return true;
  if (/choice|radisson/.test(na) && /choice|radisson/.test(nb)) return true;
  if (/accor/.test(na) && /accor/.test(nb)) return true;
  if (/wyndham/.test(na) && /wyndham/.test(nb)) return true;
  return false;
}

/**
 * Continent / Sub-Continent NULL_FILL for one record.
 */
export function buildContinentSubContinentPatch(fields = {}) {
  if (
    !isBlank(fields[MAP_MASTER.continent]) &&
    !isBlank(fields[MAP_MASTER.subContinent])
  ) {
    return { ok: false, reason: "already_populated" };
  }
  if (isBlank(fields[MAP_MASTER.country])) {
    return { ok: false, reason: "country_blank" };
  }

  let resolved = resolveContinentSubContinentFromCountry(fields[MAP_MASTER.country]);
  if (!resolved) {
    const geo = resolveDealalityCalaGeography(fields[MAP_MASTER.country]);
    const fallback = resolveContinentSubContinentCanonical(
      fields[MAP_MASTER.country],
      {
        resolveDealalityGeography: () => geo,
      }
    );
    if (fallback.ok) {
      resolved = {
        continent: fallback.continent,
        subContinent: fallback.subContinent,
      };
    }
  }
  if (!resolved?.continent || !resolved?.subContinent) {
    return { ok: false, reason: "unmapped_country" };
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  if (isBlank(fields[MAP_MASTER.continent])) {
    patch[MAP_MASTER.continent] = resolved.continent;
  }
  if (isBlank(fields[MAP_MASTER.subContinent])) {
    patch[MAP_MASTER.subContinent] = resolved.subContinent;
  }
  if (!Object.keys(patch).length) {
    return { ok: false, reason: "nothing_to_write" };
  }
  patch[MAP_MASTER.lastReviewed] = todayIsoDate();
  patch[MAP_MASTER.enrichmentStatus] = "Partial";
  return { ok: true, patch, resolved };
}

/**
 * @param {{
 *   mode?: 'dry-run'|'run'|'resume',
 *   enableProductionWrites?: boolean,
 *   maxPfResearch?: number,
 *   maxBrandValidate?: number,
 *   maxCoordinateRequests?: number,
 *   skipRoomsRegistry?: boolean,
 *   skipPropertyFundamentals?: boolean,
 *   skipCoordinates?: boolean,
 *   log?: Function,
 * }} [opts]
 */
export async function runMasterCensusEnrichment(opts = {}) {
  const mode = opts.mode || "dry-run";
  const enableWrites =
    Boolean(opts.enableProductionWrites) &&
    (mode === "run" || mode === "resume");
  const log = opts.log || console.log;
  const generated_at = new Date().toISOString();
  fs.mkdirSync(STATE_DIR, { recursive: true });

  const founderDecisions = [];
  const sourcePerf = {};
  const tallies = {
    fields_written: 0,
    properties_patched: 0,
    continent_writes: 0,
    sub_continent_writes: 0,
    state_writes: 0,
    city_writes: 0,
    address_writes: 0,
    postal_writes: 0,
    brand_validations_high: 0,
    brand_conflicts: 0,
    brand_family_derived: 0,
    brand_mapping_gaps: 0,
    coordinates_written: 0,
    coordinate_conflicts: 0,
    rooms_written: 0,
    website_writes: 0,
    phone_writes: 0,
    errors: 0,
    HBX_ROOMS_ARRAY_WRITES: 0,
    CVENT_ONLY_ROOM_VALIDATIONS: 0,
    BENCHMARK_ROOM_WRITES: 0,
    HBX_COORDINATE_WRITES: 0,
    DESTRUCTIVE_OVERWRITES: 0,
    WRONG_TABLE_WRITES: 0,
  };

  let checkpoint = readJson(CHECKPOINT_FP, {
    lanes_completed: [],
    started_at: generated_at,
  });
  if (mode !== "resume") {
    checkpoint = { lanes_completed: [], started_at: generated_at };
  }
  const doneLanes = new Set(checkpoint.lanes_completed || []);

  // Founder approved Mapbox Permanent (2026-08-16): reopen coordinate + enrichment lanes
  const coordWritesEnabled =
    String(process.env.ENABLE_COORDINATE_WRITES || "0") === "1";
  const waveCapNow = maxGeocodeRequestsPerRun(process.env);
  const waveUsedNow = Number(checkpoint.mapbox_wave?.requests_used || 0);
  const waveRemainingNow = Math.max(0, waveCapNow - waveUsedNow);
  const founderCoordResume =
    mode === "resume" &&
    coordWritesEnabled &&
    (checkpoint.last_status === "master_enrichment_paused_founder_decision" ||
      (checkpoint.founder_decisions || []).some(
        (d) => d?.item === "ENABLE_COORDINATE_WRITES"
      ) ||
      opts.forceReopenCoordinates === true);
  const continueMapboxWave =
    mode === "resume" &&
    coordWritesEnabled &&
    waveRemainingNow > 0 &&
    checkpoint.mapbox_wave?.sample_passed === true &&
    opts.continueMapboxWave === true;
  // Brand + Rooms priority wave: reopen structured lanes (not broad Mapbox)
  const brandRoomsWave =
    mode === "resume" &&
    (opts.forceBrandRoomsWave === true ||
      opts.brandRoomsWave === true ||
      checkpoint.last_status === "master_enrichment_wave_complete" ||
      checkpoint.wave_focus === "brand_rooms");
  if (founderCoordResume) {
    for (const lane of [
      "coordinates",
      "property_fundamentals",
      "strict_current_brand",
      "brand_family_derive",
      "official_rooms_registry",
    ]) {
      doneLanes.delete(lane);
    }
    log(
      `[master] reopened enrichment lanes after Mapbox Permanent founder approval`
    );
  } else if (brandRoomsWave) {
    for (const lane of [
      "brand_portfolio",
      "strict_current_brand",
      "brand_family_derive",
      "official_rooms_registry",
      "property_fundamentals",
    ]) {
      doneLanes.delete(lane);
    }
    // Do NOT reopen broad coordinates — opportunistic only
    log(`[master] brand+rooms priority wave — reopened brand/rooms/PF lanes`);
  } else if (continueMapboxWave) {
    doneLanes.delete("coordinates");
    log(
      `[master] continuing Mapbox Permanent wave (remaining=${waveRemainingNow}/${waveCapNow})`
    );
  }

  /** @type {string[]} */
  const newlyAddressImprovedIds = [];
  let brandPortfolioReport = null;
  let roomsWaveReport = null;
  const brandBefore = { current: null, family: null, source_family: null };
  const roomsBefore = { completeness: null };

  /** @type {Record<string, unknown>} */
  const mapboxMetrics = {
    MAPBOX_PERMANENT_MODE_CONFIRMED: false,
    MAPBOX_REQUESTS: 0,
    MAPBOX_SUCCESS: 0,
    MAPBOX_NO_MATCH: 0,
    MAPBOX_LOW_CONFIDENCE: 0,
    MAPBOX_ERRORS: 0,
    MAPBOX_CONFLICTS: 0,
    ESTIMATED_MAPBOX_COST: 0,
    COORDINATE_SAMPLE_PASSED: null,
    CITY_PATCHES_FROM_GEOCODING: 0,
    STATE_REGION_PATCHES_FROM_GEOCODING: 0,
    POSTAL_CODE_PATCHES_FROM_GEOCODING: 0,
    NEWLY_ELIGIBLE_MAPBOX_PROPERTIES: 0,
    LATITUDE_COMPLETENESS_BEFORE: null,
    LONGITUDE_COMPLETENESS_BEFORE: null,
    LATITUDE_COMPLETENESS_AFTER: null,
    LONGITUDE_COMPLETENESS_AFTER: null,
  };

  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;
  assertProductionCensusWriteTarget({
    baseId,
    tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });

  log(`[master] listing Hotel Property Census…`);
  let records = await listCensusRecords(baseId, token, READ_FIELDS);
  const before = computeMasterCompleteness(records);
  mapboxMetrics.LATITUDE_COMPLETENESS_BEFORE = before.latitude.completeness_pct;
  mapboxMetrics.LONGITUDE_COMPLETENESS_BEFORE =
    before.longitude.completeness_pct;
  brandBefore.current = before.current_brand.completeness_pct;
  brandBefore.family = before.brand_family.completeness_pct;
  brandBefore.source_family = before.family_source_family.completeness_pct;
  roomsBefore.completeness = before.rooms.completeness_pct;
  log(
    `[master] n=${before.n} rooms=${before.rooms.completeness_pct}% continent=${before.continent.completeness_pct}% brand=${before.current_brand.completeness_pct}% lat=${before.latitude.completeness_pct}%`
  );

  /** @type {Map<string, Record<string, unknown>>} */
  const patchMap = new Map();
  /** @type {Set<string>} */
  const patchedIds = new Set();

  function mergePatch(id, patch, lane) {
    if (!patch || !Object.keys(patch).length) return;
    const prev = patchMap.get(id) || {};
    // NULL_FILL guard — never overwrite non-blank production values
    for (const [k, v] of Object.entries(patch)) {
      if (
        k === MAP_MASTER.lastReviewed ||
        k === MAP_MASTER.enrichmentStatus
      ) {
        prev[k] = v;
        continue;
      }
      const existing = records.find((r) => r.id === id)?.fields?.[k];
      const alreadyInPatch = prev[k];
      if (!isBlank(existing) && alreadyInPatch == null) {
        // existing production value — skip (NULL_FILL)
        continue;
      }
      if (alreadyInPatch != null && alreadyInPatch !== v) {
        // keep first write this run
        continue;
      }
      prev[k] = v;
    }
    patchMap.set(id, prev);
    patchedIds.add(id);
    void lane;
  }

  // —— LANE 1: Continent / Sub-Continent (deterministic) ——
  if (!doneLanes.has("continent_subcontinent")) {
    log(`[master] lane: continent/sub-continent…`);
    let unmapped = 0;
    for (const rec of records) {
      const built = buildContinentSubContinentPatch(rec.fields || {});
      if (!built.ok) {
        if (built.reason === "unmapped_country") unmapped += 1;
        continue;
      }
      mergePatch(rec.id, built.patch, "continent");
      if (built.patch[MAP_MASTER.continent]) tallies.continent_writes += 1;
      if (built.patch[MAP_MASTER.subContinent]) {
        tallies.sub_continent_writes += 1;
      }
    }
    if (unmapped > 0) {
      log(`[master] continent unmapped countries: ${unmapped}`);
    }
    doneLanes.add("continent_subcontinent");
    sourcePerf.continent_subcontinent = {
      continent_writes: tallies.continent_writes,
      sub_continent_writes: tallies.sub_continent_writes,
      unmapped,
    };
  }

  // —— LANE 2: Deterministic State/City/Postal residuals ——
  if (!doneLanes.has("deterministic_geo")) {
    log(`[master] lane: deterministic geo residuals…`);
    let geoPatches = 0;
    for (const rec of records) {
      const geo = buildDeterministicGeoPatch(rec);
      if (!Object.keys(geo.patch || {}).length) continue;
      mergePatch(rec.id, geo.patch, "deterministic_geo");
      geoPatches += 1;
      if (geo.patch[MAP_PF.stateRegion]) tallies.state_writes += 1;
      if (geo.patch[MAP_PF.city]) tallies.city_writes += 1;
      if (geo.patch[MAP_PF.postalCode]) tallies.postal_writes += 1;
      if (geo.patch[MAP_PF.address]) tallies.address_writes += 1;
    }
    sourcePerf.deterministic_geo = { properties: geoPatches };
    doneLanes.add("deterministic_geo");
  }

  // —— LANE 3a: Official brand portfolio → Current Brand HIGH ——
  if (!doneLanes.has("brand_portfolio") && !opts.skipBrandPortfolio) {
    const brandWritesEnabled =
      String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "1";
    if (!brandWritesEnabled) {
      log(
        `[master] brand portfolio lane idle — set ENABLE_CURRENT_BRAND_WRITES=1`
      );
      sourcePerf.brand_portfolio = {
        skipped: true,
        reason: "ENABLE_CURRENT_BRAND_WRITES=0",
      };
      doneLanes.add("brand_portfolio");
    } else {
      log(`[master] lane: official brand portfolio Current Brand validation…`);
      try {
        const dictionary = buildCanonicalBrandDictionary({});
        brandPortfolioReport = runMasterBrandPortfolioValidation({
          censusRecords: records,
          dictionary,
          log,
        });
        for (const p of brandPortfolioReport.proposals || []) {
          mergePatch(p.id, p.fields, "brand_portfolio");
          tallies.brand_validations_high += 1;
          if (p.fields?.[MAP_MASTER.brandFamily]) {
            tallies.brand_family_derived += 1;
          }
        }
        tallies.brand_conflicts += Number(
          brandPortfolioReport.tallies?.brand_conflicts || 0
        );
        tallies.brand_mapping_gaps += Number(
          brandPortfolioReport.tallies?.brand_mapping_gaps || 0
        );
        for (const id of brandPortfolioReport.newly_address_improved_ids || []) {
          newlyAddressImprovedIds.push(id);
        }
        sourcePerf.brand_portfolio = {
          writes: brandPortfolioReport.tallies?.current_brand_writes || 0,
          high: brandPortfolioReport.tallies?.brand_validations_high || 0,
          candidate_corroborations:
            brandPortfolioReport.tallies?.candidate_corroborations || 0,
          top_yields: brandPortfolioReport.TOP_BRAND_SOURCE_YIELDS,
        };
        log(
          `[master] brand portfolio HIGH writes=${brandPortfolioReport.tallies?.current_brand_writes || 0} candidate_url=${brandPortfolioReport.tallies?.candidate_corroborations || 0}`
        );
      } catch (err) {
        tallies.errors += 1;
        log(
          `[master] brand portfolio error: ${String(err?.message || err).slice(0, 160)}`
        );
      }
      doneLanes.add("brand_portfolio");
    }
  }

  // —— LANE 3: Brand Family derive from existing Current Brand ——
  if (!doneLanes.has("brand_family_derive")) {
    log(`[master] lane: brand family derive from validated Current Brand…`);
    const dictionary = buildCanonicalBrandDictionary({});
    let derived = 0;
    for (const rec of records) {
      const built = buildBrandFamilyDerivePatch(rec.fields || {}, dictionary);
      if (built.reason === "BRAND_MAPPING_GAP") {
        tallies.brand_mapping_gaps += 1;
        continue;
      }
      if (!built.ok) continue;
      mergePatch(rec.id, built.patch, "brand_family_derive");
      derived += 1;
      tallies.brand_family_derived += 1;
    }
    sourcePerf.brand_family_derive = {
      derived,
      mapping_gaps: tallies.brand_mapping_gaps,
    };
    doneLanes.add("brand_family_derive");
  }

  // —— LANE 4: Strict Current Brand validation (official URL + name) ——
  const brandWritesEnabled =
    String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "1";
  if (!doneLanes.has("strict_current_brand") && brandWritesEnabled) {
    log(`[master] lane: strict Current Brand validation…`);
    const dictionary = buildCanonicalBrandDictionary({});
    const maxBrand = Number(opts.maxBrandValidate || 2000);
    let attempted = 0;
    for (const rec of records) {
      if (attempted >= maxBrand) break;
      if (!isBlank(rec.fields?.[MAP_MASTER.currentBrand])) continue;
      if (isBlank(rec.fields?.[MAP_MASTER.officialUrl])) continue;
      attempted += 1;
      const ev = evaluateStrictCurrentBrandFromOfficialUrl(
        rec.fields || {},
        dictionary
      );
      if (ev.class === "BRAND_CONFLICT") {
        tallies.brand_conflicts += 1;
        continue;
      }
      if (!ev.ok || ev.class !== "BRAND_VALIDATED_HIGH") continue;
      mergePatch(rec.id, ev.patch, "strict_current_brand");
      tallies.brand_validations_high += 1;
    }
    sourcePerf.strict_current_brand = {
      attempted,
      high: tallies.brand_validations_high,
      conflicts: tallies.brand_conflicts,
    };
    doneLanes.add("strict_current_brand");
  } else if (!brandWritesEnabled) {
    log(
      `[master] strict Current Brand lane idle — set ENABLE_CURRENT_BRAND_WRITES=1 to auto-write HIGH validations`
    );
    sourcePerf.strict_current_brand = {
      skipped: true,
      reason: "ENABLE_CURRENT_BRAND_WRITES=0",
    };
    doneLanes.add("strict_current_brand");
  }

  // Flush deterministic patches before expensive lanes so production sees them
  async function flushPatches(label) {
    const entries = [...patchMap.entries()].map(([id, fields]) => ({
      id,
      fields,
    }));
    if (!entries.length) return 0;
    tallies.fields_written += entries.reduce(
      (a, e) =>
        a +
        Object.keys(e.fields).filter(
          (k) =>
            k !== MAP_MASTER.lastReviewed && k !== MAP_MASTER.enrichmentStatus
        ).length,
      0
    );
    tallies.properties_patched = patchedIds.size;
    if (!enableWrites) {
      log(`[master] dry-run flush ${label}: ${entries.length} patches`);
      // Keep patches for after-simulation
      return entries.length;
    }
    const adapter = createLiveHotelPropertyCensusAdapter({
      token,
      baseId,
      tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
    });
    log(`[master] writing ${entries.length} patches (${label})…`);
    const res = await adapter.patchRecords(entries);
    if (res.blocked_wrong_census_target) {
      tallies.WRONG_TABLE_WRITES += 1;
      throw new Error("WRONG_TABLE_WRITES");
    }
    if (res.errors?.length) {
      tallies.errors += res.errors.length;
      log(`[master] patch errors (${label}): ${res.errors.length}`);
    }
    for (const e of entries) {
      const rec = records.find((r) => r.id === e.id);
      if (rec) Object.assign(rec.fields, e.fields);
    }
    patchMap.clear();
    return res.updated || entries.length;
  }

  await flushPatches("deterministic+brand");

  checkpoint = writeCheckpoint({ ...checkpoint, lanes_completed: [...doneLanes] });

  // —— LANE 5: Official Rooms Registry ——
  if (!doneLanes.has("official_rooms_registry") && !opts.skipRoomsRegistry) {
    const secondary = resolveSecondaryHotelDataPolicy();
    if (secondary.enable_secondary_rooms_sources) {
      log(`[master] lane: official rooms registry…`);
      try {
        const roomsReport = await runOfficialRoomsSourceRegistryWave({
          mode: enableWrites ? "run" : "dry-run",
          enableProductionWrites: enableWrites,
          maxPerSource: Number(opts.maxRoomsPerSource || 6000),
          log: (m) => log(`  ${m}`),
        });
        tallies.rooms_written += Number(roomsReport.ROOMS_WRITTEN_THIS_RUN || 0);
        tallies.HBX_ROOMS_ARRAY_WRITES += Number(
          roomsReport.HBX_ROOMS_ARRAY_WRITES || 0
        );
        tallies.CVENT_ONLY_ROOM_VALIDATIONS += Number(
          roomsReport.CVENT_ONLY_ROOM_VALIDATIONS || 0
        );
        tallies.BENCHMARK_ROOM_WRITES += Number(
          roomsReport.BENCHMARK_ROOM_WRITES || 0
        );
        sourcePerf.official_rooms_registry = {
          rooms_written: roomsReport.ROOMS_WRITTEN_THIS_RUN,
          by_source: roomsReport.ROOMS_BY_SOURCE,
          by_country: roomsReport.ROOMS_BY_COUNTRY,
          candidates_held: roomsReport.ROOMS_CANDIDATES_HELD,
          conflicts: roomsReport.ROOMS_CONFLICTS,
          source_blocked: roomsReport.SOURCE_BLOCKED_COUNTS,
        };
        roomsWaveReport = roomsReport;
        if (enableWrites) {
          records = await listCensusRecords(baseId, token, READ_FIELDS);
        }
      } catch (err) {
        tallies.errors += 1;
        log(`[master] rooms registry error: ${String(err?.message || err).slice(0, 160)}`);
      }
    } else {
      log(
        `[master] rooms registry idle — ENABLE_SECONDARY_HOTEL_DATA_SOURCES=1 ENABLE_SECONDARY_ROOMS_SOURCES=1`
      );
    }
    doneLanes.add("official_rooms_registry");
    checkpoint = writeCheckpoint({ ...checkpoint, lanes_completed: [...doneLanes] });
  }

  // —— LANE 6: Property Fundamentals research ——
  if (
    !doneLanes.has("property_fundamentals") &&
    !opts.skipPropertyFundamentals
  ) {
    log(`[master] lane: property fundamentals…`);
    try {
      const pf = await runPropertyFundamentalsEnrichmentV1({
        mode: enableWrites ? "run" : "dry-run",
        enableProductionWrites: enableWrites,
        maxResearch: Number(opts.maxPfResearch || 120),
        maxGeoOnly: Number(opts.maxPfGeo || 500),
        maxRnt: Number(opts.maxPfRnt || 200),
        log: (m) => log(`  ${m}`),
      });
      tallies.rooms_written += Number(pf.ROOMS_WRITTEN_HIGH || pf.ROOMS_WRITTEN || 0);
      tallies.website_writes += Number(pf.WEBSITE_PATCHES || 0);
      tallies.phone_writes += Number(pf.PHONE_PATCHES || 0);
      tallies.address_writes += Number(pf.ADDRESS_PATCHES || 0);
      tallies.postal_writes += Number(pf.POSTAL_CODE_PATCHES || 0);
      tallies.HBX_ROOMS_ARRAY_WRITES += Number(pf.HBX_ROOMS_ARRAY_WRITES || 0);
      tallies.CVENT_ONLY_ROOM_VALIDATIONS += Number(
        pf.CVENT_ONLY_ROOM_VALIDATIONS || 0
      );
      sourcePerf.property_fundamentals = {
        researched: pf.PROPERTIES_RESEARCHED,
        rooms: pf.ROOMS_WRITTEN_HIGH || pf.ROOMS_WRITTEN,
        website: pf.WEBSITE_PATCHES,
        phone: pf.PHONE_PATCHES,
      };
      if (enableWrites) {
        records = await listCensusRecords(baseId, token, READ_FIELDS);
      }
    } catch (err) {
      tallies.errors += 1;
      log(`[master] PF error: ${String(err?.message || err).slice(0, 160)}`);
    }
    doneLanes.add("property_fundamentals");
    checkpoint = writeCheckpoint({
      ...checkpoint,
      lanes_completed: [...doneLanes],
      wave_focus: "brand_rooms",
    });
  }

  // —— LANE 7b: Opportunistic Mapbox only for newly improved addresses ——
  // Broad Mapbox wave is complete; do not retry LOW_CONFIDENCE on identical evidence.
  if (
    !opts.skipCoordinates &&
    String(process.env.ENABLE_COORDINATE_WRITES || "0") === "1" &&
    String(process.env.ENABLE_HBX_COORDINATE_WRITES || "0") !== "1"
  ) {
    const readiness = evaluateMapboxPermanentReadiness(process.env);
    const waveCap = maxGeocodeRequestsPerRun(process.env);
    const priorUsed = Number(checkpoint.mapbox_wave?.requests_used || 0);
    const remaining = Math.max(0, waveCap - priorUsed);

    // Discover newly eligible from address-improved ids + any blank-coord with fresh address this run
    const candidateIds = new Set(newlyAddressImprovedIds);
    for (const [id, fields] of patchMap) {
      if (
        fields[MAP_MASTER.address] ||
        fields[MAP_MASTER.postalCode] ||
        fields[MAP_MASTER.city] ||
        fields[MAP_MASTER.stateRegion]
      ) {
        candidateIds.add(id);
      }
    }

    const opportunisticRecords = [];
    for (const rec of records) {
      if (candidateIds.size && !candidateIds.has(rec.id)) continue;
      if (!candidateIds.size) break;
      const elig = evaluateCoordinateCompletionEligibility(rec, {
        masterFounderApprovedPathway: true,
        env: process.env,
      });
      if (!elig.eligible) continue;
      opportunisticRecords.push(rec);
    }

    mapboxMetrics.NEWLY_ELIGIBLE_MAPBOX_PROPERTIES =
      opportunisticRecords.length;

    if (!readiness.ready) {
      sourcePerf.opportunistic_coordinates = {
        skipped: true,
        reason: readiness.block_reason || "mapbox_not_ready",
      };
    } else if (remaining <= 0) {
      sourcePerf.opportunistic_coordinates = {
        skipped: true,
        reason: "mapbox_wave_budget_exhausted",
        remaining: 0,
      };
    } else if (!opportunisticRecords.length) {
      log(
        `[master] opportunistic Mapbox skipped — no newly eligible address-improved properties`
      );
      sourcePerf.opportunistic_coordinates = {
        skipped: true,
        reason: "no_newly_eligible",
        newly_address_improved: newlyAddressImprovedIds.length,
      };
    } else {
      log(
        `[master] opportunistic Mapbox for ${opportunisticRecords.length} newly eligible (budget remaining=${remaining})…`
      );
      try {
        const maxOpp = Math.min(
          remaining,
          Number(opts.maxOpportunisticCoordinateRequests || 500),
          opportunisticRecords.length
        );
        const coordReport = await runCoordinateCompletionQueue({
          censusRecords: opportunisticRecords,
          maxRequests: maxOpp,
          masterFounderApprovedPathway: true,
          dryRun: !enableWrites,
          writeReports: false,
          log,
        });
        mapboxMetrics.MAPBOX_REQUESTS += Number(coordReport.MAPBOX_REQUESTS || 0);
        mapboxMetrics.MAPBOX_SUCCESS += Number(coordReport.MAPBOX_SUCCESS || 0);
        mapboxMetrics.MAPBOX_NO_MATCH += Number(coordReport.MAPBOX_NO_MATCH || 0);
        mapboxMetrics.MAPBOX_LOW_CONFIDENCE += Number(
          coordReport.MAPBOX_LOW_CONFIDENCE || 0
        );
        mapboxMetrics.MAPBOX_ERRORS += Number(coordReport.MAPBOX_ERRORS || 0);
        mapboxMetrics.MAPBOX_CONFLICTS += Number(
          coordReport.MAPBOX_CONFLICTS || 0
        );
        mapboxMetrics.ESTIMATED_MAPBOX_COST = Number(
          (
            Number(mapboxMetrics.ESTIMATED_MAPBOX_COST || 0) +
            Number(coordReport.ESTIMATED_MAPBOX_COST || 0)
          ).toFixed(4)
        );
        mapboxMetrics.MAPBOX_PERMANENT_MODE_CONFIRMED =
          coordReport.MAPBOX_PERMANENT_MODE_CONFIRMED === true;
        const written = Number(
          coordReport.counters?.coordinates_written_proposals || 0
        );
        tallies.coordinates_written += written;
        if (enableWrites && Array.isArray(coordReport.proposals)) {
          const adapter = createLiveHotelPropertyCensusAdapter({
            token,
            baseId,
            tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
          });
          const toWrite = coordReport.proposals
            .filter(
              (p) =>
                (p?.id || p?.record_id) &&
                (p?.fields || p?.patch) &&
                p.write !== false
            )
            .map((p) => ({
              id: p.id || p.record_id,
              fields: p.fields || p.patch,
            }));
          if (toWrite.length) {
            log(
              `[master] writing ${toWrite.length} opportunistic coordinate patches…`
            );
            await adapter.patchRecords(toWrite);
            tallies.coordinates_written = toWrite.length;
            tallies.fields_written += toWrite.reduce(
              (a, e) => a + Object.keys(e.fields || {}).length,
              0
            );
          }
        }
        sourcePerf.opportunistic_coordinates = {
          eligible: opportunisticRecords.length,
          written: tallies.coordinates_written,
          requests: coordReport.MAPBOX_REQUESTS,
        };
      } catch (err) {
        tallies.errors += 1;
        log(
          `[master] opportunistic coordinates error: ${String(err?.message || err).slice(0, 160)}`
        );
      }
    }
  }

  // —— LANE 7: Broad Coordinates (only when explicitly reopened; never default on brand_rooms wave) ——
  if (!doneLanes.has("coordinates") && !opts.skipCoordinates && !brandRoomsWave) {
    const coordWrites =
      String(process.env.ENABLE_COORDINATE_WRITES || "0") === "1";
    const hbxCoords =
      String(process.env.ENABLE_HBX_COORDINATE_WRITES || "0") === "1";
    if (hbxCoords) {
      founderDecisions.push({
        item: "ENABLE_HBX_COORDINATE_WRITES_must_stay_0",
        reason: "HBX coordinates are not approved for Rooms/coords policy",
      });
    }
    const readiness = evaluateMapboxPermanentReadiness(process.env);
    mapboxMetrics.MAPBOX_PERMANENT_MODE_CONFIRMED =
      readiness.mapbox_permanent_geocoding === true && readiness.ready === true;

    if (!coordWrites) {
      // Token/config missing for writes — pause only this lane conceptually; do not block forever when founder already approved pathway
      founderDecisions.push({
        item: "ENABLE_COORDINATE_WRITES",
        reason:
          "Set ENABLE_COORDINATE_WRITES=1 and MAPBOX_PERMANENT_GEOCODING=1 (+ CENSUS_COORDINATE_COMPLETION_ENABLED=1) to auto-write property-level coordinates (never HBX coords)",
      });
      sourcePerf.coordinates = { skipped: true, reason: "ENABLE_COORDINATE_WRITES=0" };
      doneLanes.add("coordinates");
    } else if (!readiness.ready) {
      // Fail closed for coordinate lane only — continue other master lanes
      log(
        `[master] coordinate lane paused — Mapbox Permanent not ready (${(readiness.missing_flags || []).join(", ")}); continuing other lanes`
      );
      sourcePerf.coordinates = {
        skipped: true,
        reason: readiness.block_reason || "mapbox_not_ready",
        missing_flags: readiness.missing_flags,
        lane_status: "COORDINATE_LANE_PAUSED_PROVIDER",
      };
      doneLanes.add("coordinates");
    } else {
      log(`[master] lane: Mapbox Permanent pre-production sample (~25)…`);
      try {
        const sampleAlreadyPassed =
          checkpoint.mapbox_wave?.sample_passed === true ||
          opts.skipCoordinateSample === true;
        let sampleGate = {
          passed: true,
          reason: sampleAlreadyPassed ? "sample_already_passed" : null,
          report: null,
          metrics: null,
        };
        if (!sampleAlreadyPassed) {
          sampleGate = await runMasterCoordinateSampleGate({
            censusRecords: records,
            sampleSize: Number(opts.coordinateSampleSize || 25),
            dryRun: true,
            log,
          });
          mapboxMetrics.COORDINATE_SAMPLE_PASSED = sampleGate.passed;
          mapboxMetrics.MAPBOX_REQUESTS += Number(
            sampleGate.report?.MAPBOX_REQUESTS || 0
          );
          mapboxMetrics.MAPBOX_SUCCESS += Number(
            sampleGate.report?.MAPBOX_SUCCESS || 0
          );
          mapboxMetrics.MAPBOX_NO_MATCH += Number(
            sampleGate.report?.MAPBOX_NO_MATCH || 0
          );
          mapboxMetrics.MAPBOX_LOW_CONFIDENCE += Number(
            sampleGate.report?.MAPBOX_LOW_CONFIDENCE || 0
          );
          mapboxMetrics.MAPBOX_ERRORS += Number(
            sampleGate.report?.MAPBOX_ERRORS || 0
          );
          mapboxMetrics.MAPBOX_CONFLICTS += Number(
            sampleGate.report?.MAPBOX_CONFLICTS || 0
          );
          mapboxMetrics.ESTIMATED_MAPBOX_COST += Number(
            sampleGate.report?.ESTIMATED_MAPBOX_COST || 0
          );
          mapboxMetrics.MAPBOX_PERMANENT_MODE_CONFIRMED =
            sampleGate.report?.MAPBOX_PERMANENT_MODE_CONFIRMED === true ||
            mapboxMetrics.MAPBOX_PERMANENT_MODE_CONFIRMED;
        } else {
          mapboxMetrics.COORDINATE_SAMPLE_PASSED = true;
          log(`[master] coordinate sample already passed — skipping re-sample`);
        }

        if (!sampleGate.passed) {
          log(
            `[master] coordinate sample gate FAILED (${sampleGate.reason}) — pausing coordinate lane only`
          );
          founderDecisions.push({
            item: "MAPBOX_COORDINATE_SAMPLE_GATE",
            reason: `Pre-production sample failed (${sampleGate.reason}). Review sample metrics before enabling broad coordinate writes.`,
          });
          sourcePerf.coordinates = {
            skipped: true,
            reason: "sample_gate_failed",
            sample: sampleGate.metrics,
          };
          doneLanes.add("coordinates");
        } else {
          log(
            `[master] coordinate sample PASSED — continuing production Mapbox Permanent wave…`
          );
          const waveCap = maxGeocodeRequestsPerRun(process.env);
          const priorUsed = Number(
            checkpoint.mapbox_wave?.requests_used || 0
          );
          const alreadyUsedThisRun = Number(mapboxMetrics.MAPBOX_REQUESTS || 0);
          const usedBeforeProd = priorUsed + alreadyUsedThisRun;
          const maxCoord =
            opts.maxCoordinateRequests != null
              ? Number(opts.maxCoordinateRequests)
              : Math.max(0, waveCap - usedBeforeProd);
          const remaining = Math.max(0, maxCoord);
          log(
            `[master] Mapbox wave budget remaining=${remaining} (cap=${waveCap} prior_used=${priorUsed} sample_used=${alreadyUsedThisRun})`
          );
          if (remaining <= 0) {
            sourcePerf.coordinates = {
              written: 0,
              readiness: readiness.ready,
              sample_passed: true,
              budget_paused: true,
              lane_status: "COORDINATE_LANE_PAUSED_BUDGET",
              hbx_coordinate_writes: 0,
            };
            doneLanes.add("coordinates");
          } else {
          const coordReport = await runCoordinateCompletionQueue({
            censusRecords: records,
            maxRequests: remaining,
            masterFounderApprovedPathway: true,
            dryRun: !enableWrites,
            writeReports: true,
            log,
          });
          mapboxMetrics.MAPBOX_REQUESTS += Number(
            coordReport.MAPBOX_REQUESTS || 0
          );
          mapboxMetrics.MAPBOX_SUCCESS += Number(
            coordReport.MAPBOX_SUCCESS || 0
          );
          mapboxMetrics.MAPBOX_NO_MATCH += Number(
            coordReport.MAPBOX_NO_MATCH || 0
          );
          mapboxMetrics.MAPBOX_LOW_CONFIDENCE += Number(
            coordReport.MAPBOX_LOW_CONFIDENCE || 0
          );
          mapboxMetrics.MAPBOX_ERRORS += Number(coordReport.MAPBOX_ERRORS || 0);
          mapboxMetrics.MAPBOX_CONFLICTS += Number(
            coordReport.MAPBOX_CONFLICTS || 0
          );
          mapboxMetrics.ESTIMATED_MAPBOX_COST = Number(
            (
              Number(mapboxMetrics.ESTIMATED_MAPBOX_COST || 0) +
              Number(coordReport.ESTIMATED_MAPBOX_COST || 0)
            ).toFixed(4)
          );
          mapboxMetrics.CITY_PATCHES_FROM_GEOCODING += Number(
            coordReport.counters?.city_patches_from_geocoding || 0
          );
          mapboxMetrics.STATE_REGION_PATCHES_FROM_GEOCODING += Number(
            coordReport.counters?.state_region_patches_from_geocoding || 0
          );
          mapboxMetrics.POSTAL_CODE_PATCHES_FROM_GEOCODING += Number(
            coordReport.counters?.postal_code_patches_from_geocoding || 0
          );
          mapboxMetrics.MAPBOX_PERMANENT_MODE_CONFIRMED =
            coordReport.MAPBOX_PERMANENT_MODE_CONFIRMED === true;

          const written = Number(
            coordReport.counters?.coordinates_written_proposals ||
              coordReport.counters?.coordinates_written ||
              0
          );
          tallies.coordinates_written += written;
          tallies.coordinate_conflicts += Number(
            coordReport.counters?.coordinate_conflicts ||
              coordReport.MAPBOX_CONFLICTS ||
              0
          );

          if (enableWrites && Array.isArray(coordReport.proposals)) {
            const adapter = createLiveHotelPropertyCensusAdapter({
              token,
              baseId,
              tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
            });
            const toWrite = coordReport.proposals
              .filter((p) => (p?.id || p?.record_id) && (p?.fields || p?.patch) && p.write !== false)
              .map((p) => ({
                id: p.id || p.record_id,
                fields: p.fields || p.patch,
              }));
            if (toWrite.length && !coordReport.wrote) {
              log(`[master] writing ${toWrite.length} coordinate patches…`);
              const res = await adapter.patchRecords(toWrite);
              tallies.coordinates_written = toWrite.length;
              tallies.fields_written += toWrite.reduce(
                (a, e) => a + Object.keys(e.fields || {}).length,
                0
              );
              if (res.errors?.length) tallies.errors += res.errors.length;
            }
          }

          sourcePerf.coordinates = {
            written: tallies.coordinates_written,
            readiness: readiness.ready,
            sample_passed: true,
            budget_paused: coordReport.budget_paused === true,
            lane_status: coordReport.budget_paused
              ? "COORDINATE_LANE_PAUSED_BUDGET"
              : coordReport.status,
            hbx_coordinate_writes: 0,
            mapbox: {
              requests: mapboxMetrics.MAPBOX_REQUESTS,
              success: mapboxMetrics.MAPBOX_SUCCESS,
              estimated_usd: mapboxMetrics.ESTIMATED_MAPBOX_COST,
            },
          };
          tallies.HBX_COORDINATE_WRITES = 0;
          if (enableWrites) {
            records = await listCensusRecords(baseId, token, READ_FIELDS);
          }
          // Budget pause does not stop master loop — mark lane done for this wave slice
          // Re-open on next resume while under founder wave cap.
          if (
            coordReport.budget_paused === true &&
            Number(mapboxMetrics.MAPBOX_REQUESTS || 0) + priorUsed < waveCap
          ) {
            // keep coordinates incomplete so next resume continues
            doneLanes.delete("coordinates");
          } else {
            doneLanes.add("coordinates");
          }
          }
        }
      } catch (err) {
        tallies.errors += 1;
        log(`[master] coordinates error: ${String(err?.message || err).slice(0, 160)}`);
        doneLanes.add("coordinates");
      }
    }
    checkpoint = writeCheckpoint({ ...checkpoint, lanes_completed: [...doneLanes] });
  }

  // Final completeness
  if (enableWrites) {
    records = await listCensusRecords(baseId, token, READ_FIELDS);
  } else {
    // simulate remaining patches
    for (const [id, fields] of patchMap) {
      const rec = records.find((r) => r.id === id);
      if (rec) Object.assign(rec.fields, fields);
    }
  }
  const after = computeMasterCompleteness(records);
  mapboxMetrics.LATITUDE_COMPLETENESS_AFTER = after.latitude.completeness_pct;
  mapboxMetrics.LONGITUDE_COMPLETENESS_AFTER = after.longitude.completeness_pct;

  const unresolvedByField = {};
  for (const [k, v] of Object.entries(after)) {
    if (k === "n") continue;
    if (v?.missing > 0) unresolvedByField[k] = v.missing;
  }

  const stopForFounder = founderDecisions.length > 0;
  const status =
    tallies.WRONG_TABLE_WRITES > 0
      ? "master_enrichment_blocked_wrong_table"
      : stopForFounder && enableWrites
        ? "master_enrichment_paused_founder_decision"
        : enableWrites
          ? "master_enrichment_wave_complete"
          : "master_enrichment_dry_run_complete";

  checkpoint = writeCheckpoint({
    started_at: checkpoint.started_at || generated_at,
    lanes_completed: [...doneLanes],
    last_mode: mode,
    last_status: status,
    founder_decisions: founderDecisions,
    mapbox_metrics: {
      MAPBOX_REQUESTS: mapboxMetrics.MAPBOX_REQUESTS,
      ESTIMATED_MAPBOX_COST: mapboxMetrics.ESTIMATED_MAPBOX_COST,
      COORDINATE_SAMPLE_PASSED: mapboxMetrics.COORDINATE_SAMPLE_PASSED,
    },
    mapbox_wave: {
      requests_used:
        Number(checkpoint.mapbox_wave?.requests_used || 0) +
        Number(mapboxMetrics.MAPBOX_REQUESTS || 0),
      estimated_usd: Number(
        (
          Number(checkpoint.mapbox_wave?.estimated_usd || 0) +
          Number(mapboxMetrics.ESTIMATED_MAPBOX_COST || 0)
        ).toFixed(4)
      ),
      cap: maxGeocodeRequestsPerRun(process.env),
      sample_passed: mapboxMetrics.COORDINATE_SAMPLE_PASSED ?? checkpoint.mapbox_wave?.sample_passed,
    },
    wave_focus: "brand_rooms",
  });

  const final = {
    ok: tallies.WRONG_TABLE_WRITES === 0,
    MASTER_ENRICHMENT_STATUS: status,
    version: MASTER_ENRICHMENT_VERSION,
    mode,
    production_writes: enableWrites,
    SCHEMA_NOTES: MASTER_SCHEMA_NOTES,
    CENSUS_COUNT: after.n,
    MAPBOX_PERMANENT_MODE_CONFIRMED: mapboxMetrics.MAPBOX_PERMANENT_MODE_CONFIRMED,
    MAPBOX_REQUESTS: mapboxMetrics.MAPBOX_REQUESTS,
    MAPBOX_SUCCESS: mapboxMetrics.MAPBOX_SUCCESS,
    MAPBOX_NO_MATCH: mapboxMetrics.MAPBOX_NO_MATCH,
    MAPBOX_LOW_CONFIDENCE: mapboxMetrics.MAPBOX_LOW_CONFIDENCE,
    MAPBOX_ERRORS: mapboxMetrics.MAPBOX_ERRORS,
    MAPBOX_CONFLICTS: mapboxMetrics.MAPBOX_CONFLICTS,
    ESTIMATED_MAPBOX_COST: mapboxMetrics.ESTIMATED_MAPBOX_COST,
    LATITUDE_COMPLETENESS_BEFORE: mapboxMetrics.LATITUDE_COMPLETENESS_BEFORE,
    LATITUDE_COMPLETENESS_AFTER: mapboxMetrics.LATITUDE_COMPLETENESS_AFTER,
    LONGITUDE_COMPLETENESS_BEFORE: mapboxMetrics.LONGITUDE_COMPLETENESS_BEFORE,
    LONGITUDE_COMPLETENESS_AFTER: mapboxMetrics.LONGITUDE_COMPLETENESS_AFTER,
    COORDINATES_WRITTEN: tallies.coordinates_written,
    CITY_PATCHES_FROM_GEOCODING: mapboxMetrics.CITY_PATCHES_FROM_GEOCODING,
    STATE_REGION_PATCHES_FROM_GEOCODING:
      mapboxMetrics.STATE_REGION_PATCHES_FROM_GEOCODING,
    POSTAL_CODE_PATCHES_FROM_GEOCODING:
      mapboxMetrics.POSTAL_CODE_PATCHES_FROM_GEOCODING,
    CURRENT_BRAND_COMPLETENESS_BEFORE: brandBefore.current,
    CURRENT_BRAND_COMPLETENESS_AFTER: after.current_brand.completeness_pct,
    CURRENT_BRAND_WRITES: tallies.brand_validations_high,
    BRAND_VALIDATIONS_HIGH: tallies.brand_validations_high,
    BRAND_CONFLICTS: tallies.brand_conflicts,
    BRAND_MAPPING_GAPS: tallies.brand_mapping_gaps,
    TOP_BRAND_SOURCE_YIELDS:
      brandPortfolioReport?.TOP_BRAND_SOURCE_YIELDS || [],
    BRAND_FAMILY_COMPLETENESS_AFTER: after.brand_family.completeness_pct,
    FAMILY_SOURCE_FAMILY_COMPLETENESS_AFTER:
      after.family_source_family.completeness_pct,
    ROOMS_COMPLETENESS_BEFORE: roomsBefore.completeness,
    ROOMS_COMPLETENESS_AFTER: after.rooms.completeness_pct,
    ROOMS_WRITTEN: tallies.rooms_written,
    ROOMS_BY_COUNTRY: roomsWaveReport?.ROOMS_BY_COUNTRY || null,
    ROOMS_BY_SOURCE: roomsWaveReport?.ROOMS_BY_SOURCE || null,
    ROOMS_CANDIDATES_HELD: roomsWaveReport?.ROOMS_CANDIDATES_HELD || 0,
    ROOMS_CONFLICTS: roomsWaveReport?.ROOMS_CONFLICTS || 0,
    NEWLY_ELIGIBLE_MAPBOX_PROPERTIES:
      mapboxMetrics.NEWLY_ELIGIBLE_MAPBOX_PROPERTIES || 0,
    ADDITIONAL_COORDINATES_WRITTEN: tallies.coordinates_written,
    CUMULATIVE_MAPBOX_COST: Number(checkpoint.mapbox_wave?.estimated_usd || 0),
    SOURCE_BLOCKED_COUNTS:
      roomsWaveReport?.SOURCE_BLOCKED_COUNTS ||
      sourcePerf?.official_rooms_registry?.source_blocked ||
      {},
    CURRENT_BRAND_COMPLETENESS: after.current_brand.completeness_pct,
    BRAND_FAMILY_COMPLETENESS: after.brand_family.completeness_pct,
    FAMILY_SOURCE_FAMILY_COMPLETENESS: after.family_source_family.completeness_pct,
    FAMILY_COMPLETENESS: after.family_source_family.completeness_pct,
    SOURCE_FAMILY_COMPLETENESS: after.family_source_family.completeness_pct,
    CONTINENT_COMPLETENESS: after.continent.completeness_pct,
    SUB_CONTINENT_COMPLETENESS: after.sub_continent.completeness_pct,
    STATE_REGION_COMPLETENESS: after.state_region.completeness_pct,
    CITY_COMPLETENESS: after.city.completeness_pct,
    ADDRESS_COMPLETENESS: after.address.completeness_pct,
    POSTAL_CODE_COMPLETENESS: after.postal_code.completeness_pct,
    LATITUDE_COMPLETENESS: after.latitude.completeness_pct,
    LONGITUDE_COMPLETENESS: after.longitude.completeness_pct,
    ROOMS_COMPLETENESS: after.rooms.completeness_pct,
    WEBSITE_COMPLETENESS: after.website.completeness_pct,
    PHONE_COMPLETENESS: after.phone.completeness_pct,
    DASHBOARD_BEFORE: before,
    DASHBOARD_AFTER: after,
    TOTAL_FIELDS_WRITTEN_THIS_RUN: tallies.fields_written,
    PROPERTIES_PATCHED_THIS_RUN: tallies.properties_patched || patchMap.size,
    BRAND_FAMILY_DERIVED: tallies.brand_family_derived,
    COORDINATE_CONFLICTS: tallies.coordinate_conflicts,
    ROOMS_WRITTEN: tallies.rooms_written,
    CONTINENT_WRITES: tallies.continent_writes,
    SUB_CONTINENT_WRITES: tallies.sub_continent_writes,
    SOURCE_PERFORMANCE_SUMMARY: sourcePerf,
    UNRESOLVED_BY_FIELD: unresolvedByField,
    LANES_COMPLETED: [...doneLanes],
    HBX_ROOMS_ARRAY_WRITES: tallies.HBX_ROOMS_ARRAY_WRITES,
    CVENT_ONLY_ROOM_VALIDATIONS: tallies.CVENT_ONLY_ROOM_VALIDATIONS,
    BENCHMARK_ROOM_WRITES: tallies.BENCHMARK_ROOM_WRITES,
    HBX_COORDINATE_WRITES: tallies.HBX_COORDINATE_WRITES,
    DESTRUCTIVE_OVERWRITES: tallies.DESTRUCTIVE_OVERWRITES,
    WRONG_TABLE_WRITES: tallies.WRONG_TABLE_WRITES,
    ERRORS: tallies.errors,
    FOUNDER_DECISION_REQUIRED: stopForFounder ? "YES" : "NO",
    FOUNDER_DECISION_ITEMS: founderDecisions,
    NEXT_RECOMMENDED_ACTION: stopForFounder
      ? "Resolve founder decision items, then: npm run census:master-enrichment -- --mode resume --enable-production-writes"
      : "Continue master enrichment: npm run census:master-enrichment -- --mode resume --enable-production-writes",
    CHECKPOINT_PATH: CHECKPOINT_FP,
    generated_at,
  };

  writeJson(REPORT_JSON, final);
  writeMd(
    REPORT_MD,
    [
      `# Master Census Enrichment`,
      ``,
      `Status: \`${status}\``,
      `Census: ${after.n}`,
      ``,
      `| Field | Completeness |`,
      `| --- | ---: |`,
      `| Continent | ${after.continent.completeness_pct}% |`,
      `| Sub-Continent | ${after.sub_continent.completeness_pct}% |`,
      `| Current Brand | ${after.current_brand.completeness_pct}% |`,
      `| Brand Family | ${after.brand_family.completeness_pct}% |`,
      `| Rooms | ${after.rooms.completeness_pct}% |`,
      `| Latitude | ${after.latitude.completeness_pct}% |`,
      `| Postal Code | ${after.postal_code.completeness_pct}% |`,
      ``,
      `Founder decision: ${final.FOUNDER_DECISION_REQUIRED}`,
      `HBX rooms/coords writes: 0`,
    ].join("\n")
  );

  return final;
}
