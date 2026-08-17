/**
 * Autopilot coordinate_completion — Mapbox Permanent Geocoding for
 * Hotel Property Census Latitude / Longitude.
 *
 * Write target: Hotel Property Census only (tbl9aY5ijiuIzzWam).
 * Never Brand Setup / Brand Explorer / VIC / old Census.
 * Never owner/operator/dates / Recent Momentum / validation fields.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  AUTOPILOT_ALLOWED_WRITE_FIELDS,
  AUTOPILOT_FORBIDDEN_FIELDS,
  isForbiddenAutopilotField,
} from "./census-autopilot-field-allowlist.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { isValidCoordPair } from "./production-census-coordinate-extractor.js";
import {
  COORDINATE_COMPLETION_STATUSES,
  evaluateMapboxPermanentReadiness,
  estimateMapboxPermanentCost,
  maxGeocodeRequestsPerRun,
} from "./census-coordinate-provider.js";
import {
  MAPBOX_COORDINATE_STATUSES,
  normalizeGeocodeCacheKey,
  resolveMapboxCoordinates,
} from "./census-mapbox-coordinate-provider.js";
import { evaluateCoordinateIdentityGate } from "./census-core-identity-quality.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import { CANONICAL_PROPERTY_NAME_FIELD } from "./census-canonical-property-name.js";
import { isRejectedDiscoveryHost } from "./dataforseo-validated-write-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const COORDINATE_COMPLETION_VERSION =
  "census-coordinate-completion-v1";
export const COORDINATE_COMPLETION_QUEUE_ID = "coordinate_completion";

export const SAFE_PRODUCTION_USE_STATUSES = Object.freeze([
  "Census Only / Not Owner-Facing",
  "Census Only",
  "Internal Only",
]);

const COORD_WRITE_FIELDS = Object.freeze([
  MAP_FIRST_PASS.latitude,
  MAP_FIRST_PASS.longitude,
  MAP_FIRST_PASS.coordinateSourceType,
  MAP_FIRST_PASS.coordinateConfidence,
  MAP_FIRST_PASS.geocodeProvider,
  MAP_FIRST_PASS.geocodeMethod,
  MAP_FIRST_PASS.geocodeReviewedDate,
  MAP_FIRST_PASS.radarGeographyStatus,
  MAP_FIRST_PASS.radarDisplayStatus,
  MAP_FIRST_PASS.radarDisplayReason,
  MAP_FIRST_PASS.publicCensusEligibility,
  MAP_FIRST_PASS.publicDisplayConfidence,
  MAP_FIRST_PASS.publicDisplayReviewStatus,
  MAP_FIRST_PASS.lastReviewed,
  MAP_FIRST_PASS.enrichmentStatus,
]);

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function coordsNearlyEqual(aLat, aLng, bLat, bLng, tol = 0.0001) {
  return (
    Math.abs(Number(aLat) - Number(bLat)) <= tol &&
    Math.abs(Number(aLng) - Number(bLng)) <= tol
  );
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Eligibility for Mapbox Permanent coordinate completion.
 * @param {object} record
 * @param {{
 *   activeBrandScope?: boolean,
 *   allowMediumAddressWithProvenance?: boolean,
 *   mediumMatchHighPathway?: boolean,
 *   env?: NodeJS.ProcessEnv,
 * }} [opts]
 */
export function evaluateCoordinateCompletionEligibility(record, opts = {}) {
  const fields = record?.fields || {};
  const env = opts.env || process.env;

  const lat = numOrNull(fields[MAP_FIRST_PASS.latitude]);
  const lng = numOrNull(fields[MAP_FIRST_PASS.longitude]);
  const missingCoords = lat == null || lng == null;
  if (!missingCoords) {
    return { eligible: false, reason: "coordinates_already_present", lat, lng };
  }

  const address = String(fields[MAP_FIRST_PASS.address] || "").trim();
  if (!address) {
    return { eligible: false, reason: "no_address" };
  }
  if (!isStreetLevelAddress(address)) {
    return { eligible: false, reason: "city_or_non_street_address" };
  }

  const addrConf = String(fields[MAP_FIRST_PASS.addressConfidence] || "").trim();
  const mediumFlag =
    String(env.ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS || "0").trim() ===
    "1";
  const mediumMatchHighPathway =
    opts.mediumMatchHighPathway === true ||
    (opts.allowMediumAddressWithProvenance === true && mediumFlag) ||
    mediumFlag;

  const fromMediumAddress = addrConf === "Medium";
  if (addrConf !== "High") {
    const allowMedium =
      mediumMatchHighPathway &&
      fromMediumAddress &&
      !isBlank(fields[MAP_FIRST_PASS.addressSourceUrl]);
    if (!allowMedium) {
      return {
        eligible: false,
        reason: "address_confidence_not_high",
        address_confidence: addrConf || null,
      };
    }
  }

  const addressSourceUrl = String(
    fields[MAP_FIRST_PASS.addressSourceUrl] || ""
  ).trim();
  if (!addressSourceUrl) {
    return { eligible: false, reason: "missing_address_source_url" };
  }

  const addrHost = hostFromUrl(addressSourceUrl);
  if (addrHost && isRejectedDiscoveryHost(addrHost)) {
    return {
      eligible: false,
      reason: "address_source_ota_affiliate_rejected",
      address_source_host: addrHost,
    };
  }

  const city = String(fields[MAP_FIRST_PASS.city] || "").trim();
  if (!city || /^unknown$/i.test(city)) {
    return { eligible: false, reason: "missing_or_unknown_city" };
  }

  // Core identity quality gate — never Mapbox on dirty city/canonical
  const identityGate = evaluateCoordinateIdentityGate(record, {
    canonicalFieldExists: opts.canonicalFieldExists !== false,
  });
  if (!identityGate.allow_geocode) {
    return {
      eligible: false,
      reason: identityGate.reason || "blocked_dirty_core_identity",
      identity_gate_status: identityGate.gate_status,
    };
  }

  // Full Clean Core required for High-address path; Medium match_high pathway
  // uses clean identity gate only (founder-approved Autopilot rule).
  if (!(fromMediumAddress && mediumMatchHighPathway)) {
    const cleanCore = evaluateCleanCorePass(record, {
      canonicalFieldExists: opts.canonicalFieldExists !== false,
    });
    if (!cleanCore.pass) {
      return {
        eligible: false,
        reason: "clean_core_not_pass",
        clean_core_missing: cleanCore.missing,
        clean_core_blockers: cleanCore.blockers,
      };
    }
  }

  const canonical = String(fields[CANONICAL_PROPERTY_NAME_FIELD] || "").trim();
  if (!canonical) {
    return { eligible: false, reason: "blank_canonical_property_name" };
  }

  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  if (!country) {
    return { eligible: false, reason: "missing_country" };
  }

  if (fields[MAP_FIRST_PASS.humanReview] === true) {
    return { eligible: false, reason: "human_review_required" };
  }

  const sourceUrl = String(
    fields[MAP_FIRST_PASS.sourceUrl] ||
      fields[MAP_FIRST_PASS.officialUrl] ||
      addressSourceUrl ||
      ""
  ).trim();
  if (!sourceUrl) {
    return { eligible: false, reason: "missing_source_url" };
  }

  const name = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const identityKey = String(fields[MAP_FIRST_PASS.identityKey] || "").trim();
  if (!identityKey && !(name && brand) && !canonical) {
    return { eligible: false, reason: "property_identity_unclear" };
  }

  const useStatus = String(fields["Production Use Status"] || "").trim();
  if (
    useStatus &&
    !SAFE_PRODUCTION_USE_STATUSES.some(
      (s) => s.toLowerCase() === useStatus.toLowerCase()
    ) &&
    !/census\s*only/i.test(useStatus)
  ) {
    return {
      eligible: false,
      reason: "production_use_status_not_safe",
      production_use_status: useStatus,
    };
  }

  if (opts.activeBrandScope === false) {
    return { eligible: false, reason: "outside_active_brand_setup_scope" };
  }

  const steward = String(fields["Steward Review Status"] || "").trim();
  if (/hold|conflict|duplicate/i.test(steward)) {
    return {
      eligible: false,
      reason: "steward_hold_or_conflict",
      steward_status: steward,
    };
  }

  return {
    eligible: true,
    reason: null,
    existing_lat: lat,
    existing_lng: lng,
    address,
    city,
    country,
    property_name: name || canonical || null,
    brand: brand || null,
    identity_key: identityKey || null,
    from_medium_address: fromMediumAddress && mediumMatchHighPathway,
    address_confidence: addrConf || null,
    skip_reasons: [],
  };
}

/**
 * Build allowlisted write patch from a resolved Mapbox result.
 * High address → Coordinate Confidence High.
 * Medium match_high address pathway → Coordinate Confidence Medium
 * (Mapbox Permanent still required; never direct DataForSEO coords).
 */
export function buildCoordinateCompletionPatch(geoResult, opts = {}) {
  if (geoResult?.status !== MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) return null;
  // Provider must clear the Mapbox high bar; Census confidence may be Medium
  // when the input address was Medium (founder-approved pathway).
  if (
    geoResult.confidence !== "High" &&
    opts.fromMediumAddress !== true
  ) {
    return null;
  }
  if (!isValidCoordPair(geoResult.latitude, geoResult.longitude)) return null;

  const fromMedium = opts.fromMediumAddress === true;
  const coordConfidence = fromMedium ? "Medium" : "High";
  const today = opts.reviewedDate || todayIsoDate();
  const patch = {
    [MAP_FIRST_PASS.latitude]: geoResult.latitude,
    [MAP_FIRST_PASS.longitude]: geoResult.longitude,
    [MAP_FIRST_PASS.coordinateSourceType]: "official_address_geocode",
    [MAP_FIRST_PASS.coordinateConfidence]: coordConfidence,
    [MAP_FIRST_PASS.geocodeProvider]: "Mapbox",
    [MAP_FIRST_PASS.geocodeMethod]: "permanent_geocoding_official_address",
    [MAP_FIRST_PASS.geocodeReviewedDate]: today,
    [MAP_FIRST_PASS.radarGeographyStatus]: "Coordinates Available",
    [MAP_FIRST_PASS.radarDisplayStatus]: "Public Map Eligible",
    [MAP_FIRST_PASS.radarDisplayReason]: fromMedium
      ? "Property-level coordinates from Mapbox Permanent geocode of DataForSEO match_high Medium street address"
      : "Property-level coordinates from Mapbox Permanent official-address geocode (High)",
    [MAP_FIRST_PASS.publicCensusEligibility]: "Eligible",
    [MAP_FIRST_PASS.publicDisplayConfidence]: coordConfidence,
    [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Auto-Classified",
    [MAP_FIRST_PASS.lastReviewed]: today,
    [MAP_FIRST_PASS.enrichmentStatus]: "Partial",
  };

  const sanitized = {};
  for (const [k, v] of Object.entries(patch)) {
    if (isForbiddenAutopilotField(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
      throw new Error(`protected_field_in_coordinate_patch:${k}`);
    }
    if (!AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(k)) continue;
    if (!COORD_WRITE_FIELDS.includes(k)) continue;
    sanitized[k] = v;
  }
  return sanitized;
}

/**
 * Validate existing Census coords vs Mapbox result before write.
 */
export function validateExistingCoordinatesGate(fields, geoResult) {
  const lat = numOrNull(fields?.[MAP_FIRST_PASS.latitude]);
  const lng = numOrNull(fields?.[MAP_FIRST_PASS.longitude]);

  if (lat == null && lng == null) {
    return { action: "write", reason: "both_blank" };
  }

  if (
    lat != null &&
    lng != null &&
    isValidCoordPair(lat, lng) &&
    coordsNearlyEqual(lat, lng, geoResult.latitude, geoResult.longitude)
  ) {
    return { action: "skip_identical", reason: "existing_coords_identical" };
  }

  if (lat != null && lng != null && isValidCoordPair(lat, lng)) {
    return {
      action: "steward_review",
      reason: "existing_different_coordinates",
      existing: { lat, lng },
      proposed: { lat: geoResult.latitude, lng: geoResult.longitude },
    };
  }

  // Partial fill: one present — steward if the present value conflicts
  if (lat != null && Math.abs(lat - geoResult.latitude) > 0.0001) {
    return {
      action: "steward_review",
      reason: "existing_latitude_conflicts",
      existing: { lat, lng },
    };
  }
  if (lng != null && Math.abs(lng - geoResult.longitude) > 0.0001) {
    return {
      action: "steward_review",
      reason: "existing_longitude_conflicts",
      existing: { lat, lng },
    };
  }

  return { action: "write", reason: "fill_missing_coord_axis" };
}

function emptyCounters() {
  return {
    records_missing_coordinates: 0,
    records_eligible_for_mapbox: 0,
    records_eligible_medium_address: 0,
    records_geocoded: 0,
    coordinates_written_proposals: 0,
    medium_address_coordinate_proposals: 0,
    high_address_coordinate_proposals: 0,
    skipped_no_address: 0,
    skipped_address_confidence_not_high: 0,
    skipped_city_or_unknown: 0,
    skipped_other: 0,
    mapbox_rejects: 0,
    city_centroid_rejected: 0,
    zero_zero_rejected: 0,
    country_mismatch_rejected: 0,
    low_confidence: 0,
    provider_errors: 0,
    steward_review_cases: 0,
    provider_decision_needed_cases: 0,
    skipped_identical: 0,
    cache_hits: 0,
    mapbox_requests: 0,
  };
}

/**
 * Run coordinate_completion against in-memory Census records.
 *
 * @param {{
 *   censusRecords?: object[],
 *   env?: object,
 *   fetchImpl?: typeof fetch,
 *   maxRequests?: number,
 *   runDir?: string|null,
 *   writeReports?: boolean,
 *   dryRun?: boolean,
 *   log?: Function,
 * }} opts
 */
export async function runCoordinateCompletionQueue(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const sot = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId,
  });
  if (!sot.ok) {
    return {
      ok: false,
      status: COORDINATE_COMPLETION_STATUSES.BLOCKED,
      error: BLOCKED_WRONG_CENSUS_TARGET,
      source_of_truth: sot,
      proposals: [],
      counters: emptyCounters(),
    };
  }

  const readiness = evaluateMapboxPermanentReadiness(env);
  const maxReq = opts.maxRequests ?? maxGeocodeRequestsPerRun(env);
  const counters = emptyCounters();
  const proposals = [];
  const providerDecisionNeeded = [];
  const stewardReview = [];
  const failures = [];
  const cache = new Map();
  const failedKeysThisRun = new Set();

  const records = opts.censusRecords || [];
  const missing = [];
  const eligible = [];

  for (const rec of records) {
    const fields = rec.fields || {};
    const lat = numOrNull(fields[MAP_FIRST_PASS.latitude]);
    const lng = numOrNull(fields[MAP_FIRST_PASS.longitude]);
    if (lat == null || lng == null) {
      counters.records_missing_coordinates += 1;
      missing.push(rec.id);
    }

    const elig = evaluateCoordinateCompletionEligibility(rec, {
      activeBrandScope: opts.activeBrandScope !== false,
      allowMediumAddressWithProvenance:
        opts.allowMediumAddressWithProvenance === true,
      mediumMatchHighPathway: opts.mediumMatchHighPathway === true,
      env,
    });
    if (!elig.eligible) {
      if (elig.reason === "no_address" || elig.reason === "city_or_non_street_address") {
        counters.skipped_no_address += 1;
      } else if (elig.reason === "address_confidence_not_high") {
        counters.skipped_address_confidence_not_high += 1;
      } else if (elig.reason === "missing_or_unknown_city") {
        counters.skipped_city_or_unknown += 1;
      } else if (
        elig.reason === "address_source_ota_affiliate_rejected" ||
        elig.reason === "clean_core_not_pass" ||
        elig.reason === "blocked_dirty_core_identity"
      ) {
        counters.mapbox_rejects += 1;
        counters.skipped_other += 1;
      } else if (lat == null || lng == null) {
        counters.skipped_other += 1;
      }
      continue;
    }
    if (elig.from_medium_address) {
      counters.records_eligible_medium_address += 1;
    }
    eligible.push({ rec, elig });
  }
  counters.records_eligible_for_mapbox = eligible.length;

  if (!readiness.ready) {
    for (const { rec, elig } of eligible) {
      providerDecisionNeeded.push({
        record_id: rec.id,
        identity_key: elig.identity_key,
        property_name: elig.property_name,
        reason: readiness.block_reason,
        missing_flags: readiness.missing_flags,
      });
      counters.provider_decision_needed_cases += 1;
    }

    const report = finalizeReport({
      status: COORDINATE_COMPLETION_STATUSES.READY_PROVIDER_NEEDED,
      readiness,
      counters,
      proposals,
      providerDecisionNeeded,
      stewardReview,
      failures,
      maxReq,
      env,
      sot,
    });
    maybeWriteReports(report, opts);
    log(
      `[coordinate_completion] provider_decision_needed eligible=${eligible.length} (Mapbox env incomplete)`
    );
    return report;
  }

  let requests = 0;
  for (const { rec, elig } of eligible) {
    if (requests >= maxReq) {
      providerDecisionNeeded.push({
        record_id: rec.id,
        identity_key: elig.identity_key,
        property_name: elig.property_name,
        reason: "max_geocode_requests_per_run_reached",
      });
      counters.provider_decision_needed_cases += 1;
      continue;
    }

    const fields = rec.fields || {};
    const cacheKey = normalizeGeocodeCacheKey({
      address: elig.address,
      city: elig.city,
      stateRegion: fields[MAP_FIRST_PASS.stateRegion],
      country: elig.country,
    });

    if (failedKeysThisRun.has(cacheKey)) {
      failures.push({
        record_id: rec.id,
        reason: "same_run_failed_address_skip",
        cache_key: cacheKey,
      });
      continue;
    }

    let geo;
    if (cache.has(cacheKey)) {
      geo = cache.get(cacheKey);
      counters.cache_hits += 1;
    } else {
      geo = await resolveMapboxCoordinates(
        {
          propertyName: elig.property_name,
          brand: elig.brand,
          address: elig.address,
          city: elig.city,
          stateRegion: fields[MAP_FIRST_PASS.stateRegion],
          country: elig.country,
          sourceUrl: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.addressSourceUrl],
        },
        { env, fetchImpl: opts.fetchImpl }
      );
      cache.set(cacheKey, geo);
      requests += 1;
      counters.mapbox_requests += 1;
      counters.records_geocoded += 1;
    }

    if (geo.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) {
      const gate = validateExistingCoordinatesGate(fields, geo);
      if (gate.action === "skip_identical") {
        counters.skipped_identical += 1;
        continue;
      }
      if (gate.action === "steward_review") {
        counters.steward_review_cases += 1;
        stewardReview.push({
          record_id: rec.id,
          identity_key: elig.identity_key,
          property_name: elig.property_name,
          reason: gate.reason,
          existing: gate.existing,
          proposed: gate.proposed,
        });
        continue;
      }

      const fromMedium = elig.from_medium_address === true;
      const patch = buildCoordinateCompletionPatch(geo, {
        fromMediumAddress: fromMedium,
      });
      if (!patch) {
        counters.low_confidence += 1;
        counters.mapbox_rejects += 1;
        failedKeysThisRun.add(cacheKey);
        continue;
      }

      const confidence = fromMedium ? "Medium" : "High";
      proposals.push({
        record_id: rec.id,
        identity_key: elig.identity_key,
        property_name: elig.property_name,
        brand: elig.brand,
        family: fields[MAP_FIRST_PASS.family] || null,
        queue: COORDINATE_COMPLETION_QUEUE_ID,
        action: fromMedium
          ? "propose_medium_address_mapbox_write"
          : "propose_high_write",
        confidence,
        from_medium_address: fromMedium,
        write_allowed_now: true,
        patch,
        current_fields: {
          [MAP_FIRST_PASS.latitude]: fields[MAP_FIRST_PASS.latitude] ?? null,
          [MAP_FIRST_PASS.longitude]: fields[MAP_FIRST_PASS.longitude] ?? null,
        },
        source_url: fields[MAP_FIRST_PASS.addressSourceUrl] || fields[MAP_FIRST_PASS.sourceUrl],
        method: "permanent_geocoding_official_address",
        provider: "Mapbox",
        geocode_status: geo.status,
        notes: geo.reason,
      });
      counters.coordinates_written_proposals += 1;
      if (fromMedium) counters.medium_address_coordinate_proposals += 1;
      else counters.high_address_coordinate_proposals += 1;
      continue;
    }

    // Non-high outcomes
    failedKeysThisRun.add(cacheKey);
    counters.mapbox_rejects += 1;
    const bucket = {
      record_id: rec.id,
      identity_key: elig.identity_key,
      property_name: elig.property_name,
      status: geo.status,
      reason: geo.reason,
      from_medium_address: elig.from_medium_address === true,
    };
    if (geo.status === MAPBOX_COORDINATE_STATUSES.CITY_CENTROID_REJECTED) {
      counters.city_centroid_rejected += 1;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.ZERO_ZERO_REJECTED) {
      counters.zero_zero_rejected += 1;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.COUNTRY_MISMATCH) {
      counters.country_mismatch_rejected += 1;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR) {
      counters.provider_errors += 1;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.PROVIDER_DECISION_NEEDED) {
      counters.provider_decision_needed_cases += 1;
      providerDecisionNeeded.push(bucket);
      continue;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.STEWARD_REVIEW_REQUIRED) {
      counters.steward_review_cases += 1;
      stewardReview.push(bucket);
      continue;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.NO_ADDRESS) {
      counters.skipped_no_address += 1;
    } else {
      counters.low_confidence += 1;
    }
    failures.push(bucket);
  }

  let status = COORDINATE_COMPLETION_STATUSES.READY_FOR_PRODUCTION_CYCLE;
  if (counters.coordinates_written_proposals > 0 && !stewardReview.length && !providerDecisionNeeded.length) {
    status = opts.dryRun === false
      ? COORDINATE_COMPLETION_STATUSES.APPLIED_CLEAN
      : COORDINATE_COMPLETION_STATUSES.READY_FOR_PRODUCTION_CYCLE;
  } else if (
    stewardReview.length ||
    providerDecisionNeeded.length ||
    counters.low_confidence ||
    counters.city_centroid_rejected ||
    counters.provider_errors
  ) {
    if (counters.coordinates_written_proposals > 0) {
      status = COORDINATE_COMPLETION_STATUSES.PARTIAL_PROVIDER_OR_STEWARD;
    } else if (!readiness.ready) {
      status = COORDINATE_COMPLETION_STATUSES.READY_PROVIDER_NEEDED;
    } else {
      status = COORDINATE_COMPLETION_STATUSES.PARTIAL_PROVIDER_OR_STEWARD;
    }
  }

  const report = finalizeReport({
    status,
    readiness,
    counters,
    proposals,
    providerDecisionNeeded,
    stewardReview,
    failures,
    maxReq,
    env,
    sot,
  });
  maybeWriteReports(report, opts);
  log(
    `[coordinate_completion] status=${status} proposals=${proposals.length} requests=${counters.mapbox_requests}`
  );
  return report;
}

function finalizeReport({
  status,
  readiness,
  counters,
  proposals,
  providerDecisionNeeded,
  stewardReview,
  failures,
  maxReq,
  env,
  sot,
}) {
  const cost = estimateMapboxPermanentCost(counters.mapbox_requests, env);
  return {
    ok: status !== COORDINATE_COMPLETION_STATUSES.BLOCKED,
    version: COORDINATE_COMPLETION_VERSION,
    queue_id: COORDINATE_COMPLETION_QUEUE_ID,
    generated_at: new Date().toISOString(),
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    source_of_truth: sot,
    provider_readiness: readiness,
    max_geocode_requests_per_run: maxReq,
    counters,
    estimated_cost: cost,
    exact_fields_written: [...COORD_WRITE_FIELDS],
    proposals,
    provider_decision_needed: providerDecisionNeeded,
    steward_review: stewardReview,
    failures: failures.slice(0, 200),
    constraints: {
      temporary_geocoding_blocked: true,
      nominatim_blocked: true,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      owner_operator_date_writes: false,
    },
  };
}

function maybeWriteReports(report, opts) {
  if (opts.writeReports === false) return;
  const outDir =
    opts.runDir ||
    path.join(ROOT, "reports", "research-engine-v2");
  fs.mkdirSync(outDir, { recursive: true });
  const jsonPath = path.join(outDir, "production-census-mapbox-coordinate-completion.json");
  const mdPath = path.join(outDir, "production-census-mapbox-coordinate-completion.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, renderCoordinateCompletionMarkdown(report), "utf8");
  report.report_paths = { json: jsonPath, md: mdPath };
}

export function renderCoordinateCompletionMarkdown(report) {
  const c = report.counters || {};
  const cost = report.estimated_cost || {};
  return `# Production Census — Mapbox Coordinate Completion

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Queue:** \`${report.queue_id}\`  
**Write target:** ${report.write_target?.base} → ${report.write_target?.table} (\`${report.write_target?.table_id}\`)

## Provider

- Ready: **${report.provider_readiness?.ready ? "yes" : "no"}**
- Missing flags: ${(report.provider_readiness?.missing_flags || []).join(", ") || "none"}
- Temporary geocoding: **blocked**
- Nominatim: **blocked**
- Max requests / run: ${report.max_geocode_requests_per_run}

## Counts

| Metric | Count |
|--------|------:|
| Records missing coordinates | ${c.records_missing_coordinates ?? 0} |
| Eligible for Mapbox | ${c.records_eligible_for_mapbox ?? 0} |
| Eligible via Medium match_high address | ${c.records_eligible_medium_address ?? 0} |
| Geocoded (API or cache) | ${c.records_geocoded ?? 0} |
| Mapbox API requests | ${c.mapbox_requests ?? 0} |
| Cache hits | ${c.cache_hits ?? 0} |
| Coordinate write proposals | ${c.coordinates_written_proposals ?? 0} |
| Medium-address Mapbox proposals | ${c.medium_address_coordinate_proposals ?? 0} |
| High-address Mapbox proposals | ${c.high_address_coordinate_proposals ?? 0} |
| Mapbox rejects | ${c.mapbox_rejects ?? 0} |
| Skipped — no address | ${c.skipped_no_address ?? 0} |
| Skipped — Address Confidence not High | ${c.skipped_address_confidence_not_high ?? 0} |
| City-centroid rejected | ${c.city_centroid_rejected ?? 0} |
| 0,0 rejected | ${c.zero_zero_rejected ?? 0} |
| Country mismatch rejected | ${c.country_mismatch_rejected ?? 0} |
| Low confidence | ${c.low_confidence ?? 0} |
| Provider errors | ${c.provider_errors ?? 0} |
| Steward review | ${c.steward_review_cases ?? 0} |
| Provider decision needed | ${c.provider_decision_needed_cases ?? 0} |
| Skipped identical | ${c.skipped_identical ?? 0} |

## Cost

- Requests: ${cost.requests ?? 0}
- Estimated USD: ${cost.estimated_usd ?? "n/a"} (${cost.basis || "n/a"})
- Pricing configured: ${cost.pricing_configured ? "yes" : "no (request count primary)"}

## Exact fields written (High proposals)

${(report.exact_fields_written || []).map((f) => `- ${f}`).join("\n")}

## Expected values

- Coordinate Source Type = \`official_address_geocode\`
- Coordinate Confidence = \`High\`
- Geocode Provider = \`Mapbox\`
- Geocode Method = \`permanent_geocoding_official_address\`

## Constraints

- Hotel Property Census only
- No Brand Setup / Brand Explorer / owner / operator / date / Recent Momentum / validation writes
`;
}

/**
 * Write durable docs + reports copies under repo reports/docs.
 */
export function writeCoordinateCompletionArtifacts(report, root = ROOT) {
  const reportsDir = path.join(root, "reports", "research-engine-v2");
  const docsDir = path.join(root, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "production-census-mapbox-coordinate-completion.json");
  const mdPath = path.join(reportsDir, "production-census-mapbox-coordinate-completion.md");
  const docsPath = path.join(docsDir, "production-census-mapbox-coordinate-completion.md");
  const md = renderCoordinateCompletionMarkdown(report);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(
    docsPath,
    `# Mapbox Permanent Geocoding — Census Coordinate Completion

${md}

## Commands

\`\`\`bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \\
  --strategy fastest-safe \\
  --queue coordinate_completion \\
  --run-until-complete \\
  --batch-size 250
\`\`\`

Required env: \`MAPBOX_ACCESS_TOKEN\`, \`MAPBOX_PERMANENT_GEOCODING=1\`, \`CENSUS_COORDINATE_COMPLETION_ENABLED=1\`, \`GEOCODING_PROVIDER=mapbox\`, \`MAX_GEOCODE_REQUESTS_PER_RUN=250\`.
`,
    "utf8"
  );
  return { jsonPath, mdPath, docsPath };
}
