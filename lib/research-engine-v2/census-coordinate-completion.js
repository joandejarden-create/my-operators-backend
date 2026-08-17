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
  evaluateMapboxBudgetGuard,
  estimateMapboxPermanentCost,
  maxGeocodeRequestsPerRun,
} from "./census-coordinate-provider.js";
import { POSTAL_CODE_FIELD } from "./census-postal-code-v1.js";
import {
  MAPBOX_COORDINATE_STATUSES,
  normalizeGeocodeCacheKey,
  resolveMapboxCoordinates,
} from "./census-mapbox-coordinate-provider.js";
import { evaluateCoordinateIdentityGate } from "./census-core-identity-quality.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import { CANONICAL_PROPERTY_NAME_FIELD } from "./census-canonical-property-name.js";
import { isRejectedDiscoveryHost } from "./census-discovery-host-policy.js";

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
  // Founder-approved NULL_FILL geography when Mapbox context agrees
  MAP_FIRST_PASS.city,
  MAP_FIRST_PASS.stateRegion,
  POSTAL_CODE_FIELD,
]);

export const MASTER_MAPBOX_COORDINATE_CLASS = Object.freeze({
  HIGH: "COORDINATE_HIGH",
  CANDIDATE: "COORDINATE_CANDIDATE",
  CONFLICT: "COORDINATE_CONFLICT",
  UNRESOLVED: "COORDINATE_UNRESOLVED",
});

const CARIBBEAN_COUNTRIES = new Set(
  [
    "Antigua and Barbuda",
    "Aruba",
    "Bahamas",
    "Barbados",
    "Bonaire",
    "Cayman Islands",
    "Cuba",
    "Curaçao",
    "Curacao",
    "Dominica",
    "Dominican Republic",
    "Grenada",
    "Guadeloupe",
    "Haiti",
    "Jamaica",
    "Martinique",
    "Puerto Rico",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Sint Maarten",
    "Trinidad and Tobago",
    "U.S. Virgin Islands",
    "United States Virgin Islands",
    "British Virgin Islands",
  ].map((c) => c.toLowerCase())
);

const CENTRAL_AMERICA_COUNTRIES = new Set(
  [
    "Belize",
    "Costa Rica",
    "El Salvador",
    "Guatemala",
    "Honduras",
    "Nicaragua",
    "Panama",
  ].map((c) => c.toLowerCase())
);

const SOUTH_AMERICA_COUNTRIES = new Set(
  [
    "Argentina",
    "Bolivia",
    "Brazil",
    "Chile",
    "Colombia",
    "Ecuador",
    "French Guiana",
    "Guyana",
    "Paraguay",
    "Peru",
    "Suriname",
    "Uruguay",
    "Venezuela",
  ].map((c) => c.toLowerCase())
);

/**
 * CALA sample bucket for pre-production Mapbox gate.
 * @param {object} fields
 */
export function classifyMasterCoordinateSampleBucket(fields = {}) {
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const c = country.toLowerCase();
  if (c === "mexico") return "Mexico";
  if (c === "brazil") return "Brazil";
  if (CARIBBEAN_COUNTRIES.has(c)) return "Caribbean";
  if (CENTRAL_AMERICA_COUNTRIES.has(c)) return "Central America";
  if (SOUTH_AMERICA_COUNTRIES.has(c) && c !== "brazil") return "South America";
  return null;
}

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
 *   masterFounderApprovedPathway?: boolean,
 *   env?: NodeJS.ProcessEnv,
 * }} [opts]
 */
export function evaluateCoordinateCompletionEligibility(record, opts = {}) {
  const fields = record?.fields || {};
  const env = opts.env || process.env;
  const masterPathway =
    opts.masterFounderApprovedPathway === true ||
    String(env.CENSUS_MASTER_MAPBOX_STRUCTURED_ADDRESS_PATHWAY || "0").trim() ===
      "1";

  const lat = numOrNull(fields[MAP_FIRST_PASS.latitude]);
  const lng = numOrNull(fields[MAP_FIRST_PASS.longitude]);
  const missingBoth = lat == null && lng == null;
  const missingOne = (lat == null) !== (lng == null);
  if (!missingBoth && !missingOne) {
    return { eligible: false, reason: "coordinates_already_present", lat, lng };
  }
  // Mixed-source partial pairs: do not geocode blindly — route as conflict/hold
  if (missingOne) {
    return {
      eligible: false,
      reason: "partial_coordinate_pair_hold",
      classification: MASTER_MAPBOX_COORDINATE_CLASS.CONFLICT,
      lat,
      lng,
    };
  }

  const address = String(fields[MAP_FIRST_PASS.address] || "").trim();
  if (!address) {
    return {
      eligible: false,
      reason: "no_address",
      classification: MASTER_MAPBOX_COORDINATE_CLASS.UNRESOLVED,
    };
  }
  if (!isStreetLevelAddress(address)) {
    return {
      eligible: false,
      reason: "city_or_non_street_address",
      classification: MASTER_MAPBOX_COORDINATE_CLASS.UNRESOLVED,
    };
  }

  const city = String(fields[MAP_FIRST_PASS.city] || "").trim();
  if (!city || /^unknown$/i.test(city)) {
    return {
      eligible: false,
      reason: "missing_or_unknown_city",
      classification: MASTER_MAPBOX_COORDINATE_CLASS.UNRESOLVED,
    };
  }

  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  if (!country) {
    return {
      eligible: false,
      reason: "missing_country",
      classification: MASTER_MAPBOX_COORDINATE_CLASS.UNRESOLVED,
    };
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
      classification: MASTER_MAPBOX_COORDINATE_CLASS.UNRESOLVED,
    };
  }

  if (fields[MAP_FIRST_PASS.humanReview] === true) {
    return { eligible: false, reason: "human_review_required" };
  }

  const steward = String(fields["Steward Review Status"] || "").trim();
  if (/hold|conflict|duplicate/i.test(steward)) {
    return {
      eligible: false,
      reason: "steward_hold_or_conflict",
      steward_status: steward,
    };
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

  const stateRegion = String(fields[MAP_FIRST_PASS.stateRegion] || "").trim();
  const postal = String(fields[POSTAL_CODE_FIELD] || "").trim();
  const name = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const identityKey = String(fields[MAP_FIRST_PASS.identityKey] || "").trim();
  const canonical = String(fields[CANONICAL_PROPERTY_NAME_FIELD] || "").trim();

  // —— Founder-approved master pathway: strong structured address, not Address Confidence High ——
  if (masterPathway) {
    if (!identityKey && !(name && brand) && !canonical && !name) {
      return { eligible: false, reason: "property_identity_unclear" };
    }
    const inputBasis = [
      "Address",
      postal ? "Postal" : null,
      city ? "City" : null,
      stateRegion ? "State/Region" : null,
      country ? "Country" : null,
    ]
      .filter(Boolean)
      .join("+");
    // Prefer postal + state when present (priority only — not hard required)
    let priority = 100;
    if (postal) priority -= 40;
    if (stateRegion) priority -= 20;
    if (name) priority -= 5;
    return {
      eligible: true,
      reason: null,
      existing_lat: lat,
      existing_lng: lng,
      address,
      city,
      country,
      state_region: stateRegion || null,
      postal_code: postal || null,
      property_name: name || canonical || null,
      brand: brand || null,
      identity_key: identityKey || null,
      from_medium_address: false,
      address_confidence: String(fields[MAP_FIRST_PASS.addressConfidence] || "").trim() || null,
      master_founder_approved_pathway: true,
      coordinate_input_basis: inputBasis,
      priority,
      classification: MASTER_MAPBOX_COORDINATE_CLASS.CANDIDATE,
      skip_reasons: [],
    };
  }

  // —— Legacy Autopilot pathway: Address Confidence High + Address Source URL + Clean Core ——
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

  if (!canonical) {
    return { eligible: false, reason: "blank_canonical_property_name" };
  }

  if (!identityKey && !(name && brand) && !canonical) {
    return { eligible: false, reason: "property_identity_unclear" };
  }

  return {
    eligible: true,
    reason: null,
    existing_lat: lat,
    existing_lng: lng,
    address,
    city,
    country,
    state_region: stateRegion || null,
    postal_code: postal || null,
    property_name: name || canonical || null,
    brand: brand || null,
    identity_key: identityKey || null,
    from_medium_address: fromMediumAddress && mediumMatchHighPathway,
    address_confidence: addrConf || null,
    master_founder_approved_pathway: false,
    coordinate_input_basis: postal
      ? "Address+Postal+City+Country"
      : "Address+City+Country",
    priority: postal ? 50 : 80,
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
  if (geoResult.permanent_mode_confirmed !== true && geoResult.permanent !== true) {
    return null;
  }

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

  // NULL_FILL City / State / Postal when Mapbox context agrees with property evidence
  if (opts.allowGeoNullFill === true && opts.fields) {
    const fields = opts.fields;
    const ctx = geoResult.context || {};
    const geoPatches = { city: 0, state_region: 0, postal_code: 0 };
    if (
      isBlank(fields[MAP_FIRST_PASS.city]) &&
      ctx.city &&
      String(ctx.city).trim()
    ) {
      patch[MAP_FIRST_PASS.city] = String(ctx.city).trim();
      geoPatches.city = 1;
    }
    if (
      isBlank(fields[MAP_FIRST_PASS.stateRegion]) &&
      ctx.region &&
      String(ctx.region).trim()
    ) {
      patch[MAP_FIRST_PASS.stateRegion] = String(ctx.region).trim();
      geoPatches.state_region = 1;
    }
    if (
      isBlank(fields[POSTAL_CODE_FIELD]) &&
      ctx.postcode &&
      String(ctx.postcode).trim()
    ) {
      // Only fill postal when Census postal blank AND Mapbox postcode present
      // Prefer agreement with existing city/country already validated for HIGH
      patch[POSTAL_CODE_FIELD] = String(ctx.postcode).trim();
      geoPatches.postal_code = 1;
    }
    opts._geoNullFillCounts = geoPatches;
  }

  const sanitized = {};
  for (const [k, v] of Object.entries(patch)) {
    if (isForbiddenAutopilotField(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
      throw new Error(`protected_field_in_coordinate_patch:${k}`);
    }
    if (!AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(k) && k !== POSTAL_CODE_FIELD) {
      continue;
    }
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
    records_eligible_master_pathway: 0,
    records_geocoded: 0,
    coordinates_written_proposals: 0,
    medium_address_coordinate_proposals: 0,
    high_address_coordinate_proposals: 0,
    city_patches_from_geocoding: 0,
    state_region_patches_from_geocoding: 0,
    postal_code_patches_from_geocoding: 0,
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
    coordinate_conflicts: 0,
    provider_decision_needed_cases: 0,
    skipped_identical: 0,
    cache_hits: 0,
    mapbox_requests: 0,
    mapbox_success: 0,
    mapbox_no_match: 0,
    mapbox_low_confidence: 0,
    mapbox_errors: 0,
    mapbox_conflicts: 0,
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
  const masterPathway =
    opts.masterFounderApprovedPathway === true ||
    String(env.CENSUS_MASTER_MAPBOX_STRUCTURED_ADDRESS_PATHWAY || "0").trim() ===
      "1";
  const counters = emptyCounters();
  const proposals = [];
  const providerDecisionNeeded = [];
  const stewardReview = [];
  const failures = [];
  const cache = new Map();
  const failedKeysThisRun = new Set();
  let budgetPaused = false;
  let budgetPauseReason = null;

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
      masterFounderApprovedPathway: masterPathway,
      env,
    });
    if (!elig.eligible) {
      if (elig.reason === "no_address" || elig.reason === "city_or_non_street_address") {
        counters.skipped_no_address += 1;
      } else if (elig.reason === "address_confidence_not_high") {
        counters.skipped_address_confidence_not_high += 1;
      } else if (elig.reason === "missing_or_unknown_city") {
        counters.skipped_city_or_unknown += 1;
      } else if (elig.reason === "partial_coordinate_pair_hold") {
        counters.coordinate_conflicts += 1;
        counters.mapbox_conflicts += 1;
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
    if (elig.master_founder_approved_pathway) {
      counters.records_eligible_master_pathway += 1;
    }
    eligible.push({ rec, elig });
  }
  // Prefer postal + state structured addresses
  eligible.sort(
    (a, b) => Number(a.elig.priority || 999) - Number(b.elig.priority || 999)
  );
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
    const budget = evaluateMapboxBudgetGuard(requests, env);
    if (!budget.ok || requests >= maxReq) {
      budgetPaused = true;
      budgetPauseReason =
        budget.reason || "max_geocode_requests_per_run_reached";
      providerDecisionNeeded.push({
        record_id: rec.id,
        identity_key: elig.identity_key,
        property_name: elig.property_name,
        reason: budgetPauseReason,
        status: "COORDINATE_LANE_PAUSED_BUDGET",
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
          postalCode: elig.postal_code || fields[POSTAL_CODE_FIELD],
          sourceUrl: fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.addressSourceUrl],
        },
        {
          env,
          fetchImpl: opts.fetchImpl,
          omitPropertyName: true,
        }
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
        counters.mapbox_success += 1;
        continue;
      }
      if (gate.action === "steward_review") {
        counters.steward_review_cases += 1;
        counters.coordinate_conflicts += 1;
        counters.mapbox_conflicts += 1;
        stewardReview.push({
          record_id: rec.id,
          identity_key: elig.identity_key,
          property_name: elig.property_name,
          reason: gate.reason,
          classification: MASTER_MAPBOX_COORDINATE_CLASS.CONFLICT,
          existing: gate.existing,
          proposed: gate.proposed,
        });
        continue;
      }

      const fromMedium = elig.from_medium_address === true;
      const patchOpts = {
        fromMediumAddress: fromMedium,
        allowGeoNullFill: masterPathway === true,
        fields,
      };
      const patch = buildCoordinateCompletionPatch(geo, patchOpts);
      if (!patch) {
        counters.low_confidence += 1;
        counters.mapbox_low_confidence += 1;
        counters.mapbox_rejects += 1;
        failedKeysThisRun.add(cacheKey);
        continue;
      }
      if (patchOpts._geoNullFillCounts) {
        counters.city_patches_from_geocoding +=
          patchOpts._geoNullFillCounts.city || 0;
        counters.state_region_patches_from_geocoding +=
          patchOpts._geoNullFillCounts.state_region || 0;
        counters.postal_code_patches_from_geocoding +=
          patchOpts._geoNullFillCounts.postal_code || 0;
      }

      const confidence = fromMedium ? "Medium" : "High";
      proposals.push({
        record_id: rec.id,
        id: rec.id,
        identity_key: elig.identity_key,
        property_name: elig.property_name,
        brand: elig.brand,
        family: fields[MAP_FIRST_PASS.family] || null,
        queue: COORDINATE_COMPLETION_QUEUE_ID,
        action: fromMedium
          ? "propose_medium_address_mapbox_write"
          : "propose_high_write",
        confidence,
        classification: MASTER_MAPBOX_COORDINATE_CLASS.HIGH,
        from_medium_address: fromMedium,
        write_allowed_now: true,
        fields: patch,
        patch,
        current_fields: {
          [MAP_FIRST_PASS.latitude]: fields[MAP_FIRST_PASS.latitude] ?? null,
          [MAP_FIRST_PASS.longitude]: fields[MAP_FIRST_PASS.longitude] ?? null,
        },
        source_url: fields[MAP_FIRST_PASS.addressSourceUrl] || fields[MAP_FIRST_PASS.sourceUrl],
        method: "permanent_geocoding_official_address",
        provider: "Mapbox",
        geocode_status: geo.status,
        permanent_mode_confirmed: geo.permanent_mode_confirmed === true,
        coordinate_input_basis: elig.coordinate_input_basis || null,
        notes: geo.reason,
      });
      counters.coordinates_written_proposals += 1;
      counters.mapbox_success += 1;
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
      classification: MASTER_MAPBOX_COORDINATE_CLASS.UNRESOLVED,
      from_medium_address: elig.from_medium_address === true,
    };
    if (geo.status === MAPBOX_COORDINATE_STATUSES.CITY_CENTROID_REJECTED) {
      counters.city_centroid_rejected += 1;
      counters.mapbox_low_confidence += 1;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.ZERO_ZERO_REJECTED) {
      counters.zero_zero_rejected += 1;
      counters.mapbox_low_confidence += 1;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.COUNTRY_MISMATCH) {
      counters.country_mismatch_rejected += 1;
      counters.mapbox_conflicts += 1;
      bucket.classification = MASTER_MAPBOX_COORDINATE_CLASS.CONFLICT;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.SOURCE_CONFLICT) {
      counters.coordinate_conflicts += 1;
      counters.mapbox_conflicts += 1;
      bucket.classification = MASTER_MAPBOX_COORDINATE_CLASS.CONFLICT;
    } else if (geo.status === MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR) {
      counters.provider_errors += 1;
      counters.mapbox_errors += 1;
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
      counters.mapbox_no_match += 1;
    } else if (geo.reason === "mapbox_zero_results") {
      counters.mapbox_no_match += 1;
      counters.low_confidence += 1;
      counters.mapbox_low_confidence += 1;
    } else {
      counters.low_confidence += 1;
      counters.mapbox_low_confidence += 1;
    }
    failures.push(bucket);
  }

  let status = COORDINATE_COMPLETION_STATUSES.READY_FOR_PRODUCTION_CYCLE;
  if (budgetPaused) {
    status = "COORDINATE_LANE_PAUSED_BUDGET";
  } else if (counters.coordinates_written_proposals > 0 && !stewardReview.length && !providerDecisionNeeded.length) {
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
    budgetPaused,
    budgetPauseReason,
    masterPathway,
  });
  maybeWriteReports(report, opts);
  log(
    `[coordinate_completion] status=${status} proposals=${proposals.length} requests=${counters.mapbox_requests} budget_paused=${budgetPaused}`
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
  budgetPaused = false,
  budgetPauseReason = null,
  masterPathway = false,
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
    MAPBOX_PERMANENT_MODE_CONFIRMED:
      readiness?.mapbox_permanent_geocoding === true && readiness?.ready === true,
    max_geocode_requests_per_run: maxReq,
    master_founder_approved_pathway: masterPathway,
    budget_paused: budgetPaused,
    budget_pause_reason: budgetPauseReason,
    counters,
    estimated_cost: cost,
    MAPBOX_REQUESTS: counters.mapbox_requests,
    MAPBOX_SUCCESS: counters.mapbox_success,
    MAPBOX_NO_MATCH: counters.mapbox_no_match,
    MAPBOX_LOW_CONFIDENCE: counters.mapbox_low_confidence,
    MAPBOX_ERRORS: counters.mapbox_errors,
    MAPBOX_CONFLICTS: counters.mapbox_conflicts,
    ESTIMATED_MAPBOX_COST: cost.estimated_usd,
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
      hbx_coordinate_writes: false,
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

/**
 * Select ~25 eligible records across Mexico / Brazil / Caribbean / Central / South America.
 * @param {object[]} censusRecords
 * @param {{ sampleSize?: number, env?: object, masterFounderApprovedPathway?: boolean }} [opts]
 */
export function selectMasterCoordinateSample(censusRecords = [], opts = {}) {
  const sampleSize = Number(opts.sampleSize) > 0 ? Number(opts.sampleSize) : 25;
  const perBucket = Math.max(3, Math.ceil(sampleSize / 5));
  const buckets = {
    Mexico: [],
    Brazil: [],
    Caribbean: [],
    "Central America": [],
    "South America": [],
  };
  const env = opts.env || process.env;

  for (const rec of censusRecords) {
    const bucket = classifyMasterCoordinateSampleBucket(rec.fields || {});
    if (!bucket || !buckets[bucket]) continue;
    if (buckets[bucket].length >= perBucket + 2) continue;
    const elig = evaluateCoordinateCompletionEligibility(rec, {
      masterFounderApprovedPathway: opts.masterFounderApprovedPathway !== false,
      env,
    });
    if (!elig.eligible) continue;
    buckets[bucket].push({ rec, elig, bucket });
  }

  const selected = [];
  const order = [
    "Mexico",
    "Brazil",
    "Caribbean",
    "Central America",
    "South America",
  ];
  for (const b of order) {
    const take = buckets[b].slice(0, perBucket);
    selected.push(...take);
  }
  // Trim / top-up to sampleSize
  while (selected.length > sampleSize) selected.pop();
  if (selected.length < sampleSize) {
    for (const b of order) {
      for (const row of buckets[b]) {
        if (selected.length >= sampleSize) break;
        if (selected.some((s) => s.rec.id === row.rec.id)) continue;
        selected.push(row);
      }
    }
  }
  return {
    sample_size_target: sampleSize,
    selected_count: selected.length,
    by_bucket: Object.fromEntries(
      order.map((b) => [b, selected.filter((s) => s.bucket === b).length])
    ),
    records: selected.map((s) => s.rec),
    rows: selected,
  };
}

/**
 * Controlled pre-production Mapbox sample (~25). Fail closed on systematic issues.
 * @param {{
 *   censusRecords?: object[],
 *   env?: object,
 *   fetchImpl?: typeof fetch,
 *   sampleSize?: number,
 *   dryRun?: boolean,
 *   log?: Function,
 * }} opts
 */
export async function runMasterCoordinateSampleGate(opts = {}) {
  const log = opts.log || (() => {});
  const sample = selectMasterCoordinateSample(opts.censusRecords || [], {
    sampleSize: opts.sampleSize || 25,
    env: opts.env || process.env,
    masterFounderApprovedPathway: true,
  });
  log(
    `[coordinate_sample] selected=${sample.selected_count} buckets=${JSON.stringify(sample.by_bucket)}`
  );
  if (sample.selected_count < 15) {
    return {
      ok: false,
      passed: false,
      reason: "insufficient_sample_candidates",
      sample,
      report: null,
    };
  }

  const report = await runCoordinateCompletionQueue({
    censusRecords: sample.records,
    env: opts.env || process.env,
    fetchImpl: opts.fetchImpl,
    maxRequests: sample.selected_count,
    masterFounderApprovedPathway: true,
    dryRun: opts.dryRun !== false,
    writeReports: false,
    log,
  });

  const c = report.counters || {};
  const attempted = Number(c.mapbox_requests || 0) + Number(c.cache_hits || 0);
  const high = Number(c.high_address_coordinate_proposals || 0);
  const centroids = Number(c.city_centroid_rejected || 0);
  const countryMismatch = Number(c.country_mismatch_rejected || 0);
  const errors = Number(c.mapbox_errors || c.provider_errors || 0);
  const permanentOk = report.MAPBOX_PERMANENT_MODE_CONFIRMED === true;
  const highRate = attempted > 0 ? high / attempted : 0;
  const centroidRate = attempted > 0 ? centroids / attempted : 0;

  const passed =
    permanentOk &&
    attempted >= 10 &&
    highRate >= 0.4 &&
    centroidRate <= 0.25 &&
    countryMismatch <= Math.max(2, Math.floor(attempted * 0.15)) &&
    errors <= Math.max(2, Math.floor(attempted * 0.2));

  return {
    ok: true,
    passed,
    reason: passed ? null : "sample_quality_gate_failed",
    sample,
    metrics: {
      attempted,
      high,
      high_rate: Math.round(highRate * 1000) / 1000,
      city_centroid_rejected: centroids,
      country_mismatch: countryMismatch,
      mapbox_errors: errors,
      permanent_mode_confirmed: permanentOk,
    },
    report,
  };
}
