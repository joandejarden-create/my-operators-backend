/**
 * Core identity quality scoring for Hotel Property Census.
 * Combines city/state + canonical name + identity completeness.
 */

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  classifyPropertyNameProblems,
} from "./production-census-property-name-cleanup-extractor.js";
import {
  CANONICAL_PROPERTY_NAME_FIELD,
  CANONICAL_NAME_STATUS,
  classifyCanonicalPropertyName,
  deriveCanonicalPropertyNameCandidate,
} from "./census-canonical-property-name.js";
import {
  CITY_CLASS,
  classifyAndNormalizeCityState,
  isDescriptorCity,
} from "./census-city-state-normalizer.js";

export const CORE_IDENTITY_QUALITY_VERSION = "census-core-identity-quality-v1";

export const QUALITY_GATE_STATUS = Object.freeze({
  QUALITY_PASS: "quality_pass",
  AUTOFIX_HIGH: "autofix_high_confidence",
  SOURCE_LOOKUP_NEEDED: "source_lookup_needed",
  STEWARD_REVIEW_REQUIRED: "steward_review_required",
  DUPLICATE_RISK: "duplicate_risk",
  BLOCKED_IDENTITY_CONFLICT: "blocked_identity_conflict",
  BLOCKED_DIRTY_CORE_IDENTITY: "blocked_dirty_core_identity",
});

export const RECORD_READINESS = Object.freeze({
  CENSUS_READY: "Census Ready",
  NEEDS_ENRICHMENT: "Needs Enrichment",
  NEEDS_SOURCE_ADAPTER: "Needs Source Adapter",
  NEEDS_STEWARD_REVIEW: "Needs Steward Review",
  BLOCKED: "Blocked",
});

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

function field(fields, name, alt) {
  const v = fields?.[name];
  if (!isBlank(v)) return v;
  return alt != null ? fields?.[alt] : undefined;
}

/**
 * Build per-record quality score components (reporting only).
 */
export function buildCensusQualityScore(fields = {}, opts = {}) {
  const cityNorm = opts.cityNorm || classifyAndNormalizeCityState(fields);
  const canon = opts.canon || classifyCanonicalPropertyName(fields);

  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl] || ""
  ).trim();
  const address = String(fields[MAP_FIRST_PASS.address] || "").trim();
  const lat = fields[MAP_FIRST_PASS.latitude];
  const lng = fields[MAP_FIRST_PASS.longitude];
  const nameProblems = classifyPropertyNameProblems(propertyName);

  const identity_complete = Boolean(
    propertyName &&
      !nameProblems.malformed &&
      brand &&
      cityNorm.city_clean &&
      country &&
      sourceUrl
  );
  const canonical_name_clean =
    canon.status === CANONICAL_NAME_STATUS.COMPLETE_CLEAN ||
    (canon.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL && Boolean(canon.candidate));
  const city_clean = Boolean(cityNorm.city_clean);
  const state_region_clean_or_source_blocked =
    !isBlank(fields[MAP_FIRST_PASS.stateRegion]) ||
    cityNorm.class === CITY_CLASS.CLEAN ||
    cityNorm.class === CITY_CLASS.BLANK;
  const source_supported = Boolean(sourceUrl);
  const address_complete = Boolean(address);
  const coordinates_complete = !isBlank(lat) && !isBlank(lng);
  const public_ready =
    String(fields[MAP_FIRST_PASS.publicCensusEligibility] || "") === "Eligible";
  const enrichment_complete =
    !isBlank(fields[MAP_FIRST_PASS.descriptionSource]) ||
    !isBlank(fields[MAP_FIRST_PASS.amenitiesTags]);

  const components = {
    identity_complete,
    canonical_name_clean: Boolean(
      canon.status === CANONICAL_NAME_STATUS.COMPLETE_CLEAN ||
        (!isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]) &&
          canon.status !== CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW &&
          canon.status !== CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN)
    ),
    city_clean,
    state_region_clean_or_source_blocked: Boolean(state_region_clean_or_source_blocked),
    source_supported,
    address_complete,
    coordinates_complete,
    public_ready,
    enrichment_complete,
  };

  const keys = Object.keys(components);
  const trueCount = keys.filter((k) => components[k]).length;
  const score = Math.round((trueCount / keys.length) * 100);

  let readiness = RECORD_READINESS.NEEDS_ENRICHMENT;
  if (
    fields[MAP_FIRST_PASS.humanReview] === true ||
    cityNorm.class === CITY_CLASS.UNKNOWN ||
    cityNorm.class === CITY_CLASS.DESCRIPTOR ||
    cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED ||
    canon.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW
  ) {
    readiness = RECORD_READINESS.NEEDS_STEWARD_REVIEW;
  } else if (!source_supported || !brand || !country) {
    readiness = RECORD_READINESS.NEEDS_SOURCE_ADAPTER;
  } else if (
    identity_complete &&
    components.canonical_name_clean &&
    city_clean
  ) {
    readiness =
      coordinates_complete && address_complete
        ? RECORD_READINESS.CENSUS_READY
        : RECORD_READINESS.NEEDS_ENRICHMENT;
  } else if (!city_clean || !canonical_name_clean) {
    readiness = RECORD_READINESS.BLOCKED;
  }

  return { score, components, readiness, identity_complete };
}

/**
 * Classify core identity gate outcome for one Census record.
 */
export function classifyCoreIdentityQuality(record, opts = {}) {
  const fields = record?.fields || {};
  const cityNorm = classifyAndNormalizeCityState(fields);
  const canon = classifyCanonicalPropertyName(fields, {
    fieldExists: opts.canonicalFieldExists !== false,
  });
  const quality = buildCensusQualityScore(fields, { cityNorm, canon });

  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl] || ""
  ).trim();
  const nameProblems = classifyPropertyNameProblems(propertyName);

  const blockers = [];
  if (!propertyName) blockers.push("missing_property_name");
  if (nameProblems.malformed && nameProblems.severity === "high") {
    blockers.push("property_name_malformed");
  }
  if (!brand) blockers.push("missing_brand");
  if (!country) blockers.push("missing_country");
  if (!sourceUrl) blockers.push("missing_source_url");
  if (fields[MAP_FIRST_PASS.humanReview] === true) blockers.push("human_review_required");

  if (
    cityNorm.class === CITY_CLASS.UNKNOWN ||
    cityNorm.class === CITY_CLASS.DESCRIPTOR ||
    cityNorm.class === CITY_CLASS.BLANK ||
    cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED
  ) {
    blockers.push(`city_${cityNorm.class}`);
  }

  if (
    canon.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW ||
    canon.status === CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED
  ) {
    blockers.push(`canonical_${canon.status}`);
  }

  if (isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]) && !canon.candidate) {
    if (canon.status === CANONICAL_NAME_STATUS.MISSING_SOURCE_SUPPORT) {
      blockers.push("canonical_blank_not_derivable");
    }
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  const fixes = [];

  if (cityNorm.write_allowed && cityNorm.patch) {
    Object.assign(patch, cityNorm.patch);
    fixes.push({ kind: "city_state", reason: cityNorm.reason, ...cityNorm });
  }

  if (
    (canon.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL ||
      canon.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN) &&
    canon.candidate &&
    canon.write_allowed !== false
  ) {
    // Defer duplicate check to gate runner with index
    fixes.push({
      kind: "canonical",
      reason: canon.reason,
      candidate: canon.candidate,
      status: canon.status,
      existing: canon.existing,
    });
  }

  let gateStatus = QUALITY_GATE_STATUS.QUALITY_PASS;
  if (opts.duplicateRisk) {
    gateStatus = QUALITY_GATE_STATUS.DUPLICATE_RISK;
  } else if (opts.identityConflict) {
    gateStatus = QUALITY_GATE_STATUS.BLOCKED_IDENTITY_CONFLICT;
  } else if (
    cityNorm.class === CITY_CLASS.UNKNOWN ||
    cityNorm.class === CITY_CLASS.DESCRIPTOR ||
    cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED ||
    (isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]) &&
      !canon.candidate &&
      canon.status !== CANONICAL_NAME_STATUS.COMPLETE_CLEAN)
  ) {
    gateStatus =
      cityNorm.write_allowed ||
      canon.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL ||
      canon.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN
        ? QUALITY_GATE_STATUS.AUTOFIX_HIGH
        : QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY;
    if (
      !cityNorm.write_allowed &&
      canon.status !== CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL &&
      canon.status !== CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN
    ) {
      gateStatus =
        !sourceUrl || !brand
          ? QUALITY_GATE_STATUS.SOURCE_LOOKUP_NEEDED
          : QUALITY_GATE_STATUS.STEWARD_REVIEW_REQUIRED;
      if (
        cityNorm.class === CITY_CLASS.UNKNOWN ||
        cityNorm.class === CITY_CLASS.DESCRIPTOR ||
        cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED
      ) {
        gateStatus = QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY;
      }
    }
  } else if (Object.keys(patch).length || fixes.some((f) => f.kind === "canonical")) {
    gateStatus = QUALITY_GATE_STATUS.AUTOFIX_HIGH;
  } else if (
    canon.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW ||
    cityNorm.class === CITY_CLASS.STEWARD
  ) {
    gateStatus = QUALITY_GATE_STATUS.STEWARD_REVIEW_REQUIRED;
  } else if (quality.identity_complete && cityNorm.city_clean) {
    gateStatus = QUALITY_GATE_STATUS.QUALITY_PASS;
  } else if (!sourceUrl) {
    gateStatus = QUALITY_GATE_STATUS.SOURCE_LOOKUP_NEEDED;
  }

  // If we only have case/split autofixes and no blockers left after apply intent
  if (cityNorm.write_allowed && cityNorm.city_clean && blockers.every((b) => b.startsWith("city_"))) {
    // autofix will clear city blockers
    gateStatus = QUALITY_GATE_STATUS.AUTOFIX_HIGH;
  }

  const coordinate_blocked =
    gateStatus === QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY ||
    gateStatus === QUALITY_GATE_STATUS.STEWARD_REVIEW_REQUIRED ||
    gateStatus === QUALITY_GATE_STATUS.DUPLICATE_RISK ||
    gateStatus === QUALITY_GATE_STATUS.BLOCKED_IDENTITY_CONFLICT ||
    !cityNorm.city_clean ||
    isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]) ||
    canon.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN ||
    canon.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW;

  return {
    record_id: record?.id || null,
    gate_status: gateStatus,
    quality,
    city: cityNorm,
    canonical: canon,
    patch_city_state: Object.keys(patch).length ? patch : null,
    pending_canonical_fix: fixes.find((f) => f.kind === "canonical") || null,
    blockers,
    coordinate_blocked: Boolean(coordinate_blocked),
    insert_allowed:
      Boolean(propertyName) &&
      !nameProblems.malformed &&
      Boolean(brand) &&
      Boolean(country) &&
      Boolean(sourceUrl) &&
      cityNorm.city_clean &&
      cityNorm.class !== CITY_CLASS.UNKNOWN &&
      !isDescriptorCity(fields.City) &&
      (canon.status === CANONICAL_NAME_STATUS.COMPLETE_CLEAN ||
        canon.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL ||
        canon.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN) &&
      fields[MAP_FIRST_PASS.humanReview] !== true,
  };
}

/**
 * Insert gate for discovery candidates (pre-Airtable write).
 */
export function evaluateInsertIdentityGate(discovered = {}, opts = {}) {
  const fields = {
    [MAP_FIRST_PASS.propertyName]: discovered.property_name || discovered.fields?.["Property Name"],
    [MAP_FIRST_PASS.currentBrand]: discovered.brand || discovered.fields?.["Current Brand"],
    [MAP_FIRST_PASS.city]: discovered.city || discovered.fields?.City,
    [MAP_FIRST_PASS.stateRegion]:
      discovered.state_region || discovered.fields?.["State / Region"],
    [MAP_FIRST_PASS.country]: discovered.country || discovered.fields?.Country,
    [MAP_FIRST_PASS.sourceUrl]:
      discovered.official_property_url ||
      discovered.official_directory_url ||
      discovered.fields?.["Source URL"],
    [MAP_FIRST_PASS.officialUrl]: discovered.official_property_url,
    [CANONICAL_PROPERTY_NAME_FIELD]:
      discovered.canonical_property_name ||
      discovered.fields?.[CANONICAL_PROPERTY_NAME_FIELD],
    "Family / Source Family":
      discovered.source_family || discovered.fields?.["Family / Source Family"],
  };

  // Attempt city normalize on candidate before gate
  const cityNorm = classifyAndNormalizeCityState(fields);
  if (cityNorm.write_allowed && cityNorm.patch?.City) {
    fields[MAP_FIRST_PASS.city] = cityNorm.patch.City;
    if (cityNorm.patch["State / Region"]) {
      fields[MAP_FIRST_PASS.stateRegion] = cityNorm.patch["State / Region"];
    }
  }

  const derived = deriveCanonicalPropertyNameCandidate(fields);
  if (derived.ok && isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD])) {
    fields[CANONICAL_PROPERTY_NAME_FIELD] = derived.candidate;
  }

  const classified = classifyCoreIdentityQuality({ id: null, fields }, opts);

  if (!classified.insert_allowed) {
    return {
      allow_insert: false,
      reason:
        classified.gate_status === QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY
          ? "dirty_core_identity"
          : classified.blockers[0] || classified.gate_status,
      gate_status: classified.gate_status,
      normalized_fields: fields,
      city_fix: cityNorm.write_allowed ? cityNorm.patch : null,
      canonical_candidate: derived.ok ? derived.candidate : null,
    };
  }

  return {
    allow_insert: true,
    reason: null,
    gate_status: QUALITY_GATE_STATUS.QUALITY_PASS,
    normalized_fields: fields,
    city_fix: cityNorm.write_allowed ? cityNorm.patch : null,
    canonical_candidate: fields[CANONICAL_PROPERTY_NAME_FIELD] || null,
  };
}

/**
 * Coordinate safety: dirty identity must not call Mapbox.
 */
export function evaluateCoordinateIdentityGate(record, opts = {}) {
  const classified = classifyCoreIdentityQuality(record, opts);
  const fields = record?.fields || {};
  const cityNorm = classified.city;
  const canonOk =
    classified.canonical.status === CANONICAL_NAME_STATUS.COMPLETE_CLEAN ||
    (!isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]) &&
      classified.canonical.status !== CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW &&
      classified.canonical.status !== CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN);

  if (!cityNorm.city_clean || !canonOk || classified.coordinate_blocked) {
    return {
      allow_geocode: false,
      reason: "blocked_dirty_core_identity",
      gate_status: QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY,
      classified,
    };
  }
  return {
    allow_geocode: true,
    reason: null,
    gate_status: QUALITY_GATE_STATUS.QUALITY_PASS,
    classified,
  };
}
