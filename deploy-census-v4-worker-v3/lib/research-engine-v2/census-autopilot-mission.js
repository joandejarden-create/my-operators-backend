/**
 * Census Autopilot Mission Mode — Clean CALA Census v1.
 *
 * Founder CLI approval = mission command. No per-phase ChatGPT gate.
 * Runs phased cleanup/enrichment until High writes exhaust or hard safety stop.
 * Write target: Hotel Property Census (tbl9aY5ijiuIzzWam) only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  parseAutopilotArgs,
  isProductionWriteMode,
} from "./census-autopilot-apply-guard.js";
import { runProductionCycle } from "./census-autopilot-production-cycle.js";
import { auditAllCoreIdentityIssues } from "./census-clean-core-identity-repair.js";
import { evaluateCleanCorePass, classifyMapContactSizeReadiness } from "./census-map-contact-size-readiness.js";
import {
  estimateMapboxPermanentCost,
  evaluateMapboxPermanentReadiness,
} from "./census-coordinate-provider.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MISSION_VERSION = "census-autopilot-mission-v2";

export const MISSION_STATUS = Object.freeze({
  COMPLETE: "production_census_clean_census_v1_mission_complete",
  PARTIAL: "production_census_clean_census_v1_mission_partial_steward_remaining",
  BLOCKED: "production_census_clean_census_v1_mission_blocked_safety_stop",
});

export const COMPLETE_CENSUS_MISSION_STATUS = Object.freeze({
  COMPLETE: "production_census_complete_census_v1_mission_complete",
  PARTIAL: "production_census_complete_census_v1_mission_partial_source_remaining",
  BLOCKED: "production_census_complete_census_v1_mission_blocked_safety_stop",
});

export const MISSION_OBJECTIVE_CLEAN_CENSUS_V1 = "clean-census-v1";
export const MISSION_OBJECTIVE_COMPLETE_CENSUS_V1 = "complete-census-v1";
export const MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1 = "coverage-reconciliation-v1";
export const MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1 =
  "coverage-steward-resolution-v1";
export const MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2 = "source-confirmed-census-v2";
export const MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1 =
  "brand-registry-resolution-v1";
export const MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1 = "cala-census-completion-v1";
export const MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1 =
  "level-2-source-extraction-v1";
export const MISSION_OBJECTIVE_LEVEL_2_ADAPTER_WAVE_2 = "level-2-adapter-wave-2";
export const MISSION_OBJECTIVE_OFFICIAL_PARENT_INVENTORY_CENSUS_V1 =
  "official-parent-inventory-census-v1";
export const MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1 =
  "official-parent-level-2-completion-v1";
export const MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3 =
  "full-latam-census-autopilot-v3";
export const MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1 =
  "marriott-webhound-source-pattern-learning-v1";
export const MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1 =
  "universal-record-resolver-v1";
export const MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1 =
  "commercial-fields-and-description-v1";
export const MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1 =
  "rooms-count-completion-v1";
export const MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1 =
  "rooms-secondary-source-wave-2-v1";
export const MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2 =
  "dataforseo-discovery-pilot-v2";
export const MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1 =
  "dataforseo-validated-write-policy-v1";
export const MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1 =
  "dataforseo-local-business-enrichment-v1";
export const MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1 =
  "dataforseo-local-business-validated-write-v1";
export const MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1 =
  "dataforseo-local-address-scale-v1";
export const MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1 =
  "census-autopilot-policy-controller-v1";
export const MISSION_OBJECTIVE_CENSUS_MISSING_FIELD_SOURCE_STRATEGY_CONTROLLER_V1 =
  "census-missing-field-source-strategy-controller-v1";

export const CALA_CENSUS_COMPLETION_STATUS = Object.freeze({
  COMPLETE: "production_census_cala_completion_v1_complete",
  PARTIAL: "production_census_cala_completion_v1_partial_source_remaining",
  BLOCKED: "production_census_cala_completion_v1_blocked_safety_stop",
});

/** Soft targets (aspirational — never invent data to hit them). */
export const CLEAN_CENSUS_V1_TARGETS = Object.freeze({
  clean_core_min: 875,
  unknown_city_max: 10,
  canonical_blank_max: 10,
  coordinate_blocked_dirty_max: 40,
});

export const COMPLETE_CENSUS_V1_TARGETS = Object.freeze({
  address_complete_min: 400,
  lat_long_complete_min: 350,
  phone_complete_min: 100,
  rooms_complete_min: 100,
  complete_census_v1_min: 100,
});

/** Soft Level-2 targets for CALA completion (aspirational — never invent data). */
export const CALA_CENSUS_COMPLETION_V1_TARGETS = Object.freeze({
  ...COMPLETE_CENSUS_V1_TARGETS,
  clean_core_min: CLEAN_CENSUS_V1_TARGETS.clean_core_min,
});

const FORBIDDEN_ALWAYS = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Developer",
  "Operator / Management Company",
  "Opening Date",
  "Renovation / Conversion Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
]);

export const MISSION_PHASE_CORE_IDENTITY = Object.freeze({
  id: "phase_1_core_identity",
  label: "Core Identity",
  queues: [
    "brand_normalization",
    "parent_company_normalization",
    "core_identity_quality",
    "core_identity_source_lookup",
    "canonical_property_name_completion",
    "city_state_normalization",
    "market_geography_completion",
    "key_field_completion",
  ],
  cleanup_existing_only: true,
  require_clean_core: false,
  max_passes: 3,
  allowed_fields: Object.freeze([
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "Brand Family",
    "City",
    "State / Region",
    "Country",
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
    "Source URL",
    "Official Property URL",
    "Family / Source Family",
    "Source Type",
    "Source Confidence",
    "Identity Confidence",
    "Data Confidence Tier",
    "Production Use Status",
    "Human Review Required",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const MISSION_PHASE_CLASSIFICATION = Object.freeze({
  id: "phase_2_classification",
  label: "Clean Core Classification",
  queues: ["clean_core_classification"],
  cleanup_existing_only: true,
  require_clean_core: false,
  max_passes: 1,
  allowed_fields: Object.freeze([]),
  read_only: true,
});

export const MISSION_PHASE_ADDRESS = Object.freeze({
  id: "phase_3_address",
  label: "Address Completion (Clean Core only)",
  queues: ["address_confirmation"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Address",
    "Address Confidence",
    "Address Source URL",
    "City",
    "State / Region",
    "Country",
  ]),
});

export const MISSION_PHASE_COORDINATES = Object.freeze({
  id: "phase_4_coordinates",
  label: "Coordinate Completion (Clean Core + High Address)",
  queues: ["coordinate_completion"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Latitude",
    "Longitude",
    "Coordinate Source Type",
    "Coordinate Confidence",
    "Geocode Provider",
    "Geocode Method",
    "Geocode Reviewed Date",
  ]),
});

export const MISSION_PHASE_CONTACT_SIZE = Object.freeze({
  id: "phase_5_contact_size",
  label: "Phone + Rooms (Clean Core only)",
  queues: ["phone_number_enrichment", "rooms_keys"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Phone",
    "Rooms / Keys",
    "Rooms Confidence",
    "Rooms Source URL",
    "Rooms Source Type",
    "Rooms Reviewed Date",
    "Rooms Notes",
  ]),
});

export const MISSION_PHASE_RICH = Object.freeze({
  id: "phase_6_rich_enrichment",
  label: "Rich Enrichment",
  queues: [
    "property_type_asset_context",
    "description_extraction",
    "amenities_extraction",
    "radar_public_readiness",
  ],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: false,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Property Type",
    "Asset Context",
    "Market",
    "Submarket",
    "Market / Submarket",
    "Hotel Description - Source Text",
    "Hotel Description - AI Summary",
    "Amenities - Source Text",
    "Amenities - Structured Tags",
    "F&B Flag",
    "Meeting Space Flag",
    "Resort / Leisure Flag",
    "Extended Stay Flag",
    "Mixed-Use Flag",
    "Branded Residences Flag",
    "Radar Display Status",
    "Radar Display Reason",
    "Radar Geography Status",
    "Public Census Eligibility",
    "Public Display Confidence",
    "Public Display Review Status",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const MISSION_PHASE_FINAL = Object.freeze({
  id: "phase_7_final_classification",
  label: "Final Classification",
  queues: ["clean_core_classification"],
  cleanup_existing_only: true,
  require_clean_core: false,
  max_passes: 1,
  allowed_fields: Object.freeze([]),
  read_only: true,
});

/** Ordered phases for clean-census-v1. */
export const CLEAN_CENSUS_V1_PHASES = Object.freeze([
  MISSION_PHASE_CORE_IDENTITY,
  MISSION_PHASE_CLASSIFICATION,
  MISSION_PHASE_ADDRESS,
  MISSION_PHASE_COORDINATES,
  MISSION_PHASE_CONTACT_SIZE,
  MISSION_PHASE_RICH,
  MISSION_PHASE_FINAL,
]);

/** Phase 1 — reconfirm Clean Core + geography (no discovery inserts). */
export const COMPLETE_PHASE_RECONFIRM = Object.freeze({
  id: "phase_1_reconfirm_clean_core_geography",
  label: "Reconfirm Clean Core + Geography",
  queues: [
    "core_identity_quality",
    "canonical_property_name_completion",
    "city_state_normalization",
    "market_geography_completion",
  ],
  cleanup_existing_only: true,
  skip_inserts: true,
  require_clean_core: false,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "City",
    "State / Region",
    "Country",
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
    "Source URL",
    "Official Property URL",
    "Family / Source Family",
    "Source Type",
    "Source Confidence",
    "Identity Confidence",
    "Data Confidence Tier",
    "Production Use Status",
    "Human Review Required",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

/** Phase 2 — State / Region for Clean Core. */
export const COMPLETE_PHASE_STATE_REGION = Object.freeze({
  id: "phase_2_state_region",
  label: "State / Region Completion (Clean Core)",
  queues: ["city_state_normalization", "core_identity_quality"],
  cleanup_existing_only: true,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "State / Region",
    "City",
    "Country",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const COMPLETE_PHASE_ADDRESS = Object.freeze({
  id: "phase_3_address",
  label: "Address Completion (Clean Core)",
  queues: ["address_confirmation"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 3,
  allowed_fields: Object.freeze([
    "Address",
    "Address Confidence",
    "Address Source URL",
    "City",
    "State / Region",
    "Country",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const COMPLETE_PHASE_COORDINATES = Object.freeze({
  id: "phase_4_coordinates",
  label: "Coordinate Completion (Clean Core + High Address)",
  queues: ["coordinate_completion"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 3,
  allowed_fields: Object.freeze([
    "Latitude",
    "Longitude",
    "Coordinate Source Type",
    "Coordinate Confidence",
    "Geocode Provider",
    "Geocode Method",
    "Geocode Reviewed Date",
    "Radar Geography Status",
    "Radar Display Status",
    "Radar Display Reason",
    "Public Display Review Status",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const COMPLETE_PHASE_PHONE = Object.freeze({
  id: "phase_5_phone",
  label: "Phone Completion (Clean Core / official only)",
  queues: ["phone_number_enrichment"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Phone",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const COMPLETE_PHASE_ROOMS = Object.freeze({
  id: "phase_6_rooms",
  label: "Rooms Completion (Clean Core / official only)",
  queues: ["rooms_keys"],
  cleanup_existing_only: false,
  skip_inserts: true,
  require_clean_core: true,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Rooms / Keys",
    "Rooms Confidence",
    "Rooms Source URL",
    "Rooms Source Type",
    "Rooms Reviewed Date",
    "Rooms Notes",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const COMPLETE_PHASE_FINAL = Object.freeze({
  id: "phase_7_final_readiness",
  label: "Final Readiness Classification",
  queues: ["clean_core_classification"],
  cleanup_existing_only: true,
  skip_inserts: true,
  require_clean_core: false,
  max_passes: 1,
  allowed_fields: Object.freeze([]),
  read_only: true,
});

/** Ordered phases for complete-census-v1 (Level 2 focus; no discovery inserts). */
export const COMPLETE_CENSUS_V1_PHASES = Object.freeze([
  COMPLETE_PHASE_RECONFIRM,
  COMPLETE_PHASE_STATE_REGION,
  COMPLETE_PHASE_ADDRESS,
  COMPLETE_PHASE_COORDINATES,
  COMPLETE_PHASE_PHONE,
  COMPLETE_PHASE_ROOMS,
  COMPLETE_PHASE_FINAL,
]);

/** Phase — reconfirm brand + core identity (existing records only). */
export const CALA_PHASE_RECONFIRM_BRAND_CORE = Object.freeze({
  id: "phase_2_reconfirm_brand_core",
  label: "Reconfirm Brand / Core Identity",
  queues: [
    "brand_normalization",
    "parent_company_normalization",
    "core_identity_quality",
    "core_identity_source_lookup",
    "canonical_property_name_completion",
    "city_state_normalization",
  ],
  cleanup_existing_only: true,
  skip_inserts: true,
  require_clean_core: false,
  max_passes: 3,
  allowed_fields: Object.freeze([
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "Brand Family",
    "City",
    "State / Region",
    "Country",
    "Source URL",
    "Official Property URL",
    "Family / Source Family",
    "Source Type",
    "Source Confidence",
    "Identity Confidence",
    "Data Confidence Tier",
    "Production Use Status",
    "Human Review Required",
    "Public Display Review Status",
    "Radar Display Status",
    "Radar Display Reason",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const CALA_PHASE_CLASSIFICATION = Object.freeze({
  id: "phase_3_clean_core_classification",
  label: "Clean Core Classification",
  queues: ["clean_core_classification"],
  cleanup_existing_only: true,
  skip_inserts: true,
  require_clean_core: false,
  max_passes: 1,
  allowed_fields: Object.freeze([]),
  read_only: true,
});

export const CALA_PHASE_GEOGRAPHY = Object.freeze({
  id: "phase_4_market_geography",
  label: "Market Geography",
  queues: ["market_geography_completion"],
  cleanup_existing_only: true,
  skip_inserts: true,
  require_clean_core: false,
  max_passes: 2,
  allowed_fields: Object.freeze([
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
    "Country",
    "City",
    "State / Region",
    "Enrichment Status",
    "Enrichment Priority",
    "Last Reviewed Date",
  ]),
});

export const CALA_PHASE_ADDRESS = Object.freeze({
  ...COMPLETE_PHASE_ADDRESS,
  id: "phase_5_address",
});

export const CALA_PHASE_COORDINATES = Object.freeze({
  ...COMPLETE_PHASE_COORDINATES,
  id: "phase_6_coordinates",
});

export const CALA_PHASE_PHONE = Object.freeze({
  ...COMPLETE_PHASE_PHONE,
  id: "phase_7_phone",
});

export const CALA_PHASE_ROOMS = Object.freeze({
  ...COMPLETE_PHASE_ROOMS,
  id: "phase_8_rooms",
});

export const CALA_PHASE_FINAL = Object.freeze({
  ...COMPLETE_PHASE_FINAL,
  id: "phase_9_final_readiness",
});

/**
 * Ordered phases for cala-census-completion-v1.
 * Dirty-partner park + SoT guard run as pre-steps outside this list.
 */
export const CALA_CENSUS_COMPLETION_V1_PHASES = Object.freeze([
  CALA_PHASE_RECONFIRM_BRAND_CORE,
  CALA_PHASE_CLASSIFICATION,
  CALA_PHASE_GEOGRAPHY,
  CALA_PHASE_ADDRESS,
  CALA_PHASE_COORDINATES,
  CALA_PHASE_PHONE,
  CALA_PHASE_ROOMS,
  CALA_PHASE_FINAL,
]);

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

/**
 * @param {string|null} objective
 */
export function resolveMissionObjective(objective) {
  const o = String(objective || "")
    .trim()
    .toLowerCase();
  if (!o || o === MISSION_OBJECTIVE_CLEAN_CENSUS_V1) {
    return MISSION_OBJECTIVE_CLEAN_CENSUS_V1;
  }
  if (o === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1) {
    return MISSION_OBJECTIVE_COMPLETE_CENSUS_V1;
  }
  if (o === MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1) {
    return MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1;
  }
  if (o === MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1) {
    return MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1;
  }
  if (o === MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2) {
    return MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2;
  }
  if (o === MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1) {
    return MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1;
  }
  if (o === MISSION_OBJECTIVE_OFFICIAL_PARENT_INVENTORY_CENSUS_V1) {
    return MISSION_OBJECTIVE_OFFICIAL_PARENT_INVENTORY_CENSUS_V1;
  }
  if (o === MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1) {
    return MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1;
  }
  if (o === MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3) {
    return MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3;
  }
  if (o === MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1) {
    return MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1;
  }
  if (o === MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1) {
    return MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1;
  }
  if (o === MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1) {
    return MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1;
  }
  if (o === MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1) {
    return MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1;
  }
  if (o === MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1) {
    return MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1;
  }
  if (o === MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2) {
    return MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2;
  }
  if (o === MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1) {
    return MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1;
  }
  if (o === MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1) {
    return MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1;
  }
  if (o === MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1) {
    return MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1;
  }
  if (o === MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1) {
    return MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1;
  }
  if (o === MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1) {
    return MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1;
  }
  if (o === MISSION_OBJECTIVE_CENSUS_MISSING_FIELD_SOURCE_STRATEGY_CONTROLLER_V1) {
    return MISSION_OBJECTIVE_CENSUS_MISSING_FIELD_SOURCE_STRATEGY_CONTROLLER_V1;
  }
  if (o === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1) {
    return MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1;
  }
  if (
    o === MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1 ||
    o === MISSION_OBJECTIVE_LEVEL_2_ADAPTER_WAVE_2
  ) {
    return MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1;
  }
  return null;
}

/**
 * Soft targets for a mission objective.
 * @param {string} objective
 */
export function resolveMissionSoftTargets(objective) {
  if (objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1) {
    return CALA_CENSUS_COMPLETION_V1_TARGETS;
  }
  if (objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1) {
    return COMPLETE_CENSUS_V1_TARGETS;
  }
  return CLEAN_CENSUS_V1_TARGETS;
}

/**
 * @param {string} objective
 */
export function resolveMissionPhases(objective) {
  if (objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1) {
    return COMPLETE_CENSUS_V1_PHASES;
  }
  if (objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1) {
    return CALA_CENSUS_COMPLETION_V1_PHASES;
  }
  return CLEAN_CENSUS_V1_PHASES;
}

/**
 * @param {string} objective
 */
export function resolveMissionStatusEnum(objective) {
  if (objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1) {
    return COMPLETE_CENSUS_MISSION_STATUS;
  }
  if (objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1) {
    return CALA_CENSUS_COMPLETION_STATUS;
  }
  return MISSION_STATUS;
}

/**
 * Filter High proposals to mission phase allowlist (never forbidden fields).
 * @param {object[]} proposals
 * @param {object} phase
 */
export function filterMissionPhaseProposals(proposals = [], phase = {}) {
  const allowed = new Set(phase.allowed_fields || []);
  const out = [];
  for (const p of proposals || []) {
    if (p.action === "insert" || p.type === "insert") continue;
    if (p.queue === "source_discovery") continue;
    const patch = { ...(p.patch || p.fields || {}) };
    for (const k of Object.keys(patch)) {
      if (FORBIDDEN_ALWAYS.includes(k)) {
        delete patch[k];
        continue;
      }
      if (allowed.size && !allowed.has(k)) {
        delete patch[k];
      }
    }
    if (!Object.keys(patch).length) continue;
    out.push({ ...p, patch, fields: patch });
  }
  return out;
}

/**
 * Keep only proposals for Clean Core records.
 * @param {object[]} proposals
 * @param {object[]} censusRecords
 */
export function filterProposalsToCleanCoreRecords(
  proposals = [],
  censusRecords = [],
  opts = {}
) {
  const cleanIds = new Set();
  for (const rec of censusRecords || []) {
    if (evaluateCleanCorePass(rec, opts).pass) cleanIds.add(rec.id);
  }
  return (proposals || []).filter((p) => cleanIds.has(p.record_id || p.id));
}

/**
 * Snapshot census quality metrics for mission reporting.
 * @param {object[]} censusRecords
 */
export function snapshotMissionCensusMetrics(censusRecords = [], opts = {}) {
  const audit = auditAllCoreIdentityIssues(censusRecords);
  let addressComplete = 0;
  let addressConfidenceHigh = 0;
  let addressSourceUrlComplete = 0;
  let latLongComplete = 0;
  let mapboxEligible = 0;
  let phoneComplete = 0;
  let roomsComplete = 0;
  let continentComplete = 0;
  let subContinentComplete = 0;
  let marketComplete = 0;
  let submarketComplete = 0;
  let completeCensusV1 = 0;
  let mapReady = 0;
  let contactReady = 0;
  let sizeReady = 0;
  let blockedMissingAddress = 0;
  let blockedDirtyIdentity = 0;
  let blockedSourceAccess = 0;

  const geoOpts = {
    continentFieldExists: opts.continentFieldExists === true,
    phoneFieldExists: opts.phoneFieldExists !== false,
    canonicalFieldExists: opts.canonicalFieldExists !== false,
  };

  for (const rec of censusRecords) {
    const f = rec.fields || {};
    if (!isBlank(f[MAP_FIRST_PASS.address] || f.Address)) addressComplete += 1;
    const addrConf = String(f[MAP_FIRST_PASS.addressConfidence] || f["Address Confidence"] || "")
      .trim()
      .toLowerCase();
    if (addrConf === "high") addressConfidenceHigh += 1;
    if (!isBlank(f[MAP_FIRST_PASS.addressSourceUrl] || f["Address Source URL"])) {
      addressSourceUrlComplete += 1;
    }
    if (
      !isBlank(f[MAP_FIRST_PASS.latitude] || f.Latitude) &&
      !isBlank(f[MAP_FIRST_PASS.longitude] || f.Longitude)
    ) {
      latLongComplete += 1;
    }
    if (!isBlank(f.Phone || f["Phone Number"])) phoneComplete += 1;
    if (!isBlank(f["Rooms / Keys"])) roomsComplete += 1;
    if (!isBlank(f.Continent)) continentComplete += 1;
    if (!isBlank(f["Sub-Continent"])) subContinentComplete += 1;
    if (!isBlank(f.Market)) marketComplete += 1;
    if (!isBlank(f.Submarket)) submarketComplete += 1;

    const row = classifyMapContactSizeReadiness(rec, geoOpts);
    if (row.lat_long_eligible) mapboxEligible += 1;
    if (row.blocked_missing_address) blockedMissingAddress += 1;
    if (row.blocked_dirty_identity) blockedDirtyIdentity += 1;
    if (row.blocked_source_insufficient) blockedSourceAccess += 1;
    if (row.address_complete && row.lat_long_complete && row.clean_core?.pass) mapReady += 1;
    if (row.phone_complete && row.clean_core?.pass) contactReady += 1;
    if (row.rooms_complete && row.clean_core?.pass) sizeReady += 1;

    const stateOk = !isBlank(f["State / Region"] || f[MAP_FIRST_PASS.stateRegion]);
    const complete =
      row.clean_core?.pass &&
      stateOk &&
      row.address_complete &&
      row.lat_long_complete &&
      (row.phone_complete || !row.phone_field_exists) &&
      row.rooms_complete;
    if (complete) completeCensusV1 += 1;
  }

  const mapboxReady = evaluateMapboxPermanentReadiness(opts.env || process.env);
  const mapboxCost = estimateMapboxPermanentCost(mapboxEligible, opts.env || process.env);

  return {
    total_records: censusRecords.length,
    clean_core: audit.counters.clean_core,
    below_clean_core: audit.counters.below_clean_core,
    unknown_city: audit.counters.unknown_city,
    descriptor_city: audit.counters.descriptor_city,
    canonical_blank: audit.counters.canonical_blank,
    state_region_complete: audit.counters.state_region_complete,
    address_complete: addressComplete,
    address_confidence_high: addressConfidenceHigh,
    address_source_url_complete: addressSourceUrlComplete,
    lat_long_complete: latLongComplete,
    mapbox_eligible: mapboxEligible,
    phone_complete: phoneComplete,
    rooms_complete: roomsComplete,
    continent_complete: continentComplete,
    subcontinent_complete: subContinentComplete,
    market_complete: marketComplete,
    submarket_complete: submarketComplete,
    complete_census_v1: completeCensusV1,
    map_ready: mapReady,
    contact_ready: contactReady,
    size_ready: sizeReady,
    blocked_missing_address: blockedMissingAddress,
    blocked_dirty_identity: blockedDirtyIdentity,
    blocked_source_access: blockedSourceAccess,
    mapbox_provider_ready: Boolean(mapboxReady?.approved_for_geocode_apply),
    estimated_mapbox_requests: mapboxEligible,
    estimated_mapbox_cost_usd: mapboxCost?.estimated_usd ?? mapboxCost?.usd ?? null,
    coordinate_blocked_dirty_identity:
      audit.counters.coordinate_blocked_dirty_identity,
    by_class: audit.counters.by_class,
    source_lookup_remaining: audit.counters.by_class?.["Needs Source Lookup"] ?? 0,
    steward_remaining: audit.counters.by_class?.["Needs Steward Review"] ?? 0,
    duplicate_risk_remaining: audit.counters.by_class?.["Duplicate Risk"] ?? 0,
  };
}

/**
 * Build mission plan artifact.
 */
export function buildMissionPlan(opts = {}) {
  const objective = opts.objective || MISSION_OBJECTIVE_CLEAN_CENSUS_V1;
  const phases = opts.phases || resolveMissionPhases(objective);
  const softTargets = resolveMissionSoftTargets(objective);
  return {
    version: MISSION_VERSION,
    mode: "mission",
    objective,
    region: opts.region || "CALA",
    scope: opts.scope || "active-brand-setup",
    strategy: opts.strategy || "fastest-safe",
    cleanup_existing_only: Boolean(opts.cleanupExistingOnly),
    skip_inserts: true,
    max_passes_budget: opts.maxPassesBudget ?? 6,
    max_passes_semantics: "per_phase_cap",
    batch_size: opts.batchSize || 100,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    soft_targets: softTargets,
    founder_approval: "mission_cli_command",
    per_phase_chatgpt_approval: false,
    phases: phases.map((p, i) => ({
      order: i + 1,
      id: p.id,
      label: p.label,
      queues: p.queues,
      require_clean_core: Boolean(p.require_clean_core),
      read_only: Boolean(p.read_only),
      max_passes: p.max_passes,
      allowed_fields: p.allowed_fields || [],
    })),
    hard_stop_conditions: [
      "wrong_census_table",
      "protected_field",
      "brand_setup_write",
      "brand_explorer_write",
      "legacy_census_or_vic_write",
      "owner_operator_date_field",
      "systemic_duplicate_insert_risk",
      "repeated_airtable_write_failure",
      "schema_conflict",
    ],
    soft_continue_conditions: [
      "unknown_city",
      "blank_canonical",
      "blocked_property_pages",
      "no_high_proposals_in_queue",
      "mapbox_not_eligible",
      "missing_phone_source",
      "missing_room_source",
      "steward_cases",
      "source_insufficient",
      "missing_address_source",
    ],
  };
}

function renderMissionPlanMd(plan) {
  const title =
    plan.objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1
      ? "Complete CALA Census Mission Plan (v1)"
      : "Clean CALA Census Mission Plan (v1)";
  const softLines = Object.entries(plan.soft_targets || {})
    .map(([k, v]) => `- ${k}: ${v}`)
    .join("\n");
  return `# ${title}

**Objective:** \`${plan.objective}\`  
**Region:** ${plan.region}  
**Scope:** ${plan.scope}  
**Write target:** ${plan.write_target.base} → ${plan.write_target.table} (\`${plan.write_target.table_id}\`)  
**Founder approval:** mission CLI command (no per-phase ChatGPT gate)  
**Inserts:** blocked  
**Max passes (per phase cap):** ${plan.max_passes_budget}  
**Pass semantics:** each phase uses its own max_passes (capped by CLI --max-passes); all phases run unless a hard safety stop.

## Soft targets

${softLines}

## Phases

${plan.phases
  .map(
    (p) =>
      `${p.order}. **${p.label}** (\`${p.id}\`) — queues: ${p.queues.join(", ")}${
        p.require_clean_core ? " · Clean Core only" : ""
      }${p.read_only ? " · read-only" : ""}`
  )
  .join("\n")}

## Hard stops only

${plan.hard_stop_conditions.map((c) => `- ${c}`).join("\n")}

## Soft continues

${plan.soft_continue_conditions.map((c) => `- ${c}`).join("\n")}
`;
}

/**
 * Decide final mission status from metrics + safety.
 * @param {{ safetyStop?: object, after?: object, objective?: string }} opts
 */
export function resolveMissionStatus(opts = {}) {
  const objective = opts.objective || MISSION_OBJECTIVE_CLEAN_CENSUS_V1;
  const statuses = resolveMissionStatusEnum(objective);
  if (opts.safetyStop) return statuses.BLOCKED;
  const after = opts.after || {};

  if (objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1) {
    const sourceRemaining =
      (after.blocked_missing_address || 0) +
      (after.blocked_source_access || 0) +
      (after.mapbox_eligible || 0) +
      (after.source_lookup_remaining || 0);
    // Still incomplete Level 2 on Clean Core rows → partial source remaining
    const clean = after.clean_core || 0;
    const complete = after.complete_census_v1 || 0;
    if (sourceRemaining > 0 || complete < clean || (after.steward_remaining || 0) > 0) {
      return statuses.PARTIAL;
    }
    return statuses.COMPLETE;
  }

  if (objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1) {
    // Dirty partner labels may remain parked — that is expected and not a hard stop.
    // Partial when Clean Core rows still lack Level 2 or source/steward gaps remain
    // beyond parked dirty brands.
    const clean = after.clean_core || 0;
    const complete = after.complete_census_v1 || 0;
    const sourceGaps =
      (after.blocked_missing_address || 0) +
      (after.blocked_source_access || 0) +
      (after.source_lookup_remaining || 0);
    const dirtyParked = after.dirty_partner_labels || 0;
    const stewardBeyondDirty = Math.max(
      0,
      (after.steward_remaining || 0) - dirtyParked
    );
    if (complete < clean || sourceGaps > 0 || stewardBeyondDirty > 0) {
      return statuses.PARTIAL;
    }
    // All Clean Core rows Complete Census v1; dirty partners may still be parked
    return statuses.COMPLETE;
  }

  const steward =
    (after.steward_remaining || 0) +
    (after.source_lookup_remaining || 0) +
    (after.duplicate_risk_remaining || 0) +
    (after.below_clean_core || 0);
  if (steward > 0 || (after.unknown_city || 0) > 0 || (after.canonical_blank || 0) > 0) {
    return statuses.PARTIAL;
  }
  return statuses.COMPLETE;
}

/**
 * Run Clean Census v1 mission (phased production-cycle).
 */
export async function runCleanCensusV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((msg) => console.log(msg));
  const started = Date.now();

  const objective = resolveMissionObjective(args.objective);
  const statusEnum = resolveMissionStatusEnum(objective || MISSION_OBJECTIVE_CLEAN_CENSUS_V1);
  if (!objective) {
    return {
      ok: false,
      status: statusEnum.BLOCKED,
      blocked_reason: "unsupported_mission_objective",
      objective: args.objective,
      airtable_writes: false,
    };
  }

  const phases = resolveMissionPhases(objective);
  const softTargets = resolveMissionSoftTargets(objective);

  args.mode = "mission";
  // --max-passes caps each phase (phase.max_passes is the default ceiling).
  // Do NOT treat it as a cross-phase budget — that skipped address→enrichment after identity.
  const maxPassesPerPhaseCap = args.maxPasses || opts.maxPassesBudget || null;
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms
  );

  const autopilotRoot =
    opts.root || path.join(ROOT, "reports/research-engine-v2/autopilot");
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const missionSlug =
    objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
      ? "cala-census-completion-v1-mission"
      : objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1
        ? "complete-census-v1-mission"
        : "clean-census-v1-mission";
  const runName =
    opts.runId || `${stamp}_${args.region || "CALA"}-${missionSlug}`;
  const runDir = path.join(autopilotRoot, runName);
  fs.mkdirSync(path.join(runDir, "phases"), { recursive: true });

  const plan = buildMissionPlan({
    objective,
    phases,
    region: args.region,
    scope: args.scope,
    strategy: args.strategy,
    cleanupExistingOnly: true,
    maxPassesBudget: maxPassesPerPhaseCap || 6,
    maxPassesPerPhaseCap,
    batchSize: args.batchSize,
  });
  writeJson(path.join(runDir, "mission-plan.json"), plan);
  writeText(path.join(runDir, "mission-plan.md"), renderMissionPlanMd(plan));

  log(`[mission] ${objective} starting run_dir=${runDir}`);
  log(`[mission] founder CLI = approval; no per-phase ChatGPT gate`);
  log(`[mission] phases=${phases.map((p) => p.id).join(",")}`);

  // Schema probe — Continent/Sub-Continent/Market/Submarket must exist before geo writes
  let geoFieldFlags = {
    continentFieldExists: Boolean(opts.continentFieldExists),
    subContinentFieldExists: Boolean(opts.subContinentFieldExists),
    marketFieldExists: Boolean(opts.marketFieldExists),
    submarketFieldExists: Boolean(opts.submarketFieldExists),
  };
  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  const tableId = TABLE_IDS["Hotel Property Census"];

  if (token && bases?.target_base_id && !opts.skipSchemaProbe) {
    try {
      const metaRes = await fetch(
        `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(bases.target_base_id)}/tables`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const metaJson = await metaRes.json();
      const table = (metaJson.tables || []).find(
        (t) => t.id === tableId || t.name === "Hotel Property Census"
      );
      const names = new Set((table?.fields || []).map((f) => f.name));
      geoFieldFlags = {
        continentFieldExists: names.has("Continent"),
        subContinentFieldExists: names.has("Sub-Continent"),
        marketFieldExists: names.has("Market"),
        submarketFieldExists: names.has("Submarket"),
      };
      log(
        `[mission] geo fields continent=${geoFieldFlags.continentFieldExists} subcontinent=${geoFieldFlags.subContinentFieldExists} market=${geoFieldFlags.marketFieldExists} submarket=${geoFieldFlags.submarketFieldExists}`
      );
      if (
        !geoFieldFlags.continentFieldExists ||
        !geoFieldFlags.subContinentFieldExists ||
        !geoFieldFlags.marketFieldExists ||
        !geoFieldFlags.submarketFieldExists
      ) {
        log(
          `[mission] WARNING: missing geo fields — run ensure-hotel-property-census-market-geography-fields.mjs --apply first`
        );
      }
    } catch (err) {
      log(`[mission] schema probe failed (non-fatal): ${err?.message || err}`);
    }
  }

  // Before snapshot
  let beforeMetrics = opts.beforeMetrics || null;
  let censusBefore = null;

  if (!opts.skipBeforeSnapshot && token && bases?.target_base_id) {
    try {
      censusBefore = await listCensusForMission(bases.target_base_id, token, tableId);
      beforeMetrics = snapshotMissionCensusMetrics(censusBefore, {
        ...geoFieldFlags,
        env,
      });
      writeJson(path.join(runDir, "before-metrics.json"), beforeMetrics);
    } catch (err) {
      log(`[mission] before snapshot failed (non-fatal): ${err?.message || err}`);
    }
  }

  const phaseResults = [];
  const fieldsWritten = new Set();
  let updatesTotal = 0;
  let insertsTotal = 0;
  let safetyStop = null;
  let passesUsed = 0;

  for (const phase of phases) {
    if (safetyStop) break;

    const phaseDefault = phase.max_passes || 1;
    const phasePasses = maxPassesPerPhaseCap
      ? Math.min(phaseDefault, Math.max(1, maxPassesPerPhaseCap))
      : phaseDefault;
    log(
      `[mission] ${phase.label} — queues=${phase.queues.join(",")} passes≤${phasePasses}`
    );

    if (phase.read_only) {
      // Classification: run production-cycle with read-only queues (no writes expected)
      const phaseArgv = buildPhaseArgv(argv, {
        queues: phase.queues,
        cleanupExistingOnly: true,
        mode: "production-cycle",
      });
      const phaseArgs = parseAutopilotArgs(phaseArgv);
      phaseArgs.mode = "production-cycle";
      phaseArgs.cleanupExistingOnly = true;
      phaseArgs.queues = [...phase.queues];
      phaseArgs.queue = phase.queues.join(",");

      const phaseReport = await runProductionCycle({
        argv: phaseArgv,
        args: phaseArgs,
        env,
        enableProductionWrites: false,
        maxPasses: 1,
        runId: `${runName}/phases/${phase.id}`,
        root: autopilotRoot,
        skipInserts: true,
        missionPhase: phase,
        missionFilterProposals: (proposals) =>
          filterMissionPhaseProposals(proposals, phase),
        censusRecords: opts.censusRecords || null,
        skipLiveCensusReload: Boolean(opts.skipLiveCensusReload),
        ...geoFieldFlags,
        log: (msg) => log(`  ${msg}`),
      });
      passesUsed += 1;
      phaseResults.push({
        phase_id: phase.id,
        label: phase.label,
        read_only: true,
        updates_applied: 0,
        inserts_applied: 0,
        status: phaseReport.status,
        queues_executed: phaseReport.queues_executed || phase.queues,
      });
      writeJson(path.join(runDir, "phases", `${phase.id}.json`), phaseResults[phaseResults.length - 1]);
      continue;
    }

    const phaseArgv = buildPhaseArgv(argv, {
      queues: phase.queues,
      cleanupExistingOnly: Boolean(phase.cleanup_existing_only),
      mode: "production-cycle",
    });
    const phaseArgs = parseAutopilotArgs(phaseArgv);
    phaseArgs.mode = "production-cycle";
    phaseArgs.cleanupExistingOnly = Boolean(phase.cleanup_existing_only);
    phaseArgs.queues = [...phase.queues];
    phaseArgs.queue = phase.queues.join(",");
    phaseArgs.runUntilComplete = true;

    try {
      const phaseReport = await runProductionCycle({
        argv: phaseArgv,
        args: phaseArgs,
        env,
        enableProductionWrites: enableWrites && !phase.read_only,
        maxPasses: phasePasses,
        runId: path.join(runName, "phases", phase.id),
        root: autopilotRoot,
        skipInserts: true,
        missionPhase: phase,
        missionFilterProposals: (proposals, censusRecords) => {
          let next = filterMissionPhaseProposals(proposals, phase);
          if (phase.require_clean_core) {
            next = filterProposalsToCleanCoreRecords(next, censusRecords, {
              continentFieldExists: geoFieldFlags.continentFieldExists,
            });
          }
          return next;
        },
        censusRecords: opts.censusRecords || null,
        skipLiveCensusReload: Boolean(opts.skipLiveCensusReload),
        ...geoFieldFlags,
        log: (msg) => log(`  ${msg}`),
      });

      passesUsed += Number(phaseReport.passes_completed || phaseReport.passes || phasePasses);
      updatesTotal += phaseReport.updates_applied || 0;
      insertsTotal += phaseReport.inserts_applied || 0;
      for (const f of phaseReport.fields_written || []) fieldsWritten.add(f);

      const phaseEntry = {
        phase_id: phase.id,
        label: phase.label,
        updates_applied: phaseReport.updates_applied || 0,
        inserts_applied: phaseReport.inserts_applied || 0,
        steward_cases: phaseReport.steward_cases || 0,
        status: phaseReport.status,
        queues_executed: phaseReport.queues_executed || [],
        soft_deferred: phaseReport.queues_soft_deferred || phaseReport.queues_exhausted || [],
        safety_stop: phaseReport.safety_stops?.[0] || null,
        run_dir: phaseReport.run_dir || null,
      };
      phaseResults.push(phaseEntry);
      writeJson(path.join(runDir, "phases", `${phase.id}.json`), phaseEntry);

      if (phaseReport.safety_stops?.length || phaseReport.status?.includes("blocked")) {
        // Only hard-stop on true safety — soft blocks continue
        const stop = phaseReport.safety_stops?.[0];
        if (stop && isHardMissionSafetyStop(stop)) {
          safetyStop = stop;
          log(`[mission] HARD safety stop in ${phase.id}: ${stop.reason || stop.message}`);
          break;
        }
        log(
          `[mission] soft/blocked condition in ${phase.id} — stewarded; continuing mission`
        );
      }
    } catch (err) {
      const message = err?.message || String(err);
      log(`[mission] phase ${phase.id} error (continuing if soft): ${message}`);
      if (/wrong.?census|brand.?setup|brand.?explorer|forbidden|protected.?field|owner|operator/i.test(message)) {
        safetyStop = { reason: "phase_exception_hard", message };
        break;
      }
      phaseResults.push({
        phase_id: phase.id,
        label: phase.label,
        error: message,
        updates_applied: 0,
        continued: true,
      });
    }
  }

  // After snapshot
  let afterMetrics = beforeMetrics;
  if (!opts.skipAfterSnapshot && token && bases?.target_base_id && enableWrites) {
    try {
      const afterRecords = await listCensusForMission(bases.target_base_id, token, tableId);
      afterMetrics = snapshotMissionCensusMetrics(afterRecords, {
        ...geoFieldFlags,
        env,
      });
      writeJson(path.join(runDir, "after-metrics.json"), afterMetrics);
    } catch (err) {
      log(`[mission] after snapshot failed (non-fatal): ${err?.message || err}`);
    }
  } else if (opts.afterMetrics) {
    afterMetrics = opts.afterMetrics;
  }

  const status = resolveMissionStatus({
    safetyStop,
    after: afterMetrics || {},
    objective,
  });

  const softTargetsMet =
    objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1 ||
    objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
      ? {
          address_complete:
            (afterMetrics?.address_complete ?? 0) >= softTargets.address_complete_min,
          lat_long_complete:
            (afterMetrics?.lat_long_complete ?? 0) >= softTargets.lat_long_complete_min,
          phone_complete:
            (afterMetrics?.phone_complete ?? 0) >= softTargets.phone_complete_min,
          rooms_complete:
            (afterMetrics?.rooms_complete ?? 0) >= softTargets.rooms_complete_min,
          complete_census_v1:
            (afterMetrics?.complete_census_v1 ?? 0) >= softTargets.complete_census_v1_min,
        }
      : {
          clean_core:
            (afterMetrics?.clean_core ?? 0) >= softTargets.clean_core_min,
          unknown_city:
            (afterMetrics?.unknown_city ?? 999) <= softTargets.unknown_city_max,
          canonical_blank:
            (afterMetrics?.canonical_blank ?? 999) <= softTargets.canonical_blank_max,
          coordinate_blocked_dirty:
            (afterMetrics?.coordinate_blocked_dirty_identity ?? 999) <=
            softTargets.coordinate_blocked_dirty_max,
        };

  const finalSummary = {
    ok: status !== statusEnum.BLOCKED,
    version: MISSION_VERSION,
    status,
    mode: "mission",
    objective,
    generated_at: new Date().toISOString(),
    run_dir: runDir,
    airtable_writes: enableWrites && updatesTotal > 0,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    inserts_applied: 0,
    updates_applied: updatesTotal,
    fields_written: [...fieldsWritten],
    queues_executed: [...new Set(phaseResults.flatMap((p) => p.queues_executed || []))],
    phases: phaseResults,
    before: beforeMetrics,
    after: afterMetrics,
    soft_targets: softTargets,
    soft_targets_met: softTargetsMet,
    safety_stops: safetyStop ? [safetyStop] : [],
    steward_remaining: afterMetrics?.steward_remaining ?? null,
    source_lookup_remaining: afterMetrics?.source_lookup_remaining ?? null,
    duplicate_risk_remaining: afterMetrics?.duplicate_risk_remaining ?? null,
    runtime_ms: Date.now() - started,
    next_recommended_action:
      status === statusEnum.BLOCKED
        ? "Resolve hard safety stop, then re-run mission."
        : status === statusEnum.PARTIAL
          ? objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
            ? "Official sources exhausted for some Level 2 fields — dirty partner labels remain parked; do not invent address/phone/rooms/coords or promote Brand Setup automatically."
            : objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1
              ? "Official sources exhausted for some Level 2 fields — steward/source-lookup remaining; do not invent address/phone/rooms/coords."
              : "Review steward / source-lookup remaining; re-run mission or targeted parent backfill. Do not invent data."
          : objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
            ? "CALA Census Completion v1 finished for eligible Clean Core rows — review Brand Setup promotion pack separately; dirty partners stay stewarded."
            : objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1
              ? "Complete Census v1 mission finished — review Map/Contact/Size readiness before owner-facing release."
              : "Clean Census v1 mission complete — review public readiness gates before owner-facing release.",
  };

  writeJson(path.join(runDir, "phase-results.json"), phaseResults);
  writeJson(path.join(runDir, "final-summary.json"), finalSummary);
  writeText(
    path.join(runDir, "final-summary.md"),
    renderMissionFinalMd(finalSummary)
  );
  writeJson(path.join(runDir, "records-updated.json"), {
    count: updatesTotal,
    fields_written: [...fieldsWritten],
  });
  writeJson(path.join(runDir, "steward-review-queue.json"), {
    steward_remaining: afterMetrics?.steward_remaining ?? 0,
    source_lookup_remaining: afterMetrics?.source_lookup_remaining ?? 0,
    duplicate_risk_remaining: afterMetrics?.duplicate_risk_remaining ?? 0,
    by_class: afterMetrics?.by_class || {},
  });
  writeText(
    path.join(runDir, "blocked-records.csv"),
    "record_id,reason\n"
  );

  // Mirror reports + docs
  writeMissionPublicReports(finalSummary, { runDir, objective });

  log(`[mission] complete status=${status} updates=${updatesTotal}`);
  return finalSummary;
}

/** Alias — Complete Census v1 uses the same runner with --objective complete-census-v1. */
export const runCompleteCensusV1Mission = runCleanCensusV1Mission;

function isHardMissionSafetyStop(stop) {
  const r = String(stop?.reason || stop?.message || "").toLowerCase();
  return /wrong.?census|brand.?setup|brand.?explorer|forbidden|protected|owner|operator|vic|legacy.?census|schema.?conflict|repeated.?airtable|systemic.?duplicate/.test(
    r
  );
}

function buildPhaseArgv(baseArgv, opts = {}) {
  const next = baseArgv.filter((a) => {
    if (a === "--mode" || a === "--queue" || a === "--objective") return false;
    return true;
  });
  // Remove values that followed stripped flags
  const cleaned = [];
  for (let i = 0; i < next.length; i++) {
    const prev = next[i - 1];
    if (prev === "--mode" || prev === "--queue" || prev === "--objective") continue;
    // also skip the value tokens if we removed flags from base — safer rebuild
    cleaned.push(next[i]);
  }
  // Rebuild cleanly from base without mode/queue
  const rebuild = [];
  for (let i = 0; i < baseArgv.length; i++) {
    const a = baseArgv[i];
    if (a === "--mode" || a === "--queue" || a === "--objective" || a === "--max-passes") {
      i += 1; // skip value
      continue;
    }
    rebuild.push(a);
  }
  rebuild.push("--mode", opts.mode || "production-cycle");
  rebuild.push("--queue", (opts.queues || []).join(","));
  if (opts.cleanupExistingOnly) {
    if (!rebuild.includes("--cleanup-existing-only")) {
      rebuild.push("--cleanup-existing-only");
    }
  } else {
    const idx = rebuild.indexOf("--cleanup-existing-only");
    if (idx >= 0) rebuild.splice(idx, 1);
  }
  return rebuild;
}

async function listCensusForMission(baseId, token, tableId) {
  const fields = [
    "Property Identity Key",
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "Brand Family",
    "Country",
    "City",
    "State / Region",
    "Address",
    "Address Confidence",
    "Address Source URL",
    "Source URL",
    "Official Property URL",
    "Human Review Required",
    "Production Use Status",
    "Family / Source Family",
    "Identity Confidence",
    "Data Confidence Tier",
    "Enrichment Status",
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
    "Latitude",
    "Longitude",
    "Phone",
    "Rooms / Keys",
  ];
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

function renderMissionFinalMd(s) {
  const b = s.before || {};
  const a = s.after || {};
  const title =
    s.objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
      ? "CALA Census Completion Mission — Final Summary"
      : s.objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1
        ? "Complete CALA Census Mission — Final Summary"
        : "Clean CALA Census Mission — Final Summary";
  return `# ${title}

**Status:** \`${s.status}\`  
**Objective:** \`${s.objective}\`  
**Generated:** ${s.generated_at}  
**Airtable writes:** ${s.airtable_writes ? "yes" : "no"}  
**Inserts:** ${s.inserts_applied}  
**Updates:** ${s.updates_applied}  
**Run dir:** \`${s.run_dir}\`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | ${b.total_records ?? "—"} | ${a.total_records ?? "—"} |
| Clean Core | ${b.clean_core ?? "—"} | ${a.clean_core ?? "—"} |
| Below Clean Core | ${b.below_clean_core ?? "—"} | ${a.below_clean_core ?? "—"} |
| Continent complete | ${b.continent_complete ?? "—"} | ${a.continent_complete ?? "—"} |
| Sub-Continent complete | ${b.subcontinent_complete ?? "—"} | ${a.subcontinent_complete ?? "—"} |
| Market complete | ${b.market_complete ?? "—"} | ${a.market_complete ?? "—"} |
| Submarket complete | ${b.submarket_complete ?? "—"} | ${a.submarket_complete ?? "—"} |
| State / Region complete | ${b.state_region_complete ?? "—"} | ${a.state_region_complete ?? "—"} |
| Address complete | ${b.address_complete ?? "—"} | ${a.address_complete ?? "—"} |
| Address Confidence High | ${b.address_confidence_high ?? "—"} | ${a.address_confidence_high ?? "—"} |
| Address Source URL complete | ${b.address_source_url_complete ?? "—"} | ${a.address_source_url_complete ?? "—"} |
| Lat/Long complete | ${b.lat_long_complete ?? "—"} | ${a.lat_long_complete ?? "—"} |
| Mapbox eligible | ${b.mapbox_eligible ?? "—"} | ${a.mapbox_eligible ?? "—"} |
| Phone complete | ${b.phone_complete ?? "—"} | ${a.phone_complete ?? "—"} |
| Rooms complete | ${b.rooms_complete ?? "—"} | ${a.rooms_complete ?? "—"} |
| Complete Census v1 | ${b.complete_census_v1 ?? "—"} | ${a.complete_census_v1 ?? "—"} |
| Map Ready | ${b.map_ready ?? "—"} | ${a.map_ready ?? "—"} |
| Contact Ready | ${b.contact_ready ?? "—"} | ${a.contact_ready ?? "—"} |
| Size Ready | ${b.size_ready ?? "—"} | ${a.size_ready ?? "—"} |
| Blocked missing address | ${b.blocked_missing_address ?? "—"} | ${a.blocked_missing_address ?? "—"} |
| Blocked dirty identity | ${b.blocked_dirty_identity ?? "—"} | ${a.blocked_dirty_identity ?? "—"} |
| Blocked source access | ${b.blocked_source_access ?? "—"} | ${a.blocked_source_access ?? "—"} |
| Coord blocked dirty identity | ${b.coordinate_blocked_dirty_identity ?? "—"} | ${a.coordinate_blocked_dirty_identity ?? "—"} |
| Steward remaining | ${b.steward_remaining ?? "—"} | ${a.steward_remaining ?? "—"} |
| Source lookup remaining | ${b.source_lookup_remaining ?? "—"} | ${a.source_lookup_remaining ?? "—"} |
| Duplicate risk remaining | ${b.duplicate_risk_remaining ?? "—"} | ${a.duplicate_risk_remaining ?? "—"} |
| Est. Mapbox requests | ${b.estimated_mapbox_requests ?? "—"} | ${a.estimated_mapbox_requests ?? "—"} |
| Est. Mapbox cost USD | ${b.estimated_mapbox_cost_usd ?? "—"} | ${a.estimated_mapbox_cost_usd ?? "—"} |

## Soft targets met

${Object.entries(s.soft_targets_met || {})
  .map(([k, v]) => `- ${k}: ${v ? "yes" : "no"}`)
  .join("\n")}

## Fields written

${(s.fields_written || []).map((f) => `- ${f}`).join("\n") || "- (none)"}

## Queues executed

${(s.queues_executed || []).map((q) => `- ${q}`).join("\n") || "- (none)"}

## Phases

${(s.phases || [])
  .map(
    (p) =>
      `- **${p.label || p.phase_id}**: updates=${p.updates_applied ?? 0}${
        p.error ? ` · error=${p.error}` : ""
      }`
  )
  .join("\n")}

## Safety stops

${
  (s.safety_stops || []).length
    ? (s.safety_stops || []).map((x) => `- ${x.reason || JSON.stringify(x)}`).join("\n")
    : "- (none)"
}

## Next recommended action

${s.next_recommended_action}
`;
}

export function writeMissionPublicReports(finalSummary, opts = {}) {
  const reportsDir =
    opts.reportsDir || path.join(ROOT, "reports/research-engine-v2");
  const docsDir = opts.docsDir || path.join(ROOT, "docs/data-intelligence");
  const objective =
    opts.objective || finalSummary.objective || MISSION_OBJECTIVE_CLEAN_CENSUS_V1;
  const baseName =
    objective === MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1 ||
    objective === MISSION_OBJECTIVE_LEVEL_2_ADAPTER_WAVE_2
      ? "production-census-level-2-adapter-wave-2"
      : objective === MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
        ? "production-census-cala-completion-v1"
        : objective === MISSION_OBJECTIVE_COMPLETE_CENSUS_V1
          ? "production-census-complete-census-v1-mission"
          : "production-census-clean-census-v1-mission";
  const jsonPath = path.join(reportsDir, `${baseName}.json`);
  const mdPath = path.join(reportsDir, `${baseName}.md`);
  const docsPath = path.join(docsDir, `${baseName}.md`);
  const md = renderMissionFinalMd(finalSummary);
  writeJson(jsonPath, finalSummary);
  writeText(mdPath, md);
  writeText(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}

// Re-export for tests / CLI
export { isProductionWriteMode };
