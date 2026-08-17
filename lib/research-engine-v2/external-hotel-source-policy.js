/**
 * External hotel content source policy — field approvals, write gates, hard blocks.
 */

import {
  EXTERNAL_SOURCE_TIER,
  resolveExternalSource,
} from "./external-hotel-source-registry.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";

export const EXTERNAL_HOTEL_SOURCE_POLICY_VERSION =
  "external-hotel-source-policy-v1";

/** Census fields in scope for external enrichment (never owner/operator/dates). */
export const EXTERNAL_ENRICHMENT_FIELDS = Object.freeze([
  "Canonical Property Name",
  "Property Name",
  "State / Region",
  "Address",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Official Property URL",
  "Hotel Description - AI Summary",
  "Current Brand",
  "Brand Family",
]);

/**
 * Field → approved source IDs (plus special tokens).
 * Special tokens: official_web, tourism_registry_any, licensed_master, licensed_content_api
 */
export const FIELD_SOURCE_APPROVALS = Object.freeze({
  "Canonical Property Name": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "tourism_registry",
    "cvent",
    "costar",
  ],
  "Property Name": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "tourism_registry",
  ],
  Address: [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "tourism_registry",
    "google_places",
    "openstreetmap",
    "cvent",
    "costar",
  ],
  "State / Region": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "tourism_registry",
    "google_places",
  ],
  Market: ["hotel_brand_websites", "tourism_registry"],
  Submarket: ["hotel_brand_websites"],
  Latitude: [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "google_places",
    "openstreetmap",
    "costar",
  ],
  Longitude: [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "google_places",
    "openstreetmap",
    "costar",
  ],
  Phone: [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "tourism_registry",
    "google_places",
    "cvent",
  ],
  "Rooms / Keys": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "costar",
    "cvent",
    "tourism_registry",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
  ],
  "Official Property URL": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
    "google_places",
    // Cvent listings often surface the brand/official hotel website (non-cvent host).
    "cvent",
  ],
  "Hotel Description - AI Summary": [
    "hotel_brand_websites",
    "expedia",
    "booking",
    "hotelbeds",
    "amadeus",
  ],
  // Source Text is raw listing copy; Cvent listingText is acceptable Medium raw text.
  "Hotel Description - Source Text": [
    "hotel_brand_websites",
    "cvent",
    "northstar",
    "giata",
  ],
  "Current Brand": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "amadeus",
    "costar",
    "cvent",
  ],
  "Brand Family": [
    "hotel_brand_websites",
    "giata",
    "northstar",
    "expedia",
    "amadeus",
    "costar",
  ],
});

/** Hard field blocks regardless of license. */
export const FIELD_SOURCE_HARD_BLOCKS = Object.freeze({
  "Rooms / Keys": ["google_places", "openstreetmap", "ota_consumer_sites"],
  Phone: ["ota_consumer_sites"],
  Address: ["ota_consumer_sites"],
});

/**
 * Policy-gated sources that need env flags beyond vendor approval.
 */
export const POLICY_GATES = Object.freeze({
  google_places: {
    env: "GOOGLE_PLACES_STORAGE_TERMS_REVIEWED",
    reason: "google_places_storage_policy_not_approved",
  },
  openstreetmap: {
    env: "OSM_ODBL_COMPLIANCE_POLICY_APPROVED",
    reason: "openstreetmap_odbl_policy_not_approved",
  },
  costar: {
    env: "COSTAR_STR_CENSUS_LICENSE_APPROVED",
    reason: "costar_str_license_not_approved",
  },
  str: {
    env: "COSTAR_STR_CENSUS_LICENSE_APPROVED",
    reason: "costar_str_license_not_approved",
  },
});

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function resolveExternalWriteGate(env = process.env) {
  const writesEnabled =
    String(env.ENABLE_EXTERNAL_HOTEL_CONTENT_WRITES || "0").trim() === "1";
  const approvedSource = String(env.APPROVED_EXTERNAL_SOURCE || "")
    .trim()
    .toLowerCase();
  return {
    version: EXTERNAL_HOTEL_SOURCE_POLICY_VERSION,
    enable_external_hotel_content_writes: writesEnabled,
    approved_external_source: approvedSource || null,
    writes_allowed: writesEnabled && Boolean(approvedSource),
  };
}

/**
 * @param {string} field
 * @param {string} sourceId
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function isFieldApprovedForSource(field, sourceId, env = process.env) {
  const src = resolveExternalSource(sourceId);
  const id = src?.id || String(sourceId || "").toLowerCase();

  if (isForbiddenAutopilotField(field)) {
    return {
      ok: false,
      reason: "forbidden_autopilot_field",
      field,
      source_id: id,
    };
  }

  if (id === "ota_consumer_sites") {
    return { ok: false, reason: "ota_consumer_scrape_hard_blocked", field, source_id: id };
  }

  const hard = FIELD_SOURCE_HARD_BLOCKS[field] || [];
  if (hard.includes(id)) {
    return {
      ok: false,
      reason: "field_source_hard_blocked",
      field,
      source_id: id,
    };
  }

  const approved = FIELD_SOURCE_APPROVALS[field];
  if (!approved) {
    return { ok: false, reason: "field_not_in_external_enrichment_scope", field, source_id: id };
  }
  if (!approved.includes(id)) {
    return {
      ok: false,
      reason: "source_not_approved_for_field",
      field,
      source_id: id,
    };
  }

  const gate = POLICY_GATES[id];
  if (gate && String(env[gate.env] || "").trim() !== "1") {
    return {
      ok: false,
      reason: gate.reason,
      field,
      source_id: id,
      required_env: gate.env,
    };
  }

  // Rooms from content APIs need explicit rooms-meaning approval
  if (
    field === "Rooms / Keys" &&
    src?.tier === EXTERNAL_SOURCE_TIER.B_LICENSED_CONTENT_API &&
    String(env.APPROVE_CONTENT_API_ROOMS_FIELD_SEMANTICS || "").trim() !== "1"
  ) {
    return {
      ok: false,
      reason: "content_api_rooms_semantics_not_approved",
      field,
      source_id: id,
      required_env: "APPROVE_CONTENT_API_ROOMS_FIELD_SEMANTICS",
    };
  }

  return { ok: true, reason: null, field, source_id: id };
}

/**
 * Can this source write at all under current env?
 * @param {string} sourceId
 * @param {{ pilotOnly?: boolean, env?: NodeJS.ProcessEnv }} [opts]
 */
export function assertExternalSourceWriteAllowed(sourceId, opts = {}) {
  const env = opts.env || process.env;
  const gate = resolveExternalWriteGate(env);
  const src = resolveExternalSource(sourceId);
  const id = src?.id || String(sourceId || "").toLowerCase();

  if (opts.pilotOnly) {
    return {
      ok: false,
      write: false,
      reason: "pilot_only_mode_no_writes",
      source_id: id,
      gate,
    };
  }

  if (id === "ota_consumer_sites") {
    return {
      ok: false,
      write: false,
      reason: "ota_consumer_scrape_hard_blocked",
      source_id: id,
      gate,
    };
  }

  if (!gate.enable_external_hotel_content_writes) {
    return {
      ok: false,
      write: false,
      reason: "ENABLE_EXTERNAL_HOTEL_CONTENT_WRITES_not_set",
      source_id: id,
      gate,
    };
  }

  if (!gate.approved_external_source) {
    return {
      ok: false,
      write: false,
      reason: "APPROVED_EXTERNAL_SOURCE_missing",
      source_id: id,
      gate,
    };
  }

  const approved = resolveExternalSource(gate.approved_external_source);
  if (!approved || approved.id !== id) {
    return {
      ok: false,
      write: false,
      reason: "source_not_equal_to_APPROVED_EXTERNAL_SOURCE",
      source_id: id,
      approved: gate.approved_external_source,
      gate,
    };
  }

  if (src?.go_no_go === "no_go_hard_block") {
    return { ok: false, write: false, reason: "source_hard_blocked", source_id: id, gate };
  }

  const policyGate = POLICY_GATES[id];
  if (policyGate && String(env[policyGate.env] || "").trim() !== "1") {
    return {
      ok: false,
      write: false,
      reason: policyGate.reason,
      source_id: id,
      required_env: policyGate.env,
      gate,
    };
  }

  return { ok: true, write: true, reason: null, source_id: id, gate };
}

/**
 * Filter proposed patch to approved fields for source.
 * @param {Record<string, unknown>} patch
 * @param {string} sourceId
 * @param {NodeJS.ProcessEnv} [env]
 */
export function sanitizeExternalPatch(patch, sourceId, env = process.env) {
  const out = {};
  const rejected = [];
  for (const [field, value] of Object.entries(patch || {})) {
    if (isForbiddenAutopilotField(field)) {
      rejected.push({ field, reason: "forbidden_autopilot_field" });
      continue;
    }
    const check = isFieldApprovedForSource(field, sourceId, env);
    if (!check.ok) {
      rejected.push({ field, reason: check.reason });
      continue;
    }
    if (value === undefined || value === null || value === "") continue;
    out[field] = value;
  }
  return {
    ok: Object.keys(out).length > 0,
    sanitized_payload_preview: out,
    rejected,
    field_mapping: Object.fromEntries(
      Object.keys(out).map((k) => [k, k])
    ),
  };
}
