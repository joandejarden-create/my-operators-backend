/**
 * Census Autopilot V3 — Governed Airtable Migration Pilot (Phase 1 dry-run).
 * NO WRITES unless ENABLE_VERIFIED_CENSUS_WRITES=1 AND Joan explicitly authorizes Phase 2.
 */

export const AUTOPILOT_V3_VERSION = "census-autopilot-v3-airtable-migration";
export const OUT_REL = "data/research-engine-v2/census-autopilot-v3-airtable-migration";
export const V23_OUT_REL = "data/research-engine-v2/census-autopilot-v2-3-independent-universe";

export const PHASE2_ENV_GATE = "ENABLE_VERIFIED_CENSUS_WRITES";

export const WRITE_CLASS = Object.freeze({
  AUTO_WRITE_SAFE: "AUTO_WRITE_SAFE",
  CORROBORATED_WRITE: "CORROBORATED_WRITE",
  STEWARD_REVIEW: "STEWARD_REVIEW",
  FIRST_PARTY_VALIDATION: "FIRST_PARTY_VALIDATION",
  BLOCKED_RIGHTS: "BLOCKED_RIGHTS",
  PROHIBITED: "PROHIBITED",
});

export const MATCH_CLASS = Object.freeze({
  NEW_INSERT: "NEW_INSERT",
  EXACT_EXISTING_MATCH: "EXACT_EXISTING_MATCH",
  HIGH_EXISTING_MATCH: "HIGH_EXISTING_MATCH",
  POSSIBLE_DUPLICATE: "POSSIBLE_DUPLICATE",
  IDENTITY_CONFLICT: "IDENTITY_CONFLICT",
});

export const VERIFIED_STATE = Object.freeze({
  GOLDEN_COMPLETE: "VERIFIED — GOLDEN COMPLETE",
  ROOMS_PENDING: "VERIFIED — ROOMS PENDING",
  MATERIAL_GAPS: "VERIFIED — MATERIAL GAPS",
  FIRST_PARTY_PENDING: "VERIFIED — FIRST-PARTY VALIDATION PENDING",
  RESEARCH_ESCALATION: "RESEARCH ESCALATION",
  IDENTITY_CONFLICT: "IDENTITY CONFLICT",
});

export const CIRCUIT_BREAKERS = Object.freeze({
  max_duplicate_insert_rate: 0,
  max_identity_conflicts_on_write: 0,
  max_cvent_leakage: 0,
  max_legacy_leakage: 0,
  max_missing_provenance: 0,
  max_error_rate_pct: 5,
  max_unintended_linked_mutations: 0,
  pilot_a_size: 25,
  pilot_b_target_total: 150,
});

/** Country short codes for Property Identity Key (production convention). */
export const COUNTRY_SHORT = Object.freeze({
  Mexico: "mx",
  "Dominican Republic": "do",
  "Costa Rica": "cr",
  Colombia: "co",
  Brazil: "br",
  Argentina: "ar",
  Jamaica: "jm",
  Barbados: "bb",
});
