/**
 * DEALALITY CENSUS AUTOPILOT V1 — shared constants.
 * No Airtable writes. No Webhound. No credit spend.
 */

export const AUTOPILOT_V1_VERSION = "census-autopilot-v1.0.0";
export const AUTOPILOT_V1_ARTIFACT_DIR = "data/research-engine-v2/census-autopilot-v1";

/** Field resolution statuses (canonical). */
export const FIELD_RESOLUTION_STATUS = Object.freeze({
  VERIFIED: "Verified",
  CONFIRMED_EXISTING: "Confirmed Existing",
  MISSING_FOUND: "Missing — Found",
  SUPERSEDED: "Superseded",
  CONTRADICTED: "Contradicted",
  UNKNOWN_NO_EVIDENCE: "Unknown — No Reliable Evidence",
  CONFLICTING_EVIDENCE: "Conflicting Evidence",
  NOT_APPLICABLE: "Not Applicable",
  DERIVED: "Derived",
  SOURCE_BLOCKED: "Source Blocked",
  DEEP_RESEARCH_REQUIRED: "Deep Research Required",
});

/** Hotel-level output classes — no automatic production write. */
export const OUTPUT_CLASS = Object.freeze({
  VERIFIED_PRODUCTION_CANDIDATE: "VERIFIED — PRODUCTION CANDIDATE",
  VERIFIED_MATERIAL_REMEDIATION: "VERIFIED — MATERIAL REMEDIATION REQUIRED",
  PARTIAL_NONCRITICAL: "PARTIAL — NONCRITICAL GAPS",
  DEEP_RESEARCH_REQUIRED: "DEEP RESEARCH REQUIRED",
  FIRST_PARTY_VALIDATION_REQUIRED: "FIRST-PARTY VALIDATION REQUIRED",
  SOURCE_RIGHTS_REVIEW_REQUIRED: "SOURCE RIGHTS REVIEW REQUIRED",
  HOLD_CONFLICTING: "HOLD — CONFLICTING EVIDENCE",
  REFERENCE_CHALLENGE: "REFERENCE CHALLENGE — NOT INDEPENDENTLY CONFIRMED",
});

export const PRIORITY_BAND = Object.freeze({
  P0: "P0 Critical",
  P1: "P1 High",
  P2: "P2 Medium",
  P3: "P3 Low",
});

export const SOURCE_LANE = Object.freeze({
  A_STRUCTURED_OFFICIAL: "Lane A — Structured Official",
  B_INDEPENDENT_PROPERTY: "Lane B — Independent Property Research",
  C_DEEP_ESCALATION: "Lane C — Deep Research Escalation",
});

/** Hard constraints for every Autopilot V1 run. */
export const AUTOPILOT_V1_CONSTRAINTS = Object.freeze({
  no_webhound: true,
  no_credit_spend: true,
  no_airtable_writes: true,
  legacy_used_as_source: false,
  cvent_used_as_source: false,
  no_automatic_brand_activation: true,
  no_automatic_image_rehost: true,
  unknown_is_acceptable: true,
  preserve_clean_room_firewall: true,
  preserve_source_rights: true,
  preserve_steward_governance: true,
});
