/**
 * ADP Monthly Executive Review V1 — product contract.
 * Composition layer over certified published ADP — does not alter measurement.
 * Golden fixtures remain for regression; production eligibility is registry + published data.
 */

export const MONTHLY_REVIEW_SCHEMA = "ADP_MONTHLY_EXECUTIVE_REVIEW_V1";
export const MONTHLY_REVIEW_CONTRACT_VERSION = "ADP_MONTHLY_EXECUTIVE_REVIEW_CONTRACT_V1";
export const MONTHLY_REVIEW_PRODUCT_TITLE = "Dealality AI Demand Performance Review";
export const MONTHLY_REVIEW_PRODUCT_SUBTITLE = "Monthly Executive Review";

/** Fixture regression pair only — not the production eligibility universe. */
export const GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS = Object.freeze([
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
]);

/**
 * Internal review archetypes (not customer-facing labels).
 * Guides callout selection, action density, and Watch vs Performance Risk tone.
 */
export const REVIEW_POSTURE = Object.freeze({
  STRONG_PERFORMER: "STRONG_PERFORMER",
  MIXED: "MIXED",
  CORRECTIVE: "CORRECTIVE",
  BASELINE_LIMITED: "BASELINE_LIMITED",
  STABLE_LOW: "STABLE_LOW",
  STABLE_STRONG: "STABLE_STRONG",
});

/** @deprecated alias — prefer REVIEW_POSTURE / archetype */
export const REVIEW_ARCHETYPE = REVIEW_POSTURE;

export const MONTHLY_REVIEW_ELIGIBILITY_STATUS = Object.freeze({
  READY: "READY",
  READY_WITH_DISCLOSURES: "READY_WITH_DISCLOSURES",
  NO_CURRENT_CERTIFIED_PERIOD: "NO_CURRENT_CERTIFIED_PERIOD",
  NO_VALID_PRIOR: "NO_VALID_PRIOR",
  ANALYTICAL_RECONCILIATION_FAILED: "ANALYTICAL_RECONCILIATION_FAILED",
  MISSING_REQUIRED_PAYLOAD: "MISSING_REQUIRED_PAYLOAD",
  GENERATION_FAILED: "GENERATION_FAILED",
});

export const CLIENT_READY_ELIGIBILITY_STATUSES = Object.freeze([
  MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY,
  MONTHLY_REVIEW_ELIGIBILITY_STATUS.READY_WITH_DISCLOSURES,
]);

/** Peer count below this suppresses BPP benchmark / presence index in customer surfaces. */
export const BPP_BENCHMARK_MIN_PEERS = 5;

export const ACTION_STATUS = Object.freeze({
  OPEN: "Open",
  IN_PROGRESS: "In Progress",
  COMPLETED: "Completed",
  DEFERRED: "Deferred",
  MONITOR: "Monitor",
});

export const DEFAULT_OWNER_ROLE = "Owner to be confirmed";

export const MOVEMENT_CLASS = Object.freeze({
  MATERIAL_IMPROVEMENT: "MATERIAL IMPROVEMENT",
  MATERIAL_DECLINE: "MATERIAL DECLINE",
  STABLE: "STABLE",
  MONITOR: "MONITOR",
});

/**
 * Governed heuristic from ADP executiveRead.trend.materialityPp when present.
 * Do not invent alternate thresholds.
 */
export const FALLBACK_MATERIALITY_PP = 2;

export const ACTION_CAP_BY_ARCHETYPE = Object.freeze({
  [REVIEW_POSTURE.STRONG_PERFORMER]: 3,
  [REVIEW_POSTURE.STABLE_STRONG]: 3,
  [REVIEW_POSTURE.MIXED]: 4,
  [REVIEW_POSTURE.CORRECTIVE]: 5,
  [REVIEW_POSTURE.STABLE_LOW]: 5,
  [REVIEW_POSTURE.BASELINE_LIMITED]: 2,
});

export const REQUIRED_GATES = Object.freeze([
  "SAME_CONCEPT_SAME_CANONICAL_SOURCE",
  "CURRENT_POSITION_FIRST",
  "PRIOR_RUN_SECONDARY",
  "EVIDENCE_BEFORE_ACTION",
  "NO_CAUSATION_WITHOUT_EVIDENCE",
  "ZERO_NULL_MISSING_SUPPRESSED_INTEGRITY",
  "CUSTOMER_SAFE_METHODOLOGY",
  "ACTION_MUST_TRACE_TO_OBSERVED_ISSUE",
  "ACTION_MUST_HAVE_ACCOUNTABILITY",
  "ACTION_CAUSATION_LANGUAGE_INTEGRITY",
  "MONTHLY_REVIEW_SINGLE_CANONICAL_PAYLOAD",
  "MONTHLY_REVIEW_ACTION_ACCOUNTABILITY_COMPLETE",
  "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
  "MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE",
  "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
  "ADP_MONTHLY_REVIEW_NO_PROPERTY_SPECIFIC_BUILDER_FORKS",
  "ADP_MONTHLY_REVIEW_EXECUTIVE_ASSESSMENT_PROPERTY_SPECIFIC",
  "ADP_MONTHLY_REVIEW_MOVEMENT_COMPARABILITY_INTEGRITY",
  "ADP_MONTHLY_REVIEW_CROSS_PROPERTY_ISOLATION",
  "ADP_MONTHLY_REVIEW_NO_CROSS_PROPERTY_ENTITY_LEAKAGE",
  "ADP_MONTHLY_REVIEW_NEW_HOTEL_ZERO_CODE_PATH",
  "MONTHLY_REVIEW_COMPETITOR_EVIDENCE_TRACE_INTEGRITY",
]);

export const FORBIDDEN_CAUSATION_PATTERNS = Object.freeze([
  /\bwill increase\b/i,
  /\bwill improve\b/i,
  /\bwill raise\b/i,
  /\bguarantees?\b/i,
  /\bsupercharge\b/i,
  /\boptimize AI\b/i,
  /\bAI optimization\b/i,
  /\bboost visibility\b/i,
]);

export const FORBIDDEN_CLEVER_PATTERNS = Object.freeze([
  /\bsupercharge\b/i,
  /\bunleash\b/i,
  /\bgame[- ]changer\b/i,
  /\bsecret sauce\b/i,
  /\block in dominance\b/i,
  /\bAI ecosystem\b/i,
  /\bninja\b/i,
  /\bhack\b/i,
]);
