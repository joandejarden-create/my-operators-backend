/**
 * Validation status vocabulary — no High/Medium/Low confidence.
 */

export const VALIDATION_STATUS_VERSION = "ai_intelligence_validation_status_v1";

export const OVERALL_VALIDATION_STATUS = Object.freeze({
  PASS: "PASS",
  PASS_WITH_LIMITATIONS: "PASS_WITH_LIMITATIONS",
  FAIL: "FAIL",
  NOT_VALIDATED: "NOT_VALIDATED",
});

export const METRIC_VALIDATION_STATE = Object.freeze({
  RECONCILED: "RECONCILED",
  VALIDATED: "VALIDATED",
  OBSERVED: "OBSERVED",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  NOT_MONITORED: "NOT_MONITORED",
  VALIDATION_FAILED: "VALIDATION_FAILED",
});

export const GATE_STATUS = Object.freeze({
  PASS: "PASS",
  PROVISIONAL_PASS: "PROVISIONAL_PASS",
  FAIL: "FAIL",
  REVIEW: "REVIEW",
  NOT_RUN: "NOT_RUN",
  THRESHOLD_NOT_YET_GOVERNED: "THRESHOLD_NOT_YET_GOVERNED",
});

export const METHODOLOGY_NOTE =
  "AI model outputs are probabilistic and may vary between monitoring runs. Dealality validates calculation accuracy, evidence traceability, and monitoring consistency, but AI-generated responses should be interpreted as observed model behavior rather than deterministic fact.";

export const OPERATIONAL_METHODOLOGY_NOTE =
  "Monitoring coverage and cost figures reflect the runs stored by Dealality. Cost is shown as estimated unless reconciled to provider billing.";
