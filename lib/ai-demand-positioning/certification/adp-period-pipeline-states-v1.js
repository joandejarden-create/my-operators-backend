/**
 * Global ADP period pipeline states (certification / publication lifecycle).
 * Audit-only taxonomy — does not change measurement methodology or thresholds.
 */

export const ADP_PERIOD_PIPELINE_STATES_VERSION = "adp_period_pipeline_states_v1";

/** Explicit ADP period states (Part 2). */
export const ADP_PERIOD_PIPELINE_STATES = Object.freeze({
  DRAFT: "DRAFT",
  RUNNING: "RUNNING",
  QA_FAILED: "QA_FAILED",
  QA_REVIEW_REQUIRED: "QA_REVIEW_REQUIRED",
  QA_PASSED: "QA_PASSED",
  CERTIFIED: "CERTIFIED",
  SUPERSEDED: "SUPERSEDED",
  LEGACY_UNCERTIFIED: "LEGACY_UNCERTIFIED",
});

/** Statuses allowed on customer-facing official baselines by default. */
export const ADP_CUSTOMER_OFFICIAL_ALLOWED_STATES = Object.freeze([
  ADP_PERIOD_PIPELINE_STATES.CERTIFIED,
]);

/** Statuses allowed for internal/Admin preview. */
export const ADP_ADMIN_PREVIEW_ALLOWED_STATES = Object.freeze([
  ADP_PERIOD_PIPELINE_STATES.DRAFT,
  ADP_PERIOD_PIPELINE_STATES.RUNNING,
  ADP_PERIOD_PIPELINE_STATES.QA_FAILED,
  ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED,
  ADP_PERIOD_PIPELINE_STATES.QA_PASSED,
  ADP_PERIOD_PIPELINE_STATES.CERTIFIED,
  ADP_PERIOD_PIPELINE_STATES.SUPERSEDED,
  ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED,
]);

/**
 * Map legacy certification statuses onto the global pipeline state model.
 */
export function mapLegacyCertificationToPipelineState(status) {
  const s = String(status || "").toUpperCase();
  if (s === "CERTIFIED" || s === "CERTIFIED_WITH_DISCLOSURES") {
    return ADP_PERIOD_PIPELINE_STATES.CERTIFIED;
  }
  if (s === "QA_FAILED" || s === "NOT_CERTIFIED") {
    return ADP_PERIOD_PIPELINE_STATES.QA_FAILED;
  }
  if (s === "QA_REVIEW_REQUIRED" || s === "REVIEW" || s === "QA_REVIEW") {
    return ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED;
  }
  if (s === "QA_PASSED") return ADP_PERIOD_PIPELINE_STATES.QA_PASSED;
  if (s === "SUPERSEDED") return ADP_PERIOD_PIPELINE_STATES.SUPERSEDED;
  if (s === "LEGACY_UNCERTIFIED") return ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED;
  if (s === "RUNNING") return ADP_PERIOD_PIPELINE_STATES.RUNNING;
  if (s === "DRAFT" || !s) return ADP_PERIOD_PIPELINE_STATES.DRAFT;
  return ADP_PERIOD_PIPELINE_STATES.DRAFT;
}

export function isCustomerOfficialAllowed(status) {
  const mapped = mapLegacyCertificationToPipelineState(status);
  return ADP_CUSTOMER_OFFICIAL_ALLOWED_STATES.includes(mapped);
}

export function isAdminPreviewAllowed(status) {
  const mapped = mapLegacyCertificationToPipelineState(status);
  return ADP_ADMIN_PREVIEW_ALLOWED_STATES.includes(mapped);
}
