/**
 * Brand Explorer Batch Factory 2.0 — contracts & status vocabulary.
 * Contract: BE_FACTORY_2_0_BATCH_V1
 */
export const BE_FACTORY_2_0_BATCH_V1 = "BE_FACTORY_2_0_BATCH_V1";

export const FACTORY_2_STATUSES = Object.freeze({
  QUEUED: "QUEUED",
  RESEARCH_IN_PROGRESS: "RESEARCH_IN_PROGRESS",
  READY_FOR_REVIEW: "READY_FOR_REVIEW",
  NEEDS_REMEDIATION: "NEEDS_REMEDIATION",
  APPROVED: "APPROVED",
  PUBLISHED: "PUBLISHED",
  SKIPPED_PROTECTED: "SKIPPED_PROTECTED",
  BLOCKED: "BLOCKED",
});

export const PROTECTED_BRAND_IDS = Object.freeze([
  "receQkxgjlezsc1xg", // Curio — verified Presentation; do not reprocess
  "reclkgOzvAcBheUSo", // Ascend — verified Presentation; do not reprocess
]);

export const AUTOGRAPH_BRAND_ID = "recEJCTDj1zrsjPM6";

export const DEFAULT_PAID_RESEARCH_BUDGET_USD = 0;

export const FACTORY_2_STAGING_ROOT_REL = "data/brand-explorer/factory-2-0";
export const FACTORY_2_REPORTS_ROOT_REL = "reports/brand-explorer/factory-2-0";
