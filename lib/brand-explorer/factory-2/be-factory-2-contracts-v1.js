/**
 * Brand Explorer Batch Factory 2.0 — contracts & status vocabulary.
 * Contract: BE_FACTORY_2_0_BATCH_V1
 */
export const BE_FACTORY_2_0_BATCH_V1 = "BE_FACTORY_2_0_BATCH_V1";

/**
 * Master queue status vocabulary (founder-facing).
 * Legacy aliases kept for staged docs written before consolidate.
 */
export const FACTORY_2_STATUSES = Object.freeze({
  QUEUED: "QUEUED",
  RESEARCHING: "RESEARCHING",
  /** @deprecated use RESEARCHING — preserved for existing stages */
  RESEARCH_IN_PROGRESS: "RESEARCH_IN_PROGRESS",
  NEEDS_REMEDIATION: "NEEDS_REMEDIATION",
  CERTIFIED_READY_FOR_REVIEW: "CERTIFIED_READY_FOR_REVIEW",
  /** @deprecated use CERTIFIED_READY_FOR_REVIEW */
  READY_FOR_REVIEW: "READY_FOR_REVIEW",
  APPROVED_FOR_WRITEBACK: "APPROVED_FOR_WRITEBACK",
  /** @deprecated use APPROVED_FOR_WRITEBACK */
  APPROVED: "APPROVED",
  PERSISTED_VERIFIED: "PERSISTED_VERIFIED",
  PUBLISHED: "PUBLISHED",
  SKIPPED_PROTECTED: "SKIPPED_PROTECTED",
  BLOCKED: "BLOCKED",
});

/** Normalize any legacy/alias status to the canonical master-queue vocabulary. */
export function normalizeFactory2Status(raw) {
  const s = String(raw || "").trim();
  if (!s) return FACTORY_2_STATUSES.QUEUED;
  if (s === "RESEARCH_IN_PROGRESS") return FACTORY_2_STATUSES.RESEARCHING;
  if (s === "READY_FOR_REVIEW") return FACTORY_2_STATUSES.CERTIFIED_READY_FOR_REVIEW;
  if (s === "APPROVED") return FACTORY_2_STATUSES.APPROVED_FOR_WRITEBACK;
  return s;
}

/** Separate persistence / writeback / publication axes (not collapsed into factoryStatus). */
export const PERSISTENCE_STATES = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  STRUCTURED_STAGED: "STRUCTURED_STAGED",
  STRUCTURED_PERSISTED: "STRUCTURED_PERSISTED",
  WRITEBACK_PENDING_APPROVAL: "WRITEBACK_PENDING_APPROVAL",
  WRITEBACK_AUTHORIZED: "WRITEBACK_AUTHORIZED",
  PRESENTATION_PERSISTED_VERIFIED: "PRESENTATION_PERSISTED_VERIFIED",
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
});

export const PROTECTED_BRAND_IDS = Object.freeze([
  "receQkxgjlezsc1xg", // Curio — verified Presentation; do not reprocess
  "reclkgOzvAcBheUSo", // Ascend — verified Presentation; do not reprocess
]);

export const AUTOGRAPH_BRAND_ID = "recEJCTDj1zrsjPM6";

export const DEFAULT_PAID_RESEARCH_BUDGET_USD = 0;

export const FACTORY_2_STAGING_ROOT_REL = "data/brand-explorer/factory-2-0";
export const FACTORY_2_REPORTS_ROOT_REL = "reports/brand-explorer/factory-2-0";
