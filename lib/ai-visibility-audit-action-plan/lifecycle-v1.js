/**
 * AI Visibility Audit & Action Plan — explicit lifecycle + transition rules.
 * Customer-facing publication is never automatic from PDF success alone.
 *
 * Formal gates (Phase 8):
 * - AUDIT_VALIDATED — internal research/evidence/PDF quality certification
 * - FOUNDER_APPROVED_FOR_CUSTOMER — explicit founder authorization to distribute
 * These remain separate from CUSTOMER_READY / PUBLISHED.
 */

export const AVA_LIFECYCLE_STATUSES = Object.freeze([
  "NOT_STARTED",
  "BASELINE_READY",
  "RESEARCH_IN_PROGRESS",
  "EVIDENCE_REVIEW",
  "ACTION_PLAN_STAGED",
  "PDF_STAGED",
  "VALIDATION_REQUIRED",
  "AUDIT_VALIDATED",
  "FOUNDER_REVIEWED_STAGED",
  "FOUNDER_APPROVED",
  "FOUNDER_APPROVED_FOR_CUSTOMER",
  "CUSTOMER_READY",
  "PUBLISHED",
  "FAILED",
  "BLOCKED",
]);

/**
 * Evidence-pipeline substates (meta.evidencePipelineStatus).
 * Kept separate from founder/customer publication gates.
 */
export const AVA_EVIDENCE_PIPELINE_STATUSES = Object.freeze([
  "EVIDENCE_VALIDATION_PENDING",
  "EVIDENCE_REMEDIATION_RUNNING",
  "EVIDENCE_VALIDATED",
  "VALIDATION_REQUIRED",
]);

/** Allowed forward transitions (plus FAILED/BLOCKED from most active states). */
export const AVA_LIFECYCLE_TRANSITIONS = Object.freeze({
  NOT_STARTED: ["BASELINE_READY", "BLOCKED", "FAILED"],
  BASELINE_READY: ["RESEARCH_IN_PROGRESS", "BLOCKED", "FAILED"],
  RESEARCH_IN_PROGRESS: ["EVIDENCE_REVIEW", "BLOCKED", "FAILED"],
  EVIDENCE_REVIEW: ["ACTION_PLAN_STAGED", "RESEARCH_IN_PROGRESS", "BLOCKED", "FAILED"],
  ACTION_PLAN_STAGED: ["PDF_STAGED", "EVIDENCE_REVIEW", "BLOCKED", "FAILED"],
  PDF_STAGED: [
    "VALIDATION_REQUIRED",
    "AUDIT_VALIDATED",
    "ACTION_PLAN_STAGED",
    "BLOCKED",
    "FAILED",
  ],
  VALIDATION_REQUIRED: [
    "AUDIT_VALIDATED",
    "FOUNDER_REVIEWED_STAGED",
    "PDF_STAGED",
    "BLOCKED",
    "FAILED",
  ],
  AUDIT_VALIDATED: [
    "FOUNDER_APPROVED_FOR_CUSTOMER",
    "FOUNDER_REVIEWED_STAGED",
    "FOUNDER_APPROVED",
    "VALIDATION_REQUIRED",
    "BLOCKED",
  ],
  FOUNDER_REVIEWED_STAGED: [
    "FOUNDER_APPROVED",
    "FOUNDER_APPROVED_FOR_CUSTOMER",
    "AUDIT_VALIDATED",
    "VALIDATION_REQUIRED",
    "BLOCKED",
  ],
  FOUNDER_APPROVED: [
    "FOUNDER_APPROVED_FOR_CUSTOMER",
    "CUSTOMER_READY",
    "FOUNDER_REVIEWED_STAGED",
    "BLOCKED",
  ],
  FOUNDER_APPROVED_FOR_CUSTOMER: ["CUSTOMER_READY", "AUDIT_VALIDATED", "BLOCKED"],
  CUSTOMER_READY: ["PUBLISHED", "FOUNDER_APPROVED_FOR_CUSTOMER", "BLOCKED"],
  PUBLISHED: ["BLOCKED"],
  FAILED: ["NOT_STARTED", "BASELINE_READY", "RESEARCH_IN_PROGRESS"],
  BLOCKED: ["NOT_STARTED", "BASELINE_READY", "FAILED"],
});

export function canTransitionAvaLifecycle(from, to) {
  const f = String(from || "");
  const t = String(to || "");
  if (f === t) return true;
  const allowed = AVA_LIFECYCLE_TRANSITIONS[f];
  return Array.isArray(allowed) && allowed.includes(t);
}

export function assertAvaLifecycleTransition(from, to) {
  if (!canTransitionAvaLifecycle(from, to)) {
    throw new Error(`AVA_LIFECYCLE_TRANSITION_DENIED: ${from} -> ${to}`);
  }
}

/**
 * PDF generation / AUDIT_VALIDATED alone must never imply CUSTOMER_READY / PUBLISHED.
 * FOUNDER_APPROVED_FOR_CUSTOMER is required before CUSTOMER_READY.
 */
export function isCustomerFacingPublicationAllowed(status, opts = {}) {
  const s = String(status || "");
  if (s !== "CUSTOMER_READY" && s !== "PUBLISHED") return false;
  if (opts.founderPublicationAuthorized !== true) return false;
  if (opts.customerPublicationAuthorized !== true) return false;
  if (opts.founderApprovedForCustomer !== true) return false;
  return true;
}

/**
 * Gate customer-ready transitions on ADP↔AVA cross-report certification.
 * AUDIT_CERTIFIED alone is insufficient for customer publication.
 */
export function assertAdpAvaCertificationForCustomerReady(cert = {}) {
  const status = String(cert.status || cert.crossReportStatus || "");
  const allowed =
    status === "CERTIFIED_MATCH" &&
    cert.jointPublicationAllowed === true &&
    cert.ok !== false;
  if (!allowed) {
    const err = new Error(
      `ADP_AVA_CERTIFICATION_BLOCKS_CUSTOMER_READY:${status || "MISSING"}`
    );
    err.code = "ADP_AVA_CERTIFICATION_BLOCKS_CUSTOMER_READY";
    err.certification = cert;
    throw err;
  }
  return true;
}

export function isAuditValidated(status) {
  return String(status || "") === "AUDIT_VALIDATED";
}

export function isFounderApprovedForCustomer(status) {
  return (
    String(status || "") === "FOUNDER_APPROVED_FOR_CUSTOMER" ||
    String(status || "") === "CUSTOMER_READY" ||
    String(status || "") === "PUBLISHED"
  );
}
