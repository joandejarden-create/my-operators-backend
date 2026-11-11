/**
 * Shared Full E2E execution states — GDI and ADP.
 * Use existing product-specific statuses where they already exist; map via helpers.
 */

export const OS_VERSION = "dealality-full-e2e-os-v1";

export const EXECUTION_STATE = Object.freeze({
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  WAITING_PROVIDER: "WAITING_PROVIDER",
  WAITING_AUTHORIZATION: "WAITING_AUTHORIZATION",
  RETRYABLE: "RETRYABLE",
  BLOCKED_TECHNICAL: "BLOCKED_TECHNICAL",
  BLOCKED_EVIDENCE: "BLOCKED_EVIDENCE",
  COMPLETE: "COMPLETE",
  FAILED: "FAILED",
});

export const RUN_MODE = Object.freeze({
  NEW: "new",
  REFRESH: "refresh",
  VALIDATE: "validate",
  REPAIR: "repair",
  STATUS: "status",
  DRY_RUN: "dry_run",
});

export const PRODUCT = Object.freeze({
  GDI: "GDI",
  ADP: "ADP",
});

export const PUBLICATION_LIFECYCLE = Object.freeze({
  RESEARCH_STAGED: "RESEARCH_STAGED",
  QUALITY_APPROVED: "QUALITY_APPROVED",
  PUBLICATION_APPROVED: "PUBLICATION_APPROVED",
  PUBLISHED: "PUBLISHED",
  DELIVERY_VERIFIED: "DELIVERY_VERIFIED",
});

export const OVERALL_RUN_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  RUNNING: "RUNNING",
  RESEARCH_COMPLETE: "RESEARCH_COMPLETE",
  REPORT_READY: "REPORT_READY",
  WAITING_PUBLICATION_AUTHORIZATION: "WAITING_PUBLICATION_AUTHORIZATION",
  PUBLISHED: "PUBLISHED",
  DELIVERY_VERIFIED: "DELIVERY_VERIFIED",
  FULL_E2E_COMPLETE: "FULL_E2E_COMPLETE",
  PARTIAL: "PARTIAL",
  BLOCKED: "BLOCKED",
  FAILED: "FAILED",
});

/** Map GDI evaluator statuses onto OS overall statuses. */
export function mapGdiEvaluatorStatus(status) {
  const s = String(status || "").toUpperCase();
  if (s === "COMPLETE") return OVERALL_RUN_STATUS.FULL_E2E_COMPLETE;
  if (s === "PARTIAL") return OVERALL_RUN_STATUS.PARTIAL;
  if (s === "BLOCKED") return OVERALL_RUN_STATUS.BLOCKED;
  if (s === "FAILED") return OVERALL_RUN_STATUS.FAILED;
  if (s === "RUNNING") return OVERALL_RUN_STATUS.RUNNING;
  if (s === "NOT_STARTED") return OVERALL_RUN_STATUS.NOT_STARTED;
  return OVERALL_RUN_STATUS.PARTIAL;
}

export function isTerminalExecutionState(state) {
  return (
    state === EXECUTION_STATE.COMPLETE ||
    state === EXECUTION_STATE.FAILED ||
    state === EXECUTION_STATE.BLOCKED_TECHNICAL ||
    state === EXECUTION_STATE.BLOCKED_EVIDENCE
  );
}

export function isWaitingState(state) {
  return (
    state === EXECUTION_STATE.WAITING_PROVIDER ||
    state === EXECUTION_STATE.WAITING_AUTHORIZATION
  );
}
