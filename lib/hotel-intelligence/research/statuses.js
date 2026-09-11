/**
 * Packet 2.6C / 2.6C-R2 / 2.6C-R3 — Research request / run statuses.
 * Distinguish PROVIDER_COMPLETED from RESEARCH_COMPLETED (terminal).
 * Do not use ambiguous "PROCESSING". No fake percentages.
 */

export const RESEARCH_STATUSES = Object.freeze([
  "QUEUED",
  "PROVIDER_STARTING",
  "RUNNING",
  "PROVIDER_COMPLETED",
  "RAW_ARTIFACT_SAVED",
  "NORMALIZING",
  "VALIDATING",
  "GENERATING_REPORT",
  "GENERATING_PDF",
  "ARCHIVING",
  "COMPLETED",
  "COMPLETED_WITH_OPEN_QUESTIONS",
  "PARTIAL",
  "SIMULATED",
  "FAILED",
  "CANCELLED",
]);

export const TERMINAL_STATUSES = Object.freeze([
  "COMPLETED",
  "COMPLETED_WITH_OPEN_QUESTIONS",
  "PARTIAL",
  "SIMULATED",
  "FAILED",
  "CANCELLED",
]);

export const ACTIVE_STATUSES = Object.freeze([
  "QUEUED",
  "PROVIDER_STARTING",
  "RUNNING",
  "PROVIDER_COMPLETED",
  "RAW_ARTIFACT_SAVED",
  "NORMALIZING",
  "VALIDATING",
  "GENERATING_REPORT",
  "GENERATING_PDF",
  "ARCHIVING",
]);

/** Customer-facing progress labels (no fake % complete). */
export const CUSTOMER_STAGE_LABELS = Object.freeze({
  QUEUED: "Starting Research",
  PROVIDER_STARTING: "Starting Research",
  RUNNING: "Discovering Sources",
  PROVIDER_COMPLETED: "Research Complete — Report Processing",
  RAW_ARTIFACT_SAVED: "Research Complete — Report Processing",
  NORMALIZING: "Reviewing Evidence",
  VALIDATING: "Resolving People & Organizations",
  GENERATING_REPORT: "Preparing Report",
  GENERATING_PDF: "Generating PDF",
  ARCHIVING: "Publishing Report",
  COMPLETED: "Completed",
  COMPLETED_WITH_OPEN_QUESTIONS: "Completed With Open Questions",
  PARTIAL: "Partial",
  SIMULATED: "Simulated (test only)",
  FAILED: "Research could not be completed",
  CANCELLED: "Cancelled",
});

export const SIMULATION_STAGE_ORDER = Object.freeze([
  "QUEUED",
  "RUNNING",
  "NORMALIZING",
  "VALIDATING",
  "GENERATING_REPORT",
  "SIMULATED",
]);

export const LIVE_STAGE_ORDER = Object.freeze([
  "QUEUED",
  "PROVIDER_STARTING",
  "RUNNING",
  "NORMALIZING",
  "VALIDATING",
  "GENERATING_REPORT",
  "GENERATING_PDF",
  "COMPLETED",
]);

export function isTerminalStatus(status) {
  return TERMINAL_STATUSES.includes(String(status || "").toUpperCase());
}

export function isActiveStatus(status) {
  return ACTIVE_STATUSES.includes(String(status || "").toUpperCase());
}

export function customerStageLabel(status) {
  const key = String(status || "").toUpperCase();
  return CUSTOMER_STAGE_LABELS[key] || "Research In Progress";
}

export function nextSimulationStatus(status) {
  const key = String(status || "QUEUED").toUpperCase();
  const idx = SIMULATION_STAGE_ORDER.indexOf(key);
  if (idx < 0) return "QUEUED";
  if (idx >= SIMULATION_STAGE_ORDER.length - 1) return "SIMULATED";
  return SIMULATION_STAGE_ORDER[idx + 1];
}
