/**
 * Governed monitoring run purpose — distinguishes validation from baseline/recurring.
 * Phase 3B.2: validation data must not pollute full baseline metrics.
 */

export const MONITORING_RUN_PURPOSE = Object.freeze({
  VALIDATION: "validation",
  BASELINE: "baseline",
  RECURRING: "recurring",
});

export const MONITORING_RUN_PURPOSE_VERSION = "ai_visibility_monitoring_run_purpose_v1";

/**
 * @param {object|null|undefined} row
 * @returns {string|null}
 */
export function resolveMonitoringRunPurpose(row) {
  if (!row) return null;
  const p = row.monitoringRunPurpose || row.runPurpose || row.cohort?.monitoringRunPurpose;
  if (!p) return null;
  return String(p).trim().toLowerCase() || null;
}

/**
 * True when batch/summary represents a full baseline (eligible for measured provider UI).
 */
export function isBaselineMonitoringRun(row) {
  const purpose = resolveMonitoringRunPurpose(row);
  if (purpose === MONITORING_RUN_PURPOSE.VALIDATION) return false;
  if (purpose === MONITORING_RUN_PURPOSE.BASELINE) return true;
  // Legacy OpenAI Wave-1 rows without explicit purpose but with baseline series id
  const series = String(row?.baselineSeriesId || row?.cohort?.baselineSeriesId || "");
  if (/wave1.*baseline|baseline.*wave1|aiv_wave1_openai/i.test(series)) return true;
  if (String(row?.batchId || "").includes("aiv_wave1_openai_showcase")) return true;
  return purpose == null && String(row?.provider || "").toLowerCase() === "openai";
}

/**
 * @param {object|null|undefined} row
 */
export function isValidationMonitoringRun(row) {
  return resolveMonitoringRunPurpose(row) === MONITORING_RUN_PURPOSE.VALIDATION;
}

export function monitoringRunTypeSupported() {
  return true;
}
