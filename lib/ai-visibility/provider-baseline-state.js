/**
 * Provider baseline completeness states (Phase 3B.3).
 */

export const BASELINE_COMPLETENESS = Object.freeze({
  NONE: "NONE",
  VALIDATION_ONLY: "VALIDATION_ONLY",
  PARTIAL_BASELINE: "PARTIAL_BASELINE",
  FULL_BASELINE: "FULL_BASELINE",
  BLOCKED: "BLOCKED",
});

export const PROVIDER_BASELINE_SERIES = Object.freeze({
  openai: "aiv_wave1_openai_peer_v2_showcase_prompts_v1",
  gemini: "aiv_wave1_gemini_peer_v2_showcase_prompts_v1",
  perplexity: "aiv_wave1_perplexity_peer_v2_showcase_prompts_v1",
  claude: "aiv_wave1_claude_peer_v2_showcase_prompts_v1",
});

export const PROVIDER_BASELINE_HARD_CAPS = Object.freeze({
  gemini: 50,
  perplexity: 15,
  claude: 50,
});

/**
 * @param {object|null} summary — batch summary row
 */
export function resolveBaselineCompleteness(summary) {
  if (!summary) return BASELINE_COMPLETENESS.NONE;
  if (summary.status === "NOT_EXECUTED_MISSING_CREDENTIAL" || summary.status === "failed_auth") {
    return BASELINE_COMPLETENESS.BLOCKED;
  }
  const purpose = String(summary.monitoringRunPurpose || summary.runPurpose || "").toLowerCase();
  if (purpose === "validation") return BASELINE_COMPLETENESS.VALIDATION_ONLY;
  const provider = String(summary.provider?.name || summary.provider || "").toLowerCase();
  const succeeded = Number(
    summary.succeeded ??
      summary.SUCCEEDED ??
      summary.logical?.succeeded ??
      summary.successfulRuns ??
      0
  );
  const planned = Number(summary.plannedRuns ?? summary.PLANNED ?? summary.logicalCalls ?? 84);
  if (purpose === "baseline" || summary.baselineSeriesId) {
    if (succeeded >= 84 && summary.status === "completed") {
      return BASELINE_COMPLETENESS.FULL_BASELINE;
    }
    if (succeeded > 0) return BASELINE_COMPLETENESS.PARTIAL_BASELINE;
  }
  // Legacy OpenAI wave1
  if (provider === "openai" && String(summary.batchId || summary.waveId || "").includes("aiv_wave1_openai")) {
    if (succeeded >= 84 || summary.status === "completed") {
      return BASELINE_COMPLETENESS.FULL_BASELINE;
    }
  }
  if (summary.baselineSeriesId && succeeded >= 84 && summary.status === "completed") {
    return BASELINE_COMPLETENESS.FULL_BASELINE;
  }
  return BASELINE_COMPLETENESS.NONE;
}

export function isFullBaselineSummary(summary) {
  return resolveBaselineCompleteness(summary) === BASELINE_COMPLETENESS.FULL_BASELINE;
}
