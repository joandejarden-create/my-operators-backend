/**
 * Separate technical certification sampling from commercial Full E2E queue completion.
 *
 * CERTIFICATION_SAMPLE — intentionally bounded (default maxCandidates=8) for live/process proof.
 * COMMERCIAL_FULL_E2E — process all eligible candidates via budgeted queue; never claim COMPLETE
 * while eligible unresolved candidates remain; budget exhaustion → PARTIAL_RESEARCH_BUDGET_LIMIT.
 *
 * Does not spend. Does not publish. Does not invent a parallel discovery engine.
 */

export const GDI_COMMERCIAL_DEEP_RESEARCH_OPERATING_MODES_V1 =
  "gdi-commercial-deep-research-operating-modes-v1";

export const DEEP_RESEARCH_OPERATING_MODE = Object.freeze({
  CERTIFICATION_SAMPLE: "CERTIFICATION_SAMPLE",
  COMMERCIAL_FULL_E2E: "COMMERCIAL_FULL_E2E",
});

export const RESEARCH_COMPLETION_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  PARTIAL_RESEARCH_BUDGET_LIMIT: "PARTIAL_RESEARCH_BUDGET_LIMIT",
  PARTIAL_QUEUE_REMAINING: "PARTIAL_QUEUE_REMAINING",
  NOT_STARTED: "NOT_STARTED",
  PROVIDER_AUTHORIZATION_PENDING: "PROVIDER_AUTHORIZATION_PENDING",
});

/** Historical default used by certification / CLI when mode is CERTIFICATION_SAMPLE. */
export const CERTIFICATION_SAMPLE_DEFAULT_MAX_CANDIDATES = 8;

/**
 * Resolve operating mode + deep maxCandidates policy.
 *
 * @param {object} opts
 * @param {string} [opts.operatingMode]
 * @param {number|null} [opts.maxCandidates] explicit override
 * @param {boolean} [opts.certificationSample] legacy alias
 */
export function resolveDeepResearchOperatingMode(opts = {}) {
  const explicit = String(opts.operatingMode || "").trim().toUpperCase();
  let mode = null;
  if (explicit === DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E) {
    mode = DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E;
  } else if (
    explicit === DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE ||
    opts.certificationSample === true
  ) {
    mode = DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE;
  } else if (opts.maxCandidates == null && opts.commercialFullE2e === true) {
    mode = DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E;
  } else {
    // Preserve historical behavior: unspecified maxCandidates defaults to certification sample.
    mode = DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE;
  }

  let maxCandidates;
  if (opts.maxCandidates != null && Number.isFinite(Number(opts.maxCandidates))) {
    maxCandidates = Number(opts.maxCandidates);
  } else if (mode === DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E) {
    // null → selection register uses eligible pool length (budget remains the hard gate).
    maxCandidates = null;
  } else {
    maxCandidates = CERTIFICATION_SAMPLE_DEFAULT_MAX_CANDIDATES;
  }

  return {
    version: GDI_COMMERCIAL_DEEP_RESEARCH_OPERATING_MODES_V1,
    operatingMode: mode,
    maxCandidates,
    isCertificationSample: mode === DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE,
    isCommercialFullE2e: mode === DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E,
    note:
      mode === DEEP_RESEARCH_OPERATING_MODE.CERTIFICATION_SAMPLE
        ? "Bounded sample for technical validation — not comprehensive commercial research."
        : "Commercial queue: all eligible candidates; budget exhaustion is PARTIAL, not COMPLETE.",
  };
}

/**
 * Derive research-completion status for a run.
 */
export function deriveResearchCompletionStatus({
  operatingMode = null,
  eligibleCount = 0,
  deepCompletedCount = 0,
  deferredEligibleCount = 0,
  budgetExhausted = false,
  paidAuthorized = true,
} = {}) {
  if (paidAuthorized !== true) {
    return {
      status: RESEARCH_COMPLETION_STATUS.PROVIDER_AUTHORIZATION_PENDING,
      comprehensiveResearchComplete: false,
      reason: "paid_research_not_authorized",
    };
  }
  if (budgetExhausted && deferredEligibleCount > 0) {
    return {
      status: RESEARCH_COMPLETION_STATUS.PARTIAL_RESEARCH_BUDGET_LIMIT,
      comprehensiveResearchComplete: false,
      reason: "eligible_candidates_remain_budget_exhausted",
      eligibleCount,
      deepCompletedCount,
      deferredEligibleCount,
    };
  }
  if (deferredEligibleCount > 0) {
    return {
      status: RESEARCH_COMPLETION_STATUS.PARTIAL_QUEUE_REMAINING,
      comprehensiveResearchComplete: false,
      reason: "eligible_unresolved_candidates_remain",
      eligibleCount,
      deepCompletedCount,
      deferredEligibleCount,
      operatingMode,
    };
  }
  if (eligibleCount === 0 && deepCompletedCount === 0) {
    return {
      status: RESEARCH_COMPLETION_STATUS.NOT_STARTED,
      comprehensiveResearchComplete: false,
      reason: "no_eligible_candidates",
    };
  }
  return {
    status: RESEARCH_COMPLETION_STATUS.COMPLETE,
    comprehensiveResearchComplete: true,
    reason: "all_eligible_candidates_have_defensible_disposition",
    eligibleCount,
    deepCompletedCount,
    deferredEligibleCount: 0,
    operatingMode,
  };
}

export default {
  GDI_COMMERCIAL_DEEP_RESEARCH_OPERATING_MODES_V1,
  DEEP_RESEARCH_OPERATING_MODE,
  RESEARCH_COMPLETION_STATUS,
  CERTIFICATION_SAMPLE_DEFAULT_MAX_CANDIDATES,
  resolveDeepResearchOperatingMode,
  deriveResearchCompletionStatus,
};
