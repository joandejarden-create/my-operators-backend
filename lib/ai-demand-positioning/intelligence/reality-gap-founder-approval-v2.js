/**
 * Founder approval gate for RG_WEIGHTED_PARTIAL_V2.
 *
 * Historical customer formula RG_BINARY_GAP_V1 remains immutable for published snapshots.
 * Activation applies to newly generated staged reports only after threshold validation passes.
 */

export const FOUNDER_RG_METHODOLOGY_V2_DECISION_ID =
  "FOUNDER_RG_WEIGHTED_PARTIAL_V2_CONDITIONAL_APPROVAL_20261021";

/** Immutable historical methodology — never overwrite published V1 scores. */
export const HISTORICAL_IMMUTABLE_METHODOLOGY_ID = "RG_BINARY_GAP_V1";

export const CONDITIONAL_APPROVED_METHODOLOGY_ID = "RG_WEIGHTED_PARTIAL_V2";

/**
 * Conditional founder approval (chat directive 2026-10-21).
 * Activation still requires thresholdValidationPassed === true at runtime.
 */
export const FOUNDER_RG_WEIGHTED_PARTIAL_V2_APPROVAL = Object.freeze({
  decisionId: FOUNDER_RG_METHODOLOGY_V2_DECISION_ID,
  status: "CONDITIONALLY_APPROVED",
  methodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
  historicalImmutableMethodologyId: HISTORICAL_IMMUTABLE_METHODOLOGY_ID,
  approvedBy: "founder",
  approvedAt: "2026-10-21T13:22:00.000Z",
  approvalChannel: "cursor_chat_directive",
  conditions: Object.freeze([
    "Validate 15–39% partial-recognition against original Core observations",
    "Confirm response-only recognition (no prompt leakage)",
    "Consistent subject-mentioned denominators",
    "Preserve RG_BINARY_GAP_V1 historical",
    "Staged-only activation; published snapshots untouched",
    "No paid monitoring / no customer publication / no production deploy",
  ]),
  activationScope: "STAGED_REPORTS_ONLY_PENDING_PUBLICATION_AUTHORIZATION",
  formula:
    "weightedGap = (missedCount + 0.5 * partialCount) / testedEligibleCount * 100",
  thresholds: Object.freeze({
    recognizedMinPct: 40,
    partialMinPct: 15,
    partialMaxPctExclusive: 40,
    missedMaxPctExclusive: 15,
    grain: "SUBJECT_MENTIONED_CORPUS",
  }),
});

/**
 * @param {{ thresholdValidationPassed?: boolean }} opts
 * @returns {string} active staged customer methodology id
 */
export function resolveActiveStagedRealityGapMethodologyId(opts = {}) {
  if (opts.thresholdValidationPassed === true) {
    return CONDITIONAL_APPROVED_METHODOLOGY_ID;
  }
  return HISTORICAL_IMMUTABLE_METHODOLOGY_ID;
}
