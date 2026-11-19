/**
 * Progressive research depth contract V1.
 *
 * Defines STAGE 0–3 continuation rules for Full E2E / ORS.
 * Does not replace the opportunity research supervisor.
 * Does not invent spend or force additional Exa loops.
 *
 * Research priority ≠ commercial qualification.
 * Credible inferred demand may remain actionable without confirmed room nights
 * or a second named contact.
 */

export const GDI_PROGRESSIVE_RESEARCH_DEPTH_CONTRACT_V1 =
  "gdi-progressive-research-depth-contract-v1";

export const RESEARCH_DEPTH_STAGE = Object.freeze({
  STAGE_0_DISCOVERY_SCREEN: "STAGE_0_DISCOVERY_SCREEN",
  STAGE_1_TARGETED_DEMAND_EVIDENCE: "STAGE_1_TARGETED_DEMAND_EVIDENCE",
  STAGE_2_BUYER_COMMERCIAL_COMPETITIVE: "STAGE_2_BUYER_COMMERCIAL_COMPETITIVE",
  STAGE_3_SELECTIVE_DEEP_UNRESOLVED: "STAGE_3_SELECTIVE_DEEP_UNRESOLVED",
});

export const RESEARCH_STOP_CLASS = Object.freeze({
  ACTIONABLE: "ACTIONABLE",
  UNSUITABLE: "UNSUITABLE",
  TOO_EARLY_MONITOR: "TOO_EARLY_MONITOR",
  ALREADY_SUFFICIENTLY_KNOWN: "ALREADY_SUFFICIENTLY_KNOWN",
  PERMITTED_RESEARCH_EXHAUSTED: "PERMITTED_RESEARCH_EXHAUSTED",
  BUDGET_OR_AUTH_BLOCKED: "BUDGET_OR_AUTH_BLOCKED",
  CONTINUE: "CONTINUE",
});

/**
 * Build a continuation packet. Every paid/offline continuation should carry these fields.
 */
export function buildResearchContinuationPacket({
  opportunityId = null,
  stage = RESEARCH_DEPTH_STAGE.STAGE_0_DISCOVERY_SCREEN,
  unresolvedQuestion = null,
  evidenceAlreadyCollected = [],
  expectedCommercialBenefit = null,
  proposedNextAction = null,
  estimatedCostUsd = null,
  remainingAuthorizedBudgetUsd = null,
  stoppingCondition = null,
  stopClass = RESEARCH_STOP_CLASS.CONTINUE,
} = {}) {
  const question = String(unresolvedQuestion || "").trim();
  const benefit = String(expectedCommercialBenefit || "").trim();
  const action = String(proposedNextAction || "").trim();
  const stop = String(stoppingCondition || "").trim();
  const continueOk =
    stopClass === RESEARCH_STOP_CLASS.CONTINUE &&
    Boolean(question) &&
    Boolean(benefit) &&
    Boolean(action) &&
    Boolean(stop);

  return {
    version: GDI_PROGRESSIVE_RESEARCH_DEPTH_CONTRACT_V1,
    opportunityId,
    stage,
    unresolvedQuestion: question || null,
    evidenceAlreadyCollected: Array.isArray(evidenceAlreadyCollected)
      ? evidenceAlreadyCollected
      : [],
    expectedCommercialBenefit: benefit || null,
    proposedNextAction: action || null,
    estimatedCostUsd:
      estimatedCostUsd == null || !Number.isFinite(Number(estimatedCostUsd))
        ? null
        : Number(estimatedCostUsd),
    remainingAuthorizedBudgetUsd:
      remainingAuthorizedBudgetUsd == null ||
      !Number.isFinite(Number(remainingAuthorizedBudgetUsd))
        ? null
        : Number(remainingAuthorizedBudgetUsd),
    stoppingCondition: stop || null,
    stopClass,
    continuationAuthorized: continueOk,
    notes: [
      "Never continue solely to lengthen a drawer or hit an arbitrary search count.",
      "Confirmed room nights / second contact are not mandatory for every actionable opportunity.",
      "More research is not assumed to produce better intelligence.",
    ],
  };
}

/**
 * Map existing ORS / restore outcomes onto stop classes without changing supervisor math.
 */
export function classifyResearchStopFromExistingSignals(signals = {}) {
  if (signals.budgetBlocked === true || signals.authBlocked === true) {
    return RESEARCH_STOP_CLASS.BUDGET_OR_AUTH_BLOCKED;
  }
  if (signals.unsuitable === true || signals.exclude === true) {
    return RESEARCH_STOP_CLASS.UNSUITABLE;
  }
  if (signals.tooEarly === true || signals.monitorOnly === true) {
    return RESEARCH_STOP_CLASS.TOO_EARLY_MONITOR;
  }
  if (signals.actionable === true || signals.salesReady === true) {
    return RESEARCH_STOP_CLASS.ACTIONABLE;
  }
  if (signals.alreadyKnown === true || signals.duplicateNoNewEvidence === true) {
    return RESEARCH_STOP_CLASS.ALREADY_SUFFICIENTLY_KNOWN;
  }
  if (signals.researchExhausted === true || signals.permittedExhausted === true) {
    return RESEARCH_STOP_CLASS.PERMITTED_RESEARCH_EXHAUSTED;
  }
  if (signals.shouldContinue === true) {
    return RESEARCH_STOP_CLASS.CONTINUE;
  }
  return RESEARCH_STOP_CLASS.PERMITTED_RESEARCH_EXHAUSTED;
}

export default {
  GDI_PROGRESSIVE_RESEARCH_DEPTH_CONTRACT_V1,
  RESEARCH_DEPTH_STAGE,
  RESEARCH_STOP_CLASS,
  buildResearchContinuationPacket,
  classifyResearchStopFromExistingSignals,
};
