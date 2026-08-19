/**
 * Operator owner-decision competitive gap.
 * Built only from validated Presence + commercial comparability.
 * Every gap is an absence; not every absence is a gap.
 * CLIENT_PROMOTED stays 0 until production gates pass.
 */

import { eligibilityFor, ELIGIBILITY } from "./eligibility.js";
import { isPrimaryMonitoredOperator, getOperatorById, OPERATOR_AI_UNIVERSE } from "./universe.js";
import {
  ARBOR_LODGING_ID,
  COMMERCIAL_RELATION,
  classifyPresentOperators,
} from "./comparability.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { getOperatorScenarioProductionPolicy } from "./scenario-production-policy.js";
import { getComparabilityTruth } from "./comparability-truth.js";

export const OPERATOR_GAP_VERSION = "operator_owner_decision_gap_v1";
export const OPERATOR_COMPETITIVE_GAP_VERSION = "operator_competitive_gap_v1";

export const RAW_GAP_TYPES = Object.freeze({
  PEER_PRESENT_OPERATOR_MISSING: "PEER_PRESENT_OPERATOR_MISSING",
  PERSISTENT_SCENARIO_GAP: "PERSISTENT_SCENARIO_GAP",
});

export const GAP_INTERPRETATION = Object.freeze({
  TRUE_COMPETITIVE_GAP: "TRUE_COMPETITIVE_GAP",
  EXPECTED_POSITIONING_DIFFERENCE: "EXPECTED_POSITIONING_DIFFERENCE",
  SCENARIO_OUT_OF_SCOPE: "SCENARIO_OUT_OF_SCOPE",
  INSUFFICIENT_CONTEXT: "INSUFFICIENT_CONTEXT",
  REQUIRES_REVIEW: "REQUIRES_REVIEW",
  NOT_A_GAP: "NOT_A_GAP",
});

export const GAP_GOLD_LABEL = Object.freeze({
  TRUE_COMPETITIVE_GAP: "TRUE_COMPETITIVE_GAP",
  NOT_A_GAP: "NOT_A_GAP",
  EXPECTED_POSITIONING_DIFFERENCE: "EXPECTED_POSITIONING_DIFFERENCE",
  OUT_OF_SCOPE: "OUT_OF_SCOPE",
  INSUFFICIENT_CONTEXT: "INSUFFICIENT_CONTEXT",
  REQUIRES_REVIEW: "REQUIRES_REVIEW",
});

export const GAP_DISPOSITION = Object.freeze({
  ACTION_REQUIRED: "ACTION_REQUIRED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  NO_ACTION_EXPECTED_POSITIONING: "NO_ACTION_EXPECTED_POSITIONING",
  MONITOR_ONLY: "MONITOR_ONLY",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

function emptyGap(interpretation, disposition, extra = {}) {
  return {
    version: OPERATOR_COMPETITIVE_GAP_VERSION,
    rawType: null,
    interpretation,
    disposition,
    executiveActionPill: false,
    clientPromoted: false,
    commerciallyRelevant: false,
    ...extra,
  };
}

/**
 * Governed Competitive Gap interpretation.
 * CORE comparable alternatives required for TRUE_COMPETITIVE_GAP.
 */
export function interpretOperatorCompetitiveGap(input = {}) {
  const {
    operatorId,
    scenarioId,
    operatorPresent,
    presentPeerOperatorIds = [],
    observationCount = 0,
    comparableObservation = true,
  } = input;

  if (!isPrimaryMonitoredOperator(operatorId)) {
    return emptyGap(GAP_INTERPRETATION.INSUFFICIENT_CONTEXT, GAP_DISPOSITION.INSUFFICIENT_EVIDENCE, {
      reason: "not_primary_monitored_operator",
    });
  }

  if (comparableObservation === false) {
    return emptyGap(GAP_INTERPRETATION.INSUFFICIENT_CONTEXT, GAP_DISPOSITION.INSUFFICIENT_EVIDENCE, {
      reason: "non_comparable_or_failed_provider",
    });
  }

  const elig = eligibilityFor(operatorId, scenarioId);
  const classified = classifyPresentOperators(operatorId, presentPeerOperatorIds, scenarioId);
  const core = classified.byRelation[COMMERCIAL_RELATION.CORE_COMPARABLE];
  const secondary = classified.byRelation[COMMERCIAL_RELATION.SECONDARY_CONTEXT];
  const conditional = classified.byRelation[COMMERCIAL_RELATION.CONDITIONAL];
  const nonComparable = classified.byRelation[COMMERCIAL_RELATION.NON_COMPARABLE];
  const policy = getOperatorScenarioProductionPolicy(scenarioId);
  const subject = getOperatorById(operatorId);

  const extra = {
    eligibility: elig.status,
    operatorLens: subject?.operatorLens,
    coreComparablePresent: core,
    secondaryContextPresent: secondary,
    conditionalPresent: conditional,
    commerciallyRelevant: core.length > 0,
    scenarioGapTier: policy.competitiveGapTier,
  };

  if (elig.status === ELIGIBILITY.OUT_OF_SCOPE) {
    return {
      ...emptyGap(
        GAP_INTERPRETATION.SCENARIO_OUT_OF_SCOPE,
        GAP_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING,
        extra
      ),
      goldLabel: GAP_GOLD_LABEL.OUT_OF_SCOPE,
      reason: "subject_out_of_scope",
    };
  }

  if (operatorId === ARBOR_LODGING_ID) {
    return emptyGap(GAP_INTERPRETATION.INSUFFICIENT_CONTEXT, GAP_DISPOSITION.INSUFFICIENT_EVIDENCE, {
      ...extra,
      goldLabel: GAP_GOLD_LABEL.INSUFFICIENT_CONTEXT,
      reason: "arbor_insufficient_operator_specific_evidence",
    });
  }

  if (operatorPresent) {
    return emptyGap(GAP_INTERPRETATION.NOT_A_GAP, GAP_DISPOSITION.INSUFFICIENT_EVIDENCE, {
      ...extra,
      commerciallyRelevant: false,
      reason: "subject_present_not_a_gap",
      goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    });
  }

  if (policy.competitiveGapTier === "RESEARCH_ONLY" || policy.competitiveGapTier === "DETAIL_ONLY") {
    return emptyGap(GAP_INTERPRETATION.REQUIRES_REVIEW, GAP_DISPOSITION.MONITOR_ONLY, {
      ...extra,
      goldLabel: GAP_GOLD_LABEL.REQUIRES_REVIEW,
      reason: "scenario_not_core_for_competitive_gap",
    });
  }

  if (!core.length && !secondary.length && !conditional.length) {
    return emptyGap(GAP_INTERPRETATION.NOT_A_GAP, GAP_DISPOSITION.INSUFFICIENT_EVIDENCE, {
      ...extra,
      goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
      reason: nonComparable.length
        ? "only_non_comparable_operators_present"
        : "absence_without_relevant_alternative",
    });
  }

  if (!core.length && secondary.length && !conditional.length) {
    return emptyGap(
      GAP_INTERPRETATION.EXPECTED_POSITIONING_DIFFERENCE,
      GAP_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING,
      {
        ...extra,
        goldLabel: GAP_GOLD_LABEL.EXPECTED_POSITIONING_DIFFERENCE,
        reason: "only_secondary_context_alternatives",
      }
    );
  }

  if (!core.length) {
    return emptyGap(GAP_INTERPRETATION.REQUIRES_REVIEW, GAP_DISPOSITION.REVIEW_REQUIRED, {
      ...extra,
      goldLabel: GAP_GOLD_LABEL.REQUIRES_REVIEW,
      reason: "only_conditional_or_mixed_non_core_alternatives",
    });
  }

  if (policy.competitiveGapTier === "DETAIL_ONLY" || policy.competitiveGapTier === "CONDITIONAL") {
    return emptyGap(GAP_INTERPRETATION.REQUIRES_REVIEW, GAP_DISPOSITION.MONITOR_ONLY, {
      ...extra,
      commerciallyRelevant: core.length > 0,
      goldLabel: GAP_GOLD_LABEL.REQUIRES_REVIEW,
      reason:
        policy.competitiveGapTier === "CONDITIONAL"
          ? "scenario_conditional_for_competitive_gap"
          : "scenario_not_core_for_competitive_gap",
    });
  }

  const rawType =
    observationCount >= 3
      ? RAW_GAP_TYPES.PERSISTENT_SCENARIO_GAP
      : RAW_GAP_TYPES.PEER_PRESENT_OPERATOR_MISSING;

  return {
    version: OPERATOR_COMPETITIVE_GAP_VERSION,
    rawType,
    interpretation: GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP,
    disposition: GAP_DISPOSITION.REVIEW_REQUIRED,
    eligibility: elig.status,
    executiveActionPill: false,
    clientPromoted: false,
    commerciallyRelevant: true,
    goldLabel: GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP,
    reason: "subject_absent_core_comparable_present",
    manualReview: false,
    ...extra,
  };
}

/**
 * Legacy wrapper used by foundation + presence-wave diagnostics.
 * Now routes through commercial comparability (Brand peers are not used).
 */
export function interpretOperatorGap(input = {}) {
  return interpretOperatorCompetitiveGap(input);
}

export function isClientPromotableOperatorGap(gap = {}, context = {}) {
  const policy = getOperatorScenarioProductionPolicy(context.scenarioId);
  const truth = getComparabilityTruth(context.operatorId);
  const subjectEvidenceOk =
    truth?.competitiveEvidenceState === "VALIDATED" &&
    context.operatorId !== ARBOR_LODGING_ID;
  return Boolean(
    gap.interpretation === GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP &&
      gap.eligibility === ELIGIBILITY.ELIGIBLE &&
      policy.competitiveGapTier === "CORE" &&
      policy.customerEligible === "YES" &&
      subjectEvidenceOk &&
      (gap.coreComparablePresent || []).length > 0 &&
      context.comparableObservation !== false &&
      gap.manualReview !== true &&
      context.operatorPresent === false
  );
}

function customerNames(ids) {
  return [...new Set(ids || [])]
    .map((id) => getOperatorById(id)?.canonicalName)
    .filter(Boolean);
}

/**
 * Extract diagnostic gap candidates at operator × scenario × providerScope.
 * Not per-response. Failed providers excluded.
 */
export function extractOperatorCompetitiveGapCandidates(extractions = []) {
  const providers = [...new Set((extractions || []).map((e) => e.provider).filter(Boolean))];
  const candidates = [];

  function consider(operatorId, scenarioId, providerScope, subset) {
    if (!subset.length) return;
    const presentIds = [...new Set(subset.flatMap((e) => e.presentOperatorIds || []))];
    const operatorPresent = presentIds.includes(operatorId);
    const gap = interpretOperatorCompetitiveGap({
      operatorId,
      scenarioId,
      operatorPresent,
      presentPeerOperatorIds: presentIds.filter((id) => id !== operatorId),
      observationCount: subset.length,
      comparableObservation: true,
    });
    const clientPromoted = isClientPromotableOperatorGap(gap, {
      operatorId,
      scenarioId,
      operatorPresent,
      comparableObservation: true,
    });
    const row = {
      operatorId,
      canonicalName: getOperatorById(operatorId)?.canonicalName,
      scenarioId,
      providerScope,
      subjectPresence: operatorPresent ? "PRESENT" : "ABSENT",
      evidenceCount: subset.length,
      comparableProviderCount: [...new Set(subset.map((e) => e.provider))].length,
      missingCount: operatorPresent ? 0 : 1,
      relevantOperatorsPresent: gap.coreComparablePresent || [],
      relevantOperatorsPresentCustomer: customerNames(gap.coreComparablePresent || []),
      observedCompetitorsCustomer: [],
      gapInterpretation: gap.interpretation,
      goldLabel: gap.goldLabel,
      reason: gap.reason,
      clientPromoted,
      diagnosticOnly: !clientPromoted,
    };
    if (operatorPresent) return;
    if (
      gap.interpretation === GAP_INTERPRETATION.NOT_A_GAP ||
      (gap.interpretation === GAP_INTERPRETATION.INSUFFICIENT_CONTEXT &&
        (gap.reason === "absence_without_relevant_alternative" ||
          gap.reason === "only_non_comparable_operators_present"))
    ) {
      candidates.push({ ...row, candidateKind: "ABSENCE_NOT_A_GAP" });
      return;
    }
    if (gap.interpretation === GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP) {
      candidates.push({ ...row, candidateKind: "GAP_CANDIDATE" });
      return;
    }
    if (
      [
        GAP_INTERPRETATION.EXPECTED_POSITIONING_DIFFERENCE,
        GAP_INTERPRETATION.SCENARIO_OUT_OF_SCOPE,
        GAP_INTERPRETATION.REQUIRES_REVIEW,
        GAP_INTERPRETATION.INSUFFICIENT_CONTEXT,
      ].includes(gap.interpretation)
    ) {
      candidates.push({ ...row, candidateKind: "INTERPRETED_NON_CLIENT" });
    }
  }

  for (const op of OPERATOR_AI_UNIVERSE) {
    for (const scenario of OPERATOR_DECISION_SCENARIOS) {
      const scenarioRows = (extractions || []).filter((e) => e.scenarioId === scenario.scenarioId);
      consider(op.canonicalId, scenario.scenarioId, "ALL_PROVIDERS", scenarioRows);
      for (const provider of providers) {
        const subset = scenarioRows.filter((e) => e.provider === provider);
        consider(op.canonicalId, scenario.scenarioId, provider, subset);
      }
    }
  }

  return candidates;
}

export function summarizeGapCandidates(candidates = []) {
  const count = (interpretation) =>
    candidates.filter((c) => c.gapInterpretation === interpretation).length;
  const trueGaps = candidates.filter(
    (c) => c.gapInterpretation === GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP
  );
  return {
    version: OPERATOR_COMPETITIVE_GAP_VERSION,
    candidateGaps: candidates.filter((c) => c.candidateKind === "GAP_CANDIDATE" || c.candidateKind === "INTERPRETED_NON_CLIENT").length,
    trueCompetitiveGaps: trueGaps.length,
    expectedPositioningDifferences: count(GAP_INTERPRETATION.EXPECTED_POSITIONING_DIFFERENCE),
    outOfScope: candidates.filter((c) => c.goldLabel === GAP_GOLD_LABEL.OUT_OF_SCOPE).length,
    insufficientContext: count(GAP_INTERPRETATION.INSUFFICIENT_CONTEXT),
    requiresReview: count(GAP_INTERPRETATION.REQUIRES_REVIEW),
    clientPromoted: candidates.filter((c) => c.clientPromoted).length,
    notAGap: count(GAP_INTERPRETATION.NOT_A_GAP) + candidates.filter((c) => c.candidateKind === "ABSENCE_NOT_A_GAP").length,
    scenarioOutOfScope: count(GAP_INTERPRETATION.SCENARIO_OUT_OF_SCOPE),
    emergingCompetitor: false,
  };
}
