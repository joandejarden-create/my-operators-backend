/**
 * P0D Truth Layer hook — integrates non-Census truth comparisons when available.
 */

import { isProductionTruthGapEligible } from "../truth-layer/p0c-truth-integration.js";

export const TRUTH_COMPARISON_STATUSES = Object.freeze([
  "NOT_EVALUATED",
  "INSUFFICIENT_DEALALITY_EVIDENCE",
  "ALIGNED",
  "POTENTIAL_PERCEPTION_GAP",
]);

/**
 * @param {object} [gap]
 * @param {object} [truthComparison] — optional P0D-A comparison record
 */
export function buildTruthLayerHook(gap = {}, truthComparison = null) {
  if (truthComparison) {
    const gate = isProductionTruthGapEligible(truthComparison);
    return {
      dealalityTruthRef: truthComparison.truthComparisonId || null,
      truthComparisonStatus: truthComparison.comparisonStatus || "NOT_EVALUATED",
      dealalitySource: truthComparison.dealalitySource || null,
      dealalityGovernanceState: truthComparison.dealalityGovernanceState || null,
      productionEligible: gate.eligible,
      productionBlockReason: gate.eligible ? null : gate.reason,
      gapClass: gap.gapClass || null,
      subjectBrandId: gap.subjectBrandId || truthComparison.subjectBrandId || null,
      scenarioId: gap.scenarioId || truthComparison.scenarioId || null,
      note: gate.eligible
        ? "P0D-A Truth Layer — production perception gap validated."
        : "P0D-A Truth Layer — comparison present; production gap gated.",
    };
  }

  return {
    dealalityTruthRef: null,
    truthComparisonStatus: "NOT_EVALUATED",
    productionEligible: false,
    gapClass: gap.gapClass || null,
    subjectBrandId: gap.subjectBrandId || null,
    scenarioId: gap.scenarioId || null,
    note: "Truth Layer v1 (P0D-A) — awaiting explicit claim comparison.",
  };
}

/**
 * Resolve hook from stored comparison by subject + scenario + claim type.
 */
export function resolveTruthLayerHook(gap, comparisons = []) {
  const match = comparisons.find(
    (c) =>
      c.subjectBrandId === gap.subjectBrandId &&
      c.scenarioId === gap.scenarioId &&
      c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP"
  );
  return buildTruthLayerHook(gap, match || null);
}
