/**
 * Reality Gap methodology V2 — comparison + staged activation after founder approval.
 *
 * RG_BINARY_GAP_V1 remains the immutable historical formula for published snapshots.
 * RG_WEIGHTED_PARTIAL_V2 may be used for newly staged reports after threshold validation.
 *
 * Doctrine:
 *   - Preserve prior published results
 *   - Versioned methodologies
 *   - Untested / insufficient evidence stay out of denominator
 *   - Prompt text is never recognition
 */

import {
  ATTRIBUTE_DEFINITIONS,
  computeRealityGap,
} from "./reality-gap.js";
import { ATTRIBUTE_MEASUREMENT_STATE_V1 } from "../property-attributes/adp-hotel-attribute-intelligence-standard-v1.js";
import { roundAdpPercent } from "../format-percent.js";
import {
  CONDITIONAL_APPROVED_METHODOLOGY_ID,
  resolveActiveStagedRealityGapMethodologyId,
  FOUNDER_RG_WEIGHTED_PARTIAL_V2_APPROVAL,
} from "./reality-gap-founder-approval-v2.js";

export const ADP_REALITY_GAP_METHODOLOGY_V2 = "ADP_REALITY_GAP_METHODOLOGY_V2";

/** Immutable historical customer formula (published snapshots). */
export const APPROVED_CUSTOMER_METHODOLOGY_ID = "RG_BINARY_GAP_V1";
/** Historical alias — same as APPROVED_CUSTOMER_METHODOLOGY_ID. */
export const HISTORICAL_IMMUTABLE_METHODOLOGY_ID = "RG_BINARY_GAP_V1";

export const REALITY_GAP_METHODOLOGY_REGISTRY_V2 = Object.freeze({
  RG_BINARY_GAP_V1: Object.freeze({
    id: "RG_BINARY_GAP_V1",
    label: "Option A — Binary gap (historical immutable)",
    option: "A",
    description:
      "gapScore = (missed + partial) / testedEligible × 100. Partial counts as full gap.",
    customerFacingApproved: true,
    historicalImmutable: true,
  }),
  RG_WEIGHTED_PARTIAL_V2: Object.freeze({
    id: "RG_WEIGHTED_PARTIAL_V2",
    label: "Option B — Weighted gap (miss + 0.5×partial)",
    option: "B",
    description:
      "gapScore = (missed + 0.5 × partial) / testedEligible × 100. Conditionally founder-approved for staged reports.",
    customerFacingApproved: true,
    stagedOnlyPendingPublication: true,
  }),
  RG_SEPARATE_COMPONENTS_V2: Object.freeze({
    id: "RG_SEPARATE_COMPONENTS_V2",
    label: "Option C — Separate recognition / partial / miss (no composite)",
    option: "C",
    description:
      "Reports recognizedRate, partialRate, missRate separately; no single composite gapScore.",
    customerFacingApproved: false,
  }),
});

/**
 * Active staged customer methodology.
 * Requires thresholdValidationPassed after conditional founder approval.
 * Published snapshots must continue to use RG_BINARY_GAP_V1.
 */
export function getActiveCustomerRealityGapMethodologyId(opts = {}) {
  if (opts.forceHistoricalV1 === true) return APPROVED_CUSTOMER_METHODOLOGY_ID;
  if (opts.founderApprovedMethodologyId) {
    const id = String(opts.founderApprovedMethodologyId);
    if (id === CONDITIONAL_APPROVED_METHODOLOGY_ID && opts.thresholdValidationPassed === true) {
      return CONDITIONAL_APPROVED_METHODOLOGY_ID;
    }
    if (REALITY_GAP_METHODOLOGY_REGISTRY_V2[id]?.customerFacingApproved && opts.forceActivate) {
      return id;
    }
  }
  return resolveActiveStagedRealityGapMethodologyId({
    thresholdValidationPassed: opts.thresholdValidationPassed === true,
  });
}

export { CONDITIONAL_APPROVED_METHODOLOGY_ID, FOUNDER_RG_WEIGHTED_PARTIAL_V2_APPROVAL };

function round1(n) {
  if (n == null || Number.isNaN(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

/**
 * Recalculate all three methodology options from the same computeRealityGap classifications.
 * Does not mutate the approved customer score path.
 *
 * @param {object[]} observations
 * @param {object} propertyProfile
 */
export function computeRealityGapMethodologyComparisonV2(observations, propertyProfile) {
  const base = computeRealityGap(observations, propertyProfile);
  const classifications = base.attributeClassifications || [];
  const tested = classifications.filter(
    (c) =>
      c.measurementState !== ATTRIBUTE_MEASUREMENT_STATE_V1.VERIFIED_ATTRIBUTE_NOT_TESTED &&
      c.measurementState !== ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE &&
      c.measurementState !== ATTRIBUTE_MEASUREMENT_STATE_V1.NOT_APPLICABLE
  );
  const recognized = tested.filter(
    (c) => c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_RECOGNIZED
  );
  const partial = tested.filter(
    (c) => c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_PARTIALLY_RECOGNIZED
  );
  const missed = tested.filter(
    (c) => c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_NOT_RECOGNIZED
  );
  const denom = tested.length;
  // Mirror approved computeRealityGap: measured + denom>0 → score; measured + denom=0 → 0; else null.
  const optionA = base.measured
    ? denom
      ? roundAdpPercent(((missed.length + partial.length) / denom) * 100)
      : roundAdpPercent(0)
    : null;
  const optionB = base.measured
    ? denom
      ? roundAdpPercent(((missed.length + 0.5 * partial.length) / denom) * 100)
      : roundAdpPercent(0)
    : null;
  const optionC = {
    recognizedCount: recognized.length,
    partialCount: partial.length,
    missedCount: missed.length,
    testedEligible: denom,
    recognizedRatePct: denom ? roundAdpPercent((recognized.length / denom) * 100) : null,
    partialRatePct: denom ? roundAdpPercent((partial.length / denom) * 100) : null,
    missRatePct: denom ? roundAdpPercent((missed.length / denom) * 100) : null,
    compositeGapScore: null,
    note:
      denom === 0 && base.measured
        ? "All governed attributes untested or insufficient — no Core-tested denominator"
        : null,
  };

  return {
    gate: ADP_REALITY_GAP_METHODOLOGY_V2,
    approvedCustomerMethodologyId: APPROVED_CUSTOMER_METHODOLOGY_ID,
    activeCustomerMethodologyId: getActiveCustomerRealityGapMethodologyId(),
    measured: base.measured === true,
    measurementStatus: base.measurementStatus,
    eligibilityGrain: base.eligibilityGrain,
    subjectMentionedCorpus: base.totalPropertyMentions,
    observationCount: base.observationCount,
    oldScore: base.gapScore,
    optionA_binaryGap: optionA,
    optionB_weightedGap: optionB,
    optionC_components: optionC,
    /** Must match Option A and computeRealityGap gapScore when measured. */
    optionAMatchesApprovedCustomer:
      optionA === base.gapScore || (optionA == null && base.gapScore == null),
    counts: {
      testedEligible: denom,
      recognized: recognized.length,
      partial: partial.length,
      missed: missed.length,
      untested: classifications.filter(
        (c) =>
          c.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.VERIFIED_ATTRIBUTE_NOT_TESTED
      ).length,
      insufficient: classifications.filter(
        (c) =>
          c.measurementState ===
          ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE
      ).length,
    },
    thresholds: {
      recognizedMinPct: 40,
      partialMinPct: 15,
      partialMaxPctExclusive: 40,
      missedMaxPctExclusive: 15,
    },
    registry: REALITY_GAP_METHODOLOGY_REGISTRY_V2,
    base,
  };
}

/**
 * Recommended methodology for founder approval — does not activate.
 */
export function recommendRealityGapMethodologyV2(portfolioComparisons = []) {
  const rows = portfolioComparisons.filter((r) => r?.measured);
  const avgA =
    rows.length > 0
      ? round1(rows.reduce((s, r) => s + (r.optionA_binaryGap || 0), 0) / rows.length)
      : null;
  const avgB =
    rows.length > 0
      ? round1(rows.reduce((s, r) => s + (r.optionB_weightedGap || 0), 0) / rows.length)
      : null;
  const hotelsWithPartial = rows.filter((r) => (r.counts?.partial || 0) > 0).length;

  return {
    recommendationId: "RG_WEIGHTED_PARTIAL_V2_PROPOSED",
    status: "CONDITIONALLY_APPROVED_PENDING_THRESHOLD_VALIDATION",
    doNotActivateWithoutApproval: false,
    keepHistoricalPublishedAs: APPROVED_CUSTOMER_METHODOLOGY_ID,
    keepCustomerFacingUntilApproval: APPROVED_CUSTOMER_METHODOLOGY_ID,
    recommendedMethodologyId: "RG_WEIGHTED_PARTIAL_V2",
    rationale: [
      "Option A (approved) treats partial recognition (15–39%) identically to complete misses, inflating gap severity when AI partially surfaces an attribute.",
      "Option B preserves a single executive KPI while distinguishing partial from miss via 0.5 weight — clearer owner action priority without losing a composite.",
      "Option C is diagnostically strongest but requires UI redesign (three metrics) before customer use.",
      `Portfolio: ${hotelsWithPartial}/${rows.length} hotels have ≥1 partial; mean A=${avgA}% vs mean B=${avgB}%.`,
    ],
    limitations: [
      "0.5 weight is a governance choice, not an empirical elasticity estimate.",
      "Recognition thresholds (15%/40%) remain keyword-detector dependent.",
      "Does not fix Core keyword depth limitations for amenities without detectors.",
    ],
    implementationIfApproved: [
      "Write founder approval record with methodology id + timestamp",
      "Set ACTIVE_CUSTOMER_METHODOLOGY via governed config (not silent default change)",
      "Versioned recompute of staged Reality Gap for all 31; preserve prior gapScore as methodologyVersion=RG_BINARY_GAP_V1",
      "Update Executive Summary / Priority Action copy only after staged QA",
      "Never overwrite published Live reports without separate publication authorization",
    ],
    portfolioMeans: { optionA: avgA, optionB: avgB, hotelsWithPartial, n: rows.length },
  };
}

export function assertApprovedFormulaUnchanged(comparison) {
  if (!comparison?.optionAMatchesApprovedCustomer) {
    throw new Error(
      "RG_METHODOLOGY_V2_INTEGRITY: Option A must equal computeRealityGap gapScore (approved customer formula)"
    );
  }
  return true;
}

/** Attribute definitions still used for eligibility — re-export for audits. */
export { ATTRIBUTE_DEFINITIONS };
