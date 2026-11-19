/**
 * Ownership research quality scorecard — separate from cost economics.
 * Numerical pass ≠ FULL_ACTIONABLE (classifier remains authoritative).
 */

import {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
  OWNERSHIP_SUCCESS_TIER,
  classifyOwnershipResearchSuccess,
} from "../ownership-research-objective.js";

export const QUALITY_SCORECARD_VERSION = "he-intel-p3b-m1-quality-scorecard-v1";

export const QUALITY_WEIGHTS = Object.freeze({
  actual_ownership_accuracy: 0.3,
  relevant_named_decision_makers: 0.2,
  verified_contact_pathways: 0.2,
  evidence_quality_and_relationship_verification: 0.15,
  completeness_and_uncertainty_handling: 0.1,
  freshness_of_evidence: 0.05,
});

/**
 * Score one hotel×method result. Penalties for invented/unsupported claims.
 * @param {object} input — evidence-grounded fields for classifier + rubric dimensions 0–1
 */
export function scoreOwnershipQuality(input = {}) {
  const classifier = classifyOwnershipResearchSuccess(input.classifierInput || input);
  const dims = {
    actual_ownership_accuracy: clamp01(input.actual_ownership_accuracy),
    relevant_named_decision_makers: clamp01(input.relevant_named_decision_makers),
    verified_contact_pathways: clamp01(input.verified_contact_pathways),
    evidence_quality_and_relationship_verification: clamp01(
      input.evidence_quality_and_relationship_verification
    ),
    completeness_and_uncertainty_handling: clamp01(
      input.completeness_and_uncertainty_handling
    ),
    freshness_of_evidence: clamp01(input.freshness_of_evidence),
  };

  let weighted = 0;
  for (const [k, w] of Object.entries(QUALITY_WEIGHTS)) {
    weighted += dims[k] * w;
  }

  const penalties = [];
  if (input.incorrect_ownership === true) {
    penalties.push({ code: "INCORRECT_OWNERSHIP", amount: 0.4 });
  }
  if (input.invented_contacts === true) {
    penalties.push({ code: "INVENTED_CONTACTS", amount: 0.35 });
  }
  if (input.irrelevant_people === true) {
    penalties.push({ code: "IRRELEVANT_PEOPLE", amount: 0.15 });
  }
  if (input.unsupported_relationships === true) {
    penalties.push({ code: "UNSUPPORTED_RELATIONSHIPS", amount: 0.2 });
  }
  if (input.false_confidence === true) {
    penalties.push({ code: "FALSE_CONFIDENCE", amount: 0.15 });
  }
  const penaltyTotal = penalties.reduce((s, p) => s + p.amount, 0);
  const qualityScore = clamp01(weighted - penaltyTotal);

  return {
    version: QUALITY_SCORECARD_VERSION,
    objective_id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    objective_version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
    weights: QUALITY_WEIGHTS,
    dimensions: dims,
    penalties,
    quality_score_0_to_1: Number(qualityScore.toFixed(4)),
    quality_score_pct: Number((qualityScore * 100).toFixed(2)),
    ownership_success_tier: classifier.tier,
    numerical_pass_is_not_full_actionable: true,
    full_actionable:
      classifier.tier === OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE,
    classifier,
  };
}

function clamp01(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

export function emptyScorecardRow(method, hotel) {
  return {
    method,
    hotel: hotel
      ? { hpcId: hotel.hpcId, name: hotel.name }
      : null,
    executed: false,
    quality: null,
    cost: {
      total_usd: null,
      cost_status: "NOT_EXECUTED",
    },
  };
}
