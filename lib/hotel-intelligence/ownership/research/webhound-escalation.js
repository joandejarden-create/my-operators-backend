/**
 * Webhound escalation contract — design only (P1.6).
 * No automatic Webhound spend; default webhoundAuthorized=false.
 */

export const WEBHOUND_ESCALATION_VERSION = "ownership-webhound-escalation-v1";

export const WEBHOUND_ESCALATION_STATUSES = Object.freeze([
  "not_eligible",
  "eligible_pending_authorization",
  "authorized",
  "completed",
  "declined",
]);

export function evaluateWebhoundEscalationEligibility(input = {}) {
  const env = input.env || process.env;
  const webhoundAuthorized =
    String(env?.OWNERSHIP_WEBHOUND_AUTHORIZED || "0").trim() === "1" ||
    input.webhoundAuthorized === true;

  const reasons = [];
  let score = 0;

  if (input.unresolved_economic_owner) {
    score += 3;
    reasons.push("unresolved_economic_owner");
  }
  if (input.has_validated_company_seed) {
    score += 2;
    reasons.push("validated_company_seed");
  }
  if (input.commercial_importance === "high") {
    score += 2;
    reasons.push("commercial_importance_high");
  }
  if (input.research_exhausted) {
    score += 2;
    reasons.push("deterministic_research_exhausted");
  }
  if (input.prior_webhound_regression === "NOT_REPRODUCED") {
    score += 1;
    reasons.push("native_method_gap");
  }

  const eligible = score >= 5;
  let status = "not_eligible";
  if (eligible && webhoundAuthorized) status = "authorized";
  else if (eligible) status = "eligible_pending_authorization";

  return {
    version: WEBHOUND_ESCALATION_VERSION,
    webhoundAuthorized,
    eligible,
    status,
    score,
    reasons,
    policy: {
      default_webhound_authorized: false,
      max_auto_budget_usd: Number(env?.OWNERSHIP_WEBHOUND_MAX_BUDGET_USD || 0) || 0,
      note: "Webhound findings enter candidate/review evidence — never production truth without manual review",
    },
    required_output_fields: [
      "answer",
      "relationships",
      "evidence",
      "decisive_sources",
      "failed_sources",
      "research_methods",
      "recommended_reusable_method",
    ],
  };
}

export function normalizeWebhoundEscalationResult(webhoundResult = {}) {
  return {
    source: "webhound_escalation",
    status: "needs_review",
    relationships: webhoundResult.relationships || [],
    evidence: webhoundResult.evidence || [],
    research_methods: webhoundResult.research_methods || [],
    recommended_reusable_method: webhoundResult.recommended_reusable_method || null,
    decisive_sources: webhoundResult.decisive_sources || [],
    failed_sources: webhoundResult.failed_sources || [],
    note: "candidate_review_only_not_auto_promoted",
  };
}
