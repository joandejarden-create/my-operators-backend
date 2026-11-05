/**
 * GDI 2.0 Phase 2D — adaptive research preflight (dry-run only).
 *
 * Integrates buyer-recovery planner with ORS-style evaluation surface.
 * Does NOT execute tools, override spend limits, or call paid providers.
 */

import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { planBuyerRecovery } from "./buyer-recovery-planner-v1.js";
import {
  evaluateOpportunity,
  chooseNextAction,
} from "./research/opportunity-research-supervisor-v1.js";
import { recoverCommercialEvidence } from "./commercial-evidence-recovery-v1.js";
import { redactOpportunityForShadowExport } from "./gdi2-shadow-publication-evaluation-v1.js";

export const ADAPTIVE_RESEARCH_PREFLIGHT_V1 =
  "gdi-adaptive-research-preflight-v1";

const ESTIMATED_COST_USD = Object.freeze({
  public_web: 0,
  official_pages: 0,
  org_route: 0,
  paid_enrichment: null, // never proposed as executable in Phase 2D
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function commercialBlockerFromGaps(gaps = []) {
  if (gaps.includes("MISSING_HOTEL_DEMAND_THESIS")) {
    return {
      code: "MISSING_HOTEL_DEMAND_THESIS",
      label: "Hotel-specific demand thesis incomplete",
    };
  }
  if (gaps.includes("MISSING_NAMED_RELEVANT_BUYER")) {
    return {
      code: "MISSING_NAMED_RELEVANT_BUYER",
      label: "Commercially relevant named buyer missing",
    };
  }
  if (gaps.includes("BUYER_RESPONSIBILITY_UNKNOWN")) {
    return {
      code: "BUYER_RESPONSIBILITY_UNKNOWN",
      label: "Buyer commercial responsibility unresolved",
    };
  }
  if (gaps.includes("MISSING_CONTACT_ROUTE")) {
    return {
      code: "MISSING_CONTACT_ROUTE",
      label: "Usable contact route missing",
    };
  }
  if (gaps.includes("SOURCES_INSUFFICIENT")) {
    return {
      code: "SOURCES_INSUFFICIENT",
      label: "Traceable sources insufficient",
    };
  }
  if (gaps.length) {
    return { code: gaps[0], label: gaps[0] };
  }
  return { code: "NONE", label: "No commercial blocker" };
}

function providerForApproach(approach = {}) {
  const id = text(approach.id);
  if (!id) return { tool: "none", paid: false, authorized: false };
  if (/ORG_ROUTE/i.test(id)) {
    return { tool: "organizational_contact_route", paid: false, authorized: true };
  }
  if (/PUBLIC_PROFESSIONAL|SURFE|PDL/i.test(id)) {
    return {
      tool: "public_professional_profiles",
      paid: /SURFE|PDL/i.test(id),
      authorized: false,
      note: "Paid enrichment not authorized in Phase 2D preflight",
    };
  }
  return {
    tool: "public_web_official_pages",
    paid: false,
    authorized: true,
  };
}

/**
 * Dry-run adaptive research preflight for one opportunity.
 */
export function buildAdaptiveResearchPreflight(opportunity = {}, opts = {}) {
  const recovery =
    opts.skipRecovery === true
      ? null
      : recoverCommercialEvidence(opportunity, { hotel: opts.hotel });
  const working = recovery?.recoveredOpportunity || opportunity;

  const shared = evaluateSharedCommercialEligibility(working, {
    hotel: opts.hotel,
  });
  const recoveryPlan = planBuyerRecovery(working, {
    hotel: opts.hotel,
    attempts: opts.attempts || opportunity.buyerResearchAttempts || [],
    providerFailed: opts.providerFailed === true,
    authorizedBudgetExhausted: opts.authorizedBudgetExhausted === true,
    paidResearchAuthorized: false,
  });

  let orsEvaluation = null;
  let orsNextAction = null;
  try {
    orsEvaluation = evaluateOpportunity(working, {
      dryRun: true,
      paidResearchAuthorized: false,
    });
    const noopPlanner = {
      plan: ({ blocker }) => ({
        strategy: recoveryPlan.nextApproach?.id || "REVIEW_ONLY",
        blocker: blocker?.key || blocker?.code || null,
        dryRun: true,
        execute: false,
      }),
    };
    orsNextAction = chooseNextAction(working, orsEvaluation, noopPlanner, {
      dryRun: true,
    });
  } catch (err) {
    orsEvaluation = {
      error: String(err?.message || err),
      dryRun: true,
    };
  }

  const blocker = commercialBlockerFromGaps(shared.gaps || []);
  const approach = recoveryPlan.nextApproach;
  const provider = providerForApproach(approach || {});
  const attempted = recoveryPlan.strategiesAlreadyAttempted || [];
  const lowInfo = recoveryPlan.lowInformationRepeats || [];

  const duplicateOrLowInfoBlocked =
    Boolean(approach) &&
    (attempted.includes(approach.id) ||
      lowInfo.some((r) => r.strategyId === approach.id));

  let stopOrContinue = {
    decision: "CONTINUE",
    justification: recoveryPlan.worthContinuing?.reason || "recoverable_gaps",
  };
  if (shared.salesReady) {
    stopOrContinue = {
      decision: "STOP_SALES_READY",
      justification: "shared_contract_sales_ready_after_recovery_overlay",
    };
  } else if (
    recoveryPlan.recommendation === "STOP_WITH_EVIDENCE" ||
    recoveryPlan.recommendation === "COMMERCIALLY_INELIGIBLE"
  ) {
    stopOrContinue = {
      decision: "STOP",
      justification: recoveryPlan.recommendation,
    };
  } else if (
    recoveryPlan.recommendation === "SCHEDULE_REFRESH" ||
    recoveryPlan.recommendation === "REQUEST_HUMAN_REVIEW"
  ) {
    stopOrContinue = {
      decision: recoveryPlan.recommendation,
      justification: recoveryPlan.worthContinuing?.reason || recoveryPlan.recommendation,
    };
  } else if (duplicateOrLowInfoBlocked && !recoveryPlan.prioritizedApproaches?.length) {
    stopOrContinue = {
      decision: "STOP_EXHAUSTED_PUBLIC_STRATEGIES",
      justification: "only_duplicate_or_low_information_strategies_remain",
    };
  }

  const estimatedCostUsd =
    provider.paid === true
      ? ESTIMATED_COST_USD.paid_enrichment
      : ESTIMATED_COST_USD.public_web;

  return {
    version: ADAPTIVE_RESEARCH_PREFLIGHT_V1,
    dryRun: true,
    executesTools: false,
    paidResearchAuthorized: false,
    overridesSpendLimits: false,
    opportunityRef: redactOpportunityForShadowExport(opportunity),
    recoveryCategory: recovery?.category || null,
    commercialBlocker: blocker,
    researchObjective:
      blocker.code === "NONE"
        ? "Maintain sales-ready evidence quality"
        : `Close commercial gap: ${blocker.label}`,
    recommendedStrategy: approach
      ? {
          id: approach.id,
          label: approach.label || approach.id,
          source: approach.source || null,
          expectedInformationGain: approach.expectedInformationGain ?? null,
          lowInformationRisk: approach.lowInformationRisk === true,
        }
      : null,
    evidenceSupportingStrategy: [
      ...(recovery?.demand?.evidenceLinks || []),
      ...(recovery?.buyer?.sourceRefs || []).map((s) => ({
        field: s.kind,
        url: s.url,
      })),
    ].slice(0, 8),
    previouslyAttemptedStrategies: attempted,
    lowInformationRepeats: lowInfo,
    expectedInformationGain: approach?.expectedInformationGain ?? null,
    estimatedResearchCostUsd: estimatedCostUsd,
    providerToolProposed: provider,
    authorizationStatus: {
      paidResearch: false,
      toolExecution: false,
      phase2dDryRunOnly: true,
      providerAuthorized: provider.authorized === true && provider.paid !== true,
    },
    stopOrContinuation: stopOrContinue,
    plannerRecommendation: recoveryPlan.recommendation,
    sharedEligibilityStatus: shared.status,
    sharedGaps: shared.gaps || [],
    orsDryRun: {
      evaluationAvailable: Boolean(orsEvaluation) && !orsEvaluation.error,
      blockers: orsEvaluation?.blockers?.map((b) => b.key || b.code) || [],
      nextAction: orsNextAction,
      error: orsEvaluation?.error || null,
    },
    note: "Preflight only — no tool execution, no spend, no visibility changes.",
  };
}

/**
 * Batch dry-run preflight for a list of opportunities (redacted summaries).
 */
export function buildAdaptiveResearchPreflightBatch(opportunities = [], opts = {}) {
  const results = [];
  for (const opp of opportunities) {
    results.push(buildAdaptiveResearchPreflight(opp, opts));
  }
  const byDecision = {};
  const byBlocker = {};
  let duplicateLowInfo = 0;
  for (const r of results) {
    const d = r.stopOrContinuation?.decision || "UNKNOWN";
    byDecision[d] = (byDecision[d] || 0) + 1;
    const b = r.commercialBlocker?.code || "NONE";
    byBlocker[b] = (byBlocker[b] || 0) + 1;
    if ((r.lowInformationRepeats || []).length) duplicateLowInfo += 1;
  }
  return {
    version: ADAPTIVE_RESEARCH_PREFLIGHT_V1,
    dryRun: true,
    count: results.length,
    byDecision,
    byBlocker,
    duplicateOrLowInfoStrategyCases: duplicateLowInfo,
    results,
  };
}
