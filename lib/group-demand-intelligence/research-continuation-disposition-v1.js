/**
 * Research continuation disposition hygiene (GDI 2.0 Phase 2A).
 *
 * Incomplete opportunities must not be deleted, silently discarded, or marked
 * commercially exhausted solely because contact/lodging/provider/budget/strategy
 * milestones are incomplete.
 *
 * Does not enable paid research or autonomous loops.
 */

import {
  evaluateSharedCommercialEligibility,
  COMMERCIAL_ELIGIBILITY_STATUS,
  ELIGIBILITY_REASON,
} from "./commercial-eligibility-contract-v1.js";

export const RESEARCH_CONTINUATION_DISPOSITION_V1 =
  "gdi-research-continuation-disposition-v1";

export const CONTINUATION_DISPOSITION = Object.freeze({
  SALES_READY: "SALES_READY",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  HOLD_INSUFFICIENT: "HOLD_INSUFFICIENT",
  COMMERCIALLY_INELIGIBLE: "COMMERCIALLY_INELIGIBLE",
});

/** Reasons that must NEVER alone imply commercial exhaustion / delete. */
export const NON_EXHAUSTING_CONDITIONS = Object.freeze([
  "CONTACT_NOT_YET_VERIFIED",
  "LODGING_EVIDENCE_INCOMPLETE",
  "PROVIDER_FAILED",
  "STRATEGY_EMPTY_RESULTS",
  "AUTHORIZED_BUDGET_EXHAUSTED",
  "LATER_RESEARCH_MILESTONE_PENDING",
  "BUYER_RESPONSIBILITY_UNKNOWN",
  "MISSING_CONTACT_ROUTE",
  "MISSING_HOTEL_DEMAND_THESIS",
  "SOURCES_INSUFFICIENT",
]);

/**
 * Map incomplete-research conditions to a safe disposition.
 */
export function classifyResearchContinuationDisposition(input = {}) {
  const {
    opportunity = {},
    hotel = {},
    providerFailed = false,
    strategyEmptyResults = false,
    authorizedBudgetExhausted = false,
    lodgingEvidenceIncomplete = false,
    contactNotYetVerified = false,
    laterMilestonePending = false,
    forceIneligible = false,
    forceIneligibleReason = null,
  } = input;

  const commercial = evaluateSharedCommercialEligibility(opportunity, { hotel });

  if (forceIneligible === true) {
    return {
      version: RESEARCH_CONTINUATION_DISPOSITION_V1,
      disposition: CONTINUATION_DISPOSITION.COMMERCIALLY_INELIGIBLE,
      preserveOpportunity: true,
      doNotDelete: true,
      doNotSilentWithdraw: true,
      commerciallyExhausted: false,
      reason: forceIneligibleReason || "explicit_ineligible_marker",
      commercial,
      nextActions: ["Do not republish; retain for audit/history if needed."],
    };
  }

  if (commercial.status === COMMERCIAL_ELIGIBILITY_STATUS.SALES_READY) {
    return {
      version: RESEARCH_CONTINUATION_DISPOSITION_V1,
      disposition: CONTINUATION_DISPOSITION.SALES_READY,
      preserveOpportunity: true,
      doNotDelete: true,
      doNotSilentWithdraw: true,
      commerciallyExhausted: false,
      commercial,
      nextActions: commercial.nextActions,
      note: "Sales-ready commercially; publication still needs separate authorization.",
    };
  }

  if (commercial.status === COMMERCIAL_ELIGIBILITY_STATUS.COMMERCIALLY_INELIGIBLE) {
    return {
      version: RESEARCH_CONTINUATION_DISPOSITION_V1,
      disposition: CONTINUATION_DISPOSITION.COMMERCIALLY_INELIGIBLE,
      preserveOpportunity: true,
      doNotDelete: true,
      doNotSilentWithdraw: true,
      commerciallyExhausted: false,
      commercial,
      nextActions: commercial.nextActions,
    };
  }

  const incompleteSignals = [];
  if (providerFailed) incompleteSignals.push("PROVIDER_FAILED");
  if (strategyEmptyResults) incompleteSignals.push("STRATEGY_EMPTY_RESULTS");
  if (authorizedBudgetExhausted) incompleteSignals.push("AUTHORIZED_BUDGET_EXHAUSTED");
  if (lodgingEvidenceIncomplete) incompleteSignals.push("LODGING_EVIDENCE_INCOMPLETE");
  if (contactNotYetVerified) incompleteSignals.push("CONTACT_NOT_YET_VERIFIED");
  if (laterMilestonePending) incompleteSignals.push("LATER_RESEARCH_MILESTONE_PENDING");
  for (const g of commercial.unresolvedResearchGaps || []) {
    incompleteSignals.push(String(g));
  }

  const nextActions = [...(commercial.nextActions || [])];
  if (providerFailed) {
    nextActions.push("Provider failure is not exhaustion — retry alternate strategy when authorized.");
  }
  if (strategyEmptyResults) {
    nextActions.push("Empty strategy results — advance strategy ladder; retain opportunity.");
  }
  if (authorizedBudgetExhausted) {
    nextActions.push(
      "Authorized budget exhausted — pause paid work; keep RESEARCH_CONTINUATION for resume.",
    );
  }

  return {
    version: RESEARCH_CONTINUATION_DISPOSITION_V1,
    disposition: CONTINUATION_DISPOSITION.RESEARCH_CONTINUATION,
    preserveOpportunity: true,
    doNotDelete: true,
    doNotSilentWithdraw: true,
    commerciallyExhausted: false,
    incompleteSignals: [...new Set(incompleteSignals)],
    nonExhaustingConditions: NON_EXHAUSTING_CONDITIONS,
    commercial,
    blockers: commercial.unresolvedResearchGaps || [],
    nextActions: [...new Set(nextActions)],
    note:
      "Incomplete research remains RESEARCH_CONTINUATION. Never delete or mark exhausted for provider/budget/strategy/contact gaps alone.",
  };
}

/**
 * Guard used by callers tempted to drop rows after a failed attempt.
 */
export function assertMustPreserveIncompleteOpportunity(disposition = {}) {
  const mustPreserve =
    disposition.preserveOpportunity === true &&
    disposition.commerciallyExhausted !== true &&
    disposition.doNotDelete === true;
  return {
    ok: mustPreserve,
    preserveOpportunity: disposition.preserveOpportunity === true,
    commerciallyExhausted: disposition.commerciallyExhausted === true,
    disposition: disposition.disposition || null,
  };
}

export function isRecoverableEligibilityGap(gap) {
  const g = String(gap || "");
  return (
    NON_EXHAUSTING_CONDITIONS.includes(g) ||
    g === ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN ||
    g === ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER ||
    g === ELIGIBILITY_REASON.MISSING_CONTACT_ROUTE ||
    g === ELIGIBILITY_REASON.MISSING_HOTEL_DEMAND_THESIS ||
    g === ELIGIBILITY_REASON.SOURCES_INSUFFICIENT ||
    g === ELIGIBILITY_REASON.MISSING_CREDIBLE_ORG_ACTIVITY
  );
}
