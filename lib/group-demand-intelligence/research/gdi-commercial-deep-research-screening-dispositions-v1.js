/**
 * Permanent commercial screening dispositions for every unique discovered candidate.
 *
 * Missing named buyer / contact channel must NOT auto-exclude credible demand from
 * Agent Deep Research — those are questions deep research should resolve.
 * Competitor relationships likewise do not auto-exclude; preserve Research SoT.
 */

import { PRE_DEEP_SCREEN_OUTCOME } from "./gdi-pre-deep-candidate-screening-v1.js";
import { SELECTION_DECISION } from "./gdi-deep-research-selection-register-v1.js";

export const GDI_COMMERCIAL_DEEP_RESEARCH_SCREENING_DISPOSITIONS_V1 =
  "gdi-commercial-deep-research-screening-dispositions-v1";

export const COMMERCIAL_SCREENING_DISPOSITION = Object.freeze({
  DEEP_RESEARCH_REQUIRED: "DEEP_RESEARCH_REQUIRED",
  DEEP_RESEARCH_ALREADY_COMPLETE_REUSE: "DEEP_RESEARCH_ALREADY_COMPLETE_REUSE",
  PARTIAL_DEEP_RESEARCH_REFRESH: "PARTIAL_DEEP_RESEARCH_REFRESH",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  VERIFIED_COMPETITOR_COMMITTED: "VERIFIED_COMPETITOR_COMMITTED",
  HISTORICAL_MONITOR: "HISTORICAL_MONITOR",
  DUPLICATE_REUSE: "DUPLICATE_REUSE",
  NOT_RELEVANT_WITH_EVIDENCE: "NOT_RELEVANT_WITH_EVIDENCE",
  INSUFFICIENT_INITIAL_EVIDENCE: "INSUFFICIENT_INITIAL_EVIDENCE",
});

/** Dispositions that remain on the active commercial research queue. */
export const ACTIVE_RESEARCH_QUEUE_DISPOSITIONS = Object.freeze([
  COMMERCIAL_SCREENING_DISPOSITION.DEEP_RESEARCH_REQUIRED,
  COMMERCIAL_SCREENING_DISPOSITION.PARTIAL_DEEP_RESEARCH_REFRESH,
  COMMERCIAL_SCREENING_DISPOSITION.RESEARCH_CONTINUATION,
  COMMERCIAL_SCREENING_DISPOSITION.VERIFIED_COMPETITOR_COMMITTED,
]);

/**
 * Map pre-deep + selection + optional deep maturity into a permanent disposition.
 * Deterministic. Does not spend.
 */
export function mapToCommercialScreeningDisposition({
  preDeepOutcome = null,
  pursuitDecision = null,
  selectionDecision = null,
  deepAlreadyComplete = false,
  deepPartialRefreshNeeded = false,
  competitorCommittedVerified = false,
  historicalOnly = false,
  insufficientEvidence = false,
  researchContinuation = false,
} = {}) {
  if (deepAlreadyComplete === true && deepPartialRefreshNeeded !== true) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.DEEP_RESEARCH_ALREADY_COMPLETE_REUSE,
      activeQueue: false,
      paidDeepRequired: false,
      reason: "existing_agent_dossier_reuse",
    };
  }
  if (deepPartialRefreshNeeded === true) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.PARTIAL_DEEP_RESEARCH_REFRESH,
      activeQueue: true,
      paidDeepRequired: true,
      reason: "partial_dossier_material_gap_refresh",
    };
  }
  if (
    preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE ||
    selectionDecision === SELECTION_DECISION.SKIPPED_NOT_ELIGIBLE
  ) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.DUPLICATE_REUSE,
      activeQueue: false,
      paidDeepRequired: false,
      reason: "duplicate_or_inventory_reuse",
    };
  }
  if (
    preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.REJECT_UNSUPPORTED ||
    preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY
  ) {
    return {
      disposition:
        preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY
          ? COMMERCIAL_SCREENING_DISPOSITION.HISTORICAL_MONITOR
          : COMMERCIAL_SCREENING_DISPOSITION.NOT_RELEVANT_WITH_EVIDENCE,
      activeQueue: false,
      paidDeepRequired: false,
      reason: String(preDeepOutcome),
    };
  }
  if (historicalOnly === true) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.HISTORICAL_MONITOR,
      activeQueue: false,
      paidDeepRequired: false,
      reason: "historical_monitor_only",
    };
  }
  if (competitorCommittedVerified === true) {
    // Still active research — investigate overflow / future / displacement.
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.VERIFIED_COMPETITOR_COMMITTED,
      activeQueue: true,
      paidDeepRequired: deepAlreadyComplete !== true,
      reason: "competitor_committed_requires_overflow_or_future_investigation",
    };
  }
  if (insufficientEvidence === true) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.INSUFFICIENT_INITIAL_EVIDENCE,
      activeQueue: false,
      paidDeepRequired: false,
      reason: "insufficient_initial_evidence",
    };
  }
  if (
    researchContinuation === true ||
    preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.ELIGIBLE_FOR_TARGETED_RESEARCH ||
    String(pursuitDecision || "") === "RESEARCH_CONTINUATION"
  ) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.RESEARCH_CONTINUATION,
      activeQueue: true,
      paidDeepRequired: true,
      reason: "targeted_or_continuation_research",
    };
  }
  if (
    preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.ELIGIBLE_FOR_DEEP_RESEARCH ||
    preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.EXISTING_OPPORTUNITY_ENRICHMENT ||
    selectionDecision === SELECTION_DECISION.SELECTED_FOR_DEEP ||
    selectionDecision === SELECTION_DECISION.SELECTED_FOR_TARGETED_TIER3 ||
    selectionDecision === SELECTION_DECISION.DEFERRED_LOWER_RANK ||
    selectionDecision === SELECTION_DECISION.DEFERRED_FAIRNESS_OR_BUDGET
  ) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.DEEP_RESEARCH_REQUIRED,
      activeQueue: true,
      paidDeepRequired: true,
      reason: "commercially_credible_requires_deep",
    };
  }
  if (preDeepOutcome === PRE_DEEP_SCREEN_OUTCOME.SCREEN_ONLY) {
    return {
      disposition: COMMERCIAL_SCREENING_DISPOSITION.INSUFFICIENT_INITIAL_EVIDENCE,
      activeQueue: false,
      paidDeepRequired: false,
      reason: "screen_only",
    };
  }
  return {
    disposition: COMMERCIAL_SCREENING_DISPOSITION.INSUFFICIENT_INITIAL_EVIDENCE,
    activeQueue: false,
    paidDeepRequired: false,
    reason: "unmapped_fallback",
  };
}

/**
 * Policy note: contact gaps are deep-research questions, not eligibility killers.
 */
export function contactGapBlocksDeepResearch() {
  return false;
}

export default {
  GDI_COMMERCIAL_DEEP_RESEARCH_SCREENING_DISPOSITIONS_V1,
  COMMERCIAL_SCREENING_DISPOSITION,
  ACTIVE_RESEARCH_QUEUE_DISPOSITIONS,
  mapToCommercialScreeningDisposition,
  contactGapBlocksDeepResearch,
};
