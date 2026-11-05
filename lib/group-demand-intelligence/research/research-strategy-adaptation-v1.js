/**
 * Research strategy adaptation V1 (run-050) + coherence (run-051).
 *
 * When a strategy fails (e.g. heuristic /about|/team), select a different
 * justified strategy that still targets the top commercial gap.
 * Strategy and question MUST remain coherent (research-action-contract-v1).
 */

import {
  assessCommercialResearchState,
} from "./commercial-research-assessment-v1.js";
import {
  isHeuristicOrgPathUrl,
  heuristicHostsAlreadyProbed,
  FOLLOWUP_STATUS,
} from "./related-entity-followup-disposition-v1.js";
import {
  buildCoherentResearchAction,
  selectCoherentAdaptedStrategy,
  RESEARCH_STRATEGIES,
  RESEARCH_ACTION_CONTRACT_V1,
} from "./research-action-contract-v1.js";

export const RESEARCH_STRATEGY_ADAPTATION_V1 =
  "gdi-research-strategy-adaptation-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function attemptedStrategies(opportunity = {}) {
  const fromAttempts = (opportunity.researchAttempts || []).map((a) =>
    text(a.strategy || a.notes || ""),
  );
  const fromFailed = opportunity.failedResearchStrategies || [];
  return [...new Set([...fromAttempts, ...fromFailed].filter(Boolean))];
}

/**
 * Choose the next justified research step after assessing commercial gaps
 * and prior failed strategies. Always regenerates question + evidenceSought
 * to match the selected strategy (run-051 research-action contract).
 */
export function selectNextAdaptedResearchStep(opportunity = {}, hotel = {}) {
  const assessment = assessCommercialResearchState(opportunity, hotel);
  const attempted = attemptedStrategies(opportunity);
  const failed = [...(opportunity.failedResearchStrategies || [])];
  const probedHosts = heuristicHostsAlreadyProbed(opportunity);
  const recentHeuristic = (opportunity.investigatedLeadUrls || []).filter(
    isHeuristicOrgPathUrl,
  );
  const heuristicOrgPathsExhausted =
    recentHeuristic.length >= 2 && probedHosts.size > 0;

  if (heuristicOrgPathsExhausted && !failed.includes("HEURISTIC_ORG_PATH")) {
    failed.push("HEURISTIC_ORG_PATH");
  }

  const selected = selectCoherentAdaptedStrategy({
    assessment,
    failedStrategies: [...attempted, ...failed],
    heuristicOrgPathsExhausted,
  });

  let strategy = selected.strategy;

  if (!strategy && assessment.gaps.length === 0) {
    return {
      version: RESEARCH_STRATEGY_ADAPTATION_V1,
      action: "NONE",
      executable: false,
      status: FOLLOWUP_STATUS.RESOLVED_WITH_EVIDENCE,
      assessment,
      note: "no_material_gaps_remaining",
      contractVersion: RESEARCH_ACTION_CONTRACT_V1,
    };
  }

  if (!strategy || strategy === RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH) {
    const refresh = buildCoherentResearchAction({
      strategy: RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH,
      opportunity,
      hotel,
    });
    return {
      version: RESEARCH_STRATEGY_ADAPTATION_V1,
      action: "SCHEDULE_FUTURE_REFRESH",
      executable: false,
      status: FOLLOWUP_STATUS.SCHEDULED_FOR_RESUME,
      question: refresh.question,
      strategy: RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH,
      failedStrategies: [...new Set(failed)],
      assessment,
      researchAction: refresh,
      note: "adapted_to_refresh_or_exhausted",
      contractVersion: RESEARCH_ACTION_CONTRACT_V1,
    };
  }

  // ALWAYS rebuild question/evidence/query from the final strategy — never keep
  // a prior fit question when strategy was redirected to contact (or vice versa).
  const researchAction = buildCoherentResearchAction({
    strategy,
    opportunity,
    hotel,
  });

  return {
    version: RESEARCH_STRATEGY_ADAPTATION_V1,
    action: "EXECUTE_STRATEGY",
    executable: true,
    status: FOLLOWUP_STATUS.PENDING_EXECUTABLE,
    question: researchAction.question,
    strategy: researchAction.strategy,
    failedStrategies: [...new Set(failed)],
    assessment,
    researchAction,
    note: heuristicOrgPathsExhausted
      ? "adapted_after_heuristic_exhaustion_preserving_top_gap_family"
      : "adapted_next_strategy_selected",
    contractVersion: RESEARCH_ACTION_CONTRACT_V1,
    nextResearchStep: {
      action: researchAction.strategy,
      question: researchAction.question,
      evidenceSought: researchAction.evidenceSought,
      targetEntity: researchAction.targetEntity,
      query: researchAction.query,
      objective: researchAction.objective,
      url: null,
      executable: true,
      paidCalls: researchAction.paidCalls,
      publicationEligibilityChanged: false,
      stoppingCriteria:
        "Execute coherent adapted strategy; do not repeat failed heuristic paths; never use strategy IDs as entity names.",
      followUpStatus: FOLLOWUP_STATUS.PENDING_EXECUTABLE,
    },
  };
}

/**
 * Record a failed strategy on the opportunity for future adaptation.
 */
export function recordFailedResearchStrategy(opportunity = {}, strategy = "", detail = "") {
  const next = { ...opportunity };
  const list = [...(next.failedResearchStrategies || [])];
  const key = text(strategy);
  if (key && !list.includes(key)) list.push(key);
  next.failedResearchStrategies = list;
  next.notes = [
    ...(Array.isArray(next.notes) ? next.notes : []),
    `failed_research_strategy:${key}${detail ? `:${detail}` : ""}`,
  ];
  return next;
}
