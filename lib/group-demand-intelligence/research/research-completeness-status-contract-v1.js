/**
 * Research Completeness Status Contract V1 (hotel-generic).
 *
 * Technical PowerShell / provider execution success ≠ commercial research complete.
 * A fixed candidate cap may protect budget but must NEVER be reported as COMPLETE
 * while high-priority eligible candidates remain uninvestigated solely because of the cap.
 */
export const RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1 =
  "gdi-research-completeness-status-contract-v1";

export const RESEARCH_COMPLETENESS_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  PARTIAL_BUDGET_LIMIT: "PARTIAL_BUDGET_LIMIT",
  PARTIAL_CANDIDATE_CAP: "PARTIAL_CANDIDATE_CAP",
  PARTIAL_PROVIDER_FAILURE: "PARTIAL_PROVIDER_FAILURE",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  NO_ELIGIBLE_DEMAND: "NO_ELIGIBLE_DEMAND",
});

/**
 * @param {{
 *   uniqueCandidates?: number,
 *   deepAttempted?: number,
 *   deepCompleted?: number,
 *   eligibleUninvestigatedHighPriority?: number,
 *   stopReason?: string,
 *   budgetRemainingUsd?: number,
 *   candidateCapHit?: boolean,
 *   budgetCapHit?: boolean,
 *   providerFailures?: number,
 *   strategiesExhaustedForEligible?: boolean,
 * }} input
 */
export function classifyWorkflowResearchCompleteness(input = {}) {
  const unique = Number(input.uniqueCandidates || 0);
  const deepCompleted = Number(input.deepCompleted || 0);
  const eligibleLeft = Number(input.eligibleUninvestigatedHighPriority || 0);
  const stop = String(input.stopReason || "").toUpperCase();
  const budgetRemaining = Number(input.budgetRemainingUsd ?? NaN);
  const candidateCapHit =
    input.candidateCapHit === true ||
    /CANDIDATE_CAP|WORKFLOW_CAP|GLOBAL_CAP|MAX_DEEP/i.test(stop);
  const budgetCapHit =
    input.budgetCapHit === true ||
    /BUDGET|CEILING|SOFT_CAP|HARD_CEILING/i.test(stop);
  const providerFailures = Number(input.providerFailures || 0);

  if (unique === 0) {
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: RESEARCH_COMPLETENESS_STATUS.NO_ELIGIBLE_DEMAND,
      reason: "no_unique_discovery_candidates",
      technicalExecutionComplete: true,
      commercialResearchComplete: true,
    };
  }

  // Hotel-generic V33: discoveries with zero deep research are never commercially
  // COMPLETE. LIVE_COMPLETED / empty eligibleLeft must not conceal undeepened queues
  // (observed: preferred_lodging + professional_event_planners deep=0 marked COMPLETE).
  if (unique > 0 && deepCompleted === 0) {
    if (
      input.allCandidatesCommerciallyUnsuitable === true &&
      eligibleLeft <= 0
    ) {
      return {
        version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
        status: RESEARCH_COMPLETENESS_STATUS.NO_ELIGIBLE_DEMAND,
        reason: "all_discovered_candidates_commercially_unsuitable_before_deep",
        technicalExecutionComplete: true,
        commercialResearchComplete: true,
      };
    }
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: RESEARCH_COMPLETENESS_STATUS.PARTIAL_CANDIDATE_CAP,
      reason: "discoveries_exist_but_zero_deep_research_completed",
      eligibleUninvestigatedHighPriority: Math.max(eligibleLeft, unique),
      uniqueCandidates: unique,
      deepCompleted: 0,
      technicalExecutionComplete: true,
      commercialResearchComplete: false,
      preserveResearchQueue: true,
    };
  }

  if (eligibleLeft <= 0 && deepCompleted > 0) {
    if (input.strategiesExhaustedForEligible === true) {
      return {
        version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
        status: RESEARCH_COMPLETENESS_STATUS.RESEARCH_EXHAUSTED,
        reason: "eligible_strategies_exhausted",
        technicalExecutionComplete: true,
        commercialResearchComplete: true,
      };
    }
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: RESEARCH_COMPLETENESS_STATUS.COMPLETE,
      reason: "no_high_priority_eligible_candidates_remain",
      technicalExecutionComplete: true,
      commercialResearchComplete: true,
    };
  }

  // Cap stop with unfinished high-priority work → NEVER COMPLETE
  if (eligibleLeft > 0 && candidateCapHit) {
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: RESEARCH_COMPLETENESS_STATUS.PARTIAL_CANDIDATE_CAP,
      reason: "high_priority_eligible_remain_under_candidate_cap",
      eligibleUninvestigatedHighPriority: eligibleLeft,
      technicalExecutionComplete: true,
      commercialResearchComplete: false,
      preserveResearchQueue: true,
    };
  }

  if (eligibleLeft > 0 && budgetCapHit) {
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: RESEARCH_COMPLETENESS_STATUS.PARTIAL_BUDGET_LIMIT,
      reason: "high_priority_eligible_remain_under_budget_limit",
      eligibleUninvestigatedHighPriority: eligibleLeft,
      budgetRemainingUsd: Number.isFinite(budgetRemaining)
        ? budgetRemaining
        : null,
      technicalExecutionComplete: true,
      commercialResearchComplete: false,
      preserveResearchQueue: true,
    };
  }

  if (eligibleLeft > 0 && providerFailures > 0) {
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: RESEARCH_COMPLETENESS_STATUS.PARTIAL_PROVIDER_FAILURE,
      reason: "provider_failures_left_eligible_uninvestigated",
      eligibleUninvestigatedHighPriority: eligibleLeft,
      providerFailures,
      technicalExecutionComplete: false,
      commercialResearchComplete: false,
      preserveResearchQueue: true,
    };
  }

  if (eligibleLeft > 0) {
    // Default: treat residual unfinished high-priority as cap/budget partial
    // when LIVE_COMPLETED was declared with deep << unique.
    const impliedCap =
      deepCompleted > 0 && deepCompleted < unique && unique - deepCompleted >= eligibleLeft;
    return {
      version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
      status: impliedCap
        ? RESEARCH_COMPLETENESS_STATUS.PARTIAL_CANDIDATE_CAP
        : RESEARCH_COMPLETENESS_STATUS.PARTIAL_BUDGET_LIMIT,
      reason: impliedCap
        ? "live_completed_with_uninvestigated_high_priority_under_implied_cap"
        : "live_completed_with_uninvestigated_high_priority",
      eligibleUninvestigatedHighPriority: eligibleLeft,
      deepCompleted,
      uniqueCandidates: unique,
      technicalExecutionComplete: /LIVE_COMPLETED|SUCCESS/i.test(stop) || stop === "",
      commercialResearchComplete: false,
      preserveResearchQueue: true,
    };
  }

  return {
    version: RESEARCH_COMPLETENESS_STATUS_CONTRACT_V1,
    status: RESEARCH_COMPLETENESS_STATUS.COMPLETE,
    reason: "default_complete",
    technicalExecutionComplete: true,
    commercialResearchComplete: true,
  };
}

export function assertCannotMarkCompleteUnderCap(statusRow = {}) {
  const status = String(statusRow.status || "");
  const eligible = Number(statusRow.eligibleUninvestigatedHighPriority || 0);
  if (
    status === RESEARCH_COMPLETENESS_STATUS.COMPLETE &&
    eligible > 0 &&
    (statusRow.candidateCapHit === true || statusRow.preserveResearchQueue === true)
  ) {
    const err = new Error(
      "COMPLETE_FORBIDDEN_WHILE_HIGH_PRIORITY_ELIGIBLE_REMAIN_UNDER_CAP",
    );
    err.code = "research_completeness_contract_violation";
    throw err;
  }
  return true;
}
