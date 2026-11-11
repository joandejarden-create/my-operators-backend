/**
 * Bridge V2 commercial-research advisory context into TypeSafe System One state.
 *
 * V2 offline policy alone does NOT reach the model. This module maps persisted
 * advisory fields into ALLOWED sanitizeJevState keys (hardPolicyContext,
 * evidenceSummary, costSoFarUsd, missingFields, priorRunResults, etc.).
 *
 * Advisory only — never Sales Ready authority.
 */
import {
  ACTION_ATTEMPT_STATUS,
  DEMAND_TIMING_CLASS,
  normalizeAdvisoryContextV2,
} from "./jev-commercial-research-decision-v2.js";

/**
 * Map normalized V2 context → Jev decide() context fields that survive sanitizeJevState.
 */
export function bridgeAdvisoryContextToSystemOneState(rawAdvisory = {}, baseContext = {}) {
  const ctx = normalizeAdvisoryContextV2(rawAdvisory);
  const hard = {
    ...(baseContext.hardPolicyContext && typeof baseContext.hardPolicyContext === "object"
      ? baseContext.hardPolicyContext
      : {}),
    advisoryOnly: true,
    notSalesReadyAuthority: true,
    demandTimingClass: ctx.demandTimingClass,
    deepResearchCompleted: ctx.deepResearchCompleted,
    recommendedActionAttemptStatus: ctx.recommendedActionAttemptStatus,
    recurringAccountPotential: ctx.recurringAccountPotential,
    publicResearchCanResolveBlockers:
      ctx.publicResearchCanRealisticallyResolveBlockers === true
        ? "YES"
        : ctx.publicResearchCanRealisticallyResolveBlockers === false
          ? "NO"
          : "UNKNOWN",
    lifetimeResearchCostUsd: ctx.lifetimeResearchCostUsd,
    remainingAuthorizedBudgetUsd:
      ctx.remainingAuthorizedBudgetUsd == null
        ? "UNBOUNDED_FOR_RUN"
        : ctx.remainingAuthorizedBudgetUsd,
    outstandingBlockerCount: ctx.outstandingBlockers.length,
    recommendedNextAction: ctx.recommendedNextAction || "NONE",
    HISTORICAL_DEMAND: ctx.demandTimingClass === DEMAND_TIMING_CLASS.HISTORICAL,
    FUTURE_DEMAND: ctx.demandTimingClass === DEMAND_TIMING_CLASS.FUTURE,
    RECURRING_ACCOUNT_POTENTIAL: ctx.recurringAccountPotential === true,
    DEEP_RESEARCH_COMPLETED: ctx.deepResearchCompleted === true,
    ACTION_ALREADY_ATTEMPTED:
      ctx.recommendedActionAttemptStatus ===
        ACTION_ATTEMPT_STATUS.ATTEMPTED_SOURCE_PRESENT ||
      ctx.recommendedActionAttemptStatus === ACTION_ATTEMPT_STATUS.EXHAUSTED ||
      ctx.recommendedActionAttemptStatus ===
        ACTION_ATTEMPT_STATUS.ATTEMPTED_PARTIAL,
    ACTION_SOURCE_ALREADY_PRESENT:
      ctx.recommendedActionAttemptStatus ===
      ACTION_ATTEMPT_STATUS.ATTEMPTED_SOURCE_PRESENT,
    PUBLIC_PATH_EXHAUSTED:
      ctx.publicResearchCanRealisticallyResolveBlockers === false &&
      ctx.deepResearchCompleted === true,
  };

  const timingBit = `timing=${ctx.demandTimingClass}`;
  const deepBit = `deep=${ctx.deepResearchCompleted ? "done" : "open"}`;
  const attemptBit = `nextActionAttempt=${ctx.recommendedActionAttemptStatus}`;
  const recurBit = `recurringAccount=${ctx.recurringAccountPotential ? "yes" : "no"}`;
  const budgetBit =
    ctx.remainingAuthorizedBudgetUsd == null
      ? `lifetimeCostUsd=${ctx.lifetimeResearchCostUsd}`
      : `lifetimeCostUsd=${ctx.lifetimeResearchCostUsd};remainingBudgetUsd=${ctx.remainingAuthorizedBudgetUsd}`;

  const bridgeSummary = [
    timingBit,
    deepBit,
    attemptBit,
    recurBit,
    budgetBit,
    ctx.recommendedNextAction
      ? `proposedAction=${ctx.recommendedNextAction}`
      : null,
    ctx.outstandingBlockers.length
      ? `blockers=${ctx.outstandingBlockers.slice(0, 4).join(",")}`
      : null,
  ]
    .filter(Boolean)
    .join("; ")
    .slice(0, 600);

  const priorBase = Array.isArray(baseContext.priorRunResults)
    ? baseContext.priorRunResults
    : [];
  const priorFromStrategies = (ctx.priorStrategies || []).map(
    (s) => `${s.strategy}:${s.outcome}`,
  );

  return {
    ...baseContext,
    futureDateStatus:
      baseContext.futureDateStatus ||
      (ctx.demandTimingClass === DEMAND_TIMING_CLASS.FUTURE
        ? "FUTURE"
        : ctx.demandTimingClass === DEMAND_TIMING_CLASS.HISTORICAL
          ? "PAST"
          : ctx.demandTimingClass === DEMAND_TIMING_CLASS.RECURRING
            ? "RECURRING"
            : "UNKNOWN"),
    eventForwardness:
      baseContext.eventForwardness ||
      (ctx.demandTimingClass === DEMAND_TIMING_CLASS.FUTURE
        ? "FORWARD"
        : ctx.demandTimingClass === DEMAND_TIMING_CLASS.HISTORICAL
          ? "HISTORICAL"
          : "AMBIGUOUS"),
    costSoFarUsd:
      baseContext.costSoFarUsd != null
        ? baseContext.costSoFarUsd
        : ctx.lifetimeResearchCostUsd,
    missingFields: [
      ...(Array.isArray(baseContext.missingFields) ? baseContext.missingFields : []),
      ...ctx.outstandingBlockers,
    ].slice(0, 20),
    priorRunResults: [...priorBase, ...priorFromStrategies].slice(0, 20),
    queriesAlreadyRun:
      baseContext.queriesAlreadyRun != null
        ? baseContext.queriesAlreadyRun
        : ctx.sourcesAlreadyInvestigated.length,
    previousFailures: [
      ...(Array.isArray(baseContext.previousFailures)
        ? baseContext.previousFailures
        : []),
      ...(ctx.recommendedActionAttemptStatus === ACTION_ATTEMPT_STATUS.EXHAUSTED
        ? ["recommended_action_exhausted"]
        : []),
      ...(ctx.recommendedActionAttemptStatus ===
      ACTION_ATTEMPT_STATUS.ATTEMPTED_SOURCE_PRESENT
        ? ["recommended_action_source_already_present"]
        : []),
    ].slice(0, 10),
    hardPolicyContext: hard,
    evidenceSummary: [
      bridgeSummary,
      baseContext.evidenceSummary ? String(baseContext.evidenceSummary) : "",
    ]
      .filter(Boolean)
      .join(" | ")
      .slice(0, 600),
    evidenceSnippets: [
      ...(Array.isArray(baseContext.evidenceSnippets)
        ? baseContext.evidenceSnippets
        : []),
      ctx.opportunityDateLabel,
      ctx.recurringAccountEvidence,
      ...(ctx.sourcesAlreadyInvestigated || []).slice(0, 2),
    ]
      .map((s) => String(s || "").slice(0, 280))
      .filter(Boolean)
      .slice(0, 5),
    _bridgeMeta: {
      version: "jev-system-one-context-bridge-v2",
      fieldsInjected: Object.keys(hard).length,
      demandTimingClass: ctx.demandTimingClass,
      deepResearchCompleted: ctx.deepResearchCompleted,
      recommendedActionAttemptStatus: ctx.recommendedActionAttemptStatus,
    },
  };
}

/**
 * Verify bridged state would survive sanitizeJevState (structural check only).
 */
export function assertBridgeSurvivesSanitize(bridged, sanitizeFn) {
  const cleaned = sanitizeFn(bridged);
  const h = cleaned.hardPolicyContext || {};
  const required = [
    "demandTimingClass",
    "deepResearchCompleted",
    "recommendedActionAttemptStatus",
    "DEEP_RESEARCH_COMPLETED",
    "notSalesReadyAuthority",
  ];
  const missing = required.filter((k) => h[k] === undefined);
  return {
    ok: missing.length === 0 && Boolean(cleaned.evidenceSummary),
    missingHardPolicyKeys: missing,
    hasEvidenceSummary: Boolean(cleaned.evidenceSummary),
    hardPolicyKeyCount: Object.keys(h).length,
    sampleHardPolicy: {
      demandTimingClass: h.demandTimingClass ?? null,
      deepResearchCompleted: h.deepResearchCompleted ?? null,
      recommendedActionAttemptStatus: h.recommendedActionAttemptStatus ?? null,
      HISTORICAL_DEMAND: h.HISTORICAL_DEMAND ?? null,
      ACTION_SOURCE_ALREADY_PRESENT: h.ACTION_SOURCE_ALREADY_PRESENT ?? null,
    },
  };
}
