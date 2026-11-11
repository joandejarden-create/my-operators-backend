/**
 * GDI–Jev V2 — Commercial Research Decision Intelligence (advisory only).
 *
 * Deterministic policy layer over enriched research context + optional native
 * Jev routing answers. Does NOT promote to Sales Ready. Does NOT delete
 * research-continuation inventory.
 *
 * Zero paid calls by itself — consumes persisted evidence / prior Jev outputs.
 */

export const JEV_COMMERCIAL_RESEARCH_DECISION_V2 =
  "gdi-jev-commercial-research-decision-v2";

export const DEMAND_TIMING_CLASS = Object.freeze({
  FUTURE: "FUTURE",
  RECURRING: "RECURRING",
  HISTORICAL: "HISTORICAL",
  UNDETERMINED: "UNDETERMINED",
});

export const ACTION_ATTEMPT_STATUS = Object.freeze({
  NEVER_INVESTIGATED: "NEVER_INVESTIGATED",
  ATTEMPTED_PARTIAL: "ATTEMPTED_PARTIAL",
  ATTEMPTED_STILL_OPEN: "ATTEMPTED_STILL_OPEN",
  ATTEMPTED_SOURCE_PRESENT: "ATTEMPTED_SOURCE_PRESENT",
  EXHAUSTED: "EXHAUSTED",
  UNKNOWN: "UNKNOWN",
});

/** Advisory investigation posture (not Sales Ready). */
export const RESEARCH_POSTURE_V2 = Object.freeze({
  CONTINUE_RESEARCH: "CONTINUE_RESEARCH",
  WATCH: "WATCH",
  STOP_RESEARCH: "STOP_RESEARCH",
});

/**
 * Permanent discovery disposition is separate from stopping the current wave.
 * STOP_RESEARCH ≠ REJECT_DISCOVERY.
 */
export const DISCOVERY_RETENTION_V2 = Object.freeze({
  KEEP_RESEARCH_CONTINUATION: "KEEP_RESEARCH_CONTINUATION",
  KEEP_WATCH: "KEEP_WATCH",
  MARK_EXHAUSTED_PUBLIC_PATH: "MARK_EXHAUSTED_PUBLIC_PATH",
  // Never auto-delete promising RC candidates.
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function num(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
}

/**
 * Normalize / validate advisory context. Missing fields stay explicit null/false.
 */
export function normalizeAdvisoryContextV2(raw = {}) {
  const timing = String(raw.demandTimingClass || DEMAND_TIMING_CLASS.UNDETERMINED);
  const attempt = String(
    raw.recommendedActionAttemptStatus || ACTION_ATTEMPT_STATUS.UNKNOWN,
  );
  return {
    version: JEV_COMMERCIAL_RESEARCH_DECISION_V2,
    candidateId: text(raw.candidateId) || null,
    hotelId: text(raw.hotelId) || null,
    hotelName: text(raw.hotelName) || null,
    hotelCity: text(raw.hotelCity) || null,
    hotelCountry: text(raw.hotelCountry) || null,
    hotelPositioning: text(raw.hotelPositioning) || null,
    hotelCapabilities: Array.isArray(raw.hotelCapabilities)
      ? raw.hotelCapabilities.map(text).filter(Boolean)
      : [],
    segmentId: text(raw.segmentId) || null,
    title: text(raw.title) || null,
    organizationName: text(raw.organizationName) || null,
    opportunityDateLabel: text(raw.opportunityDateLabel) || null,
    demandTimingClass: Object.values(DEMAND_TIMING_CLASS).includes(timing)
      ? timing
      : DEMAND_TIMING_CLASS.UNDETERMINED,
    recurringAccountPotential: raw.recurringAccountPotential === true,
    recurringAccountEvidence: text(raw.recurringAccountEvidence) || null,
    sourcesAlreadyInvestigated: Array.isArray(raw.sourcesAlreadyInvestigated)
      ? [...new Set(raw.sourcesAlreadyInvestigated.map(text).filter(Boolean))]
      : [],
    priorStrategies: Array.isArray(raw.priorStrategies)
      ? raw.priorStrategies.map((s) => ({
          strategy: text(s?.strategy || s?.id || s),
          outcome: text(s?.outcome || "UNKNOWN"),
        }))
      : [],
    deepResearchCompleted: raw.deepResearchCompleted === true,
    outstandingBlockers: Array.isArray(raw.outstandingBlockers)
      ? raw.outstandingBlockers.map(text).filter(Boolean)
      : [],
    recommendedNextAction: text(raw.recommendedNextAction) || null,
    recommendedActionAttemptStatus: Object.values(ACTION_ATTEMPT_STATUS).includes(
      attempt,
    )
      ? attempt
      : ACTION_ATTEMPT_STATUS.UNKNOWN,
    lifetimeResearchCostUsd: num(raw.lifetimeResearchCostUsd, 0),
    remainingAuthorizedBudgetUsd:
      raw.remainingAuthorizedBudgetUsd == null
        ? null
        : num(raw.remainingAuthorizedBudgetUsd, 0),
    publicResearchCanRealisticallyResolveBlockers:
      raw.publicResearchCanRealisticallyResolveBlockers === true
        ? true
        : raw.publicResearchCanRealisticallyResolveBlockers === false
          ? false
          : null,
    commercialFitInadequate: raw.commercialFitInadequate === true,
    nativeJevPrequal: text(raw.nativeJevPrequal) || null,
    nativeJevStopContinue: text(raw.nativeJevStopContinue) || null,
    provenanceGaps: Array.isArray(raw.provenanceGaps)
      ? raw.provenanceGaps.map(text).filter(Boolean)
      : [],
  };
}

function actionIsDuplicate(ctx) {
  return (
    ctx.recommendedActionAttemptStatus ===
      ACTION_ATTEMPT_STATUS.ATTEMPTED_SOURCE_PRESENT ||
    ctx.recommendedActionAttemptStatus === ACTION_ATTEMPT_STATUS.EXHAUSTED
  );
}

function actionUnattemptedOrOpen(ctx) {
  return (
    ctx.recommendedActionAttemptStatus ===
      ACTION_ATTEMPT_STATUS.NEVER_INVESTIGATED ||
    ctx.recommendedActionAttemptStatus ===
      ACTION_ATTEMPT_STATUS.ATTEMPTED_STILL_OPEN
  );
}

function futureOrRecurring(ctx) {
  return (
    ctx.demandTimingClass === DEMAND_TIMING_CLASS.FUTURE ||
    ctx.demandTimingClass === DEMAND_TIMING_CLASS.RECURRING ||
    ctx.recurringAccountPotential === true
  );
}

function historicalNonRecurring(ctx) {
  return (
    ctx.demandTimingClass === DEMAND_TIMING_CLASS.HISTORICAL &&
    ctx.recurringAccountPotential !== true
  );
}

function budgetExhausted(ctx) {
  if (ctx.remainingAuthorizedBudgetUsd == null) return false;
  return ctx.remainingAuthorizedBudgetUsd <= 0;
}

/**
 * Core V2 advisory decision.
 * @returns {{
 *   posture: string,
 *   discoveryRetention: string,
 *   reasons: string[],
 *   duplicateResearchAvoided: boolean,
 *   nextActionAllowed: boolean,
 *   recommendedNextAction: string|null,
 *   eligibilityNote: string,
 *   requiresFreshJevCall: boolean,
 * }}
 */
export function decideCommercialResearchPostureV2(rawContext = {}) {
  const ctx = normalizeAdvisoryContextV2(rawContext);
  const reasons = [];
  let posture = null;
  let discoveryRetention =
    DISCOVERY_RETENTION_V2.KEEP_RESEARCH_CONTINUATION;
  let duplicateResearchAvoided = false;
  let nextActionAllowed = false;

  // --- STOP_RESEARCH (current investigation) ---
  if (budgetExhausted(ctx)) {
    posture = RESEARCH_POSTURE_V2.STOP_RESEARCH;
    reasons.push("REMAINING_AUTHORIZED_BUDGET_EXHAUSTED");
  } else if (historicalNonRecurring(ctx)) {
    posture = RESEARCH_POSTURE_V2.STOP_RESEARCH;
    discoveryRetention = DISCOVERY_RETENTION_V2.KEEP_WATCH;
    reasons.push("HISTORICAL_NONRECURRING_DEMAND");
  } else if (ctx.commercialFitInadequate) {
    posture = RESEARCH_POSTURE_V2.STOP_RESEARCH;
    discoveryRetention = DISCOVERY_RETENTION_V2.MARK_EXHAUSTED_PUBLIC_PATH;
    reasons.push("COMMERCIAL_FIT_DEMONSTRABLY_INADEQUATE");
  } else if (actionIsDuplicate(ctx) && ctx.deepResearchCompleted) {
    posture = RESEARCH_POSTURE_V2.STOP_RESEARCH;
    duplicateResearchAvoided = true;
    discoveryRetention = futureOrRecurring(ctx)
      ? DISCOVERY_RETENTION_V2.KEEP_WATCH
      : DISCOVERY_RETENTION_V2.MARK_EXHAUSTED_PUBLIC_PATH;
    reasons.push("RECOMMENDED_ACTION_DUPLICATES_COMPLETED_RESEARCH");
  } else if (
    ctx.publicResearchCanRealisticallyResolveBlockers === false &&
    ctx.deepResearchCompleted
  ) {
    posture = RESEARCH_POSTURE_V2.STOP_RESEARCH;
    discoveryRetention = futureOrRecurring(ctx)
      ? DISCOVERY_RETENTION_V2.KEEP_WATCH
      : DISCOVERY_RETENTION_V2.MARK_EXHAUSTED_PUBLIC_PATH;
    reasons.push("NO_REALISTIC_REMAINING_PUBLIC_RESEARCH_PATH");
  } else if (
    ctx.recommendedActionAttemptStatus === ACTION_ATTEMPT_STATUS.EXHAUSTED
  ) {
    posture = RESEARCH_POSTURE_V2.STOP_RESEARCH;
    discoveryRetention = DISCOVERY_RETENTION_V2.MARK_EXHAUSTED_PUBLIC_PATH;
    reasons.push("RELEVANT_STRATEGIES_EXHAUSTED");
  }

  // --- WATCH ---
  if (!posture) {
    const nativeWatch = ctx.nativeJevPrequal === "LIKELY_WATCH";
    const futureOrRecurringNotActionable =
      futureOrRecurring(ctx) &&
      ctx.deepResearchCompleted &&
      !actionUnattemptedOrOpen(ctx);
    const historicalRecurringAccount =
      ctx.demandTimingClass === DEMAND_TIMING_CLASS.HISTORICAL &&
      ctx.recurringAccountPotential === true;
    const partialMonitorPreferred =
      ctx.deepResearchCompleted &&
      ctx.recommendedActionAttemptStatus ===
        ACTION_ATTEMPT_STATUS.ATTEMPTED_PARTIAL &&
      ctx.publicResearchCanRealisticallyResolveBlockers !== true;

    const watchSignals =
      nativeWatch ||
      futureOrRecurringNotActionable ||
      historicalRecurringAccount ||
      partialMonitorPreferred;

    if (watchSignals && !actionUnattemptedOrOpen(ctx)) {
      posture = RESEARCH_POSTURE_V2.WATCH;
      discoveryRetention = DISCOVERY_RETENTION_V2.KEEP_WATCH;
      if (futureOrRecurringNotActionable || historicalRecurringAccount) {
        reasons.push("CREDIBLE_FUTURE_OR_RECURRING_BUT_NOT_ACTIONABLE_NOW");
      }
      if (nativeWatch) reasons.push("NATIVE_JEV_PREQUAL_LIKELY_WATCH");
      if (partialMonitorPreferred) {
        reasons.push("PARTIAL_PRIOR_RESEARCH_MONITOR_OVER_IMMEDIATE_DEEP");
      }
      if (
        !futureOrRecurringNotActionable &&
        !historicalRecurringAccount &&
        !nativeWatch &&
        partialMonitorPreferred
      ) {
        reasons.push("NO_SUFFICIENTLY_ACTIONABLE_DEMAND_MONITOR_OVER_DEEP");
      }
    }
  }

  // --- CONTINUE_RESEARCH ---
  if (!posture) {
    const specificUnattempted =
      Boolean(ctx.recommendedNextAction) && actionUnattemptedOrOpen(ctx);
    const plausibleFuture =
      futureOrRecurring(ctx) ||
      ctx.demandTimingClass === DEMAND_TIMING_CLASS.UNDETERMINED;
    const valueJustified =
      ctx.publicResearchCanRealisticallyResolveBlockers !== false &&
      !budgetExhausted(ctx);

    if (specificUnattempted && plausibleFuture && valueJustified) {
      posture = RESEARCH_POSTURE_V2.CONTINUE_RESEARCH;
      nextActionAllowed = true;
      reasons.push("SPECIFIC_UNATTEMPTED_OR_OPEN_STRATEGY");
      if (plausibleFuture) reasons.push("PLAUSIBLE_FUTURE_OR_UNDETERMINED_RELEVANCE");
      if (valueJustified) reasons.push("EXPECTED_VALUE_JUSTIFIES_BOUNDED_EFFORT");
    } else if (
      specificUnattempted &&
      ctx.demandTimingClass === DEMAND_TIMING_CLASS.UNDETERMINED &&
      valueJustified
    ) {
      posture = RESEARCH_POSTURE_V2.CONTINUE_RESEARCH;
      nextActionAllowed = true;
      reasons.push("OPEN_ORGANIZER_OR_EVENT_GAP_WITH_UNDETERMINED_TIMING");
    } else {
      // Default soft watch — never invent continue; never delete RC
      posture = RESEARCH_POSTURE_V2.WATCH;
      discoveryRetention = DISCOVERY_RETENTION_V2.KEEP_WATCH;
      reasons.push("DEFAULT_WATCH_INSUFFICIENT_CONTINUE_CRITERIA");
    }
  }

  return {
    version: JEV_COMMERCIAL_RESEARCH_DECISION_V2,
    posture,
    discoveryRetention,
    reasons,
    duplicateResearchAvoided,
    nextActionAllowed,
    recommendedNextAction: nextActionAllowed ? ctx.recommendedNextAction : null,
    withheldNextAction: nextActionAllowed ? null : ctx.recommendedNextAction,
    eligibilityNote:
      "Advisory only. CONFIRMED_DEMAND or RELIABLE_INFERRED_DEMAND may qualify under GDI gates with named commercial entry point; SPECULATIVE_DEMAND stays research continuation. Jev cannot promote to Sales Ready.",
    requiresFreshJevCall: false,
    contextSnapshot: {
      demandTimingClass: ctx.demandTimingClass,
      deepResearchCompleted: ctx.deepResearchCompleted,
      recommendedActionAttemptStatus: ctx.recommendedActionAttemptStatus,
      recurringAccountPotential: ctx.recurringAccountPotential,
      outstandingBlockerCount: ctx.outstandingBlockers.length,
      lifetimeResearchCostUsd: ctx.lifetimeResearchCostUsd,
      remainingAuthorizedBudgetUsd: ctx.remainingAuthorizedBudgetUsd,
    },
  };
}

/**
 * Map V2 posture to benchmark-style advisory decision labels (still not Sales Ready).
 */
export function mapPostureToBenchmarkDecisionV2(posture) {
  if (posture === RESEARCH_POSTURE_V2.CONTINUE_RESEARCH) {
    return "RESEARCH_CONTINUATION";
  }
  if (posture === RESEARCH_POSTURE_V2.WATCH) return "WATCH";
  if (posture === RESEARCH_POSTURE_V2.STOP_RESEARCH) return "WATCH";
  return "RESEARCH_CONTINUATION";
}
