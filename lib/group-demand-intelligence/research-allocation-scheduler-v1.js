/**
 * GDI 2.0 Phase 2F — portfolio-level research allocation scheduler.
 *
 * Experimental policy: 1 action + ≤2 retrievals per eligible opportunity first,
 * reserve capacity for broader cohort coverage, then reallocate unused budget
 * toward high-potential unresolved cases. Early stop when no useful strategy.
 */

import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { planBuyerRecovery } from "./buyer-recovery-planner-v1.js";
import { buildAdaptiveResearchPreflight } from "./adaptive-research-preflight-v1.js";

export const RESEARCH_ALLOCATION_SCHEDULER_V1 = "gdi-research-allocation-scheduler-v1";

export const PHASE2F_ALLOCATION_POLICY = Object.freeze({
  /**
   * RESEARCH_BATCH_LIMIT / SAFETY — wave size, NOT total inventory ceiling.
   * null | Infinity | <=0 → include full cohort; use batchWaveSize for resumable waves.
   */
  maxOpportunities: null,
  /** Preferred resumable wave size when maxOpportunities is unbounded. */
  batchWaveSize: 25,
  maxExternalRetrievalsTotal: 60,
  initialActionsPerOpportunity: 1,
  initialRetrievalsPerOpportunity: 2,
  maxActionsPerOpportunity: 5,
  maxRetrievalsPerOpportunity: 8,
  maxRetrievalsPerHotel: 20,
  reserveFractionForCoverage: 0.4,
  authorizePaidResearch: false,
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function commercialValueScore(opportunity = {}, shared = {}) {
  let score = 0;
  const gaps = shared.gaps || [];
  if (shared.gates?.realisticHotelDemand) score += 8;
  if (shared.gates?.credibleOrganizationActivity) score += 3;
  if (gaps.length === 1) score += 6;
  else if (gaps.length === 2) score += 4;
  else if (gaps.length === 3) score += 2;
  if (/MISSING_NAMED_RELEVANT_BUYER|BUYER_RESPONSIBILITY_UNKNOWN|MISSING_CONTACT_ROUTE|MISSING_HOTEL_DEMAND_THESIS/.test(gaps.join("|"))) {
    score += 2;
  }
  const sources = (opportunity.sources || []).filter((s) => /^https?:\/\//i.test(text(s?.url || s)));
  score += Math.min(3, sources.length);
  return score;
}

function expectedInformationGain(opportunity = {}, shared = {}) {
  const plan = planBuyerRecovery(opportunity, {
    attempts: opportunity.buyerResearchAttempts || [],
    paidResearchAuthorized: false,
  });
  if (!plan.nextApproach) return 0;
  if (
    plan.recommendation === "STOP_WITH_EVIDENCE" ||
    plan.recommendation === "COMMERCIALLY_INELIGIBLE"
  ) {
    return 0;
  }
  const base = Number(plan.nextApproach.expectedInformationGain) || 8;
  const gaps = (shared.gaps || []).length;
  return base + Math.max(0, 4 - gaps);
}

/**
 * Build initial portfolio allocation plan (offline / resumable).
 */
export function buildResearchAllocationPlan(opportunities = [], opts = {}) {
  const policy = { ...PHASE2F_ALLOCATION_POLICY, ...(opts.policy || {}) };
  const list = Array.isArray(opportunities) ? opportunities : [];
  const cursor = Math.max(0, Number(opts.cursor || policy.cursor || 0));
  const unbounded =
    policy.maxOpportunities == null ||
    policy.maxOpportunities === Infinity ||
    Number(policy.maxOpportunities) <= 0;
  const waveSize = unbounded
    ? Math.max(1, Number(policy.batchWaveSize || 25))
    : Math.max(1, Number(policy.maxOpportunities));
  const window = list.slice(cursor, cursor + waveSize);
  const cohort = window.map((opp, index) => {
    const shared = evaluateSharedCommercialEligibility(opp);
    const preflight = buildAdaptiveResearchPreflight(opp, { skipRecovery: true });
    const value = commercialValueScore(opp, shared);
    const gain = expectedInformationGain(opp, shared);
    const usefulStrategy = gain > 0;
    return {
      index,
      id: opp.id,
      hotelId: opp.hotelId || null,
      organizationName: opp.organizationName || null,
      salesReady: shared.salesReady === true,
      gaps: shared.gaps || [],
      commercialValue: value,
      expectedInformationGain: gain,
      usefulStrategy,
      priority: usefulStrategy ? value + gain : 0,
      initialBudget: {
        actions: usefulStrategy && !shared.salesReady ? policy.initialActionsPerOpportunity : 0,
        retrievals: usefulStrategy && !shared.salesReady ? policy.initialRetrievalsPerOpportunity : 0,
      },
      used: { actions: 0, retrievals: 0 },
      status: shared.salesReady
        ? "SKIP_ALREADY_SALES_READY"
        : usefulStrategy
          ? "ELIGIBLE_INITIAL"
          : "SKIP_NO_USEFUL_STRATEGY",
      commercialBlocker: preflight.commercialBlocker || null,
    };
  });

  const eligible = cohort.filter((c) => c.status === "ELIGIBLE_INITIAL");
  const initialReserved = eligible.reduce((n, c) => n + c.initialBudget.retrievals, 0);
  const coverageReserve = Math.floor(
    policy.maxExternalRetrievalsTotal * policy.reserveFractionForCoverage,
  );
  // Cap initial wave so reserve remains for second-pass coverage/reallocation
  let remainingForInitial = Math.max(
    0,
    policy.maxExternalRetrievalsTotal - coverageReserve,
  );
  for (const c of eligible) {
    if (remainingForInitial < c.initialBudget.retrievals) {
      if (remainingForInitial <= 0) {
        c.initialBudget = { actions: 0, retrievals: 0 };
        c.status = "DEFERRED_COVERAGE_RESERVE";
      } else {
        c.initialBudget.retrievals = remainingForInitial;
        c.initialBudget.actions = remainingForInitial > 0 ? 1 : 0;
        remainingForInitial = 0;
      }
    } else {
      remainingForInitial -= c.initialBudget.retrievals;
    }
  }

  const nextCursor = cursor + window.length;
  return {
    version: RESEARCH_ALLOCATION_SCHEDULER_V1,
    policy,
    generatedAt: new Date().toISOString(),
    cohortSize: cohort.length,
    inventoryTotal: list.length,
    cursor,
    waveSize,
    nextCursor: nextCursor < list.length ? nextCursor : null,
    hasMore: nextCursor < list.length,
    eligibleCount: eligible.filter((c) => c.initialBudget.retrievals > 0).length,
    initialRetrievalBudgetAssigned: cohort.reduce((n, c) => n + c.initialBudget.retrievals, 0),
    coverageReserveRetrievals: coverageReserve,
    reallocationPoolEstimate: Math.max(
      0,
      policy.maxExternalRetrievalsTotal -
        cohort.reduce((n, c) => n + c.initialBudget.retrievals, 0),
    ),
    rawInitialDemand: initialReserved,
    entries: cohort.sort((a, b) => b.priority - a.priority),
    resumable: true,
    note:
      "Wave size is a RESEARCH_BATCH_LIMIT / SAFETY control — not a total inventory ceiling. Initial wave prefers breadth (≤2 retrievals/opportunity). Unused capacity reallocates to high-potential unresolved cases. authorizePaidResearch defaults false.",
  };
}

/**
 * Mutable portfolio scheduler state for a live pilot.
 */
export function createAllocationState(plan, opts = {}) {
  const policy = plan.policy || PHASE2F_ALLOCATION_POLICY;
  const byId = new Map();
  for (const e of plan.entries || []) {
    byId.set(e.id, {
      ...e,
      phase: e.initialBudget.retrievals > 0 ? "INITIAL" : "WAITING_REALLOCATION",
      stopReason: null,
      productiveActions: 0,
      candidatesPreserved: [],
    });
  }
  return {
    version: RESEARCH_ALLOCATION_SCHEDULER_V1,
    policy,
    byId,
    hotelUsage: new Map(),
    externalRetrievals: 0,
    phase: "INITIAL",
    deniedPaidAttempts: 0,
    attemptedUrls: [],
    requests: [],
    diagnostics: {
      uniqueUsefulSources: new Set(),
      failedRequests: 0,
      duplicateRequests: 0,
      novelEvidenceFound: 0,
      buyerGapClosures: 0,
      demandThesisGapClosures: 0,
      wastedQuotaAvoided: 0,
    },
    history: [],
    ...(opts.seed || {}),
  };
}

function hotelUsed(state, hotelId) {
  return Number(state.hotelUsage.get(hotelId) || 0);
}

/**
 * Select next opportunity slot under portfolio constraints.
 */
export function selectNextAllocationSlot(state, { preferReallocation = false } = {}) {
  const policy = state.policy;
  if (state.externalRetrievals >= policy.maxExternalRetrievalsTotal) {
    return {
      done: true,
      reason: "overall_retrieval_ceiling",
      resumable: true,
    };
  }

  const entries = [...state.byId.values()].filter((e) => {
    if (e.salesReady) return false;
    if (e.stopReason) return false;
    if (!e.usefulStrategy && e.phase !== "REALLOCATION") return false;
    const hotelCap = hotelUsed(state, e.hotelId) >= policy.maxRetrievalsPerHotel;
    if (hotelCap) return false;
    if (e.used.retrievals >= policy.maxRetrievalsPerOpportunity) return false;
    if (e.used.actions >= policy.maxActionsPerOpportunity) return false;
    return true;
  });

  if (!entries.length) {
    return { done: true, reason: "no_eligible_slots_remaining", resumable: true };
  }

  const remaining = policy.maxExternalRetrievalsTotal - state.externalRetrievals;

  // Phase INITIAL: opportunities with remaining initial budget
  if (!preferReallocation && state.phase === "INITIAL") {
    const initialOpen = entries.filter(
      (e) =>
        e.phase === "INITIAL" &&
        e.used.retrievals < e.initialBudget.retrievals &&
        e.used.actions < Math.max(1, e.initialBudget.actions),
    );
    if (initialOpen.length) {
      initialOpen.sort((a, b) => b.priority - a.priority);
      const pick = initialOpen[0];
      const budgetLeft = pick.initialBudget.retrievals - pick.used.retrievals;
      return {
        done: false,
        opportunityId: pick.id,
        hotelId: pick.hotelId,
        phase: "INITIAL",
        maxRetrievalsThisAction: Math.min(2, budgetLeft, remaining),
        reason: "initial_breadth_allocation",
      };
    }
    state.phase = "REALLOCATION";
  }

  // Reallocation: high potential unresolved with unused overall capacity
  const unresolved = entries
    .filter((e) => e.used.actions >= 1 || e.phase === "WAITING_REALLOCATION" || e.phase === "REALLOCATION")
    .map((e) => {
      const unresolvedGaps = (e.gaps || []).length;
      const gainLeft = e.expectedInformationGain * (1 + (e.productiveActions > 0 ? 0.25 : 0));
      return {
        ...e,
        reallocationScore:
          e.commercialValue + gainLeft + (unresolvedGaps === 1 ? 4 : unresolvedGaps === 2 ? 2 : 0),
      };
    })
    .filter((e) => e.reallocationScore > 0 && e.usefulStrategy)
    .sort((a, b) => b.reallocationScore - a.reallocationScore);

  // Also include never-started deferred coverage
  const deferred = entries
    .filter((e) => e.used.actions === 0)
    .sort((a, b) => b.priority - a.priority);

  const pool = [...deferred, ...unresolved];
  const seen = new Set();
  const ordered = [];
  for (const e of pool) {
    if (seen.has(e.id)) continue;
    seen.add(e.id);
    ordered.push(e);
  }

  if (!ordered.length || remaining <= 0) {
    return { done: true, reason: "no_useful_reallocation_targets", resumable: true };
  }

  const pick = ordered[0];
  pick.phase = "REALLOCATION";
  state.byId.set(pick.id, pick);
  return {
    done: false,
    opportunityId: pick.id,
    hotelId: pick.hotelId,
    phase: "REALLOCATION",
    maxRetrievalsThisAction: Math.min(2, remaining, policy.maxRetrievalsPerOpportunity - pick.used.retrievals),
    reason: pick.used.actions === 0 ? "coverage_reserve_first_touch" : "evidence_based_reallocation",
  };
}

/**
 * Record action outcome and update diagnostics / early-stop.
 */
export function recordAllocationOutcome(state, opportunityId, outcome = {}) {
  const entry = state.byId.get(opportunityId);
  if (!entry) return state;

  const retrievals = Number(outcome.retrievals || 0);
  entry.used.actions += 1;
  entry.used.retrievals += retrievals;
  state.externalRetrievals += retrievals;
  if (entry.hotelId) {
    state.hotelUsage.set(entry.hotelId, hotelUsed(state, entry.hotelId) + retrievals);
  }

  if (outcome.informationFound) entry.productiveActions += 1;
  if (outcome.salesReady) {
    entry.salesReady = true;
    entry.stopReason = "STOP_SALES_READY";
  } else if (outcome.stopReason) {
    entry.stopReason = outcome.stopReason;
    if (/NO_USEFUL|NO_NOVEL|LOW_INFORMATION|EXHAUSTED/i.test(outcome.stopReason)) {
      state.diagnostics.wastedQuotaAvoided += 1;
    }
  }

  if (Array.isArray(outcome.gapsAfter)) entry.gaps = outcome.gapsAfter;
  if (outcome.usefulStrategy === false) entry.usefulStrategy = false;

  if (outcome.failedRequests) state.diagnostics.failedRequests += outcome.failedRequests;
  if (outcome.duplicateRequests) state.diagnostics.duplicateRequests += outcome.duplicateRequests;
  if (outcome.novelEvidenceFound) state.diagnostics.novelEvidenceFound += outcome.novelEvidenceFound;
  if (outcome.buyerGapClosed) state.diagnostics.buyerGapClosures += 1;
  if (outcome.demandGapClosed) state.diagnostics.demandThesisGapClosures += 1;
  for (const u of outcome.usefulSourceUrls || []) {
    state.diagnostics.uniqueUsefulSources.add(u);
  }
  if (outcome.preservedCandidates?.length) {
    entry.candidatesPreserved.push(...outcome.preservedCandidates);
  }

  state.history.push({
    at: new Date().toISOString(),
    opportunityId,
    phase: outcome.phase || entry.phase,
    retrievals,
    informationFound: outcome.informationFound === true,
    stopReason: outcome.stopReason || null,
    nextActionReason: outcome.nextActionReason || null,
  });

  state.byId.set(opportunityId, entry);
  return state;
}

export function summarizeAllocationState(state) {
  const entries = [...state.byId.values()];
  const usefulSources = state.diagnostics.uniqueUsefulSources.size;
  const requests = state.externalRetrievals || 1;
  return {
    version: RESEARCH_ALLOCATION_SCHEDULER_V1,
    phase: state.phase,
    externalRetrievals: state.externalRetrievals,
    remainingRetrievals: Math.max(
      0,
      state.policy.maxExternalRetrievalsTotal - state.externalRetrievals,
    ),
    opportunitiesTouched: entries.filter((e) => e.used.actions > 0).length,
    opportunitiesWithProductiveAction: entries.filter((e) => e.productiveActions > 0).length,
    opportunitiesStillOpen: entries.filter((e) => !e.salesReady && !e.stopReason && e.usefulStrategy).length,
    perOpportunityUsage: entries.map((e) => ({
      id: e.id,
      hotelId: e.hotelId,
      actions: e.used.actions,
      retrievals: e.used.retrievals,
      productiveActions: e.productiveActions,
      stopReason: e.stopReason,
      status: e.salesReady ? "SALES_READY" : e.stopReason || e.phase,
    })),
    diagnostics: {
      uniqueUsefulSources: usefulSources,
      failedRequests: state.diagnostics.failedRequests,
      duplicateRequests: state.diagnostics.duplicateRequests,
      novelEvidenceFound: state.diagnostics.novelEvidenceFound,
      buyerGapClosures: state.diagnostics.buyerGapClosures,
      demandThesisGapClosures: state.diagnostics.demandThesisGapClosures,
      usefulSourcesPerRequest: Number((usefulSources / requests).toFixed(3)),
      costPerEvidenceImprovementUsd: 0,
      wastedQuotaAvoidedStops: state.diagnostics.wastedQuotaAvoided,
    },
    resumable: true,
    deniedPaidAttempts: state.deniedPaidAttempts,
  };
}

/**
 * Offline replay helper: simulate fair allocation over a frozen cohort
 * without network I/O, using prior run retrieval counts as demand.
 */
export function replayAllocationFairness(priorPerOpportunityRetrievals = [], policy = PHASE2F_ALLOCATION_POLICY) {
  const synthetic = priorPerOpportunityRetrievals.map((row, i) => ({
    id: row.id || `opp_${i}`,
    hotelId: row.hotelId || `hotel_${i % 5}`,
    organizationName: row.organizationName || row.id,
    sources: [{ url: "https://example.com" }],
    hotelDemandThesis: "Hotel demand thesis present for allocation replay.",
    researchSegment: "scaling_companies",
    // Force non-sales-ready incomplete profile for scheduling math only
    primaryContact: null,
    contactName: null,
    _priorRetrievals: Number(row.retrievals || 0),
    _priorTouched: Number(row.retrievals || 0) > 0,
  }));

  // Lightweight plan without full eligibility (replay uses prior demand signal)
  const plan = {
    version: RESEARCH_ALLOCATION_SCHEDULER_V1,
    policy,
    entries: synthetic.map((opp, index) => ({
      index,
      id: opp.id,
      hotelId: opp.hotelId,
      organizationName: opp.organizationName,
      salesReady: false,
      gaps: ["MISSING_NAMED_RELEVANT_BUYER"],
      commercialValue: 10,
      expectedInformationGain: 12,
      usefulStrategy: true,
      priority: 22 - index,
      initialBudget: {
        actions: policy.initialActionsPerOpportunity,
        retrievals: policy.initialRetrievalsPerOpportunity,
      },
      used: { actions: 0, retrievals: 0 },
      status: "ELIGIBLE_INITIAL",
    })),
  };

  // Apply reserve cap
  const coverageReserve = Math.floor(
    policy.maxExternalRetrievalsTotal * policy.reserveFractionForCoverage,
  );
  let remaining = policy.maxExternalRetrievalsTotal - coverageReserve;
  for (const e of plan.entries) {
    if (remaining < e.initialBudget.retrievals) {
      e.initialBudget.retrievals = Math.max(0, remaining);
      e.initialBudget.actions = e.initialBudget.retrievals > 0 ? 1 : 0;
      remaining = 0;
    } else {
      remaining -= e.initialBudget.retrievals;
    }
  }

  const state = createAllocationState(plan);
  let guard = 0;
  while (guard++ < 200) {
    const slot = selectNextAllocationSlot(state);
    if (slot.done) break;
    const entry = state.byId.get(slot.opportunityId);
    const take = Math.min(slot.maxRetrievalsThisAction, 2);
    if (take <= 0) {
      recordAllocationOutcome(state, slot.opportunityId, {
        retrievals: 0,
        stopReason: "STOP_NO_USEFUL_STRATEGY",
        usefulStrategy: false,
      });
      continue;
    }
    recordAllocationOutcome(state, slot.opportunityId, {
      retrievals: take,
      informationFound: entry.used.actions === 0,
      phase: slot.phase,
      nextActionReason: slot.reason,
      // stop after 1 reallocation touch for replay demo unless more budget
      stopReason:
        slot.phase === "REALLOCATION" && entry.used.actions >= 1
          ? "STOP_REPLAY_CAP"
          : entry.used.retrievals + take >= policy.initialRetrievalsPerOpportunity &&
              slot.phase === "INITIAL"
            ? null
            : null,
    });
    // After consuming initial budget, mark ready for reallocation consideration
    const updated = state.byId.get(slot.opportunityId);
    if (
      updated.phase === "INITIAL" &&
      updated.used.retrievals >= updated.initialBudget.retrievals
    ) {
      updated.phase = "WAITING_REALLOCATION";
      state.byId.set(slot.opportunityId, updated);
    }
  }

  const summary = summarizeAllocationState(state);
  const touched = summary.opportunitiesTouched;
  const priorTouched = priorPerOpportunityRetrievals.filter((r) => Number(r.retrievals) > 0).length;
  return {
    version: RESEARCH_ALLOCATION_SCHEDULER_V1,
    mode: "offline_replay",
    priorOpportunitiesTouched: priorTouched,
    replayOpportunitiesTouched: touched,
    coverageImproved: touched > priorTouched,
    summary,
    comparisonNote:
      "Replay demonstrates breadth-first allocation vs Phase 2E monopoly of early cohort members.",
  };
}
