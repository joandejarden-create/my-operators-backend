/**
 * Waterstone Need-Date Deep Discovery V6 — full campaign plan builder (zero paid).
 */

import {
  NEED_DATE_DEEP_DISCOVERY_V6,
  NEED_DATE_V6_WAVE,
  NEED_DATE_V6_WAVE_META,
  NEED_DATE_V6_SUCCESS,
  NEED_DATE_V6_QUALIFIED_STANDARDS,
  WATERSTONE_V6_HOTEL,
} from "./need-date-deep-discovery-v6-contract.js";
import { buildWaterstoneFourteenWorkflowCoveragePlan } from "./need-date-deep-discovery-coverage-v6.js";
import { buildNeedDateDualTrackResearchPlan } from "./need-date-research-track-planning-v1.js";
import { buildResearchDepthAllocationV6 } from "./need-date-research-depth-allocation-v6.js";
import { NEED_DATE_V6_WORKFLOW_PRIORITY } from "./need-date-deep-discovery-v6-contract.js";

export const NEED_DATE_V6_BUDGET_SCENARIOS = Object.freeze({
  FOCUSED_COMPLETION: {
    id: "FOCUSED_COMPLETION",
    label: "Focused completion (within original leftover)",
    aggregateUsd: 11.1,
    note: "Uses only remaining V4 leftover ceilings conceptually; still requires NEW founder GO — leftover is not auto-spend.",
    monthUsd: [3.7, 3.7, 3.7],
  },
  COMMERCIAL_DEPTH: {
    id: "COMMERCIAL_DEPTH",
    label: "Commercial depth (recommended)",
    aggregateUsd: 36,
    note: "Recommended for meaningful PRIMARY depth across weddings, meetings, accounts, catering, planners + adaptive follow-up. New authorization required (separate from $18 V4).",
    monthUsd: [12, 12, 12],
  },
  COMPREHENSIVE: {
    id: "COMPREHENSIVE",
    label: "Comprehensive multi-wave",
    aggregateUsd: 54,
    note: "Broader SECONDARY/CONDITIONAL coverage + more Wave E blocker resolution. New authorization required.",
    monthUsd: [18, 18, 18],
  },
});

function buildWaves() {
  return Object.values(NEED_DATE_V6_WAVE).map((wave) => ({
    wave,
    ...NEED_DATE_V6_WAVE_META[wave],
    status: NEED_DATE_V6_WAVE_META[wave].paid
      ? "BLOCKED_UNTIL_FOUNDER_GO"
      : "READY_ZERO_PAID",
  }));
}

function weddingResearchDesign() {
  return {
    mandatory: true,
    powershell: "Invoke-GdiSocialSpecialEvents.ps1",
    segmentId: "social_special_events",
    priorStatus: "NOT_RESEARCHED",
    notSubstitutes: [
      "corporate_celebrations discovery",
      "professional_event_planners discovery alone",
      "preferred_lodging alone",
    ],
    investigate: [
      "Destination wedding programmes (Boca / Palm Beach)",
      "Wedding planners producing local lodging demand",
      "Confirmed future weddings where legitimately public",
      "Wedding groups with potential hotel blocks",
      "Rehearsal dinners and welcome receptions",
      "Family reunions and social group travel",
      "Off-site wedding venues lacking accommodation",
      "Wedding venue and lodging partnerships",
      "Group catering / private-event adjacent social demand",
    ],
    reject: [
      "Generic wedding directories",
      "Venue existence without lodging demand",
      "Planner website without future demand evidence",
      "Competing-hotel wedding calendar as sales-ready overflow",
    ],
    distinguish: {
      dateSpecific: "Bookings/programmes with timing → verified need-month path",
      relationship: "Planner/venue partnerships → Research Continuation until timing adequate",
    },
  };
}

function buyerFirstSearchStrategy() {
  return {
    order: [
      "Demand-generating organization",
      "Relevant programme or event",
      "Commercial decision-maker",
      "Timing and demand mechanism",
      "Hotel sourcing requirements",
      "THEN existing hotels / competing venues / lodging arrangements / Waterstone accessibility",
    ],
    forbid: [
      "Primary discovery via competing hotel event calendars",
      "Default speculative overflow thesis",
      "Unrelated executives as contact filler",
      "Surfe during research waves",
    ],
  };
}

function priorFailureAnalysis() {
  return {
    spentUsd: WATERSTONE_V6_HOTEL.priorCampaignSpentUsd,
    stagedCompetitorAssociated: WATERSTONE_V6_HOTEL.priorStagedCompetitorAssociated,
    independentlyVerifiedDirectAddressable: 0,
    weddingResearch: WATERSTONE_V6_HOTEL.weddingResearchStatus,
    failures: [
      "Shallow diversified discovery — association deep monopolized spend ($4.20 of $6.90)",
      "Competitor-hosted events advanced with speculative overflow next steps",
      "PRIMARY categories (scaling, catering, celebrations, planners) received discovery-only or none",
      "Weddings / Social Special Events: NOT RESEARCHED",
      "Sep 2027 staged qualified yield: 0",
      "Relevance scoring boosted 'overflow demand' language",
      "5–10 target treated too much like cheap calendar scraping rather than commercial depth",
    ],
    structuralCorrectionsInV6: [
      "PRIMARY wedding discovery every need month",
      "Demand-owner-first query order",
      "Commercial accessibility gate with Watch-only speculative overflow",
      "Depth allocation diversity (association deep capped)",
      "Dual tracks: date-specific vs account/relationship",
      "Blocker ledger + adaptive Wave E follow-ups",
      "Budget scenarios sized to commercial depth — not only leftover $11.10",
    ],
  };
}

/**
 * Build complete V6 plan for Waterstone (zero paid).
 */
export function buildWaterstoneDeepDiscoveryPlanV6(opts = {}) {
  const coverage = buildWaterstoneFourteenWorkflowCoveragePlan({
    hotelId: opts.hotelId || WATERSTONE_V6_HOTEL.hotelId,
    needMonths: opts.needMonths || WATERSTONE_V6_HOTEL.needMonths,
  });

  const tracks = buildNeedDateDualTrackResearchPlan({
    strategies: coverage.workflows.map((w) => ({
      strategyId: w.strategyId,
      workflowNumber: w.workflowNumber,
      powershell: w.powershell,
      relevance: w.priority,
      pursueInContinuation: w.priority !== NEED_DATE_V6_WORKFLOW_PRIORITY.NOT_APPLICABLE,
    })),
    includeWeddingSocial: true,
  });

  const scenarios = {};
  for (const [key, sc] of Object.entries(NEED_DATE_V6_BUDGET_SCENARIOS)) {
    scenarios[key] = {
      ...sc,
      depthAllocation: buildResearchDepthAllocationV6(coverage, {
        scenarioBudgetUsd: sc.aggregateUsd,
        perMonthBudgetUsd: sc.monthUsd,
      }),
    };
  }

  const recommended = scenarios.COMMERCIAL_DEPTH;

  return {
    version: NEED_DATE_DEEP_DISCOVERY_V6,
    hotel: { ...WATERSTONE_V6_HOTEL },
    successMeasure: { ...NEED_DATE_V6_SUCCESS },
    qualifiedStandards: [...NEED_DATE_V6_QUALIFIED_STANDARDS],
    priorFailureAnalysis: priorFailureAnalysis(),
    coverage,
    researchTracks: tracks,
    waves: buildWaves(),
    weddingResearchDesign: weddingResearchDesign(),
    buyerFirstSearchStrategy: buyerFirstSearchStrategy(),
    budgetScenarios: scenarios,
    recommendedScenarioId: "COMMERCIAL_DEPTH",
    recommendedScenario: {
      id: recommended.id,
      label: recommended.label,
      aggregateUsd: recommended.aggregateUsd,
      plannedSpendUsd: recommended.depthAllocation.plannedAggregateUsd,
      diversityCheck: recommended.depthAllocation.diversityCheck,
      monthPlans: recommended.depthAllocation.monthPlans,
      note: recommended.note,
    },
    stopConditions: [
      "Evidence-backed strategies for PRIMARY/SECONDARY pathways are exhausted",
      "Commercial marginal yield becomes poor",
      "Authorized budget boundary reached",
      "Further searches would only refine speculative overflow / competitor-locked theses",
      "Hitting 5–10 would require lowering qualification standards",
      "Founder has not authorized paid waves",
    ],
    deliverables: [
      "Coverage plan with all 14 workflows classified",
      "Wave A inventory match report",
      "Discovery + deep run ledger",
      "Blocker ledgers per candidate",
      "Staged opportunities with full V2 CIB / ten-section drawer (when publication later authorized)",
      "Month-specific yield vs 5–10 target",
      "Independent certification gate matrix",
    ],
    paidExecutionAuthorized: false,
    publicationAuthorized: false,
    note: "This plan does NOT authorize Exa, Surfe, Firecrawl or publication. Founder must approve a budget scenario explicitly.",
  };
}
