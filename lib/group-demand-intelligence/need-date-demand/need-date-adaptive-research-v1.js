/**
 * Adaptive Need-Date research waves (PowerShell #15 V5).
 * Planning + zero-paid orchestration only — never auto-launches paid Exa.
 */

import { evaluateFourteenStrategiesForNeedDateCampaign } from "./need-date-fourteen-strategy-relevance-v1.js";
import { selectStrategiesForCampaign } from "./need-period-strategy-selection-v1.js";
import { buildNeedDateDualTrackResearchPlan } from "./need-date-research-track-planning-v1.js";

export const NEED_DATE_RESEARCH_WAVE = Object.freeze({
  WAVE_1_EXISTING_INVENTORY: "WAVE_1_EXISTING_INVENTORY",
  WAVE_2_TARGETED_DISCOVERY: "WAVE_2_TARGETED_DISCOVERY",
  WAVE_3_COMMERCIAL_QUALIFICATION: "WAVE_3_COMMERCIAL_QUALIFICATION",
  WAVE_4_GAP_ANALYSIS: "WAVE_4_GAP_ANALYSIS",
  WAVE_5_TARGETED_FOLLOW_UP: "WAVE_5_TARGETED_FOLLOW_UP",
  WAVE_6_FINAL_QUALIFICATION: "WAVE_6_FINAL_QUALIFICATION",
});

/**
 * Build adaptive research plan without spending.
 */
export function buildAdaptiveNeedDateResearchPlan({
  hotelId,
  hotel = {},
  needPeriods = [],
  commercialProfile = null,
  inventoryMatch = null,
  priorCampaign = null,
  targetQualifiedMin = 5,
  targetQualifiedMax = 10,
  monthCeilingsUsd = { "2027-08": 6, "2027-09": 6, "2027-10": 6 },
  aggregateCeilingUsd = 18,
  spentByMonth = {},
  authorizePaidFollowUp = false,
} = {}) {
  const relevance = evaluateFourteenStrategiesForNeedDateCampaign({
    hotel: { hotelId, ...hotel },
    needPeriods,
    commercialProfile,
    inventoryCount: inventoryMatch?.existingMatchCount || 0,
    priorSelected: priorCampaign?.strategiesSelected || [],
  });

  const strategyPlan = selectStrategiesForCampaign({
    needPeriods,
    hotel,
    inventoryMatch,
    maxStrategies: 8,
    commercialProfile,
  });

  const remainingByMonth = {};
  let remainingAggregate = Number(aggregateCeilingUsd) || 0;
  for (const [mk, ceil] of Object.entries(monthCeilingsUsd || {})) {
    const spent = Number(spentByMonth?.[mk] || 0);
    const rem = Math.max(0, Number(ceil) - spent);
    remainingByMonth[mk] = Number(rem.toFixed(2));
  }
  const spentAgg = Object.values(spentByMonth || {}).reduce((a, b) => a + Number(b || 0), 0);
  remainingAggregate = Number((aggregateCeilingUsd - spentAgg).toFixed(2));

  const waves = [
    {
      wave: NEED_DATE_RESEARCH_WAVE.WAVE_1_EXISTING_INVENTORY,
      paid: false,
      objective: "Match known GDI inventory and prior research to need periods.",
      status: "READY",
    },
    {
      wave: NEED_DATE_RESEARCH_WAVE.WAVE_2_TARGETED_DISCOVERY,
      paid: true,
      requiresFounderApproval: true,
      objective: "Highest-value strategies only — never blind all-14 launch.",
      candidateStrategies: relevance.selectedForPursuit,
      status: authorizePaidFollowUp ? "AWAITING_EXECUTION" : "BLOCKED_UNTIL_FOUNDER_GO",
    },
    {
      wave: NEED_DATE_RESEARCH_WAVE.WAVE_3_COMMERCIAL_QUALIFICATION,
      paid: true,
      requiresFounderApproval: true,
      objective: "Deep research for verified dates, buyers, hotel-specific fit.",
      status: authorizePaidFollowUp ? "AWAITING_EXECUTION" : "BLOCKED_UNTIL_FOUNDER_GO",
    },
    {
      wave: NEED_DATE_RESEARCH_WAVE.WAVE_4_GAP_ANALYSIS,
      paid: false,
      objective: "Per-month gap: qualified count vs 5–10 target; underexplored strategies.",
      status: "READY",
    },
    {
      wave: NEED_DATE_RESEARCH_WAVE.WAVE_5_TARGETED_FOLLOW_UP,
      paid: true,
      requiresFounderApproval: true,
      objective: "Only if month ceilings remain and commercial gaps persist.",
      transferForbidden: true,
      remainingByMonth,
      status: authorizePaidFollowUp ? "CONDITIONAL" : "BLOCKED_UNTIL_FOUNDER_GO",
    },
    {
      wave: NEED_DATE_RESEARCH_WAVE.WAVE_6_FINAL_QUALIFICATION,
      paid: false,
      objective: "Independent audit before staging; no artificial yield inflation.",
      status: "READY",
    },
  ];

  const priorStaged = Number(priorCampaign?.stagedCount || 0);
  const gapToMin = Math.max(0, targetQualifiedMin - priorStaged);
  const gapToMax = Math.max(0, targetQualifiedMax - priorStaged);

  const researchTracks = buildNeedDateDualTrackResearchPlan({
    strategies: relevance.strategies || [],
    includeWeddingSocial: true,
  });

  return {
    version: "gdi-need-date-adaptive-research-v1",
    hotelId,
    targetQualifiedRange: { min: targetQualifiedMin, max: targetQualifiedMax },
    priorStagedCount: priorStaged,
    remainingYieldGap: { toMin: gapToMin, toMax: gapToMax },
    strategyRelevance: relevance,
    researchTracks,
    softRankedStrategyIds: strategyPlan.strategyIds || [],
    waves,
    budget: {
      aggregateCeilingUsd,
      spentAggregateUsd: Number(spentAgg.toFixed(2)),
      remainingAggregateUsd: remainingAggregate,
      monthCeilingsUsd,
      remainingByMonth,
      transferForbidden: true,
      paidFollowUpAuthorized: authorizePaidFollowUp === true,
      note: "Unused prior balance is NOT automatic new authorization.",
    },
    stopConditions: [
      "Month ceiling would be exceeded by next quote",
      "Aggregate ceiling would be exceeded",
      "No commercially promising unresolved candidates",
      "Founder has not approved paid follow-up",
      "Qualification gates would require lowering standards to hit 5–10",
    ],
    paidExecutionBlockedUntilFounderGo: authorizePaidFollowUp !== true,
  };
}

/**
 * Build costed continuation proposal from V4 ledger + relevance (zero paid).
 */
export function buildCostedContinuationProposal({
  adaptivePlan,
  priorLedger = null,
  stagedCandidates = [],
  rejectedSummary = [],
} = {}) {
  const rem = adaptivePlan?.budget?.remainingByMonth || {};
  const proposedByMonth = {};
  let proposedAggregate = 0;
  for (const [mk, remUsd] of Object.entries(rem)) {
    // Propose up to remaining, capped suggestion: 1 discovery + up to 4 deep ≈ $2.10 if quotes hold
    const suggested = Math.min(Number(remUsd) || 0, 2.1);
    proposedByMonth[mk] = {
      remainingUsd: Number(remUsd),
      proposedSpendUsd: Number(suggested.toFixed(2)),
      shape: "1× medium discovery ($0.10) + up to 4× high deep ($0.50) if candidates clear Wave 4 gaps",
    };
    proposedAggregate += suggested;
  }

  const revisit = (adaptivePlan?.strategyRelevance?.strategies || [])
    .filter((s) => s.pursueInContinuation)
    .map((s) => ({
      strategyId: s.strategyId,
      relevance: s.relevance,
      reasons: s.reasons,
    }));

  const deepNeeded = (stagedCandidates || [])
    .filter((c) => /UNKNOWN_SOURCING|HYPOTHESIS|Hilton hotel block/i.test(JSON.stringify(c)))
    .map((c) => ({
      opportunityId: c.opportunityId || c.id,
      title: c.eventDisplayName || c.eventName || c.title,
      reason: "Sourcing accessibility / host-hotel commitment still unresolved — deep follow-up if approved",
    }));

  return {
    version: "gdi-need-date-costed-continuation-v5",
    authorizationRequired: true,
    automaticSpend: false,
    priorSpentUsd: priorLedger?.spentUsd ?? null,
    priorRemainingReportedUsd: priorLedger?.remainingAggregateUsd ?? null,
    proposedSpendUsd: Number(proposedAggregate.toFixed(2)),
    proposedByMonth,
    strategiesToRevisit: revisit,
    newStrategiesRecommended: (adaptivePlan?.strategyRelevance?.strategies || [])
      .filter(
        (s) =>
          s.relevance === "MEDIUM_RELEVANCE" &&
          s.pursueInContinuation &&
          !(priorLedger?.strategiesSelected || []).includes?.(s.strategyId),
      )
      .map((s) => s.strategyId),
    stagedCandidatesNeedingDeeperFollowUp: deepNeeded,
    rejectedNotRevivedBlindly: Array.isArray(rejectedSummary)
      ? rejectedSummary.slice(0, 12)
      : rejectedSummary && typeof rejectedSummary === "object"
        ? Object.keys(rejectedSummary).slice(0, 12)
        : [],
    expectedCommercialValue:
      "Close toward 5–10 CONFIRMED_TIMING_MATCH or SUPPORTED_PROBABLE_TIMING opportunities without lowering buyer/timing gates.",
    stopConditions: adaptivePlan?.stopConditions || [],
    note: "This proposal does not authorize Exa. Founder must explicitly approve any paid continuation.",
  };
}
