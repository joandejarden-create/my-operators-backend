/**
 * Commercial-value deep-research selection register V1.
 *
 * Two-pass allocation after pre-deep screening:
 *   Pass A already screened all candidates (PR2).
 *   Pass B ranks eligible candidates with transparent scores, reserves
 *   fair segment coverage, then fills remaining slots by commercial value.
 *
 * Extends existing scoreDeepResearchInvestment + fair scheduler — does not
 * invent a parallel ranking system. Does not spend. Does not publish.
 */

import { scoreDeepResearchInvestment } from "./commercial-pursuit-strategy-v1.js";
import { evaluateDeepResearchEligibility, RESEARCH_DEPTH_TIER } from "./deep-research-eligibility-v1.js";
import { listFourteenWorkflowSegmentIds } from "./universal-contact-research-contract-v1.js";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "./exa-agent/exa-agent-provider-adapter-v1.js";

export const GDI_DEEP_RESEARCH_SELECTION_REGISTER_V1 =
  "gdi-deep-research-selection-register-v1";

/**
 * Founder-proposed relative weights — mapped onto existing investment score
 * components rather than replacing them. Documented for transparency.
 */
export const SELECTION_DIMENSION_WEIGHTS = Object.freeze({
  credibleDemandAndCommercialRelevance: 0.25,
  hotelSpecificFit: 0.2,
  noveltyAndIncrementalIntelligence: 0.15,
  forwardLookingTimingAndActionability: 0.15,
  researchGapsWorthResolving: 0.1,
  potentialCommercialValue: 0.1,
  researchCostEfficiency: 0.05,
});

export const SELECTION_DECISION = Object.freeze({
  SELECTED_FOR_DEEP: "SELECTED_FOR_DEEP",
  SELECTED_FOR_TARGETED_TIER3: "SELECTED_FOR_TARGETED_TIER3",
  DEFERRED_FAIRNESS_OR_BUDGET: "DEFERRED_FAIRNESS_OR_BUDGET",
  DEFERRED_LOWER_RANK: "DEFERRED_LOWER_RANK",
  SKIPPED_NOT_ELIGIBLE: "SKIPPED_NOT_ELIGIBLE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function clamp01(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

/**
 * Transparent dimension scores (0–1) derived from existing signals.
 * Aligns with scoreDeepResearchInvestment rather than contradicting it.
 */
export function scoreSelectionDimensions(opportunity = {}, context = {}) {
  const investment = scoreDeepResearchInvestment(opportunity, context);
  const eligibility = evaluateDeepResearchEligibility(opportunity, context);
  const screen = investment.screen || {};
  const thesis = text(opportunity.hotelOpportunityThesis || opportunity.whyThisHotel);
  const blob = [
    thesis,
    opportunity.title,
    opportunity.organizationName,
    opportunity.opportunityType,
  ]
    .map(text)
    .join(" ");

  const credible = clamp01(
    (screen.decision === "ADVANCE_TO_DEEP_RESEARCH" ? 0.7 : 0.35) +
      ((opportunity.seedUrls || []).length || (opportunity.evidence || []).length ? 0.2 : 0) +
      (/guestroom|room block|housing|meeting|catering|training|project/i.test(blob)
        ? 0.1
        : 0),
  );
  const hotelFit = clamp01(
    /OUT_OF_MARKET|MISMATCH|ELSEWHERE/i.test(
      String(opportunity.geoFit?.geoFitClass || opportunity.hotelFit || ""),
    )
      ? 0.05
      : 0.55 + (thesis.length > 40 ? 0.25 : 0) + (hotelCityHint(opportunity, context) ? 0.15 : 0),
  );
  const novelty = clamp01(
    opportunity.preDeepScreen?.outcome === "EXISTING_OPPORTUNITY_ENRICHMENT"
      ? 0.55
      : opportunity.preDeepScreen?.outcome === "DUPLICATE_NO_NEW_EVIDENCE"
        ? 0.05
        : 0.75,
  );
  const timing = clamp01(
    context.needDateMatch === true || opportunity.needDateMatch === true
      ? 0.9
      : /202[6-9]|next (year|edition)|upcoming|future/i.test(blob)
        ? 0.65
        : 0.35,
  );
  const gaps = clamp01(((screen.materialGaps || []).length || 0) / 4);
  const commercialValue = clamp01(
    (Number(investment.score) || 0) / 120 +
      (/recurring|annual|preferred|partnership|overflow/i.test(blob) ? 0.15 : 0),
  );
  const costEfficiency = clamp01(
    eligibility.tier === RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT
      ? 0.85
      : opportunity.preDeepScreen?.preferTier3
        ? 0.8
        : 0.55,
  );

  // V1.2 controlled ranking correction: prefer hotel demand mechanics +
  // reachable entry points over high-volume expansion news without lodging signals.
  const lodgingDemandSignal = clamp01(
    /hotel|lodging|room\s*block|room.?night|housing|overnight|guest\s*room|meeting\s*space|venue\s*sourcing|accommodation|peak\s*rooms/i.test(
      blob,
    )
      ? 1
      : /conference|congress|annual\s+meeting|scientific\s+meeting|trade\s+day/i.test(
            blob,
          )
        ? 0.45
        : 0.1,
  );
  const reachableBuyerSignal = clamp01(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      (opportunity.namedStakeholders || []).length ||
      opportunity.contactUrl ||
      opportunity.organizationRouteUrl
      ? 0.85
      : 0.15,
  );

  const weighted =
    credible * SELECTION_DIMENSION_WEIGHTS.credibleDemandAndCommercialRelevance +
    hotelFit * SELECTION_DIMENSION_WEIGHTS.hotelSpecificFit +
    novelty * SELECTION_DIMENSION_WEIGHTS.noveltyAndIncrementalIntelligence +
    timing * SELECTION_DIMENSION_WEIGHTS.forwardLookingTimingAndActionability +
    gaps * SELECTION_DIMENSION_WEIGHTS.researchGapsWorthResolving +
    commercialValue * SELECTION_DIMENSION_WEIGHTS.potentialCommercialValue +
    costEfficiency * SELECTION_DIMENSION_WEIGHTS.researchCostEfficiency;

  const lodgingBuyerBoost =
    lodgingDemandSignal * 8 + reachableBuyerSignal * 4; // points on 0–100 combined scale

  return {
    version: GDI_DEEP_RESEARCH_SELECTION_REGISTER_V1,
    dimensions: {
      credibleDemandAndCommercialRelevance: Number(credible.toFixed(3)),
      hotelSpecificFit: Number(hotelFit.toFixed(3)),
      noveltyAndIncrementalIntelligence: Number(novelty.toFixed(3)),
      forwardLookingTimingAndActionability: Number(timing.toFixed(3)),
      researchGapsWorthResolving: Number(gaps.toFixed(3)),
      potentialCommercialValue: Number(commercialValue.toFixed(3)),
      researchCostEfficiency: Number(costEfficiency.toFixed(3)),
      lodgingDemandSignal: Number(lodgingDemandSignal.toFixed(3)),
      reachableBuyerSignal: Number(reachableBuyerSignal.toFixed(3)),
    },
    weights: SELECTION_DIMENSION_WEIGHTS,
    weightedScore: Number((weighted * 100).toFixed(2)),
    legacyInvestmentScore: Number(investment.score) || 0,
    combinedScore: Number(
      (
        weighted * 100 * 0.65 +
        (Number(investment.score) || 0) * 0.35 +
        lodgingBuyerBoost
      ).toFixed(2),
    ),
    eligibilityTier: eligibility.tier || null,
    eligibilityDecision: eligibility.decision || null,
    screenDecision: screen.decision || null,
    notes: [
      ...(investment.notes || []),
      "v1_2_lodging_buyer_ranking_boost",
    ],
  };
}

function hotelCityHint(opportunity, context) {
  const city = text(context.hotelProfile?.city || "").toLowerCase();
  if (!city) return false;
  const blob = `${opportunity.city || ""} ${opportunity.hotelOpportunityThesis || ""}`.toLowerCase();
  return blob.includes(city) || /bethesda|rockville|montgomery|dmv/i.test(blob);
}

function expectedCostUsd(tier, effort = EXA_AGENT_EFFORT.MEDIUM) {
  const quote = quoteExaAgentRun({ effort });
  if (tier === RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT) {
    // Targeted enrichment modeled as low effort upper bound when available
    const low = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.LOW });
    return Number(low.maxUsd || quote.maxUsd || 0.1);
  }
  return Number(quote.maxUsd || 0.1);
}

/**
 * Select candidates for deep research with fair segment floor + commercial fill.
 */
export function selectCandidatesForDeepResearch(candidates = [], opts = {}) {
  const maxCandidates =
    opts.maxCandidates == null ? candidates.length : Number(opts.maxCandidates);
  const segmentIds = opts.segmentIds || listFourteenWorkflowSegmentIds();
  const fairMinPerSegment =
    opts.fairMinPerSegment == null ? 1 : Number(opts.fairMinPerSegment);
  const remainingBudgetUsd =
    opts.remainingBudgetUsd == null ? null : Number(opts.remainingBudgetUsd);
  const effort = opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM;

  const scored = candidates.map((opp, idx) => {
    const dim = scoreSelectionDimensions(opp, opts);
    const preferTier3 =
      opp.preDeepScreen?.preferTier3 === true ||
      dim.eligibilityTier ===
        RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT;
    const researchTier = preferTier3
      ? RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT
      : RESEARCH_DEPTH_TIER.TIER_2_AUTONOMOUS_DEEP_RESEARCH;
    return {
      opportunity: opp,
      candidateId: opp.id || opp.opportunityId || `idx_${idx}`,
      hotelId: opp.hotelId || opts.hotelId || null,
      globalDemandId: opp.globalDemandId || null,
      segmentId: opp.researchSegment || opp.segmentId || null,
      preliminaryScore: dim.combinedScore,
      dimensions: dim,
      researchTier,
      expectedCostUsd: expectedCostUsd(researchTier, effort),
      sourceIndex: idx,
    };
  });

  scored.sort((a, b) => {
    if (b.preliminaryScore !== a.preliminaryScore) {
      return b.preliminaryScore - a.preliminaryScore;
    }
    return a.sourceIndex - b.sourceIndex;
  });

  const selected = [];
  const selectedIds = new Set();
  const segmentSelected = Object.fromEntries(segmentIds.map((s) => [s, 0]));
  let committedCost = 0;

  function canAfford(row) {
    if (remainingBudgetUsd == null) return true;
    return committedCost + row.expectedCostUsd <= remainingBudgetUsd + 1e-9;
  }

  function trySelect(row, reason) {
    if (selectedIds.has(row.candidateId)) return false;
    if (selected.length >= maxCandidates) return false;
    if (!canAfford(row)) return false;
    selectedIds.add(row.candidateId);
    selected.push({
      ...row,
      ranking: selected.length + 1,
      selectionDecision:
        row.researchTier ===
        RESEARCH_DEPTH_TIER.TIER_3_TARGETED_SPECIALIST_ENRICHMENT
          ? SELECTION_DECISION.SELECTED_FOR_TARGETED_TIER3
          : SELECTION_DECISION.SELECTED_FOR_DEEP,
      decisionReason: reason,
    });
    committedCost += row.expectedCostUsd;
    if (row.segmentId && segmentSelected[row.segmentId] != null) {
      segmentSelected[row.segmentId] += 1;
    }
    return true;
  }

  // Fairness pass: up to fairMinPerSegment per segment with eligible candidates.
  // Within each segment, prefer lodging/buyer signals (V1.2) over raw score ties.
  if (fairMinPerSegment > 0) {
    for (const seg of segmentIds) {
      const pool = scored
        .filter((r) => r.segmentId === seg)
        .slice()
        .sort((a, b) => {
          const aLodging = a.dimensions?.dimensions?.lodgingDemandSignal || 0;
          const bLodging = b.dimensions?.dimensions?.lodgingDemandSignal || 0;
          const aBuyer = a.dimensions?.dimensions?.reachableBuyerSignal || 0;
          const bBuyer = b.dimensions?.dimensions?.reachableBuyerSignal || 0;
          if (bLodging !== aLodging) return bLodging - aLodging;
          if (bBuyer !== aBuyer) return bBuyer - aBuyer;
          if (b.preliminaryScore !== a.preliminaryScore) {
            return b.preliminaryScore - a.preliminaryScore;
          }
          return a.sourceIndex - b.sourceIndex;
        });
      let taken = 0;
      for (const row of pool) {
        if (taken >= fairMinPerSegment) break;
        if (
          trySelect(
            row,
            `fair_segment_floor:${seg}:score=${row.preliminaryScore}:lodging=${row.dimensions?.dimensions?.lodgingDemandSignal ?? "n/a"}`,
          )
        ) {
          taken += 1;
        }
      }
    }
  }

  // Commercial fill: remaining by score
  for (const row of scored) {
    if (selected.length >= maxCandidates) break;
    trySelect(
      row,
      `commercial_rank:score=${row.preliminaryScore}`,
    );
  }

  const register = scored.map((row) => {
    const chosen = selected.find((s) => s.candidateId === row.candidateId);
    if (chosen) return chosen;
    let decision = SELECTION_DECISION.DEFERRED_LOWER_RANK;
    let reason = `below_cap_or_outranked:score=${row.preliminaryScore}`;
    if (remainingBudgetUsd != null && !canAfford(row) && selected.length < maxCandidates) {
      decision = SELECTION_DECISION.DEFERRED_FAIRNESS_OR_BUDGET;
      reason = `budget_or_cap:expected=${row.expectedCostUsd};committed=${committedCost}`;
    }
    return {
      ...row,
      ranking: null,
      selectionDecision: decision,
      decisionReason: reason,
    };
  });

  return {
    version: GDI_DEEP_RESEARCH_SELECTION_REGISTER_V1,
    at: new Date().toISOString(),
    maxCandidates,
    fairMinPerSegment,
    remainingBudgetUsd,
    expectedCommittedUsd: Number(committedCost.toFixed(6)),
    selectedCount: selected.length,
    deferredCount: register.length - selected.length,
    segmentSelected,
    selected: selected.map((s) => s.opportunity),
    register,
  };
}

export default {
  GDI_DEEP_RESEARCH_SELECTION_REGISTER_V1,
  SELECTION_DIMENSION_WEIGHTS,
  SELECTION_DECISION,
  scoreSelectionDimensions,
  selectCandidatesForDeepResearch,
};
