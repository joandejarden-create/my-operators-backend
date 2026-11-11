/**
 * All-fourteen workflow coverage plan for Need-Date Deep Discovery V6.
 * Relevance classification only — does not authorize paid Exa.
 */

import {
  NEED_DATE_RESEARCH_STRATEGY,
  NEED_DATE_STRATEGY_META,
} from "./need-date-demand-contract-v1.js";
import {
  NEED_DATE_V6_WORKFLOW_PRIORITY,
  WATERSTONE_V6_HOTEL,
} from "./need-date-deep-discovery-v6-contract.js";
import {
  DATE_SPECIFIC_STRATEGY_IDS,
  ACCOUNT_RELATIONSHIP_STRATEGY_IDS,
  NEED_DATE_RESEARCH_TRACK,
} from "./need-date-research-track-planning-v1.js";

const P = NEED_DATE_V6_WORKFLOW_PRIORITY;

/**
 * Waterstone Aug–Oct 2027 default coverage (marina/leisure resort).
 * PRIMARY gets mandatory meaningful discovery (+ deep when candidates qualify).
 */
export function buildWaterstoneFourteenWorkflowCoveragePlan(opts = {}) {
  const hotelId = opts.hotelId || WATERSTONE_V6_HOTEL.hotelId;
  const needMonths = opts.needMonths || WATERSTONE_V6_HOTEL.needMonths;

  const rows = [
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS,
      priority: P.PRIMARY,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason:
        "Weddings & social groups are product-core for marina resort; prior campaign NOT RESEARCHED — mandatory dedicated discovery.",
      mandatoryDeepIfCandidates: true,
      weddingSpecific: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES,
      priority: P.PRIMARY,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason: "Corporate meetings/training are directly addressable demand with Waterstone meeting product.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES,
      priority: P.PRIMARY,
      track: NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
      reason: "Corporate accounts — prior campaign discovery-only; needs deep on promising accounts.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS,
      priority: P.PRIMARY,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason: "Marina F&B / day meetings — commercially accessible; prior deep = 0.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS,
      priority: P.PRIMARY,
      track: NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
      reason:
        "Wedding/social/corporate planners as relationship demand; reframe away from overflow intermediaries.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.CORPORATE_CELEBRATIONS,
      priority: P.PRIMARY,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason: "Corporate banquets/private events — distinct from weddings; prior discovery-only.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.SPORTS_ENTERTAINMENT,
      priority: P.SECONDARY,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason: "High potential if Aug–Oct schedules verify; Sep coverage gap.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.PREFERRED_LODGING,
      priority: P.SECONDARY,
      track: NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
      reason: "Off-site wedding/social venues needing lodging partners — relationship track.",
      mandatoryDeepIfCandidates: false,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.EDUCATION_SCIENTIFIC,
      priority: P.SECONDARY,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason: "Pursue only editions without competing host/block and with need-month timing.",
      mandatoryDeepIfCandidates: true,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE,
      priority: P.SECONDARY,
      track: NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
      reason: "Intermediaries with Boca marina-resort groups — light unless strong signals.",
      mandatoryDeepIfCandidates: false,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS,
      priority: P.CONDITIONAL,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason:
        "Still relevant but prior V4 over-allocated deep to competitor-hosted pages. Cap: demand-owner first; demote host-locked editions.",
      mandatoryDeepIfCandidates: true,
      depthCapNote: "Do not allocate majority of deep budget here; require no competing host/block OR evidenced overflow.",
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.GOVERNMENT_PUBLIC_SECTOR,
      priority: P.CONDITIONAL,
      track: NEED_DATE_RESEARCH_TRACK.DATE_SPECIFIC_DEMAND,
      reason: "Evaluate only if verified public programmes with lodging near Boca and open sourcing.",
      mandatoryDeepIfCandidates: false,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.PROJECTS_INFRASTRUCTURE,
      priority: P.CONDITIONAL,
      track: NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
      reason: "Only with verified crew timing near market; not default marina leisure product.",
      mandatoryDeepIfCandidates: false,
    },
    {
      strategyId: NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY,
      priority: P.NOT_APPLICABLE,
      track: NEED_DATE_RESEARCH_TRACK.ACCOUNT_RELATIONSHIP_DEMAND,
      reason: "Waterstone is not primarily extended-stay product for these need months.",
      mandatoryDeepIfCandidates: false,
    },
  ];

  const enriched = rows.map((r) => {
    const meta = NEED_DATE_STRATEGY_META[r.strategyId] || {};
    return {
      ...r,
      workflowNumber: meta.number,
      powershell: meta.powershell,
      isDateSpecific: DATE_SPECIFIC_STRATEGY_IDS.includes(r.strategyId),
      isAccountRelationship: ACCOUNT_RELATIONSHIP_STRATEGY_IDS.includes(r.strategyId),
    };
  });

  const byPriority = (p) => enriched.filter((r) => r.priority === p);

  return {
    version: "gdi-need-date-deep-discovery-coverage-v6",
    hotelId,
    needMonths,
    workflows: enriched,
    summary: {
      primary: byPriority(P.PRIMARY).map((r) => r.strategyId),
      secondary: byPriority(P.SECONDARY).map((r) => r.strategyId),
      conditional: byPriority(P.CONDITIONAL).map((r) => r.strategyId),
      notApplicable: byPriority(P.NOT_APPLICABLE).map((r) => r.strategyId),
      primaryCount: byPriority(P.PRIMARY).length,
      allFourteenAccounted: enriched.length === 14,
      weddingMandatory: true,
    },
    evolutionRule:
      "Selection may evolve as research identifies new demand pathways — document changes; never blind-launch all 14 equally.",
    note: "Coverage plan only. Does not authorize paid Exa.",
  };
}

export function assertAllFourteenWorkflowsCovered(coveragePlan) {
  const ids = new Set((coveragePlan?.workflows || []).map((w) => w.strategyId));
  const missing = Object.values(NEED_DATE_RESEARCH_STRATEGY).filter((id) => !ids.has(id));
  return {
    ok: missing.length === 0 && (coveragePlan?.workflows || []).length === 14,
    missing,
    count: (coveragePlan?.workflows || []).length,
  };
}
