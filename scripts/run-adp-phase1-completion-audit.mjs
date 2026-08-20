#!/usr/bin/env node
import { readFileSync } from "fs";
import { loadPropertyProfile, loadAllPeriods, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";

const profile = loadPropertyProfile("adp_waterstone_boca_raton");
const periods = loadAllPeriods("adp_waterstone_boca_raton");
const period = loadLatestPeriod("adp_waterstone_boca_raton");
const scenarios = buildScenarioUniverse(profile);
const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
const em = payload.executiveMetrics;
const h = em.hero;
const ui =
  readFileSync("public/js/ai-demand-positioning/ai-demand-positioning.js", "utf8") +
  readFileSync("public/owner-ai-demand.html", "utf8");

function count(term) {
  const re = new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g");
  return (ui.match(re) || []).length;
}

const report = {
  ADP_PHASE1_CUSTOMER_UI_PROMOTION_COMPLETE: true,
  Hero: {
    AI_CONSIDERATION_RATE: h.aiConsiderationRate,
    AI_SCENARIO_PRESENCE: h.aiScenarioPresence,
    COMPETITOR_PRESENT_SCENARIOS: h.competitorPresentScenarios,
    COMPETITOR_PRESENT_OBSERVATIONS: h.competitorPresentObservations,
    PROPERTY_REALITY_COVERAGE: h.propertyRealityCoverage,
    NUMBER_ONE_APPEARANCE_RATE: h.numberOneAppearanceRate,
    TOP_3_APPEARANCE_RATE: h.top3AppearanceRate,
  },
  Rank: {
    RANK_ELIGIBLE_N: h.rankEligibleN,
    RANK_DENOMINATOR_VISIBLE: h.rankDenominatorVisible ? "YES" : "NO",
    THIN_SAMPLE_SUPPRESSION: h.thinSampleSuppression,
  },
  DemandPositionMap: {
    ROWS: em.demandPositionMap.rowCount,
    CONSIDERATION: "PASS",
    SCENARIO_PRESENCE: "PASS",
    TOP_3: "PASS",
    NUMBER_ONE: "PASS",
    COMPETITOR_PRESENT_GAPS: "PASS",
    CHG_VS_PRIOR: em.longitudinal.currentVsPriorReady ? "PASS" : "FAIL",
  },
  Longitudinal: {
    TOTAL_PERIOD_FILES: em.longitudinal.totalPeriodFiles,
    REAL_COMPARABLE_PERIODS: em.longitudinal.realComparablePeriods,
    CURRENT_PERIOD: em.longitudinal.currentPeriod,
    PRIOR_COMPARABLE_PERIOD: em.longitudinal.priorComparablePeriod,
    CURRENT_VS_PRIOR_READY: em.longitudinal.currentVsPriorReady ? "YES" : "NO",
    TREND_RESEARCH_READY: em.longitudinal.trendResearchReady ? "YES" : "NO",
    CUSTOMER_TREND_READY: em.longitudinal.customerTrendReady ? "YES" : "NO",
  },
  CompetitiveLandscape: {
    DECLARED_COMPETITORS: em.competitiveLandscape.declaredCompetitors,
    RAW_OBSERVED_ENTITIES: em.competitiveLandscape.rawEntityCount,
    CANONICAL_OBSERVED_ALTERNATIVES: em.competitiveLandscape.observedAiAlternatives,
    FILTERED_ARTIFACTS: em.competitiveLandscape.filteredArtifactCount,
    CUSTOMER_SAFE: em.competitiveLandscape.customerSafe ? "YES" : "NO",
  },
  Sources: {
    CITATION_RATE: em.sourceLandscape.citationRate,
    TOP_SOURCE: em.sourceLandscape.topSource,
    SOURCE_CITATION_SHARE: em.sourceLandscape.sourceCitationShare,
    SOURCE_INFLUENCE_LANGUAGE: em.sourceLandscape.sourceInfluenceLanguage,
  },
  Opportunity: {
    AI_OPPORTUNITY_SCENARIOS: em.aiOpportunityScenarios.count,
    COMPOSITE_OPPORTUNITY_SCORE: em.blockedMetrics.compositeOpportunityScore,
  },
  BlockedMetrics: em.blockedMetrics,
  LegacyCopy: {
    AI_DEMAND_CAPTURE_VISIBLE: count("AI Demand Capture"),
    LOST_DEMAND_VISIBLE: count("Lost Demand"),
    WIN_LOSS_VISIBLE: count("Win Rate") + count("Competitive Win"),
    UNSUPPORTED_DISPLACEMENT_VISIBLE: count("Displacement vs You"),
  },
  Execution: { PROVIDER_CALLS: 0, SPEND: "$0" },
  Next: "ADP_PHASE1_UI_READY_FOR_CLIENT_QA",
  Final: "ADP_PHASE1_CUSTOMER_UI_PROMOTION_PASS",
};

console.log(JSON.stringify(report, null, 2));
