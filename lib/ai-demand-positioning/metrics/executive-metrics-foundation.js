/**
 * ADP P0 Executive Metrics Foundation — orchestrator.
 * Observation-supported production candidates + research-only benchmarks.
 */

import { PROVIDERS } from "../data-model.js";
import { computeConsiderationMetrics } from "./consideration-rate.js";
import { computePositionMetrics, auditPositionDetection } from "./position-metrics.js";
import { classifyObservedCompetitors, computeObservedShareCandidates, buildCompetitorAppearanceLedger } from "./observed-competitive-share.js";
import { computeSourceMetrics } from "./source-metrics.js";
import { buildScenarioEligibilityMap, computeDemandCoverage } from "./scenario-eligibility.js";
import { buildDemandPositionMap } from "./demand-position-map.js";
import { computeHeadToHeadResearch } from "./head-to-head-research.js";
import { computeExpectedShareResearch } from "./expected-share-research.js";
import { extractPropertyRank } from "./position-extraction.js";
import { filterComparableObservations } from "./grain-governance.js";

export const EXECUTIVE_METRICS_VERSION = "adp_executive_metrics_foundation_v1";

/**
 * Re-parse observations with governed rank extraction (no provider calls).
 */
export function enrichObservationsWithRank(observations, propertyProfile) {
  return (observations || []).map((obs) => {
    const rank = extractPropertyRank(obs.rawResponse || "", propertyProfile);
    return {
      ...obs,
      mentioned: rank.mentioned,
      position: rank.position,
      rankEligible: rank.rankEligible,
      rankSource: rank.rankSource,
      positionConfidence: rank.positionConfidence,
      context: rank.context ?? obs.context,
      parsed: true,
    };
  });
}

export function buildExecutiveMetricsFoundation(period, scenarios, propertyProfile, options = {}) {
  const observations = options.enrichRank !== false
    ? enrichObservationsWithRank(period.observations || [], propertyProfile)
    : (period.observations || []);

  const consideration = computeConsiderationMetrics(observations, scenarios, propertyProfile);
  const position = computePositionMetrics(observations, scenarios, propertyProfile);
  const positionAudit = auditPositionDetection(observations);
  const competitive = classifyObservedCompetitors(observations, propertyProfile);
  const shareCandidates = computeObservedShareCandidates(
    observations,
    propertyProfile,
    propertyProfile.name
  );
  const sources = computeSourceMetrics(observations, propertyProfile);
  const eligibility = buildScenarioEligibilityMap(scenarios, propertyProfile);
  const demandCoverage = computeDemandCoverage(eligibility, scenarios);
  const demandPositionMap = buildDemandPositionMap(observations, scenarios, propertyProfile);
  const headToHead = computeHeadToHeadResearch(observations, propertyProfile);
  const expectedShare = computeExpectedShareResearch(observations, scenarios, propertyProfile);
  const ledger = buildCompetitorAppearanceLedger(observations, propertyProfile);

  const periods = options.periodCount || 1;

  return {
    version: EXECUTIVE_METRICS_VERSION,
    status: "RESEARCH_AND_PRODUCTION_CANDIDATES",
    propertyId: propertyProfile.propertyId,
    periodId: period.periodId,
    grain: {
      observation: "property × scenario × provider × period",
      scenario: "property × scenario (aggregates providers)",
    },
    observationGrain: {
      totalComparableObservations: filterComparableObservations(observations).length,
      totalScenarios: scenarios.length,
      providers: PROVIDERS.length,
    },
    consideration,
    position,
    positionAudit,
    competitiveSet: competitive,
    observedShareCandidates: shareCandidates,
    sources,
    scenarioEligibility: eligibility,
    demandCoverage,
    demandPositionMap,
    headToHeadResearch: headToHead,
    expectedShareResearch: expectedShare,
    aiConsiderationIndex: {
      customerStatus: "BLOCKED",
      reason: "EXPECTED_CONSIDERATION_SHARE not certified",
    },
    opportunity: {
      aiOpportunityScenarios: "Use existing whiteSpace module — composite score BLOCKED",
      compositeOpportunityScore: "BLOCKED",
    },
    longitudinal: {
      periods,
      currentVsPriorReady: periods >= 2,
      trendReady: periods >= 3,
    },
    competitorAppearanceLedgerSample: ledger.slice(0, 3),
    terminology: ADP_TERMINOLOGY_V1,
    promptMoat: {
      rawPromptCustomerLeaks: 0,
      expectedShareEngineCustomerLeaks: 0,
    },
  };
}

export const ADP_TERMINOLOGY_V1 = Object.freeze({
  demandCaptureRecommendedReplacement: "AI Consideration Rate (observation grain) + Demand Scenario Coverage (scenario grain)",
  lostDemandRecommendedReplacement: "Competitor-Present Gaps",
  displacementRecommendedReplacement: "Frequently Observed Alternative (within-period recurrence only)",
  winLossLanguage: "BLOCKED until head-to-head validation certified",
  unsafeTermsAudit: [
    { term: "Demand Capture", replaceWith: "AI Consideration Rate / Demand Scenario Coverage", status: "PROPOSED_NOT_SHIPPED" },
    { term: "Lost Demand", replaceWith: "Competitor-Present Gaps", status: "PROPOSED_NOT_SHIPPED" },
    { term: "Displacement", replaceWith: "Competitor-Present Gap count", status: "PROPOSED_NOT_SHIPPED" },
    { term: "Win / Loss / Beat", replaceWith: "Head-to-Head Position (research)", status: "BLOCKED" },
    { term: "Influence", replaceWith: "Source Citation Share", status: "PROPOSED_NOT_SHIPPED" },
    { term: "Recommended", replaceWith: "Appears in AI response / rank-eligible appearance", status: "PROPOSED_NOT_SHIPPED" },
    { term: "Dominates", replaceWith: "High competitive concentration (internal)", status: "PROPOSED_NOT_SHIPPED" },
  ],
  heroKpiRecommendation: [
    "AI Consideration Rate",
    "Demand Scenario Coverage",
    "Competitor-Present Gaps",
    "Observed AI Alternatives (count)",
    "Property Reality Coverage (existing Reality Gap module)",
  ],
  infoIcons: {
    aiConsiderationRate: "How often this hotel appears across comparable monitored AI responses for relevant traveler-demand scenarios.",
    demandScenarioCoverage: "The share of relevant monitored demand scenarios in which the hotel appears on at least one comparable AI provider.",
    top3AppearanceRate: "The share of rank-eligible AI responses where the hotel appears in the first three positions.",
    numberOneAppearanceRate: "The share of rank-eligible AI responses where the hotel appears first.",
    observedAiAlternatives: "Hotels that appear as alternatives across monitored AI responses. Not every observed alternative is necessarily a direct commercial competitor.",
    competitorPresentGaps: "Relevant demand observations where this hotel was absent while one or more governed comparable hotels appeared.",
    sourceCitationShare: "How often a source domain is cited among AI responses that contain citations. This measures citation presence, not influence.",
  },
});

export function validatePeriodIntegrity(period) {
  const required = ["periodId", "propertyId", "observations"];
  const missing = required.filter((k) => !period?.[k]);
  const obs = period?.observations || [];
  const obsMissing = obs.filter((o) => !o.observationId || !o.scenarioId || !o.provider).length;
  return {
    ok: missing.length === 0 && obsMissing === 0,
    missingFields: missing,
    observationsMissingIds: obsMissing,
  };
}
