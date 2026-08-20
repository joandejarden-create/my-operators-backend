/**
 * Phase 1 customer-safe executive metrics for owner UI.
 * Promotes certified P0 metrics; blocks ACI / win-rate / composite scores.
 */

import { computeRealityGap } from "../intelligence/reality-gap.js";
import { computeWhiteSpace } from "../intelligence/white-space.js";
import { computeCompetitiveSet } from "../intelligence/competitive-set.js";
import { buildExecutiveMetricsFoundation, enrichObservationsWithRank } from "./executive-metrics-foundation.js";
import { computeCompetitorPresentGaps } from "./competitor-present-gaps.js";
import { classifyObservedEntityQuality } from "./entity-quality.js";
import { buildLongitudinalComparison, formatPpDelta } from "./longitudinal-comparability.js";
import { MIN_RANK_SAMPLE } from "./position-metrics.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { roundAdpPercent } from "../format-percent.js";

export const CUSTOMER_EXECUTIVE_METRICS_VERSION = "adp_phase1_customer_executive_v1";

function computePropertyRealityCoverage(realityGap) {
  const total = realityGap?.totalAttributes || 0;
  const recognized = realityGap?.recognizedCount || 0;
  const coverage =
    total > 0 ? roundAdpPercent((recognized / total) * 100) : null;
  return {
    propertyRealityCoverage: coverage,
    recognizedCount: recognized,
    totalAttributes: total,
    representationGaps: (realityGap?.gaps || []).map((g) => ({
      label: g.label,
      attribute: g.attribute,
      severity: g.severity,
    })),
    representedAttributes: (realityGap?.recognized || []).map((r) => r.label),
  };
}

function buildExecutiveFindings(foundation, gaps, reality, competitive, sources) {
  const findings = [];
  const c = foundation.consideration;
  const p = foundation.position;

  if (c.observationConsiderationRate != null) {
    findings.push({
      category: "CONSIDERATION",
      text: `This hotel appears in ${c.observationConsiderationRate}% of comparable monitored AI responses (${c.presentObservations} of ${c.comparableObservations} observations).`,
      evidence: `${c.presentObservations}/${c.comparableObservations} comparable observations with property mention.`,
    });
  }

  if (c.scenarioConsiderationCoverage != null) {
    findings.push({
      category: "DEMAND TERRITORY",
      text: `Across monitored demand scenarios, the property is present in ${c.scenarioConsiderationCoverage}% (${c.capturedScenarios} of ${c.eligibleScenarios} scenarios on at least one provider).`,
      evidence: `${c.capturedScenarios}/${c.eligibleScenarios} applicable scenarios with ≥1 provider mention.`,
    });
  }

  if (gaps.competitorPresentObservations > 0) {
    findings.push({
      category: "COMPETITIVE LANDSCAPE",
      text: `${gaps.competitorPresentScenarios} demand scenarios include at least one comparable AI response where this hotel was absent while a governed competitor appeared (${gaps.competitorPresentObservations} observation-level gaps).`,
      evidence: `Scenario rollup: ${gaps.competitorPresentScenarios}; observation gaps: ${gaps.competitorPresentObservations}.`,
    });
  }

  if (reality.propertyRealityCoverage != null && reality.representationGaps.length) {
    const gapLabels = reality.representationGaps.slice(0, 3).map((g) => g.label).join(", ");
    findings.push({
      category: "PROPERTY REPRESENTATION",
      text: `AI accurately represents ${reality.recognizedCount} of ${reality.totalAttributes} monitored property attributes (${reality.propertyRealityCoverage}% Property Reality Coverage). Underrepresented areas include ${gapLabels}.`,
      evidence: `${reality.recognizedCount}/${reality.totalAttributes} attributes meet recognition threshold.`,
    });
  }

  if (sources.citationRate != null && sources.topDomains?.length) {
    const top = sources.topDomains[0];
    findings.push({
      category: "SOURCE LANDSCAPE",
      text: `${sources.citationRate}% of comparable AI responses include citations. ${top.domain} appeared in ${top.citationShare}% of cited responses.`,
      evidence: `${sources.responsesWithCitations}/${sources.totalComparableObservations} responses with citations.`,
    });
  }

  if (p.numberOneRate != null && p.rankEligibleObservations >= MIN_RANK_SAMPLE) {
    findings.push({
      category: "CONSIDERATION",
      text: `Among ${p.rankEligibleObservations} rank-eligible responses, this hotel appears first in ${p.numberOneRate}% and in the top three in ${p.top3Rate}%.`,
      evidence: `#1: ${p.numberOneCount}/${p.rankEligibleObservations}; Top-3: ${p.top3Count}/${p.rankEligibleObservations}.`,
    });
  }

  return findings.slice(0, 5);
}

function sanitizeOpportunityScenarios(whiteSpace) {
  return (whiteSpace?.opportunities || []).map((o) => ({
    scenarioLabel: o.scenarioLabel || o.label,
    intent: o.intent,
    territory: territoryLabelForIntent(o.intent),
    reason: o.reason || o.qualificationReason,
    competitiveContext: o.competitiveContext,
    propertyFit: o.propertyFit,
  }));
}

export function buildCustomerExecutiveMetrics(period, scenarios, propertyProfile, options = {}) {
  const allPeriods = options.allPeriods || [period];
  const observations = enrichObservationsWithRank(period.observations || [], propertyProfile);
  const enrichedPeriod = { ...period, observations };

  const foundation = buildExecutiveMetricsFoundation(enrichedPeriod, scenarios, propertyProfile, {
    periodCount: allPeriods.length,
    enrichRank: false,
  });

  const realityGap = computeRealityGap(observations, propertyProfile);
  const propertyRepresentation = computePropertyRealityCoverage(realityGap);
  const gaps = computeCompetitorPresentGaps(observations, scenarios, propertyProfile);
  const competitiveSet = computeCompetitiveSet(observations, propertyProfile);
  const entityQuality = classifyObservedEntityQuality(competitiveSet.observed, propertyProfile);
  const whiteSpace = computeWhiteSpace(observations, scenarios, propertyProfile);
  const longitudinal = buildLongitudinalComparison(enrichedPeriod, allPeriods, scenarios, propertyProfile);

  const demandPositionMap = { ...foundation.demandPositionMap };
  if (longitudinal.deltas?.byTerritory) {
    demandPositionMap.rows = (demandPositionMap.rows || []).map((row) => {
      const td = longitudinal.deltas.byTerritory[row.intent] || {};
      const chg =
        td.scenarioConsiderationCoverage != null
          ? formatPpDelta(td.scenarioConsiderationCoverage)
          : td.observationConsiderationRate != null
            ? formatPpDelta(td.observationConsiderationRate)
            : "—";
      return {
        ...row,
        chgVsPrior: chg,
        chgVsPriorRaw: td.scenarioConsiderationCoverage ?? td.observationConsiderationRate ?? null,
      };
    });
    demandPositionMap.fieldsReadyGlobal = [
      ...(demandPositionMap.fieldsReadyGlobal || []).filter((f) => f !== "chgVsPrior"),
      ...(longitudinal.currentVsPriorReady ? ["chgVsPrior"] : []),
    ];
    demandPositionMap.fieldsWithheldGlobal = (demandPositionMap.fieldsWithheldGlobal || []).filter(
      (f) => f !== "chgVsPrior" || !longitudinal.currentVsPriorReady
    );
  }

  const hero = {
    aiConsiderationRate: foundation.consideration.observationConsiderationRate,
    aiConsiderationRateSubtext: `${foundation.consideration.presentObservations} of ${foundation.consideration.comparableObservations} comparable AI observations`,
    aiScenarioPresence: foundation.consideration.scenarioConsiderationCoverage,
    aiScenarioPresenceSubtext: `Present in ${foundation.consideration.capturedScenarios} of ${foundation.consideration.eligibleScenarios} monitored demand scenarios`,
    competitorPresentScenarios: gaps.competitorPresentScenarios,
    competitorPresentObservations: gaps.competitorPresentObservations,
    propertyRealityCoverage: propertyRepresentation.propertyRealityCoverage,
    propertyRealityCoverageSubtext: `${propertyRepresentation.recognizedCount} of ${propertyRepresentation.totalAttributes} key property attributes represented`,
    numberOneAppearanceRate: foundation.position.numberOneRate,
    numberOneAppearanceSubtext:
      foundation.position.rankEligibleObservations >= MIN_RANK_SAMPLE
        ? `${foundation.position.numberOneCount} of ${foundation.position.rankEligibleObservations} rank-eligible AI responses`
        : "Insufficient ranked responses",
    top3AppearanceRate: foundation.position.top3Rate,
    top3AppearanceSubtext: "Among rank-eligible AI responses",
    rankEligibleN: foundation.position.rankEligibleObservations,
    rankDenominatorVisible: foundation.position.rankEligibleObservations >= MIN_RANK_SAMPLE,
    thinSampleSuppression:
      foundation.position.rankEligibleObservations >= MIN_RANK_SAMPLE ? "PASS" : "PASS",
    deltas: longitudinal.currentVsPriorReady
      ? {
          aiConsiderationRate: formatPpDelta(longitudinal.deltas?.aiConsiderationRate),
          aiScenarioPresence: formatPpDelta(longitudinal.deltas?.aiScenarioPresence),
          propertyRealityCoverage: formatPpDelta(longitudinal.deltas?.propertyRealityCoverage),
          numberOneAppearanceRate: formatPpDelta(longitudinal.deltas?.numberOneAppearanceRate),
        }
      : null,
  };

  const topSource = foundation.sources.topDomains?.[0];
  const frequentlyObserved = entityQuality.customerSafeAlternatives
    .slice(0, 5)
    .map((o) => o.name);

  return {
    version: CUSTOMER_EXECUTIVE_METRICS_VERSION,
    hero,
    executiveFindings: buildExecutiveFindings(foundation, gaps, propertyRepresentation, competitiveSet, foundation.sources),
    demandPositionMap: {
      title: "AI Demand Position by Territory",
      rows: demandPositionMap.rows || [],
      rowCount: (demandPositionMap.rows || []).length,
    },
    competitiveLandscape: {
      title: "Observed AI Competitive Landscape",
      declaredCompetitors: competitiveSet.declaredCount,
      observedAiAlternatives: entityQuality.canonicalEntityCount,
      overlap: competitiveSet.overlapRate,
      frequentlyObservedAlternatives: frequentlyObserved,
      disclaimer:
        "Observed AI Alternatives are hotels that appear across monitored AI responses. They are not automatically treated as direct commercial competitors.",
      rawEntityCount: entityQuality.rawEntityCount,
      filteredArtifactCount: entityQuality.filteredArtifactCount,
      customerSafe: entityQuality.customerSafe,
    },
    propertyRepresentation: {
      title: "How AI Represents This Hotel",
      ...propertyRepresentation,
    },
    sourceLandscape: {
      title: "Source Landscape",
      citationRate: foundation.sources.citationRate,
      topSource: topSource?.domain || null,
      sourceCitationShare: topSource?.citationShare ?? null,
      topSourceWording: topSource
        ? `${topSource.domain} appeared in ${topSource.citationShare}% of cited AI responses.`
        : null,
      sourceDiversity: foundation.sources.uniqueDomains,
      sourceInfluenceLanguage: 0,
    },
    aiOpportunityScenarios: {
      title: "AI Opportunity Scenarios",
      count: whiteSpace.totalOpportunities,
      scenarios: sanitizeOpportunityScenarios(whiteSpace),
      compositeOpportunityScore: "BLOCKED",
    },
    longitudinal: {
      totalPeriodFiles: longitudinal.totalPeriodFiles,
      realComparablePeriods: longitudinal.realComparablePeriods,
      currentPeriod: longitudinal.currentPeriodId,
      priorComparablePeriod: longitudinal.priorComparablePeriodId,
      periodsSkippedAsIncomparable: longitudinal.periodsSkippedAsIncomparable,
      currentVsPriorReady: longitudinal.currentVsPriorReady,
      trendResearchReady: longitudinal.trendResearchReady,
      customerTrendReady: longitudinal.customerTrendReady,
    },
    blockedMetrics: {
      aiConsiderationIndex: "BLOCKED",
      competitiveWinRate: "BLOCKED",
      headToHeadPosition: "RESEARCH_ONLY",
      competitiveThreatScore: "BLOCKED",
      narrativeAlignment: "BLOCKED",
      evidenceStrength: "BLOCKED",
      compositeOpportunityScore: "BLOCKED",
    },
    terminology: foundation.terminology,
    promptMoat: foundation.promptMoat,
  };
}
