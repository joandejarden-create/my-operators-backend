/**
 * AI Demand Positioning — Customer-Safe Owner Payload Assembly.
 * Combines all intelligence into a single payload for the owner UI.
 * NEVER exposes raw prompts, internal IDs, or raw AI responses.
 */

import { computeDemandCaptureIndex } from "../intelligence/demand-capture-index.js";
import { computeLostDemand } from "../intelligence/lost-demand.js";
import { computeCompetitiveSet } from "../intelligence/competitive-set.js";
import { computeRealityGap } from "../intelligence/reality-gap.js";
import { computeWhiteSpace } from "../intelligence/white-space.js";
import { roundAdpPercent } from "../format-percent.js";
import { buildOptionalExecutiveMetrics } from "../metrics/optional-executive-metrics.js";
import { buildGovernedIntentPresenceIndex } from "../metrics/governed-customer-presence-index.js";
import { buildExecutiveReadWithUx } from "./executive-read-v2.js";
import { buildAllTerritoryCompetitiveRankings } from "./competitive-ranking-overall-view-v1.js";
import { attachDisplacementToCompetitiveRanking } from "./resolve-displacement-evidence-v1.js";
import { computeOwnedExternalSourceMix } from "../metrics/owned-source-classification-v1.js";

export const ADP_PRODUCT_VERSION = "ai_demand_positioning_v1";

export function buildOwnerPayload(period, scenarios, propertyProfile, options = {}) {
  if (!period || !period.observations?.length) {
    return { ok: false, error: "no_data", message: "No monitoring data available for this property." };
  }

  const observations = period.observations.filter((o) => o.parsed);
  if (!observations.length) {
    return { ok: false, error: "not_parsed", message: "Monitoring data has not been processed yet." };
  }

  const demandCapture = computeDemandCaptureIndex(observations, scenarios);
  const lostDemand = computeLostDemand(observations, scenarios, propertyProfile);
  const competitiveSet = computeCompetitiveSet(observations, propertyProfile);
  const realityGap = computeRealityGap(observations, propertyProfile);
  const whiteSpace = computeWhiteSpace(observations, scenarios, propertyProfile);

  const intentPresenceIndex = buildGovernedIntentPresenceIndex(observations, scenarios, propertyProfile);

  const actions = generateActionRecommendations(demandCapture, lostDemand, realityGap, whiteSpace, propertyProfile);
  const brief = generateOwnerBrief(demandCapture, lostDemand, competitiveSet, realityGap, whiteSpace, propertyProfile);

  const payload = {
    ok: true,
    version: ADP_PRODUCT_VERSION,
    property: {
      propertyId: propertyProfile.propertyId,
      name: propertyProfile.name,
      city: propertyProfile.city,
      state: propertyProfile.state,
      chainScale: propertyProfile.chainScale,
      affiliation: propertyProfile.affiliation,
      rooms: propertyProfile.rooms,
      website: propertyProfile.website || null,
      ownedDomains: propertyProfile.ownedDomains || [],
      officialBrandDomain: propertyProfile.officialBrandDomain || null,
      officialPropertyPageUrl: propertyProfile.officialPropertyPageUrl || null,
    },
    period: {
      periodId: period.periodId,
      executionDate: period.executionDate,
      scenarioCount: scenarios.length,
      providerCount: period.providerCount,
      status: period.status,
    },
    demandCapture,
    intentPresenceIndex,
    lostDemand: {
      totalLost: lostDemand.totalLost,
      highRelevanceLost: lostDemand.highRelevanceLost,
      displacement: lostDemand.displacement.slice(0, 5),
      topReasons: lostDemand.topReasons,
      scenarios: lostDemand.scenarios.slice(0, 10).map(sanitizeLostScenario),
    },
    competitiveSet: {
      declaredCount: competitiveSet.declaredCount,
      observedCount: competitiveSet.observedCount,
      overlapRate: competitiveSet.overlapRate,
      surprises: competitiveSet.surprises,
      observed: competitiveSet.observed.slice(0, 10),
      declaredButNotObserved: competitiveSet.declaredButNotObserved,
    },
    realityGap,
    whiteSpace: {
      totalOpportunities: whiteSpace.totalOpportunities,
      highOpportunities: whiteSpace.highOpportunities,
      opportunities: whiteSpace.opportunities.map(sanitizeWhiteSpace),
    },
    brief,
    actions,
    evidence: computeEvidence(observations, scenarios, propertyProfile),
  };

  try {
    const executiveMetrics = buildOptionalExecutiveMetrics(period, scenarios, propertyProfile, {
      allPeriods: options.allPeriods,
    });
    if (executiveMetrics) payload.executiveMetrics = executiveMetrics;
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] optional executiveMetrics build failed:", err.message);
    }
  }

  try {
    payload.competitiveRankingByTerritory = attachDisplacementToCompetitiveRanking(
      buildAllTerritoryCompetitiveRankings(observations, scenarios, propertyProfile),
      observations,
      scenarios,
      propertyProfile
    );
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] competitiveRankingByTerritory build failed:", err.message);
    }
  }

  try {
    payload.executiveRead = buildExecutiveReadWithUx(payload, period, scenarios, propertyProfile, {
      allPeriods: options.allPeriods,
    });
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] executiveRead build failed:", err.message);
    }
  }

  return payload;
}

function computeEvidence(observations, scenarios, propertyProfile = null) {
  // Source landscape — count how many observations cite each domain (deduplicated per obs)
  const domainCounts = {};
  let totalWithSources = 0;
  for (const obs of observations) {
    if (obs.sourcesCited && obs.sourcesCited.length) {
      totalWithSources++;
      const seenDomains = new Set();
      for (const src of obs.sourcesCited) {
        const url = src.url || "";
        try {
          const domain = new URL(url).hostname.replace(/^www\./, "");
          if (!seenDomains.has(domain)) {
            seenDomains.add(domain);
            domainCounts[domain] = (domainCounts[domain] || 0) + 1;
          }
        } catch (_) {}
      }
    }
  }
  const topSources = Object.entries(domainCounts)
    .map(([domain, count]) => ({ domain, count, frequency: totalWithSources > 0 ? roundAdpPercent((count / totalWithSources) * 100) : 0 }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  // Citation intelligence
  const totalObs = observations.length;
  const citationRate = totalObs > 0 ? roundAdpPercent((totalWithSources / totalObs) * 100) : 0;
  const avgSourcesPerCitation = totalWithSources > 0
    ? Math.round(observations.reduce((sum, o) => sum + (o.sourcesCited?.length || 0), 0) / totalWithSources * 10) / 10
    : 0;

  // Provider visibility
  const providerStats = {};
  for (const obs of observations) {
    if (!providerStats[obs.provider]) providerStats[obs.provider] = { total: 0, mentioned: 0, withCitations: 0 };
    providerStats[obs.provider].total++;
    if (obs.mentioned) providerStats[obs.provider].mentioned++;
    if (obs.sourcesCited?.length) providerStats[obs.provider].withCitations++;
  }
  const providers = Object.entries(providerStats).map(([provider, s]) => ({
    provider,
    total: s.total,
    mentioned: s.mentioned,
    presence: s.total > 0 ? roundAdpPercent((s.mentioned / s.total) * 100) : 0,
  })).sort((a, b) => b.presence - a.presence);
  const providerCitations = {};
  for (const [provider, s] of Object.entries(providerStats)) {
    providerCitations[provider] = s.total > 0 ? roundAdpPercent((s.withCitations / s.total) * 100) : 0;
  }

  // Discovery by intent
  const intentStats = {};
  for (const obs of observations) {
    const scenario = scenarios.find((s) => s.scenarioId === obs.scenarioId);
    if (!scenario) continue;
    const intent = scenario.intent;
    if (!intentStats[intent]) intentStats[intent] = { total: 0, withSources: 0 };
    intentStats[intent].total++;
    if (obs.sourcesCited?.length) intentStats[intent].withSources++;
  }
  const discovery = Object.entries(intentStats)
    .map(([intent, s]) => ({ intent, citationRate: s.total > 0 ? roundAdpPercent((s.withSources / s.total) * 100) : 0, total: s.total }))
    .sort((a, b) => b.citationRate - a.citationRate);

  const ownedMix = computeOwnedExternalSourceMix(observations, propertyProfile);

  return {
    citationRate,
    avgSourcesPerCitation,
    totalWithSources,
    totalObservations: totalObs,
    topSources,
    providers,
    providerCitations,
    discovery,
    ownedSourceShare: ownedMix.ownedShare,
    externalSourceShare: ownedMix.externalShare,
    unknownSourceShare: ownedMix.unknownShare,
    ownedSourceMix: ownedMix,
    ownedDomainsConfigured: ownedMix.domainsConfigured,
  };
}

function sanitizeLostScenario(scenario) {
  return {
    intent: scenario.intent,
    competitorsPresent: scenario.competitorsPresent.slice(0, 5),
    likelyReason: scenario.likelyReason,
    relevance: scenario.relevance,
  };
}

function sanitizeWhiteSpace(opportunity) {
  return {
    intent: opportunity.intent,
    ownerIntentSummary: opportunity.ownerIntentSummary,
    opportunityScore: opportunity.opportunityScore,
    currentOwnership: opportunity.currentOwnership,
    rationale: opportunity.rationale,
    topCompetitors: opportunity.topCompetitors,
  };
}

function generateActionRecommendations(demandCapture, lostDemand, realityGap, whiteSpace, profile) {
  const actions = [];

  const highGaps = realityGap.gaps.filter((g) => g.severity === "HIGH");
  if (highGaps.length) {
    actions.push({
      priority: "HIGH",
      category: "REALITY_GAP",
      title: `Improve AI representation of ${highGaps[0].label.toLowerCase()}`,
      description: `AI currently misses or underrepresents your ${highGaps[0].label.toLowerCase()}. Review third-party content and authority signals for this attribute before estimating impact.`,
      // Recovery: no unsupported numeric causal impact claims.
      expectedImpact: null,
      impactNote: "Review evidence for this attribute gap before estimating scenario impact.",
    });
  }

  if (whiteSpace.highOpportunities > 0) {
    actions.push({
      priority: "HIGH",
      category: "WHITE_SPACE",
      title: `Pursue ${whiteSpace.highOpportunities} high-potential demand opportunities`,
      description: `${whiteSpace.highOpportunities} demand scenarios have weak competitor ownership where your property attributes align. Building authority here could establish category ownership.`,
      expectedImpact: null,
      impactNote: "Opportunity count reflects weak competitor ownership, not validated capture uplift.",
    });
  }

  if (lostDemand.displacement.length) {
    const top = lostDemand.displacement[0];
    actions.push({
      priority: "MEDIUM",
      category: "DISPLACEMENT",
      title: `Address displacement by ${top.name}`,
      description: `${top.name} appears in ${top.displacementCount} scenarios where you are absent. Analyze their positioning strengths and differentiate.`,
      expectedImpact: null,
      impactNote: "Displacement count is observational; expected reduction is not certified.",
    });
  }

  if (demandCapture.overallRate < 40) {
    actions.push({
      priority: "MEDIUM",
      category: "GENERAL",
      title: "Strengthen overall AI authority signals",
      description: "Your demand capture rate is below 40%. Consider improving third-party review presence, structured data, and authoritative content across all demand segments.",
      expectedImpact: null,
      impactNote: "Broad authority work may help multiple segments; no numeric uplift claimed.",
    });
  }

  return actions.slice(0, 5);
}

function generateOwnerBrief(demandCapture, lostDemand, competitiveSet, realityGap, whiteSpace, profile) {
  const items = [];

  items.push({
    type: "headline",
    text: `Your property captured ${demandCapture.display} of relevant AI demand scenarios this period.`,
  });

  if (lostDemand.highRelevanceLost > 0) {
    items.push({
      type: "risk",
      text: `${lostDemand.highRelevanceLost} high-relevance demand scenarios did not include your property.`,
    });
  }

  if (competitiveSet.surprises.length) {
    items.push({
      type: "insight",
      text: `${competitiveSet.surprises.length} competitor(s) consistently appear in AI recommendations that are not in your declared comp set.`,
    });
  }

  if (whiteSpace.highOpportunities > 0) {
    items.push({
      type: "opportunity",
      text: `${whiteSpace.highOpportunities} high-potential demand opportunities identified where no competitor dominates.`,
    });
  }

  if (realityGap.gapScore > 20) {
    items.push({
      type: "gap",
      text: `AI Reality Gap: ${realityGap.display}. AI misses or misrepresents ${realityGap.gapCount} of your property's key attributes.`,
    });
  }

  return { items: items.slice(0, 5), generatedAt: new Date().toISOString() };
}

/**
 * LEGACY declared-comp AI Presence Index.
 * DEPRECATED_CUSTOMER_RENDER — INTERNAL_ROLLBACK_ONLY.
 */
const ADP_INDEX_MIN_AVG_COMP_RATE = 30;
const ADP_INDEX_MAX = 200;

export function computeIntentPresenceIndexLegacy(observations, scenarios, propertyProfile, demandCapture) {
  return computeIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture);
}

/** @deprecated Use computeIntentPresenceIndexLegacy — customer render is governed CORE index. */
export function computeIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture) {
  const declared = (propertyProfile.declaredCompSet || []).map((d) => d.toLowerCase());
  if (!declared.length) return {};

  function isDeclaredComp(name) {
    const nLow = name.toLowerCase();
    for (const d of declared) {
      if (d === nLow || d.includes(nLow) || nLow.includes(d)) return true;
      const nWords = nLow.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 2);
      const dWords = d.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 2);
      const overlap = nWords.filter(w => dWords.includes(w));
      if (overlap.length >= 2 && overlap.length >= nWords.length * 0.6) return true;
    }
    return false;
  }

  const scenariosByIntent = {};
  for (const s of scenarios) {
    if (!scenariosByIntent[s.intent]) scenariosByIntent[s.intent] = [];
    scenariosByIntent[s.intent].push(s.scenarioId);
  }

  const result = {};
  for (const [intent, scenarioIds] of Object.entries(scenariosByIntent)) {
    const intentObs = observations.filter((o) => scenarioIds.includes(o.scenarioId));
    if (!intentObs.length) continue;

    const totalScenarios = scenarioIds.length;

    // Per-scenario presence for core competitors (scenario-level dedup, same as property rate)
    const coreCompScenarios = new Set();
    const compScenarioCounts = {};
    for (const d of declared) compScenarioCounts[d] = new Set();

    for (const obs of intentObs) {
      const competitors = obs.competitorsMentioned || [];
      for (const comp of competitors) {
        if (isDeclaredComp(comp)) {
          // Find which declared entry it matches and credit that entry
          for (const d of declared) {
            const cLow = comp.toLowerCase();
            if (d === cLow || d.includes(cLow) || cLow.includes(d)) {
              compScenarioCounts[d].add(obs.scenarioId);
              break;
            }
          }
        }
      }
    }

    const compRates = Object.values(compScenarioCounts).map((s) => (s.size / totalScenarios) * 100);
    const participatingComps = compRates.filter((r) => r > 0).length;
    const avgCompRate = participatingComps >= 3
      ? compRates.filter((r) => r > 0).reduce((a, b) => a + b, 0) / participatingComps
      : 0;

    const myRate = demandCapture.byIntent[intent]?.rate || 0;
    let index = null;
    if (avgCompRate >= ADP_INDEX_MIN_AVG_COMP_RATE && participatingComps >= 3) {
      index = Math.min(Math.round((myRate / avgCompRate) * 100), ADP_INDEX_MAX);
    }

    result[intent] = { index, myRate, avgCompRate: roundAdpPercent(avgCompRate) };
  }

  return result;
}
