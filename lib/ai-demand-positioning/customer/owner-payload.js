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

export const ADP_PRODUCT_VERSION = "ai_demand_positioning_v1";

export function buildOwnerPayload(period, scenarios, propertyProfile) {
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

  // Compute per-intent presence index vs core comp set average
  const intentPresenceIndex = computeIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture);

  const actions = generateActionRecommendations(demandCapture, lostDemand, realityGap, whiteSpace, propertyProfile);
  const brief = generateOwnerBrief(demandCapture, lostDemand, competitiveSet, realityGap, whiteSpace, propertyProfile);

  return {
    ok: true,
    version: ADP_PRODUCT_VERSION,
    property: {
      name: propertyProfile.name,
      city: propertyProfile.city,
      state: propertyProfile.state,
      chainScale: propertyProfile.chainScale,
      affiliation: propertyProfile.affiliation,
      rooms: propertyProfile.rooms,
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
    evidence: computeEvidence(observations, scenarios),
  };
}

function computeEvidence(observations, scenarios) {
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
    .map(([domain, count]) => ({ domain, count, frequency: totalWithSources > 0 ? Math.round((count / totalWithSources) * 1000) / 10 : 0 }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  // Citation intelligence
  const totalObs = observations.length;
  const citationRate = totalObs > 0 ? Math.round((totalWithSources / totalObs) * 1000) / 10 : 0;
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
    presence: s.total > 0 ? Math.round((s.mentioned / s.total) * 1000) / 10 : 0,
  })).sort((a, b) => b.presence - a.presence);
  const providerCitations = {};
  for (const [provider, s] of Object.entries(providerStats)) {
    providerCitations[provider] = s.total > 0 ? Math.round((s.withCitations / s.total) * 1000) / 10 : 0;
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
    .map(([intent, s]) => ({ intent, citationRate: s.total > 0 ? Math.round((s.withSources / s.total) * 1000) / 10 : 0, total: s.total }))
    .sort((a, b) => b.citationRate - a.citationRate);

  return {
    citationRate,
    avgSourcesPerCitation,
    totalWithSources,
    totalObservations: totalObs,
    topSources,
    providers,
    providerCitations,
    discovery,
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
      description: `AI currently misses or underrepresents your ${highGaps[0].label.toLowerCase()}. Strengthening third-party content and authority signals for this attribute would increase demand capture.`,
      expectedImpact: "Could improve capture in " + Math.min(highGaps.length * 3, 15) + "+ scenarios",
    });
  }

  if (whiteSpace.highOpportunities > 0) {
    actions.push({
      priority: "HIGH",
      category: "WHITE_SPACE",
      title: `Pursue ${whiteSpace.highOpportunities} high-potential demand opportunities`,
      description: `${whiteSpace.highOpportunities} demand scenarios have weak competitor ownership where your property attributes align. Building authority here could establish category ownership.`,
      expectedImpact: `${whiteSpace.highOpportunities} new demand scenarios captured`,
    });
  }

  if (lostDemand.displacement.length) {
    const top = lostDemand.displacement[0];
    actions.push({
      priority: "MEDIUM",
      category: "DISPLACEMENT",
      title: `Address displacement by ${top.name}`,
      description: `${top.name} appears in ${top.displacementCount} scenarios where you are absent. Analyze their positioning strengths and differentiate.`,
      expectedImpact: `Reduce displacement in ${Math.ceil(top.displacementCount * 0.3)} scenarios`,
    });
  }

  if (demandCapture.overallRate < 40) {
    actions.push({
      priority: "MEDIUM",
      category: "GENERAL",
      title: "Strengthen overall AI authority signals",
      description: "Your demand capture rate is below 40%. Consider improving third-party review presence, structured data, and authoritative content across all demand segments.",
      expectedImpact: "Broad improvement across multiple demand categories",
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
      text: `AI Reality Gap: ${realityGap.display} — AI misses or misrepresents ${realityGap.gapCount} of your property's key attributes.`,
    });
  }

  return { items: items.slice(0, 5), generatedAt: new Date().toISOString() };
}

/**
 * Compute AI Presence Index per intent category.
 * Index = (your presence rate / core comp set average presence rate) × 100.
 * 100 = parity with core comp set, >100 = outperforming, <100 = underperforming.
 */
function computeIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture) {
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
    const index = avgCompRate > 0 && participatingComps >= 3 ? Math.round((myRate / avgCompRate) * 100) : null;

    result[intent] = { index, myRate, avgCompRate: Math.round(avgCompRate * 10) / 10 };
  }

  return result;
}
