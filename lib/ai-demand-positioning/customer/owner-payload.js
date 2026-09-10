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
import { composeExecutiveReadV3 } from "../executive-read-v3/compose-executive-read-v3.js";
import {
  attachInactiveExecutiveReadV3,
  resolveExecutiveReadCompositionWritePolicyV3,
} from "../executive-read-v3/historical-immutability-v3.js";
import { COMPOSITION_V3_STATUS } from "../governance/adp-executive-read-composition-v3.js";
import { buildAllTerritoryCompetitiveRankings } from "./competitive-ranking-overall-view-v1.js";
import { attachDisplacementToCompetitiveRanking } from "./resolve-displacement-evidence-v1.js";
import { computeOwnedExternalSourceMix } from "../metrics/owned-source-classification-v1.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import { filterCustomerFacingEntityNames } from "./customer-entity-resolution-v1.js";
import {
  buildProviderPresenceRows,
  ADP_PROVIDER_DENOMINATOR_GRAIN,
} from "../metrics/provider-presence-v1.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import {
  ADP_METRIC_SCOPE_LABELS_V1,
  attachMetricScopeLabels,
} from "../metrics/metric-scope-labels-v1.js";
import { deriveTopObservedAiAlternative } from "./top-observed-ai-alternative-v1.js";
import { buildOverallCompetitiveRanking } from "./competitive-ranking-overall-view-v1.js";

export const ADP_PRODUCT_VERSION = "ai_demand_positioning_v1";

export { ADP_PROVIDER_DENOMINATOR_GRAIN };

export function buildOwnerPayload(period, scenarios, propertyProfile, options = {}) {
  if (!period || !period.observations?.length) {
    return { ok: false, error: "no_data", message: "No monitoring data available for this property." };
  }

  const parsed = period.observations.filter((o) => o.parsed);
  if (!parsed.length) {
    return { ok: false, error: "not_parsed", message: "Monitoring data has not been processed yet." };
  }

  // Single canonical subject-presence path for all customer metrics (no Path-B rewrite).
  const observations = enrichObservationsWithRank(parsed, propertyProfile);

  const demandCapture = computeDemandCaptureIndex(observations, scenarios);
  const lostDemand = computeLostDemand(observations, scenarios, propertyProfile);
  const competitiveSet = computeCompetitiveSet(observations, propertyProfile);
  try {
    const overallRanking = buildOverallCompetitiveRanking(observations, scenarios, propertyProfile);
    const topAlt = deriveTopObservedAiAlternative(observations, propertyProfile, {
      overallRanking,
      scenarios,
    });
    if (topAlt?.topObservedAlternative) {
      competitiveSet.topObservedAlternative = topAlt.topObservedAlternative;
      competitiveSet.topObservedAlternativeMeta = {
        version: topAlt.version,
        rankingBasis: topAlt.rankingBasis,
        tiedMaxSet: topAlt.tiedMaxSet,
      };
    }
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] topObservedAlternative attach failed:", err.message);
    }
  }
  const realityGap = computeRealityGap(observations, propertyProfile);
  const whiteSpace = computeWhiteSpace(observations, scenarios, propertyProfile);

  const intentPresenceIndex = buildGovernedIntentPresenceIndex(observations, scenarios, propertyProfile);

  const actions = generateActionRecommendations(demandCapture, lostDemand, realityGap, whiteSpace, propertyProfile);
  const brief = generateOwnerBrief(demandCapture, lostDemand, competitiveSet, realityGap, whiteSpace, propertyProfile);

  // Provider presence scheduled counts must include failed/unparsed attempts from the full period ledger.
  const evidence = computeEvidence(period.observations || observations, scenarios, propertyProfile);

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
      scenarios: lostDemand.scenarios.slice(0, 10).map((s) => sanitizeLostScenario(s, propertyProfile)),
    },
    competitiveSet: {
      declaredCount: competitiveSet.declaredCount,
      observedCount: competitiveSet.observedCount,
      overlapRate: competitiveSet.overlapRate,
      surprises: competitiveSet.surprises,
      observed: competitiveSet.observed.slice(0, 10),
      declaredButNotObserved: competitiveSet.declaredButNotObserved,
      ...(competitiveSet.topObservedAlternative
        ? {
            topObservedAlternative: competitiveSet.topObservedAlternative,
            topObservedAlternativeMeta: competitiveSet.topObservedAlternativeMeta || null,
          }
        : {}),
    },
    realityGap,
    whiteSpace: {
      totalOpportunities: whiteSpace.totalOpportunities,
      highOpportunities: whiteSpace.highOpportunities,
      opportunities: whiteSpace.opportunities.map(sanitizeWhiteSpace),
    },
    brief,
    actions,
    evidence,
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

  // V3 structured composition — inactive preview attach OR customer-default when activated.
  // Does not overwrite writeup/ux. Does not mutate issued history writeup.
  try {
    if (payload.executiveRead && options.attachExecutiveReadV3Preview !== false) {
      const v3Activated = COMPOSITION_V3_STATUS.activated === true;
      const v3 = composeExecutiveReadV3(payload, {
        propertyId: propertyProfile.propertyId,
        market: propertyProfile.market,
        distributionStatus: options.distributionStatus || "INTERNAL_ONLY",
        zeroCodePath: true,
        allowNewEdition: options.allowNewEdition === true || v3Activated,
      });
      if (v3?.ok) {
        if (v3Activated) {
          // Customer default: stamp compositionVersion + sections; preserve writeup.
          const beforeWriteup = payload.executiveRead.writeup;
          const beforeUx = payload.executiveRead.ux;
          payload.executiveRead = {
            ...payload.executiveRead,
            compositionVersion: v3.compositionVersion,
            compositionContract: v3.compositionContract,
            sections: v3.sections,
            numericAnchors: v3.numericAnchors,
            primaryIssueId: v3.primaryIssueId,
            insightArchetype: v3.insightArchetype,
            evidenceTrace: v3.evidenceTrace,
            qualityGates: v3.qualityGates,
            sourceSnapshotHash: v3.sourceSnapshotHash,
            compositionHash: v3.compositionHash,
            compositionV3: {
              ...v3,
              activated: true,
              customerFacingDefault: true,
            },
            writeup: beforeWriteup,
            ux: beforeUx,
          };
        } else {
          const writePolicy = resolveExecutiveReadCompositionWritePolicyV3({
            distributionStatus: options.distributionStatus || "INTERNAL_ONLY",
            existingCompositionVersion: payload.executiveRead.compositionVersion || null,
            v3Activated: false,
          });
          const attached = attachInactiveExecutiveReadV3(payload.executiveRead, v3, writePolicy);
          payload.executiveRead = attached.executiveRead;
        }
      } else if (v3?.reason === "EXECUTIVE_READ_REVIEW_REQUIRED") {
        payload.executiveRead = {
          ...payload.executiveRead,
          compositionV3: {
            ok: false,
            activated: false,
            reason: v3.reason,
            detail: v3.detail,
            compositionVersion: v3.compositionVersion,
          },
        };
      }
    }
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.error("[ADP] executiveRead V3 attach failed:", err.message);
    }
  }

  return attachMetricScopeLabels(payload);
}

function computeEvidence(observations, scenarios, propertyProfile = null) {
  const comparableObs = filterComparableObservations(observations);

  // Source landscape — comparable observations only (deduplicated per obs)
  const domainCounts = {};
  let totalWithSources = 0;
  for (const obs of comparableObs) {
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

  // Citation intelligence (comparable observations only — failed/missing provider rows excluded)
  const totalObs = comparableObs.length;
  const citationRate = totalObs > 0 ? roundAdpPercent((totalWithSources / totalObs) * 100) : 0;
  const avgSourcesPerCitation = totalWithSources > 0
    ? Math.round(comparableObs.reduce((sum, o) => sum + (o.sourcesCited?.length || 0), 0) / totalWithSources * 10) / 10
    : 0;

  const { providers, providerCitations } = buildProviderPresenceRows(observations);

  // Discovery by intent (comparable observations only)
  const intentStats = {};
  for (const obs of comparableObs) {
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

  const ownedMix = computeOwnedExternalSourceMix(comparableObs, propertyProfile);

  return {
    citationRate,
    avgSourcesPerCitation,
    totalWithSources,
    totalObservations: totalObs,
    scheduledObservations: observations.length,
    comparableObservations: totalObs,
    providerDenominatorGrain: ADP_PROVIDER_DENOMINATOR_GRAIN,
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

function sanitizeLostScenario(scenario, propertyProfile) {
  return {
    intent: scenario.intent,
    competitorsPresent: filterCustomerFacingEntityNames(
      scenario.competitorsPresent || [],
      propertyProfile
    ).slice(0, 5),
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
