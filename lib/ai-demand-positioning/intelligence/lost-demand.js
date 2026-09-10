/**
 * AI Demand Positioning — Lost Demand Analysis.
 * For each scenario where property is NOT mentioned: who IS, and why?
 *
 * SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET:
 * Displacement counts are delegated to ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1
 * (same object as the evidence drawer). This module retains lost-scenario
 * narrative (reasons / relevance) only.
 */

import {
  resolveCustomerFacingEntity,
} from "../customer/customer-entity-resolution-v1.js";
import {
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
} from "../customer/resolve-displacement-evidence-v1.js";
import {
  buildAllCanonicalDisplacementSupportSets,
  displacementRowsFromSupportBundle,
  SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET,
  ADP_DISPLACEMENT_COUNT_AND_EVIDENCE_SINGLE_SOURCE,
  DISPLACEMENT_SUPPORT_SET_VERSION,
} from "../customer/adp-canonical-displacement-support-set-v1.js";

export const SINGLE_CANONICAL_DISPLACEMENT_AGGREGATOR =
  SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET;
export const ADP_LOST_DEMAND_DISPLACEMENT_DELEGATE_VERSION =
  "adp_lost_demand_displacement_delegate_v2_support_set";

export function computeLostDemand(observations, scenarios, propertyProfile) {
  const scenarioMentions = new Map();
  for (const obs of observations) {
    if (!scenarioMentions.has(obs.scenarioId)) {
      scenarioMentions.set(obs.scenarioId, { mentioned: false, competitors: [], providers: [] });
    }
    const entry = scenarioMentions.get(obs.scenarioId);
    entry.providers.push(obs.provider);
    if (obs.mentioned) entry.mentioned = true;
    for (const comp of obs.competitorsMentioned || []) {
      if (!entry.competitors.includes(comp)) entry.competitors.push(comp);
    }
  }

  const lostDemand = [];
  for (const scenario of scenarios) {
    const result = scenarioMentions.get(scenario.scenarioId);
    if (result?.mentioned) continue;

    lostDemand.push({
      scenarioId: scenario.scenarioId,
      intent: scenario.intent,
      ownerIntent: scenario.query,
      competitorsPresent: result?.competitors || [],
      providerCount: result?.providers?.length || 0,
      likelyReason: inferLossReason(scenario, result, propertyProfile),
      relevance: computeRelevance(scenario, propertyProfile),
    });
  }

  lostDemand.sort((a, b) => {
    const relOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
    return (relOrder[a.relevance] || 2) - (relOrder[b.relevance] || 2);
  });

  const displacementAgg = aggregateDisplacementFromCanonical(
    observations,
    scenarios,
    propertyProfile
  );

  return {
    totalLost: lostDemand.length,
    highRelevanceLost: lostDemand.filter((l) => l.relevance === "HIGH").length,
    scenarios: lostDemand,
    displacement: displacementAgg,
    topReasons: aggregateReasons(lostDemand),
    displacementSource: SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET,
    displacementResolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    displacementSupportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
    lostDemandDisplacementDelegateVersion: ADP_LOST_DEMAND_DISPLACEMENT_DELEGATE_VERSION,
    singleSource: ADP_DISPLACEMENT_COUNT_AND_EVIDENCE_SINGLE_SOURCE,
  };
}

function inferLossReason(scenario, result, profile) {
  const query = scenario.query.toLowerCase();
  if (query.includes("meeting") || query.includes("event") || query.includes("retreat")) {
    return "Meeting/event capability may be underrepresented in AI sources";
  }
  if (query.includes("marina") || query.includes("boat")) {
    return "Marina/waterfront attribute not consistently recognized by AI";
  }
  if (query.includes("luxury") || query.includes("five star")) {
    return "Property may not be positioned as luxury tier by AI";
  }
  if ((result?.competitors || []).length > 3) {
    return "High competitor density. Stronger authority signals needed";
  }
  if (query.includes("family") && !profile.attributes?.includes("family_friendly")) {
    return "Property not strongly associated with this traveler segment";
  }
  return "Competitor has stronger authority or recency signals for this demand";
}

function computeRelevance(scenario, profile) {
  const query = scenario.query.toLowerCase();
  const attrs = profile.attributes || [];
  let score = 0;
  if (query.includes("boca raton")) score += 2;
  if (query.includes("waterfront") || query.includes("water")) score += attrs.includes("waterfront") ? 2 : 0;
  if (query.includes("marina") || query.includes("boat")) score += attrs.includes("marina") ? 3 : 0;
  if (query.includes("meeting") || query.includes("event")) score += attrs.includes("meeting_space") ? 2 : 0;
  if (query.includes("upscale") || query.includes("resort")) score += 1;
  if (scenario.source === "property_specific") score += 2;
  if (score >= 4) return "HIGH";
  if (score >= 2) return "MEDIUM";
  return "LOW";
}

/**
 * Thin delegate to ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1.
 * Displayed count === uniqueScenarioIds.length on the same object the drawer uses.
 */
function aggregateDisplacementFromCanonical(observations, scenarios, propertyProfile) {
  const bundle = buildAllCanonicalDisplacementSupportSets({
    propertyProfile,
    observations,
    scenarios,
    periodMeta: {
      propertyId: propertyProfile?.propertyId || null,
      periodId: null,
    },
    scope: "overall",
  });
  return displacementRowsFromSupportBundle(bundle, { limit: 10 });
}

/** @deprecated Kept for tests that call the old private shape — use canonical path. */
export function aggregateDisplacementLegacyForTests(lostDemand, propertyProfile) {
  const byEntity = new Map();
  for (const lost of lostDemand || []) {
    const seenInScenario = new Set();
    for (const comp of lost.competitorsPresent || []) {
      const resolved = resolveCustomerFacingEntity(comp, propertyProfile);
      if (!resolved.ok || !resolved.entityId) continue;
      if (seenInScenario.has(resolved.entityId)) continue;
      seenInScenario.add(resolved.entityId);
      const prev = byEntity.get(resolved.entityId);
      byEntity.set(resolved.entityId, {
        name: resolved.displayName || prev?.name || comp,
        displacementCount: (prev?.displacementCount || 0) + 1,
        entityId: resolved.entityId,
      });
    }
  }
  return [...byEntity.values()]
    .sort((a, b) => b.displacementCount - a.displacementCount)
    .slice(0, 10);
}

function aggregateReasons(lostDemand) {
  const reasons = {};
  for (const lost of lostDemand) {
    reasons[lost.likelyReason] = (reasons[lost.likelyReason] || 0) + 1;
  }
  return Object.entries(reasons)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([reason, count]) => ({ reason, count }));
}
