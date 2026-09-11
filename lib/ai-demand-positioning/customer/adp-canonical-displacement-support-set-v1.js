/**
 * ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1
 *
 * Single source of truth for Competitive Displacement:
 *   displayed scenario count === drawer unique scenario count
 *   === uniqueScenarioIds.length on this support set
 *
 * Doctrine:
 *   DISPLAYED_DISPLACEMENT_COUNT_MUST_EQUAL_EVIDENCE_BACKED_UNIQUE_SCENARIO_COUNT
 *   ADP_DISPLACEMENT_COUNT_AND_EVIDENCE_SINGLE_SOURCE
 *   ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN. — presentation/evidence packaging only.
 */

import {
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
  resolveDisplacementEvidence,
  computeDisplacementCountsByEntity,
} from "./resolve-displacement-evidence-v1.js";
import { OVERALL_RANKING_KEY } from "./competitive-ranking-overall-view-v1.js";
import { hotelById } from "../metrics/presence-benchmark-v1.js";

export const ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1 =
  "ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1";
export const ADP_DISPLACEMENT_COUNT_AND_EVIDENCE_SINGLE_SOURCE =
  "ADP_DISPLACEMENT_COUNT_AND_EVIDENCE_SINGLE_SOURCE";
export const ADP_DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY =
  "ADP_DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY";
export const ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED =
  "ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED";
export const SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET =
  "SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET";
export const ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1 =
  "ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1";

export const DISPLACEMENT_SUPPORT_SET_VERSION =
  "adp_canonical_displacement_support_set_v1";

function displayName(entityId, propertyProfile, fallback) {
  const hotel = hotelById(entityId, propertyProfile);
  if (hotel?.canonical) return hotel.canonical;
  if (fallback) return String(fallback);
  return String(entityId || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Build one competitor support set for a scope from immutable observations.
 * Count grain = unique scenarioId with resolvable raw response (same as resolver).
 */
export function buildCanonicalDisplacementSupportSet({
  propertyProfile,
  observations,
  scenarios,
  competitorId,
  competitorName = null,
  scope = "overall",
  periodMeta = {},
} = {}) {
  const resolved = resolveDisplacementEvidence({
    propertyProfile,
    observations,
    scenarios,
    competitorId,
    competitorName,
    scope,
    periodMeta,
    maxItems: 200,
  });

  const uniqueScenarioIds = [];
  const supportingObservationIds = [];
  const supportingProviders = [];
  const supportingEvidenceRefs = [];
  const evidenceByScenario = [];

  for (const item of resolved.evidence || []) {
    const sid = item.scenarioId;
    if (!sid || uniqueScenarioIds.includes(sid)) {
      // Additional provider observations for an already-counted scenario
      if (sid && item.observationId) {
        supportingObservationIds.push(item.observationId);
        if (item.provider && !supportingProviders.includes(item.provider)) {
          supportingProviders.push(item.provider);
        }
      }
      continue;
    }
    uniqueScenarioIds.push(sid);
    if (item.observationId) supportingObservationIds.push(item.observationId);
    if (item.provider && !supportingProviders.includes(item.provider)) {
      supportingProviders.push(item.provider);
    }
    supportingEvidenceRefs.push({
      scenarioId: sid,
      observationId: item.observationId,
      provider: item.provider,
      evidenceType: item.evidenceType || "competitive_displacement",
      periodId: item.periodId || periodMeta.periodId || null,
    });
    evidenceByScenario.push(item);
  }

  const displacementScenarioCount = uniqueScenarioIds.length;

  return Object.freeze({
    contract: ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
    version: DISPLACEMENT_SUPPORT_SET_VERSION,
    resolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    subjectPropertyId: propertyProfile?.propertyId || periodMeta.propertyId || null,
    periodId: periodMeta.periodId || null,
    competitorCanonicalHotelId: resolved.competitorId || competitorId || null,
    competitorEntityId: resolved.competitorId || competitorId || null,
    competitorName:
      resolved.competitorName ||
      displayName(resolved.competitorId || competitorId, propertyProfile, competitorName),
    scope: resolved.scope || { type: scope === "overall" ? "overall" : "demand_territory", key: scope },
    uniqueScenarioIds: Object.freeze([...uniqueScenarioIds]),
    supportingObservationIds: Object.freeze([...supportingObservationIds]),
    supportingProviders: Object.freeze([...supportingProviders]),
    supportingEvidenceRefs: Object.freeze(supportingEvidenceRefs),
    evidence: Object.freeze(evidenceByScenario),
    displacementScenarioCount,
    displayedCount: displacementScenarioCount,
    supportCount: displacementScenarioCount,
    evidenceAvailable: displacementScenarioCount > 0,
    claim: Object.freeze({
      claimId: `displacement:${propertyProfile?.propertyId}:${periodMeta.periodId}:${resolved.competitorId || competitorId}:${scope}`,
      claimType: "competitive_displacement",
      aggregationGrain: "unique_scenario_id",
      supportIds: Object.freeze([...uniqueScenarioIds]),
      displayedCount: displacementScenarioCount,
      supportCount: displacementScenarioCount,
    }),
  });
}

/**
 * Build overall-scope support sets for every evidence-backed competitor.
 */
export function buildAllCanonicalDisplacementSupportSets({
  propertyProfile,
  observations,
  scenarios,
  periodMeta = {},
  scope = "overall",
} = {}) {
  const counts = computeDisplacementCountsByEntity(
    observations,
    scenarios,
    propertyProfile,
    scope
  );
  const byCompetitor = Object.create(null);
  for (const entityId of Object.keys(counts).sort()) {
    byCompetitor[entityId] = buildCanonicalDisplacementSupportSet({
      propertyProfile,
      observations,
      scenarios,
      competitorId: entityId,
      scope,
      periodMeta,
    });
  }
  return Object.freeze({
    contract: ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
    version: DISPLACEMENT_SUPPORT_SET_VERSION,
    singleSource: SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET,
    publishedSelfContained: ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED,
    subjectPropertyId: propertyProfile?.propertyId || periodMeta.propertyId || null,
    periodId: periodMeta.periodId || null,
    scope,
    scopeKey: scope === "overall" || !scope ? OVERALL_RANKING_KEY : scope,
    byCompetitor: Object.freeze(byCompetitor),
    competitorIds: Object.freeze(Object.keys(byCompetitor)),
  });
}

/**
 * Top displacement rows for lostDemand.displacement — counts from support sets only.
 */
export function displacementRowsFromSupportBundle(bundle, { limit = 10 } = {}) {
  const rows = Object.values(bundle?.byCompetitor || {})
    .filter((s) => s.displacementScenarioCount > 0)
    .map((s) => ({
      entityId: s.competitorEntityId,
      name: s.competitorName,
      displacementCount: s.displacementScenarioCount,
      evidenceAvailable: true,
      supportSetVersion: DISPLACEMENT_SUPPORT_SET_VERSION,
    }))
    .sort((a, b) => b.displacementCount - a.displacementCount || String(a.name).localeCompare(String(b.name)));
  return rows.slice(0, limit);
}

/**
 * Look up a published support set for drawer serving.
 */
export function getPublishedDisplacementSupportSet(evidenceIndex, competitorId, scope = "overall") {
  if (!evidenceIndex || !competitorId) return null;
  const bundle =
    evidenceIndex.displacementSupportSets ||
    evidenceIndex.canonicalDisplacementSupport ||
    null;
  if (!bundle) return null;

  const scopeKey =
    scope === "overall" || scope === OVERALL_RANKING_KEY || scope === "OVERALL" || !scope
      ? "overall"
      : String(scope);

  // Preferred shape: { overall: { byCompetitor }, byTerritory: { business: … } }
  if (bundle.overall || bundle.byTerritory) {
    if (scopeKey === "overall") {
      return bundle.overall?.byCompetitor?.[competitorId] || null;
    }
    return bundle.byTerritory?.[scopeKey]?.byCompetitor?.[competitorId] || null;
  }

  // Flat overall bundle
  if (bundle.byCompetitor) {
    if (scopeKey !== "overall" && bundle.scope && bundle.scope !== "overall") {
      if (bundle.scope !== scopeKey) return null;
    }
    return bundle.byCompetitor[competitorId] || null;
  }

  return null;
}

/**
 * Exact parity check for one competitor row.
 */
export function assertDisplacementCountEvidenceExactParity({
  displayedScenarioCount,
  supportSet,
} = {}) {
  const supportCount = supportSet?.displacementScenarioCount ?? supportSet?.uniqueScenarioIds?.length ?? 0;
  const drawerCount = Array.isArray(supportSet?.evidence)
    ? new Set(supportSet.evidence.map((e) => e.scenarioId).filter(Boolean)).size
    : supportCount;
  const displayed = Number(displayedScenarioCount) || 0;
  const pass =
    displayed === supportCount &&
    supportCount === drawerCount &&
    (displayed === 0 || (supportSet?.evidenceAvailable === true && drawerCount > 0));

  return {
    gate: ADP_DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY,
    pass,
    displayedScenarioCount: displayed,
    canonicalSupportScenarioCount: supportCount,
    drawerScenarioCount: drawerCount,
    observationCount: supportSet?.supportingObservationIds?.length ?? 0,
  };
}
