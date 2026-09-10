/**
 * Displacement Evidence Consistency V1 — shared governed resolver.
 * Display counts and modal evidence MUST derive from this module.
 *
 * Definition (unchanged from lost-demand.js):
 * A displacement event is a monitored demand scenario where the subject hotel
 * is not mentioned in any comparable provider response for that scenario, and
 * the competitor hotel appears in at least one comparable provider response.
 *
 * Count grain = unique scenarioId within the selected scope.
 * Methodology / ranking / benchmarks are not modified here.
 *
 * Doctrine:
 * EVERY_CUSTOMER_FACING_ANALYTICAL_CLAIM_REQUIRES_CLICKABLE_SUPPORTING_EVIDENCE
 * COMPETITIVE_DISPLACEMENT_MUST_TRACE_TO_EXACT_SUPPORTING_OBSERVATIONS
 * RAW_PROVIDER_RESPONSE_MUST_PRESERVE_READABLE_TEXT_STRUCTURE
 */

import { filterComparableObservations, groupObservationsByScenario } from "../metrics/grain-governance.js";
import { canonicalizeForProperty } from "../metrics/adp-property-entity-registries.js";
import { isGovernedNonWaterstoneProperty } from "../metrics/property-core-governance-data.js";
import { canonicalizeToEntityId } from "../metrics/south-florida-entity-registry.js";
import { hotelById } from "../metrics/presence-benchmark-v1.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { OVERALL_RANKING_KEY } from "./competitive-ranking-overall-view-v1.js";
import { buildVerbatimResponseFields } from "./verbatim-evidence-response-v1.js";
import { filterCustomerFacingEntityNames } from "./customer-entity-resolution-v1.js";

export const DISPLACEMENT_EVIDENCE_RESOLVER_VERSION = "adp_displacement_evidence_consistency_v2";

export const ADP_COMPETITIVE_DISPLACEMENT_EXACT_EVIDENCE_TRACE =
  "ADP_COMPETITIVE_DISPLACEMENT_EXACT_EVIDENCE_TRACE";
export const ADP_DISPLACEMENT_AGGREGATE_REQUIRES_SUPPORTING_EVIDENCE =
  "ADP_DISPLACEMENT_AGGREGATE_REQUIRES_SUPPORTING_EVIDENCE";
export const ADP_EVIDENCE_ENTITY_RECONCILIATION_PARITY =
  "ADP_EVIDENCE_ENTITY_RECONCILIATION_PARITY";
export const ADP_COMPETITIVE_EVIDENCE_DRAWER_NONEMPTY =
  "ADP_COMPETITIVE_EVIDENCE_DRAWER_NONEMPTY";
export const ADP_DISPLACEMENT_COUNT_EVIDENCE_PARITY =
  "ADP_DISPLACEMENT_COUNT_EVIDENCE_PARITY";

export const DISPLACEMENT_EVENT_DEFINITION =
  "A displacement event is a monitored demand scenario where the subject hotel is not mentioned in any comparable provider response for that scenario, and the competitor hotel appears in at least one comparable provider response for that scenario.";

export const DISPLACEMENT_COUNT_DEFINITION =
  "Count of unique scenarioIds with a displacement event for the competitor within the selected scope (Overall = full comparable period; Demand Territory = scenarios for that intent only).";

export const DISPLAY_GRAIN = "SCENARIO_ID";
export const MODAL_GRAIN = "SCENARIO_ID";
export const EVIDENCE_GRAIN = "property × competitor × scenario (provider exemplar per scenario)";

const MAX_SOURCES = 3;
const MAX_MODAL_ITEMS = 50;

function resolveEntityId(name, propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, name);
  }
  return canonicalizeToEntityId(name);
}

function entityDisplayName(entityId, propertyProfile, fallbackName) {
  const hotel = hotelById(entityId, propertyProfile);
  if (hotel?.canonical) return hotel.canonical;
  if (fallbackName) return String(fallbackName);
  return String(entityId || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function normalizeScope(scope) {
  if (!scope || scope === OVERALL_RANKING_KEY || scope === "OVERALL" || scope?.type === "overall") {
    return { type: "overall", intent: null, key: OVERALL_RANKING_KEY };
  }
  if (typeof scope === "string") {
    return { type: "demand_territory", intent: scope, key: scope };
  }
  if (scope.type === "demand_territory" || scope.intent) {
    const intent = scope.intent || scope.key;
    return { type: "demand_territory", intent, key: intent };
  }
  return { type: "overall", intent: null, key: OVERALL_RANKING_KEY };
}

function scenarioIdsForScope(scenarios, scope) {
  const normalized = normalizeScope(scope);
  if (normalized.type === "overall") {
    return new Set((scenarios || []).map((s) => s.scenarioId));
  }
  return new Set((scenarios || []).filter((s) => s.intent === normalized.intent).map((s) => s.scenarioId));
}

/**
 * Build scenario-level lost-demand map using the certified definition,
 * with competitor names canonicalized to entityIds.
 */
export function buildDisplacementScenarioMap(observations, scenarios, propertyProfile, scope = "overall") {
  const allowed = scenarioIdsForScope(scenarios, scope);
  const comparable = filterComparableObservations(observations).filter((o) => allowed.has(o.scenarioId));
  const byScenario = groupObservationsByScenario(comparable);
  const scenarioMap = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));

  /** @type {Map<string, { scenarioId: string, intent: string, competitorEntityIds: Set<string>, competitorNamesById: Map<string,string>, observations: object[] }>} */
  const lost = new Map();

  for (const [scenarioId, obsList] of byScenario.entries()) {
    if (!allowed.has(scenarioId)) continue;
    const subjectMentioned = obsList.some((o) => o.mentioned);
    if (subjectMentioned) continue;

    const competitorEntityIds = new Set();
    const competitorNamesById = new Map();
    for (const obs of obsList) {
      for (const raw of obs.competitorsMentioned || []) {
        const id = resolveEntityId(raw, propertyProfile);
        if (!id) continue;
        competitorEntityIds.add(id);
        if (!competitorNamesById.has(id)) competitorNamesById.set(id, String(raw));
      }
    }
    if (!competitorEntityIds.size) continue;

    lost.set(scenarioId, {
      scenarioId,
      intent: scenarioMap[scenarioId]?.intent || "",
      territory: territoryLabelForIntent(scenarioMap[scenarioId]?.intent) || "",
      scenarioLabel: scenarioMap[scenarioId]?.label || scenarioId,
      competitorEntityIds,
      competitorNamesById,
      observations: obsList,
    });
  }

  return lost;
}

function pickExemplarForCompetitor(lostRow, competitorId, propertyProfile) {
  const matching = (lostRow.observations || []).filter((o) =>
    (o.competitorsMentioned || []).some((name) => resolveEntityId(name, propertyProfile) === competitorId)
  );
  let best = null;
  for (const obs of matching.length ? matching : []) {
    if (!best || (obs.rawResponse || "").length > (best.rawResponse || "").length) best = obs;
  }
  return best;
}

function toEvidenceItem(lostRow, competitorId, propertyProfile, periodMeta = {}) {
  const obs = pickExemplarForCompetitor(lostRow, competitorId, propertyProfile);
  const sources = (obs?.sourcesCited || []).slice(0, MAX_SOURCES).map((s) => ({
    url: s.url || "",
    title: s.title || "",
  }));
  const rawObservedName = lostRow.competitorNamesById?.get(competitorId) || null;
  const competitorName = entityDisplayName(competitorId, propertyProfile, rawObservedName);
  const verbatim = buildVerbatimResponseFields(obs || {}, propertyProfile, { highlightSubject: false });
  const observationId =
    obs?.observationId ||
    obs?.id ||
    [periodMeta.periodId || "period", lostRow.scenarioId, obs?.provider || "unknown", competitorId]
      .filter(Boolean)
      .join("::");

  return {
    // Stable evidence keys (do not rely on display-name matching alone)
    propertyId: propertyProfile?.propertyId || periodMeta.propertyId || null,
    periodId: periodMeta.periodId || null,
    scenarioId: lostRow.scenarioId,
    provider: obs?.provider || "unknown",
    observationId,
    subjectEntityId: propertyProfile?.entityId || propertyProfile?.canonicalEntityId || null,
    competitorEntityId: competitorId,
    displacementCompetitorEntityId: competitorId,
    evidenceType: "competitive_displacement",

    scenarioLabel: lostRow.scenarioLabel,
    intent: lostRow.intent,
    demandTerritory: lostRow.territory || territoryLabelForIntent(lostRow.intent) || "",
    territory: lostRow.territory || territoryLabelForIntent(lostRow.intent) || "",
    mentioned: false,
    status: "Displaced",
    displacingCompetitor: competitorName,
    rawObservedCompetitorName: rawObservedName,
    competitorsMentioned: filterCustomerFacingEntityNames(
      (obs?.competitorsMentioned || []).slice(0, 12),
      propertyProfile
    ).slice(0, 8),
    sourcesCited: sources,
    timestamp: periodMeta.executionDate || obs?.timestamp || null,
    ...verbatim,
  };
}

/**
 * Resolve displacement evidence for one competitor in one scope.
 */
export function resolveDisplacementEvidence({
  propertyProfile,
  observations,
  scenarios,
  competitorId,
  competitorName,
  scope = "overall",
  periodMeta = {},
  maxItems = MAX_MODAL_ITEMS,
} = {}) {
  const normalizedScope = normalizeScope(scope);
  let entityId = competitorId || null;
  if (!entityId && competitorName) {
    entityId = resolveEntityId(competitorName, propertyProfile);
  }
  if (!entityId) {
    return {
      ok: true,
      version: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
      competitorId: null,
      competitorName: competitorName || null,
      scope: normalizedScope,
      count: 0,
      evidenceAvailable: false,
      evidence: [],
      total: 0,
      grain: DISPLAY_GRAIN,
      supportingObservationCount: 0,
      uniqueScenarioCount: 0,
      drawerItemCount: 0,
    };
  }

  const lostMap = buildDisplacementScenarioMap(observations, scenarios, propertyProfile, normalizedScope);
  const matching = [];
  for (const lostRow of lostMap.values()) {
    if (lostRow.competitorEntityIds.has(entityId)) matching.push(lostRow);
  }

  matching.sort((a, b) => String(a.scenarioId).localeCompare(String(b.scenarioId)));
  const evidence = matching
    .slice(0, maxItems)
    .map((row) => toEvidenceItem(row, entityId, propertyProfile, periodMeta))
    .filter((item) => Boolean(item.aiResponse || item.responseExcerpt));

  const displayName = entityDisplayName(entityId, propertyProfile, competitorName);
  // Evidence-backed only — scenarios without resolvable raw responses do not count.
  const uniqueScenarioCount = evidence.length;
  const supportingObservationCount = evidence.length;
  const evidenceAvailable = evidence.length > 0;

  return {
    ok: true,
    version: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    competitorId: entityId,
    competitorName: displayName,
    scope: normalizedScope,
    count: uniqueScenarioCount,
    evidenceAvailable,
    evidence,
    total: evidence.length,
    grain: DISPLAY_GRAIN,
    definition: DISPLACEMENT_EVENT_DEFINITION,
    supportingObservationCount,
    uniqueScenarioCount,
    drawerItemCount: evidence.length,
    // Aggregate may not appear customer-facing when evidence cannot be resolved.
    aggregateRequiresSupportingEvidence: ADP_DISPLACEMENT_AGGREGATE_REQUIRES_SUPPORTING_EVIDENCE,
  };
}

/**
 * Counts by competitor entityId for a scope (for attaching to ranking rows).
 * Evidence-backed only — scenarios without a resolvable raw response do not count.
 */
export function computeDisplacementCountsByEntity(observations, scenarios, propertyProfile, scope = "overall") {
  const lostMap = buildDisplacementScenarioMap(observations, scenarios, propertyProfile, scope);
  const counts = Object.create(null);
  for (const lostRow of lostMap.values()) {
    for (const entityId of lostRow.competitorEntityIds) {
      const exemplar = pickExemplarForCompetitor(lostRow, entityId, propertyProfile);
      if (!exemplar?.rawResponse) continue;
      counts[entityId] = (counts[entityId] || 0) + 1;
    }
  }
  return counts;
}

/**
 * Attach displacement { count, evidenceAvailable } onto competitive ranking display rows.
 * Uses the same resolver source as the evidence modal.
 * Fail-closed: count > 0 requires at least one resolvable supporting evidence item.
 */
export function attachDisplacementToCompetitiveRanking(rankingBlock, observations, scenarios, propertyProfile) {
  if (!rankingBlock?.byTerritory) return rankingBlock;

  const byTerritory = { ...rankingBlock.byTerritory };
  for (const [key, ranking] of Object.entries(byTerritory)) {
    if (!ranking?.displayRows) continue;
    const scopeKey = ranking.isOverall || key === OVERALL_RANKING_KEY ? OVERALL_RANKING_KEY : ranking.intent || key;
    const displayRows = ranking.displayRows.map((row) => {
      if (row.isSubject) {
        return {
          ...row,
          displacement: { count: 0, evidenceAvailable: false, competitorId: null },
        };
      }
      const resolved = resolveDisplacementEvidence({
        propertyProfile,
        observations,
        scenarios,
        competitorId: row.entityId,
        competitorName: row.name || row.displayName,
        scope: scopeKey,
      });
      const count = resolved.uniqueScenarioCount || 0;
      const evidenceAvailable = resolved.evidenceAvailable === true && resolved.drawerItemCount > 0;
      return {
        ...row,
        displacement: {
          count: evidenceAvailable ? count : 0,
          evidenceAvailable,
          competitorId: row.entityId,
          supportingObservationCount: resolved.supportingObservationCount,
          drawerItemCount: resolved.drawerItemCount,
        },
      };
    });
    byTerritory[key] = { ...ranking, displayRows };
  }

  return {
    ...rankingBlock,
    byTerritory,
    displacementEvidenceVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    DISPLAY_AND_MODAL_SAME_SOURCE: true,
  };
}

export function resolveCompetitorIdFromQuery(propertyProfile, { competitorId, competitor } = {}) {
  if (competitorId) return competitorId;
  if (competitor) return resolveEntityId(competitor, propertyProfile);
  return null;
}
