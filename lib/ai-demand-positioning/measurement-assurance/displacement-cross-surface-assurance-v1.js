/**
 * Displacement cross-surface reconciliation + evidence-set integrity.
 *
 * Defect: DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH
 * Principle: SAME_CONCEPT_SAME_CANONICAL_SOURCE
 * Gate: DISPLACEMENT_CROSS_SURFACE_MUST_MATCH + DISPLACEMENT_EVIDENCE_SET_INTEGRITY
 */

import { computeLostDemand } from "../intelligence/lost-demand.js";
import {
  computeDisplacementCountsByEntity,
  resolveDisplacementEvidence,
  buildDisplacementScenarioMap,
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
} from "../customer/resolve-displacement-evidence-v1.js";
import { attachDisplacementToCompetitiveRanking } from "../customer/resolve-displacement-evidence-v1.js";
import { buildAllTerritoryCompetitiveRankings } from "../customer/competitive-ranking-overall-view-v1.js";
import { OVERALL_RANKING_KEY, TERRITORY_INTENT_ORDER } from "../customer/competitive-ranking-overall-view-v1.js";

export const DISPLACEMENT_CROSS_SURFACE_MUST_MATCH = "DISPLACEMENT_CROSS_SURFACE_MUST_MATCH";
export const DISPLACEMENT_EVIDENCE_SET_INTEGRITY = "DISPLACEMENT_EVIDENCE_SET_INTEGRITY";
export const DEFECT_DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH =
  "DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH";
export const PRINCIPLE_SAME_CONCEPT_SAME_CANONICAL_SOURCE = "SAME_CONCEPT_SAME_CANONICAL_SOURCE";

export const CAMBRIDGE_REEFS_GOLD_CASE = Object.freeze({
  propertyId: "adp_cambridge_beaches_bermuda",
  entityId: "reefs_resort",
  competitorName: "The Reefs Resort & Club",
  periodBackupPath:
    "reports/ai-demand-positioning/measurement-assurance/gemini-same-period-recovery/2026-08-21T08-45-14/period-backups/adp_period_adp_cambridge_beaches_bermuda_20260820141258_547a7b.json",
  historicalBug: { overview: 1, contextBuggy: 2 },
  note: "Alias double-count in lost-demand aggregateDisplacement (The Reefs Resort & Club + The Reefs Resort in same scenario).",
});

/**
 * Compare Overview displacement counts vs Context leaderboard for entities that appear in BOTH.
 */
export function reconcileDisplacementOverviewVsContext({
  observations,
  scenarios,
  propertyProfile,
  scope = OVERALL_RANKING_KEY,
}) {
  const ranking = attachDisplacementToCompetitiveRanking(
    buildAllTerritoryCompetitiveRankings(observations, scenarios, propertyProfile),
    observations,
    scenarios,
    propertyProfile
  );
  const scopeKey = scope === OVERALL_RANKING_KEY || scope === "overall" ? OVERALL_RANKING_KEY : scope;
  const rankingBlock = ranking.byTerritory?.[scopeKey];
  const overviewById = Object.create(null);
  for (const row of rankingBlock?.displayRows || []) {
    if (row.isSubject) continue;
    if (row.entityId && row.displacement) {
      overviewById[row.entityId] = {
        name: row.name,
        count: Number(row.displacement.count) || 0,
      };
    }
  }

  const lost = computeLostDemand(observations, scenarios, propertyProfile);
  // Context UI is Overall-only today; for territory scopes compare canonical counts directly
  const contextById = Object.create(null);
  if (scopeKey === OVERALL_RANKING_KEY) {
    for (const d of lost.displacement || []) {
      if (!d.entityId) continue;
      contextById[d.entityId] = { name: d.name, count: Number(d.displacementCount) || 0 };
    }
  } else {
    const counts = computeDisplacementCountsByEntity(observations, scenarios, propertyProfile, scopeKey);
    for (const [entityId, count] of Object.entries(counts)) {
      contextById[entityId] = { name: entityId, count };
    }
  }

  const mismatches = [];
  const matches = [];
  for (const entityId of Object.keys(overviewById)) {
    if (!(entityId in contextById)) continue; // not on both surfaces
    const ov = overviewById[entityId].count;
    const cx = contextById[entityId].count;
    const row = {
      entityId,
      name: overviewById[entityId].name,
      overviewDisplacement: ov,
      contextDisplacement: cx,
      match: ov === cx,
      scope: scopeKey,
    };
    if (row.match) matches.push(row);
    else {
      mismatches.push({
        ...row,
        defect: DEFECT_DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH,
      });
    }
  }

  return {
    gate: DISPLACEMENT_CROSS_SURFACE_MUST_MATCH,
    principle: PRINCIPLE_SAME_CONCEPT_SAME_CANONICAL_SOURCE,
    relationshipClass: "MUST_MATCH",
    version: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    scope: scopeKey,
    status: mismatches.length ? "FAIL" : "PASS",
    matches,
    mismatches,
    comparedCount: matches.length + mismatches.length,
  };
}

/**
 * Evidence scenario IDs must equal the metric's displacement scenario set.
 */
export function runDisplacementEvidenceSetIntegrity({
  observations,
  scenarios,
  propertyProfile,
  competitorId,
  scope = OVERALL_RANKING_KEY,
  periodMeta = {},
}) {
  const metricMap = buildDisplacementScenarioMap(observations, scenarios, propertyProfile, scope);
  const metricIds = [...metricMap.values()]
    .filter((row) => row.competitorEntityIds.has(competitorId))
    .map((row) => row.scenarioId)
    .sort();

  const evidence = resolveDisplacementEvidence({
    propertyProfile,
    observations,
    scenarios,
    competitorId,
    scope,
    periodMeta,
  });
  const evidenceIds = (evidence.evidence || []).map((e) => e.scenarioId).sort();

  const metricCount = metricIds.length;
  const evidenceCount = evidence.count;
  const setMatch =
    metricCount === evidenceCount &&
    metricIds.length === evidenceIds.length &&
    metricIds.every((id, i) => id === evidenceIds[i]);

  return {
    gate: DISPLACEMENT_EVIDENCE_SET_INTEGRITY,
    status: setMatch && metricCount === evidenceCount ? "PASS" : "FAIL",
    competitorId,
    metricCount,
    evidenceCount,
    metricScenarioIds: metricIds,
    evidenceScenarioIds: evidenceIds,
    countMatch: metricCount === evidenceCount,
    setMatch,
  };
}

export function runDisplacementCrossSurfaceAssurance({
  observations,
  scenarios,
  propertyProfile,
  scopes = null,
}) {
  const scopeList = scopes || [OVERALL_RANKING_KEY, ...TERRITORY_INTENT_ORDER];
  const byScope = {};
  const allMismatches = [];
  for (const scope of scopeList) {
    const result = reconcileDisplacementOverviewVsContext({
      observations,
      scenarios,
      propertyProfile,
      scope,
    });
    byScope[scope] = result;
    allMismatches.push(...result.mismatches.map((m) => ({ ...m, scope })));
  }

  // Evidence integrity sample: top overall context competitor if any
  const lost = computeLostDemand(observations, scenarios, propertyProfile);
  const top = lost.displacement?.[0];
  let evidenceIntegrity = null;
  if (top?.entityId) {
    evidenceIntegrity = runDisplacementEvidenceSetIntegrity({
      observations,
      scenarios,
      propertyProfile,
      competitorId: top.entityId,
      scope: OVERALL_RANKING_KEY,
    });
  }

  const overallFail = byScope[OVERALL_RANKING_KEY]?.status === "FAIL";
  const evidenceFail = evidenceIntegrity?.status === "FAIL";

  return {
    gate: DISPLACEMENT_CROSS_SURFACE_MUST_MATCH,
    principle: PRINCIPLE_SAME_CONCEPT_SAME_CANONICAL_SOURCE,
    defectClass: DEFECT_DUPLICATED_CONCEPT_CROSS_SURFACE_MISMATCH,
    status: overallFail || evidenceFail ? "FAIL" : "PASS",
    byScope,
    overallMismatches: byScope[OVERALL_RANKING_KEY]?.mismatches || [],
    allMismatches,
    evidenceIntegrity,
    // Overall MUST_MATCH is material for customer page (both surfaces visible)
    materialBlocker: overallFail,
  };
}
