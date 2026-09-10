/**
 * AI Demand Positioning — Pre-built evidence index for drawer drill-downs.
 * Stores customer-safe excerpts needed by the UI (no full raw corpus).
 *
 * Displacement drawer evidence is NOT truncated: full canonical support sets
 * are baked under displacementSupportSets so production can serve drawers
 * without runtime period JSON (ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED).
 */

import {
  buildAllCanonicalDisplacementSupportSets,
  DISPLACEMENT_SUPPORT_SET_VERSION,
  ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
  SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET,
} from "./adp-canonical-displacement-support-set-v1.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";

const MAX_EXCERPT = 800;
const MAX_MISSING_PER_INTENT = 5;
const MAX_DISPLACEMENT_PER_COMPETITOR = 3;
const MAX_INTENTS = 12;
const MAX_COMPETITORS = 8;
const MAX_SOURCES_PER_ITEM = 3;

function normalizeCompetitorKey(name) {
  return String(name || "").trim().toLowerCase();
}

function toEvidenceItem(obs, scenarioMap, period) {
  const sources = (obs.sourcesCited || []).slice(0, MAX_SOURCES_PER_ITEM).map((s) => ({
    url: s.url || "",
    title: s.title || "",
  }));
  return {
    scenarioId: obs.scenarioId,
    scenarioLabel: scenarioMap[obs.scenarioId]?.label || obs.scenarioId,
    intent: scenarioMap[obs.scenarioId]?.intent || "",
    provider: obs.provider,
    mentioned: !!obs.mentioned,
    competitorsMentioned: (obs.competitorsMentioned || []).slice(0, 5),
    responseExcerpt: obs.rawResponse ? obs.rawResponse.slice(0, MAX_EXCERPT) : "",
    sourcesCited: sources,
    timestamp: period.executionDate,
  };
}

function dedupeByScenario(observations) {
  const byScenario = {};
  for (const obs of observations) {
    if (
      !byScenario[obs.scenarioId] ||
      (obs.rawResponse || "").length > (byScenario[obs.scenarioId].rawResponse || "").length
    ) {
      byScenario[obs.scenarioId] = obs;
    }
  }
  return Object.values(byScenario);
}

function isDeclaredCompMatch(comp, competitorName) {
  const compLow = String(comp || "").toLowerCase();
  const targetLow = normalizeCompetitorKey(competitorName);
  if (!compLow || !targetLow) return false;
  return compLow.includes(targetLow) || targetLow.includes(compLow);
}

function buildDisplacementSupportSetsBundle(period, scenarios, propertyProfile) {
  if (!propertyProfile) return null;
  const observations = enrichObservationsWithRank(
    (period.observations || []).filter((o) => o.parsed),
    propertyProfile
  );
  const periodMeta = {
    periodId: period.periodId,
    propertyId: period.propertyId || propertyProfile.propertyId,
    executionDate: period.executionDate,
  };

  const overall = buildAllCanonicalDisplacementSupportSets({
    propertyProfile,
    observations,
    scenarios,
    periodMeta,
    scope: "overall",
  });

  const intents = [...new Set((scenarios || []).map((s) => s.intent).filter(Boolean))];
  const byTerritory = Object.create(null);
  for (const intent of intents) {
    byTerritory[intent] = buildAllCanonicalDisplacementSupportSets({
      propertyProfile,
      observations,
      scenarios,
      periodMeta,
      scope: intent,
    });
  }

  return {
    contract: ADP_CANONICAL_DISPLACEMENT_SUPPORT_SET_V1,
    version: DISPLACEMENT_SUPPORT_SET_VERSION,
    singleSource: SINGLE_CANONICAL_DISPLACEMENT_SUPPORT_SET,
    subjectPropertyId: propertyProfile.propertyId,
    periodId: period.periodId,
    overall,
    byTerritory,
  };
}

/**
 * @param {object} period
 * @param {object[]} scenarios
 * @param {object} [propertyProfile] — required to bake canonical displacement support sets
 */
export function buildEvidenceIndex(period, scenarios, propertyProfile = null) {
  if (!period?.observations?.length) {
    return {
      ok: false,
      error: "no_data",
      periodId: period?.periodId || null,
      missingByIntent: {},
      displacementByCompetitor: {},
      displacementSupportSets: null,
    };
  }

  const scenarioMap = Object.fromEntries(scenarios.map((s) => [s.scenarioId, s]));
  const observations = period.observations.filter((o) => o.parsed);

  const missingByIntent = {};
  for (const scenario of scenarios) {
    const intent = scenario.intent;
    if (!missingByIntent[intent]) missingByIntent[intent] = [];
    if (missingByIntent[intent].length >= MAX_MISSING_PER_INTENT) continue;

    const missingObs = dedupeByScenario(
      observations.filter((o) => o.scenarioId === scenario.scenarioId && !o.mentioned)
    );
    for (const obs of missingObs) {
      if (missingByIntent[intent].length >= MAX_MISSING_PER_INTENT) break;
      missingByIntent[intent].push(toEvidenceItem(obs, scenarioMap, period));
    }
  }

  // Legacy truncated name-keyed index (compat). Drawer must use displacementSupportSets.
  const displacementByCompetitor = {};
  const competitorNames = new Set();
  for (const obs of observations) {
    if (obs.mentioned) continue;
    for (const comp of obs.competitorsMentioned || []) {
      if (comp && competitorNames.size < MAX_COMPETITORS) competitorNames.add(comp);
    }
  }

  for (const competitor of competitorNames) {
    const rows = dedupeByScenario(
      observations.filter(
        (o) =>
          !o.mentioned &&
          (o.competitorsMentioned || []).some((c) => isDeclaredCompMatch(c, competitor))
      )
    )
      .slice(0, MAX_DISPLACEMENT_PER_COMPETITOR)
      .map((obs) => toEvidenceItem(obs, scenarioMap, period));

    if (rows.length) {
      displacementByCompetitor[competitor] = rows;
    }
  }

  const intentKeys = Object.keys(missingByIntent).slice(0, MAX_INTENTS);
  const trimmedMissing = {};
  for (const key of intentKeys) trimmedMissing[key] = missingByIntent[key];

  const displacementSupportSets = buildDisplacementSupportSetsBundle(
    period,
    scenarios,
    propertyProfile
  );

  return {
    ok: true,
    periodId: period.periodId,
    propertyId: period.propertyId,
    generatedAt: new Date().toISOString(),
    missingByIntent: trimmedMissing,
    displacementByCompetitor,
    displacementSupportSets,
    displacementSupportSetVersion: displacementSupportSets?.version || null,
  };
}

export function queryEvidenceIndex(index, { intent, type, competitor } = {}) {
  if (!index?.ok) return { evidence: [], total: 0 };

  if (type === "displacement" && competitor) {
    const key = Object.keys(index.displacementByCompetitor || {}).find(
      (k) => normalizeCompetitorKey(k) === normalizeCompetitorKey(competitor)
    );
    const rows = key ? index.displacementByCompetitor[key] : [];
    return { evidence: rows.slice(0, 5), total: rows.length };
  }

  let rows = [];
  if (intent && index.missingByIntent?.[intent]) {
    rows = index.missingByIntent[intent];
  }

  if (type === "present") {
    rows = rows.filter((r) => r.mentioned);
  } else {
    rows = rows.filter((r) => !r.mentioned);
  }

  return { evidence: rows.slice(0, 5), total: rows.length };
}
