/**
 * AI Competitive Set Overall View V1 — full-property presence ranking.
 * TOP 10, or TOP 9 + subject when subject ranks outside Top 10.
 * No Overall CORE benchmark; territory CORE logic unchanged.
 */

import { filterComparableObservations } from "../metrics/grain-governance.js";
import { roundAdpPercent } from "../format-percent.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { coreIdsForIntent, hotelById } from "../metrics/presence-benchmark-v1.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import {
  BASE_OBSERVED_ROWS,
  buildTerritoryCompetitiveRanking,
  COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION,
} from "./competitive-ranking-core-transparency-v1.js";
import {
  countCanonicalPresenceAppearances,
  resolveCompetitiveEntityId,
  SUBJECT_PRESENCE_KEY,
} from "./canonical-presence-per-observation-v1.js";

export { BASE_OBSERVED_ROWS, buildTerritoryCompetitiveRanking } from "./competitive-ranking-core-transparency-v1.js";

export const COMPETITIVE_RANKING_OVERALL_VIEW_VERSION = "adp_ai_competitive_set_overall_view_v1";
export const OVERALL_RANKING_KEY = "overall";
export const OVERALL_RANKING_LABEL = "Overall";
export const OVERALL_BASE_ROWS = 10;
export const OVERALL_TOP_SLICE_WHEN_SUBJECT_APPENDED = 9;

/** Governed dropdown order after Overall. */
export const TERRITORY_INTENT_ORDER = Object.freeze([
  TRAVELER_INTENTS.BUSINESS,
  TRAVELER_INTENTS.LEISURE,
  TRAVELER_INTENTS.COUPLES,
  TRAVELER_INTENTS.FAMILY,
  TRAVELER_INTENTS.GROUP_MEETING,
  TRAVELER_INTENTS.WELLNESS,
  TRAVELER_INTENTS.ADVENTURE,
  TRAVELER_INTENTS.CELEBRATION,
]);

function entityDisplayName(entityId, propertyProfile) {
  const hotel = hotelById(entityId, propertyProfile);
  return hotel?.canonical || entityId.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function territoryObservations(observations, scenarios, intent) {
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  return filterComparableObservations(observations || []).filter((o) => ids.has(o.scenarioId));
}

/**
 * Per-hotel strongest demand territory = territory with the most AI presence
 * credits (unique-per-observation) for that hotel in the comparable period.
 */
export function computeTopDemandTerritoryByEntity(observations, scenarios, propertyProfile) {
  const tallies = Object.create(null);

  function bump(entityId, intent) {
    if (!tallies[entityId]) tallies[entityId] = Object.create(null);
    tallies[entityId][intent] = (tallies[entityId][intent] || 0) + 1;
  }

  for (const intent of TERRITORY_INTENT_ORDER) {
    const scoped = territoryObservations(observations, scenarios, intent);
    for (const obs of scoped) {
      if (obs.mentioned) bump(SUBJECT_PRESENCE_KEY, intent);
      const seen = new Set();
      for (const name of obs.competitorsMentioned || []) {
        const id = resolveCompetitiveEntityId(name, propertyProfile);
        if (!id || seen.has(id)) continue;
        seen.add(id);
        bump(id, intent);
      }
    }
  }

  const result = Object.create(null);
  for (const [entityId, byIntent] of Object.entries(tallies)) {
    let bestIntent = null;
    let bestCount = -1;
    for (const intent of TERRITORY_INTENT_ORDER) {
      const count = byIntent[intent] || 0;
      if (count > bestCount) {
        bestCount = count;
        bestIntent = intent;
      }
    }
    result[entityId] =
      bestCount > 0
        ? {
            intent: bestIntent,
            territory: territoryLabelForIntent(bestIntent),
            appearances: bestCount,
          }
        : null;
  }
  return result;
}

function countEntityAppearances(scoped, propertyProfile) {
  return countCanonicalPresenceAppearances(scoped, propertyProfile);
}

function buildOverallObservedRankedList(scoped, propertyProfile) {
  const n = scoped.length;
  const { counts, subjectKey } = countEntityAppearances(scoped, propertyProfile);
  const subjectAppearances = counts[subjectKey] || 0;

  const competitorRows = [];
  for (const [entityId, appearances] of Object.entries(counts)) {
    if (entityId === subjectKey) continue;
    competitorRows.push({
      entityId,
      name: entityDisplayName(entityId, propertyProfile),
      isSubject: false,
      appearances,
      aiPresenceRate: n ? appearances / n : null,
      aiPresencePct: n ? roundAdpPercent((appearances / n) * 100) : null,
      relationship: "OBSERVED_ALTERNATIVE",
      isCore: false,
    });
  }

  competitorRows.sort((a, b) => b.appearances - a.appearances || a.name.localeCompare(b.name));

  const subjectRow = {
    entityId: subjectKey,
    name: propertyProfile?.name || "Your Property",
    isSubject: true,
    appearances: subjectAppearances,
    aiPresenceRate: n ? subjectAppearances / n : null,
    aiPresencePct: subjectAppearances > 0 && n ? roundAdpPercent((subjectAppearances / n) * 100) : 0,
    relationship: "SUBJECT",
    isCore: false,
  };

  const allForRank = [...competitorRows];
  if (subjectAppearances > 0) {
    allForRank.push(subjectRow);
  }
  allForRank.sort((a, b) => b.appearances - a.appearances || a.name.localeCompare(b.name));
  allForRank.forEach((row, idx) => {
    row.observedRank = idx + 1;
  });

  if (subjectAppearances === 0) {
    subjectRow.observedRank = null;
    subjectRow.isZeroPresenceSubject = true;
    subjectRow.status = "NOT_SURFACED";
    subjectRow.statusLabel = "Not surfaced this period";
  }

  return {
    observedRanked: allForRank,
    competitorRows,
    subjectRow,
    comparableN: n,
  };
}

function mapDisplayRow(row, extras = {}) {
  return {
    ...row,
    displayRank: row.displayRank ?? row.observedRank,
    isAppendedCore: false,
    isCore: false,
    status: row.status ?? null,
    statusLabel: row.statusLabel ?? null,
    ...extras,
  };
}

export function buildOverallCompetitiveRanking(observations, scenarios, propertyProfile) {
  const scoped = filterComparableObservations(observations || []);
  const { observedRanked, competitorRows, subjectRow, comparableN } = buildOverallObservedRankedList(
    scoped,
    propertyProfile
  );
  const topByEntity = computeTopDemandTerritoryByEntity(observations, scenarios, propertyProfile);

  function withTopTerritory(row, extras = {}) {
    const top = topByEntity[row.entityId];
    return mapDisplayRow(row, {
      topDemandTerritory: top?.territory || "—",
      topDemandTerritoryIntent: top?.intent || null,
      ...extras,
    });
  }

  if (!comparableN) {
    return {
      version: COMPETITIVE_RANKING_OVERALL_VIEW_VERSION,
      viewType: "overall",
      intent: OVERALL_RANKING_KEY,
      territory: OVERALL_RANKING_LABEL,
      comparableN: 0,
      baseObservedRows: OVERALL_BASE_ROWS,
      displayRows: [],
      insufficientData: true,
      hideTerritoryColumn: false,
      territoryColumnMode: "top_demand_territory",
      meta: {
        SUBJECT_ALWAYS_VISIBLE: false,
        OVERALL_CORE_BENCHMARK: false,
      },
    };
  }

  let displayRows = [];
  const subjectInTop10 =
    subjectRow.observedRank != null && subjectRow.observedRank <= OVERALL_BASE_ROWS && !subjectRow.isZeroPresenceSubject;

  if (subjectInTop10) {
    displayRows = observedRanked.slice(0, OVERALL_BASE_ROWS).map((row) => withTopTerritory(row));
  } else {
    const topSlice = competitorRows
      .slice(0, OVERALL_TOP_SLICE_WHEN_SUBJECT_APPENDED)
      .map((row) => withTopTerritory(row));
    if (subjectRow.isZeroPresenceSubject) {
      displayRows = [
        ...topSlice,
        withTopTerritory(subjectRow, {
          displayRank: "—",
          isAppendedSubject: true,
          isZeroPresenceSubject: true,
        }),
      ];
    } else {
      displayRows = [
        ...topSlice,
        withTopTerritory(subjectRow, {
          displayRank: subjectRow.observedRank,
          isAppendedSubject: true,
        }),
      ];
    }
  }

  const subjectVisible = displayRows.some((r) => r.isSubject);

  return {
    version: COMPETITIVE_RANKING_OVERALL_VIEW_VERSION,
    viewType: "overall",
    intent: OVERALL_RANKING_KEY,
    territory: OVERALL_RANKING_LABEL,
    comparableN,
    baseObservedRows: OVERALL_BASE_ROWS,
    displayRows,
    hideTerritoryColumn: false,
    territoryColumnMode: "top_demand_territory",
    reconciliation: null,
    meta: {
      RANKING_SCOPE: "FULL_PROPERTY_COMPARABLE_PERIOD",
      SUBJECT_ALWAYS_VISIBLE: subjectVisible,
      SUBJECT_OUTSIDE_TOP10_BEHAVIOR: "TOP_9_PLUS_SUBJECT",
      TRUE_RANK_PRESERVED: true,
      OVERALL_CORE_BENCHMARK: false,
      OVERALL_AI_PRESENCE_INDEX: false,
      TOP_10_IS_MAXIMUM: true,
      TOP_DEMAND_TERRITORY_COLUMN: true,
    },
  };
}

export function buildAllTerritoryCompetitiveRankings(observations, scenarios, propertyProfile, options = {}) {
  const intents = options.intents || TERRITORY_INTENT_ORDER;
  const byTerritory = {};
  const reconciliation = [];

  const overall = buildOverallCompetitiveRanking(observations, scenarios, propertyProfile);
  if (overall.comparableN > 0 && overall.displayRows?.length) {
    byTerritory[OVERALL_RANKING_KEY] = overall;
  }

  for (const intent of intents) {
    const coreIds = coreIdsForIntent(intent, propertyProfile);
    if (!coreIds.length && !options.includeEmpty) continue;

    const ranking = buildTerritoryCompetitiveRanking(observations, scenarios, intent, propertyProfile, options);
    if (ranking.comparableN === 0 && !coreIds.length) continue;

    byTerritory[intent] = ranking;
    reconciliation.push({
      PROPERTY: propertyProfile?.name,
      TERRITORY: ranking.territory,
      intent,
      CORE_COUNT: ranking.reconciliation.CORE_COUNT,
      CORE_IN_TOP_10: ranking.reconciliation.CORE_IN_TOP_10,
      CORE_APPENDED: ranking.reconciliation.CORE_APPENDED,
      CORE_ZERO_PRESENCE: ranking.reconciliation.CORE_ZERO_PRESENCE,
      IDENTIFIABLE_CORE_TOTAL: ranking.reconciliation.IDENTIFIABLE_CORE_TOTAL,
      COUNT_MATCH: ranking.reconciliation.COUNT_MATCH ? "YES" : "NO",
      VISIBLE_ROWS: ranking.displayRows.length,
    });
  }

  const selectorOrder = [OVERALL_RANKING_KEY, ...intents.filter((i) => byTerritory[i])];

  return {
    version: COMPETITIVE_RANKING_OVERALL_VIEW_VERSION,
    coreTransparencyVersion: COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION,
    byTerritory,
    selectorOrder,
    defaultView: OVERALL_RANKING_KEY,
    reconciliation,
    PROPERTY_SPECIFIC_RANKING_DISPLAY_CODE: 0,
    PROPERTY_SPECIFIC_OVERALL_RANKING_CODE: 0,
  };
}
