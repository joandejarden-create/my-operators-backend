/**
 * Competitive Ranking CORE Transparency V1 — display contract only.
 * TOP 10 observed + all governed CORE hotels not already in top 10.
 * True rank preserved; zero-presence CORE shown as Not surfaced.
 */

import { filterComparableObservations } from "../metrics/grain-governance.js";
import { coreIdsForIntent, hotelById } from "../metrics/presence-benchmark-v1.js";
import { peerAppearsInObservation } from "../metrics/presence-index-v2.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { roundAdpPercent } from "../format-percent.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import {
  countCanonicalPresenceAppearances,
  MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION,
} from "./canonical-presence-per-observation-v1.js";

export const COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION = "adp_competitive_ranking_core_transparency_v1";
export const BASE_OBSERVED_ROWS = 10;
export { MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION };

const ALL_INTENTS = Object.values(TRAVELER_INTENTS);

function entityDisplayName(entityId, propertyProfile) {
  const hotel = hotelById(entityId, propertyProfile);
  return hotel?.canonical || entityId.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function territoryObservations(observations, scenarios, intent) {
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  return filterComparableObservations(observations).filter((o) => ids.has(o.scenarioId));
}

function countEntityAppearances(scoped, propertyProfile) {
  return countCanonicalPresenceAppearances(scoped, propertyProfile);
}

function buildObservedRankedList(scoped, propertyProfile) {
  const n = scoped.length;
  const { counts, subjectKey } = countEntityAppearances(scoped, propertyProfile);
  const rows = [];

  if (counts[subjectKey]) {
    rows.push({
      entityId: subjectKey,
      name: propertyProfile?.name || "Your Property",
      isSubject: true,
      appearances: counts[subjectKey],
      aiPresenceRate: n ? counts[subjectKey] / n : null,
      aiPresencePct: n ? roundAdpPercent((counts[subjectKey] / n) * 100) : null,
      relationship: "SUBJECT",
      isCore: false,
    });
  }

  for (const [entityId, appearances] of Object.entries(counts)) {
    if (entityId === subjectKey) continue;
    rows.push({
      entityId,
      name: entityDisplayName(entityId, propertyProfile),
      isSubject: false,
      appearances,
      aiPresenceRate: n ? appearances / n : null,
      aiPresencePct: n ? roundAdpPercent((appearances / n) * 100) : null,
      relationship: null,
      isCore: false,
    });
  }

  rows.sort((a, b) => b.appearances - a.appearances || a.name.localeCompare(b.name));
  rows.forEach((row, idx) => {
    row.observedRank = idx + 1;
  });
  return { rows, comparableN: n };
}

export function buildTerritoryCompetitiveRanking(observations, scenarios, intent, propertyProfile, options = {}) {
  const coreIds = coreIdsForIntent(intent, propertyProfile);
  const scoped = territoryObservations(observations, scenarios, intent);
  const { rows: observedRanked, comparableN } = buildObservedRankedList(scoped, propertyProfile);

  const coreIdSet = new Set(coreIds);
  observedRanked.forEach((row) => {
    if (row.isSubject) return;
    if (coreIdSet.has(row.entityId)) {
      row.isCore = true;
      row.relationship = "CORE";
    } else {
      row.relationship = "OBSERVED_ALTERNATIVE";
    }
  });

  const top10 = observedRanked.slice(0, BASE_OBSERVED_ROWS);
  const top10Ids = new Set(top10.map((r) => r.entityId));

  const subjectRow = observedRanked.find((r) => r.isSubject);
  const appendedSubject =
    subjectRow && !top10Ids.has(subjectRow.entityId)
      ? [{ ...subjectRow, isAppendedSubject: true, displayRank: subjectRow.observedRank }]
      : [];

  const appendedCore = [];
  for (const coreId of coreIds) {
    if (top10Ids.has(coreId)) continue;
    const existing = observedRanked.find((r) => r.entityId === coreId);
    if (existing) {
      appendedCore.push({
        ...existing,
        isCore: true,
        relationship: "CORE",
        isAppendedCore: true,
        displayRank: existing.observedRank,
      });
    } else {
      appendedCore.push({
        entityId: coreId,
        name: entityDisplayName(coreId, propertyProfile),
        isSubject: false,
        appearances: 0,
        aiPresenceRate: 0,
        aiPresencePct: 0,
        observedRank: null,
        displayRank: "—",
        relationship: "CORE",
        isCore: true,
        isAppendedCore: true,
        isZeroPresenceCore: true,
        status: "NOT_SURFACED",
        statusLabel: "Not surfaced this period",
      });
    }
  }

  appendedCore.sort((a, b) => {
    const ar = a.observedRank == null ? Infinity : a.observedRank;
    const br = b.observedRank == null ? Infinity : b.observedRank;
    return ar - br || a.name.localeCompare(b.name);
  });

  const rankedAppended = [...appendedSubject, ...appendedCore.filter((r) => !r.isZeroPresenceCore)];
  rankedAppended.sort((a, b) => (a.observedRank || Infinity) - (b.observedRank || Infinity));
  const zeroCore = appendedCore.filter((r) => r.isZeroPresenceCore);

  const displayRows = [
    ...top10.map((row) => ({
      ...row,
      displayRank: row.observedRank,
      isAppendedCore: false,
      status: null,
      statusLabel: null,
    })),
    ...rankedAppended.map((row) => ({
      ...row,
      displayRank: row.observedRank,
      isAppendedCore: row.isAppendedCore || false,
      status: null,
      statusLabel: null,
    })),
    ...zeroCore,
  ];

  const zeroPresenceCore = appendedCore.filter((r) => r.isZeroPresenceCore).length;
  const coreInTop10 = top10.filter((r) => r.isCore && !r.isSubject).length;
  const coreAppended = appendedCore.filter((r) => !r.isZeroPresenceCore).length;
  const identifiableCore = new Set(displayRows.filter((r) => r.isCore).map((r) => r.entityId)).size;

  return {
    version: COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION,
    intent,
    territory: territoryLabelForIntent(intent),
    coreCount: coreIds.length,
    coreIds,
    comparableN,
    baseObservedRows: BASE_OBSERVED_ROWS,
    displayRows,
    reconciliation: {
      CORE_COUNT: coreIds.length,
      CORE_IN_TOP_10: coreInTop10,
      CORE_APPENDED: coreAppended,
      CORE_ZERO_PRESENCE: zeroPresenceCore,
      IDENTIFIABLE_CORE_TOTAL: identifiableCore,
      COUNT_MATCH: identifiableCore === coreIds.length,
    },
    meta: {
      ALL_CORE_INCLUDED: identifiableCore === coreIds.length,
      TRUE_RANK_PRESERVED: true,
      TOP_10_IS_MAXIMUM: false,
    },
  };
}

export function buildAllTerritoryCompetitiveRankings(observations, scenarios, propertyProfile, options = {}) {
  const intents = options.intents || ALL_INTENTS;
  const byTerritory = {};
  const reconciliation = [];

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

  return {
    version: COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION,
    byTerritory,
    reconciliation,
    PROPERTY_SPECIFIC_RANKING_DISPLAY_CODE: 0,
  };
}

/** Verify peer rate for a CORE entity matches ranking (zero-presence included). */
export function corePeerRateForEntity(scoped, entityId, propertyProfile) {
  const n = scoped.length;
  if (!n) return { rate: null, measured: false };
  const appearances = scoped.filter((o) => peerAppearsInObservation(o, entityId, propertyProfile)).length;
  return { rate: appearances / n, appearances, measured: true };
}
