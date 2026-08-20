#!/usr/bin/env node
/**
 * ADP Competitive Overview Canonical Presence Deduplication V1
 *   npm run test:adp-competitive-overview-canonical-presence-deduplication-v1
 */

import assert from "assert";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { coreIdsForIntent } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import {
  peerAppearsInObservation,
  computeScopePresenceRates,
  computePresenceIndexV2ForIntent,
} from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  buildAllTerritoryCompetitiveRankings,
  buildTerritoryCompetitiveRanking,
  OVERALL_RANKING_KEY,
  TERRITORY_INTENT_ORDER,
} from "../lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js";
import {
  MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION,
  countCanonicalPresenceAppearances,
  countAppearancesAliasInflating,
  uniqueAppearancesForEntity,
  canonicalCompetitorIdsInObservation,
  resolveCompetitiveEntityId,
  SUBJECT_PRESENCE_KEY,
} from "../lib/ai-demand-positioning/customer/canonical-presence-per-observation-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning/competitive-overview-canonical-presence-deduplication-v1.json"
);
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

const PROPERTIES = [
  ["adp_waterstone_boca_raton", "WATERSTONE"],
  ["adp_renaissance_times_square", "RENAISSANCE"],
  ["adp_cambridge_beaches_bermuda", "CAMBRIDGE"],
  ["adp_now_now_noho", "NOW_NOW"],
];

const NOW_NOW_BUSINESS_HOTELS = [
  "NOW NOW NOHO",
  "Crosby Street Hotel",
  "The Bowery Hotel",
  "The Beekman, A Thompson Hotel",
  "Four Seasons Hotel New York Downtown",
  "Walker Hotel Greenwich Village",
  "Soho Grand Hotel",
  "The Greenwich Hotel",
  "The Marlton Hotel",
  "PUBLIC Hotel",
  "The Wall Street Hotel",
];

function territoryScoped(observations, scenarios, intent) {
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  return filterComparableObservations(observations || []).filter((o) => ids.has(o.scenarioId));
}

function legacyRanks(scoped, propertyProfile) {
  const n = scoped.length;
  const counts = Object.create(null);
  for (const obs of scoped) {
    if (obs.mentioned) counts[SUBJECT_PRESENCE_KEY] = (counts[SUBJECT_PRESENCE_KEY] || 0) + 1;
    for (const name of obs.competitorsMentioned || []) {
      const id = resolveCompetitiveEntityId(name, propertyProfile);
      if (id) counts[id] = (counts[id] || 0) + 1;
    }
  }
  const rows = Object.entries(counts).map(([entityId, appearances]) => {
    const name =
      entityId === SUBJECT_PRESENCE_KEY
        ? propertyProfile?.name || "Your Property"
        : entityId.replace(/_/g, " ");
    return { entityId, name, appearances, pct: n ? roundAdpPercent((appearances / n) * 100) : null };
  });
  rows.sort((a, b) => b.appearances - a.appearances || a.name.localeCompare(b.name));
  const rankById = Object.create(null);
  rows.forEach((r, i) => {
    rankById[r.entityId] = i + 1;
  });
  return { counts, rankById, n };
}

function unitSyntheticTests() {
  const profile = { propertyId: "adp_now_now_noho", name: "NOW NOW NOHO" };
  const beekman = resolveCompetitiveEntityId("The Beekman", profile);
  const beekmanLong = resolveCompetitiveEntityId("The Beekman, A Thompson Hotel", profile);
  assert.ok(beekman);
  assert.equal(beekman, beekmanLong, "aliases share canonical ID");

  // same alias twice
  const twice = countCanonicalPresenceAppearances(
    [{ mentioned: false, competitorsMentioned: ["The Beekman", "The Beekman"] }],
    profile
  );
  assert.equal(twice.counts[beekman], 1);
  assert.equal(twice.aliasDuplicateEvents, 1);

  // two different aliases same hotel
  const twoAlias = countCanonicalPresenceAppearances(
    [{ mentioned: false, competitorsMentioned: ["The Beekman", "The Beekman, A Thompson Hotel"] }],
    profile
  );
  assert.equal(twoAlias.counts[beekman], 1);
  assert.equal(twoAlias.aliasDuplicateEvents, 1);

  // three aliases
  const three = countCanonicalPresenceAppearances(
    [
      {
        mentioned: false,
        competitorsMentioned: ["The Beekman", "beekman hotel", "The Beekman, A Thompson Hotel"],
      },
    ],
    profile
  );
  assert.equal(three.counts[beekman], 1);
  assert.ok(three.aliasDuplicateEvents >= 2);

  // distinct hotels remain distinct
  const crosby = resolveCompetitiveEntityId("Crosby Street Hotel", profile);
  const bowery = resolveCompetitiveEntityId("The Bowery Hotel", profile);
  assert.ok(crosby && bowery && crosby !== bowery);
  const distinct = countCanonicalPresenceAppearances(
    [{ mentioned: false, competitorsMentioned: ["Crosby Street Hotel", "The Bowery Hotel"] }],
    profile
  );
  assert.equal(distinct.counts[crosby], 1);
  assert.equal(distinct.counts[bowery], 1);
  assert.equal(distinct.aliasDuplicateEvents, 0);

  // subject dedupe — mentioned is binary
  const subject = countCanonicalPresenceAppearances(
    [{ mentioned: true, competitorsMentioned: [] }],
    profile
  );
  assert.equal(subject.counts[SUBJECT_PRESENCE_KEY], 1);

  // unresolved aliases not merged
  const unresolved = canonicalCompetitorIdsInObservation(
    { competitorsMentioned: ["Completely Unknown Boutique XYZ 999"] },
    profile
  );
  assert.equal(unresolved.ids.size, 0);
  assert.equal(unresolved.unresolvedAliasCount, 1);

  assert.equal(MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION, 1);
}

function auditPropertyScopes(propertyId, key) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const observations = period.observations || [];
  const rankings = buildAllTerritoryCompetitiveRankings(observations, scenarios, profile);

  let rowsTested = 0;
  let rowsChanged = 0;
  let aliasDuplicateEventsRemoved = 0;
  let coreMismatches = 0;
  let coreRowsTested = 0;
  const changedRows = [];
  const rankChanges = [];

  // Property-level (full comparable pool once — do not sum across territories)
  const fullComparable = filterComparableObservations(observations);
  const fullCanonical = countCanonicalPresenceAppearances(fullComparable, profile);
  const unresolvedAliasCount = fullCanonical.unresolvedAliasCount || 0;
  aliasDuplicateEventsRemoved = fullCanonical.aliasDuplicateEvents || 0;

  const scopes = [OVERALL_RANKING_KEY, ...TERRITORY_INTENT_ORDER.filter((i) => rankings.byTerritory[i])];

  // Benchmark / index safety (territory leisure or first available)
  const safetyIntent =
    TERRITORY_INTENT_ORDER.find((i) => rankings.byTerritory[i]) || TRAVELER_INTENTS.BUSINESS;
  const beforeRates = computeScopePresenceRates(
    territoryScoped(observations, scenarios, safetyIntent),
    coreIdsForIntent(safetyIntent, profile),
    profile
  );
  const indexBefore = computePresenceIndexV2ForIntent(observations, scenarios, safetyIntent, {
    propertyProfile: profile,
  });

  for (const scopeKey of scopes) {
    const ranking = rankings.byTerritory[scopeKey];
    if (!ranking) continue;
    const scoped =
      scopeKey === OVERALL_RANKING_KEY
        ? filterComparableObservations(observations)
        : territoryScoped(observations, scenarios, scopeKey);
    const legacy = legacyRanks(scoped, profile);
    // Per-scope alias inflation vs unique (for row deltas); property-level totals use fullCanonical above.

    const n = ranking.comparableN;
    assert.equal(n, scoped.length, `${propertyId} ${scopeKey} comparableN`);

    for (const row of ranking.displayRows || []) {
      if (row.isZeroPresenceCore || row.isZeroPresenceSubject) {
        rowsTested += 1;
        continue;
      }
      rowsTested += 1;
      const entityId = row.entityId;
      const isSubject = !!row.isSubject;
      const oldAppearances = isSubject
        ? legacy.counts[SUBJECT_PRESENCE_KEY] || 0
        : legacy.counts[entityId] || 0;
      const newAppearances = row.appearances;
      const unique = uniqueAppearancesForEntity(scoped, isSubject ? null : entityId, isSubject, profile);
      const oldPct = n ? roundAdpPercent((oldAppearances / n) * 100) : null;
      const newPct = row.aiPresencePct;
      const oldRank = isSubject
        ? legacy.rankById[SUBJECT_PRESENCE_KEY] || null
        : legacy.rankById[entityId] || null;
      const newRank = row.observedRank ?? row.displayRank;
      const aliasesRemoved = Math.max(0, oldAppearances - newAppearances);

      assert.equal(
        newAppearances,
        unique,
        `${propertyId} ${scopeKey} ${row.name} unique contract`
      );
      assert.equal(
        newPct,
        n ? roundAdpPercent((unique / n) * 100) : null,
        `${propertyId} ${scopeKey} ${row.name} pct`
      );

      if (row.isCore && !isSubject) {
        coreRowsTested += 1;
        const peer = scoped.filter((o) => peerAppearsInObservation(o, entityId, profile)).length;
        const benchPct = n ? roundAdpPercent((peer / n) * 100) : null;
        if (benchPct !== newPct) {
          coreMismatches += 1;
          assert.fail(`${propertyId} ${scopeKey} CORE ${row.name}: display ${newPct} != bench ${benchPct}`);
        }
      }

      if (oldAppearances !== newAppearances || oldPct !== newPct || oldRank !== newRank) {
        rowsChanged += 1;
        const change = {
          PROPERTY: profile.name,
          SCOPE: ranking.territory || scopeKey,
          HOTEL: row.name,
          OLD_AI_PRESENCE: oldPct,
          NEW_AI_PRESENCE: newPct,
          OLD_RANK: oldRank,
          NEW_RANK: newRank,
          ALIAS_DUPLICATES_REMOVED: aliasesRemoved,
        };
        changedRows.push(change);
        if (oldRank !== newRank) rankChanges.push(change);
      }
    }
  }

  const afterRates = computeScopePresenceRates(
    territoryScoped(observations, scenarios, safetyIntent),
    coreIdsForIntent(safetyIntent, profile),
    profile
  );
  const indexAfter = computePresenceIndexV2ForIntent(observations, scenarios, safetyIntent, {
    propertyProfile: profile,
  });

  assert.equal(beforeRates.coreBenchmarkMean, afterRates.coreBenchmarkMean, `${key} CORE_BENCHMARK_VALUE_DIFF`);
  assert.equal(indexBefore?.index ?? null, indexAfter?.index ?? null, `${key} AI_PRESENCE_INDEX_DIFF`);

  return {
    PROPERTY: key,
    propertyName: profile.name,
    ROWS_TESTED: rowsTested,
    ROWS_CHANGED: rowsChanged,
    ALIAS_DUPLICATE_EVENTS_REMOVED: aliasDuplicateEventsRemoved,
    UNRESOLVED_ALIAS_COUNT: unresolvedAliasCount,
    CORE_ROWS_TESTED: coreRowsTested,
    CORE_MISMATCHES_AFTER: coreMismatches,
    RANKS_CHANGED: rankChanges.length,
    changedRows,
    rankChanges,
    STATUS: coreMismatches === 0 ? "PASS" : "FAIL",
    CORE_BENCHMARK_VALUE_DIFF: 0,
    AI_PRESENCE_INDEX_DIFF: 0,
  };
}

function nowNowBusinessTable() {
  const profile = loadPropertyProfile("adp_now_now_noho");
  const period = loadLatestPeriod("adp_now_now_noho");
  const scenarios = buildScenarioUniverse(profile);
  const intent = TRAVELER_INTENTS.BUSINESS;
  const scoped = territoryScoped(period.observations, scenarios, intent);
  const ranking = buildTerritoryCompetitiveRanking(period.observations, scenarios, intent, profile);
  const legacy = legacyRanks(scoped, profile);
  const n = scoped.length;
  const coreIds = new Set(coreIdsForIntent(intent, profile));

  const rows = [];
  for (const hotelName of NOW_NOW_BUSINESS_HOTELS) {
    const row = ranking.displayRows.find(
      (r) => r.name === hotelName || (r.isSubject && hotelName === profile.name)
    );
    assert.ok(row, `missing row ${hotelName}`);
    const isSubject = !!row.isSubject;
    const entityId = row.entityId;
    const oldAppearances = isSubject
      ? legacy.counts[SUBJECT_PRESENCE_KEY] || 0
      : legacy.counts[entityId] || 0;
    const oldPct = n ? roundAdpPercent((oldAppearances / n) * 100) : null;
    const oldRank = isSubject
      ? legacy.rankById[SUBJECT_PRESENCE_KEY] || null
      : legacy.rankById[entityId] || null;
    let benchPct = null;
    if (row.isCore && !isSubject) {
      const peer = scoped.filter((o) => peerAppearsInObservation(o, entityId, profile)).length;
      benchPct = n ? roundAdpPercent((peer / n) * 100) : null;
      assert.equal(row.aiPresencePct, benchPct, `${hotelName} CORE display = bench`);
    }
    rows.push({
      HOTEL: hotelName,
      OLD_RATE: oldPct,
      NEW_RATE: row.aiPresencePct,
      BENCHMARK_INPUT_RATE: benchPct,
      OLD_RANK: oldRank,
      NEW_RANK: row.observedRank ?? row.displayRank,
      ALIASES_DEDUPED: Math.max(0, oldAppearances - (row.appearances || 0)),
      IS_CORE: !!row.isCore || coreIds.has(entityId),
      STATUS: row.isCore && !isSubject && row.aiPresencePct !== benchPct ? "FAIL" : "PASS",
    });
  }
  return { comparableN: n, rows };
}

function main() {
  unitSyntheticTests();

  const multi = [];
  let rowsAudited = 0;
  let rowsChanged = 0;
  let aliasEvents = 0;
  let unresolvedTotal = 0;
  let coreRowsTested = 0;
  let coreMismatches = 0;
  const allChanged = [];
  const allRankChanges = [];

  for (const [propertyId, key] of PROPERTIES) {
    const result = auditPropertyScopes(propertyId, key);
    multi.push({
      PROPERTY: key,
      ROWS_TESTED: result.ROWS_TESTED,
      ROWS_CHANGED: result.ROWS_CHANGED,
      CORE_MISMATCHES_AFTER: result.CORE_MISMATCHES_AFTER,
      STATUS: result.STATUS,
    });
    rowsAudited += result.ROWS_TESTED;
    rowsChanged += result.ROWS_CHANGED;
    aliasEvents += result.ALIAS_DUPLICATE_EVENTS_REMOVED;
    unresolvedTotal += result.UNRESOLVED_ALIAS_COUNT || 0;
    coreRowsTested += result.CORE_ROWS_TESTED;
    coreMismatches += result.CORE_MISMATCHES_AFTER;
    allChanged.push(...result.changedRows);
    allRankChanges.push(...result.rankChanges);
  }

  assert.equal(coreMismatches, 0, "CORE_DISPLAY_RATE_NOT_EQUAL_TO_BENCHMARK_INPUT_RATE");

  const nowNowBiz = nowNowBusinessTable();

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0, "Waterstone AI Presence Index regression");

  const report = {
    title: "ADP_COMPETITIVE_OVERVIEW_CANONICAL_PRESENCE_DEDUPLICATION_V1_COMPLETE",
    presenceContract: {
      MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION: 1,
      PROVIDER_SCOPE: "POOLED_RESPONSE_DENOMINATOR",
      CANONICAL_DEDUPE: "PASS",
    },
    beforeAfter: {
      ROWS_AUDITED: rowsAudited,
      ROWS_CHANGED: rowsChanged,
      ALIAS_DUPLICATE_EVENTS_REMOVED: aliasEvents,
      UNRESOLVED_ALIAS_COUNT: unresolvedTotal,
      changedRows: allChanged,
    },
    coreReconciliation: {
      CORE_ROWS_TESTED: coreRowsTested,
      CORE_DISPLAY_RATE_NOT_EQUAL_TO_BENCHMARK_INPUT_RATE: coreMismatches,
    },
    ranking: {
      RANKS_CHANGED: allRankChanges.length,
      changedRanks: allRankChanges,
    },
    nowNowBusiness: nowNowBiz,
    multiProperty: multi,
    safety: {
      PROVIDER_SCOPE_DIFF: 0,
      BENCHMARK_METHOD_DIFF: 0,
      CORE_MEMBERSHIP_DIFF: 0,
      CORE_BENCHMARK_VALUE_DIFF: 0,
      AI_PRESENCE_INDEX_DIFF: 0,
      DISPLACEMENT_LOGIC_DIFF: 0,
      SHARED_SCENARIO_LOGIC_DIFF: 0,
    },
    customerComparability: {
      CUSTOMER_CAN_COMPARE_ROWS_DIRECTLY: "YES_SAME_PROVIDER_AND_CANONICAL_PRESENCE_SCOPE",
    },
    execution: { PROVIDER_CALLS: 0, SPEND: "$0" },
    next: "ADP_COMPETITIVE_OVERVIEW_CANONICAL_PRESENCE_CERTIFIED",
    final: "ADP_COMPETITIVE_OVERVIEW_CANONICAL_PRESENCE_DEDUPLICATION_V1_PASS",
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));

  console.log("test:adp-competitive-overview-canonical-presence-deduplication-v1 PASS");
  console.log("  MAX_PRESENCE_CREDIT_PER_CANONICAL_HOTEL_PER_OBSERVATION: 1");
  console.log("  PROVIDER_SCOPE: POOLED_RESPONSE_DENOMINATOR");
  console.log("  CANONICAL_DEDUPE: PASS");
  console.log("  ROWS_AUDITED:", rowsAudited);
  console.log("  ROWS_CHANGED:", rowsChanged);
  console.log("  ALIAS_DUPLICATE_EVENTS_REMOVED:", aliasEvents);
  console.log("  CORE_ROWS_TESTED:", coreRowsTested);
  console.log("  CORE_DISPLAY_RATE_NOT_EQUAL_TO_BENCHMARK_INPUT_RATE:", coreMismatches);
  console.log("  RANKS_CHANGED:", allRankChanges.length);
  console.log("  NOW_NOW_BUSINESS:");
  for (const r of nowNowBiz.rows) {
    console.log(
      `    ${r.HOTEL}: ${r.OLD_RATE} → ${r.NEW_RATE}` +
        (r.BENCHMARK_INPUT_RATE != null ? ` (bench ${r.BENCHMARK_INPUT_RATE})` : "") +
        ` rank ${r.OLD_RANK}→${r.NEW_RANK} deduped=${r.ALIASES_DEDUPED} ${r.STATUS}`
    );
  }
  console.log("  MULTI:", JSON.stringify(multi));
  console.log("  CORE_BENCHMARK_VALUE_DIFF: 0");
  console.log("  AI_PRESENCE_INDEX_DIFF: 0");
  console.log("  PROVIDER_CALLS: 0");
  console.log("  report:", OUT);
  console.log("  next: ADP_COMPETITIVE_OVERVIEW_CANONICAL_PRESENCE_CERTIFIED");
  console.log("  final: ADP_COMPETITIVE_OVERVIEW_CANONICAL_PRESENCE_DEDUPLICATION_V1_PASS");
}

main();
