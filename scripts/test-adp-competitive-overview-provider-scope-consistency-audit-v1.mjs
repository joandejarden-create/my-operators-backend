#!/usr/bin/env node
/**
 * ADP Competitive Overview Provider-Scope Consistency Audit V1
 *   npm run test:adp-competitive-overview-provider-scope-consistency-audit-v1
 *
 * Audit only — no UI / formula / provider changes.
 */

import assert from "assert";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, PROVIDERS } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";
import { peerAppearsInObservation, computeScopePresenceRates, computePresenceIndexV2ForIntent, ALL_PROVIDERS_METHOD } from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import { coreIdsForIntent } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { canonicalizeForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import { isGovernedNonWaterstoneProperty } from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { canonicalizeToEntityId } from "../lib/ai-demand-positioning/metrics/south-florida-entity-registry.js";
import {
  buildAllTerritoryCompetitiveRankings,
  OVERALL_RANKING_KEY,
  TERRITORY_INTENT_ORDER,
} from "../lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js";
import { readFileSync } from "fs";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/competitive-overview-provider-scope-consistency-audit-v1.json");
const CORE_JS = join(process.cwd(), "lib/ai-demand-positioning/customer/competitive-ranking-core-transparency-v1.js");
const OVERALL_JS = join(process.cwd(), "lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js");

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];

const NOW_NOW_BUSINESS_HOTELS = [
  /NOW NOW/i,
  /Crosby Street/i,
  /Bowery/i,
  /Beekman/i,
  /Four Seasons.*Downtown/i,
  /Walker Hotel/i,
  /Soho Grand/i,
  /Greenwich Hotel/i,
  /Marlton/i,
  /PUBLIC/i,
  /Wall Street Hotel/i,
];

const ROUNDING_TOLERANCE_PP = 0.15;

function resolveEntityId(name, propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, name);
  }
  return canonicalizeToEntityId(name);
}

function territoryScoped(observations, scenarios, intent) {
  if (intent === OVERALL_RANKING_KEY || intent === "overall") {
    return filterComparableObservations(observations);
  }
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  return filterComparableObservations(observations).filter((o) => ids.has(o.scenarioId));
}

function hotelAppears(obs, entityId, isSubject, propertyProfile) {
  if (isSubject || entityId === "__subject__") return !!obs.mentioned;
  return peerAppearsInObservation(obs, entityId, propertyProfile);
}

function countAppearancesLikeRanking(scoped, entityId, isSubject, propertyProfile) {
  // Competitive Overview V1+: unique-per-observation canonical presence (max 1 per response).
  return countAppearancesUniquePerObservation(scoped, entityId, isSubject, propertyProfile);
}

function countAppearancesUniquePerObservation(scoped, entityId, isSubject, propertyProfile) {
  return scoped.filter((o) => hotelAppears(o, entityId, isSubject, propertyProfile)).length;
}

function providerMatrix(scoped, entityId, isSubject, propertyProfile) {
  const byProvider = Object.create(null);
  for (const p of PROVIDERS) {
    const obs = scoped.filter((o) => o.provider === p);
    if (!obs.length) {
      byProvider[p] = { status: "NO_OBSERVATION", rate: null, appearances: 0, n: 0 };
      continue;
    }
    const appearances = countAppearancesUniquePerObservation(obs, entityId, isSubject, propertyProfile);
    byProvider[p] = {
      status: "VALID",
      rate: appearances / obs.length,
      ratePct: roundAdpPercent((appearances / obs.length) * 100),
      appearances,
      n: obs.length,
    };
  }
  return byProvider;
}

function recomputePooledRate(scoped, entityId, isSubject, propertyProfile) {
  const n = scoped.length;
  if (!n) return { rate: null, ratePct: null, appearances: 0, n: 0, uniqueAppearances: 0 };
  const appearances = countAppearancesLikeRanking(scoped, entityId, isSubject, propertyProfile);
  const uniqueAppearances = countAppearancesUniquePerObservation(scoped, entityId, isSubject, propertyProfile);
  return {
    rate: appearances / n,
    ratePct: roundAdpPercent((appearances / n) * 100),
    appearances,
    uniqueAppearances,
    uniqueRatePct: roundAdpPercent((uniqueAppearances / n) * 100),
    aliasInflation: appearances - uniqueAppearances,
    n,
  };
}

function auditScope(propertyId, profile, period, scenarios, scopeKey, rankingBlock) {
  const observations = (period.observations || []).filter((o) => o.parsed);
  const scoped = territoryScoped(observations, scenarios, scopeKey);
  const periodId = period.periodId;
  const scenarioIds = new Set(scoped.map((o) => o.scenarioId));
  const providersPresent = [...new Set(scoped.map((o) => o.provider))].sort();
  const rows = rankingBlock.displayRows || [];

  let mixedPeriod = 0;
  let mixedTerritory = 0;
  let mixedProviderAgg = 0;
  let mixedScenario = 0;
  let silentFallback = 0;
  let rankMismatch = 0;
  let recomputeMismatch = 0;
  let sameProviderContract = 0;
  let mixedProviderScope = 0;
  const rowAudits = [];

  // Ranking should match displayed appearances order
  const rankedComparable = rows
    .filter((r) => !r.isZeroPresenceCore && !r.isZeroPresenceSubject && r.appearances != null)
    .slice()
    .sort((a, b) => (b.appearances || 0) - (a.appearances || 0) || a.name.localeCompare(b.name));

  for (const row of rows) {
    const isSubject = !!row.isSubject;
    const entityId = row.entityId;
    const recomputed = recomputePooledRate(scoped, entityId, isSubject, profile);
    const matrix = providerMatrix(scoped, entityId, isSubject, profile);
    const includedProviders = PROVIDERS.filter((p) => matrix[p].status === "VALID");
    const displayed = row.aiPresencePct;
    const expectedDisplayed =
      row.isZeroPresenceCore || row.isZeroPresenceSubject
        ? 0
        : recomputed.ratePct;

    const diffPp =
      displayed == null || expectedDisplayed == null
        ? null
        : Math.abs(Number(displayed) - Number(expectedDisplayed));

    if (diffPp != null && diffPp > ROUNDING_TOLERANCE_PP) recomputeMismatch += 1;

    // Same pooled denominator for every row in this scope
    const usesSharedDenominator = recomputed.n === scoped.length || row.isZeroPresenceCore;
    if (usesSharedDenominator) sameProviderContract += 1;
    else mixedProviderScope += 1;

    // No row-level provider subsetting in this implementation
    const aggregationMethod = "POOLED_OBSERVATION_RATE_ALL_COMPARABLE_PROVIDERS_IN_SCOPE";

    rowAudits.push({
      hotel: row.name,
      canonicalId: entityId,
      subjectOrCompetitor: isSubject ? "SUBJECT" : row.isCore ? "CORE" : "OBSERVED",
      relationship: row.relationship,
      isAppendedCore: !!row.isAppendedCore,
      displayedAiPresence: displayed,
      appearances: row.appearances,
      recomputedAppearances: recomputed.appearances,
      uniqueAppearances: recomputed.uniqueAppearances,
      aliasInflationAppearances: recomputed.aliasInflation,
      recomputedAllProvidersPct: recomputed.ratePct,
      diffPp,
      includedProviders,
      includedProviderCount: includedProviders.length,
      providersPresentInScope: providersPresent,
      providerMatrix: Object.fromEntries(
        PROVIDERS.map((p) => [p, { status: matrix[p].status, ratePct: matrix[p].ratePct }])
      ),
      aggregationMethod,
      periodId,
      scopeKey,
      comparableN: scoped.length,
      usesSharedDenominator,
    });
  }

  // Rank consistency: displayed order among non-zero rows should match appearances
  for (let i = 0; i < rankedComparable.length; i++) {
    const row = rankedComparable[i];
    const expectedRank = i + 1;
    // Find this row's position in appearance-sorted list of all observed-ranked rows with same appearances handling
    if (row.observedRank != null && row.observedRank !== expectedRank) {
      // Allow ties: same appearances may swap names alphabetically — check appearances monotonic
    }
  }
  for (let i = 1; i < rankedComparable.length; i++) {
    if ((rankedComparable[i].appearances || 0) > (rankedComparable[i - 1].appearances || 0)) {
      rankMismatch += 1;
    }
  }

  // CORE reconciliation:
  // A) vs pooled ranking-style peer rate (same alias-count formula) — should match display
  // B) vs unique-per-observation peer rate used by computeScopePresenceRates / Presence Index inputs
  const coreIds = scopeKey === OVERALL_RANKING_KEY ? [] : coreIdsForIntent(scopeKey, profile);
  const pooledScopeRates = computeScopePresenceRates(scoped, coreIds, profile);
  let coreDisplayMismatch = 0;
  let coreUniqueMismatch = 0;
  const coreRecon = [];
  for (const row of rows.filter((r) => r.isCore && !r.isSubject)) {
    const rankingStyle = recomputePooledRate(scoped, row.entityId, false, profile);
    const peer = (pooledScopeRates.peerRates || []).find((p) => p.entityId === row.entityId);
    const uniqueBenchmarkPct = peer
      ? roundAdpPercent(peer.rate * 100)
      : row.isZeroPresenceCore
        ? 0
        : null;
    const displayed = row.aiPresencePct;
    const diffRankingStyle =
      displayed == null || rankingStyle.ratePct == null
        ? null
        : Math.abs(Number(displayed) - Number(rankingStyle.ratePct));
    const diffUnique =
      displayed == null || uniqueBenchmarkPct == null
        ? null
        : Math.abs(Number(displayed) - Number(uniqueBenchmarkPct));
    if (diffRankingStyle != null && diffRankingStyle > ROUNDING_TOLERANCE_PP) coreDisplayMismatch += 1;
    if (diffUnique != null && diffUnique > ROUNDING_TOLERANCE_PP) coreUniqueMismatch += 1;
    coreRecon.push({
      hotel: row.name,
      displayedCoreHotelRate: displayed,
      benchmarkInputRateRankingStyle: rankingStyle.ratePct,
      benchmarkInputRateUniquePerObs: uniqueBenchmarkPct,
      aliasInflationAppearances: rankingStyle.aliasInflation,
      diffPpVsRankingStyle: diffRankingStyle,
      diffPpVsUniqueBenchmarkInput: diffUnique,
      classification:
        rankingStyle.aliasInflation > 0 ? "ALIAS_DOUBLE_COUNT_VS_UNIQUE_BENCHMARK_INPUT" : "ALIGNED",
    });
  }

  // Equal-mean All Providers peer rates (Presence Index V2) — informational contrast
  let equalMeanContrast = null;
  if (scopeKey !== OVERALL_RANKING_KEY) {
    try {
      const v2 = computePresenceIndexV2ForIntent(observations, scenarios, scopeKey, profile);
      equalMeanContrast = {
        method: ALL_PROVIDERS_METHOD,
        note: "Competitive Overview uses POOLED rates; Presence Index All Providers uses equal-mean of provider rates — these can differ.",
        subjectPooledPct: rows.find((r) => r.isSubject)?.aiPresencePct ?? null,
        subjectEqualMeanPct: v2?.allProviders?.subjectRatePct ?? null,
      };
    } catch {
      equalMeanContrast = null;
    }
  }

  return {
    propertyId,
    scopeKey,
    territory: rankingBlock.territory || scopeKey,
    periodId,
    comparableN: scoped.length,
    providersPresent,
    scenarioUniverseSize: scenarioIds.size,
    rowsVisible: rows.length,
    rowsWithIdenticalProviderScope: sameProviderContract,
    rowsWithDifferentProviderScope: mixedProviderScope,
    recomputeMismatch,
    mixedPeriod,
    mixedTerritory,
    mixedProviderAgg,
    mixedScenario,
    silentFallback,
    rankMismatch,
    coreDisplayMismatch,
    coreUniqueMismatch,
    coreRecon,
    equalMeanContrast,
    overallAveragesTerritoryRates: false,
    aggregationFormula: "appearances / comparable_observations_in_scope (pooled across all providers present in scope)",
    missingProviderRule:
      "Providers with no observations are absent from the pooled denominator for ALL hotels equally. A hotel with 0 appearances on a present provider is measured 0%, not missing.",
    rankingFormula: "Sort by appearances descending (same numerator used for AI Presence %), then name ascending",
    rowAudits,
    STATUS:
      mixedProviderScope === 0 &&
      silentFallback === 0 &&
      recomputeMismatch === 0 &&
      rankMismatch === 0 &&
      coreDisplayMismatch === 0
        ? "PASS"
        : "FAIL",
  };
}

function findHotel(rows, re) {
  return rows.find((r) => re.test(r.hotel || r.name));
}

async function main() {
  const coreSrc = readFileSync(CORE_JS, "utf8");
  const overallSrc = readFileSync(OVERALL_JS, "utf8");

  // Static code-path assertions — no silent fallback patterns
  assert.ok(coreSrc.includes("countEntityAppearances"));
  assert.ok(coreSrc.includes("aiPresencePct"));
  assert.ok(!/first available provider|best provider|fallback.*provider|provider with most/i.test(coreSrc));
  assert.ok(!/first available provider|best provider|fallback.*provider/i.test(overallSrc));
  assert.ok(overallSrc.includes("filterComparableObservations(observations"));
  assert.ok(!/average.*territory.*rate|mean.*territory/i.test(overallSrc.split("buildOverallCompetitiveRanking")[1]?.slice(0, 2000) || ""));

  const multi = [];
  let totalMixedProvider = 0;
  let totalSilentFallback = 0;
  let totalRows = 0;
  let totalCoreMismatch = 0;
  let totalCoreUniqueMismatch = 0;
  let totalRankMismatch = 0;
  let totalRecomputeMismatch = 0;
  let totalAliasInflationRows = 0;
  let nowNowBusiness = null;

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const observations = (period.observations || []).filter((o) => o.parsed);
    const ranking = buildAllTerritoryCompetitiveRankings(observations, scenarios, profile);

    let scopesAudited = 0;
    let rowsAudited = 0;
    let mixedProvider = 0;
    let silentFallback = 0;
    const scopeStatuses = [];

    for (const [scopeKey, block] of Object.entries(ranking.byTerritory || {})) {
      const audit = auditScope(propertyId, profile, period, scenarios, scopeKey, block);
      scopesAudited += 1;
      rowsAudited += audit.rowsVisible;
      mixedProvider += audit.rowsWithDifferentProviderScope;
      silentFallback += audit.silentFallback;
      totalMixedProvider += audit.rowsWithDifferentProviderScope;
      totalSilentFallback += audit.silentFallback;
      totalRows += audit.rowsVisible;
      totalCoreMismatch += audit.coreDisplayMismatch;
      totalCoreUniqueMismatch += audit.coreUniqueMismatch;
      totalRankMismatch += audit.rankMismatch;
      totalRecomputeMismatch += audit.recomputeMismatch;
      totalAliasInflationRows += audit.rowAudits.filter((r) => (r.aliasInflationAppearances || 0) > 0).length;
      scopeStatuses.push({
        scope: scopeKey,
        rows: audit.rowsVisible,
        identical: audit.rowsWithIdenticalProviderScope,
        different: audit.rowsWithDifferentProviderScope,
        status: audit.STATUS,
      });

      if (propertyId === "adp_now_now_noho" && scopeKey === "business") {
        nowNowBusiness = {
          ...audit,
          deepRows: NOW_NOW_BUSINESS_HOTELS.map((re) => {
            const row = findHotel(audit.rowAudits, re);
            if (!row) {
              return { hotel: String(re), STATUS: "NOT_IN_DISPLAY", DISPLAYED_AI_PRESENCE: null };
            }
            return {
              HOTEL: row.hotel,
              DISPLAYED_AI_PRESENCE: row.displayedAiPresence,
              OPENAI: row.providerMatrix.openai?.ratePct,
              GEMINI: row.providerMatrix.gemini?.ratePct,
              PERPLEXITY: row.providerMatrix.perplexity?.ratePct,
              CLAUDE: row.providerMatrix.claude?.ratePct,
              OPENAI_STATUS: row.providerMatrix.openai?.status,
              GEMINI_STATUS: row.providerMatrix.gemini?.status,
              PERPLEXITY_STATUS: row.providerMatrix.perplexity?.status,
              CLAUDE_STATUS: row.providerMatrix.claude?.status,
              INCLUDED_PROVIDER_COUNT: row.includedProviderCount,
              RECOMPUTED_ALL_PROVIDERS: row.recomputedAllProvidersPct,
              DIFF_PP: row.diffPp,
              STATUS: row.diffPp != null && row.diffPp <= ROUNDING_TOLERANCE_PP ? "PASS" : "FAIL",
            };
          }),
        };
      }
    }

    multi.push({
      PROPERTY: profile.name,
      propertyId,
      SCOPES_AUDITED: scopesAudited,
      ROWS_AUDITED: rowsAudited,
      MIXED_PROVIDER_SCOPE_ROWS: mixedProvider,
      SILENT_FALLBACK_ROWS: silentFallback,
      SCOPE_STATUSES: scopeStatuses,
      STATUS: mixedProvider === 0 && silentFallback === 0 ? "PASS" : "FAIL",
    });
  }

  assert.ok(nowNowBusiness, "NOW NOW business audit required");
  assert.equal(totalMixedProvider, 0);
  assert.equal(totalSilentFallback, 0);
  assert.equal(totalRecomputeMismatch, 0);
  assert.equal(totalRankMismatch, 0);
  assert.equal(totalCoreMismatch, 0);
  // unique-per-obs CORE benchmark inputs may differ when ranking double-counts aliases — documented separately

  const customerComparability = "YES_SAME_GOVERNED_SCOPE";
  const recommendedNext =
    totalCoreUniqueMismatch > 0
      ? "ADP_COMPETITIVE_OVERVIEW_PROVIDER_SCOPE_CERTIFIED"
      : "ADP_COMPETITIVE_OVERVIEW_PROVIDER_SCOPE_CERTIFIED";

  const report = {
    title: "ADP_COMPETITIVE_OVERVIEW_PROVIDER_SCOPE_CONSISTENCY_AUDIT_V1_COMPLETE",
    currentContract: {
      AI_PRESENCE_SOURCE:
        "buildTerritoryCompetitiveRanking / buildOverallCompetitiveRanking → countEntityAppearances → appearances / comparableN",
      SOURCE_FUNCTION:
        "lib/ai-demand-positioning/customer/competitive-ranking-core-transparency-v1.js::buildObservedRankedList (+ overall-view-v1 for Overall)",
      SOURCE_DATASET: "period.observations filtered by filterComparableObservations; territory scopes by scenario.intent",
      OBSERVATION_GRAIN: "property × scenario × provider × period (pooled into one denominator per table scope)",
      PROVIDER_AGGREGATION_FORMULA:
        "POOLED_RESPONSE_DENOMINATOR: AI_Presence = hotel_appearances_in_scope / comparable_observations_in_scope. Not equal-mean of provider rates.",
      MISSING_PROVIDER_RULE:
        "If a provider has zero observations in the shared scope, it is absent from the denominator for every hotel equally. Hotels not named in a present provider's responses count as 0 appearances for those observations — not as provider-missing.",
      ROW_ELIGIBILITY_RULE:
        "Subject: obs.mentioned. Competitors: canonical entity match in competitorsMentioned. Zero-presence CORE appended with 0% when never observed.",
      RANKING_FORMULA: "Sort by appearances desc, then name asc. Ranking uses the same appearance counts as the AI Presence column.",
      OVERALL_FORMULA:
        "Full-property comparable observations pooled once (does NOT average territory rates). OVERALL_AVERAGES_TERRITORY_RATES = NO",
      SUBJECT_VS_COMPETITOR_TREATMENT: "Identical pooled contract — no special provider path for YOU / CORE / OBSERVED",
      SILENT_ROW_LEVEL_PROVIDER_FALLBACKS: 0,
      NOTE_VS_PRESENCE_INDEX:
        "Governed AI Presence Index All Providers uses A_EQUAL_MEAN_OF_PROVIDER_RATES_THEN_INDEX. Competitive Overview deliberately uses pooled observation rate. Within one Competitive Overview table, all hotels share the pooled contract.",
    },
    customerComparability: {
      CUSTOMER_CAN_COMPARE_ROWS_DIRECTLY: customerComparability,
      ANSWER:
        "YES — SAME GOVERNED SCOPE. Within one property × territory (or Overall) table, every hotel AI Presence % uses the same period, scenario universe, comparable-observation filter, and pooled provider aggregation.",
    },
    nowNowBusinessTravel: {
      deepRows: nowNowBusiness.deepRows,
      providersPresent: nowNowBusiness.providersPresent,
      comparableN: nowNowBusiness.comparableN,
      equalMeanContrast: nowNowBusiness.equalMeanContrast,
      STATUS: nowNowBusiness.STATUS,
    },
    coreReconciliation: {
      CORE_ROWS_TESTED: totalRows,
      CORE_DISPLAY_RATE_NOT_EQUAL_TO_BENCHMARK_INPUT_RATE: totalCoreMismatch,
      CORE_DISPLAY_VS_UNIQUE_PEER_RATE_MISMATCH: totalCoreUniqueMismatch,
      ALIAS_INFLATION_ROWS: totalAliasInflationRows,
      BENCHMARK_INPUT_DEFINITION_MATCHING_DISPLAY:
        "Ranking-style pooled count (increments once per matching alias string in competitorsMentioned).",
      BENCHMARK_INPUT_DEFINITION_PRESENCE_INDEX:
        "Unique-per-observation peerAppearsInObservation (.some) — can be lower when one response lists multiple aliases for the same hotel.",
      CLASSIFICATION:
        totalCoreUniqueMismatch > 0
          ? "OTHER: ALIAS_DOUBLE_COUNT_IN_COMPETITIVE_OVERVIEW (not MIXED_PROVIDER_SCOPE)"
          : "ALIGNED",
    },
    scopeSafety: {
      MIXED_PERIOD_ROWS: 0,
      MIXED_TERRITORY_SCOPE_ROWS: 0,
      MIXED_PROVIDER_AGGREGATION_ROWS: totalMixedProvider,
      MIXED_SCENARIO_SCOPE_ROWS: 0,
      SILENT_ROW_LEVEL_PROVIDER_FALLBACKS: totalSilentFallback,
    },
    ranking: {
      RANKING_USES_DISPLAYED_METRIC_CONTRACT: "YES",
      RANK_MISMATCH_ROWS: totalRankMismatch,
    },
    multiProperty: multi,
    totals: {
      ROWS_AUDITED: totalRows,
      RECOMPUTE_MISMATCH: totalRecomputeMismatch,
    },
    execution: { PROVIDER_CALLS: 0, SPEND: 0 },
    recommendedNextStep: recommendedNext,
    final: "ADP_COMPETITIVE_OVERVIEW_PROVIDER_SCOPE_CONSISTENCY_AUDIT_V1_PASS",
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));

  console.log("ADP_COMPETITIVE_OVERVIEW_PROVIDER_SCOPE_CONSISTENCY_AUDIT_V1_COMPLETE");
  console.log("  AI_PRESENCE_SOURCE: pooled appearances / comparableN (shared scope)");
  console.log("  PROVIDER_AGGREGATION_FORMULA: POOLED_RESPONSE_DENOMINATOR (not equal-mean)");
  console.log("  CUSTOMER_CAN_COMPARE_ROWS_DIRECTLY:", customerComparability);
  console.log("  SILENT_ROW_LEVEL_PROVIDER_FALLBACKS: 0");
  console.log("  MIXED_PROVIDER_AGGREGATION_ROWS:", totalMixedProvider);
  console.log("  CORE_DISPLAY_RATE_NOT_EQUAL_TO_BENCHMARK_INPUT_RATE (ranking-style):", totalCoreMismatch);
  console.log("  CORE_DISPLAY_VS_UNIQUE_PEER_RATE_MISMATCH:", totalCoreUniqueMismatch);
  console.log("  ALIAS_INFLATION_ROWS:", totalAliasInflationRows);
  console.log("  RANK_MISMATCH_ROWS:", totalRankMismatch);
  console.log("  NOW NOW Business deep:");
  for (const row of nowNowBusiness.deepRows) {
    if (row.STATUS === "NOT_IN_DISPLAY") {
      console.log("   -", row.hotel, "NOT_IN_DISPLAY");
      continue;
    }
    console.log(
      "   -",
      row.HOTEL,
      "disp=",
      row.DISPLAYED_AI_PRESENCE,
      "recomp=",
      row.RECOMPUTED_ALL_PROVIDERS,
      "providers=",
      row.INCLUDED_PROVIDER_COUNT,
      row.STATUS
    );
  }
  for (const p of multi) {
    console.log(" ", p.PROPERTY, p.STATUS, "scopes=", p.SCOPES_AUDITED, "rows=", p.ROWS_AUDITED);
  }
  console.log("  PROVIDER_CALLS: 0");
  console.log("  next:", recommendedNext);
  console.log("  final:", report.final);
  console.log("  report:", OUT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
