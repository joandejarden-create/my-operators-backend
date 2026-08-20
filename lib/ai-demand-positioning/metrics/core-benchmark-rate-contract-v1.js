/**
 * CORE benchmark rate contract V1 — RESEARCH ONLY.
 * Customer surface is subject presence % vs CORE mean presence %.
 * Not added to owner payload. Live Presence Index unchanged.
 */

import { PROVIDERS } from "../data-model.js";
import { filterComparableObservations } from "./grain-governance.js";
import { coreIdsForIntent, MIN_CORE_PEERS_PRODUCTION, PRESENCE_BENCHMARK_VERSION } from "./presence-benchmark-v1.js";
import {
  computePresenceIndexV2ForIntent,
  ALL_PROVIDERS_METHOD,
} from "./presence-index-v2.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import { roundAdpPercent } from "../format-percent.js";

export const CORE_BENCHMARK_RATE_CONTRACT_VERSION = "adp_core_benchmark_rate_v1";

export const SUBJECT_RATE_FORMULA =
  "SUBJECT_AI_PRESENCE_RATE = comparable observations where the subject hotel appears / all comparable observations, at property × territory × provider × period. All Providers = equal mean of included provider subject rates. Missing provider is omitted, not zero.";

export const CORE_BENCHMARK_FORMULA =
  "CORE_BENCHMARK_AI_PRESENCE_RATE = arithmetic mean of presence rates of every valid CORE peer in the same property × territory × provider × period grain. Zero-presence CORE peers remain in the mean. Secondary peers are excluded (weight 0). All Providers = equal mean of included provider CORE means.";

export const ZERO_CORE_PEERS_INCLUDED = "YES";
export const SECONDARY_IN_BENCHMARK = 0;

export const ALL_PROVIDERS_RATE_METHOD =
  "A_EQUAL_MEAN_OF_INCLUDED_PROVIDER_RATES (same derivation as Presence Index V2 All Providers; rates are the customer numbers, index is not)";

export const SUBJECT_CUSTOMER_QUESTION =
  "How often does this hotel appear in AI responses for this demand territory?";

export const CORE_BENCHMARK_CUSTOMER_QUESTION =
  "How often do comparable hotels appear on average in the same monitored territory and provider scope?";

export const EXECUTIVE_FINDING_TEMPLATE =
  "{property} appeared in {subjectRate}% of monitored {territory} responses, compared with {benchmarkRate}% for the average CORE comparable hotel.";

export const DISPLAY_FORBIDDEN = Object.freeze([
  "Presence Index V2 (e.g. 421)",
  "ACI as a customer number (e.g. 264)",
  "4.21× CORE",
  "+321% vs benchmark",
]);

export const RECOMMENDED_DISPLAY = Object.freeze({
  YOUR_AI_PRESENCE: "subject presence %",
  CORE_BENCHMARK: "CORE mean presence %",
  DIFFERENCE: "percentage-point gap (supporting line only)",
  CUSTOMER_INDEX_REQUIRED: "NO",
});

export const RECOMMENDED_TERRITORY_TABLE_COLUMNS = Object.freeze([
  "Demand Territory",
  "Your AI Presence",
  "CORE Benchmark",
  "Difference (pp)",
  "Chg vs Prior (pp, exact scope only)",
]);

export const TERRITORY_TABLE_DEFERRED_COLUMNS = Object.freeze([
  "AI Scenario Presence",
  "Competitor-Present Gaps",
  "#1 Appearance",
  "Top-3 Appearance",
]);

const MATERIAL_SUBJECT_LOO_PP = 8;
export const MATERIAL_PROVIDER_LOO_PP = 8;

function mean(xs) {
  const v = xs.filter((n) => Number.isFinite(n));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

export function differencePp(subjectRate, benchmarkRate) {
  if (!Number.isFinite(subjectRate) || !Number.isFinite(benchmarkRate)) return null;
  return roundAdpPercent((subjectRate - benchmarkRate) * 100);
}

export function computeTerritoryBenchmarkRates(observations, scenarios, intent, options = {}) {
  const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent, options);
  const ap = v2.allProviders;
  const byProvider = {};
  for (const provider of PROVIDERS) {
    const row = v2.byProvider[provider];
    byProvider[provider] = {
      included: Boolean(row?.included),
      missingNotZero: true,
      SUBJECT_RATE: row?.included ? row.subjectRatePct : null,
      CORE_BENCHMARK_RATE: row?.included ? row.coreBenchmarkMeanPct : null,
      DIFFERENCE_PP: row?.included ? differencePp(row.subjectRate, row.coreBenchmarkMean) : null,
      comparableN: row?.comparableN || 0,
      reason: row?.included ? null : row?.reason || "MISSING_PROVIDER_NOT_ZERO",
    };
  }
  return {
    intent,
    territory: territoryLabelForIntent(intent),
    contractVersion: CORE_BENCHMARK_RATE_CONTRACT_VERSION,
    benchmarkVersion: PRESENCE_BENCHMARK_VERSION,
    CORE_COUNT: v2.coreCount,
    ZERO_CORE_PEERS_INCLUDED,
    SECONDARY_IN_BENCHMARK,
    ALL_PROVIDERS_METHOD: ALL_PROVIDERS_RATE_METHOD,
    v2AllProvidersMethodAlias: ALL_PROVIDERS_METHOD,
    byProvider,
    allProviders: {
      includedProviders: ap.includedProviders || [],
      SUBJECT_RATE: ap.subjectRatePct ?? null,
      CORE_BENCHMARK_RATE: ap.coreBenchmarkRatePct ?? null,
      DIFFERENCE_PP: differencePp(ap.subjectRate, ap.coreBenchmarkRate),
      comparableN: ap.comparableN || 0,
      zeroPresencePeers: ap.zeroPresencePeers || [],
    },
    researchIndexNotForCustomer: ap.index ?? null,
  };
}

export function scenarioLeaveOneOutRates(observations, scenarios, intent, propertyProfile = null) {
  const ids = (scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId);
  const options = propertyProfile ? { propertyProfile, coreIds: coreIdsForIntent(intent, propertyProfile) } : {};
  const base = computeTerritoryBenchmarkRates(observations, scenarios, intent, options);
  let maxSubjectPp = 0;
  let maxBenchPp = 0;
  const drops = [];
  for (const sid of ids) {
    const subset = filterComparableObservations(observations).filter((o) => o.scenarioId !== sid);
    const row = computeTerritoryBenchmarkRates(subset, scenarios, intent, options);
    const ds =
      base.allProviders.SUBJECT_RATE != null && row.allProviders.SUBJECT_RATE != null
        ? Math.abs(base.allProviders.SUBJECT_RATE - row.allProviders.SUBJECT_RATE)
        : 0;
    const db =
      base.allProviders.CORE_BENCHMARK_RATE != null && row.allProviders.CORE_BENCHMARK_RATE != null
        ? Math.abs(base.allProviders.CORE_BENCHMARK_RATE - row.allProviders.CORE_BENCHMARK_RATE)
        : 0;
    maxSubjectPp = Math.max(maxSubjectPp, ds);
    maxBenchPp = Math.max(maxBenchPp, db);
    drops.push({ scenarioId: sid, subjectPpMove: roundAdpPercent(ds), benchmarkPpMove: roundAdpPercent(db) });
  }
  const thin = ids.length < 6 || maxSubjectPp >= MATERIAL_SUBJECT_LOO_PP;
  return {
    scenarioCount: ids.length,
    maxSubjectPpMove: roundAdpPercent(maxSubjectPp),
    maxBenchmarkPpMove: roundAdpPercent(maxBenchPp),
    SCENARIO_THINNESS_HIGH: thin,
    SCENARIO_SENSITIVITY: thin ? "HIGH" : maxSubjectPp >= 5 ? "MEDIUM" : "LOW",
    drops,
  };
}

/**
 * MODEL_P_D provider leave-one-out: dual underlying-rate stability.
 * Production certification uses subject ≤10pp AND CORE ≤5pp.
 * The 8pp MATERIAL_PROVIDER_LOO_PP flag is retained as EARLY_WARNING / RESEARCH_DIAGNOSTIC only —
 * it does NOT independently block customer certification.
 */
export const PROVIDER_MODEL_PD_SUBJECT_PP_MAX = 10;
export const PROVIDER_MODEL_PD_CORE_PP_MAX = 5;

export function providerLeaveOneOutRates(observations, scenarios, intent, propertyProfile = null) {
  const options = propertyProfile ? { propertyProfile, coreIds: coreIdsForIntent(intent, propertyProfile) } : {};
  const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent, options);
  const included = v2.allProviders.includedProviders || [];
  const baseS = v2.allProviders.subjectRate;
  const baseC = v2.allProviders.coreBenchmarkRate;
  const coreIds = options.coreIds || coreIdsForIntent(intent, propertyProfile);
  const moves = {};
  const coreMoves = {};
  let earlyWarning = false;
  let maxSubjectPp = 0;
  let maxCorePp = 0;
  for (const p of PROVIDERS) {
    const others = included.filter((x) => x !== p);
    if (others.length < 2 || !Number.isFinite(baseS)) {
      moves[p] = null;
      coreMoves[p] = null;
      continue;
    }
    const subject = mean(others.map((id) => v2.byProvider[id].subjectRate));
    const deltaSPp = subject == null ? null : Math.abs(subject - baseS) * 100;
    moves[p] = deltaSPp == null ? null : roundAdpPercent(deltaSPp);
    if (deltaSPp != null && deltaSPp >= MATERIAL_PROVIDER_LOO_PP) earlyWarning = true;
    if (deltaSPp != null && deltaSPp > maxSubjectPp) maxSubjectPp = deltaSPp;

    const looCore = Number.isFinite(baseC)
      ? mean(
          coreIds.map((cid) =>
            mean(others.map((id) => v2.byProvider[id]?.peerRates?.find((r) => r.entityId === cid)?.rate))
          )
        )
      : null;
    const deltaCPp = looCore != null && Number.isFinite(baseC) ? Math.abs(looCore - baseC) * 100 : null;
    coreMoves[p] = deltaCPp == null ? null : roundAdpPercent(deltaCPp);
    if (deltaCPp != null && deltaCPp > maxCorePp) maxCorePp = deltaCPp;
  }

  const subjectUnstable = maxSubjectPp >= PROVIDER_MODEL_PD_SUBJECT_PP_MAX;
  const coreUnstable = maxCorePp >= PROVIDER_MODEL_PD_CORE_PP_MAX;
  const MODEL_PD_FAIL = subjectUnstable || coreUnstable;

  return {
    dropProviderSubjectPp: moves,
    dropProviderCorePp: coreMoves,
    maxSubjectProviderLooPp: roundAdpPercent(maxSubjectPp),
    maxCoreProviderLooPp: roundAdpPercent(maxCorePp),
    PROVIDER_CONCENTRATION_RISK: earlyWarning,
    PROVIDER_MODEL_PD_FAIL: MODEL_PD_FAIL,
    PROVIDER_MODEL_PD_SUBJECT_FAIL: subjectUnstable,
    PROVIDER_MODEL_PD_CORE_FAIL: coreUnstable,
    PROVIDER_SENSITIVITY: MODEL_PD_FAIL ? "HIGH" : included.length >= 3 ? "MEDIUM" : "HIGH",
  };
}

export function certifyTerritoryBenchmarkRates({
  coreCount,
  scenarioCount,
  providerCount,
  comparableN,
  comparablePeriods,
  scenarioLoo,
  providerLoo,
  canonicalPeers,
}) {
  const blockers = [];
  if (coreCount < MIN_CORE_PEERS_PRODUCTION) blockers.push("core_lt_4");
  if (scenarioCount < 8) blockers.push("scenario_density_lt_8");
  if (providerCount < 3) blockers.push("provider_scopes_lt_3");
  if (!canonicalPeers) blockers.push("peer_identity");
  if ((comparableN || 0) < 20) blockers.push("thin_observations");
  if (scenarioLoo?.SCENARIO_THINNESS_HIGH) blockers.push("single_scenario_dominance");
  if (providerLoo?.PROVIDER_MODEL_PD_FAIL) blockers.push("provider_concentration");
  else if (providerLoo?.PROVIDER_CONCENTRATION_RISK) blockers.push("provider_early_warning");
  if ((comparablePeriods || 0) < 4) blockers.push("lt_4_comparable_periods");

  let STATUS = "READY_FOR_CUSTOMER_BENCHMARK_DISPLAY";
  if (coreCount < MIN_CORE_PEERS_PRODUCTION || scenarioCount < 6) STATUS = "NOT_READY";
  else if (blockers.length) STATUS = "CONDITIONAL";
  if (scenarioCount < 8 && coreCount >= MIN_CORE_PEERS_PRODUCTION) {
    STATUS = scenarioCount < 6 ? "NOT_READY" : "CONDITIONAL";
  }
  return { STATUS, blockers, ratioStabilityRequired: false };
}
