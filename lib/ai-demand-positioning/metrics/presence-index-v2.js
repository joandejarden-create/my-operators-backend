/**
 * AI Presence Index V2 — RESEARCH ONLY.
 * Observation grain × territory × provider, then derived All Providers.
 * CORE peers include zero-presence. No score cap. Not added to owner payload.
 */

import { PROVIDERS } from "../data-model.js";
import { filterComparableObservations } from "./grain-governance.js";
import { canonicalizeToEntityId } from "./south-florida-entity-registry.js";
import { canonicalizeForProperty } from "./adp-property-entity-registries.js";
import { isGovernedNonWaterstoneProperty } from "./property-core-governance-data.js";
import { roundAdpPercent } from "../format-percent.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import {
  coreIdsForIntent,
  MIN_CORE_PEERS_PRODUCTION,
  MIN_CORE_PEERS_RESEARCH,
  MIN_PROVIDER_OBSERVATIONS,
  PRESENCE_BENCHMARK_VERSION,
  PRESENCE_INDEX_V2_METRIC_VERSION,
  assertCoreSetIntegrity,
} from "./presence-benchmark-v1.js";

export const ALL_PROVIDERS_METHOD = "A_EQUAL_MEAN_OF_PROVIDER_RATES_THEN_INDEX";

export const PRESENCE_INDEX_V2_CUSTOMER_QUESTION =
  "How often does this hotel appear in comparable AI responses for this demand territory, relative to the average appearance rate of governed CORE comparable hotels in the same territory and provider scope?";

export const PRESENCE_INDEX_V2_FORMULA =
  "PRESENCE_INDEX_V2 = SUBJECT_PRESENCE_RATE / CORE_BENCHMARK_PRESENCE_RATE × 100; " +
  "CORE_BENCHMARK = arithmetic mean of all territory CORE peer rates (zeros included); no cap";

function mean(values) {
  const xs = values.filter((v) => Number.isFinite(v));
  if (!xs.length) return null;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function median(values) {
  const xs = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!xs.length) return null;
  const m = Math.floor(xs.length / 2);
  return xs.length % 2 ? xs[m] : (xs[m - 1] + xs[m]) / 2;
}

function resolveEntityId(name, propertyProfile) {
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return canonicalizeForProperty(propertyId, name);
  }
  return canonicalizeToEntityId(name);
}

export function peerAppearsInObservation(obs, entityId, propertyProfile = null) {
  return (obs.competitorsMentioned || []).some((name) => resolveEntityId(name, propertyProfile) === entityId);
}

export function computeScopePresenceRates(observations, coreIds, propertyProfile = null) {
  const comparable = filterComparableObservations(observations);
  const n = comparable.length;
  if (!n) {
    return { ok: false, reason: "NO_COMPARABLE_OBSERVATIONS", comparableN: 0 };
  }
  const subjectAppearances = comparable.filter((o) => o.mentioned).length;
  const subjectRate = subjectAppearances / n;
  const peerRates = (coreIds || []).map((id) => {
    const appearances = comparable.filter((o) => peerAppearsInObservation(o, id, propertyProfile)).length;
    return { entityId: id, appearances, rate: appearances / n };
  });
  const rates = peerRates.map((p) => p.rate);
  const coreBenchmarkMean = peerRates.length ? mean(rates) : null;
  const coreBenchmarkMedian = peerRates.length ? median(rates) : null;
  const zeroPresencePeers = peerRates.filter((p) => p.rate === 0).map((p) => p.entityId);
  return {
    ok: true,
    comparableN: n,
    subjectAppearances,
    subjectRate,
    subjectRatePct: roundAdpPercent(subjectRate * 100),
    peerRates,
    coreBenchmarkMean,
    coreBenchmarkMedian,
    coreBenchmarkMeanPct: coreBenchmarkMean == null ? null : roundAdpPercent(coreBenchmarkMean * 100),
    zeroPresencePeers,
    recommendedBenchmark: "MEAN",
  };
}

export function presenceIndexFromRates(subjectRate, benchmarkRate) {
  if (!Number.isFinite(subjectRate) || !Number.isFinite(benchmarkRate)) {
    return { index: null, status: "BLOCKED", reason: "missing_rates" };
  }
  if (benchmarkRate === 0) {
    return { index: null, status: "BENCHMARK_NOT_ESTABLISHED", reason: "zero_core_benchmark" };
  }
  const raw = (subjectRate / benchmarkRate) * 100;
  return {
    index: Math.round(raw),
    raw,
    status: "COMPUTED",
    cap: "NONE",
    parityValid: true,
  };
}

export function classifyExtreme(index, subjectRate, benchmarkRate, comparableN) {
  if (index == null) return null;
  const flags = [];
  if (index > 300) flags.push(">300");
  else if (index > 200) flags.push(">200");
  if (index < 25) flags.push("<25");
  else if (index < 50) flags.push("<50");
  if (!flags.length) return { extreme: false, flags, class: "IN_RANGE" };
  let cls = "VALID_EXTREME";
  if (benchmarkRate > 0 && benchmarkRate < 0.05 && index > 200) cls = "DENOMINATOR_ARTIFACT";
  if (comparableN < 12) cls = "THIN_SAMPLE";
  return { extreme: true, flags, class: cls, subjectRate, benchmarkRate, comparableN };
}

function territoryObservations(observations, scenarios, intent) {
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  return filterComparableObservations(observations).filter((o) => ids.has(o.scenarioId));
}

export function computePresenceIndexV2ForIntent(observations, scenarios, intent, options = {}) {
  const propertyProfile = options.propertyProfile || null;
  const coreIds = options.coreIds || coreIdsForIntent(intent, propertyProfile);
  const integrity = assertCoreSetIntegrity(coreIds, propertyProfile);
  const inTerritory = territoryObservations(observations, scenarios, intent);

  const byProvider = {};
  const includedProviders = [];
  for (const provider of PROVIDERS) {
    const scoped = inTerritory.filter((o) => o.provider === provider);
    const rates = computeScopePresenceRates(scoped, coreIds, propertyProfile);
    if (!rates.ok || rates.comparableN < MIN_PROVIDER_OBSERVATIONS) {
      byProvider[provider] = {
        included: false,
        missingNotZero: true,
        reason: rates.ok ? "THIN_PROVIDER_SAMPLE" : rates.reason,
        comparableN: rates.comparableN || 0,
        index: null,
      };
      continue;
    }
    const idx = presenceIndexFromRates(rates.subjectRate, rates.coreBenchmarkMean);
    byProvider[provider] = {
      included: true,
      missingNotZero: true,
      ...rates,
      index: idx.index,
      status: idx.status,
      extreme: classifyExtreme(idx.index, rates.subjectRate, rates.coreBenchmarkMean, rates.comparableN),
    };
    includedProviders.push(provider);
  }

  let allProviders = {
    method: ALL_PROVIDERS_METHOD,
    includedProviders,
    index: null,
    status: includedProviders.length ? "COMPUTED" : "BENCHMARK_DEVELOPING",
    subjectRate: null,
    coreBenchmarkRate: null,
  };

  if (includedProviders.length) {
    const subjectRate = mean(includedProviders.map((p) => byProvider[p].subjectRate));
    const peerRates = coreIds.map((id) => ({
      entityId: id,
      rate: mean(includedProviders.map((p) => byProvider[p].peerRates.find((r) => r.entityId === id)?.rate)),
    }));
    const coreBenchmarkRate = mean(peerRates.map((p) => p.rate));
    const idx = presenceIndexFromRates(subjectRate, coreBenchmarkRate);
    allProviders = {
      method: ALL_PROVIDERS_METHOD,
      includedProviders,
      comparableN: includedProviders.reduce((s, p) => s + byProvider[p].comparableN, 0),
      subjectRate,
      subjectRatePct: roundAdpPercent(subjectRate * 100),
      peerRates,
      zeroPresencePeers: peerRates.filter((p) => p.rate === 0).map((p) => p.entityId),
      coreBenchmarkRate,
      coreBenchmarkMedian: median(peerRates.map((p) => p.rate)),
      coreBenchmarkRatePct: coreBenchmarkRate == null ? null : roundAdpPercent(coreBenchmarkRate * 100),
      index: idx.index,
      status: idx.status,
      extreme: classifyExtreme(idx.index, subjectRate, coreBenchmarkRate, includedProviders.reduce((s, p) => s + byProvider[p].comparableN, 0)),
    };
  }

  return {
    intent,
    territory: territoryLabelForIntent(intent),
    coreIds,
    coreCount: coreIds.length,
    integrity,
    versions: { PRESENCE_BENCHMARK_VERSION, PRESENCE_INDEX_V2_METRIC_VERSION },
    byProvider,
    allProviders,
    grain: "OBSERVATION × territory × provider; All Providers derived by equal-mean of included provider rates",
  };
}

export function certifyPresenceIndexV2(row, longitudinal) {
  const blockers = [];
  const coreCount = row.coreCount;
  const ap = row.allProviders;
  if (coreCount < MIN_CORE_PEERS_RESEARCH) blockers.push("core_lt_3");
  if (coreCount < MIN_CORE_PEERS_PRODUCTION) blockers.push("core_lt_4");
  if (row.integrity.DUPLICATE_CORE_ENTITIES) blockers.push("duplicate_core");
  if (row.integrity.GENERIC_CORE_ENTITIES) blockers.push("generic_core");
  if (!ap || ap.index == null) {
    if (ap?.status === "BENCHMARK_NOT_ESTABLISHED") blockers.push("zero_benchmark");
    else if (!(ap?.includedProviders || []).length) blockers.push("thin_provider_samples");
    else blockers.push("index_null");
  }
  if ((ap?.includedProviders || []).length < 3) blockers.push("provider_scopes_lt_3");
  if ((ap?.comparableN || 0) < 20) blockers.push("thin_observations");
  if (ap?.extreme?.class === "DENOMINATOR_ARTIFACT") blockers.push("denominator_artifact");
  if ((longitudinal?.comparablePeriods || 0) < 2) blockers.push("lt_2_comparable_periods");
  if (longitudinal?.volatility === "HIGH VOLATILITY") blockers.push("high_volatility");

  let status = "PRODUCTION_VALIDATED";
  if (coreCount < MIN_CORE_PEERS_PRODUCTION) status = "BENCHMARK_DEVELOPING";
  else if (blockers.includes("zero_benchmark")) status = "BLOCKED";
  else if (blockers.includes("thin_provider_samples") || blockers.includes("index_null")) status = "BENCHMARK_DEVELOPING";
  else if (blockers.includes("high_volatility") || blockers.includes("thin_observations") || blockers.includes("core_lt_4")) {
    status = blockers.includes("core_lt_4") ? "BENCHMARK_DEVELOPING" : "CONDITIONALLY_ELIGIBLE";
  } else if (blockers.length) status = "RESEARCH_ONLY";
  if (coreCount >= MIN_CORE_PEERS_PRODUCTION && ap?.index != null && blockers.includes("high_volatility")) {
    status = "CONDITIONALLY_ELIGIBLE";
  }
  if (coreCount >= MIN_CORE_PEERS_PRODUCTION && ap?.index != null && !blockers.includes("zero_benchmark") && blockers.length) {
    if (status === "PRODUCTION_VALIDATED") status = "CONDITIONALLY_ELIGIBLE";
  }
  return { status, blockers };
}
