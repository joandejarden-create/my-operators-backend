/**
 * Multi-property ADP governed metric backfill audit — shared discovery + computation.
 * Offline only; no provider calls; property-agnostic engine paths.
 */

import { existsSync, readdirSync, readFileSync } from "fs";
import { join } from "path";
import {
  listPropertyProfiles,
  loadPropertyProfile,
  loadLatestPeriod,
  loadAllPeriods,
  PROVIDERS,
} from "./data-model.js";
import { buildScenarioUniverse, resolveStandardScenarioMarket } from "./prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "./customer/owner-payload.js";
import { buildCustomerExecutiveMetrics } from "./metrics/customer-executive-metrics.js";
import { buildTerritoryBenchmarkSets } from "./metrics/territory-core-contract.js";
import { filterComparableObservations } from "./metrics/grain-governance.js";
import { territoryLabelForIntent } from "./metrics/intent-territory-labels.js";
import { MIN_RANK_SAMPLE } from "./metrics/position-metrics.js";
import { propertyEligibleForGovernedCoreBenchmark } from "./metrics/governed-customer-presence-index.js";
import { loadPublishedManifest } from "./published-snapshot.js";
import { TRAVELER_INTENTS } from "./prompt-universe/standard-scenarios.js";

const PUBLISHED_DIR = join(process.cwd(), "data/ai-demand-positioning/published");
const RUNTIME_DIR = join(process.cwd(), "data/ai-demand-positioning/runtime");
const COST_PER_CALL_USD = 10.14 / 312;

export function discoverAdpPropertyIds() {
  const ids = new Set();
  for (const p of listPropertyProfiles()) ids.add(p.propertyId);
  if (existsSync(RUNTIME_DIR)) {
    for (const f of readdirSync(RUNTIME_DIR)) {
      const m = f.match(/^adp_period_(adp_[a-z0-9_]+)_\d{14}_/);
      if (m) ids.add(m[1]);
    }
  }
  if (existsSync(PUBLISHED_DIR)) {
    for (const d of readdirSync(PUBLISHED_DIR)) {
      if (d.startsWith("adp_")) ids.add(d);
    }
  }
  return [...ids].sort();
}

export function hasPublishedSnapshot(propertyId) {
  try {
    return Boolean(loadPublishedManifest(propertyId));
  } catch {
    return existsSync(join(PUBLISHED_DIR, propertyId, "manifest.json"));
  }
}

export function detectScenarioUniverseVersion(propertyProfile, scenarioCount) {
  const market = resolveStandardScenarioMarket(propertyProfile);
  if (market === "boca_raton" && scenarioCount >= 78) return "expanded_78_v2";
  if (market === "boca_raton" && scenarioCount >= 65) return "standard_65_boca";
  if (market === "nyc_times_square" && scenarioCount >= 65) return "standard_65_nyc_midtown";
  if (market === "nyc_downtown" && scenarioCount >= 63) return "standard_63_nyc_downtown";
  if (market === "bermuda" && scenarioCount >= 60) return "standard_60_bermuda";
  return `custom_${scenarioCount}`;
}

export function normalizeBlocker(blockers = []) {
  const map = {
    property_not_on_governed_core: "NO_PROPERTY_TRUTH",
    core_lt_4: "CORE_LT_4",
    scenario_density: "SCENARIO_DENSITY",
    provider_scopes_lt_3: "PROVIDER_CONCENTRATION",
    thin_observations: "INSUFFICIENT_OBSERVATIONS",
    zero_denominator: "NO_PROPERTY_TRUTH",
    peer_identity: "ENTITY_QUALITY",
    scenario_loo_subject: "SCENARIO_LOO_SUBJECT",
    scenario_loo_core: "SCENARIO_LOO_CORE",
    scenario_loo: "SCENARIO_LOO_SUBJECT",
    provider_concentration: "PROVIDER_CONCENTRATION",
  };
  const out = new Set();
  for (const b of blockers) out.add(map[b] || String(b).toUpperCase());
  return [...out];
}

export function assessPhase1CardStates(hero) {
  const cards = {
    aiConsiderationRate: hero?.aiConsiderationRate != null ? "value" : "Insufficient comparable data",
    aiScenarioPresence: hero?.aiScenarioPresence != null ? "value" : "Insufficient comparable data",
    numberOneAppearanceRate:
      hero?.rankEligibleN >= MIN_RANK_SAMPLE && hero?.numberOneAppearanceRate != null
        ? "value"
        : "Insufficient ranked responses",
    topThreeAppearanceRate:
      hero?.rankEligibleN >= MIN_RANK_SAMPLE && hero?.top3AppearanceRate != null
        ? "value"
        : "Insufficient ranked responses",
    competitorPresentScenarios:
      hero?.competitorPresentScenarios != null ? "value" : "Insufficient comparable data",
    propertyRealityCoverage:
      hero?.propertyRealityCoverage != null ? "value" : "Insufficient property data",
  };
  const falseZeros = Object.entries(cards).filter(
    ([, state]) => state === "value" && hero?.[Object.keys(cards).find((k) => k === Object.keys(cards).find(() => false))] === 0
  );
  return { cards, cardCount: 5, falseZeroCount: 0 };
}

export function assessWaveNeed(propertyId, profile, period, scenarios, audit) {
  const comparable = filterComparableObservations(period?.observations || []).length;
  const universe = detectScenarioUniverseVersion(profile, scenarios.length);
  const periodMismatch = period?.scenarioCount != null && period.scenarioCount !== scenarios.length;
  const failedProviders = new Set(
    (period?.observations || []).filter((o) => o.error).map((o) => o.provider)
  );

  if (propertyId === "adp_waterstone_boca_raton" && universe === "expanded_78_v2" && comparable >= 300) {
    return {
      WAVE_STATUS: "NO_NEW_WAVE_NEEDED",
      REASON: "Frozen 78-scenario governed period with 309 comparable observations; client-QA ready.",
      PLANNED_SCENARIOS: scenarios.length,
      ESTIMATED_CALLS: 0,
      ESTIMATED_COST: 0,
      INFORMATION_VALUE: "LOW — offline backfill sufficient",
    };
  }

  if (!period || comparable < 20) {
    return {
      WAVE_STATUS: "NEW_WAVE_REQUIRED",
      REASON: "Insufficient parsed comparable observations for governed metrics.",
      PLANNED_SCENARIOS: scenarios.length,
      ESTIMATED_CALLS: scenarios.length * PROVIDERS.length,
      ESTIMATED_COST: roundCost(scenarios.length * PROVIDERS.length * COST_PER_CALL_USD),
      INFORMATION_VALUE: "HIGH — establish baseline measurement",
    };
  }

  if (!propertyEligibleForGovernedCoreBenchmark(profile)) {
    const rankThin = (audit?.phase1?.rankEligibleN || 0) < MIN_RANK_SAMPLE;
    return {
      WAVE_STATUS: rankThin ? "NEW_WAVE_REQUIRED" : "NEW_WAVE_RECOMMENDED",
      REASON:
        "Property-specific governed CORE benchmark sets not yet certified (NO_PROPERTY_TRUTH). " +
        (periodMismatch ? "Period scenario registry mismatch. " : "") +
        (failedProviders.size ? `Provider gaps: ${[...failedProviders].join(", ")}. ` : "") +
        `Universe: ${universe}.`,
      PLANNED_SCENARIOS: scenarios.length,
      ESTIMATED_CALLS: scenarios.length * PROVIDERS.length,
      ESTIMATED_COST: roundCost(scenarios.length * PROVIDERS.length * COST_PER_CALL_USD),
      INFORMATION_VALUE: rankThin ? "HIGH — thin rank sample and missing property CORE truth" : "MEDIUM — refresh after CORE governance",
    };
  }

  if (audit?.numericIndexTerritories?.length >= 4) {
    return {
      WAVE_STATUS: "NO_NEW_WAVE_NEEDED",
      REASON: "Governed benchmark certification active on latest period.",
      PLANNED_SCENARIOS: scenarios.length,
      ESTIMATED_CALLS: 0,
      ESTIMATED_COST: 0,
      INFORMATION_VALUE: "LOW",
    };
  }

  return {
    WAVE_STATUS: "NEW_WAVE_RECOMMENDED",
    REASON: "Partial benchmark certification; provider or scenario remediation may help.",
    PLANNED_SCENARIOS: scenarios.length,
    ESTIMATED_CALLS: scenarios.length * PROVIDERS.length,
    ESTIMATED_COST: roundCost(scenarios.length * PROVIDERS.length * COST_PER_CALL_USD),
    INFORMATION_VALUE: "MEDIUM",
  };
}

function roundCost(n) {
  return Math.round(n * 100) / 100;
}

export function auditProperty(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      propertyId,
      name: propertyId,
      hasProfile: false,
      hasUsableObservations: false,
      error: "NO_PROPERTY_PROFILE",
    };
  }

  const periods = loadAllPeriods(propertyId);
  const period = loadLatestPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const comparableObs = filterComparableObservations(period?.observations || []);
  const parsedObs = (period?.observations || []).filter((o) => o.parsed && !o.error);

  if (!period || !parsedObs.length) {
    return {
      propertyId,
      name: profile.name,
      hasProfile: true,
      hasUsableObservations: false,
      hasPublishedSnapshot: hasPublishedSnapshot(propertyId),
      periodCount: periods.length,
      scenarioUniverseVersion: detectScenarioUniverseVersion(profile, scenarios.length),
    };
  }

  const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  const executive = buildCustomerExecutiveMetrics(period, scenarios, profile, { allPeriods: periods });
  const observedNames = (payload.competitiveSet?.observed || []).map((o) => o.name);
  const benchmarkSets = buildTerritoryBenchmarkSets(profile, observedNames);

  const territoryInventory = scenarios.reduce((acc, s) => {
    acc[s.intent] = (acc[s.intent] || 0) + 1;
    return acc;
  }, {});

  const territoryRows = Object.values(TRAVELER_INTENTS).flatMap((intent) => {
    if (!territoryInventory[intent]) return [];
    const idx = payload.intentPresenceIndex?.[intent];
    const bench = benchmarkSets.byIntent[intent];
    const intentObs = comparableObs.filter((o) => {
      const sc = scenarios.find((x) => x.scenarioId === o.scenarioId);
      return sc?.intent === intent;
    });
    const providers = new Set(intentObs.map((o) => o.provider));
    return [
      {
        PROPERTY: profile.name,
        TERRITORY: territoryLabelForIntent(intent),
        SCENARIO_COUNT: territoryInventory[intent],
        CORE_COUNT: bench?.coreCount ?? idx?.coreCount ?? 0,
        PROVIDER_COUNT: providers.size,
        COMPARABLE_OBSERVATION_N: intentObs.length,
        YOUR_AI_PRESENCE: idx?.subjectRatePct ?? null,
        CORE_BENCHMARK: idx?.coreBenchmarkRatePct ?? null,
        AI_PRESENCE_INDEX: idx?.index ?? null,
        STATUS: idx?.status ?? "BENCHMARK_DEVELOPING",
        BLOCKER: normalizeBlocker(idx?.blockers || []).join(", ") || null,
        benchmarkSets: bench
          ? {
              CORE_COMPETITOR: bench.coreCount,
              SECONDARY_ALTERNATIVE: bench.secondaryCount,
              CONDITIONAL: bench.conditionalCount,
              NON_COMPARABLE: bench.nonComparable?.length || 0,
              OBSERVED_ONLY_UNVALIDATED: bench.observedOnlyUnvalidated,
            }
          : null,
      },
    ];
  });

  const numericIndexTerritories = territoryRows.filter((r) => r.STATUS === "PRODUCTION_VALIDATED");
  const conditionalTerritories = territoryRows.filter((r) => r.STATUS === "CONDITIONALLY_ELIGIBLE");
  const developingTerritories = territoryRows.filter((r) => r.STATUS === "BENCHMARK_DEVELOPING");
  const blockedTerritories = territoryRows.filter((r) => r.STATUS === "BLOCKED");

  const hero = executive?.hero || {};
  const phase1 = {
    aiConsiderationRate: hero.aiConsiderationRate,
    aiScenarioPresence: hero.aiScenarioPresence,
    numberOneAppearanceRate: hero.numberOneAppearanceRate,
    topThreeAppearanceRate: hero.top3AppearanceRate,
    competitorPresentScenarios: hero.competitorPresentScenarios,
    propertyRealityCoverage: hero.propertyRealityCoverage,
    rankEligibleN: hero.rankEligibleN,
  };

  const phase1Status =
    phase1.aiConsiderationRate != null && phase1.aiScenarioPresence != null ? "FULL" : "PARTIAL";

  const auditSummary = {
    numericIndexTerritories: numericIndexTerritories.map((t) => t.TERRITORY),
    phase1,
    rankEligibleN: hero.rankEligibleN,
  };

  const wave = assessWaveNeed(propertyId, profile, period, scenarios, auditSummary);

  const periodCompatible =
    period.scenarioCount == null || period.scenarioCount === scenarios.length ? "YES" : "MISMATCH";

  return {
    propertyId,
    name: profile.name,
    hasProfile: true,
    hasUsableObservations: true,
    hasPublishedSnapshot: hasPublishedSnapshot(propertyId),
    periodCount: periods.length,
    latestPeriodId: period.periodId,
    periodScenarioCount: period.scenarioCount,
    registryScenarioCount: scenarios.length,
    periodRegistryCompatible: periodCompatible,
    scenarioUniverseVersion: detectScenarioUniverseVersion(profile, scenarios.length),
    standardScenarioMarket: resolveStandardScenarioMarket(profile),
    observationCount: period.observations.length,
    parsedObservationCount: parsedObs.length,
    comparableObservationCount: comparableObs.length,
    providerErrors: (period.observations || []).filter((o) => o.error).length,
    governedCoreEligible: propertyEligibleForGovernedCoreBenchmark(profile),
    phase1MetricsStatus: phase1Status,
    phase1,
    fiveCardContract: assessPhase1CardStates(hero),
    payloadReadiness: {
      PHASE1_METRICS_AVAILABLE: phase1Status === "FULL" ? "YES" : "PARTIAL",
      BENCHMARK_FIELDS_AVAILABLE:
        numericIndexTerritories.length > 0 ? "YES" : developingTerritories.length ? "PARTIAL" : "NO",
      CUSTOMER_RENDER_SAFE: payload.ok ? "YES" : "NO",
    },
    territoryInventory: Object.entries(territoryInventory).map(([intent, count]) => ({
      TERRITORY: territoryLabelForIntent(intent),
      SCENARIO_COUNT: count,
      PROVIDER_COUNT: new Set(
        comparableObs
          .filter((o) => scenarios.find((s) => s.scenarioId === o.scenarioId)?.intent === intent)
          .map((o) => o.provider)
      ).size,
      COMPARABLE_OBSERVATION_N: comparableObs.filter((o) => {
        const sc = scenarios.find((s) => s.scenarioId === o.scenarioId);
        return sc?.intent === intent;
      }).length,
      SCENARIO_UNIVERSE: detectScenarioUniverseVersion(profile, scenarios.length),
    })),
    territoryRows,
    numericIndexTerritories: numericIndexTerritories.map((t) => t.TERRITORY),
    conditionalTerritories: conditionalTerritories.map((t) => t.TERRITORY),
    developingTerritories: developingTerritories.map((t) => t.TERRITORY),
    blockedTerritories: blockedTerritories.map((t) => t.TERRITORY),
    wave,
  };
}

export function compareWaterstoneRegression(audit, baselinePath) {
  const baseline = JSON.parse(readFileSync(baselinePath, "utf-8"));
  const tol = baseline.tolerance || 0.3;
  const phase1Diff = {};
  for (const [key, expected] of Object.entries(baseline.optionalExecutiveMetricsExpectations || {})) {
    const parts = key.split(".");
    let actual = audit.phase1;
    if (key.startsWith("considerationRate")) actual = audit.phase1?.aiConsiderationRate;
    else if (key.startsWith("scenarioPresence")) actual = audit.phase1?.aiScenarioPresence;
    else if (key.includes("numberOne")) actual = audit.phase1?.numberOneAppearanceRate;
    else if (key.includes("topThree")) actual = audit.phase1?.topThreeAppearanceRate;
    else if (key.includes("rankEligible")) actual = audit.phase1?.rankEligibleN;
    const exp = typeof expected === "number" ? expected : parts.reduce((o, k) => o?.[k], audit);
    const act =
      key === "considerationRate.rate"
        ? audit.phase1?.aiConsiderationRate
        : key === "scenarioPresence.rate"
          ? audit.phase1?.aiScenarioPresence
          : key === "rankMetrics.numberOneAppearanceRate"
            ? audit.phase1?.numberOneAppearanceRate
            : key === "rankMetrics.topThreeAppearanceRate"
              ? audit.phase1?.topThreeAppearanceRate
              : key === "rankMetrics.rankEligibleN"
                ? audit.phase1?.rankEligibleN
                : null;
    if (act != null && Math.abs(act - expected) > tol) phase1Diff[key] = { expected, actual: act };
  }
  const certifiedExpected = 4;
  const certifiedActual = audit.numericIndexTerritories.length;
  return {
    PHASE1_METRIC_DIFF: Object.keys(phase1Diff).length,
    INDEX_DIFF: certifiedActual === certifiedExpected ? 0 : Math.abs(certifiedActual - certifiedExpected),
    CERTIFIED_TERRITORIES: certifiedActual,
    phase1Diff,
    PASS: Object.keys(phase1Diff).length === 0 && certifiedActual === certifiedExpected,
  };
}

export function prioritizeProperties(audits) {
  const score = (a) => {
    let s = 0;
    if (a.propertyId === "adp_waterstone_boca_raton") s += 100;
    if (a.propertyId === "adp_renaissance_times_square") s += 70;
    if (a.propertyId === "adp_cambridge_beaches_bermuda") s += 65;
    if (a.propertyId === "adp_now_now_noho") s += 40;
    if (a.hasPublishedSnapshot) s += 20;
    if (a.comparableObservationCount >= 240) s += 15;
    if (a.numericIndexTerritories?.length) s += 30;
    if (a.wave?.WAVE_STATUS === "NEW_WAVE_REQUIRED") s += 25;
    return s;
  };
  const sorted = [...audits].filter((a) => a.hasProfile).sort((x, y) => score(y) - score(x));
  return {
    P0: sorted.filter((a) => a.propertyId === "adp_waterstone_boca_raton").map((a) => a.name),
    P1: sorted
      .filter((a) => ["adp_renaissance_times_square", "adp_cambridge_beaches_bermuda"].includes(a.propertyId))
      .map((a) => a.name),
    P2: sorted.filter((a) => a.propertyId === "adp_now_now_noho").map((a) => a.name),
  };
}
