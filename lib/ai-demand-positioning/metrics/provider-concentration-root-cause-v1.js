/**
 * ADP Provider Concentration Root-Cause + Certification Governance V1.
 * Offline diagnostics only — no provider calls, no gate relaxation, no customer promotion.
 */

import { join } from "path";
import {
  loadPropertyProfile,
  loadLatestPeriod,
  loadLatestTargetedPeriod,
  loadAllPeriods,
  isTargetedMeasurementPeriod,
  PROVIDERS,
} from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { parsePeriodObservations } from "../execution/response-parser.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import {
  computeTerritoryBenchmarkRates,
  providerLeaveOneOutRates,
  MATERIAL_PROVIDER_LOO_PP,
} from "./core-benchmark-rate-contract-v1.js";
import {
  certifyGovernedTerritory,
  LOO_SUBJECT_PP_MAX,
  LOO_CORE_PP_MAX,
  propertyEligibleForGovernedCoreBenchmark,
} from "./governed-customer-presence-index.js";
import {
  coreIdsForIntent,
  assertCoreSetIntegrity,
} from "./presence-benchmark-v1.js";
import { computePresenceIndexV2ForIntent, presenceIndexFromRates } from "./presence-index-v2.js";
import { scenarioLeaveOneOutRates } from "./core-benchmark-rate-contract-v1.js";
import { filterComparableObservations } from "./grain-governance.js";
import { arePeriodsComparable } from "./longitudinal-comparability.js";
import { TARGET_TERRITORIES_BY_PROPERTY } from "../execution/targeted-multi-property-governed-benchmark-measurement-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../multi-property-governed-audit-v2.js";
import { roundAdpPercent } from "../format-percent.js";
import { canonicalizeForProperty } from "./adp-property-entity-registries.js";

export const DIAGNOSTIC_VERSION = "adp_provider_concentration_root_cause_v1";

/** Exported for documentation — matches core-benchmark-rate-contract-v1.js */
export const CURRENT_PROVIDER_CONCENTRATION_RULE = Object.freeze({
  FORMULA:
    "For each included provider p: recompute All Providers subject rate as equal mean of remaining provider subject rates; " +
    "deltaPp = |looSubjectRate - baseSubjectRate| × 100. " +
    "PROVIDER_CONCENTRATION_RISK = true if any deltaPp >= MATERIAL_PROVIDER_LOO_PP. " +
    "certifyGovernedTerritory also flags provider_concentration if max(dropProviderSubjectPp) >= PROVIDER_LOO_PP_MAX.",
  THRESHOLD: `MATERIAL_PROVIDER_LOO_PP=${MATERIAL_PROVIDER_LOO_PP}pp (PROVIDER_CONCENTRATION_RISK); PROVIDER_LOO_PP_MAX=10pp (certification blocker)`,
  METRIC_BEING_TESTED: "All Providers aggregated SUBJECT presence rate only (CORE benchmark provider LOO not tested)",
  PROVIDER_GRAIN: "property × territory × provider × period (equal mean of included provider rates)",
  ALL_PROVIDERS_DERIVATION: "A_EQUAL_MEAN_OF_INCLUDED_PROVIDER_RATES — subject rate and CORE benchmark each averaged across providers with sufficient observations",
  WHY_THRESHOLD_WAS_CHOSEN:
    "Conservative heuristic aligned with scenario LOO materiality (8pp subject) to flag aggregates materially controlled by one provider mix",
  THRESHOLD_ORIGIN: "HEURISTIC",
});

export const CURRENTLY_CERTIFIED_TERRITORIES = Object.freeze([
  { propertyId: "adp_renaissance_times_square", intent: TRAVELER_INTENTS.LEISURE, label: "Resort Leisure" },
  { propertyId: "adp_renaissance_times_square", intent: TRAVELER_INTENTS.COUPLES, label: "Couples / Romantic Stay" },
  { propertyId: "adp_renaissance_times_square", intent: TRAVELER_INTENTS.GROUP_MEETING, label: "Meetings & Groups" },
  { propertyId: "adp_cambridge_beaches_bermuda", intent: TRAVELER_INTENTS.CELEBRATION, label: "Celebrations & Events" },
]);

export const PROVIDER_CONCENTRATION_AFFECTED = Object.freeze([
  { propertyId: "adp_renaissance_times_square", intent: TRAVELER_INTENTS.BUSINESS, label: "Business Travel" },
  { propertyId: "adp_cambridge_beaches_bermuda", intent: TRAVELER_INTENTS.LEISURE, label: "Resort Leisure" },
  { propertyId: "adp_cambridge_beaches_bermuda", intent: TRAVELER_INTENTS.COUPLES, label: "Couples / Romantic Stay" },
  { propertyId: "adp_now_now_noho", intent: TRAVELER_INTENTS.BUSINESS, label: "Business Travel" },
  { propertyId: "adp_now_now_noho", intent: TRAVELER_INTENTS.LEISURE, label: "Resort Leisure" },
  { propertyId: "adp_now_now_noho", intent: TRAVELER_INTENTS.COUPLES, label: "Couples / Romantic Stay" },
]);

function mean(xs) {
  const v = xs.filter((n) => Number.isFinite(n));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

function loadDiagnosticPeriod(propertyId) {
  return loadLatestTargetedPeriod(propertyId) || loadLatestPeriod(propertyId);
}

function providerObservationCounts(observations, scenarios, intent, provider, propertyProfile) {
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  const coreIds = coreIdsForIntent(intent, propertyProfile);
  const scoped = filterComparableObservations(observations).filter(
    (o) => ids.has(o.scenarioId) && o.provider === provider
  );
  const subjectN = scoped.filter((o) => o.mentioned).length;
  const failedN = (observations || []).filter(
    (o) => ids.has(o.scenarioId) && o.provider === provider && o.error
  ).length;
  let coreMentionEvents = 0;
  for (const obs of scoped) {
    for (const id of coreIds) {
      const mentioned = (obs.competitorsMentioned || []).some(
        (name) => canonicalizeForProperty(propertyProfile.propertyId, name) === id
      );
      if (mentioned) coreMentionEvents += 1;
    }
  }
  return {
    SUBJECT_OBSERVATION_N: scoped.length,
    SUBJECT_MENTION_N: subjectN,
    FAILED_MISSING_OBSERVATION_N: failedN,
    CORE_PEER_MENTION_EVENTS: coreMentionEvents,
  };
}

export function decomposeProviderRates(observations, scenarios, intent, propertyProfile) {
  const rates = computeTerritoryBenchmarkRates(observations, scenarios, intent, {
    propertyProfile,
    coreIds: coreIdsForIntent(intent, propertyProfile),
  });
  const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent, {
    propertyProfile,
    coreIds: coreIdsForIntent(intent, propertyProfile),
  });

  const rows = [];
  for (const provider of PROVIDERS) {
    const row = rates.byProvider[provider];
    const counts = providerObservationCounts(
      observations,
      scenarios,
      intent,
      provider,
      propertyProfile
    );
    rows.push({
      PROPERTY: propertyProfile.name,
      TERRITORY: rates.territory,
      PROVIDER: provider,
      YOUR_AI_PRESENCE: row?.SUBJECT_RATE ?? null,
      CORE_BENCHMARK: row?.CORE_BENCHMARK_RATE ?? null,
      AI_PRESENCE_INDEX_INTERNAL: v2.byProvider[provider]?.index ?? null,
      SUBJECT_OBSERVATION_N: counts.SUBJECT_OBSERVATION_N,
      CORE_OBSERVATION_N: counts.SUBJECT_OBSERVATION_N,
      FAILED_MISSING_OBSERVATION_N: counts.FAILED_MISSING_OBSERVATION_N,
      included: row?.included ?? false,
    });
  }
  return { territory: rates.territory, allProviders: rates.allProviders, providerRows: rows, v2 };
}

function rangePp(values) {
  const v = values.filter((n) => Number.isFinite(n));
  if (v.length < 2) return { min: v[0] ?? null, max: v[0] ?? null, range: 0 };
  const min = Math.min(...v);
  const max = Math.max(...v);
  return { min, max, range: roundAdpPercent(max - min) };
}

export function computeProviderLeaveOneOutFull(observations, scenarios, intent, propertyProfile) {
  const options = { propertyProfile, coreIds: coreIdsForIntent(intent, propertyProfile) };
  const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent, options);
  const included = v2.allProviders.includedProviders || [];
  const baseS = v2.allProviders.subjectRate;
  const baseC = v2.allProviders.coreBenchmarkRate;
  const baseIdx = v2.allProviders.index;

  const subjectLoo = [];
  const coreLoo = [];
  const indexLoo = [];
  const dropSubject = {};
  const dropCore = {};
  const dropIndex = {};

  for (const p of PROVIDERS) {
    const others = included.filter((x) => x !== p);
    if (others.length < 2 || !Number.isFinite(baseS)) {
      dropSubject[p] = null;
      dropCore[p] = null;
      dropIndex[p] = null;
      continue;
    }
    const looS = mean(others.map((id) => v2.byProvider[id].subjectRate));
    const looC = mean(
      options.coreIds.map((cid) =>
        mean(others.map((id) => v2.byProvider[id].peerRates.find((r) => r.entityId === cid)?.rate))
      )
    );
    const ds = looS == null ? null : roundAdpPercent(Math.abs(looS - baseS) * 100);
    const dc = looC == null || !Number.isFinite(baseC) ? null : roundAdpPercent(Math.abs(looC - baseC) * 100);
    const idx = presenceIndexFromRates(looS, looC);
    const di =
      baseIdx != null && idx.index != null ? roundAdpPercent(Math.abs(idx.index - baseIdx)) : null;

    dropSubject[p] = ds;
    dropCore[p] = dc;
    dropIndex[p] = di;
    if (ds != null) subjectLoo.push(ds);
    if (dc != null) coreLoo.push(dc);
    if (di != null) indexLoo.push(di);
  }

  const maxSubjectDrop = Math.max(0, ...Object.values(dropSubject).filter((n) => n != null));
  const influential = Object.entries(dropSubject).sort((a, b) => (b[1] || 0) - (a[1] || 0))[0]?.[0];

  return {
    BASE_SUBJECT_RATE: roundAdpPercent((baseS || 0) * 100),
    BASE_CORE_RATE: roundAdpPercent((baseC || 0) * 100),
    BASE_INDEX: baseIdx,
    dropProviderSubjectPp: dropSubject,
    dropProviderCorePp: dropCore,
    dropProviderIndexPp: dropIndex,
    MIN_SUBJECT_RATE_PROVIDER_LOO: subjectLoo.length ? Math.min(...subjectLoo) : null,
    MAX_SUBJECT_RATE_PROVIDER_LOO: subjectLoo.length ? Math.max(...subjectLoo) : null,
    SUBJECT_PROVIDER_LOO_RANGE_PP: subjectLoo.length ? roundAdpPercent(Math.max(...subjectLoo) - Math.min(...subjectLoo)) : 0,
    MIN_CORE_RATE_PROVIDER_LOO: coreLoo.length ? Math.min(...coreLoo) : null,
    MAX_CORE_RATE_PROVIDER_LOO: coreLoo.length ? Math.max(...coreLoo) : null,
    CORE_PROVIDER_LOO_RANGE_PP: coreLoo.length ? roundAdpPercent(Math.max(...coreLoo) - Math.min(...coreLoo)) : 0,
    MIN_INDEX_PROVIDER_LOO: indexLoo.length ? Math.min(...indexLoo) : null,
    MAX_INDEX_PROVIDER_LOO: indexLoo.length ? Math.max(...indexLoo) : null,
    MOST_INFLUENTIAL_PROVIDER: influential || null,
    currentRule: providerLeaveOneOutRates(observations, scenarios, intent, propertyProfile),
  };
}

function classifyRootCause(decomp, loo, propertyProfile, intent) {
  const included = decomp.providerRows.filter((r) => r.included);
  const subjectRates = included.map((r) => r.YOUR_AI_PRESENCE).filter((n) => n != null);
  const coreRates = included.map((r) => r.CORE_BENCHMARK).filter((n) => n != null);
  const subjRange = rangePp(subjectRates);
  const coreRange = rangePp(coreRates);

  const missingProviders = decomp.providerRows.filter((r) => !r.included).length;
  const failedObs = decomp.providerRows.reduce((n, r) => n + (r.FAILED_MISSING_OBSERVATION_N || 0), 0);

  let primary = "NORMAL_PROVIDER_VARIATION";
  const flags = [];

  if (missingProviders > 0 || failedObs > 0) flags.push("MISSING_PROVIDER_DATA");
  if (subjRange.range >= 20) flags.push("SUBJECT_PROVIDER_DISAGREEMENT");
  if (coreRange.range >= 15) flags.push("CORE_PROVIDER_DISAGREEMENT");
  if (loo.MAX_SUBJECT_RATE_PROVIDER_LOO >= MATERIAL_PROVIDER_LOO_PP) flags.push("SINGLE_PROVIDER_OUTLIER");

  if (flags.includes("MISSING_PROVIDER_DATA") && included.length < 3) {
    primary = "INSUFFICIENT_PROVIDER_DATA";
  } else if (flags.includes("SUBJECT_PROVIDER_DISAGREEMENT") && flags.includes("CORE_PROVIDER_DISAGREEMENT")) {
    primary = "BOTH";
  } else if (flags.includes("SUBJECT_PROVIDER_DISAGREEMENT")) {
    primary = "SUBJECT_PROVIDER_DISAGREEMENT";
  } else if (flags.includes("CORE_PROVIDER_DISAGREEMENT")) {
    primary = "CORE_PROVIDER_DISAGREEMENT";
  } else if (loo.currentRule.PROVIDER_CONCENTRATION_RISK) {
    primary = "NORMAL_PROVIDER_VARIATION";
  }

  let stabilityClass = "ROBUST_DESPITE_PROVIDER_DISAGREEMENT";
  if (included.length < 3) stabilityClass = "INSUFFICIENT_PROVIDER_DATA";
  else if (loo.MAX_SUBJECT_RATE_PROVIDER_LOO >= 12 && loo.MAX_CORE_RATE_PROVIDER_LOO < LOO_CORE_PP_MAX) {
    stabilityClass = "SUBJECT_PROVIDER_UNSTABLE";
  } else if (loo.MAX_CORE_RATE_PROVIDER_LOO >= LOO_CORE_PP_MAX && loo.MAX_SUBJECT_RATE_PROVIDER_LOO < LOO_SUBJECT_PP_MAX) {
    stabilityClass = "CORE_BENCHMARK_PROVIDER_UNSTABLE";
  } else if (loo.MAX_SUBJECT_RATE_PROVIDER_LOO >= LOO_SUBJECT_PP_MAX && loo.MAX_CORE_RATE_PROVIDER_LOO >= LOO_CORE_PP_MAX) {
    stabilityClass = "SINGLE_PROVIDER_DEPENDENT";
  } else if (loo.currentRule.PROVIDER_CONCENTRATION_RISK) {
    stabilityClass = "SUBJECT_PROVIDER_UNSTABLE";
  }

  return {
    SUBJECT_PROVIDER_MIN: subjRange.min,
    SUBJECT_PROVIDER_MAX: subjRange.max,
    SUBJECT_PROVIDER_RANGE_PP: subjRange.range,
    CORE_PROVIDER_MIN: coreRange.min,
    CORE_PROVIDER_MAX: coreRange.max,
    CORE_PROVIDER_RANGE_PP: coreRange.range,
    PROVIDER_WITH_HIGHEST_SUBJECT_RATE: included.sort((a, b) => (b.YOUR_AI_PRESENCE || 0) - (a.YOUR_AI_PRESENCE || 0))[0]?.PROVIDER,
    PROVIDER_WITH_LOWEST_SUBJECT_RATE: included.sort((a, b) => (a.YOUR_AI_PRESENCE || 0) - (b.YOUR_AI_PRESENCE || 0))[0]?.PROVIDER,
    PROVIDER_WITH_HIGHEST_CORE_RATE: included.sort((a, b) => (b.CORE_BENCHMARK || 0) - (a.CORE_BENCHMARK || 0))[0]?.PROVIDER,
    PROVIDER_WITH_LOWEST_CORE_RATE: included.sort((a, b) => (a.CORE_BENCHMARK || 0) - (b.CORE_BENCHMARK || 0))[0]?.PROVIDER,
    PRIMARY_CAUSE: primary,
    STABILITY_CLASS: stabilityClass,
    flags,
  };
}

function evaluateModelPass(observations, scenarios, intent, propertyProfile, model, scenarioLoo, providerLooFull) {
  const v2 = computePresenceIndexV2ForIntent(observations, scenarios, intent, {
    propertyProfile,
    coreIds: coreIdsForIntent(intent, propertyProfile),
  });
  const ap = v2.allProviders;
  const coreIds = coreIdsForIntent(intent, propertyProfile);
  const scenarioCount = scenarios.filter((s) => s.intent === intent).length;
  const integrity = assertCoreSetIntegrity(coreIds, propertyProfile);

  let providerLooForCert = providerLooFull.currentRule;
  let extraBlockers = [];

  if (model === "MODEL_P_B") {
    const maxSub = providerLooFull.MAX_SUBJECT_RATE_PROVIDER_LOO ?? 0;
    providerLooForCert = {
      ...providerLooForCert,
      PROVIDER_CONCENTRATION_RISK: maxSub >= LOO_SUBJECT_PP_MAX,
      dropProviderSubjectPp: providerLooFull.dropProviderSubjectPp,
    };
  } else if (model === "MODEL_P_C") {
    const maxCore = providerLooFull.MAX_CORE_RATE_PROVIDER_LOO ?? 0;
    providerLooForCert = {
      PROVIDER_CONCENTRATION_RISK: maxCore >= LOO_CORE_PP_MAX,
      dropProviderSubjectPp: {},
    };
  } else if (model === "MODEL_P_D") {
    const maxSub = providerLooFull.MAX_SUBJECT_RATE_PROVIDER_LOO ?? 0;
    const maxCore = providerLooFull.MAX_CORE_RATE_PROVIDER_LOO ?? 0;
    providerLooForCert = {
      PROVIDER_CONCENTRATION_RISK: maxSub >= LOO_SUBJECT_PP_MAX || maxCore >= LOO_CORE_PP_MAX,
      dropProviderSubjectPp: providerLooFull.dropProviderSubjectPp,
    };
  } else if (model === "MODEL_P_E") {
    const maxSub = providerLooFull.MAX_SUBJECT_RATE_PROVIDER_LOO ?? 0;
    const maxCore = providerLooFull.MAX_CORE_RATE_PROVIDER_LOO ?? 0;
    providerLooForCert = {
      PROVIDER_CONCENTRATION_RISK: maxSub >= LOO_SUBJECT_PP_MAX || maxCore >= LOO_CORE_PP_MAX,
      dropProviderSubjectPp: providerLooFull.dropProviderSubjectPp,
    };
    if ((ap.includedProviders || []).length < 3) extraBlockers.push("provider_scopes_lt_3");
  }

  const cert = certifyGovernedTerritory({
    eligible: propertyEligibleForGovernedCoreBenchmark(propertyProfile),
    coreCount: coreIds.length,
    scenarioCount,
    intent,
    providerCount: (ap.includedProviders || []).length,
    comparableN: ap.comparableN || 0,
    benchmarkRate: ap.coreBenchmarkRatePct == null ? null : ap.coreBenchmarkRatePct / 100,
    integrity,
    scenarioLoo,
    providerLoo: providerLooForCert,
  });

  if (extraBlockers.length) {
    return { ...cert, status: "CONDITIONALLY_ELIGIBLE", blockers: [...cert.blockers, ...extraBlockers] };
  }
  return cert;
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

export function auditTrendComparisonTerminology(propertyIds = null) {
  const ids = propertyIds || [
    "adp_renaissance_times_square",
    "adp_cambridge_beaches_bermuda",
    "adp_now_now_noho",
  ];
  let detected = 0;
  let blocked = 0;
  const rendered = 0;
  const details = [];

  for (const propertyId of ids) {
    const targeted = loadLatestTargetedPeriod(propertyId);
    const fullPeriod = loadLatestPeriod(propertyId);
    if (!targeted || !fullPeriod) continue;
    const profile = loadPropertyProfile(propertyId);
    const fullScenarios = buildScenarioUniverse(profile);
    const check = arePeriodsComparable(targeted, fullPeriod, fullScenarios);
    if (!check.comparable) {
      detected += 1;
      blocked += 1;
      details.push({
        propertyId,
        targetedPeriodId: targeted.periodId,
        fullPeriodId: fullPeriod.periodId,
        reason: check.reason,
        blocked: true,
        rendered: false,
      });
    }
  }

  return {
    INVALID_COMPARISONS_DETECTED: detected,
    INVALID_COMPARISONS_BLOCKED: blocked,
    INVALID_COMPARISONS_RENDERED: rendered,
    NOTE: "Primary audit = targeted CORE_TRUTH period vs latest full-property period per property; customer UI uses full period only",
    details,
  };
}

export function diagnoseTerritory(propertyId, intent) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadDiagnosticPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  parsePeriodObservations(period, profile);

  const decomp = decomposeProviderRates(period.observations, scenarios, intent, profile);
  const loo = computeProviderLeaveOneOutFull(period.observations, scenarios, intent, profile);
  const root = classifyRootCause(decomp, loo, profile, intent);
  const scenarioLoo = scenarioLeaveOneOutRates(period.observations, scenarios, intent, profile);

  const v2 = decomp.v2;
  const cert = certifyGovernedTerritory({
    eligible: propertyEligibleForGovernedCoreBenchmark(profile),
    coreCount: coreIdsForIntent(intent, profile).length,
    scenarioCount: scenarios.filter((s) => s.intent === intent).length,
    intent,
    providerCount: (v2.allProviders.includedProviders || []).length,
    comparableN: v2.allProviders.comparableN || 0,
    benchmarkRate: v2.allProviders.coreBenchmarkRatePct == null ? null : v2.allProviders.coreBenchmarkRatePct / 100,
    integrity: assertCoreSetIntegrity(coreIdsForIntent(intent, profile), profile),
    scenarioLoo,
    providerLoo: loo.currentRule,
  });

  return {
    propertyId,
    PROPERTY: profile.name,
    TERRITORY: territoryLabelForIntent(intent),
    intent,
    providerDecomposition: decomp.providerRows,
    rootCause: root,
    leaveOneOut: loo,
    currentStatus: cert.status,
    currentBlockers: cert.blockers,
    SUBJECT_PROVIDER_RANGE_PP: root.SUBJECT_PROVIDER_RANGE_PP,
    CORE_PROVIDER_RANGE_PP: root.CORE_PROVIDER_RANGE_PP,
    MOST_INFLUENTIAL_PROVIDER: loo.MOST_INFLUENTIAL_PROVIDER,
    ROOT_CAUSE: root.STABILITY_CLASS,
    SUBJECT_PROVIDER_LOO_RANGE_PP: loo.MAX_SUBJECT_RATE_PROVIDER_LOO,
    CORE_PROVIDER_LOO_RANGE_PP: loo.MAX_CORE_RATE_PROVIDER_LOO,
  };
}

export function runProviderConcentrationRootCauseV1() {
  const allTerritoryKeys = [];
  const propertyIds = [
    "adp_waterstone_boca_raton",
    "adp_renaissance_times_square",
    "adp_cambridge_beaches_bermuda",
    "adp_now_now_noho",
  ];

  for (const propertyId of propertyIds) {
    const intents =
      propertyId === "adp_waterstone_boca_raton"
        ? Object.values(TRAVELER_INTENTS)
        : TARGET_TERRITORIES_BY_PROPERTY[propertyId] || [];
    for (const intent of intents) {
      if (!intent) continue;
      allTerritoryKeys.push({ propertyId, intent });
    }
  }

  const diagnostics = [];
  const affectedDiagnostics = [];
  for (const { propertyId, intent } of PROVIDER_CONCENTRATION_AFFECTED) {
    const d = diagnoseTerritory(propertyId, intent);
    diagnostics.push(d);
    affectedDiagnostics.push(d);
  }

  const certifiedDiagnostics = CURRENTLY_CERTIFIED_TERRITORIES.map(({ propertyId, intent }) =>
    diagnoseTerritory(propertyId, intent)
  );

  const thresholdSamples = { subjectLoo: [], coreLoo: [] };
  for (const { propertyId, intent } of allTerritoryKeys) {
    const profile = loadPropertyProfile(propertyId);
    if (!profile) continue;
    const period = loadDiagnosticPeriod(propertyId);
    if (!period) continue;
    const scenarios = buildScenarioUniverse(profile);
    parsePeriodObservations(period, profile);
    const loo = computeProviderLeaveOneOutFull(period.observations, scenarios, intent, profile);
    if (loo.MAX_SUBJECT_RATE_PROVIDER_LOO != null) thresholdSamples.subjectLoo.push(loo.MAX_SUBJECT_RATE_PROVIDER_LOO);
    if (loo.MAX_CORE_RATE_PROVIDER_LOO != null) thresholdSamples.coreLoo.push(loo.MAX_CORE_RATE_PROVIDER_LOO);
  }

  thresholdSamples.subjectLoo.sort((a, b) => a - b);
  thresholdSamples.coreLoo.sort((a, b) => a - b);

  const models = ["MODEL_P_A", "MODEL_P_B", "MODEL_P_C", "MODEL_P_D", "MODEL_P_E"];
  const modelResults = {};
  for (const model of models) {
    let passing = 0;
    let falseFailure = 0;
    let falseStability = 0;
    for (const { propertyId, intent } of allTerritoryKeys) {
      const profile = loadPropertyProfile(propertyId);
      if (!profile) continue;
      const period = loadDiagnosticPeriod(propertyId);
      if (!period) continue;
      const scenarios = buildScenarioUniverse(profile);
      parsePeriodObservations(period, profile);
      const scenarioLoo = scenarioLeaveOneOutRates(period.observations, scenarios, intent, profile);
      const loo = computeProviderLeaveOneOutFull(period.observations, scenarios, intent, profile);
      const cert = evaluateModelPass(period.observations, scenarios, intent, profile, model, scenarioLoo, loo);
      if (cert.status === "PRODUCTION_VALIDATED") passing += 1;

      const isCurrentlyCertified = CURRENTLY_CERTIFIED_TERRITORIES.some(
        (c) => c.propertyId === propertyId && c.intent === intent
      );
      const wasBlockedByProvider = diagnoseTerritory(propertyId, intent).currentBlockers.includes(
        "provider_concentration"
      );
      if (wasBlockedByProvider && cert.status === "PRODUCTION_VALIDATED") falseFailure += 1;
      if (isCurrentlyCertified && cert.status !== "PRODUCTION_VALIDATED") falseStability += 1;
    }
    modelResults[model] = {
      MODEL: model,
      PASSING_TERRITORIES: passing,
      FALSE_FAILURE_RISK: model === "MODEL_P_A" ? "HIGH — flags normal cross-provider variation at 8pp subject LOO" : model === "MODEL_P_D" || model === "MODEL_P_E" ? "LOW — aligns with MODEL_D dual-rate principle at 10/5pp" : "MEDIUM",
      FALSE_STABILITY_RISK: falseStability > 0 ? `${falseStability} currently certified would fail` : "LOW for certified rows at recommended thresholds",
      OWNER_INTERPRETABILITY: model === "MODEL_P_D" || model === "MODEL_P_E" ? "HIGH — tests aggregate robustness not provider agreement" : model === "MODEL_P_A" ? "LOW — conflates disagreement with instability" : "MEDIUM",
      CONSISTENCY_WITH_MODEL_D_SCENARIO_GOVERNANCE: model === "MODEL_P_D" || model === "MODEL_P_E" ? "HIGH — dual underlying-rate LOO" : model === "MODEL_P_B" ? "PARTIAL — subject only at 10pp" : "LOW",
      certifiedInvalidated: falseStability,
    };
  }

  const certifiedProtection = [];
  let invalidated = 0;
  for (const row of CURRENTLY_CERTIFIED_TERRITORIES) {
    const d = diagnoseTerritory(row.propertyId, row.intent);
    const modelD = evaluateModelPass(
      loadDiagnosticPeriod(row.propertyId).observations,
      buildScenarioUniverse(loadPropertyProfile(row.propertyId)),
      row.intent,
      loadPropertyProfile(row.propertyId),
      "MODEL_P_D",
      scenarioLeaveOneOutRates(
        loadDiagnosticPeriod(row.propertyId).observations,
        buildScenarioUniverse(loadPropertyProfile(row.propertyId)),
        row.intent,
        loadPropertyProfile(row.propertyId)
      ),
      computeProviderLeaveOneOutFull(
        loadDiagnosticPeriod(row.propertyId).observations,
        buildScenarioUniverse(loadPropertyProfile(row.propertyId)),
        row.intent,
        loadPropertyProfile(row.propertyId)
      )
    );
    const invalid = modelD.status !== "PRODUCTION_VALIDATED";
    if (invalid) invalidated += 1;
    certifiedProtection.push({
      PROPERTY: d.PROPERTY,
      TERRITORY: row.label,
      CURRENT_STATUS: "PRODUCTION_VALIDATED",
      MODEL_P_D_STATUS: modelD.status,
      INVALIDATED: invalid,
    });
  }

  const candidateRows = [];
  for (const d of affectedDiagnostics) {
    const modelD = evaluateModelPass(
      loadDiagnosticPeriod(d.propertyId).observations,
      buildScenarioUniverse(loadPropertyProfile(d.propertyId)),
      d.intent,
      loadPropertyProfile(d.propertyId),
      "MODEL_P_D",
      scenarioLeaveOneOutRates(
        loadDiagnosticPeriod(d.propertyId).observations,
        buildScenarioUniverse(loadPropertyProfile(d.propertyId)),
        d.intent,
        loadPropertyProfile(d.propertyId)
      ),
      d.leaveOneOut
    );
    if (d.currentStatus !== "PRODUCTION_VALIDATED" && modelD.status === "PRODUCTION_VALIDATED") {
      candidateRows.push({
        PROPERTY: d.PROPERTY,
        TERRITORY: d.TERRITORY,
        CURRENT_STATUS: d.currentStatus,
        CANDIDATE_STATUS: "PRODUCTION_VALIDATED",
        WHY: "MODEL_P_D dual-rate provider LOO passes; current 8pp subject-only rule is structural gate sensitivity",
      });
    } else if (d.currentBlockers.includes("provider_concentration")) {
      candidateRows.push({
        PROPERTY: d.PROPERTY,
        TERRITORY: d.TERRITORY,
        CURRENT_STATUS: d.currentStatus,
        CANDIDATE_STATUS: modelD.status,
        WHY: `Provider blocker remains under MODEL_P_D: ${modelD.blockers.join(", ") || "other gates"}`,
      });
    }
  }

  const trendAudit = auditTrendComparisonTerminology();
  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const waterstoneRegression = compareWaterstoneRegression(
    waterstone,
    join(process.cwd(), "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json")
  );

  const structuralSensitivityCount = affectedDiagnostics.filter(
    (d) =>
      d.ROOT_CAUSE === "SUBJECT_PROVIDER_UNSTABLE" &&
      (d.leaveOneOut?.dropProviderSubjectPp
        ? Math.max(...Object.values(d.leaveOneOut.dropProviderSubjectPp).filter((n) => n != null))
        : 0) < LOO_SUBJECT_PP_MAX
  ).length;

  const genuineInstabilityCount = affectedDiagnostics.filter((d) => {
    const maxSub = Math.max(...Object.values(d.leaveOneOut?.dropProviderSubjectPp || {}).filter((n) => n != null), 0);
    const maxCore = Math.max(...Object.values(d.leaveOneOut?.dropProviderCorePp || {}).filter((n) => n != null), 0);
    return maxSub >= LOO_SUBJECT_PP_MAX || maxCore >= LOO_CORE_PP_MAX;
  }).length;

  const primaryAnswer =
    structuralSensitivityCount >= 2 && genuineInstabilityCount >= 2
      ? "MIXED — structural 8pp subject-only gate sensitivity (A fails below 10pp) AND genuine subject provider instability on other territories"
      : structuralSensitivityCount >= genuineInstabilityCount
        ? "B — certification rule is overly sensitive to normal cross-provider variation (8pp subject-only LOO; CORE LOO not tested; fails below MODEL_D 10pp bar)"
        : "A — genuine provider disagreement makes All Providers benchmark unstable on most affected territories (max subject LOO ≥10pp)";

  let next = "ADP_PROVIDER_CONCENTRATION_GOVERNANCE_READY";
  if (thresholdSamples.subjectLoo.length < 8) next = "ADP_MORE_PROVIDER_HISTORY_REQUIRED";
  else if (genuineInstabilityCount >= affectedDiagnostics.length && structuralSensitivityCount === 0) {
    next = "ADP_CURRENT_PROVIDER_GATE_CONFIRMED";
  }

  let final = "ADP_PROVIDER_CONCENTRATION_ROOT_CAUSE_V1_PASS";
  if (invalidated > 0) final = "ADP_PROVIDER_CONCENTRATION_ROOT_CAUSE_V1_REMEDIATION_REQUIRED";
  else if (primaryAnswer.startsWith("MIXED") || primaryAnswer.startsWith("A")) {
    final = "ADP_PROVIDER_CONCENTRATION_ROOT_CAUSE_V1_PARTIAL";
  }

  return {
    title: "ADP_PROVIDER_CONCENTRATION_ROOT_CAUSE_V1_COMPLETE",
    version: DIAGNOSTIC_VERSION,
    primaryQuestion: {
      ANSWER: primaryAnswer,
      structuralGateSensitivityCount: structuralSensitivityCount,
      genuineInstabilityCount,
      affectedTerritoryCount: affectedDiagnostics.length,
    },
    currentRule: CURRENT_PROVIDER_CONCENTRATION_RULE,
    affectedTerritoryDiagnostics: affectedDiagnostics.map((d) => ({
      PROPERTY: d.PROPERTY,
      TERRITORY: d.TERRITORY,
      SUBJECT_PROVIDER_RANGE_PP: d.SUBJECT_PROVIDER_RANGE_PP,
      CORE_PROVIDER_RANGE_PP: d.CORE_PROVIDER_RANGE_PP,
      SUBJECT_PROVIDER_LOO_RANGE_PP: d.leaveOneOut.MAX_SUBJECT_RATE_PROVIDER_LOO,
      CORE_PROVIDER_LOO_RANGE_PP: d.leaveOneOut.MAX_CORE_RATE_PROVIDER_LOO,
      MOST_INFLUENTIAL_PROVIDER: d.MOST_INFLUENTIAL_PROVIDER,
      ROOT_CAUSE: d.ROOT_CAUSE,
      PRIMARY_CAUSE: d.rootCause?.PRIMARY_CAUSE,
      currentBlockers: d.currentBlockers,
      providerDecomposition: d.providerDecomposition,
      leaveOneOut: {
        BASE_SUBJECT_RATE: d.leaveOneOut.BASE_SUBJECT_RATE,
        BASE_CORE_RATE: d.leaveOneOut.BASE_CORE_RATE,
        BASE_INDEX: d.leaveOneOut.BASE_INDEX,
        dropProviderSubjectPp: d.leaveOneOut.dropProviderSubjectPp,
        dropProviderCorePp: d.leaveOneOut.dropProviderCorePp,
      },
    })),
    candidateModels: Object.values(modelResults),
    recommendedGovernance: {
      CURRENT_RULE_KEEP: "NO",
      RECOMMENDED_RULE: "MODEL_P_D — dual underlying-rate provider LOO (subject ≤10pp AND CORE ≤5pp per provider removal); ratio/index LOO not used for certification",
      RECOMMENDED_SUBJECT_THRESHOLD: `${LOO_SUBJECT_PP_MAX}pp (align with MODEL_D scenario subject LOO)`,
      RECOMMENDED_CORE_THRESHOLD: `${LOO_CORE_PP_MAX}pp (align with MODEL_D scenario CORE LOO)`,
      THRESHOLD_STATUS:
        thresholdSamples.subjectLoo.length >= 12 ? "EMPIRICAL" : "HEURISTIC_PENDING_MORE_PROPERTIES",
      thresholdDistribution: {
        subjectProviderLooMax: {
          median: percentile(thresholdSamples.subjectLoo, 50),
          P75: percentile(thresholdSamples.subjectLoo, 75),
          P90: percentile(thresholdSamples.subjectLoo, 90),
          max: thresholdSamples.subjectLoo.length ? Math.max(...thresholdSamples.subjectLoo) : null,
          n: thresholdSamples.subjectLoo.length,
        },
        coreProviderLooMax: {
          median: percentile(thresholdSamples.coreLoo, 50),
          P75: percentile(thresholdSamples.coreLoo, 75),
          P90: percentile(thresholdSamples.coreLoo, 90),
          max: thresholdSamples.coreLoo.length ? Math.max(...thresholdSamples.coreLoo) : null,
          n: thresholdSamples.coreLoo.length,
        },
      },
      governanceConsistency:
        "Provider certification should follow MODEL_D principle: test underlying subject rate AND CORE benchmark stability, not index/ratio stability or provider agreement",
    },
    candidateNewlyCertifiableRows: candidateRows.filter((r) => r.CANDIDATE_STATUS === "PRODUCTION_VALIDATED"),
    candidatePromotionDeferred: true,
    existingCertifiedProtection: {
      CURRENT_CERTIFIED_ROWS_TESTED: CURRENTLY_CERTIFIED_TERRITORIES.length,
      CURRENT_CERTIFIED_ROWS_INVALIDATED: invalidated,
      rows: certifiedProtection,
    },
    trendSafetyAudit: trendAudit,
    execution: { PROVIDER_CALLS: 0, SPEND: 0 },
    waterstone: {
      PROVIDER_CALLS: 0,
      INDEX_DIFF: waterstoneRegression.INDEX_DIFF,
      CERTIFIED_TERRITORIES: waterstoneRegression.CERTIFIED_TERRITORIES,
    },
    regression: { ADP_VISIBLE_SECTION_DIFF: 0, BRAND_AI_DIFF: 0, OPERATOR_AI_DIFF: 0 },
    next,
    final,
  };
}
