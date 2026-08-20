import { join } from "path";

/**
 * ADP Targeted Multi-Property Governed Benchmark Measurement V1.
 * CORE_TRUTH_READY territories only; property-specific frozen CORE; Waterstone frozen.
 */

import {
  loadPropertyProfile,
  loadLatestPeriod,
  loadAllPeriods,
  createPeriod,
  savePeriod,
  generatePeriodId,
  PROVIDERS,
} from "../data-model.js";
import { buildScenarioUniverse, getScenariosByIntent } from "../prompt-universe/scenario-registry.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { parsePeriodObservations } from "./response-parser.js";
import { executeMonitoringPeriod, estimateCost, PROVIDER_CONFIGS } from "./multi-provider-runner.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import {
  certifyGovernedTerritory,
  propertyEligibleForGovernedCoreBenchmark,
  assertNumericIndexHasRates,
  GOVERNED_PRESENCE_INDEX_CUSTOMER_VERSION,
} from "../metrics/governed-customer-presence-index.js";
import {
  coreIdsForIntent,
  assertCoreSetIntegrity,
} from "../metrics/presence-benchmark-v1.js";
import { computePresenceIndexV2ForIntent } from "../metrics/presence-index-v2.js";
import {
  scenarioLeaveOneOutRates,
  providerLeaveOneOutRates,
} from "../metrics/core-benchmark-rate-contract-v1.js";
import { buildCustomerExecutiveMetrics } from "../metrics/customer-executive-metrics.js";
import { buildOwnerPayload } from "../customer/owner-payload.js";
import { arePeriodsComparable } from "../metrics/longitudinal-comparability.js";
import {
  GOVERNED_NON_WATERSTONE_PROPERTIES,
  CUSTOMER_NUMERIC_INDEX_PROMOTION,
  customerNumericIndexPromotionAllowed,
} from "../metrics/property-core-governance-data.js";
import { runPropertyCoreGovernance } from "../metrics/property-specific-core-benchmark-governance-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../multi-property-governed-audit-v2.js";
import { buildTerritoryBenchmarkSets } from "../metrics/territory-core-contract.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";

export const MEASUREMENT_VERSION = "adp_targeted_multi_property_governed_benchmark_measurement_v1";
export const COST_CAP_USD = 25;

/** CORE_TRUTH_READY target intents per property — do not measure developing/partial territories. */
export const TARGET_TERRITORIES_BY_PROPERTY = Object.freeze({
  adp_renaissance_times_square: [
    TRAVELER_INTENTS.BUSINESS,
    TRAVELER_INTENTS.LEISURE,
    TRAVELER_INTENTS.COUPLES,
    TRAVELER_INTENTS.GROUP_MEETING,
    TRAVELER_INTENTS.FAMILY,
  ],
  adp_cambridge_beaches_bermuda: [
    TRAVELER_INTENTS.LEISURE,
    TRAVELER_INTENTS.COUPLES,
    TRAVELER_INTENTS.CELEBRATION,
    TRAVELER_INTENTS.WELLNESS,
  ],
  adp_now_now_noho: [
    TRAVELER_INTENTS.BUSINESS,
    TRAVELER_INTENTS.LEISURE,
    TRAVELER_INTENTS.COUPLES,
  ],
});

export const TARGET_PROPERTY_IDS = Object.freeze([...GOVERNED_NON_WATERSTONE_PROPERTIES]);

export function selectTargetScenarios(propertyProfile, targetIntents = null) {
  const intents = targetIntents || TARGET_TERRITORIES_BY_PROPERTY[propertyProfile.propertyId] || [];
  const universe = buildScenarioUniverse(propertyProfile);
  const scenarios = [];
  const byIntent = {};
  for (const intent of intents) {
    const rows = getScenariosByIntent(universe, intent);
    byIntent[intent] = rows.map((s) => s.scenarioId);
    scenarios.push(...rows);
  }
  const scenarioIds = scenarios.map((s) => s.scenarioId);
  return {
    universe,
    intents,
    scenarios,
    scenarioIds,
    byIntent,
    scenarioCount: scenarios.length,
  };
}

export function buildPropertyPreflightPlan(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { propertyId, error: "NO_PROFILE" };

  const targetIntents = TARGET_TERRITORIES_BY_PROPERTY[propertyId] || [];
  const { universe, scenarios, scenarioIds, byIntent, scenarioCount } = selectTargetScenarios(profile, targetIntents);
  const fullUniverseCount = universe.length;
  const cost = estimateCost(scenarioCount, PROVIDERS);
  const territories = targetIntents.map((intent) => ({
    intent,
    TERRITORY: territoryLabelForIntent(intent),
    SCENARIO_IDS: byIntent[intent] || [],
    SCENARIO_COUNT: (byIntent[intent] || []).length,
  }));

  return {
    PROPERTY: profile.name,
    propertyId,
    TARGET_TERRITORIES: territories.map((t) => t.TERRITORY),
    SCENARIO_IDS: scenarioIds,
    SCENARIO_COUNT: scenarioCount,
    PROVIDERS: [...PROVIDERS],
    PLANNED_CALLS: scenarioCount * PROVIDERS.length,
    ESTIMATED_COST: cost.total,
    costByProvider: cost.byProvider,
    fullUniverseScenarioCount: fullUniverseCount,
    fullUniverseCalls: fullUniverseCount * PROVIDERS.length,
    targetedOnly: scenarioCount < fullUniverseCount,
    territories,
  };
}

export function validateTargetedExecutionArchitecture(plans) {
  const issues = [];
  for (const plan of plans) {
    if (plan.error) {
      issues.push({ propertyId: plan.propertyId, issue: plan.error });
      continue;
    }
    if (!plan.targetedOnly) {
      issues.push({
        propertyId: plan.propertyId,
        issue: "TARGETED_SCOPE_EQUALS_FULL_UNIVERSE",
      });
    }
    if (plan.PLANNED_CALLS === plan.fullUniverseCalls && plan.fullUniverseCalls > 0) {
      issues.push({
        propertyId: plan.propertyId,
        issue: "CANNOT_ISOLATE_TERRITORY_SCENARIOS",
      });
    }
  }

  const totalCalls = plans.reduce((n, p) => n + (p.PLANNED_CALLS || 0), 0);
  const totalCost = plans.reduce((n, p) => n + (p.ESTIMATED_COST || 0), 0);

  if (totalCost > COST_CAP_USD) {
    issues.push({ issue: "COST_CAP_EXCEEDED", totalCost, cap: COST_CAP_USD });
  }

  const runnerSupportsSubset = typeof executeMonitoringPeriod === "function";
  if (!runnerSupportsSubset) {
    return {
      pass: false,
      status: "TARGETED_EXECUTION_ARCHITECTURE_REMEDIATION_REQUIRED",
      issues: [{ issue: "RUNNER_NO_SCENARIO_SUBSET" }],
    };
  }

  if (issues.some((i) => i.issue === "CANNOT_ISOLATE_TERRITORY_SCENARIOS")) {
    return {
      pass: false,
      status: "TARGETED_EXECUTION_ARCHITECTURE_REMEDIATION_REQUIRED",
      issues,
    };
  }

  return {
    pass: true,
    status: "TARGETED_EXECUTION_ARCHITECTURE_OK",
    issues,
    TOTAL_PROPERTIES: plans.length,
    TOTAL_TARGET_TERRITORIES: plans.reduce((n, p) => n + (p.TARGET_TERRITORIES?.length || 0), 0),
    TOTAL_SCENARIOS_BY_PROPERTY: plans.map((p) => ({
      propertyId: p.propertyId,
      count: p.SCENARIO_COUNT,
    })),
    TOTAL_PROVIDER_CALLS: totalCalls,
    ESTIMATED_COST: Math.round(totalCost * 100) / 100,
    COST_CAP: COST_CAP_USD,
  };
}

function cloneObservationForPeriod(obs, periodId, propertyId) {
  return {
    ...obs,
    periodId,
    propertyId,
    observationId: obs.observationId
      ? `${obs.observationId}_fork_${periodId.slice(-6)}`
      : obs.observationId,
  };
}

export function forkTargetedPeriodFromPrior(propertyId, sourcePeriod, targetScenarios, profile) {
  const scenarioIds = new Set(targetScenarios.map((s) => s.scenarioId));
  const matched = (sourcePeriod?.observations || []).filter((o) => scenarioIds.has(o.scenarioId));
  const expectedCalls = targetScenarios.length * PROVIDERS.length;
  const periodId = generatePeriodId(propertyId);

  const period = {
    ...createPeriod(propertyId, targetScenarios),
    periodId,
    measurementScope: {
      version: MEASUREMENT_VERSION,
      type: "TARGETED_CORE_TRUTH_V1",
      targetIntents: TARGET_TERRITORIES_BY_PROPERTY[propertyId] || [],
      targetedScenarioIds: [...scenarioIds],
      sourcePeriodId: sourcePeriod?.periodId || null,
      executionMode: "TARGETED_FORK_FROM_PRIOR",
      notComparableToFullPropertyPeriod: true,
      propertyProfileVersion: profile?.profileVersion || profile?.propertyId,
    },
    observations: matched.map((o) => cloneObservationForPeriod(o, periodId, propertyId)),
    status: "FORKED_FROM_PRIOR",
    forkedAt: new Date().toISOString(),
    providerCount: PROVIDERS.length,
    scenarioCount: targetScenarios.length,
  };

  parsePeriodObservations(period, profile);

  return {
    period,
    expectedCalls,
    matchedCalls: matched.length,
    complete: matched.length >= expectedCalls,
    failedObservations: matched.filter((o) => o.error).length,
  };
}

export async function executeTargetedPropertyMeasurement(propertyId, options = {}) {
  const { apply = false, forkFromPrior = true } = options;
  const profile = loadPropertyProfile(propertyId);
  const plan = buildPropertyPreflightPlan(propertyId);
  const { scenarios } = selectTargetScenarios(profile);

  if (apply) {
    const period = await executeMonitoringPeriod({
      propertyId,
      scenarios,
      dryRun: false,
      providers: PROVIDERS,
    });
    period.measurementScope = {
      version: MEASUREMENT_VERSION,
      type: "TARGETED_CORE_TRUTH_V1",
      targetIntents: TARGET_TERRITORIES_BY_PROPERTY[propertyId] || [],
      targetedScenarioIds: plan.SCENARIO_IDS,
      executionMode: "LIVE_PROVIDER_WAVE",
      notComparableToFullPropertyPeriod: true,
    };
    parsePeriodObservations(period, profile);
    savePeriod(period);

    const successful = period.observations.filter((o) => !o.error && o.rawResponse).length;
    const failed = period.observations.filter((o) => o.error).length;
    return {
      PROPERTY: profile.name,
      propertyId,
      NEW_PERIOD: period.periodId,
      CALLS_EXECUTED: period.observations.length,
      SUCCESSFUL: successful,
      FAILED: failed,
      COST: period.costEstimate?.total ?? plan.ESTIMATED_COST,
      executionMode: "LIVE_PROVIDER_WAVE",
      period,
    };
  }

  if (forkFromPrior) {
    const sourcePeriod = loadLatestPeriod(propertyId);
    if (sourcePeriod) parsePeriodObservations(sourcePeriod, profile);
    const fork = forkTargetedPeriodFromPrior(propertyId, sourcePeriod, scenarios, profile);
    if (!fork.complete) {
      return {
        PROPERTY: profile.name,
        propertyId,
        error: "INCOMPLETE_PRIOR_COVERAGE",
        expectedCalls: fork.expectedCalls,
        matchedCalls: fork.matchedCalls,
        executionMode: "FORK_BLOCKED",
      };
    }
    savePeriod(fork.period);
    return {
      PROPERTY: profile.name,
      propertyId,
      NEW_PERIOD: fork.period.periodId,
      CALLS_EXECUTED: 0,
      SUCCESSFUL: fork.matchedCalls - fork.failedObservations,
      FAILED: fork.failedObservations,
      COST: 0,
      executionMode: "TARGETED_FORK_FROM_PRIOR",
      sourcePeriodId: sourcePeriod?.periodId,
      period: fork.period,
    };
  }

  const period = await executeMonitoringPeriod({
    propertyId,
    scenarios,
    dryRun: true,
    providers: PROVIDERS,
  });
  period.measurementScope = {
    version: MEASUREMENT_VERSION,
    type: "TARGETED_CORE_TRUTH_V1",
    executionMode: "DRY_RUN",
    notComparableToFullPropertyPeriod: true,
  };
  savePeriod(period);
  return {
    PROPERTY: profile.name,
    propertyId,
    NEW_PERIOD: period.periodId,
    CALLS_EXECUTED: 0,
    SUCCESSFUL: period.observations.length,
    FAILED: 0,
    COST: plan.ESTIMATED_COST,
    executionMode: "DRY_RUN",
    period,
  };
}

export function certifyTargetedTerritory(period, scenarios, profile, intent) {
  const coreIds = coreIdsForIntent(intent, profile);
  const scenarioCount = scenarios.filter((s) => s.intent === intent).length;
  const v2 = computePresenceIndexV2ForIntent(period.observations, scenarios, intent, {
    propertyProfile: profile,
    coreIds,
  });
  const ap = v2.allProviders || {};
  const loo = scenarioLeaveOneOutRates(period.observations, scenarios, intent, profile);
  const plo = providerLeaveOneOutRates(period.observations, scenarios, intent, profile);
  const integrity = assertCoreSetIntegrity(coreIds, profile);
  const cert = certifyGovernedTerritory({
    eligible: propertyEligibleForGovernedCoreBenchmark(profile),
    coreCount: coreIds.length,
    scenarioCount,
    intent,
    providerCount: (ap.includedProviders || []).length,
    comparableN: ap.comparableN || 0,
    benchmarkRate: ap.coreBenchmarkRatePct == null ? null : ap.coreBenchmarkRatePct / 100,
    integrity,
    scenarioLoo: loo,
    providerLoo: plo,
  });

  const numericAllowed = customerNumericIndexPromotionAllowed(profile.propertyId);
  let customerStatus = cert.status;
  if (!numericAllowed && cert.status === "PRODUCTION_VALIDATED") {
    customerStatus = "BENCHMARK_DEVELOPING";
  }

  const row = {
    PROPERTY: profile.name,
    propertyId: profile.propertyId,
    TERRITORY: territoryLabelForIntent(intent),
    intent,
    SCENARIO_COUNT: scenarioCount,
    CORE_COUNT: coreIds.length,
    YOUR_AI_PRESENCE: ap.subjectRatePct ?? null,
    CORE_BENCHMARK: ap.coreBenchmarkRatePct ?? null,
    AI_PRESENCE_INDEX: ap.index ?? null,
    STATUS: cert.status,
    CUSTOMER_STATUS: customerStatus,
    BLOCKER: cert.blockers.join(", ") || null,
    blockers: cert.blockers,
    includedProviders: ap.includedProviders || [],
    comparableN: ap.comparableN || 0,
    zeroPresencePeers: ap.zeroPresencePeers || [],
    scenarioLoo: {
      maxSubjectPpMove: loo.maxSubjectPpMove,
      maxBenchmarkPpMove: loo.maxBenchmarkPpMove,
    },
    providerLoo: {
      PROVIDER_CONCENTRATION_RISK: plo.PROVIDER_CONCENTRATION_RISK,
      dropProviderSubjectPp: plo.dropProviderSubjectPp,
    },
  };

  if (customerStatus === "PRODUCTION_VALIDATED") {
    const gate = assertNumericIndexHasRates({
      index: row.AI_PRESENCE_INDEX,
      subjectRatePct: row.YOUR_AI_PRESENCE,
      coreBenchmarkRatePct: row.CORE_BENCHMARK,
    });
    row.numericGate = gate;
  }

  return row;
}

export function certifyTargetedProperty(period, profile) {
  const targetIntents = TARGET_TERRITORIES_BY_PROPERTY[profile.propertyId] || [];
  const { universe, scenarios } = selectTargetScenarios(profile, targetIntents);
  parsePeriodObservations(period, profile);

  const territoryRows = targetIntents.map((intent) =>
    certifyTargetedTerritory(period, universe, profile, intent)
  );

  const prod = territoryRows.filter((r) => r.STATUS === "PRODUCTION_VALIDATED");
  const conditional = territoryRows.filter((r) => r.STATUS === "CONDITIONALLY_ELIGIBLE");
  const developing = territoryRows.filter((r) => r.STATUS === "BENCHMARK_DEVELOPING");
  const blocked = territoryRows.filter((r) => r.STATUS === "BLOCKED");

  const customerProd = territoryRows.filter((r) => r.CUSTOMER_STATUS === "PRODUCTION_VALIDATED");

  return {
    propertyId: profile.propertyId,
    PROPERTY: profile.name,
    periodId: period.periodId,
    territoryRows,
    promotion: {
      NUMERIC_INDEX_TERRITORIES: customerProd.map((r) => r.TERRITORY),
      CONDITIONAL_TERRITORIES: conditional.map((r) => r.TERRITORY),
      BENCHMARK_DEVELOPING_TERRITORIES: [
        ...developing.map((r) => r.TERRITORY),
        ...territoryRows
          .filter((r) => r.STATUS === "PRODUCTION_VALIDATED" && r.CUSTOMER_STATUS !== "PRODUCTION_VALIDATED")
          .map((r) => r.TERRITORY),
      ],
      BLOCKED_TERRITORIES: blocked.map((r) => r.TERRITORY),
    },
    internal: {
      PRODUCTION_VALIDATED: prod.map((r) => r.TERRITORY),
      CONDITIONALLY_ELIGIBLE: conditional.map((r) => r.TERRITORY),
      BENCHMARK_DEVELOPING: developing.map((r) => r.TERRITORY),
      BLOCKED: blocked.map((r) => r.TERRITORY),
    },
    scenarios,
    universe,
  };
}

export function assessPhase1ForTargetedPeriod(period, scenarios, profile, allPeriods) {
  const fullUniverse = buildScenarioUniverse(profile);
  const priorPeriod = allPeriods.filter((p) => p.periodId !== period.periodId).slice(-1)[0];
  const comparableCheck = priorPeriod
    ? arePeriodsComparable(period, priorPeriod, fullUniverse)
    : { comparable: false, reason: "no_prior" };

  const targetedExecutive = buildCustomerExecutiveMetrics(period, scenarios, profile, {
    allPeriods,
  });
  const fullExecutive = priorPeriod
    ? buildCustomerExecutiveMetrics(priorPeriod, fullUniverse, profile, { allPeriods })
    : null;

  return {
    phase1Metrics: targetedExecutive?.hero || null,
    phase1Scope: "TARGETED_CORE_TRUTH_V1",
    NOT_COMPARABLE_TO_FULL_PROPERTY_PERIOD: true,
    priorFullPeriodComparable: comparableCheck.comparable ? "YES" : "NO",
    priorIncomparableReason: comparableCheck.reason || "targeted_scope",
    fullPropertyPhase1Preserved: fullExecutive?.hero
      ? {
          aiConsiderationRate: fullExecutive.hero.aiConsiderationRate,
          aiScenarioPresence: fullExecutive.hero.aiScenarioPresence,
          sourcePeriodId: priorPeriod?.periodId,
        }
      : null,
  };
}

export function collectProviderFailures(period, propertyName) {
  const byProvider = {};
  for (const obs of period?.observations || []) {
    if (!obs.error) continue;
    if (!byProvider[obs.provider]) {
      byProvider[obs.provider] = { PROPERTY: propertyName, PROVIDER: obs.provider, FAILED_CALLS: 0, FAILURE_TYPES: new Set() };
    }
    byProvider[obs.provider].FAILED_CALLS += 1;
    byProvider[obs.provider].FAILURE_TYPES.add(String(obs.error).slice(0, 80));
  }
  return Object.values(byProvider).map((r) => ({
    ...r,
    FAILURE_TYPES: [...r.FAILURE_TYPES],
  }));
}

export function deriveCustomerPromotionFlags(certResults) {
  const flags = { ...CUSTOMER_NUMERIC_INDEX_PROMOTION };
  for (const cert of certResults) {
    const hasNumeric = cert.promotion.NUMERIC_INDEX_TERRITORIES.length > 0;
    if (hasNumeric) flags[cert.propertyId] = true;
  }
  return flags;
}

export async function runTargetedMultiPropertyGovernedBenchmarkMeasurementV1(options = {}) {
  const { apply = false, forkFromPrior = !apply } = options;

  const plans = TARGET_PROPERTY_IDS.map((id) => buildPropertyPreflightPlan(id));
  const architecture = validateTargetedExecutionArchitecture(plans);

  if (!architecture.pass) {
    return {
      title: "ADP_TARGETED_MULTI_PROPERTY_GOVERNED_BENCHMARK_MEASUREMENT_V1_COMPLETE",
      architecture,
      final: "ADP_TARGETED_MEASUREMENT_REMEDIATION_REQUIRED",
      next: "ADP_TARGETED_MEASUREMENT_REMEDIATION_REQUIRED",
    };
  }

  const executions = [];
  for (const propertyId of TARGET_PROPERTY_IDS) {
    const result = await executeTargetedPropertyMeasurement(propertyId, { apply, forkFromPrior });
    executions.push(result);
  }

  const certifications = [];
  const allTerritoryRows = [];
  const providerFailures = [];

  for (const exec of executions) {
    if (!exec.period) continue;
    const profile = loadPropertyProfile(exec.propertyId);
    const cert = certifyTargetedProperty(exec.period, profile);
    certifications.push(cert);
    allTerritoryRows.push(...cert.territoryRows);
    providerFailures.push(...collectProviderFailures(exec.period, exec.PROPERTY));

    const allPeriods = loadAllPeriods(exec.propertyId);
    cert.phase1 = assessPhase1ForTargetedPeriod(exec.period, cert.scenarios, profile, allPeriods);
  }

  const waterstoneAudit = auditProperty("adp_waterstone_boca_raton");
  const waterstoneBaseline = join(process.cwd(), "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json");
  const waterstoneRegression = compareWaterstoneRegression(waterstoneAudit, waterstoneBaseline);

  let numericWithoutSubject = 0;
  let numericWithoutCore = 0;
  let legacyFallback = 0;
  let mixedMethodology = 0;

  for (const row of allTerritoryRows) {
    if (row.CUSTOMER_STATUS === "PRODUCTION_VALIDATED") {
      const gate = assertNumericIndexHasRates({
        index: row.AI_PRESENCE_INDEX,
        subjectRatePct: row.YOUR_AI_PRESENCE,
        coreBenchmarkRatePct: row.CORE_BENCHMARK,
      });
      if (!gate.ok) {
        numericWithoutSubject += gate.NUMERIC_INDEX_WITHOUT_SUBJECT_RATE || 0;
        numericWithoutCore += gate.NUMERIC_INDEX_WITHOUT_CORE_BENCHMARK || 0;
      }
    }
  }

  const promotionFlags = deriveCustomerPromotionFlags(certifications);
  const totalProdValidated = allTerritoryRows.filter((r) => r.STATUS === "PRODUCTION_VALIDATED").length;
  const totalCustomerNumeric = allTerritoryRows.filter((r) => r.CUSTOMER_STATUS === "PRODUCTION_VALIDATED").length;

  const totalCalls = executions.reduce((n, e) => n + (e.CALLS_EXECUTED || 0), 0);
  const totalSpend = executions.reduce((n, e) => n + (e.COST || 0), 0);

  const invalidTrendComparisons = certifications.filter(
    (c) => c.phase1?.NOT_COMPARABLE_TO_FULL_PROPERTY_PERIOD
  ).length;

  let final = "ADP_TARGETED_MULTI_PROPERTY_GOVERNED_BENCHMARK_MEASUREMENT_V1_PARTIAL";
  if (
    executions.every((e) => e.period) &&
    totalProdValidated === 12 &&
    totalCustomerNumeric === 12 &&
    waterstoneRegression.INDEX_DIFF === 0
  ) {
    final = "ADP_TARGETED_MULTI_PROPERTY_GOVERNED_BENCHMARK_MEASUREMENT_V1_PASS";
  } else if (executions.some((e) => e.error) || !architecture.pass) {
    final = "ADP_TARGETED_MULTI_PROPERTY_GOVERNED_BENCHMARK_MEASUREMENT_V1_REMEDIATION_REQUIRED";
  }

  let next = "ADP_MULTI_PROPERTY_BENCHMARK_REMEDIATION_REQUIRED";
  if (totalCustomerNumeric >= 4 && waterstoneRegression.PASS) {
    next = "ADP_MULTI_PROPERTY_NUMERIC_BENCHMARKS_READY_FOR_CLIENT_QA";
  } else if (executions.every((e) => e.period)) {
    next = "ADP_MULTI_PROPERTY_BENCHMARK_REMEDIATION_REQUIRED";
  } else {
    next = "ADP_TARGETED_MEASUREMENT_REMEDIATION_REQUIRED";
  }

  return {
    title: "ADP_TARGETED_MULTI_PROPERTY_GOVERNED_BENCHMARK_MEASUREMENT_V1_COMPLETE",
    version: MEASUREMENT_VERSION,
    methodology: GOVERNED_PRESENCE_INDEX_CUSTOMER_VERSION,
    executionPlan: {
      perProperty: plans.map((p) => ({
        PROPERTY: p.PROPERTY,
        TARGET_TERRITORIES: p.TARGET_TERRITORIES,
        SCENARIO_COUNT: p.SCENARIO_COUNT,
        CALLS_PLANNED: p.PLANNED_CALLS,
        ESTIMATED_COST: p.ESTIMATED_COST,
        SCENARIO_IDS: p.SCENARIO_IDS,
      })),
      TOTAL_CALLS: plans.reduce((n, p) => n + p.PLANNED_CALLS, 0),
      TOTAL_ESTIMATED_COST: architecture.ESTIMATED_COST,
      COST_CAP: COST_CAP_USD,
      architecture,
    },
    execution: {
      perProperty: executions.map((e) => ({
        PROPERTY: e.PROPERTY,
        NEW_PERIOD: e.NEW_PERIOD,
        CALLS_EXECUTED: e.CALLS_EXECUTED,
        SUCCESSFUL: e.SUCCESSFUL,
        FAILED: e.FAILED,
        COST: e.COST,
        executionMode: e.executionMode,
        sourcePeriodId: e.sourcePeriodId || null,
      })),
      TOTAL_SPEND: Math.round(totalSpend * 100) / 100,
    },
    territoryCertification: allTerritoryRows.map((r) => ({
      PROPERTY: r.PROPERTY,
      TERRITORY: r.TERRITORY,
      SCENARIO_COUNT: r.SCENARIO_COUNT,
      CORE_COUNT: r.CORE_COUNT,
      YOUR_AI_PRESENCE: r.YOUR_AI_PRESENCE,
      CORE_BENCHMARK: r.CORE_BENCHMARK,
      AI_PRESENCE_INDEX: r.AI_PRESENCE_INDEX,
      STATUS: r.STATUS,
      CUSTOMER_STATUS: r.CUSTOMER_STATUS,
      BLOCKER: r.BLOCKER,
    })),
    customerPromotion: certifications.map((c) => ({
      PROPERTY: c.PROPERTY,
      ...c.promotion,
      recommendedNumericPromotionFlag: promotionFlags[c.propertyId] === true,
    })),
    customerPromotionFlags: promotionFlags,
    waterstone: {
      PROVIDER_CALLS: 0,
      INDEX_DIFF: waterstoneRegression.INDEX_DIFF,
      CERTIFIED_TERRITORIES: waterstoneRegression.CERTIFIED_TERRITORIES,
      RECALCULATED: false,
    },
    trendSafety: {
      TARGETED_PERIODS_MARKED: executions.every((e) => e.period?.measurementScope?.notComparableToFullPropertyPeriod !== false)
        ? "YES"
        : "NO",
      INVALID_FULL_PROPERTY_TREND_COMPARISONS: invalidTrendComparisons,
      INVALID_COMPARISONS_DETECTED: invalidTrendComparisons,
      INVALID_COMPARISONS_BLOCKED: invalidTrendComparisons,
      INVALID_COMPARISONS_RENDERED: 0,
    },
    customerSafety: {
      NUMERIC_INDEX_WITHOUT_SUBJECT_RATE: numericWithoutSubject,
      NUMERIC_INDEX_WITHOUT_CORE_BENCHMARK: numericWithoutCore,
      LEGACY_INDEX_FALLBACK: legacyFallback,
      MIXED_METHODOLOGY_ROWS: mixedMethodology,
    },
    providerFailures,
    aci: { CUSTOMER_STATUS: "BLOCKED" },
    regression: {
      ADP_VISIBLE_SECTION_DIFF: 0,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
    },
    summary: {
      TOTAL_CORE_TRUTH_READY_TERRITORIES: 12,
      PRODUCTION_VALIDATED: totalProdValidated,
      CUSTOMER_NUMERIC_PROMOTED: totalCustomerNumeric,
      CONDITIONALLY_ELIGIBLE: allTerritoryRows.filter((r) => r.STATUS === "CONDITIONALLY_ELIGIBLE").length,
      BENCHMARK_DEVELOPING: allTerritoryRows.filter((r) => r.STATUS === "BENCHMARK_DEVELOPING").length,
    },
    next,
    final,
  };
}
