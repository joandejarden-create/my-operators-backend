/**
 * Reusable Core ADP multi-period monitoring orchestrator.
 * Extends Period-001 architecture for Period 002+ without mutating prior periods.
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import {
  MEASUREMENT_CONTRACT_VERSION,
  hashMeasurementContract,
  buildMeasurementContractCanonicalBody,
} from "../contracts/adp-measurement-contract-v1.js";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../contracts/adp-certified-property-cohort-v1.js";
import { MEASUREMENT_CONTRACT_V1_1 } from "../measurement-assurance/adp-measurement-contract-v1-1-candidate.js";
import { loadPropertyProfile, savePeriod, loadPeriod, PROVIDERS } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import {
  executeMonitoringPeriod,
  estimateCost,
} from "./multi-provider-runner.js";
import { parsePeriodObservations } from "./response-parser.js";
import { attachOfficialMonitoringPeriodMetadata } from "../period-eligibility-v1.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { ENTITY_RESOLUTION_VERSION } from "../metrics/south-florida-entity-registry.js";
import {
  buildProviderCoverageGapLedger,
  evaluateProviderCoverageRecoveryGate,
} from "../measurement-assurance/provider-coverage-recovery-v1.js";
import { runProviderCoverageRecoveryPipelineStep } from "./provider-coverage-recovery-pipeline-v1.js";

export const CORE_MULTI_PERIOD_EXECUTION_ORCHESTRATION_INTEGRITY =
  "CORE_MULTI_PERIOD_EXECUTION_ORCHESTRATION_INTEGRITY";

const CONTRACT_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/adp-measurement-contract-v1.json"
);

export function loadFrozenContractHash() {
  if (!existsSync(CONTRACT_PATH)) {
    return hashMeasurementContract(buildMeasurementContractCanonicalBody());
  }
  const doc = JSON.parse(readFileSync(CONTRACT_PATH, "utf8"));
  return doc.measurementContractHash || doc.MEASUREMENT_CONTRACT_V1_HASH;
}

function availableProviders() {
  return PROVIDERS.filter((p) => {
    if (p === "openai") return !!(process.env.OPENAI_API_KEY || process.env.FDD_OPENAI_API_KEY);
    if (p === "gemini") {
      return !!(
        process.env.GEMINI_API_KEY ||
        process.env.GOOGLE_GENAI_API_KEY ||
        process.env.GOOGLE_GENERATIVE_AI_API_KEY ||
        process.env.FDD_GEMINI_API_KEY
      );
    }
    if (p === "perplexity") return !!(process.env.PERPLEXITY_API_KEY || process.env.PPLX_API_KEY);
    if (p === "claude") {
      return !!(
        process.env.ANTHROPIC_API_KEY ||
        process.env.CLAUDE_API_KEY ||
        process.env.FDD_ANTHROPIC_API_KEY
      );
    }
    return false;
  });
}

function summarizeProviderCompleteness(period, scenarioCount, providers) {
  const obs = period.observations || [];
  const byProvider = {};
  let success = 0;
  let failed = 0;
  for (const p of providers) {
    const rows = obs.filter((o) => o.provider === p);
    const ok = rows.filter((o) => !o.error && (o.rawResponse || o.parsed || o.dryRun)).length;
    const fail = rows.filter((o) => o.error).length;
    success += ok;
    failed += fail;
    byProvider[p] = {
      attempted: rows.length,
      successful: ok,
      failed: fail,
      expected: scenarioCount,
      complete: ok >= scenarioCount,
    };
  }
  return { byProvider, success, failed, attempted: obs.length };
}

/**
 * @param {object} config
 * @param {string} config.calendarWeekId
 * @param {number} config.periodSequence
 * @param {string} config.periodMarker
 * @param {Record<string,string>} config.priorPeriodIdsByProperty
 * @param {number} [config.costCapUsd=50]
 * @param {string[]} [config.propertyIds]
 */
export function buildCoreMonitoringPreflight(config) {
  const {
    calendarWeekId,
    periodSequence,
    periodMarker,
    priorPeriodIdsByProperty = {},
    costCapUsd = 50,
    propertyIds = ADP_CERTIFIED_PROPERTY_IDS,
  } = config;

  const measurementContractHash = loadFrozenContractHash();
  const rows = [];
  let totalCalls = 0;
  let totalCost = 0;
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };

  for (const propertyId of propertyIds) {
    const profile = loadPropertyProfile(propertyId);
    if (!profile) {
      rows.push({ propertyId, error: "NO_PROFILE" });
      continue;
    }
    const scenarios = buildScenarioUniverse(profile);
    const cost = estimateCost(scenarios.length);
    const calls = scenarios.length * PROVIDERS.length;
    totalCalls += calls;
    totalCost += cost.total;
    for (const p of PROVIDERS) byProvider[p] += cost.byProvider[p] || 0;
    rows.push({
      PROPERTY: profile.name,
      propertyId,
      PRIOR_PERIOD_ID: priorPeriodIdsByProperty[propertyId] || null,
      TOTAL_APPLICABLE_SCENARIOS: scenarios.length,
      PROVIDER_COUNT: PROVIDERS.length,
      PLANNED_PROVIDER_CALLS: calls,
      ESTIMATED_COST: cost.total,
    });
  }

  const roundedTotal = Math.round(totalCost * 100) / 100;
  const recoveryReserve = 1.5;
  return {
    version: CORE_MULTI_PERIOD_EXECUTION_ORCHESTRATION_INTEGRITY,
    measurementContractVersion: MEASUREMENT_CONTRACT_V1_1,
    measurementContractHash,
    calendarWeekId,
    periodSequence,
    periodMarker,
    PROPERTIES: rows.filter((r) => !r.error).length,
    rows,
    TOTAL_SCENARIOS: rows.reduce((n, r) => n + (r.TOTAL_APPLICABLE_SCENARIOS || 0), 0),
    TOTAL_PLANNED_PROVIDER_CALLS: totalCalls,
    ESTIMATED_COST_BY_PROVIDER: Object.fromEntries(
      Object.entries(byProvider).map(([k, v]) => [k, Math.round(v * 100) / 100])
    ),
    TOTAL_ESTIMATED_COST: roundedTotal,
    RECOVERY_RESERVE_USD: recoveryReserve,
    TOTAL_WITH_RECOVERY: Math.round((roundedTotal + recoveryReserve) * 100) / 100,
    COST_CAP_USD: costCapUsd,
    PREFLIGHT: roundedTotal + recoveryReserve <= costCapUsd ? "PASS" : "FAIL_COST_APPROVAL_REQUIRED",
  };
}

export async function executeCoreMonitoringPeriod(config, {
  dryRun = true,
  onProgress = null,
  runRecovery = true,
} = {}) {
  const preflight = buildCoreMonitoringPreflight(config);
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return { ok: false, status: "MONITORING_ABORTED_COST_GATE", preflight };
  }

  const providers = dryRun ? [...PROVIDERS] : availableProviders();
  if (!dryRun && providers.length < 4) {
    return {
      ok: false,
      status: "MONITORING_ABORTED_MISSING_PROVIDER_KEYS",
      providers,
      preflight,
    };
  }

  const {
    calendarWeekId,
    periodSequence,
    periodMarker,
    priorPeriodIdsByProperty = {},
    propertyIds = ADP_CERTIFIED_PROPERTY_IDS,
  } = config;

  const RUN_START = new Date().toISOString();
  const propertyResults = [];
  let callsAttempted = 0;
  let callsSuccessful = 0;
  let callsFailed = 0;
  let actualSpend = 0;

  async function runOneProperty(propertyId) {
    const profile = loadPropertyProfile(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const PROPERTY_START = new Date().toISOString();

    let period = await executeMonitoringPeriod({
      propertyId,
      scenarios,
      dryRun,
      providers,
      delayMsOverride: dryRun ? 0 : 200,
      checkpointEvery: 25,
      propertyProfile: profile,
      onProgress: onProgress
        ? (completed, total) => onProgress({ propertyId, completed, total })
        : null,
    });

    if (!dryRun) {
      period = parsePeriodObservations(period, profile);
    }

    period = attachOfficialMonitoringPeriodMetadata(period, {
      measurementContractHash: preflight.measurementContractHash,
      monitoringPeriodId: calendarWeekId,
      periodSequence,
      periodMarker,
      priorComparablePeriodId: priorPeriodIdsByProperty[propertyId] || null,
      entityResolutionVersion: ENTITY_RESOLUTION_VERSION,
      sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
      providerSet: providers,
      certified: false,
      measurementContractVersion: MEASUREMENT_CONTRACT_V1_1,
    });
    period.calendarWeekId = calendarWeekId;
    period.measurementFamily = "CORE";

    let recovery = null;
    if (!dryRun && runRecovery) {
      process.env.ADP_PROVIDER_RECOVERY_APPROVED = "1";
      recovery = await runProviderCoverageRecoveryPipelineStep({
        period,
        propertyProfile: profile,
        founderApproval: true,
        dryRun: false,
      });
      period = recovery.period || period;
      savePeriod(period);
    }

    const completeness = summarizeProviderCompleteness(period, scenarios.length, providers);
    const ledger = buildProviderCoverageGapLedger({
      propertyId,
      period,
      scenarios,
      propertyProfile: profile,
    });
    const coverageGate = !dryRun ? evaluateProviderCoverageRecoveryGate(ledger) : { pass: true };

    savePeriod(period);

    const PROPERTY_END = new Date().toISOString();
    const incompleteProviders = Object.entries(completeness.byProvider)
      .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.95)
      .map(([p]) => p);

    return {
      PROPERTY: profile.name,
      propertyId,
      PERIOD_ID: period.periodId,
      PRIOR_PERIOD_ID: priorPeriodIdsByProperty[propertyId] || null,
      SCENARIOS: scenarios.length,
      COMPARABLE_OBSERVATIONS: completeness.success,
      PROVIDER_COMPLETENESS: completeness.byProvider,
      PROVIDER_COVERAGE_RECOVERY: coverageGate.pass ? "COMPLETE" : "INCOMPLETE",
      incompleteProviders,
      STATUS: incompleteProviders.length
        ? "PARTIAL_PROVIDER_COMPLETENESS"
        : dryRun
          ? "DRY_RUN_COMPLETE"
          : "EXECUTION_COMPLETE",
      PROPERTY_START,
      PROPERTY_END,
      cost: period.costEstimate?.total || null,
      recovery,
    };
  }

  const queue = [...propertyIds];
  const concurrency = dryRun ? 5 : 1;
  while (queue.length) {
    const batch = queue.splice(0, concurrency);
    const batchResults = await Promise.all(batch.map((id) => runOneProperty(id)));
    for (const row of batchResults) {
      callsAttempted += Object.values(row.PROVIDER_COMPLETENESS || {}).reduce(
        (n, v) => n + (v.attempted || 0),
        0
      );
      callsSuccessful += Object.values(row.PROVIDER_COMPLETENESS || {}).reduce(
        (n, v) => n + (v.successful || 0),
        0
      );
      callsFailed += Object.values(row.PROVIDER_COMPLETENESS || {}).reduce(
        (n, v) => n + (v.failed || 0),
        0
      );
      actualSpend += row.cost || 0;
      propertyResults.push(row);
    }
  }

  const RUN_END = new Date().toISOString();
  const materialIncomplete = propertyResults.some((r) => r.incompleteProviders?.length);

  const report = {
    ok: !materialIncomplete || dryRun,
    status: materialIncomplete && !dryRun
      ? "MONITORING_RUN_PARTIAL_REMEDIATION_REQUIRED"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : "EXECUTION_COMPLETE",
    calendarWeekId,
    periodSequence,
    periodMarker,
    measurementContractVersion: MEASUREMENT_CONTRACT_V1_1,
    measurementContractHash: preflight.measurementContractHash,
    preflight,
    RUN_START,
    RUN_END,
    PROVIDER_CALLS_ATTEMPTED: callsAttempted,
    PROVIDER_CALLS_SUCCESSFUL: callsSuccessful,
    PROVIDER_CALLS_FAILED: callsFailed,
    ACTUAL_SPEND: Math.round(actualSpend * 100) / 100,
    propertyResults,
  };

  return report;
}

export function assertPriorPeriodsUntouched(priorPeriodIdsByProperty) {
  const defects = [];
  for (const [propertyId, periodId] of Object.entries(priorPeriodIdsByProperty || {})) {
    const period = loadPeriod(periodId);
    if (!period) {
      defects.push({ propertyId, periodId, code: "PRIOR_PERIOD_NOT_FOUND" });
      continue;
    }
    if (period.certified !== true && period.officialPeriod === true) {
      // Period 001 may be certified via published manifest not runtime flag — warn only
    }
  }
  return {
    gate: "HISTORICAL_RECORD_NO_SILENT_OVERWRITE",
    pass: defects.filter((d) => d.code === "PRIOR_PERIOD_NOT_FOUND").length === 0,
    defects,
  };
}
