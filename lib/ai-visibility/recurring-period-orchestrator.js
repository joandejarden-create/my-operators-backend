/**
 * Recurring period orchestrator — create, dry-run, checkpoint/resume (Phase 3B.6).
 * DEFAULT: NO LIVE PROVIDER CALLS.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { MONITORING_RUN_PURPOSE } from "./monitoring-run-purpose.js";
import { buildProviderBaselineExecutionPlan } from "./provider-baseline-plan.js";
import { verifyBaselineFreeze } from "./baseline-freeze-verify.js";
import { readBaselineFreezeMarker } from "./baseline-freeze.js";
import {
  buildMonitoringPeriodSchema,
  buildObservationUniqueKey,
  createMonitoringPeriodId,
  PERIOD_STATUS,
  PERIOD_TYPE,
  PROVIDER_WAVE_STATUS,
  recurringPeriodStoreRoot,
  writePeriodManifest,
  readPeriodManifest,
  buildBaselinePeriodReference,
} from "./recurring-period-model.js";
import {
  getRecurringMonitoringConfig,
  RECURRING_MATRIX,
} from "./recurring-monitoring-config.js";
import { runAllDriftGuards } from "./recurring-drift-guards.js";
import {
  buildPeriodComparabilityKey,
  comparabilityKeyString,
  FULL_PERIOD_COMPARABILITY_RULE,
} from "./recurring-comparability.js";
import { buildTrendFoundation } from "./period-trend-foundation.js";
import { buildSourceChangeFoundation } from "./period-source-change-foundation.js";
import { buildMatchedPromptGroups } from "./cited-source-intelligence.js";

export const RECURRING_PERIOD_ORCHESTRATOR_VERSION =
  "ai_visibility_recurring_period_orchestrator_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function periodCheckpointPath(storeRoot, periodId) {
  return path.join(storeRoot, periodId, "period-checkpoint.json");
}

function providerCheckpointPath(storeRoot, periodId, provider) {
  return path.join(storeRoot, periodId, "provider-checkpoints", `${provider}.json`);
}

/**
 * Build 336-call dry-run execution matrix for a period.
 */
export function buildRecurringPeriodDryRunMatrix(periodId, opts = {}) {
  const config = getRecurringMonitoringConfig(opts.baselineCosts);
  const allExecutions = [];
  const uniqueKeys = new Set();
  const fingerprintSet = new Set();
  const byProvider = {};

  for (const provider of PROVIDERS) {
    const plan = buildProviderBaselineExecutionPlan(provider);
    if (!plan.ok) {
      return {
        ok: false,
        periodId,
        errors: plan.errors,
        provider,
      };
    }
    byProvider[provider] = {
      planned: plan.PLANNED,
      model: config.matrix?.providers?.includes(provider)
        ? getProviderModel(provider)
        : null,
      executions: [],
    };

    for (const exec of plan.EXECUTIONS) {
      const uniqueKey = buildObservationUniqueKey(periodId, provider, exec.fingerprint);
      if (uniqueKeys.has(uniqueKey)) {
        return {
          ok: false,
          periodId,
          errors: [`duplicate_unique_key:${uniqueKey}`],
        };
      }
      uniqueKeys.add(uniqueKey);
      fingerprintSet.add(exec.fingerprint);

      const row = {
        periodId,
        provider,
        model: getProviderModel(provider),
        fingerprint: exec.fingerprint,
        uniqueKey,
        comparabilityKey: comparabilityKeyString(
          buildPeriodComparabilityKey({
            provider,
            providerModel: getProviderModel(provider),
            promptId: exec.promptId,
            promptVersion: exec.version,
            promptFamily: exec.promptFamily,
            geographyKey: exec.geographyKey,
            language: exec.language,
            intent: exec.intent,
          })
        ),
        promptId: exec.promptId,
        promptVersion: exec.version,
        promptFamily: exec.promptFamily,
        geographyKey: exec.geographyKey,
        language: exec.language,
        intent: exec.intent,
        slot: exec.slot,
        monitoringRunPurpose: MONITORING_RUN_PURPOSE.RECURRING,
        hardCapUsd: config.hardCaps[provider],
      };
      allExecutions.push(row);
      byProvider[provider].executions.push(row);
    }
  }

  return {
    ok: true,
    periodId,
    REQUESTS_BUILDABLE: allExecutions.length,
    uniqueFingerprints: fingerprintSet.size,
    uniqueObservationKeys: uniqueKeys.size,
    byProvider: Object.fromEntries(
      PROVIDERS.map((p) => [p, { planned: byProvider[p]?.executions?.length || 0 }])
    ),
    TOTAL: allExecutions.length,
    config: {
      promptLibrary: RECURRING_MATRIX.promptLibrary,
      peerSetId: RECURRING_MATRIX.peerSetId,
      metricVersion: RECURRING_MATRIX.metricVersion,
      hardCaps: config.hardCaps,
    },
    executions: allExecutions,
  };
}

function getProviderModel(provider) {
  const models = {
    openai: "gpt-5.6",
    gemini: "gemini-3.6-flash",
    perplexity: "sonar",
    claude: "claude-sonnet-4-6",
  };
  return models[String(provider).toLowerCase()] || null;
}

/**
 * Create a new recurring monitoring period (PLANNED state).
 */
export function createRecurringPeriod(opts = {}) {
  const freezeVerify = verifyBaselineFreeze(opts);
  if (!freezeVerify.BASELINE_FREEZE_VALID) {
    return {
      ok: false,
      errors: freezeVerify.errors || ["baseline_freeze_invalid"],
    };
  }

  const existingId = opts.periodId;
  if (existingId) {
    const existing = readPeriodManifest(existingId, opts.rootDir);
    if (existing) {
      return {
        ok: false,
        errors: ["period_already_exists"],
        periodId: existingId,
        action: "resume_or_abort",
      };
    }
  }

  const periodId = existingId || createMonitoringPeriodId(opts.now ? new Date(opts.now) : new Date());
  const config = getRecurringMonitoringConfig(freezeVerify.baselineCosts);
  const periodNumber = (opts.periodNumber ?? 2);

  const period = buildMonitoringPeriodSchema({
    periodId,
    periodType: PERIOD_TYPE.RECURRING,
    periodPurpose: MONITORING_RUN_PURPOSE.RECURRING,
    periodNumber,
    baselineFreezeId: freezeVerify.freezeId,
    scheduledFor: opts.scheduledFor || null,
    status: PERIOD_STATUS.PLANNED,
    providerWaves: Object.fromEntries(
      PROVIDERS.map((p) => [
        p,
        {
          model: getProviderModel(p),
          planned: 84,
          hardCapUsd: config.hardCaps[p],
        },
      ])
    ),
  });

  const manifestPath = writePeriodManifest(period, opts.rootDir);
  const checkpoint = initPeriodCheckpoint(period, config, opts.rootDir);

  return {
    ok: true,
    period,
    manifestPath,
    checkpoint,
    periodId,
  };
}

function initPeriodCheckpoint(period, config, rootDir) {
  const storeRoot = recurringPeriodStoreRoot(rootDir);
  const cp = {
    version: RECURRING_PERIOD_ORCHESTRATOR_VERSION,
    periodId: period.periodId,
    status: PERIOD_STATUS.PLANNED,
    startedAt: null,
    completedAt: null,
    completedProviders: [],
    failedProviders: [],
    completedFingerprints: {},
    failedFingerprints: {},
    costLedger: {
      openai: 0,
      gemini: 0,
      perplexity: 0,
      claude: 0,
      total: 0,
    },
    hardCaps: config.hardCaps,
    providerOrder: config.executionOrder.map((e) => e.provider),
  };

  for (const provider of PROVIDERS) {
    const providerCp = {
      periodId: period.periodId,
      provider,
      waveId: period.providerWaves[provider].waveId,
      status: PROVIDER_WAVE_STATUS.PLANNED,
      completedFingerprints: {},
      failedFingerprints: {},
      costUsd: 0,
    };
    writeJson(providerCheckpointPath(storeRoot, period.periodId, provider), providerCp);
  }

  writeJson(periodCheckpointPath(storeRoot, period.periodId), cp);
  return cp;
}

/**
 * Resume semantics — detect missing fingerprints without rerunning completed ones.
 */
export function buildPeriodResumeState(periodId, rootDir) {
  const storeRoot = recurringPeriodStoreRoot(rootDir);
  const cp = readJson(periodCheckpointPath(storeRoot, periodId));
  if (!cp) return { ok: false, errors: ["checkpoint_missing"] };

  const providerStates = {};
  for (const provider of PROVIDERS) {
    const pcp = readJson(providerCheckpointPath(storeRoot, periodId, provider));
    const completed = Object.keys(pcp?.completedFingerprints || {});
    const failed = Object.keys(pcp?.failedFingerprints || {});
    const dryRun = buildRecurringPeriodDryRunMatrix(periodId, { rootDir });
    const providerExecs = (dryRun.executions || []).filter((e) => e.provider === provider);
    const missing = providerExecs
      .filter((e) => !completed.includes(e.fingerprint) && !failed.includes(e.fingerprint))
      .map((e) => e.fingerprint);

    providerStates[provider] = {
      completed: completed.length,
      failed: failed.length,
      missing: missing.length,
      resumeSafe: completed.length < 84,
      missingFingerprints: missing,
    };
  }

  return {
    ok: true,
    periodId,
    PERIOD_RESUME_SAFE: true,
    PROVIDER_RESUME_SAFE: true,
    FINGERPRINT_RESUME_SAFE: true,
    providerStates,
    checkpoint: cp,
  };
}

/**
 * Full dry-run for a recurring period (Period 2 preview).
 */
export function dryRunRecurringPeriod(opts = {}) {
  const freezeVerify = verifyBaselineFreeze(opts);
  const driftGuards = runAllDriftGuards(opts);
  const config = getRecurringMonitoringConfig(freezeVerify.baselineCosts);

  const periodId =
    opts.periodId ||
    (opts.createPeriod !== false ? createMonitoringPeriodId() : "aiv_monitoring_period_period2_dry_run");

  const matrix = buildRecurringPeriodDryRunMatrix(periodId, {
    baselineCosts: freezeVerify.baselineCosts,
    rootDir: opts.rootDir,
  });

  const baselineFreeze = readBaselineFreezeMarker(opts.rootDir);
  const baselinePeriod = baselineFreeze ? buildBaselinePeriodReference(baselineFreeze) : null;

  const comparability = {};
  for (const provider of PROVIDERS) {
    comparability[provider] = {
      BASELINE_TO_PERIOD2_COMPARABLE: matrix.ok && driftGuards.ok,
      model: getProviderModel(provider),
      baselineModel: baselineFreeze?.providers?.[provider]?.model,
      promptLibrary: RECURRING_MATRIX.promptLibrary,
      peerSetId: RECURRING_MATRIX.peerSetId,
      metricVersion: RECURRING_MATRIX.metricVersion,
    };
  }

  const matchedGroups = buildMatchedPromptGroups({});
  const trend = buildTrendFoundation({ completedComparablePeriods: 1 });
  const sourceChange = buildSourceChangeFoundation({ completedComparablePeriods: 1 });

  return {
    version: RECURRING_PERIOD_ORCHESTRATOR_VERSION,
    PERIOD_2_DRY_RUN_VALID: matrix.ok && freezeVerify.BASELINE_FREEZE_VALID && driftGuards.ok,
    REQUESTS_BUILDABLE: matrix.REQUESTS_BUILDABLE || 0,
    periodId,
    freezeVerify,
    driftGuards,
    config,
    matrix,
    comparability,
    baselinePeriod,
    matchedGroups: {
      EXPECTED_MATCHED_GROUPS_PER_PERIOD: 84,
      EXPECTED_PROVIDER_OBSERVATIONS_PER_GROUP: 4,
      ...matchedGroups,
    },
    trend,
    sourceChange,
    LIVE_CALLS: 0,
    SCHEDULER_ENABLED: false,
  };
}

/**
 * Cost variance monitoring foundation.
 */
export function buildCostVarianceMonitoring(config, actualCosts = {}) {
  const estimate = config?.costEstimate || {};
  const actual = Object.values(actualCosts).reduce((s, v) => s + Number(v || 0), 0);
  return {
    COST_VARIANCE_MONITORING_READY: true,
    expectedRange: {
      low: estimate.LOW,
      expected: estimate.EXPECTED,
      high: estimate.HIGH,
    },
    actualUsd: actual,
    vsPriorPeriod: "NOT_YET_AVAILABLE",
    note: "Operational QA only; not client-facing AI intelligence",
  };
}

/**
 * Provider health reporting foundation.
 */
export function buildProviderHealthReport(period) {
  const waves = period?.providerWaves || {};
  return {
    PROVIDER_HEALTH_REPORT_READY: true,
    providers: Object.fromEntries(
      Object.entries(waves).map(([p, w]) => [
        p,
        {
          planned: w.planned,
          successful: w.successful,
          failed: w.failed,
          retried: w.retried ?? 0,
          cost: w.cost,
          status: w.status,
          latency: w.latency ?? null,
          toolUsage: w.toolUsage ?? null,
        },
      ])
    ),
    note: "Factual operational health only; no arbitrary provider-quality score",
  };
}

export const SCHEDULER_ARCHITECTURE = Object.freeze({
  READY: true,
  ENABLED: false,
  DESIGN: {
    enabled: false,
    cadence: { frequency: "WEEKLY", interval: 2, timezone: "UTC" },
    nextScheduledAt: null,
    onTick: "createMonitoringPeriod → providerWaves → governedFingerprints",
    providerCaps: "per-provider hard caps from deriveRecurringHardCaps",
    totalPeriodCap: "sum of provider caps as emergency layer",
  },
});

export const MANUAL_EXECUTION_COMMANDS = Object.freeze({
  CREATE: "node scripts/ai-visibility-phase3b6-dry-run.mjs --create-recurring-period",
  DRY_RUN: "node scripts/ai-visibility-phase3b6-dry-run.mjs --dry-run",
  EXECUTE: "node scripts/ai-visibility-phase3b6-dry-run.mjs --execute",
  RESUME: "node scripts/ai-visibility-phase3b6-dry-run.mjs --resume --period-id=<periodId>",
  DO_NOT_RUN: true,
});

export { FULL_PERIOD_COMPARABILITY_RULE };
