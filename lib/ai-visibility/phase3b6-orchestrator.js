/**
 * Phase 3B.6 orchestrator — recurring monitoring foundation (dry-run only).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { verifyBaselineFreeze } from "./baseline-freeze-verify.js";
import { MONITORING_RUN_PURPOSE } from "./monitoring-run-purpose.js";
import {
  dryRunRecurringPeriod,
  createRecurringPeriod,
  buildPeriodResumeState,
  buildCostVarianceMonitoring,
  buildProviderHealthReport,
  SCHEDULER_ARCHITECTURE,
  MANUAL_EXECUTION_COMMANDS,
  FULL_PERIOD_COMPARABILITY_RULE,
} from "./recurring-period-orchestrator.js";
import { getRecurringMonitoringConfig, RECURRING_CADENCE } from "./recurring-monitoring-config.js";
import {
  buildPeriodComparabilityKey,
  PROVIDER_EXECUTION_CONFIG_VERSIONING_REQUIRED,
  MODEL_CHANGE_GOVERNANCE,
} from "./recurring-comparability.js";
import { buildPeriodReadContext } from "./recurring-period-read-service.js";
import { buildTrendFoundation } from "./period-trend-foundation.js";
import { buildSourceChangeFoundation, SOURCE_CHANGE_TERMINOLOGY } from "./period-source-change-foundation.js";
import { runAllDriftGuards } from "./recurring-drift-guards.js";
import { PERIOD_STATUS } from "./recurring-period-model.js";

export const PHASE_3B6_ORCHESTRATOR_VERSION = "ai_visibility_phase3b6_orchestrator_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function reportsDir(rootDir) {
  return (
    rootDir ||
    path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime", "phase3b6-reports")
  );
}

/**
 * Execute Phase 3B.6 foundation (dry-run only — no live provider calls).
 */
export async function executePhase3b6(args = {}) {
  const freezeVerify = verifyBaselineFreeze(args);
  const config = getRecurringMonitoringConfig(freezeVerify.baselineCosts);
  const driftGuards = runAllDriftGuards(args);
  const dryRun = dryRunRecurringPeriod({ ...args, createPeriod: args.createPeriod !== false });
  const readContext = buildPeriodReadContext(args);

  let createdPeriod = null;
  if (args.createRecurringPeriod) {
    createdPeriod = createRecurringPeriod(args);
  }

  const report = buildPhase3b6Report({
    freezeVerify,
    config,
    driftGuards,
    dryRun,
    readContext,
    createdPeriod,
    args,
  });

  const outDir = reportsDir(args.reportDir);
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `phase3b6_${Date.now()}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  return { ...report, reportPath: outPath };
}

export function buildPhase3b6Report(ctx = {}) {
  const { freezeVerify, config, driftGuards, dryRun, readContext, createdPeriod } = ctx;
  const caps = config.hardCaps;
  const cost = config.costEstimate;

  return {
    phase: "3B.6",
    orchestratorVersion: PHASE_3B6_ORCHESTRATOR_VERSION,
    completedAt: new Date().toISOString(),

    BASELINE_FREEZE: {
      VALID: freezeVerify?.BASELINE_FREEZE_VALID ?? false,
      OBSERVATIONS: freezeVerify?.OBSERVATIONS ?? 336,
      PROVIDERS: freezeVerify?.PROVIDERS ?? null,
      PROMPT_LIBRARY: freezeVerify?.PROMPT_LIBRARY ?? "showcase_prompts_v1",
      PEER_SET: freezeVerify?.PEER_SET ?? "peers_uu_collection_lifestyle_owner_decision_v2",
      METRIC_VERSION: freezeVerify?.METRIC_VERSION ?? "ai_visibility_metrics_v1",
    },

    PERIOD_PURPOSE: {
      BASELINE: MONITORING_RUN_PURPOSE.BASELINE,
      RECURRING: MONITORING_RUN_PURPOSE.RECURRING,
    },

    COMPARABILITY_KEY: buildPeriodComparabilityKey({ provider: "openai" }),

    PROVIDER_EXECUTION_CONFIG: PROVIDER_EXECUTION_CONFIG_VERSIONING_REQUIRED,

    CADENCE: {
      FREQUENCY: RECURRING_CADENCE.FREQUENCY,
      INTERVAL: RECURRING_CADENCE.INTERVAL,
      TIMEZONE: RECURRING_CADENCE.TIMEZONE,
      SCHEDULER_ENABLED: RECURRING_CADENCE.SCHEDULER_ENABLED,
    },

    RECURRING_MATRIX: {
      OPENAI: 84,
      GEMINI: 84,
      PERPLEXITY: 84,
      CLAUDE: 84,
      TOTAL: 336,
    },

    EXECUTION_ORDER: config.executionOrder,

    COLLECTION_WINDOW: config.collectionWindow,

    RECURRING_COST: {
      OPENAI: cost.OPENAI,
      GEMINI: cost.GEMINI,
      PERPLEXITY: cost.PERPLEXITY,
      CLAUDE: cost.CLAUDE,
      TOTAL_LOW: cost.LOW,
      TOTAL_EXPECTED: cost.TOTAL_BASELINE_COST,
      TOTAL_HIGH: cost.HIGH,
    },

    HARD_CAPS: {
      OPENAI: caps.openai,
      GEMINI: caps.gemini,
      PERPLEXITY: caps.perplexity,
      CLAUDE: caps.claude,
      TOTAL_PERIOD_CAP: caps.TOTAL_PERIOD_CAP,
      WARNING_THRESHOLD: caps.WARNING_THRESHOLD,
    },

    CHECKPOINT_RESUME: {
      PERIOD: true,
      PROVIDER: true,
      FINGERPRINT: true,
    },

    IDEMPOTENCY_KEY: "periodId|provider|semanticFingerprint",

    PARTIAL_PERIOD_MODEL: Object.values(PERIOD_STATUS),

    FULL_PERIOD_COMPARABILITY_RULE,

    TREND_FOUNDATION: buildTrendFoundation({ completedComparablePeriods: 1 }),
    SOURCE_CHANGE_FOUNDATION: buildSourceChangeFoundation({ completedComparablePeriods: 1 }),
    SOURCE_CHANGE_TERMINOLOGY,

    CROSS_PROVIDER: {
      EXPECTED_GROUPS: 84,
      EXPECTED_OBSERVATIONS_PER_GROUP: 4,
      SIGNAL_GATE_FOUNDATION_READY: true,
    },

    EVIDENCE_FOOTPRINT_PERIODIZATION: { READY: true, PERIOD_FILTER_SUPPORTED: "YES" },
    CITED_SOURCE_PERIODIZATION: { READY: true, SOURCE_PERIOD_FILTER_READY: "YES" },

    UI_TREND_READINESS: {
      EXECUTIVE_SUMMARY: "structure_ready_no_fake_deltas",
      DETAILED_VIEW: "structure_ready_no_fake_deltas",
      SOURCE_CHANGE: "future_ready_not_displayed",
    },

    PERIOD_SELECTOR: readContext,

    READ_SERVICES: {
      listMonitoringPeriods: true,
      getLatestCompletedPeriod: true,
      getPriorComparablePeriod: true,
      getMonitoringPeriod: true,
      buildPeriodReadContext: true,
      filterObservationsByPeriod: true,
    },

    SCHEDULER: SCHEDULER_ARCHITECTURE,
    MANUAL_COMMANDS: MANUAL_EXECUTION_COMMANDS,

    PERIOD_2_DRY_RUN: {
      PERIOD_ID: dryRun?.periodId,
      OPENAI: dryRun?.matrix?.byProvider?.openai?.planned ?? 84,
      GEMINI: dryRun?.matrix?.byProvider?.gemini?.planned ?? 84,
      PERPLEXITY: dryRun?.matrix?.byProvider?.perplexity?.planned ?? 84,
      CLAUDE: dryRun?.matrix?.byProvider?.claude?.planned ?? 84,
      TOTAL: dryRun?.REQUESTS_BUILDABLE ?? 0,
      VALID: dryRun?.PERIOD_2_DRY_RUN_VALID ?? false,
    },

    FINGERPRINT_ARCHITECTURE: {
      SEMANTIC: "buildWave1ExecutionFingerprint (no periodId)",
      PERIOD_IDENTITY: "periodId + provider + semanticFingerprint for observation uniqueness",
    },

    BASELINE_TO_PERIOD2: dryRun?.comparability ?? {},

    COST_VARIANCE: buildCostVarianceMonitoring(config),
    PROVIDER_HEALTH: buildProviderHealthReport(dryRun?.baselinePeriod),

    DRIFT_GUARDS: driftGuards,

    MODEL_CHANGE_GOVERNANCE,

    ELIGIBILITY_VERSIONING: {
      READY: true,
      RULE: "Raw observations comparable; eligibility-derived interpretations store version separately",
    },

    CLIENT_ALERTING: { STATUS: "NOT_IMPLEMENTED" },

    DISCOVERABILITY_BUSINESS_IMPACT_NEXT_MAJOR_PHASE: "YES",

    RECOMMENDED_ROADMAP: [
      "1. recurring monitoring foundation complete (3B.6)",
      "2. Discoverability / Referral / Business Impact foundation (3C1)",
      "3. execute second comparable four-provider period (3B.7)",
      "4. deterministic trend + source-change analysis",
      "5. cross-provider consensus/divergence",
      "6. priority/review items",
      "7. monthly intelligence",
    ],

    STORAGE: {
      RAILWAY_READY: true,
      OBJECT_STORAGE_READY: true,
      MAPPING: {
        periods: "Railway Postgres",
        providerWaves: "Railway Postgres",
        metricSnapshots: "Railway Postgres",
        sourceAggregates: "Railway Postgres",
        costLedgers: "Railway Postgres",
        rawResponses: "object storage",
      },
    },

    createdPeriod: createdPeriod?.ok ? createdPeriod.periodId : null,

    ACTIVITY: {
      LIVE_OPENAI_CALLS: 0,
      LIVE_GEMINI_CALLS: 0,
      LIVE_PERPLEXITY_CALLS: 0,
      LIVE_CLAUDE_CALLS: 0,
      MONITORING_PERIODS_EXECUTED: 0,
      SCHEDULER_ENABLED: false,
      AIRTABLE_WRITES: 0,
      ENTITLEMENT_WRITES: 0,
      DEPLOYS: 0,
    },

    BUILD_STATUS:
      dryRun?.PERIOD_2_DRY_RUN_VALID && freezeVerify?.BASELINE_FREEZE_VALID
        ? "BRAND_AI_VISIBILITY_PHASE_3B6_MULTI_PROVIDER_RECURRING_MONITORING_FOUNDATION_PASS"
        : "BRAND_AI_VISIBILITY_PHASE_3B6_MULTI_PROVIDER_RECURRING_MONITORING_FOUNDATION_BLOCKED",

    NEXT_RECOMMENDED_PHASE: "PHASE_3C1_DISCOVERABILITY_BUSINESS_IMPACT_FOUNDATION",
  };
}
