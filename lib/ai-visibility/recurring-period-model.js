/**
 * Governed recurring monitoring period model (Phase 3B.6).
 * Each period = one intended four-provider 336-call cycle.
 */

import fs from "fs";
import path from "path";
import { randomBytes } from "crypto";
import { fileURLToPath } from "url";
import { METRIC_VERSION } from "./config.js";
import { PEER_SET_ID_V2 } from "./peer-sets.js";
import { MONITORING_RUN_PURPOSE } from "./monitoring-run-purpose.js";
import { BASELINE_FREEZE_ID } from "./baseline-freeze.js";

export const RECURRING_PERIOD_MODEL_VERSION = "ai_visibility_recurring_period_model_v1";

export const PERIOD_STATUS = Object.freeze({
  PLANNED: "PLANNED",
  RUNNING: "RUNNING",
  PARTIAL: "PARTIAL",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  STOPPED_COST_CAP: "STOPPED_COST_CAP",
});

export const PERIOD_TYPE = Object.freeze({
  BASELINE: "baseline",
  RECURRING: "recurring",
});

export const PROVIDER_WAVE_STATUS = Object.freeze({
  PLANNED: "PLANNED",
  RUNNING: "RUNNING",
  PARTIAL: "PARTIAL",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  STOPPED_COST_CAP: "STOPPED_COST_CAP",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const BASELINE_PERIOD_ID = "aiv_monitoring_period_baseline_four_provider_v1";

export const PROVIDER_MODELS = Object.freeze({
  openai: "gpt-5.6",
  gemini: "gemini-3.6-flash",
  perplexity: "sonar",
  claude: "claude-sonnet-4-6",
});

/**
 * Create period ID: aiv_monitoring_period_<YYYYMMDD>_<hex>
 */
export function createMonitoringPeriodId(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_monitoring_period_${y}${m}${d}_${rand}`;
}

function createProviderWaveId(provider, periodId) {
  const slug = String(periodId).replace(/^aiv_monitoring_period_/, "");
  return `aiv_recurring_${provider}_${slug}`;
}

/**
 * Build provider child wave record.
 */
export function buildProviderWaveRecord(provider, periodId, opts = {}) {
  const id = String(provider || "").toLowerCase();
  return {
    provider: id,
    model: opts.model || PROVIDER_MODELS[id] || null,
    waveId: opts.waveId || createProviderWaveId(id, periodId),
    startedAt: opts.startedAt || null,
    completedAt: opts.completedAt || null,
    planned: opts.planned ?? 84,
    successful: opts.successful ?? 0,
    failed: opts.failed ?? 0,
    retried: opts.retried ?? 0,
    cost: opts.cost ?? 0,
    status: opts.status || PROVIDER_WAVE_STATUS.PLANNED,
    hardCapUsd: opts.hardCapUsd ?? null,
  };
}

/**
 * Build full monitoring period schema.
 */
export function buildMonitoringPeriodSchema(input = {}) {
  const periodId = input.periodId || createMonitoringPeriodId();
  const periodType = input.periodType || PERIOD_TYPE.RECURRING;
  const purpose =
    input.periodPurpose ||
    (periodType === PERIOD_TYPE.BASELINE
      ? MONITORING_RUN_PURPOSE.BASELINE
      : MONITORING_RUN_PURPOSE.RECURRING);

  const providers = ["openai", "gemini", "perplexity", "claude"];
  const providerWaves = {};
  for (const p of providers) {
    providerWaves[p] = buildProviderWaveRecord(p, periodId, input.providerWaves?.[p] || {});
  }

  return {
    version: RECURRING_PERIOD_MODEL_VERSION,
    periodId,
    periodType,
    periodPurpose: purpose,
    periodNumber: input.periodNumber ?? null,
    baselineFreezeId: input.baselineFreezeId || null,
    scheduledFor: input.scheduledFor || null,
    startedAt: input.startedAt || null,
    completedAt: input.completedAt || null,
    status: input.status || PERIOD_STATUS.PLANNED,
    promptLibraryVersion: input.promptLibraryVersion || "showcase_prompts_v1",
    peerSetId: input.peerSetId || PEER_SET_ID_V2,
    peerSetVersion: input.peerSetVersion || "2",
    metricVersion: input.metricVersion || METRIC_VERSION,
    plannedCalls: input.plannedCalls ?? 336,
    successfulCalls: input.successfulCalls ?? 0,
    failedCalls: input.failedCalls ?? 0,
    totalCostUsd: input.totalCostUsd ?? 0,
    providerWaves,
    TREND_AVAILABLE: false,
    SOURCE_CHANGE_AVAILABLE: false,
    IMMUTABLE_BASELINE_REFERENCE:
      periodType === PERIOD_TYPE.BASELINE ? BASELINE_FREEZE_ID : null,
  };
}

/**
 * Resolve period status from provider wave completion.
 */
export function resolvePeriodStatus(period) {
  const waves = Object.values(period?.providerWaves || {});
  if (!waves.length) return PERIOD_STATUS.PLANNED;

  const allCompleted = waves.every(
    (w) => w.status === PROVIDER_WAVE_STATUS.COMPLETED && w.successful >= 84
  );
  if (allCompleted) return PERIOD_STATUS.COMPLETED;

  const anyRunning = waves.some((w) => w.status === PROVIDER_WAVE_STATUS.RUNNING);
  if (anyRunning) return PERIOD_STATUS.RUNNING;

  const anyStoppedCap = waves.some((w) => w.status === PROVIDER_WAVE_STATUS.STOPPED_COST_CAP);
  if (anyStoppedCap) return PERIOD_STATUS.STOPPED_COST_CAP;

  const anySuccess = waves.some((w) => w.successful > 0);
  const anyIncomplete = waves.some((w) => w.successful < 84 && w.successful > 0);
  if (anySuccess && anyIncomplete) return PERIOD_STATUS.PARTIAL;

  const allFailed = waves.every((w) => w.status === PROVIDER_WAVE_STATUS.FAILED);
  if (allFailed) return PERIOD_STATUS.FAILED;

  return period.status || PERIOD_STATUS.PLANNED;
}

/**
 * Idempotency unique key for observations within a period.
 * periodId + provider + semantic fingerprint (period NOT in semantic fingerprint).
 */
export function buildObservationUniqueKey(periodId, provider, semanticFingerprint) {
  return `${periodId}|${String(provider).toLowerCase()}|${semanticFingerprint}`;
}

export function recurringPeriodStoreRoot(rootDir) {
  return (
    rootDir ||
    path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime", "monitoring-periods")
  );
}

export function periodManifestPath(storeRoot, periodId) {
  return path.join(storeRoot, periodId, "period-manifest.json");
}

export function writePeriodManifest(period, rootDir) {
  const storeRoot = recurringPeriodStoreRoot(rootDir);
  const dir = path.join(storeRoot, period.periodId);
  fs.mkdirSync(dir, { recursive: true });
  const outPath = periodManifestPath(storeRoot, period.periodId);
  fs.writeFileSync(outPath, JSON.stringify(period, null, 2), "utf8");
  return outPath;
}

export function readPeriodManifest(periodId, rootDir) {
  const storeRoot = recurringPeriodStoreRoot(rootDir);
  const p = periodManifestPath(storeRoot, periodId);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function listPeriodManifests(rootDir) {
  const storeRoot = recurringPeriodStoreRoot(rootDir);
  if (!fs.existsSync(storeRoot)) return [];
  const entries = fs.readdirSync(storeRoot, { withFileTypes: true });
  const periods = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const manifest = readPeriodManifest(e.name, storeRoot);
    if (manifest) periods.push(manifest);
  }
  return periods.sort((a, b) => String(b.periodId).localeCompare(String(a.periodId)));
}

/**
 * Build immutable baseline period reference (does not mutate freeze).
 */
export function buildBaselinePeriodReference(freezeManifest) {
  return buildMonitoringPeriodSchema({
    periodId: BASELINE_PERIOD_ID,
    periodType: PERIOD_TYPE.BASELINE,
    periodPurpose: MONITORING_RUN_PURPOSE.BASELINE,
    periodNumber: 1,
    baselineFreezeId: freezeManifest?.freezeId || BASELINE_FREEZE_ID,
    status: PERIOD_STATUS.COMPLETED,
    completedAt: freezeManifest?.completedAt || null,
    successfulCalls: 336,
    providerWaves: Object.fromEntries(
      ["openai", "gemini", "perplexity", "claude"].map((p) => [
        p,
        {
          model: freezeManifest?.providers?.[p]?.model || PROVIDER_MODELS[p],
          waveId: freezeManifest?.providers?.[p]?.waveId || null,
          planned: 84,
          successful: 84,
          failed: 0,
          status: PROVIDER_WAVE_STATUS.COMPLETED,
          startedAt: freezeManifest?.providers?.[p]?.startedAt || null,
          completedAt: freezeManifest?.providers?.[p]?.completedAt || null,
        },
      ])
    ),
    TREND_AVAILABLE: false,
    SOURCE_CHANGE_AVAILABLE: false,
  });
}
