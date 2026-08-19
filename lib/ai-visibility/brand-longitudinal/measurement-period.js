/**
 * Brand longitudinal measurement period model + file storage.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomBytes } from "crypto";
import { COHORT_ID } from "./cohort-v1.js";
import { METRIC_VERSION } from "../config.js";

export const BRAND_LONGITUDINAL_PERIOD_VERSION = "brand_longitudinal_measurement_period_v1";

export const PERIOD_QUALITY_STATE = Object.freeze({
  VALID: "VALID",
  PARTIAL_PERIOD: "PARTIAL_PERIOD",
  FAILED: "FAILED",
  PLANNED: "PLANNED",
  RUNNING: "RUNNING",
});

/** >=95% planned calls successful for VALID period (documented threshold). */
export const VALID_PERIOD_SUCCESS_THRESHOLD = 0.95;

export const RETRY_POLICY = Object.freeze({
  maxRetriesPerCall: 1,
  maxAttemptsPerCall: 2,
  preserveFailedAttempt: true,
  oneSuccessfulObservationPerGrain: true,
  noRetryStorm: true,
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const BRAND_LONGITUDINAL_STORE_ROOT = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "data",
  "ai-visibility",
  "runtime",
  "brand-longitudinal"
);

export const DATASET_NAMESPACE = "demo_validation_multi_parent_brand_ai";

export function createBrandMeasurementPeriodId(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_brand_longitudinal_period_${y}${m}${d}_${rand}`;
}

/**
 * Build measurement period manifest schema.
 */
export function buildMeasurementPeriodManifest(input = {}) {
  return {
    version: BRAND_LONGITUDINAL_PERIOD_VERSION,
    measurementPeriodId: input.measurementPeriodId || createBrandMeasurementPeriodId(),
    cohortId: input.cohortId || COHORT_ID,
    cohortVersion: input.cohortVersion || "1.0.0",
    datasetNamespace: input.datasetNamespace || DATASET_NAMESPACE,
    environment: input.environment || "demo_validation",
    startedAt: input.startedAt || null,
    completedAt: input.completedAt || null,
    geography: input.geography || { key: "CALA", geographyScope: "Region", commercialRegion: "CALA" },
    language: input.language || "en",
    providerPanel: input.providerPanel || ["openai", "gemini", "perplexity", "claude"],
    promptCount: input.promptCount ?? 0,
    brandCount: input.brandCount ?? 19,
    plannedCalls: input.plannedCalls ?? 0,
    successfulCalls: input.successfulCalls ?? 0,
    failedCalls: input.failedCalls ?? 0,
    totalCostUsd: input.totalCostUsd ?? 0,
    modelMetadata: input.modelMetadata || {},
    comparabilityNotes: input.comparabilityNotes || [],
    qualityState: input.qualityState || PERIOD_QUALITY_STATE.PLANNED,
    grains: input.grains || [],
    observations: input.observations || [],
    successRate:
      input.plannedCalls > 0
        ? Number(((input.successfulCalls || 0) / input.plannedCalls).toFixed(4))
        : null,
    metricVersion: input.metricVersion || METRIC_VERSION,
    SYNTHETIC_HISTORY: 0,
    BACKDATED_HISTORY: 0,
  };
}

/**
 * Qualify period for trend use.
 */
export function qualifyMeasurementPeriod(period) {
  if (!period) {
    return { valid: false, qualityState: PERIOD_QUALITY_STATE.FAILED, reason: "missing_period" };
  }
  const rate =
    period.plannedCalls > 0 ? (period.successfulCalls || 0) / period.plannedCalls : 0;
  if (rate >= VALID_PERIOD_SUCCESS_THRESHOLD && period.qualityState !== PERIOD_QUALITY_STATE.PARTIAL_PERIOD) {
    return { valid: true, qualityState: PERIOD_QUALITY_STATE.VALID, successRate: rate };
  }
  return {
    valid: false,
    qualityState: PERIOD_QUALITY_STATE.PARTIAL_PERIOD,
    successRate: rate,
    reason: "below_success_threshold_or_partial",
  };
}

export function periodManifestPath(storeRoot, periodId) {
  return path.join(storeRoot, periodId, "period-manifest.json");
}

export function writeMeasurementPeriodManifest(period, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  fs.mkdirSync(path.join(storeRoot, period.measurementPeriodId), { recursive: true });
  const out = periodManifestPath(storeRoot, period.measurementPeriodId);
  fs.writeFileSync(out, JSON.stringify(period, null, 2), "utf8");
  return out;
}

export function readMeasurementPeriodManifest(periodId, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  const p = periodManifestPath(storeRoot, periodId);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function listMeasurementPeriodManifests(storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  if (!fs.existsSync(storeRoot)) return [];
  return fs
    .readdirSync(storeRoot, { withFileTypes: true })
    .filter((e) => e.isDirectory())
    .map((e) => readMeasurementPeriodManifest(e.name, storeRoot))
    .filter(Boolean)
    .sort((a, b) =>
      String(b.measurementPeriodId || "").localeCompare(String(a.measurementPeriodId || ""))
    );
}
