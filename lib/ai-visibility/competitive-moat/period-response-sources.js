/**
 * Period-tagged response loading for benchmark recertification.
 * Baseline corpus = DEMO_VALIDATION. Longitudinal periods loaded separately.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  collectStoredResponses,
  DEFAULT_RESPONSE_DIRS,
  DATASET_NAMESPACE,
} from "./presence-corpus.js";
import { BASELINE_MEASUREMENT_PERIOD } from "./period-scoped-grain.js";
import {
  BRAND_LONGITUDINAL_STORE_ROOT,
  listMeasurementPeriodManifests,
} from "../brand-longitudinal/measurement-period.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");

function normalizeLongitudinalResponse(raw, fileName, sourceDir, measurementPeriodId) {
  const text =
    raw.text ||
    raw.responseText ||
    raw.rawText ||
    (raw.response && (raw.response.text || raw.response.content)) ||
    "";
  if (!text) return null;
  return {
    fileName,
    sourceDir,
    responseId: raw.responseId || raw.slotId || fileName.replace(/\.json$/, ""),
    waveId: raw.waveId || raw.batchId || measurementPeriodId,
    runId: raw.runId || null,
    provider: raw.provider || raw.providerMeta?.provider || "unknown",
    model: raw.model || raw.providerMeta?.model || null,
    promptId: raw.promptId || null,
    promptVersion: raw.promptVersion || "1",
    language: raw.language || null,
    geography: raw.geography || raw.commercialRegion || "CALA",
    intentTerritory: raw.intentTerritory || null,
    timestamp: raw.timestamp || raw.completedAt || raw.capturedAt || null,
    text: String(text),
    datasetNamespace: raw.datasetNamespace || DATASET_NAMESPACE,
    measurementPeriodId,
  };
}

/**
 * Load responses stored under a brand longitudinal period directory.
 */
export function collectLongitudinalPeriodResponses(periodId, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  const dir = path.join(storeRoot, periodId, "responses");
  const records = [];
  if (!fs.existsSync(dir)) return records;
  for (const file of fs.readdirSync(dir)) {
    if (!file.endsWith(".json")) continue;
    try {
      const raw = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
      const rec = normalizeLongitudinalResponse(raw, file, dir, periodId);
      if (rec) records.push(rec);
    } catch {
      /* skip malformed */
    }
  }
  return records;
}

/**
 * Baseline frozen-corpus responses tagged with DEMO_VALIDATION period.
 */
export function collectBaselineResponses(responseDirs = DEFAULT_RESPONSE_DIRS) {
  return collectStoredResponses(responseDirs).map((rec) => ({
    ...rec,
    measurementPeriodId: BASELINE_MEASUREMENT_PERIOD,
  }));
}

/**
 * All available period slices — each kept independent (never merged for index math).
 */
export function listAvailableMeasurementPeriods(opts = {}) {
  const storeRoot = opts.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT;
  const periods = [{ measurementPeriodId: BASELINE_MEASUREMENT_PERIOD, source: "baseline_corpus", kind: "FROZEN_CERTIFIED_BASELINE" }];
  for (const manifest of listMeasurementPeriodManifests(storeRoot)) {
    if (!manifest?.measurementPeriodId) continue;
    periods.push({
      measurementPeriodId: manifest.measurementPeriodId,
      source: "brand_longitudinal",
      kind: "LONGITUDINAL_PERIOD",
      qualityState: manifest.qualityState || null,
      completedAt: manifest.completedAt || null,
    });
  }
  return periods.sort((a, b) => String(a.measurementPeriodId).localeCompare(String(b.measurementPeriodId)));
}

/**
 * Load responses for exactly one measurement period.
 */
export function collectResponsesForPeriod(measurementPeriodId, opts = {}) {
  if (!measurementPeriodId || measurementPeriodId === BASELINE_MEASUREMENT_PERIOD) {
    return collectBaselineResponses(opts.responseDirs || DEFAULT_RESPONSE_DIRS);
  }
  return collectLongitudinalPeriodResponses(measurementPeriodId, opts.storeRoot);
}

/**
 * Load all periods as independent slices (for audit / readiness — not for pooled index).
 */
export function collectAllPeriodSlices(opts = {}) {
  const slices = new Map();
  slices.set(BASELINE_MEASUREMENT_PERIOD, collectBaselineResponses(opts.responseDirs));
  for (const p of listAvailableMeasurementPeriods(opts)) {
    if (p.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD) continue;
    const recs = collectLongitudinalPeriodResponses(p.measurementPeriodId, opts.storeRoot);
    if (recs.length) slices.set(p.measurementPeriodId, recs);
  }
  return slices;
}
