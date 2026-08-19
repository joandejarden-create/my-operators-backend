/**
 * Canonical longitudinal measurement grain for Brand AI Intelligence.
 * PROMPT × BRAND × PROVIDER × MODEL × GEOGRAPHY × LANGUAGE × MEASUREMENT DATE
 */

import { createHash } from "crypto";

export const BRAND_LONGITUDINAL_GRAIN_VERSION = "brand_longitudinal_grain_v1";

export const LONGITUDINAL_GRAIN_FIELDS = Object.freeze([
  "promptId",
  "promptVersion",
  "promptTextHash",
  "scenarioId",
  "promptOrigin",
  "brandId",
  "provider",
  "providerModel",
  "geographyKey",
  "geographyScope",
  "commercialRegion",
  "language",
  "measurementDate",
  "measurementPeriodId",
  "runId",
  "observationId",
]);

/**
 * Normalize ISO timestamp to UTC calendar date (measurement period date).
 * Same-day repeats share one trend date.
 */
export function normalizeMeasurementDate(isoTimestamp) {
  if (!isoTimestamp) return null;
  const s = String(isoTimestamp).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

/**
 * Build prompt text hash for version tracking (does not replace promptVersion).
 */
export function hashPromptText(promptText) {
  if (!promptText) return null;
  return createHash("sha256").update(String(promptText).trim()).digest("hex").slice(0, 16);
}

/**
 * Build canonical grain key string for deduplication within a measurement period.
 */
export function buildLongitudinalGrainKey(parts = {}) {
  return [
    parts.promptId || "",
    parts.promptVersion != null ? String(parts.promptVersion) : "",
    parts.brandId || "",
    String(parts.provider || "").toLowerCase(),
    parts.providerModel || "",
    parts.geographyKey || "",
    parts.language || "",
    parts.measurementDate || "",
  ].join("|");
}

/**
 * Build full observation grain record.
 */
export function buildLongitudinalGrainRecord(parts = {}) {
  const measurementDate =
    parts.measurementDate || normalizeMeasurementDate(parts.timestamp || parts.completedAt);
  return {
    version: BRAND_LONGITUDINAL_GRAIN_VERSION,
    promptId: parts.promptId || null,
    promptVersion: parts.promptVersion != null ? String(parts.promptVersion) : null,
    promptTextHash: parts.promptTextHash || hashPromptText(parts.promptText) || null,
    scenarioId: parts.scenarioId || null,
    promptOrigin: parts.promptOrigin || null,
    brandId: parts.brandId || null,
    provider: parts.provider ? String(parts.provider).toLowerCase() : null,
    providerModel: parts.providerModel || parts.model || null,
    geographyKey: parts.geographyKey || null,
    geographyScope: parts.geographyScope || null,
    commercialRegion: parts.commercialRegion || null,
    language: parts.language || null,
    measurementDate,
    measurementPeriodId: parts.measurementPeriodId || null,
    runId: parts.runId || null,
    observationId: parts.observationId || null,
    grainKey: buildLongitudinalGrainKey({ ...parts, measurementDate }),
  };
}

export const SYNTHETIC_HISTORY_FORBIDDEN = Object.freeze({
  SYNTHETIC_HISTORY: 0,
  BACKDATED_HISTORY: 0,
  SAME_DAY_AS_SEPARATE_TREND: 0,
  INTERPOLATED_POINTS: 0,
  STAGE_B_REPETITION_AS_TREND_PERIOD: 0,
});
