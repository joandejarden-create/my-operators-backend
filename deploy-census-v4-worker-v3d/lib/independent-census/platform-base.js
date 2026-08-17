/**
 * Airtable base accessor for independent census staging tables.
 *
 * Uses the same Platform base as Hotel Census (AIRTABLE_BASE_ID_ALT) but only
 * exposes staging table names. Does not perform any Hotel Census reads or writes.
 */

import Airtable from "airtable";
import {
  CANDIDATES_TABLE,
  EVIDENCE_TABLE,
  VERIFIED_TABLE,
  STAGING_TABLES,
} from "./fields.js";

let cachedBase = null;

/**
 * True when INDEPENDENT_CENSUS_PIPELINE_ENABLED is 1, true, or yes (case-insensitive).
 */
export function isIndependentCensusPipelineEnabled() {
  const v = String(process.env.INDEPENDENT_CENSUS_PIPELINE_ENABLED ?? "")
    .trim()
    .toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}

/**
 * Platform Airtable base for staging tables. Returns null if API key or base id missing.
 */
export function getIndependentCensusBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    return null;
  }
  if (!cachedBase) {
    cachedBase = new Airtable({ apiKey }).base(baseId);
  }
  return cachedBase;
}

/**
 * Express helper — 500 if Platform base config missing.
 */
export function ensureIndependentCensusBaseConfig(res) {
  if (getIndependentCensusBase()) return true;
  res.status(500).json({
    success: false,
    error:
      "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT for independent census staging",
  });
  return false;
}

/**
 * Express helper — 503 if pipeline flag is off (for future internal routes only).
 */
export function ensureIndependentCensusPipelineEnabled(res) {
  if (isIndependentCensusPipelineEnabled()) return true;
  res.status(503).json({
    success: false,
    error: "Independent census pipeline is disabled (INDEPENDENT_CENSUS_PIPELINE_ENABLED)",
  });
  return false;
}

/** Reset cached base (tests). */
export function resetIndependentCensusBaseCache() {
  cachedBase = null;
}

export {
  CANDIDATES_TABLE,
  EVIDENCE_TABLE,
  VERIFIED_TABLE,
  STAGING_TABLES,
};
