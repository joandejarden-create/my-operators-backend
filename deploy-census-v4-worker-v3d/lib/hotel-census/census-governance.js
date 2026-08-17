/**
 * Optional Hotel Census governance fields (Phase 1B).
 * Safe when columns are missing — probed once per process.
 */

import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";

let cachedGovernanceAvailability = null;

/**
 * @param {import('airtable').Base} base Platform Airtable base
 */
export async function getGovernanceFieldAvailability(base) {
  if (cachedGovernanceAvailability) return cachedGovernanceAvailability;

  const includeField = CENSUS_FIELDS.includeInBrandExplorer;
  const confidenceField = CENSUS_FIELDS.dataConfidence;

  let includeInBrandExplorer = false;
  let dataConfidence = false;

  for (const [key, fieldName] of [
    ["includeInBrandExplorer", includeField],
    ["dataConfidence", confidenceField],
  ]) {
    try {
      await base(HOTEL_CENSUS_TABLE).select({ fields: [fieldName], maxRecords: 1 }).firstPage();
      if (key === "includeInBrandExplorer") includeInBrandExplorer = true;
      else dataConfidence = true;
    } catch (err) {
      const msg = err?.message || String(err);
      if (!/unknown field|not found|invalid/i.test(msg)) throw err;
    }
  }

  cachedGovernanceAvailability = { includeInBrandExplorer, dataConfidence };
  return cachedGovernanceAvailability;
}

/** Reset probe cache (tests). */
export function resetGovernanceFieldCache() {
  cachedGovernanceAvailability = null;
}

/**
 * Phase 1B: include when checked or blank; exclude only when explicitly false.
 */
export function shouldIncludeRowForBrandExplorer(fields, includeFieldName, includeFieldExists) {
  if (!includeFieldExists || !includeFieldName) return true;
  const v = fields[includeFieldName];
  if (v === false) return false;
  if (v === true) return true;
  if (v == null || v === "") return true;
  const s = String(v).trim().toLowerCase();
  if (s === "false" || s === "no" || s === "0") return false;
  if (s === "true" || s === "yes" || s === "1") return true;
  return true;
}

export function governanceMeta(availability) {
  return {
    includeInBrandExplorerFieldPresent: !!availability?.includeInBrandExplorer,
    dataConfidenceFieldPresent: !!availability?.dataConfidence,
    includeInBrandExplorerRule:
      "Include when checked or blank/null; exclude only when explicitly false (Phase 1B).",
  };
}
