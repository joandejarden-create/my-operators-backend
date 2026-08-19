/**
 * Brand resolution metrics — affiliation status separate from Current Brand fill rate.
 * BRAND_RESOLUTION_RATE = (BRANDED_VALIDATED + INDEPENDENT_VALIDATED) / TOTAL
 */
import { MAP_MASTER } from "./master-census-enrichment-v1.js";

export const BRAND_RESOLUTION_METRICS_VERSION = "brand-resolution-metrics-v1";

export const BRAND_RESOLUTION_CLASS = Object.freeze({
  BRANDED_VALIDATED: "BRANDED_VALIDATED",
  INDEPENDENT_VALIDATED: "INDEPENDENT_VALIDATED",
  BRAND_UNRESOLVED: "BRAND_UNRESOLVED",
});

const VALIDATED_AFFILIATION_BRANDED = new Set([
  "Branded",
  "Soft-Branded / Collection",
  "Formerly Branded",
]);

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

/**
 * Classify one Census row into brand resolution state (metrics only).
 * Current Brand populated → BRANDED_VALIDATED.
 * Affiliation Status Independent (validated lane) → INDEPENDENT_VALIDATED.
 * Otherwise → BRAND_UNRESOLVED.
 */
export function classifyBrandResolution(fields = {}) {
  if (!isBlank(fields[MAP_MASTER.currentBrand])) {
    return BRAND_RESOLUTION_CLASS.BRANDED_VALIDATED;
  }
  const aff = String(fields["Affiliation Status"] || "").trim();
  if (aff === "Independent") {
    return BRAND_RESOLUTION_CLASS.INDEPENDENT_VALIDATED;
  }
  if (VALIDATED_AFFILIATION_BRANDED.has(aff) && !isBlank(fields[MAP_MASTER.currentBrand])) {
    return BRAND_RESOLUTION_CLASS.BRANDED_VALIDATED;
  }
  return BRAND_RESOLUTION_CLASS.BRAND_UNRESOLVED;
}

/**
 * @param {object[]} records
 */
export function computeBrandResolutionMetrics(records = []) {
  let branded = 0;
  let independent = 0;
  let unresolved = 0;
  for (const rec of records) {
    const cls = classifyBrandResolution(rec.fields || {});
    if (cls === BRAND_RESOLUTION_CLASS.BRANDED_VALIDATED) branded += 1;
    else if (cls === BRAND_RESOLUTION_CLASS.INDEPENDENT_VALIDATED) independent += 1;
    else unresolved += 1;
  }
  const total = records.length;
  const resolved = branded + independent;
  const rate = total > 0 ? Number(((resolved / total) * 100).toFixed(2)) : 0;
  return {
    TOTAL_PROPERTIES: total,
    BRANDED_VALIDATED: branded,
    INDEPENDENT_VALIDATED: independent,
    BRAND_UNRESOLVED: unresolved,
    BRAND_RESOLUTION_RATE: rate,
  };
}

export function countBlankCurrentBrand(records = []) {
  return records.filter((r) => isBlank(r.fields?.[MAP_MASTER.currentBrand])).length;
}
