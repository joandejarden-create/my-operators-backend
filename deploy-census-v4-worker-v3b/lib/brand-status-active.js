/**
 * Brand Setup - Brand Basics "Brand Status" helpers (Explorer, Brand Library list).
 *
 * Canonical Brand Explorer active universe:
 *   OR({Brand Status}='Active', {Brand Status}='Live')
 * Loader / inventory: lib/partner-intelligence/brand-explorer-active-universe.js
 * Operational cohorts (PRIMARY_RELEASE, Lane 1/2, etc.) are NOT this universe.
 */

/** Airtable filterByFormula for Active + Live brands only. */
export const BRAND_STATUS_ACTIVE_FORMULA =
  "OR({Brand Status}='Active', {Brand Status}='Live')";

export function isBrandStatusActive(statusRaw) {
  let raw = statusRaw;
  if (Array.isArray(raw)) raw = raw[0];
  if (raw && typeof raw === "object" && raw.name != null) raw = raw.name;
  const s = String(raw ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  return s === "active" || s === "live";
}
