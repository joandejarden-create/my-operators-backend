/**
 * Brand Setup - Brand Basics "Brand Status" helpers (Explorer, Brand Library list).
 */

/** Airtable filterByFormula for Active + Live brands only. */
export const BRAND_STATUS_ACTIVE_FORMULA =
  "OR({Brand Status}='Active', {Brand Status}='Live')";

export function isBrandStatusActive(statusRaw) {
  const s = String(statusRaw ?? "").trim().toLowerCase();
  return s === "active" || s === "live";
}
