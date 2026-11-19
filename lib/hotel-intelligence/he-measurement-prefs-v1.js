/**
 * Hotel Explorer measurement conversion helpers (Node / tests).
 * Mirrors public/js/he-measurement-prefs-v1.js constants.
 */

export const HE_MEASUREMENT_PREFS_VERSION = "he-measurement-prefs-v1";
export const SQFT_TO_SQM = 0.09290304;
export const MI_TO_KM = 1.609344;

export function sqFtToSqM(sqFt) {
  const n = Number(sqFt);
  return Number.isFinite(n) ? n * SQFT_TO_SQM : null;
}

export function kmToMi(km) {
  const n = Number(km);
  return Number.isFinite(n) ? n / MI_TO_KM : null;
}

export function miToKm(mi) {
  const n = Number(mi);
  return Number.isFinite(n) ? n * MI_TO_KM : null;
}

/** Founder default search radii (miles) by location setting. */
export function defaultRadiusMilesForLocationType(locationType) {
  const loc = String(locationType || "").toLowerCase();
  // Order matters: "suburban" contains the substring "urban".
  if (loc.includes("suburban")) return 10;
  if (loc.includes("airport")) return 10;
  if (loc.includes("resort") || loc.includes("destination")) return 30;
  if (loc.includes("rural") || loc.includes("remote")) return 25;
  if (loc.includes("urban") || loc.includes("city center") || loc.includes("city-center")) {
    return 5;
  }
  return 10;
}

export function defaultRadiusKmForLocationType(locationType) {
  return miToKm(defaultRadiusMilesForLocationType(locationType));
}

/**
 * Sort key must use source numeric units (sq ft / km), never formatted display strings.
 */
export function areaSortValueSqFt(rawSqFt) {
  const n = Number(String(rawSqFt ?? "").replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : null;
}

export function distanceSortValueKm(rawKm) {
  const n = Number(rawKm);
  return Number.isFinite(n) ? n : null;
}
