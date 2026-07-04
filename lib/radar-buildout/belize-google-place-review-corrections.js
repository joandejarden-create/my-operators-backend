/**
 * Google Places review corrections for Belize countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const BELIZE_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyBelizePlaceReviewCorrection(point) {
  const fix = BELIZE_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyBelizePlaceReviewCorrections(points) {
  return points.map(applyBelizePlaceReviewCorrection);
}
