/**
 * Google Places review corrections for Argentina countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const ARGENTINA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyArgentinaPlaceReviewCorrection(point) {
  const fix = ARGENTINA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyArgentinaPlaceReviewCorrections(points) {
  return points.map(applyArgentinaPlaceReviewCorrection);
}
