/**
 * Google Places review corrections for Honduras candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const HONDURAS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyHondurasPlaceReviewCorrection(point) {
  const fix = HONDURAS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyHondurasPlaceReviewCorrections(points) {
  return points.map(applyHondurasPlaceReviewCorrection);
}
