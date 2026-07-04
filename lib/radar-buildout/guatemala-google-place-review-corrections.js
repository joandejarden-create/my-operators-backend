/**
 * Google Places review corrections for Guatemala countrywide candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const GUATEMALA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyGuatemalaPlaceReviewCorrection(point) {
  const fix = GUATEMALA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyGuatemalaPlaceReviewCorrections(points) {
  return points.map(applyGuatemalaPlaceReviewCorrection);
}
