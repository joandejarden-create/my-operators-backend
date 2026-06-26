/**
 * Google Places review corrections for Nicaragua candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const NICARAGUA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyNicaraguaPlaceReviewCorrection(point) {
  const fix = NICARAGUA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyNicaraguaPlaceReviewCorrections(points) {
  return points.map(applyNicaraguaPlaceReviewCorrection);
}
