/**
 * Google Places review corrections for El Salvador candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const EL_SALVADOR_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyElSalvadorPlaceReviewCorrection(point) {
  const fix = EL_SALVADOR_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyElSalvadorPlaceReviewCorrections(points) {
  return points.map(applyElSalvadorPlaceReviewCorrection);
}
