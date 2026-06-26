/**
 * Google Places review corrections for Chile — Valparaíso / Patagonia candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const CHILE_VALPARAISO_PATAGONIA_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyChileValparaisoPatagoniaPlaceReviewCorrection(point) {
  const fix = CHILE_VALPARAISO_PATAGONIA_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyChileValparaisoPatagoniaPlaceReviewCorrections(points) {
  return points.map(applyChileValparaisoPatagoniaPlaceReviewCorrection);
}
