/**
 * Google Places review corrections for Peru — Arequipa / Paracas candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const PERU_AREQUIPA_PARACAS_GOOGLE_PLACE_REVIEW_CORRECTIONS = {};

export function applyPeruArequipaParacasPlaceReviewCorrection(point) {
  const fix = PERU_AREQUIPA_PARACAS_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyPeruArequipaParacasPlaceReviewCorrections(points) {
  return points.map(applyPeruArequipaParacasPlaceReviewCorrection);
}
