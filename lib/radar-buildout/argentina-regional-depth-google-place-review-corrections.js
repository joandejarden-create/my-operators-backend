/**
 * Google Places review corrections for Argentina Regional Depth candidates (identity pass).
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const ARGENTINA_REGIONAL_DEPTH_GOOGLE_PLACE_REVIEW_CORRECTIONS = {
  "Uco Valley Wine Route": {
    googleSearchQuery: "Valle de Uco wine route Mendoza Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Potrerillos Dam Recreation Area": {
    googleSearchQuery: "Dique Potrerillos Mendoza Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Mendoza Convention and Expo Center": {
    googleSearchQuery: "Centro de Convenciones Mendoza Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Luján de Cuyo Wine Corridor": {
    googleSearchQuery: "Luján de Cuyo wine corridor Mendoza Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Cerro Catedral Ski Resort": {
    googleSearchQuery: "Cerro Catedral ski resort Bariloche Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Llao Llao Resort District": {
    googleSearchQuery: "Llao Llao Bariloche Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Circuito Chico Scenic Route": {
    googleSearchQuery: "Circuito Chico Bariloche Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Villa La Angostura Gateway": {
    googleSearchQuery: "Villa La Angostura Neuquén Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Hito Tres Fronteras": {
    googleSearchQuery: "Hito Tres Fronteras Puerto Iguazú Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Yriapú Nature Reserve": {
    googleSearchQuery: "Reserva Natural Yriapú Puerto Iguazú Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
  "Puerto Iguazú Duty-Free Corridor": {
    googleSearchQuery: "Puerto Iguazú duty free Argentina",
    manuallyVerified: true,
    reviewAction: "identity",
  },
};

export function applyArgentinaRegionalDepthPlaceReviewCorrection(point) {
  const fix = ARGENTINA_REGIONAL_DEPTH_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
  return merged;
}

export function applyArgentinaRegionalDepthPlaceReviewCorrections(points) {
  return points.map(applyArgentinaRegionalDepthPlaceReviewCorrection);
}
