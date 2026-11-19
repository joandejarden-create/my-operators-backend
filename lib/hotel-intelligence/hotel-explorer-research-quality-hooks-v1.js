/**
 * Hooks that bind Hotel Explorer research / population workflows to the
 * permanent quality standard (descriptions, amenities, rooms, evidence).
 *
 * Import from research engines and preview builders — do not fork hotel-specific copy.
 */
import {
  resolveCommercialDescription,
  HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
} from "./hotel-commercial-description-v1.js";

export const HE_RESEARCH_QUALITY_HOOKS_VERSION = "he-research-quality-hooks-v1";
export const HE_QUALITY_STANDARD_DOC =
  "docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md";
export const HE_QUALITY_LESSONS_REGISTRY =
  "docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json";

/**
 * Attach a quality-gated commercial description onto a hotel payload (non-destructive).
 */
export function applyCommercialDescriptionStandard(hotel = {}, opts = {}) {
  const resolved = resolveCommercialDescription(hotel, opts);
  return {
    ...hotel,
    hotelDescriptionResolved: resolved.text,
    hotelDescriptionMeta: {
      version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
      source: resolved.source,
      lowQuality: resolved.lowQuality,
      standardDoc: HE_QUALITY_STANDARD_DOC,
    },
  };
}

/**
 * Guard: proposed / staged facts must never be labeled as live Airtable persistence.
 */
export function assertProposedNotPersisted(previewState) {
  const s = String(previewState || "").toUpperCase();
  if (s.includes("PROPOSED") || s.includes("STAGED")) {
    return {
      ok: true,
      uiLabel: "Proposed · not persisted",
      airtablePersisted: false,
    };
  }
  if (s === "LIVE" || s === "AIRTABLE" || s === "CANONICAL") {
    return { ok: true, uiLabel: "Live Airtable", airtablePersisted: true };
  }
  return { ok: true, uiLabel: "Unknown", airtablePersisted: false };
}

export function qualityHooksMeta() {
  return {
    version: HE_RESEARCH_QUALITY_HOOKS_VERSION,
    standardDoc: HE_QUALITY_STANDARD_DOC,
    lessonsRegistry: HE_QUALITY_LESSONS_REGISTRY,
    descriptionVersion: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
    gate: "npm run test:he-product-quality-gates-v1",
  };
}
