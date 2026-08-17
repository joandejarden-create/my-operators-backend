/**
 * Independent census source profiles — policy metadata per upstream source.
 * Phase 2D: framework only; no ingest or Airtable writes.
 */

import { SOURCE_TYPES } from "./fields.js";

/** @typedef {'discovery'|'enrichment'|'verification'|'lookup'|'submitted'} InitialUse */
/** @typedef {'yes'|'no'|'review_required'|'restricted_refresh_required'|'conditional'} ProductUse */
/** @typedef {'low'|'medium'|'high'} RiskLevel */

/**
 * @typedef {object} SourceProfile
 * @property {string} sourceName
 * @property {string} sourceType
 * @property {string} sourceLicense
 * @property {InitialUse} initialUse
 * @property {ProductUse} canUseInProduct
 * @property {boolean} requiresAttribution
 * @property {boolean} requiresManualReview
 * @property {boolean} [requiresRefresh]
 * @property {boolean} canShowToUsers
 * @property {boolean} canUseForScoring
 * @property {RiskLevel} riskLevel
 * @property {string} notes
 * @property {string[]} [allowedStorageNotes]
 */

/** @type {Record<string, SourceProfile>} */
export const SOURCE_PROFILES = {
  [SOURCE_TYPES.OSM]: {
    sourceName: "OpenStreetMap",
    sourceType: SOURCE_TYPES.OSM,
    sourceLicense: "ODbL",
    initialUse: "discovery",
    canUseInProduct: "review_required",
    requiresAttribution: true,
    requiresManualReview: true,
    requiresRefresh: false,
    canShowToUsers: false,
    canUseForScoring: false,
    riskLevel: "medium",
    notes:
      "Useful for location discovery, coordinates, and open POI coverage; weak on city, website, and phone in many markets.",
    allowedStorageNotes: [
      "Store OSM element id and attribution.",
      "Do not treat OSM tags as verified brand or operator facts without corroboration.",
    ],
  },

  [SOURCE_TYPES.WIKIDATA]: {
    sourceName: "Wikidata",
    sourceType: SOURCE_TYPES.WIKIDATA,
    sourceLicense: "CC0",
    initialUse: "enrichment",
    canUseInProduct: "yes",
    requiresAttribution: false,
    requiresManualReview: true,
    requiresRefresh: false,
    canShowToUsers: true,
    canUseForScoring: false,
    riskLevel: "low",
    notes:
      "Useful for notable hotels, historic assets, official websites, coordinates, and operators/owners when available; incomplete coverage.",
    allowedStorageNotes: [
      "Track Wikidata Q-id as Source Record ID.",
      "Optional source line in UI; CC0 generally permissive.",
    ],
  },

  [SOURCE_TYPES.BRAND_DIRECTORY]: {
    sourceName: "Brand Directory",
    sourceType: SOURCE_TYPES.BRAND_DIRECTORY,
    sourceLicense: "source_specific_terms",
    initialUse: "verification",
    canUseInProduct: "review_required",
    requiresAttribution: true,
    requiresManualReview: true,
    requiresRefresh: false,
    canShowToUsers: false,
    canUseForScoring: false,
    riskLevel: "medium",
    notes:
      "Official brand/property lists (Marriott, Choice, Hilton, Hyatt, IHG, Accor, etc.). Manual CSV/JSON import only in early phases; no automatic scraping without legal review per source.",
    allowedStorageNotes: [
      "Record source brand site and import file version.",
      "Honor each directory's terms for display and retention.",
    ],
  },

  [SOURCE_TYPES.GOVERNMENT_REGISTRY]: {
    sourceName: "Government Registry",
    sourceType: SOURCE_TYPES.GOVERNMENT_REGISTRY,
    sourceLicense: "jurisdiction_specific",
    initialUse: "verification",
    canUseInProduct: "review_required",
    requiresAttribution: true,
    requiresManualReview: true,
    requiresRefresh: false,
    canShowToUsers: false,
    canUseForScoring: false,
    riskLevel: "medium",
    notes:
      "Official lodging/license registers by country or municipality. Country-specific adapters; strong for legal name, license status, and room counts when published.",
    allowedStorageNotes: [
      "Store jurisdiction, registry id, and license snapshot date.",
      "Do not assume cross-border harmonization of fields.",
    ],
  },

  google_places: {
    sourceName: "Google Places",
    sourceType: "google_places",
    sourceLicense: "Google Maps Platform Terms",
    initialUse: "lookup",
    canUseInProduct: "restricted_refresh_required",
    requiresAttribution: true,
    requiresManualReview: true,
    requiresRefresh: true,
    canShowToUsers: false,
    canUseForScoring: false,
    riskLevel: "high",
    notes:
      "Identity and enrichment lookup only — not the permanent master census. Store Place ID where permitted. No photos/reviews. Do not treat Google lat/lng as permanent master coordinates unless terms allow. No API calls in Phase 2D.",
    allowedStorageNotes: [
      "Store google_place_id with refresh timestamp if API used later.",
      "Do not persist restricted fields (photos, reviews, ratings) without terms review.",
      "Refresh on schedule; do not use as sole verification source.",
    ],
  },

  submitted: {
    sourceName: "Owner / Brand / Operator Submitted",
    sourceType: "submitted",
    sourceLicense: "user_submitted",
    initialUse: "submitted",
    canUseInProduct: "conditional",
    requiresAttribution: false,
    requiresManualReview: true,
    requiresRefresh: false,
    canShowToUsers: true,
    canUseForScoring: true,
    riskLevel: "low",
    notes:
      "Highest-confidence long-term source when submitted by the relevant owner, brand, or operator under platform permissions.",
    allowedStorageNotes: [
      "Link to submitting account and permission scope.",
      "Conditional product use subject to platform terms.",
    ],
  },

  [SOURCE_TYPES.MANUAL_UPLOAD]: {
    sourceName: "Manual Upload",
    sourceType: SOURCE_TYPES.MANUAL_UPLOAD,
    sourceLicense: "internal_review",
    initialUse: "verification",
    canUseInProduct: "review_required",
    requiresAttribution: false,
    requiresManualReview: true,
    requiresRefresh: false,
    canShowToUsers: false,
    canUseForScoring: false,
    riskLevel: "medium",
    notes: "Ops-curated CSV/JSON or one-off research imports pending source classification.",
    allowedStorageNotes: ["Document importer and file hash in evidence table."],
  },
};

export function listSourceProfiles() {
  return Object.values(SOURCE_PROFILES);
}

export function getSourceProfile(sourceType) {
  const key = normalizeSourceTypeKey(sourceType);
  return SOURCE_PROFILES[key] || null;
}

export function normalizeSourceTypeKey(sourceType) {
  return String(sourceType ?? "")
    .trim()
    .toLowerCase();
}

export function sourcePolicySummaryForReport() {
  return listSourceProfiles().map((p) => ({
    sourceType: p.sourceType,
    sourceName: p.sourceName,
    sourceLicense: p.sourceLicense,
    initialUse: p.initialUse,
    canUseInProduct: p.canUseInProduct,
    requiresAttribution: p.requiresAttribution,
    requiresManualReview: p.requiresManualReview,
    requiresRefresh: !!p.requiresRefresh,
    canShowToUsers: p.canShowToUsers,
    canUseForScoring: p.canUseForScoring,
    riskLevel: p.riskLevel,
  }));
}
