/**
 * Census Autopilot V2 — Full LATAM/Caribbean Census Factory
 * Extends V1/Golden/SerpApi/Cvent harvest. No Airtable. No Webhound. No StayingAPI spend.
 */

export const AUTOPILOT_V2_VERSION = "census-autopilot-v2-full-universe";

export const OUT_REL = "data/research-engine-v2/census-autopilot-v2-full-universe";

export const CANDIDATE_ORIGINS = Object.freeze({
  CVENT_CHALLENGE: "CVENT_CHALLENGE",
  VERIFIED_INDEPENDENT: "VERIFIED_INDEPENDENT",
  OFFICIAL_BRAND_DIRECTORY: "OFFICIAL_BRAND_DIRECTORY",
  OTHER_INDEPENDENT: "OTHER_INDEPENDENT",
});

export const CLASSIFICATION = Object.freeze({
  EXISTING_VERIFIED_PROPERTY: "EXISTING VERIFIED PROPERTY",
  EXISTING_NEEDS_ENRICHMENT: "EXISTING PROPERTY — NEEDS ENRICHMENT",
  PROBABLE_DUPLICATE: "PROBABLE DUPLICATE",
  NEW_PROPERTY_CANDIDATE: "NEW PROPERTY CANDIDATE",
  IDENTITY_CONFLICT: "IDENTITY CONFLICT",
  INSUFFICIENT_IDENTITY: "INSUFFICIENT IDENTITY",
  NON_HOTEL_EXCLUDED: "NON-HOTEL / EXCLUDED TYPE",
});

export const OPERATING_STATES = Object.freeze({
  VERIFIED_95_PRODUCTION_CANDIDATE: "VERIFIED ≥95 — PRODUCTION CANDIDATE",
  VERIFIED_LT95_REMEDIATION: "VERIFIED <95 — MATERIAL REMEDIATION",
  PARTIAL_RESEARCH_CONTINUES: "PARTIAL — RESEARCH CONTINUES",
  FIRST_PARTY_VALIDATION_NEEDED: "FIRST-PARTY VALIDATION NEEDED",
  DEEP_RESEARCH_ESCALATION: "DEEP RESEARCH ESCALATION",
  IDENTITY_REVIEW: "IDENTITY REVIEW",
  RIGHTS_BLOCKED: "RIGHTS BLOCKED",
  EXCLUDED_NON_HOTEL: "EXCLUDED / NON-HOTEL",
});

export const PRIORITY = Object.freeze({
  P0: "P0",
  P1: "P1",
  P2: "P2",
  P3: "P3",
  P4: "P4",
  P5: "P5",
});

/** Default Phase B ceiling: min(500, 25% available searches) */
export function computeSerpApiPhaseBCeiling(availableSearches) {
  const hard = Number(process.env.SERPAPI_V2_PHASE_B_CEILING || 500);
  const pct = Math.floor(Number(availableSearches || 0) * 0.25);
  if (!availableSearches || availableSearches <= 0) return Math.min(hard, 50);
  return Math.max(5, Math.min(hard, pct, availableSearches - 5));
}

export const BRAND_FAMILY_ADAPTER = Object.freeze({
  IHG: "NATIVE_STRONG",
  Hilton: "NATIVE_STRONG",
  Choice: "NATIVE_STRONG",
  Marriott: "NATIVE_PARTIAL",
  Accor: "NO_ADAPTER",
  Wyndham: "NO_ADAPTER",
  Hyatt: "NO_ADAPTER",
  Minor: "NO_ADAPTER",
  Radisson: "NO_ADAPTER",
  Melia: "NO_ADAPTER",
  Independent: "LONG_TAIL_INDEPENDENT",
  Unknown: "LONG_TAIL_INDEPENDENT",
});

export const SERPAPI_ALLOWED_FIELDS = Object.freeze([
  "Property Name",
  "Address",
  "City",
  "State / Region",
  "Country",
  "Postal Code",
  "Latitude",
  "Longitude",
  "Phone",
  "Official Property URL",
  "Amenities",
  "Hotel Class (raw)",
  "Property Type (input)",
  "Google property_token",
]);

export const SERPAPI_PROHIBITED_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Owner Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Market",
  "Submarket",
  "production images",
]);
