/**
 * Census Autopilot V2.3 — Independent Universe Discovery + Cvent Decoupling
 * DRY-RUN ONLY. No Airtable. No Webhound. Cvent never production evidence.
 */

export const AUTOPILOT_V23_VERSION = "census-autopilot-v2.3-independent-universe";

export const OUT_REL = "data/research-engine-v2/census-autopilot-v2-3-independent-universe";

export const V22_OUT_REL = "data/research-engine-v2/census-autopilot-v2-2-official-first-rooms";

/** Pilot countries — country labels only; no Cvent hotel records. */
export const PILOT_COUNTRIES = Object.freeze([
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Colombia",
  "Brazil",
  "Argentina",
  "Jamaica",
  "Barbados", // smaller Caribbean market
]);

export const DISCOVERY_LANES = Object.freeze({
  A_OFFICIAL_BRAND: "LANE_A_OFFICIAL_BRAND_DIRECTORY",
  B_SOFT_COLLECTION: "LANE_B_SOFT_BRAND_COLLECTION",
  C_INDEPENDENT: "LANE_C_INDEPENDENT_DISCOVERY",
  D_LONG_TAIL: "LANE_D_LONG_TAIL_HARD",
});

export const MATCH_CLASS = Object.freeze({
  BOTH: "BOTH",
  INDEPENDENT_ONLY: "INDEPENDENT_ONLY",
  CVENT_ONLY: "CVENT_ONLY",
  PROBABLE_MATCH: "PROBABLE_MATCH",
  IDENTITY_CONFLICT: "IDENTITY_CONFLICT",
  NON_HOTEL_OR_DUPLICATE: "NON_HOTEL_OR_DUPLICATE",
});

export const VERIFIED_STATES = Object.freeze({
  DISCOVERED: "DISCOVERED",
  IDENTITY_VERIFIED: "IDENTITY VERIFIED",
  VERIFIED_MATERIAL_GAPS: "VERIFIED — MATERIAL GAPS",
  VERIFIED_ROOMS_PENDING: "VERIFIED — ROOMS PENDING",
  VERIFIED_FIRST_PARTY_PENDING: "VERIFIED — FIRST-PARTY VALIDATION PENDING",
  VERIFIED_GOLDEN_COMPLETE: "VERIFIED — GOLDEN COMPLETE",
  RESEARCH_ESCALATION: "RESEARCH ESCALATION",
  INACTIVE_HISTORICAL: "INACTIVE / HISTORICAL",
  IDENTITY_CONFLICT: "IDENTITY CONFLICT",
});

/** Nuanced SerpApi rights dimensions — not a binary RIGHTS_BLOCKED flag. */
export const SERPAPI_RIGHTS_DIMENSIONS = Object.freeze([
  "COLLECTION_ALLOWED",
  "RESEARCH_ALLOWED",
  "PERSISTENCE_POSITION",
  "CUSTOMER_DISPLAY_POSITION",
  "DERIVED_DATA_POSITION",
  "DOWNSTREAM_USE_CUSTOMER_RESPONSIBILITY",
  "IMAGE_REUSE_POSITION",
]);
