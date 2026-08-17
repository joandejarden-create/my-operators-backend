/**
 * Tripadvisor / Apify room-count enrichment — constants & config.
 * READ-ONLY toward census: never auto-writes Rooms / Keys.
 */

export const TRIPADVISOR_ROOMS_VERSION = "tripadvisor-apify-rooms-enrichment-v2";

export const MAP_PROVIDER_TRIPADVISOR_APIFY = "tripadvisor_apify";

/** Verification statuses for candidate room counts (local enrichment model). */
export const ROOMS_VERIFICATION_STATUS = Object.freeze({
  VERIFIED_PRIMARY_SOURCE: "VERIFIED_PRIMARY_SOURCE",
  VERIFIED_MULTI_SOURCE: "VERIFIED_MULTI_SOURCE",
  CANDIDATE_SINGLE_SOURCE: "CANDIDATE_SINGLE_SOURCE",
  CONFLICT_REVIEW_REQUIRED: "CONFLICT_REVIEW_REQUIRED",
  SOURCE_INDEPENDENCE_UNCERTAIN: "SOURCE_INDEPENDENCE_UNCERTAIN",
  UNRESOLVED: "UNRESOLVED",
  FALSE_MATCH_REJECTED: "FALSE_MATCH_REJECTED",
  /** Existing authoritative rooms — comparison only, never overwrite */
  AUTHORITATIVE_EXACT: "AUTHORITATIVE_COMPARE_EXACT",
  AUTHORITATIVE_NEAR: "AUTHORITATIVE_COMPARE_NEAR_MATCH",
  AUTHORITATIVE_CONFLICT: "AUTHORITATIVE_COMPARE_CONFLICT",
  AUTHORITATIVE_MISSING_TA: "AUTHORITATIVE_COMPARE_MISSING",
});

/** Comparison vs existing trusted census Rooms / Keys */
export const ROOM_COMPARE = Object.freeze({
  EXACT: "EXACT",
  NEAR_MATCH: "NEAR_MATCH",
  CONFLICT: "CONFLICT",
  MISSING: "MISSING",
  NO_MATCH: "NO_MATCH",
});

export const MATCH_CONFIG = Object.freeze({
  minNameSimilarity: 0.72,
  minAcceptScore: 0.82,
  maxGeoKmHard: 15,
  maxGeoKmPreferred: 3,
  sisterBrandConflictPct: 0.2,
  bannedQueryPatterns: [
    /^hotels?\s+in\s+/i,
    /^hoteles?\s+en\s+/i,
    /^hoteis?\s+em\s+/i,
  ],
});

export const PPE_USD = Object.freeze({
  FREE: 0.005,
  BRONZE: 0.0029,
  SILVER: 0.0025,
  GOLD: 0.002,
  PLATINUM: 0.0015,
  DIAMOND: 0.00125,
});

/** Estimated SerpApi incremental cost per search (order-of-magnitude). */
export const SERPAPI_USD_PER_SEARCH = 0.01;
