/**
 * Apify usage / cost tracking — operational metadata only.
 * Never mixed into authoritative Hotel Property Census fields.
 * Never stores tokens or credentials.
 */

export const APIFY_USAGE_VERSION = "hotel-intelligence-apify-usage-v1";

/** Dealality use cases for Apify Actor runs. */
export const APIFY_USE_CASES = Object.freeze({
  HOTEL_DISCOVERY: "HOTEL_DISCOVERY",
  HOTEL_IDENTITY: "HOTEL_IDENTITY",
  /** Full Tripadvisor hotel Profile Pack (rating/rank/amenities/contacts). */
  HOTEL_PROFILE: "HOTEL_PROFILE",
  ROOM_COUNT: "ROOM_COUNT",
  OWNER_INTELLIGENCE: "OWNER_INTELLIGENCE",
  LEGAL_ENTITY: "LEGAL_ENTITY",
  CONTACT_INTELLIGENCE: "CONTACT_INTELLIGENCE",
  MARKET_ALERT: "MARKET_ALERT",
  OPPORTUNITY_DISCOVERY: "OPPORTUNITY_DISCOVERY",
});

export const APIFY_AUTH_METHODS = Object.freeze({
  MCP: "mcp",
  LOCAL_TOKEN: "local_token",
  UNKNOWN: "unknown",
});

/** Cost source provenance for ledger rows. */
export const APIFY_COST_SOURCE = Object.freeze({
  APIFY_USAGE_TOTAL_USD: "apify_usage_total_usd",
  PPE_ESTIMATE: "ppe_estimate",
  MANUAL: "manual",
  UNKNOWN: "unknown",
});

/** Tripadvisor Actor defaults used by Hotel Intelligence. */
export const DEFAULT_TRIPADVISOR_ACTOR = Object.freeze({
  actor_id: "dbEyMBriog95Fv8CW",
  actor_name: "maxcopell/tripadvisor",
});
