/**
 * Dealality Hotel Intelligence — public library entry.
 */

export { MAP_HOTEL_INTELLIGENCE_VERSION, MAP_CENSUS_FIELDS, MAP_PROVIDER_IDS } from "./map_hotel_intelligence_fields.js";
export {
  generateDealalityHotelId,
  isDealalityHotelId,
  createEmptyCanonicalHotel,
  toMvpHotelSummary,
} from "./canonical-hotel.js";
export { createLocalStore } from "./local-store.js";
export { createExternalIdRegistry } from "./external-ids.js";
export { createEvidenceStore } from "./evidence-store.js";
export {
  scoreFieldConfidence,
  preferCanonicalValue,
  CONFIDENCE_TIERS,
  SOURCE_FIELD_AUTHORITY,
} from "./confidence.js";
export { resolveHotelIdentity, MATCH_STATUS } from "./identity-resolve.js";
export { createReviewQueue, ISSUE_TYPES } from "./review-queue.js";
export { findNearbyHotels } from "./nearby.js";
export { createBatchJobStore, BATCH_STATUS } from "./batch-jobs.js";
export { createHotelbedsProvider } from "./providers/hotelbeds.js";
export { createStayingApiProvider, STAYINGAPI_ROOMS_CAPABILITY } from "./providers/stayingapi.js";
export {
  createSerpApiProvider,
  SERPAPI_ROOMS_CAPABILITY,
} from "./providers/serpapi.js";
export {
  createGiataDriveProvider,
  GIATA_DRIVE_ROOMS_CAPABILITY_STATUS,
  PROVIDER_ID as GIATA_DRIVE_PROVIDER_ID,
} from "./providers/giata-drive.js";
export {
  createGiataDriveSyncStore,
  GIATA_OPEN_CONTENT_REMOVED,
} from "./providers/giata-drive-sync.js";
export { createCensusReadProvider } from "./providers/census-read.js";
export { createProviderRegistry } from "./providers/registry.js";
export { createHotelIntelligenceService } from "./orchestration/service.js";
export {
  researchHotelRoomCount,
  ROOM_COUNT_RESEARCH_VERSION,
  RESEARCH_STATUS,
  extractRoomCountsFromText,
  SOURCE_CATEGORIES,
} from "./room-count-research/index.js";
export {
  APIFY_USAGE_VERSION,
  APIFY_USE_CASES,
  APIFY_AUTH_METHODS,
  APIFY_COST_SOURCE,
  createApifyUsageStore,
  buildApifyUsageRecord,
  fromApifyRunPayload,
  extractApifyRunCost,
  summarizeApifyUsage,
  recordTripadvisorRoomCountRun,
} from "./apify-usage/index.js";
export {
  TRIPADVISOR_PROFILE_INTEL_VERSION,
  competitiveRankPercentile,
  buildOwnerCompSnapshot,
  amenityGapsVsComps,
  ratingHistogramShares,
} from "./tripadvisor-profile/index.js";

/** Future MCP tool contracts (not implemented in V1). */
export const FUTURE_TOOL_CONTRACTS = Object.freeze([
  "market_hotels",
  "market_supply_summary",
  "brand_market_presence",
  "brand_whitespace",
  "brand_saturation",
  "competitive_set_generate",
  "owner_get",
  "owner_portfolio",
  "operator_get",
  "operator_portfolio",
  "hotel_history",
  "hotel_reflags",
  "pipeline_search",
  "opportunity_search",
]);
