/**
 * Tripadvisor / Apify room-count enrichment (read-only census path).
 */

export {
  TRIPADVISOR_ROOMS_VERSION,
  MAP_PROVIDER_TRIPADVISOR_APIFY,
  ROOMS_VERIFICATION_STATUS,
  ROOM_COMPARE,
  MATCH_CONFIG,
  PPE_USD,
  SERPAPI_USD_PER_SEARCH,
} from "./constants.js";

export {
  buildTripadvisorResolutionPlan,
  buildTripadvisorActorInput,
  assertNotBannedDestinationQuery,
  TRIPADVISOR_QUERY_URLS_VERSION,
} from "./query-urls.js";

export {
  matchTripadvisorHotel,
  classifyRoomCompare,
  usableTripadvisorRooms,
  isHotelItem,
  nameSimilarity,
  sisterBrandCollision,
  TRIPADVISOR_MATCH_VERSION,
} from "./match.js";

export {
  verifyTripadvisorRoomCandidate,
  TRIPADVISOR_VERIFY_VERSION,
} from "./verify.js";

export {
  verifyOfficialWebsiteRoomCount,
  extractCompositionRoomTotals,
  OFFICIAL_ROOM_PATHS,
  OFFICIAL_SITE_VERIFY_VERSION,
} from "./official-site-verify.js";

export {
  annotateIndependence,
  assessMultiSourceIndependence,
  assignUpstreamCluster,
  UPSTREAM_CLUSTERS,
  SOURCE_INDEPENDENCE_VERSION,
} from "./independence.js";

export {
  classifyRoomConflict,
  CONFLICT_CAUSE,
  CONFLICT_CLASSIFY_VERSION,
} from "./conflicts.js";

export {
  enrichHotelTripadvisorRooms,
  enrichHotelsTripadvisorRooms,
  summarizeEnrichmentBatch,
  TRIPADVISOR_ENRICH_VERSION,
} from "./enrich.js";
