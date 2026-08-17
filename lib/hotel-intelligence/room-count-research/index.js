/**
 * Room Count Research Engine — public exports.
 */

export {
  researchHotelRoomCount,
  ROOM_COUNT_RESEARCH_VERSION,
  RESEARCH_STATUS,
} from "./research.js";
export {
  extractRoomCountsFromText,
  extractMultilingualRoomPhrases,
  extractQuote,
  ROOM_COUNT_EXTRACT_VERSION,
} from "./extract.js";
export {
  SOURCE_CATEGORIES,
  classifySourceUrl,
  isFetchEligibleUrl,
  baseConfidenceForCategory,
  ROOM_COUNT_TRUST_VERSION,
} from "./trust.js";
export {
  scoreRoomCountResearch,
  ROOM_COUNT_CONFIDENCE_VERSION,
} from "./confidence.js";
export { buildRoomCountQueries, selectFetchCandidates } from "./queries.js";
export { fetchResearchPage, htmlToSearchableText } from "./fetch.js";
