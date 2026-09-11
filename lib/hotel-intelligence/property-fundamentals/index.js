export {
  resolvePropertyRooms,
  resolvePropertyFundamentals,
  applyRoomsResolutionToPortfolioHotel,
  roomsObservationMatchesHotel,
  isMissingRooms,
  toPositiveInt,
  ROOMS_SOURCE,
  ROOMS_CONFIDENCE,
} from "./rooms-resolver.js";

export {
  extractRoomsObservationFromText,
  extractRoomsObservationsForHotels,
  parseRoomsFromWindow,
  windowsAfterHotelName,
} from "./rooms-extractor.js";

export {
  loadExistingResearchCorpusTexts,
  buildResearchRoomsObservationsFromExistingCorpus,
  mergeResearchRoomsIntoAssets,
  CAMBRIDGE_RESEARCH_RAW_DIR,
} from "./load-existing-research-rooms.js";
