/**
 * Re-export Discovery Factory city inference so universe-expansion stays in sync.
 */
export {
  CITY_INFER_VERSION,
  foldCityKey,
  normalizeCityLabel,
  inferCityFromCventUrl,
  inferCityFromHotelName,
  resolveDiscoveryCity,
  titleCaseSlug,
} from "../discovery-factory/city-infer.js";
