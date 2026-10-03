export {
  HOTEL_ADP_ATTRIBUTES_TABLE,
  HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION,
  HOTEL_ADP_ATTRIBUTE_VERSION,
  MAP_HOTEL_ADP_ATTRIBUTE,
  VAL_ATTRIBUTE_CATEGORY,
  VAL_ADP_USE_TYPE,
  VAL_SOURCE_TYPE,
  buildAttributeDedupeKey,
} from "./field-map.js";

export { buildHotelIntelligenceProfile } from "./build-hotel-intelligence-profile.js";
export { buildAdpHotelAttributes, classifyAdpUse } from "./build-adp-hotel-attributes.js";
export { syncHotelAdpAttributesToAirtable } from "./airtable-store.js";
