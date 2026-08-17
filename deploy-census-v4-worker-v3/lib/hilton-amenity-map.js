/**
 * Hilton locations directory amenityIds → display labels and census Y/N columns.
 */

/** @type {Record<string, string>} */
export const HILTON_AMENITY_LABELS = {
  adjoiningRooms: "Adjoining Rooms",
  adultsOnly: "Adults Only",
  airportShuttle: "Airport Shuttle",
  allInclusive: "All-Inclusive",
  beach: "Beach Access",
  boutique: "Boutique",
  businessCenter: "Business Center",
  casino: "Casino",
  concierge: "Concierge",
  conventionCenter: "Convention Center",
  cribs: "Cribs Available",
  digitalKey: "Digital Key",
  dining: "On-Site Restaurant",
  evCharging: "EV Charging",
  fitnessCenter: "Fitness Center",
  freeBreakfast: "Free Breakfast",
  freeWifi: "Free WiFi",
  golf: "Golf",
  indoorPool: "Indoor Pool",
  meetingRooms: "Meeting Rooms",
  nonSmoking: "Non-Smoking Rooms",
  outdoorPool: "Outdoor Pool",
  petsAllowed: "Pet-Friendly",
  petsNotAllowed: "Pets Not Allowed",
  pool: "Pool",
  resort: "Resort",
  roomAccessibility: "Accessible Rooms",
  roomService: "Room Service",
  ski: "Ski Access",
  spa: "Spa",
  tennis: "Tennis",
  valetParking: "Valet Parking",
  freeParking: "Free Parking",
  executiveLounge: "Executive Lounge",
  newHotel: "Newly Built",
};

/**
 * Hilton amenityId → Hotel Census single-select column (STR naming).
 * Values written: "Y" only when amenity present (fill-blank).
 */
export const HILTON_AMENITY_TO_CENSUS_YN = {
  boutique: "Boutique (Y/N)",
  dining: "Restaurant (Y/N)",
  meetingRooms: "Conference (Y/N)",
  conventionCenter: "Convention (Y/N)",
  golf: "Golf (Y/N)",
  spa: "Spa (Y/N)",
  casino: "Casino (Y/N)",
  resort: "Resort (Y/N)",
};

export const CENSUS_AMENITIES_TEXT_FIELD = "Amenities";

export const CENSUS_AMENITY_YN_COLUMNS = Object.values(HILTON_AMENITY_TO_CENSUS_YN);

/** @param {string} id */
export function labelForHiltonAmenityId(id) {
  const key = String(id || "").trim();
  if (!key) return "";
  if (HILTON_AMENITY_LABELS[key]) return HILTON_AMENITY_LABELS[key];
  return key
    .replace(/([A-Z])/g, " $1")
    .replace(/[_-]+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * @param {string[]} amenityIds
 * @returns {string} Semicolon-separated labels for Amenities multilineText.
 */
export function formatAmenitiesText(amenityIds) {
  const labels = (amenityIds || [])
    .map((id) => labelForHiltonAmenityId(id))
    .filter(Boolean);
  const unique = [...new Set(labels)];
  unique.sort((a, b) => a.localeCompare(b));
  return unique.join("; ");
}

/**
 * @param {string[]} amenityIds
 * @returns {Record<string, string>} Airtable fields (Amenities + Y/N columns).
 */
export function directoryAmenityIdsToCensusFields(amenityIds) {
  const fields = {};
  const text = formatAmenitiesText(amenityIds);
  if (text) fields[CENSUS_AMENITIES_TEXT_FIELD] = text;

  const ids = new Set((amenityIds || []).map((x) => String(x)));
  for (const [amenityId, column] of Object.entries(HILTON_AMENITY_TO_CENSUS_YN)) {
    if (ids.has(amenityId)) fields[column] = "Y";
  }
  return fields;
}

/**
 * @param {string[]} amenityIds
 * @returns {Record<string, string>}
 */
export function summarizeAmenityYnFlags(amenityIds) {
  const fields = directoryAmenityIdsToCensusFields(amenityIds);
  const flags = {};
  for (const col of CENSUS_AMENITY_YN_COLUMNS) {
    if (fields[col]) flags[col] = fields[col];
  }
  return flags;
}
