/**
 * Resolve Hilton amenityIds + display order for radar hotel detail panel.
 */
import {
  HILTON_AMENITY_LABELS,
  labelForHiltonAmenityId,
} from "./hilton-amenity-map.js";

/** Hilton hotel-page carousel order (featured amenities first). */
export const HILTON_AMENITY_DISPLAY_ORDER = [
  "freeWifi",
  "nonSmoking",
  "dining",
  "outdoorPool",
  "indoorPool",
  "pool",
  "freeParking",
  "valetParking",
  "fitnessCenter",
  "digitalKey",
  "concierge",
  "allInclusive",
  "beach",
  "boutique",
  "newHotel",
  "executiveLounge",
  "cribs",
  "roomService",
  "meetingRooms",
  "spa",
  "resort",
  "petsNotAllowed",
  "petsAllowed",
  "businessCenter",
  "adjoiningRooms",
  "connectingRooms",
  "airportShuttle",
  "evCharging",
  "freeBreakfast",
  "golf",
  "casino",
  "conventionCenter",
  "tennis",
  "ski",
  "adultsOnly",
  "roomAccessibility",
];

/** Census / Airtable label variants → Hilton amenityId */
const LABEL_TO_AMENITY_ID = (() => {
  /** @type {Record<string, string>} */
  const map = {};
  for (const [id, label] of Object.entries(HILTON_AMENITY_LABELS)) {
    map[normalizeAmenityLabelKey(label)] = id;
  }
  const aliases = {
    "free wifi": "freeWifi",
    "free parking": "freeParking",
    "digital key": "digitalKey",
    "on site restaurant": "dining",
    "onsite restaurant": "dining",
    "non smoking rooms": "nonSmoking",
    "non-smoking rooms": "nonSmoking",
    "outdoor pool": "outdoorPool",
    "indoor pool": "indoorPool",
    "fitness center": "fitnessCenter",
    "meeting rooms": "meetingRooms",
    "meeting room": "meetingRooms",
    "room service": "roomService",
    "beach access": "beach",
    "all inclusive": "allInclusive",
    "cribs available": "cribs",
    "pets not allowed": "petsNotAllowed",
    "pet friendly": "petsAllowed",
    "pet-friendly": "petsAllowed",
    "pet friendly rooms": "petsAllowed",
    "executive lounge": "executiveLounge",
    "new hotel": "newHotel",
    "newly built": "newHotel",
    "connecting rooms": "adjoiningRooms",
    "adjoining rooms": "adjoiningRooms",
    "business center": "businessCenter",
    "complimentary wi fi": "freeWifi",
    "complimentary wi-fi": "freeWifi",
    "complimentary wifi": "freeWifi",
    "meeting space": "meetingRooms",
    "meeting event space": "meetingEventSpace",
    "event space": "meetingEventSpace",
    "wedding services": "weddingServices",
    "wedding service": "weddingServices",
    "coffee tea in room": "coffeeTeaInRoom",
    "coffee tea maker": "coffeeTeaInRoom",
    "coffee maker": "coffeeTeaInRoom",
    "mobile key": "digitalKey",
    "restaurant": "dining",
    "airport shuttle": "airportShuttle",
    "ev charging": "evCharging",
    "electric car charging": "evCharging",
    "electric car charging station": "evCharging",
    "free breakfast": "freeBreakfast",
    "complimentary breakfast": "freeBreakfast",
    "breakfast available fee": "breakfastAvailable",
    "breakfast available": "breakfastAvailable",
    "valet parking": "valetParking",
    "ski access": "ski",
    "accessible rooms": "roomAccessibility",
    "convention center": "conventionCenter",
    "adults only": "adultsOnly",
    "wake up call": "wakeUpCall",
    "wake up calls": "wakeUpCall",
    "wake-up call": "wakeUpCall",
    "wake-up calls": "wakeUpCall",
    "convenience store": "convenienceStore",
    "children s recreation": "kidsRecreation",
    "children recreation": "kidsRecreation",
    "sustainability": "sustainability",
    "service request": "serviceRequest",
  };
  Object.assign(map, aliases);
  return map;
})();

export function normalizeAmenityLabelKey(label) {
  return String(label || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function parseCtyhocnFromHiltonWebsite(url) {
  const m = String(url || "").match(/\/hotels\/([a-z0-9]{5,12})-/i);
  return m ? m[1].toUpperCase() : null;
}

export function parseAmenitiesTextField(raw) {
  if (!raw || !String(raw).trim()) return [];
  return String(raw)
    .split(/[,;|\n]+|(?:\s+and\s+)/i)
    .map((part) => part.replace(/^[\s•\-–*]+/, "").trim())
    .filter((part) => part.length > 0 && part.length < 80);
}

/**
 * @param {string} label
 * @returns {string|null}
 */
export function amenityIdFromLabel(label) {
  const key = normalizeAmenityLabelKey(label);
  return LABEL_TO_AMENITY_ID[key] || null;
}

/**
 * @param {string[]} amenityIds
 */
export function sortAmenityIdsForDisplay(amenityIds) {
  const order = HILTON_AMENITY_DISPLAY_ORDER;
  return [...new Set(amenityIds)].sort((a, b) => {
    const ia = order.indexOf(a);
    const ib = order.indexOf(b);
    if (ia === -1 && ib === -1) return a.localeCompare(b);
    if (ia === -1) return 1;
    if (ib === -1) return -1;
    return ia - ib;
  });
}

/**
 * @param {string} amenitiesText
 * @returns {string[]}
 */
export function amenityIdsFromAmenitiesText(amenitiesText) {
  const labels = parseAmenitiesTextField(amenitiesText);
  const ids = [];
  const seen = new Set();
  for (const label of labels) {
    const id = amenityIdFromLabel(label);
    if (id && !seen.has(id)) {
      seen.add(id);
      ids.push(id);
    }
  }
  return sortAmenityIdsForDisplay(ids);
}

/**
 * @param {object} hotel — formatted census hotel
 * @returns {{ id: string|null, label: string }[]}
 */
export function buildHiltonAmenitiesDisplay(hotel) {
  const labels = parseAmenitiesTextField(hotel?.amenities);
  if (!labels.length) return [];

  /** @type {{ id: string|null, label: string }[]} */
  const result = [];
  const seenId = new Set();
  const seenLabelKey = new Set();

  for (const rawLabel of labels) {
    const label = String(rawLabel || "").trim();
    if (!label) continue;

    const id = amenityIdFromLabel(label);
    if (id) {
      if (seenId.has(id)) continue;
      seenId.add(id);
      result.push({ id, label });
      continue;
    }

    const key = normalizeAmenityLabelKey(label);
    if (seenLabelKey.has(key)) continue;
    seenLabelKey.add(key);
    result.push({ id: null, label });
  }

  return result;
}
