/**
 * Normalize StayingAPI Property → Dealality candidate object.
 */

import { applyFieldFirewall, STAYINGAPI_ROOMS_CAPABILITY } from "./field-firewall.js";

const PROPERTY_TYPE_MAP = Object.freeze({
  hotel: { dealality: "Hotel", usefulness: "DIRECTLY_USABLE" },
  resort: { dealality: "Resort", usefulness: "DIRECTLY_USABLE" },
  apartment: { dealality: "Apartment", usefulness: "MAPPABLE" },
  house: { dealality: "House", usefulness: "REFERENCE_ONLY" },
  villa: { dealality: "Villa", usefulness: "MAPPABLE" },
  cottage: { dealality: "Cottage", usefulness: "REFERENCE_ONLY" },
  hostel: { dealality: "Hostel", usefulness: "MAPPABLE" },
  other: { dealality: "Other", usefulness: "REFERENCE_ONLY" },
});

/** Staying amenity token → Dealality amenity field */
export const AMENITY_MAP = Object.freeze({
  pool: "Pool",
  spa: "Spa",
  gym: "Fitness",
  beachfront: "Beach / Beachfront",
  parking_free: "Parking",
  parking_paid: "Parking",
  // no golf/casino/kids_club/club_lounge/all_inclusive/airport_shuttle/ski/residences/beach_club in base taxonomy
});

/**
 * @param {object} raw StayingAPI Property
 */
export function normalizeProperty(raw) {
  if (!raw || typeof raw !== "object") return null;
  const loc = raw.location || {};
  const typeKey = String(raw.propertyType || "other").toLowerCase();
  const typeMap = PROPERTY_TYPE_MAP[typeKey] || PROPERTY_TYPE_MAP.other;

  const amenities = Array.isArray(raw.amenities) ? raw.amenities.map(String) : [];
  const dealalityAmenities = {};
  for (const token of amenities) {
    const field = AMENITY_MAP[token];
    if (field) dealalityAmenities[field] = "Yes";
  }

  const candidate = applyFieldFirewall({
    provider: "StayingAPI",
    staying_id: raw.id || null,
    platform: raw.platform || null,
    platform_listing_id: raw.platformListingId || null,
    url: raw.url || null,
    name: raw.name || null,
    property_type_raw: raw.propertyType || null,
    property_type_dealality: typeMap.dealality,
    property_type_usefulness: typeMap.usefulness,
    address: loc.address || null,
    city: loc.city || null,
    state_region: loc.region || null,
    country: loc.country || null,
    postal_code: loc.postalCode || loc.postal_code || null,
    latitude: loc.lat ?? null,
    longitude: loc.lng ?? null,
    amenities_raw: amenities,
    amenities_dealality: dealalityAmenities,
    image_urls_reference_only: Array.isArray(raw.images) ? raw.images.slice(0, 8) : [],
    // retained for firewall proof only — must not map to Rooms/Keys
    _observed_bedrooms: raw.bedrooms ?? null,
    _observed_max_occupancy: raw.maxOccupancy ?? null,
    rooms_capability: STAYINGAPI_ROOMS_CAPABILITY,
    identity: raw.identity || null,
  });

  return candidate;
}

export function buildPropertyTypeMappingArtifact() {
  return {
    version: "stayingapi-property-type-map-v1",
    map: PROPERTY_TYPE_MAP,
    note: "Do not assume OTA taxonomy equals Dealality Property Type without mapping.",
  };
}
