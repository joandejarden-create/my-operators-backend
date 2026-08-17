/**
 * Normalize SerpApi Google Hotels property → Dealality candidate object.
 */

import { applyFieldFirewall, SERPAPI_ROOMS_CAPABILITY } from "./field-firewall.js";

/** Google amenity string (normalized) → Dealality Golden Census amenity */
export const AMENITY_MAP = Object.freeze({
  pool: "Pool",
  "outdoor pool": "Pool",
  "indoor pool": "Pool",
  pools: "Pool",
  spa: "Spa",
  "full-service spa": "Spa",
  fitness: "Fitness",
  "fitness center": "Fitness",
  gym: "Fitness",
  "fitness centre": "Fitness",
  golf: "Golf",
  "golf course": "Golf",
  beach: "Beach / Beachfront",
  "beach access": "Beach / Beachfront",
  "beachfront": "Beach / Beachfront",
  "private beach": "Beach / Beachfront",
  "beach club": "Beach Club",
  casino: "Casino",
  "kids club": "Kids Club",
  "children's club": "Kids Club",
  "club lounge": "Club Lounge",
  "executive lounge": "Club Lounge",
  "all-inclusive": "All-Inclusive",
  "all inclusive": "All-Inclusive",
  parking: "Parking",
  "free parking": "Parking",
  "paid parking": "Parking",
  "airport shuttle": "Airport Shuttle",
  "free airport shuttle": "Airport Shuttle",
  ski: "Ski",
  "ski-in": "Ski",
  residences: "Residences",
  "residential": "Residences",
});

/** Excluded amenity phrases → Dealality field with NO — EXPLICIT */
export const EXCLUDED_AMENITY_MAP = Object.freeze({
  "no pool": "Pool",
  "no spa": "Spa",
  "no fitness": "Fitness",
  "no gym": "Fitness",
  "no beach": "Beach / Beachfront",
  "no beach access": "Beach / Beachfront",
  "no airport shuttle": "Airport Shuttle",
  "no parking": "Parking",
  "not pet-friendly": null, // not in Golden amenity set for this benchmark
});

const PROPERTY_TYPE_USEFULNESS = Object.freeze({
  hotel: "DIRECTLY_USABLE",
  resort: "DIRECTLY_USABLE",
  inn: "USEFUL_INPUT_TO_DERIVATION",
  motel: "USEFUL_INPUT_TO_DERIVATION",
  "vacation rental": "REFERENCE_ONLY",
  apartment: "REFERENCE_ONLY",
  villa: "USEFUL_INPUT_TO_DERIVATION",
  house: "REFERENCE_ONLY",
});

function parseAddressParts(address) {
  const raw = String(address || "").trim();
  if (!raw) return { city: null, state_region: null, country: null, postal_code: null };
  const parts = raw.split(",").map((p) => p.trim()).filter(Boolean);
  let country = null;
  let postal_code = null;
  let state_region = null;
  let city = null;
  if (parts.length) {
    const last = parts[parts.length - 1];
    if (/mexico|méxico|mx/i.test(last)) country = "Mexico";
    else country = last;
  }
  // postal often in last or second-last token
  for (const p of parts) {
    const m = p.match(/\b(\d{5})\b/);
    if (m) postal_code = m[1];
  }
  if (parts.length >= 3) {
    // Often: street, neighborhood, "77500 Cancún", "Q.R.", "Mexico"
    const cityPart = parts.find((p) => /\d{5}/.test(p) && /[A-Za-zÁÉÍÓÚáéíóúñÑ]/.test(p)) || parts[parts.length - 3];
    city = String(cityPart || "")
      .replace(/\b\d{5}\b/g, "")
      .replace(/\s+/g, " ")
      .trim() || null;
    const stateCand = parts[parts.length - 2];
    if (stateCand && !/mexico|méxico/i.test(stateCand)) {
      state_region = stateCand.replace(/\d{5}/g, "").trim() || null;
    }
  } else if (parts.length === 2) {
    city = parts[0].replace(/\b\d{5}\b/g, "").trim();
  }
  return { city, state_region, country, postal_code };
}

function mapAmenities(list) {
  const dealality = {};
  const raw = Array.isArray(list) ? list.map(String) : [];
  for (const item of raw) {
    const n = item.toLowerCase().trim();
    for (const [key, field] of Object.entries(AMENITY_MAP)) {
      if (n === key || n.includes(key)) {
        dealality[field] = "Yes";
      }
    }
  }
  return { raw, dealality };
}

function mapExcludedAmenities(list) {
  const dealality = {};
  const raw = Array.isArray(list) ? list.map(String) : [];
  for (const item of raw) {
    const n = item.toLowerCase().trim();
    for (const [key, field] of Object.entries(EXCLUDED_AMENITY_MAP)) {
      if (!field) continue;
      if (n === key || n.includes(key.replace(/^no\s+/, "")) && n.startsWith("no ")) {
        dealality[field] = "NO — EXPLICIT";
      } else if (n.includes(key)) {
        dealality[field] = "NO — EXPLICIT";
      }
    }
    // generic: "No X" patterns for mapped amenities
    const m = n.match(/^no\s+(.+)$/i) || n.match(/^not\s+(.+)$/i);
    if (m) {
      const rest = m[1].trim();
      for (const [key, field] of Object.entries(AMENITY_MAP)) {
        if (rest.includes(key) || key.includes(rest)) {
          dealality[field] = "NO — EXPLICIT";
        }
      }
    }
  }
  return { raw, dealality };
}

function hotelClassUsefulness() {
  return "USEFUL_INPUT_TO_DERIVATION";
}

/**
 * @param {object} raw - property object from search `properties[]` or property-details root
 * @param {{ source?: string }} [meta]
 */
export function normalizeGoogleHotelProperty(raw, meta = {}) {
  if (!raw || typeof raw !== "object") return null;

  const gps = raw.gps_coordinates || {};
  const address = raw.address || null;
  const parsed = parseAddressParts(address);

  const typeRaw = raw.type || raw.property_type || null;
  const typeKey = String(typeRaw || "").toLowerCase();
  const typeUsefulness = PROPERTY_TYPE_USEFULNESS[typeKey] || "REFERENCE_ONLY";

  const amenityMap = mapAmenities(raw.amenities);
  const excludedMap = mapExcludedAmenities(raw.excluded_amenities);

  // Merge: explicit No wins over absent; Yes from amenities
  const amenitiesDealality = { ...excludedMap.dealality, ...amenityMap.dealality };
  // If both Yes and NO — EXPLICIT, prefer conflict flag
  for (const field of Object.keys(amenityMap.dealality)) {
    if (excludedMap.dealality[field] === "NO — EXPLICIT") {
      amenitiesDealality[field] = "CONFLICT_YES_AND_EXPLICIT_NO";
    }
  }

  const images = Array.isArray(raw.images)
    ? raw.images
        .map((img) => (typeof img === "string" ? img : img?.original_image || img?.thumbnail || null))
        .filter(Boolean)
        .slice(0, 8)
    : raw.thumbnail
      ? [raw.thumbnail]
      : [];

  // Observe but never promote rooms/bedrooms to Keys
  const bookableRoomTypeCount = Array.isArray(raw.featured_prices)
    ? raw.featured_prices.reduce((n, p) => n + (Array.isArray(p.rooms) ? p.rooms.length : 0), 0)
    : Array.isArray(raw.rooms)
      ? raw.rooms.length
      : null;

  const essentialInfo = Array.isArray(raw.essential_info) ? raw.essential_info.map(String) : [];

  const candidate = applyFieldFirewall({
    provider: "SerpApi_Google_Hotels",
    source_shape: meta.source || "unknown",
    property_token: raw.property_token || null,
    google_property_url: raw.link || null,
    name: raw.name || null,
    description_analysis_only: raw.description || null,
    address,
    city: parsed.city,
    state_region: parsed.state_region,
    country: parsed.country || (/\bmexico\b/i.test(String(address || "")) ? "Mexico" : null),
    postal_code: parsed.postal_code,
    latitude: gps.latitude ?? null,
    longitude: gps.longitude ?? null,
    phone: raw.phone || null,
    website:
      raw.link && !/google\.com\/(travel|maps)/i.test(String(raw.link))
        ? raw.link
        : null,
    // google_property_url keeps travel links; website only non-Google hosts
    hotel_class_raw: raw.hotel_class ?? null,
    extracted_hotel_class: raw.extracted_hotel_class ?? null,
    hotel_class_usefulness: hotelClassUsefulness(),
    property_type_raw: typeRaw,
    property_type_usefulness: typeUsefulness,
    amenities_raw: amenityMap.raw,
    excluded_amenities_raw: excludedMap.raw,
    amenities_dealality: amenitiesDealality,
    overall_rating: raw.overall_rating ?? null,
    reviews: raw.reviews ?? null,
    check_in_time: raw.check_in_time || null,
    check_out_time: raw.check_out_time || null,
    image_urls_reference_only: images,
    _observed_bookable_room_type_count: bookableRoomTypeCount,
    _observed_essential_info: essentialInfo,
    rooms_capability: SERPAPI_ROOMS_CAPABILITY,
  });

  return candidate;
}

export function buildPropertyTypeClassArtifact() {
  return {
    version: "serpapi-property-type-class-v1",
    property_type_map: PROPERTY_TYPE_USEFULNESS,
    hotel_class_usefulness: "USEFUL_INPUT_TO_DERIVATION",
    hotel_class_note:
      "Preserve Google hotel_class / extracted_hotel_class as raw source values. Do not auto-map to STR Chain Scale or Dealality Segment.",
    rooms_note: "SERPAPI_ROOMS_CAPABILITY = NOT_SUPPORTED",
  };
}
