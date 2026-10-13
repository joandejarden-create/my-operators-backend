/**
 * Generic hotel context + practical catchment for Private Events V1.
 * No hotel-name or city hardcodes in production logic.
 */

import {
  ARCHETYPE_VENUE_MIX,
  CATCHMENT_MILES_BY_ARCHETYPE,
  HOTEL_ARCHETYPE_PE,
} from "./constants.js";

function clean(s) {
  return String(s || "").trim();
}

/**
 * Map coarse market/setting strings → PE archetype (portable).
 */
export function resolveHotelArchetype(hotel = {}) {
  const explicit = clean(hotel.archetype || hotel.hotelArchetype).toUpperCase();
  if (HOTEL_ARCHETYPE_PE[explicit]) return HOTEL_ARCHETYPE_PE[explicit];
  if (Object.values(HOTEL_ARCHETYPE_PE).includes(explicit)) return explicit;

  const setting = clean(hotel.urbanSuburbanResort || hotel.setting || hotel.marketType)
    .toLowerCase();
  const service = clean(hotel.serviceLevel || hotel.brandClass).toLowerCase();
  const rooms = Number(hotel.roomCount || hotel.rooms || 0);

  if (/resort|destination|island|beach/.test(setting)) {
    return HOTEL_ARCHETYPE_PE.RESORT_DESTINATION;
  }
  if (/urban|downtown|city/.test(setting)) {
    if (/luxury|upscale|upper\s*upscale/.test(service)) {
      return HOTEL_ARCHETYPE_PE.LUXURY_URBAN;
    }
    return HOTEL_ARCHETYPE_PE.URBAN_FULL_SERVICE;
  }
  if (/select|limited|focused/.test(service) || (rooms > 0 && rooms < 180)) {
    return HOTEL_ARCHETYPE_PE.SELECT_SERVICE;
  }
  if (/boutique/.test(service) || /boutique/.test(setting)) {
    return HOTEL_ARCHETYPE_PE.BOUTIQUE;
  }
  if (/suburban|edge|corridor/.test(setting)) {
    return HOTEL_ARCHETYPE_PE.SUBURBAN_FULL_SERVICE;
  }
  return HOTEL_ARCHETYPE_PE.SUBURBAN_FULL_SERVICE;
}

export function practicalCatchmentMiles(archetype, hotel = {}) {
  if (
    hotel.practicalCatchment &&
    Number.isFinite(Number(hotel.practicalCatchment.coreMiles))
  ) {
    return {
      coreMiles: Number(hotel.practicalCatchment.coreMiles),
      stretchMiles: Number(
        hotel.practicalCatchment.stretchMiles ||
          hotel.practicalCatchment.coreMiles * 2
      ),
    };
  }
  const band =
    CATCHMENT_MILES_BY_ARCHETYPE[archetype] ||
    CATCHMENT_MILES_BY_ARCHETYPE[HOTEL_ARCHETYPE_PE.SUBURBAN_FULL_SERVICE];
  return { coreMiles: band.core, stretchMiles: band.stretch };
}

/**
 * Haversine miles between two lat/long points.
 */
export function distanceMiles(a, b) {
  const lat1 = Number(a?.lat ?? a?.latitude);
  const lon1 = Number(a?.long ?? a?.lng ?? a?.longitude);
  const lat2 = Number(b?.lat ?? b?.latitude);
  const lon2 = Number(b?.long ?? b?.lng ?? b?.longitude);
  if (
    ![lat1, lon1, lat2, lon2].every((n) => Number.isFinite(n))
  ) {
    return null;
  }
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 3958.7613;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return Math.round(R * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x)) * 10) / 10;
}

export function buildHotelContext(hotel = {}) {
  const hotelId = clean(hotel.hotelId || hotel.id);
  if (!hotelId) {
    throw new Error("hotelContext requires hotelId");
  }
  const archetype = resolveHotelArchetype(hotel);
  const catchment = practicalCatchmentMiles(archetype, hotel);
  const lat = Number(hotel.lat ?? hotel.latitude);
  const long = Number(hotel.long ?? hotel.lng ?? hotel.longitude);

  return {
    hotelId,
    displayName: clean(hotel.displayName || hotel.name) || null,
    lat: Number.isFinite(lat) ? lat : null,
    long: Number.isFinite(long) ? long : null,
    roomCount: Number(hotel.roomCount || hotel.rooms || 0) || null,
    meetingSpace: hotel.meetingSpace ?? null,
    serviceLevel: clean(hotel.serviceLevel) || null,
    marketType: clean(hotel.marketType) || null,
    brandClass: clean(hotel.brandClass) || null,
    ratePosition: hotel.ratePosition ?? null,
    parking: hotel.parking ?? null,
    airportAccess: hotel.airportAccess ?? null,
    urbanSuburbanResort: clean(hotel.urbanSuburbanResort || hotel.setting) || null,
    practicalCatchment: catchment,
    groupStrategy: clean(hotel.groupStrategy) || null,
    archetype,
    preferredVenueTypes: ARCHETYPE_VENUE_MIX[archetype] || [],
  };
}

/**
 * Hotel–venue relationship (hotel-specific derived intelligence).
 */
export function buildHotelVenueRelationship(hotelCtx, venue, overrides = {}) {
  const miles =
    overrides.distanceMiles != null
      ? Number(overrides.distanceMiles)
      : distanceMiles(hotelCtx, venue);
  const km =
    miles != null ? Math.round(miles * 1.60934 * 10) / 10 : null;

  const core = hotelCtx.practicalCatchment.coreMiles;
  const stretch = hotelCtx.practicalCatchment.stretchMiles;

  let lodgingCatchmentFit = "UNKNOWN";
  if (miles != null) {
    if (miles <= core) lodgingCatchmentFit = "CORE";
    else if (miles <= core + (stretch - core) * 0.45) lodgingCatchmentFit = "COMPETITIVE";
    else if (miles <= stretch) lodgingCatchmentFit = "STRETCH";
    else lodgingCatchmentFit = "OUTSIDE";
  }

  const preferred = new Set(hotelCtx.preferredVenueTypes || []);
  const productFit = preferred.has(venue.venueType)
    ? "STRONG"
    : venue.venueType
      ? "MODERATE"
      : "UNKNOWN";

  let potentialPartnershipFit = "LOW";
  if (
    lodgingCatchmentFit === "CORE" &&
    venue.onSiteLodgingStatus === "NO_LODGING" &&
    !venue.exclusiveHotelRelationship
  ) {
    potentialPartnershipFit = "HIGH";
  } else if (
    lodgingCatchmentFit !== "OUTSIDE" &&
    (venue.onSiteLodgingStatus === "NO_LODGING" ||
      venue.onSiteLodgingStatus === "LIMITED_LODGING") &&
    !venue.exclusiveHotelRelationship
  ) {
    potentialPartnershipFit = "MODERATE";
  }

  return {
    hotelId: hotelCtx.hotelId,
    venueId: venue.venueId,
    distanceMiles: miles,
    distanceKm: km,
    driveTimeMinutes:
      overrides.driveTimeMinutes != null
        ? Number(overrides.driveTimeMinutes)
        : miles != null
          ? Math.round(miles * 2.5)
          : null,
    lodgingCatchmentFit,
    productFit,
    potentialPartnershipFit,
    existingRelationshipStatus:
      clean(overrides.existingRelationshipStatus) ||
      (venue.hotelPartners || []).some((p) =>
        normIncludes(p, hotelCtx.displayName || hotelCtx.hotelId)
      )
        ? "LISTED_PARTNER"
        : "NONE_KNOWN",
    lastEvaluatedAt: new Date().toISOString(),
  };
}

function normIncludes(a, b) {
  const x = String(a || "").toLowerCase();
  const y = String(b || "").toLowerCase();
  return Boolean(x && y && (x.includes(y) || y.includes(x)));
}
