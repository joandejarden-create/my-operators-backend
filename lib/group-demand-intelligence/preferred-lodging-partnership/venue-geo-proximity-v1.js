/**
 * Hotel-centric venue proximity — fail closed on unverified route metrics.
 * Straight-line haversine from verified coordinates is explicitly labeled.
 * Driving/walking time requires an approved routing provider (interface only).
 */
import {
  CATCHMENT_CLASS,
  CATCHMENT_CLASS_LABEL,
  DISTANCE_METHOD,
  DISTANCE_METHOD_LABEL,
  text,
} from "./preferred-lodging-partnership-contract-v1.js";

export const VENUE_GEO_PROXIMITY_V1 = "gdi-venue-geo-proximity-v1";

const EARTH_RADIUS_MI = 3958.7613;

function toRad(d) {
  return (Number(d) * Math.PI) / 180;
}

export function haversineMiles(lat1, lng1, lat2, lng2) {
  const aLat = Number(lat1);
  const aLng = Number(lng1);
  const bLat = Number(lat2);
  const bLng = Number(lng2);
  if (![aLat, aLng, bLat, bLng].every((n) => Number.isFinite(n))) return null;
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLng / 2) ** 2;
  return 2 * EARTH_RADIUS_MI * Math.asin(Math.min(1, Math.sqrt(h)));
}

/**
 * Default catchment bands — provisional; market policy may override.
 * Miles are straight-line thresholds only when distanceMethod is haversine.
 */
export const DEFAULT_CATCHMENT_POLICY_MI = Object.freeze({
  coreMaxMi: 1.5,
  secondaryMaxMi: 3.5,
  extendedMaxMi: 8,
});

export function classifyCatchmentFromStraightLineMiles(
  miles,
  policy = DEFAULT_CATCHMENT_POLICY_MI,
) {
  if (miles == null || !Number.isFinite(Number(miles))) {
    return CATCHMENT_CLASS.UNKNOWN;
  }
  const m = Number(miles);
  if (m <= policy.coreMaxMi) return CATCHMENT_CLASS.CORE;
  if (m <= policy.secondaryMaxMi) return CATCHMENT_CLASS.SECONDARY;
  if (m <= policy.extendedMaxMi) return CATCHMENT_CLASS.EXTENDED;
  return CATCHMENT_CLASS.OUT_OF_CATCHMENT;
}

/**
 * Routing provider boundary — no live paid calls. Mock or registered provider only.
 */
export function createRoutingProviderStub({ mode = "unavailable" } = {}) {
  return {
    version: VENUE_GEO_PROXIMITY_V1,
    mode,
    async getRouteMetrics() {
      return {
        ok: false,
        reason: "routing_provider_unavailable",
        travelTimeMinutes: null,
        routeDistanceMiles: null,
        travelMode: null,
        provider: null,
        note: "Travel time not yet verified.",
      };
    },
  };
}

/**
 * Build proximity evidence pack. Never invent drive times from straight-line miles.
 */
export function buildVenueProximityEvidence({
  hotel = {},
  venue = {},
  historical = {},
  catchmentPolicy = DEFAULT_CATCHMENT_POLICY_MI,
  routingProvider = null,
} = {}) {
  const hotelLat = hotel.lat ?? hotel.latitude;
  const hotelLng = hotel.lng ?? hotel.longitude;
  const venueLat = venue.lat ?? venue.latitude;
  const venueLng = venue.lng ?? venue.longitude;

  const straightLine = haversineMiles(hotelLat, hotelLng, venueLat, venueLng);
  let distanceMiles = straightLine != null ? Number(straightLine.toFixed(2)) : null;
  let distanceMethod =
    straightLine != null
      ? DISTANCE_METHOD.STRAIGHT_LINE_HAVERSINE
      : DISTANCE_METHOD.UNKNOWN;
  let distanceLabel = null;
  let travelTimeMinutes = null;
  let travelMode = null;
  let routeDistanceMiles = null;
  let travelTimeNote = "Travel time not yet verified.";
  let historicalClaim = null;

  // Historical claims (e.g. The Lane 0.9 mi / ~2 min) — audit only, never certify as live route.
  if (
    distanceMiles == null &&
    historical.distanceMiles != null &&
    Number.isFinite(Number(historical.distanceMiles))
  ) {
    distanceMiles = Number(historical.distanceMiles);
    distanceMethod = DISTANCE_METHOD.HISTORICAL_CLAIM_UNVERIFIED;
    historicalClaim = {
      distanceMiles,
      driveTimeMinutes: historical.driveTimeMinutes ?? null,
      distanceMethodRaw: historical.distanceMethod || null,
      note: "Historical research claim — re-verify coordinates and routing before sales use.",
    };
  }

  if (distanceMiles != null) {
    if (distanceMethod === DISTANCE_METHOD.STRAIGHT_LINE_HAVERSINE) {
      distanceLabel = `${distanceMiles} mi straight-line`;
    } else if (distanceMethod === DISTANCE_METHOD.HISTORICAL_CLAIM_UNVERIFIED) {
      distanceLabel = `${distanceMiles} mi (historical claim — unverified)`;
    } else {
      distanceLabel = `${distanceMiles} mi`;
    }
  }

  // Never derive drive minutes from straight-line distance.
  if (
    historical.driveTimeMinutes != null &&
    distanceMethod === DISTANCE_METHOD.HISTORICAL_CLAIM_UNVERIFIED
  ) {
    travelTimeNote = `Historical claim ~${historical.driveTimeMinutes} min driving — not route-certified. Travel time not yet verified.`;
  }

  const catchmentClass = classifyCatchmentFromStraightLineMiles(
    distanceMiles,
    catchmentPolicy,
  );

  return {
    version: VENUE_GEO_PROXIMITY_V1,
    hotelCoordinates:
      Number.isFinite(Number(hotelLat)) && Number.isFinite(Number(hotelLng))
        ? { lat: Number(hotelLat), lng: Number(hotelLng) }
        : null,
    venueCoordinates:
      Number.isFinite(Number(venueLat)) && Number.isFinite(Number(venueLng))
        ? { lat: Number(venueLat), lng: Number(venueLng) }
        : null,
    distanceMiles,
    distanceMethod,
    distanceMethodLabel: DISTANCE_METHOD_LABEL[distanceMethod] || null,
    distanceLabel,
    routeDistanceMiles,
    travelTimeMinutes,
    travelMode,
    travelTimeNote,
    catchmentClass,
    catchmentClassLabel: CATCHMENT_CLASS_LABEL[catchmentClass] || null,
    catchmentPolicy,
    routingProviderAvailable: Boolean(routingProvider),
    historicalClaim,
    customerFacingDistance:
      distanceLabel || "Distance not yet verified from coordinates",
    customerFacingTravelTime: travelTimeNote,
  };
}

export function assertNoInventedDriveTime(proximity = {}) {
  const failures = [];
  if (
    proximity.travelTimeMinutes != null &&
    proximity.distanceMethod === DISTANCE_METHOD.STRAIGHT_LINE_HAVERSINE &&
    !proximity.routeProviderCertified
  ) {
    failures.push("drive_time_inferred_from_straight_line");
  }
  if (
    /~\s*\d+\s*min/i.test(text(proximity.distanceLabel)) &&
    proximity.distanceMethod === DISTANCE_METHOD.STRAIGHT_LINE_HAVERSINE
  ) {
    failures.push("drive_time_embedded_in_distance_label");
  }
  return { ok: failures.length === 0, failures };
}
