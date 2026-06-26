/**
 * Demand anchor coordinate verification — geocode cross-check + map display gate.
 */

const COUNTRY_BBOX = {
  "Dominican Republic": { minLat: 17.47, maxLat: 20.05, minLng: -72.05, maxLng: -68.25 },
  "Puerto Rico": { minLat: 17.88, maxLat: 18.52, minLng: -67.95, maxLng: -65.22 },
};

/** Countries where map display requires Last Verified date (after geocode pass). */
export const MAP_REQUIRE_VERIFIED_COUNTRIES = new Set(
  String(process.env.DEMAND_ANCHORS_MAP_REQUIRE_VERIFIED_COUNTRIES || "Dominican Republic")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);

const NOMINATIM_UA =
  process.env.NOMINATIM_USER_AGENT || "deal-capture-proxy/1.0 (coordinate verification)";

export function hasValidCoordinates(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  return true;
}

export function isInCountryBbox(country, lat, lng) {
  const box = COUNTRY_BBOX[country];
  if (!box) return true;
  return lat >= box.minLat && lat <= box.maxLat && lng >= box.minLng && lng <= box.maxLng;
}

export function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export function buildGeocodeQueries(point) {
  const name = String(point.name || "")
    .replace(/—/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const city = String(point.city || "").trim();
  const country = String(point.country || "Dominican Republic").trim();
  const address = String(point.address || "").trim();
  const queries = [];
  if (name && city) queries.push(`${name}, ${city}, ${country}`);
  if (name) queries.push(`${name}, ${country}`);
  if (address) queries.push(`${address}, ${country}`);
  if (city && !name.toLowerCase().includes(city.toLowerCase())) {
    queries.push(`${name} near ${city}, ${country}`);
  }
  return [...new Set(queries.filter(Boolean))];
}

const HIGH_CONFIDENCE_OSM_TYPES = new Set([
  "hospital",
  "hotel",
  "mall",
  "stadium",
  "university",
  "college",
  "school",
  "beach",
  "marina",
  "airport",
  "aerodrome",
  "port",
  "ferry_terminal",
  "theme_park",
  "attraction",
  "museum",
  "theatre",
  "convention_centre",
  "resort",
  "park",
  "village",
  "town",
  "city",
  "commercial",
  "industrial",
  "pitch",
  "wreck",
]);

export async function nominatimSearch(query, countryCode = "do") {
  const cc =
    countryCode === "Puerto Rico" || countryCode === "United States" ? "us" : "do";
  const url =
    "https://nominatim.openstreetmap.org/search?" +
    new URLSearchParams({
      q: query,
      format: "json",
      limit: "3",
      countrycodes: cc,
    });
  const res = await fetch(url, { headers: { "User-Agent": NOMINATIM_UA } });
  if (!res.ok) throw new Error(`Nominatim HTTP ${res.status}`);
  return res.json();
}

/**
 * @param {object} point
 * @param {object} [opts]
 * @param {Record<string, { latitude: number, longitude: number }>} [opts.manualReference]
 */
export async function verifyDemandAnchorCoordinates(point, opts = {}) {
  const lat = Number(point.latitude ?? point.lat);
  const lng = Number(point.longitude ?? point.lng);
  const country = String(point.country || "").trim();
  const manual = opts.manualReference?.[point.name];

  if (!hasValidCoordinates(lat, lng)) {
    return {
      status: "hidden",
      includeOnRadarMap: false,
      reason: "missing_coordinates",
      latitude: lat,
      longitude: lng,
    };
  }

  if (!isInCountryBbox(country, lat, lng)) {
    return {
      status: "hidden",
      includeOnRadarMap: false,
      reason: "outside_country_bbox",
      latitude: lat,
      longitude: lng,
    };
  }

  let bestOsm = null;
  let bestDrift = Infinity;
  let matchedQuery = null;

  for (const q of buildGeocodeQueries(point)) {
    let hits = [];
    try {
      hits = await nominatimSearch(q, country);
    } catch {
      continue;
    }
    for (const hit of hits) {
      const osmLat = Number(hit.lat);
      const osmLng = Number(hit.lon);
      if (!hasValidCoordinates(osmLat, osmLng)) continue;
      const drift = haversineKm(lat, lng, osmLat, osmLng);
      if (drift < bestDrift) {
        bestDrift = drift;
        bestOsm = hit;
        matchedQuery = q;
      }
    }
    if (bestDrift <= 0.5) break;
  }

  const osmType = bestOsm?.type || "";
  const osmClass = bestOsm?.class || "";
  const highConfidence =
    HIGH_CONFIDENCE_OSM_TYPES.has(osmType) ||
    HIGH_CONFIDENCE_OSM_TYPES.has(osmClass) ||
    /hotel|hospital|beach|airport|marina|mall|stadium|university|convention|resort|park/i.test(
      bestOsm?.display_name || ""
    );

  if (bestOsm && bestDrift <= 1.5) {
    const osmLat = Number(bestOsm.lat);
    const osmLng = Number(bestOsm.lon);
    const useLat = bestDrift > 0.2 ? osmLat : lat;
    const useLng = bestDrift > 0.2 ? osmLng : lng;
    return {
      status: bestDrift > 0.2 ? "corrected" : "verified",
      includeOnRadarMap: true,
      latitude: useLat,
      longitude: useLng,
      driftKm: bestDrift,
      osmDisplay: bestOsm.display_name,
      osmType,
      matchedQuery,
      reason: "osm_match_close",
    };
  }

  if (bestOsm && bestDrift <= 5 && highConfidence) {
    return {
      status: "corrected",
      includeOnRadarMap: true,
      latitude: Number(bestOsm.lat),
      longitude: Number(bestOsm.lon),
      driftKm: bestDrift,
      osmDisplay: bestOsm.display_name,
      osmType,
      matchedQuery,
      reason: "osm_match_corrected",
    };
  }

  if (manual && isInCountryBbox(country, manual.latitude, manual.longitude)) {
    const manualDrift = haversineKm(lat, lng, manual.latitude, manual.longitude);
    if (manualDrift <= 2 || !bestOsm) {
      return {
        status: manualDrift > 0.2 ? "corrected" : "verified",
        includeOnRadarMap: true,
        latitude: manual.latitude,
        longitude: manual.longitude,
        driftKm: manualDrift,
        reason: "manual_reference",
      };
    }
  }

  if (bestOsm && bestDrift <= 10 && highConfidence) {
    return {
      status: "corrected",
      includeOnRadarMap: true,
      latitude: Number(bestOsm.lat),
      longitude: Number(bestOsm.lon),
      driftKm: bestDrift,
      osmDisplay: bestOsm.display_name,
      reason: "osm_match_far_corrected",
    };
  }

  return {
    status: "hidden",
    includeOnRadarMap: false,
    latitude: lat,
    longitude: lng,
    driftKm: bestOsm ? bestDrift : null,
    osmDisplay: bestOsm?.display_name,
    matchedQuery,
    reason: bestOsm ? "osm_drift_too_large" : "no_geocode_match",
  };
}

/**
 * Whether a normalized point should render on the radar map.
 * @param {{ includeOnRadarMap?: boolean, lastVerified?: string, country?: string, latitude?: number|null, longitude?: number|null }} point
 */
export function isDemandAnchorMapDisplayReady(point) {
  if (/Suppressed during .* duplicate cleanup/i.test(String(point.notes || ""))) return false;
  if (point.includeOnRadarMap === false) return false;
  const lat = point.latitude;
  const lng = point.longitude;
  if (!hasValidCoordinates(lat, lng)) return false;
  if (!isInCountryBbox(point.country, lat, lng)) return false;
  if (MAP_REQUIRE_VERIFIED_COUNTRIES.has(String(point.country || "").trim())) {
    if (!String(point.lastVerified || "").trim()) return false;
  }
  return true;
}

export function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
