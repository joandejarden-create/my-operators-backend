/**
 * Mapbox Permanent Geocoding provider for Hotel Property Census coordinates.
 *
 * Stored Census lat/long must use permanent=true only.
 * Never temporary geocoding. Never public Nominatim.
 */

import {
  isValidCoordPair,
  matchesRejectedPin,
} from "./production-census-coordinate-extractor.js";
import {
  isStreetLevelAddress,
  placeMatchesCensus,
} from "./production-census-geocoding-providers.js";
import { CALA_DISCOVERY_COUNTRY_ISO } from "./census-autopilot-cala-discovery-shared.js";
import { evaluateMapboxPermanentReadiness } from "./census-coordinate-provider.js";

export const MAPBOX_COORDINATE_PROVIDER_VERSION =
  "census-mapbox-coordinate-provider-v1";

export const MAPBOX_COORDINATE_STATUSES = Object.freeze({
  RESOLVED_HIGH: "resolved_high_confidence",
  PROVIDER_DECISION_NEEDED: "provider_decision_needed",
  NO_ADDRESS: "no_address",
  LOW_CONFIDENCE: "low_confidence",
  CITY_CENTROID_REJECTED: "city_centroid_rejected",
  ZERO_ZERO_REJECTED: "zero_zero_rejected",
  COUNTRY_MISMATCH: "country_mismatch",
  SOURCE_CONFLICT: "source_conflict",
  STEWARD_REVIEW_REQUIRED: "steward_review_required",
  PROVIDER_ERROR: "provider_error",
});

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function countryIso(country) {
  const c = String(country || "").trim();
  if (!c) return null;
  if (/^[a-z]{2}$/i.test(c)) return c.toLowerCase();
  return CALA_DISCOVERY_COUNTRY_ISO[c] || null;
}

/**
 * Build Mapbox forward query from address parts (no city-only / country-only).
 */
export function buildMapboxOfficialAddressQuery({
  propertyName,
  address,
  city,
  stateRegion,
  country,
  omitPropertyName = false,
}) {
  const a = String(address || "").trim();
  const cityS = String(city || "").trim();
  const countryS = String(country || "").trim();
  if (!a || !isStreetLevelAddress(a)) return null;
  if (!cityS || /^unknown$/i.test(cityS)) return null;
  if (!countryS) return null;
  if (a.toLowerCase() === cityS.toLowerCase()) return null;
  if (a.toLowerCase() === countryS.toLowerCase()) return null;

  const parts = [];
  const name = String(propertyName || "").trim();
  if (name && !omitPropertyName) parts.push(name);
  parts.push(a);
  if (!a.toLowerCase().includes(cityS.toLowerCase())) parts.push(cityS);
  const state = String(stateRegion || "").trim();
  if (state && !a.toLowerCase().includes(state.toLowerCase())) parts.push(state);
  if (!a.toLowerCase().includes(countryS.toLowerCase())) parts.push(countryS);
  return parts.filter(Boolean).join(", ");
}

function isCityOrAdminCentroid(feature) {
  const types = feature?.place_type || [];
  if (!types.length) return true;
  if (types.includes("address")) return false;
  // POI with street context can be property-level; pure place/region/country = centroid
  const adminOnly = types.every((t) =>
    ["place", "region", "country", "district", "locality", "neighborhood", "postcode"].includes(
      t
    )
  );
  return adminOnly;
}

/**
 * Extract locality / region / postcode / country from a Mapbox feature.
 * @param {object} feature
 */
export function extractMapboxFeatureContext(feature) {
  const ctx = {
    city: null,
    region: null,
    postcode: null,
    country: null,
    country_code: null,
  };
  const list = Array.isArray(feature?.context) ? feature.context : [];
  for (const item of list) {
    const id = String(item?.id || "");
    const text = String(item?.text || "").trim();
    if (!text) continue;
    if (id.startsWith("place.") || id.startsWith("locality.")) {
      if (!ctx.city) ctx.city = text;
    } else if (id.startsWith("region.")) {
      ctx.region = text;
    } else if (id.startsWith("postcode.")) {
      ctx.postcode = text;
    } else if (id.startsWith("country.")) {
      ctx.country = text;
      ctx.country_code = String(item?.short_code || "").toLowerCase() || null;
    }
  }
  // Some responses put locality on the feature itself
  if (!ctx.city && Array.isArray(feature?.place_type)) {
    if (feature.place_type.includes("place") || feature.place_type.includes("locality")) {
      ctx.city = String(feature.text || "").trim() || ctx.city;
    }
  }
  return ctx;
}

function normalizeLoose(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
}

/**
 * Soft postal agreement when Census has a postal code.
 */
function postalAgrees(censusPostal, mapboxPostal) {
  const a = normalizeLoose(censusPostal);
  const b = normalizeLoose(mapboxPostal);
  if (!a || !b) return { ok: true, soft: true };
  if (a === b) return { ok: true, soft: false };
  // Allow partial (BR CEP truncated vs full)
  if (a.length >= 5 && b.length >= 5 && (a.startsWith(b.slice(0, 5)) || b.startsWith(a.slice(0, 5)))) {
    return { ok: true, soft: true };
  }
  return { ok: false, soft: false };
}

function resultShell(status, extras = {}) {
  return {
    status,
    latitude: extras.latitude ?? null,
    longitude: extras.longitude ?? null,
    provider: "Mapbox",
    method: "official_address_geocode",
    confidence: extras.confidence ?? null,
    sourceType: "official_address_geocode",
    reason: extras.reason ?? status,
    reviewedDate: todayIsoDate(),
    permanent: extras.permanent ?? true,
    query: extras.query ?? null,
    place_name: extras.place_name ?? null,
    relevance: extras.relevance ?? null,
    place_types: extras.place_types ?? null,
    ...extras,
  };
}

/**
 * Resolve property coordinates via Mapbox Permanent Geocoding.
 *
 * @param {{
 *   propertyName?: string,
 *   brand?: string,
 *   address?: string,
 *   city?: string,
 *   stateRegion?: string,
 *   country?: string,
 *   sourceUrl?: string,
 * }} input
 * @param {{
 *   env?: NodeJS.ProcessEnv|Record<string,string|undefined>,
 *   fetchImpl?: typeof fetch,
 *   allowTemporary?: boolean,
 * }} [opts]
 */
export async function resolveMapboxCoordinates(input = {}, opts = {}) {
  const env = opts.env || process.env;
  const fetchImpl = opts.fetchImpl || globalThis.fetch;
  const readiness = evaluateMapboxPermanentReadiness(env);

  if (!readiness.ready) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_DECISION_NEEDED, {
      reason: readiness.block_reason,
      missing_flags: readiness.missing_flags,
      permanent: false,
    });
  }

  // Hard block: never use temporary geocoding for stored Census coords
  if (opts.allowTemporary === true) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR, {
      reason: "temporary_geocoding_blocked_for_census_storage",
      permanent: false,
    });
  }
  if (String(env.MAPBOX_PERMANENT_GEOCODING || "").trim() !== "1") {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR, {
      reason: "fail_closed_permanent_mode_not_confirmed",
      permanent: false,
    });
  }

  // Prefer address-primary query (founder 2026-08-16) unless caller opts in name
  const omitPropertyName = opts.omitPropertyName !== false;

  const query = buildMapboxOfficialAddressQuery({
    ...input,
    omitPropertyName,
  });
  if (!query) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.NO_ADDRESS, {
      reason: "missing_or_non_street_address_or_city_country",
    });
  }

  const token = String(env.MAPBOX_ACCESS_TOKEN || env.MAPBOX_TOKEN || "").trim();
  const iso = countryIso(input.country);
  const params = new URLSearchParams({
    access_token: token,
    permanent: "true",
    limit: "1",
    autocomplete: "false",
    types: String(opts.types || "address"),
    language: "en",
  });
  // Fail closed — permanent must be exactly "true" on every stored request
  if (params.get("permanent") !== "true") {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR, {
      reason: "fail_closed_permanent_param_missing",
      permanent: false,
    });
  }
  if (iso) params.set("country", iso.toLowerCase());
  const proxLng = Number(opts.proximity?.longitude ?? opts.proximity?.lng);
  const proxLat = Number(opts.proximity?.latitude ?? opts.proximity?.lat);
  if (Number.isFinite(proxLng) && Number.isFinite(proxLat)) {
    params.set("proximity", `${proxLng},${proxLat}`);
  }

  const url =
    `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(query)}.json` +
    `?${params.toString()}`;

  // Never log the URL (contains access_token)
  let res;
  let json;
  try {
    res = await fetchImpl(url);
    json = await res.json().catch(() => ({}));
  } catch (err) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR, {
      reason: "mapbox_network_error",
      detail: err?.message || String(err),
      query,
      permanent: true,
      permanent_mode_confirmed: true,
    });
  }

  if (!res.ok) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR, {
      reason: `mapbox_http_${res.status}`,
      detail:
        typeof json === "object" && json
          ? { message: json.message || json.error || "http_error" }
          : {},
      query,
      permanent: true,
      permanent_mode_confirmed: true,
    });
  }

  const features = json.features || [];
  if (!features.length) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.LOW_CONFIDENCE, {
      reason: "mapbox_zero_results",
      confidence: "Low",
      query,
    });
  }

  const best = features[0];
  const [lng, lat] = best.center || [];
  const latN = Number(lat);
  const lngN = Number(lng);

  if (latN === 0 && lngN === 0) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.ZERO_ZERO_REJECTED, {
      reason: "null_island_0_0",
      latitude: 0,
      longitude: 0,
      confidence: "Low",
      query,
    });
  }

  if (!isValidCoordPair(latN, lngN)) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR, {
      reason: "mapbox_invalid_coords",
      query,
    });
  }

  if (isCityOrAdminCentroid(best)) {
    const types = best.place_type || [];
    const poiOk = opts.allowPoi === true && types.includes("poi");
    if (!poiOk) {
      return resultShell(MAPBOX_COORDINATE_STATUSES.CITY_CENTROID_REJECTED, {
        reason: "mapbox_place_type_not_address",
        latitude: latN,
        longitude: lngN,
        place_types: types,
        place_name: best.place_name || null,
        confidence: "Low",
        query,
      });
    }
  }

  const rejected = matchesRejectedPin(latN, lngN, {
    propertyName: input.propertyName,
  });
  if (rejected) {
    const status =
      rejected.label === "null island"
        ? MAPBOX_COORDINATE_STATUSES.ZERO_ZERO_REJECTED
        : MAPBOX_COORDINATE_STATUSES.CITY_CENTROID_REJECTED;
    return resultShell(status, {
      reason: `rejected_pin_${rejected.label}`,
      latitude: latN,
      longitude: lngN,
      confidence: "Low",
      query,
      place_name: best.place_name || null,
    });
  }

  const placeName = best.place_name || "";
  const mapboxCtx = extractMapboxFeatureContext(best);
  const match = placeMatchesCensus(placeName, {
    city: input.city,
    state: input.stateRegion,
    country: input.country,
  });
  if (match.failures?.includes("country_mismatch")) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.COUNTRY_MISMATCH, {
      reason: "result_country_mismatch",
      latitude: latN,
      longitude: lngN,
      place_name: placeName,
      failures: match.failures,
      confidence: "Low",
      query,
      context: mapboxCtx,
      permanent: true,
      permanent_mode_confirmed: true,
    });
  }
  if (!match.ok) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.LOW_CONFIDENCE, {
      reason: "place_mismatch",
      latitude: latN,
      longitude: lngN,
      place_name: placeName,
      failures: match.failures,
      confidence: "Low",
      query,
      context: mapboxCtx,
      permanent: true,
      permanent_mode_confirmed: true,
    });
  }

  // Postal conflict when Census postal present and Mapbox disagrees materially
  if (input.postalCode) {
    const postalCheck = postalAgrees(input.postalCode, mapboxCtx.postcode);
    if (!postalCheck.ok) {
      return resultShell(MAPBOX_COORDINATE_STATUSES.SOURCE_CONFLICT, {
        reason: "postal_code_context_conflict",
        latitude: latN,
        longitude: lngN,
        place_name: placeName,
        confidence: "Low",
        query,
        context: mapboxCtx,
        permanent: true,
        permanent_mode_confirmed: true,
      });
    }
  }

  const relevance = Number(best.relevance || 0);
  const placeTypes = best.place_type || [];
  const minRelevance = Number(opts.minRelevance);
  const relevanceFloor = Number.isFinite(minRelevance) ? minRelevance : 0.85;
  // Prefer house numbers (1–3 digits / 50A). Explicit No./# civic numbers may be 4–5 digits
  // (common in MX); bare 4–5 digit tokens are treated as postal and ignored.
  const digitCandidates = String(input.address || "").match(/\b(\d{1,5}[A-Za-z]?)\b/g) || [];
  const explicitCivic =
    String(input.address || "").match(
      /(?:\bno\.?\s*|\bn[ºo°]\s*|#\s*)(\d{1,5}[A-Za-z]?)\b/i
    )?.[1] || null;
  const streetDigits =
    (explicitCivic && /^\d{1,5}[A-Za-z]?$/i.test(explicitCivic) ? explicitCivic : null) ||
    digitCandidates.find((d) => /^\d{1,3}[A-Za-z]?$/i.test(d)) ||
    null;
  const digitInPlace =
    Boolean(streetDigits) &&
    new RegExp(`\\b${String(streetDigits).replace(/[A-Za-z]/g, "")}\\b`).test(
      String(placeName)
    );
  const addrTokens = String(input.address || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 5);
  const STOP_GEO_TOKENS = new Set([
    "punta",
    "cana",
    "dominican",
    "republic",
    "hotel",
    "resort",
    "beach",
    "playa",
    "avenue",
    "street",
    "carretera",
    "highway",
    "boulevard",
    "santo",
    "domingo",
    "santiago",
    "romana",
    "miches",
    "bavaro",
    "higuey",
    "macao",
    "arena",
    "gorda",
    "altagracia",
    "distrito",
    "nacional",
    "caballeros",
  ]);
  const placeNorm = String(placeName)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  const sharedStreetToken = addrTokens.some(
    (t) => !STOP_GEO_TOKENS.has(t) && placeNorm.includes(t)
  );
  // Reject "address" hits that are really just the city (+ optional postal).
  const placeLooksLikeCityOnly =
    /^(punta cana|santo domingo|santiago|miches|cap cana|bavaro|bávaro)(\s+\d{4,5})?\b/i.test(
      String(placeName).trim()
    ) &&
    !/\b(calle|av\.?|ave|avenue|carr\.?|carretera|blvd|boulevard|street|st\.|km|maximo|gomez|sarasota|washington|juanillo)\b/i.test(
      placeName
    );
  // When address names an explicit street type + route number (Carrera 3 / Calle 10),
  // require the same route number in the Mapbox place (block Carrera 13 for Carrera 3).
  const addrStreetRoute = String(input.address || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .match(
      /\b(carrera|calle|avenida|boulevard|av|blvd|cra)\.?\s*(\d+[a-z]?)\b/
    );
  let streetRouteAligned = null;
  if (addrStreetRoute) {
    const streetType = String(addrStreetRoute[1])
      .replace(/^cra$/, "carrera")
      .replace(/^av$/, "avenida")
      .replace(/^blvd$/, "boulevard");
    const routeNum = addrStreetRoute[2];
    const placeHasSame =
      new RegExp(`\\b${streetType}\\s+${routeNum}\\b`, "i").test(placeNorm);
    const placeHasOther =
      new RegExp(`\\b${streetType}\\s+\\d+`, "i").test(placeNorm) && !placeHasSame;
    streetRouteAligned = placeHasSame ? true : placeHasOther ? false : null;
  }
  // Exact house-number address hits — accept lower relevance when digit + city match.
  // Soft path requires shared street token when relevance is below the hard floor,
  // and never accepts a conflicting street-route number.
  const softAddressHigh =
    placeTypes.includes("address") &&
    digitInPlace &&
    !placeLooksLikeCityOnly &&
    streetRouteAligned !== false &&
    relevance >= (streetRouteAligned === true ? 0.6 : 0.65) &&
    match.ok &&
    (relevance >= relevanceFloor || sharedStreetToken || streetRouteAligned === true);
  // Strong shared street token (e.g. Juanillo / Sarasota / Gomez) with city match.
  const softTokenAddressHigh =
    placeTypes.includes("address") &&
    sharedStreetToken &&
    !placeLooksLikeCityOnly &&
    streetRouteAligned !== false &&
    relevance >= 0.72 &&
    match.ok;
  const high =
    !placeLooksLikeCityOnly &&
    streetRouteAligned !== false &&
    (softAddressHigh ||
      softTokenAddressHigh ||
      (relevance >= relevanceFloor &&
        (placeTypes.includes("address") ||
          (opts.allowPoi === true && placeTypes.includes("poi"))) &&
        match.ok));

  if (!high) {
    return resultShell(MAPBOX_COORDINATE_STATUSES.LOW_CONFIDENCE, {
      reason: placeLooksLikeCityOnly
        ? "mapbox_city_only_place_rejected"
        : relevance < relevanceFloor
          ? "mapbox_low_relevance"
          : "not_address_high_bar",
      latitude: latN,
      longitude: lngN,
      place_name: placeName,
      relevance,
      place_types: placeTypes,
      confidence: "Medium",
      query,
    });
  }

  return resultShell(MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH, {
    latitude: latN,
    longitude: lngN,
    confidence: "High",
    reason: softAddressHigh
      ? "mapbox_permanent_address_street_number"
      : softTokenAddressHigh
        ? "mapbox_permanent_address_shared_token"
        : opts.allowPoi && placeTypes.includes("poi")
          ? "mapbox_permanent_poi_high"
          : "mapbox_permanent_official_address",
    query,
    place_name: placeName,
    relevance,
    place_types: placeTypes,
    permanent: true,
    permanent_mode_confirmed: true,
    context: mapboxCtx,
    geocode_method: "permanent_geocoding_official_address",
  });
}

/**
 * Normalize address+country for in-run cache keys.
 */
export function normalizeGeocodeCacheKey({ address, city, stateRegion, country }) {
  const parts = [address, city, stateRegion, country].map((p) =>
    String(p || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
  return parts.join("|");
}
