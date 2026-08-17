/**
 * Approved geocoding providers for Census address-first coordinate resolution.
 * GEOCODING_PROVIDER = mapbox | google | none
 *
 * Never use public Nominatim for bulk production.
 * Never geocode city-only / brand-only / country-only.
 */

import {
  isValidCoordPair,
  matchesRejectedPin,
} from "./production-census-coordinate-extractor.js";

export const GEOCODING_PROVIDERS = Object.freeze(["mapbox", "google", "none"]);

/** Rough USD per 1k requests for cost estimates (order-of-magnitude only). */
export const GEOCODE_COST_PER_1K_USD = Object.freeze({
  mapbox_temporary: 0.5,
  mapbox_permanent: 5.0,
  google: 5.0,
  none: 0,
});

export function isStreetLevelAddress(address) {
  const a = String(address || "").trim();
  return a.length >= 12 && /\d/.test(a);
}

export function buildOfficialGeocodeQuery({ name, address, city, state, country }) {
  const n = String(name || "").trim();
  const a = String(address || "").trim();
  if (!n || !isStreetLevelAddress(a)) return null;
  // Reject if address is only city/country noise
  const cityOnly = String(city || "").trim();
  if (cityOnly && a.toLowerCase() === cityOnly.toLowerCase()) return null;
  const parts = [n, a];
  if (city && !a.toLowerCase().includes(String(city).toLowerCase())) parts.push(city);
  if (state && !a.toLowerCase().includes(String(state).toLowerCase())) parts.push(state);
  if (country && !a.toLowerCase().includes(String(country).toLowerCase())) parts.push(country);
  return parts.filter(Boolean).join(", ");
}

/**
 * Resolve provider from env / explicit override.
 * Default preference: mapbox (if token) → google (if key) → none.
 */
export function resolveGeocodingProvider(explicit) {
  const raw = String(
    explicit || process.env.GEOCODING_PROVIDER || ""
  )
    .trim()
    .toLowerCase();

  if (raw === "none") {
    return { provider: "none", reason: "explicit_none", credentials_ok: true };
  }
  if (raw === "mapbox") {
    const token =
      String(process.env.MAPBOX_ACCESS_TOKEN || process.env.MAPBOX_TOKEN || "").trim() ||
      null;
    return {
      provider: "mapbox",
      reason: "explicit_mapbox",
      credentials_ok: Boolean(token),
      token_present: Boolean(token),
      permanent_storage_enabled:
        String(process.env.MAPBOX_PERMANENT_GEOCODING || "").trim() === "1",
    };
  }
  if (raw === "google") {
    const key = String(process.env.GOOGLE_MAPS_API_KEY || "").trim() || null;
    return {
      provider: "google",
      reason: "explicit_google",
      credentials_ok: Boolean(key),
      key_present: Boolean(key),
      storage_terms_reviewed:
        String(process.env.GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED || "").trim() === "1",
    };
  }

  // Auto-detect
  const mapboxToken =
    String(process.env.MAPBOX_ACCESS_TOKEN || process.env.MAPBOX_TOKEN || "").trim() ||
    null;
  if (mapboxToken) {
    return {
      provider: "mapbox",
      reason: "auto_mapbox_preferred",
      credentials_ok: true,
      token_present: true,
      permanent_storage_enabled:
        String(process.env.MAPBOX_PERMANENT_GEOCODING || "").trim() === "1",
    };
  }
  const googleKey = String(process.env.GOOGLE_MAPS_API_KEY || "").trim() || null;
  if (googleKey) {
    return {
      provider: "google",
      reason: "auto_google_fallback",
      credentials_ok: true,
      key_present: true,
      storage_terms_reviewed:
        String(process.env.GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED || "").trim() === "1",
    };
  }
  return { provider: "none", reason: "no_credentials", credentials_ok: true };
}

export function geocodingTermsWarnings(providerInfo) {
  const warnings = [];
  if (providerInfo.provider === "google") {
    warnings.push(
      "Google Geocoding API: do not permanently store lat/long in Airtable unless Dealality's Maps Platform terms allow storage/display for this use case. Set GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1 only after legal/founder review."
    );
    if (!providerInfo.storage_terms_reviewed) {
      warnings.push(
        "GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED is not set — dry-run may propose coords for review, but apply must not proceed until terms are confirmed."
      );
    }
  }
  if (providerInfo.provider === "mapbox") {
    warnings.push(
      "Mapbox: use an account/plan that permits permanent storage before writing coordinates to Airtable. Set MAPBOX_PERMANENT_GEOCODING=1 only when permanent geocoding is enabled."
    );
    if (!providerInfo.permanent_storage_enabled) {
      warnings.push(
        "MAPBOX_PERMANENT_GEOCODING is not set — temporary Mapbox results must not be written as permanent Census coordinates."
      );
    }
  }
  if (providerInfo.provider === "none") {
    warnings.push(
      "GEOCODING_PROVIDER=none (or no credentials): address-first lane can propose official page coordinates only; no address geocodes."
    );
  }
  warnings.push(
    "Do not use the public Nominatim/OSM endpoint for bulk production geocoding."
  );
  return warnings;
}

function normalizePlaceToken(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Soft geography agreement between Census fields and provider place text.
 */
export function placeMatchesCensus(placeText, census) {
  const hay = normalizePlaceToken(placeText);
  const city = normalizePlaceToken(census.city);
  const state = normalizePlaceToken(census.state);
  const country = normalizePlaceToken(census.country);
  const failures = [];
  if (city && city.length >= 3 && !hay.includes(city)) {
    // Allow common aliases
    const aliases = {
      "mexico city": ["ciudad de mexico", "cdmx", "df"],
      cancun: ["cancun", "cancún"],
      "puerto vallarta": ["vallarta"],
      // DR resort corridors — City label may be Cap Cana / Miches while Mapbox returns metro place text
      "cap cana": ["punta cana", "juanillo", "higüey", "higuey", "la altagracia"],
      miches: ["el seibo", "higuey", "higüey", "costa esmeralda"],
      "las terrenas": ["samana", "samaná"],
      bayahibe: ["la romana", "dominicus"],
      // MX metro / Choice
      monterrey: [
        "san nicolas de los garza",
        "san nicolás de los garza",
        "san pedro garza garcia",
        "san pedro garza garcía",
        "san pedro",
        "apodaca",
        "guadalupe",
        "nuevo leon",
        "nuevo león",
      ],
      zapopan: ["guadalajara", "jalisco"],
      guadalajara: ["zapopan", "jalisco"],
      "san jose": ["san josé", "alajuela", "heredia", "costa rica"],
      "san josé": ["san jose", "alajuela", "heredia", "costa rica"],
      torreon: ["torreón", "coahuila", "gomez palacio", "gómez palacio"],
      "torreón": ["torreon", "coahuila", "gomez palacio", "gómez palacio"],
      mexicali: ["baja california"],
      "mexico city": ["ciudad de mexico", "cdmx", "df"],
      bogota: ["bogotá", "cundinamarca"],
      "bogotá": ["bogota", "cundinamarca"],
      chame: ["panama oeste", "coronado", "playa caracol", "panama"],
      coronado: ["chame", "panama oeste", "panama"],
      amador: ["panama", "panama city", "ciudad de panama"],
      "panama city": ["amador", "panama"],
      chitre: ["chitré", "herrera"],
      "chitré": ["chitre", "herrera"],
      "cerro punta": ["volcan", "volcán", "chiriqui", "chiriquí", "bambito"],
      "juan dolio": ["san pedro de macoris", "san pedro de macorís"],
      cuajimalpa: ["cuajimalpa de morelos", "mexico city", "ciudad de mexico", "cdmx"],
      "cuajimalpa de morelos": ["cuajimalpa", "mexico city", "ciudad de mexico", "cdmx"],
      cucuta: ["cúcuta", "norte de santander"],
      "cúcuta": ["cucuta", "norte de santander"],
    };
    const alt = aliases[city] || [];
    if (!alt.some((a) => hay.includes(normalizePlaceToken(a)))) {
      failures.push("city_mismatch");
    }
  }
  if (state && state.length >= 3 && !hay.includes(state)) {
    failures.push("state_mismatch_soft");
  }
  if (country) {
    const countryOk =
      hay.includes(country) ||
      (country === "mexico" && (hay.includes("mexico") || hay.includes("mx")));
    if (!countryOk) failures.push("country_mismatch");
  }
  // Hard fail only on city or country; state soft
  const hard = failures.filter((f) => f !== "state_mismatch_soft");
  return { ok: hard.length === 0, failures, soft_only: failures };
}

async function geocodeMapbox(query, opts = {}) {
  const token = String(
    process.env.MAPBOX_ACCESS_TOKEN || process.env.MAPBOX_TOKEN || ""
  ).trim();
  if (!token) return { ok: false, reason: "MAPBOX_ACCESS_TOKEN_missing" };

  const permanent = String(process.env.MAPBOX_PERMANENT_GEOCODING || "").trim() === "1";
  const url =
    `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(query)}.json` +
    `?access_token=${encodeURIComponent(token)}` +
    `&limit=5&types=address,poi&language=en` +
    (permanent ? "&permanent=true" : "");

  const res = await fetch(url);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    return { ok: false, reason: `mapbox_http_${res.status}`, detail: json };
  }
  const features = json.features || [];
  if (!features.length) return { ok: false, reason: "mapbox_zero_results", query };

  // Prefer address relevance; reject if top result is weak
  const best = features[0];
  const [lng, lat] = best.center || [];
  if (!isValidCoordPair(Number(lat), Number(lng))) {
    return { ok: false, reason: "mapbox_invalid_coords" };
  }
  const rejected = matchesRejectedPin(Number(lat), Number(lng), {
    propertyName: opts.propertyName,
  });
  if (rejected) {
    return { ok: false, reason: "geocode_matches_rejected_pin", pin: rejected.label };
  }

  const placeName = best.place_name || "";
  const match = placeMatchesCensus(placeName, opts.census || {});
  if (!match.ok) {
    return {
      ok: false,
      reason: "place_mismatch",
      failures: match.failures,
      place_name: placeName,
      lat: Number(lat),
      lng: Number(lng),
    };
  }

  // Multiple high-relevance candidates with different coords → low confidence
  const altDifferent = features.slice(1, 3).some((f) => {
    const [alng, alat] = f.center || [];
    return (
      Math.abs(Number(alat) - Number(lat)) > 0.01 ||
      Math.abs(Number(alng) - Number(lng)) > 0.01
    );
  });

  const relevance = Number(best.relevance || 0);
  if (relevance < 0.6) {
    return { ok: false, reason: "mapbox_low_relevance", relevance, place_name: placeName };
  }

  let confidence = "Medium";
  if (relevance >= 0.85 && !altDifferent && (best.place_type || []).includes("address")) {
    confidence = "High";
  } else if (altDifferent) {
    return {
      ok: false,
      reason: "multiple_divergent_candidates",
      place_name: placeName,
      confidence: "Low",
    };
  }

  return {
    ok: true,
    lat: Number(lat),
    lng: Number(lng),
    provider: "mapbox",
    method: permanent ? "mapbox_permanent_geocode" : "mapbox_temporary_geocode",
    confidence,
    query,
    formatted_address: placeName,
    relevance,
    permanent,
    place_types: best.place_type || [],
  };
}

async function geocodeGoogle(query, opts = {}) {
  const key = String(process.env.GOOGLE_MAPS_API_KEY || "").trim();
  if (!key) return { ok: false, reason: "GOOGLE_MAPS_API_KEY_missing" };

  const url =
    `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(query)}` +
    `&key=${encodeURIComponent(key)}`;
  const res = await fetch(url);
  const json = await res.json();
  if (!res.ok || json.status !== "OK" || !json.results?.[0]) {
    return { ok: false, reason: `geocode_${json.status || res.status}`, query };
  }

  if ((json.results || []).length > 3) {
    // Too many candidates often means ambiguous
    // Still evaluate top if ROOFTOP
  }

  const top = json.results[0];
  const loc = top.geometry?.location;
  const lat = Number(loc?.lat);
  const lng = Number(loc?.lng);
  if (!isValidCoordPair(lat, lng)) {
    return { ok: false, reason: "geocode_invalid_coords" };
  }
  const rejected = matchesRejectedPin(lat, lng, { propertyName: opts.propertyName });
  if (rejected) {
    return { ok: false, reason: "geocode_matches_rejected_pin", pin: rejected.label };
  }

  const locType = top.geometry?.location_type;
  if (locType === "APPROXIMATE") {
    return { ok: false, reason: "geocode_approximate_only", lat, lng, confidence: "Low" };
  }

  const formatted = top.formatted_address || "";
  const match = placeMatchesCensus(formatted, opts.census || {});
  if (!match.ok) {
    return {
      ok: false,
      reason: "place_mismatch",
      failures: match.failures,
      formatted_address: formatted,
      lat,
      lng,
    };
  }

  // Guard against another hotel with similar name if types lack establishment/street
  const types = top.types || [];
  if (types.includes("locality") || types.includes("administrative_area_level_1")) {
    return { ok: false, reason: "geocode_admin_area_not_property", types, confidence: "Low" };
  }

  return {
    ok: true,
    lat,
    lng,
    provider: "google",
    method: "google_geocode_official_address",
    confidence: locType === "ROOFTOP" ? "High" : "Medium",
    query,
    formatted_address: formatted,
    location_type: locType || null,
    types,
    result_count: json.results.length,
  };
}

/**
 * Geocode official property name + street address only via selected provider.
 */
export async function geocodeOfficialAddress(input, providerInfo) {
  const provider = providerInfo?.provider || "none";
  if (provider === "none") {
    return { ok: false, reason: "provider_none" };
  }
  if (providerInfo && providerInfo.credentials_ok === false) {
    return { ok: false, reason: `${provider}_credentials_missing` };
  }

  const query = buildOfficialGeocodeQuery(input);
  if (!query) {
    return { ok: false, reason: "address_not_street_level_or_missing_name" };
  }

  const opts = {
    propertyName: input.name,
    census: {
      city: input.city,
      state: input.state,
      country: input.country,
    },
  };

  if (provider === "mapbox") return geocodeMapbox(query, opts);
  if (provider === "google") return geocodeGoogle(query, opts);
  return { ok: false, reason: `unsupported_provider_${provider}` };
}

export function estimateGeocodeCostUsd(requestCount, providerInfo) {
  const n = Number(requestCount) || 0;
  if (!n || !providerInfo || providerInfo.provider === "none") {
    return { requests: n, estimated_usd: 0, basis: "none" };
  }
  let rate = GEOCODE_COST_PER_1K_USD.google;
  let basis = "google";
  if (providerInfo.provider === "mapbox") {
    if (providerInfo.permanent_storage_enabled) {
      rate = GEOCODE_COST_PER_1K_USD.mapbox_permanent;
      basis = "mapbox_permanent";
    } else {
      rate = GEOCODE_COST_PER_1K_USD.mapbox_temporary;
      basis = "mapbox_temporary";
    }
  }
  return {
    requests: n,
    estimated_usd: Math.round((n * rate) / 1000 * 10000) / 10000,
    basis,
    rate_per_1k_usd: rate,
    note: "Order-of-magnitude estimate only; confirm current provider pricing.",
  };
}
