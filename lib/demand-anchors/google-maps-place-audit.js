/**
 * Cross-reference demand anchor names/locations with geocoding providers.
 * Uses Google Geocoding API when GOOGLE_MAPS_API_KEY is set; otherwise Nominatim (OSM).
 */

import {
  buildGeocodeQueries,
  haversineKm,
  hasValidCoordinates,
  isInCountryBbox,
} from "./coordinate-verification.js";
import { DR_CURATED_MAP_PLACES } from "../radar-buildout/dominican-republic-curated-map-places.js";

const GOOGLE_KEY =
  process.env.GOOGLE_MAPS_API_KEY ||
  process.env.GOOGLE_GEOCODING_API_KEY ||
  "";

const NOMINATIM_UA =
  process.env.NOMINATIM_USER_AGENT || "deal-capture-proxy/1.0 (maps audit)";

/** Names that are intentional corridor/zone labels — verify location only, not POI name. */
export const GENERIC_ANCHOR_NAME =
  /corridor|coastline|growth zone|growth node|business district|eco-tourism|resort growth|gateway|access node|logistics zone|industrial zone|city center|commercial hub|waterfront promenade|whale watching corridor|maritime terminal|highway access|national park|lake \/ nature|highland|mountain valley|beach resort coast|resort coast|tourism hub|entertainment corridor|government complex|medical center logistics|cruise port|ferry terminal|bus terminal|airport$/i;

export function normalizeForCompare(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function tokenSet(s) {
  const stop = new Set(["the", "de", "del", "la", "el", "los", "las", "and", "of", "dr", "rd"]);
  return new Set(
    normalizeForCompare(s)
      .split(" ")
      .filter((t) => t.length > 2 && !stop.has(t))
  );
}

/** Jaccard similarity 0–1 */
export function nameSimilarity(a, b) {
  const A = tokenSet(a);
  const B = tokenSet(b);
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter += 1;
  return inter / (A.size + B.size - inter);
}

export function isGenericAnchorName(name) {
  return GENERIC_ANCHOR_NAME.test(String(name || ""));
}

export function extractPlaceName(hit, provider) {
  if (!hit) return "";
  if (provider === "google") {
    for (const c of hit.address_components || []) {
      if (c.types?.includes("establishment") || c.types?.includes("point_of_interest")) {
        return c.long_name;
      }
    }
    return hit.formatted_address?.split(",")[0] || "";
  }
  return hit.name || String(hit.display_name || "").split(",")[0] || "";
}

async function googleGeocode(query) {
  if (!GOOGLE_KEY) return [];
  const url =
    "https://maps.googleapis.com/maps/api/geocode/json?" +
    new URLSearchParams({ address: query, key: GOOGLE_KEY, region: "do" });
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Google geocode HTTP ${res.status}`);
  const data = await res.json();
  if (data.status !== "OK" && data.status !== "ZERO_RESULTS") {
    throw new Error(`Google geocode ${data.status}: ${data.error_message || ""}`);
  }
  return (data.results || []).map((r) => ({
    lat: r.geometry.location.lat,
    lng: r.geometry.location.lng,
    display_name: r.formatted_address,
    name: extractPlaceName(r, "google"),
    types: r.types || [],
    raw: r,
  }));
}

async function nominatimGeocode(query, countryCode = "do", attempt = 0) {
  const url =
    "https://nominatim.openstreetmap.org/search?" +
    new URLSearchParams({
      q: query,
      format: "json",
      limit: "3",
      addressdetails: "1",
      countrycodes: countryCode,
    });
  const res = await fetch(url, { headers: { "User-Agent": NOMINATIM_UA } });
  if (res.status === 429 && attempt < 4) {
    const wait = 15000 * (attempt + 1);
    await new Promise((r) => setTimeout(r, wait));
    return nominatimGeocode(query, countryCode, attempt + 1);
  }
  if (!res.ok) throw new Error(`Nominatim HTTP ${res.status}`);
  const hits = await res.json();
  return (hits || []).map((h) => ({
    lat: Number(h.lat),
    lng: Number(h.lon),
    display_name: h.display_name,
    name: h.name || String(h.display_name || "").split(",")[0],
    types: [h.type, h.class].filter(Boolean),
    raw: h,
  }));
}

async function searchBestMatch(point, opts = {}) {
  const provider = GOOGLE_KEY && !opts.forceOsm ? "google" : "nominatim";
  const country = String(point.country || "Dominican Republic");
  const cc = country === "Puerto Rico" ? "us" : "do";

  let best = null;
  let matchedQuery = null;

  for (const q of buildGeocodeQueries(point)) {
    const hits =
      provider === "google"
        ? await googleGeocode(q)
        : await nominatimGeocode(q, cc);
    for (const hit of hits) {
      if (!hasValidCoordinates(hit.lat, hit.lng)) continue;
      const drift = haversineKm(point.latitude, point.longitude, hit.lat, hit.lng);
      const refName = hit.name || hit.display_name;
      const sim = nameSimilarity(point.name, refName);
      const score = sim * 2 - drift; // prefer name match + close distance
      if (!best || score > best.score) {
        best = { ...hit, driftKm: drift, nameSimilarity: sim, score, provider };
        matchedQuery = q;
      }
    }
    if (best && best.driftKm <= 0.5 && best.nameSimilarity >= 0.35) break;
  }

  return best ? { ...best, matchedQuery, provider } : { provider, matchedQuery: null };
}

/**
 * Audit one demand anchor against maps reference.
 * @returns {Promise<object>}
 */
export async function auditDemandAnchorAgainstMaps(point) {
  const generic = isGenericAnchorName(point.name);
  const curated = DR_CURATED_MAP_PLACES[point.name];
  const match = await searchBestMatch(point);

  if (!match?.lat && curated) {
    const drift = haversineKm(point.latitude, point.longitude, curated.latitude, curated.longitude);
    return {
      id: point.id,
      name: point.name,
      city: point.city,
      latitude: point.latitude,
      longitude: point.longitude,
      genericName: generic,
      provider: "curated",
      referenceName: curated.name || point.name,
      referenceAddress: curated.address,
      referenceLat: curated.latitude,
      referenceLng: curated.longitude,
      driftKm: Number(drift.toFixed(2)),
      nameSimilarity: 1,
      status: drift >= 0.5 ? "location_drift" : "ok",
      action: drift >= 0.5 ? "update_location" : "none",
      suggestedName: curated.name && curated.name !== point.name ? curated.name : null,
    };
  }

  if (!match?.lat) {
    return {
      id: point.id,
      name: point.name,
      city: point.city,
      latitude: point.latitude,
      longitude: point.longitude,
      status: "no_match",
      genericName: generic,
      provider: match?.provider || (GOOGLE_KEY ? "google" : "nominatim"),
      action: generic ? "review_manual" : "hide_or_review",
    };
  }

  const suggestedName = match.name || match.display_name?.split(",")[0] || "";
  const nameSim = match.nameSimilarity ?? nameSimilarity(point.name, suggestedName);
  const drift = match.driftKm;

  let status = "ok";
  let action = "none";

  if (drift >= 2) {
    status = "location_far";
    action = "update_location";
  } else if (drift >= 0.5) {
    status = "location_drift";
    action = "update_location";
  }

  if (!generic && nameSim < 0.25 && drift <= 2) {
    status = status === "ok" ? "name_mismatch" : status + "_name";
    action = action === "none" ? "review_name" : action + "_and_name";
  } else if (!generic && nameSim < 0.4 && suggestedName && drift <= 0.5) {
    status = status === "ok" ? "name_weak" : status;
    if (action === "none") action = "review_name";
  }

  if (generic && drift < 0.5) {
    status = "ok";
    action = "none";
  }

  return {
    id: point.id,
    name: point.name,
    city: point.city,
    latitude: point.latitude,
    longitude: point.longitude,
    genericName: generic,
    provider: match.provider,
    matchedQuery: match.matchedQuery,
    referenceName: suggestedName,
    referenceAddress: match.display_name,
    referenceLat: match.lat,
    referenceLng: match.lng,
    driftKm: Number(drift.toFixed(2)),
    nameSimilarity: Number(nameSim.toFixed(2)),
    status,
    action,
    suggestedName: !generic && nameSim >= 0.35 && nameSim < 0.85 ? suggestedName : null,
  };
}

export function shouldAutoApply(audit) {
  if (audit.action === "none" || audit.status === "ok" || audit.status === "no_match") {
    return false;
  }
  if (audit.action.includes("update_location") && audit.driftKm >= 0.5 && audit.referenceLat) {
    return true;
  }
  return false;
}

export function buildAutoApplyPatch(audit, fields) {
  const patch = {};
  if (shouldAutoApply(audit)) {
    patch[fields.lat] = audit.referenceLat;
    patch[fields.lng] = audit.referenceLng;
  }
  if (
    audit.suggestedName &&
    audit.nameSimilarity >= 0.35 &&
    audit.driftKm <= 1 &&
    !audit.genericName &&
    normalizeForCompare(audit.suggestedName) !== normalizeForCompare(audit.name)
  ) {
    patch[fields.name] = audit.suggestedName;
  }
  if (audit.referenceAddress && shouldAutoApply(audit)) {
    const addr = String(audit.referenceAddress).split(",").slice(0, 3).join(", ");
    if (addr.length <= 200) patch[fields.address] = addr;
  }
  return patch;
}

export function mapsAuditProviderLabel() {
  return GOOGLE_KEY ? "google" : "nominatim (OSM — set GOOGLE_MAPS_API_KEY for Google)";
}
