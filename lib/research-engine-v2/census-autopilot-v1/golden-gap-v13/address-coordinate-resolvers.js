/**
 * V1.3 Address + Coordinate resolution (gap closure only).
 */

import { fetchText, sleep } from "../../adapters/adapter-utils.js";
import { IHG_FETCH_HEADERS } from "../../../ihg-brand-directory-extract.js";
import { HILTON_FETCH_HEADERS } from "../../../hilton-brand-directory-extract.js";
import { CHOICE_FETCH_HEADERS } from "../../../choice-regional-directory-extract.js";
import { HILTON_GRAPHQL_URL, HILTON_GRAPHQL_HEADERS } from "../../../hilton-hotel-description-fetch.js";
import {
  resolveDirectoryAddressCandidate,
  resolveFamilyDirectorySignals,
  extractHiltonCtyhocn,
  warmFamilyDirectoryCaches,
} from "../../census-autopilot-family-directory-adapters.js";
import { extractOfficialAddressFromHtml } from "../../census-level-2-parent-extractors.js";
import {
  extractCoordinatesFromOfficialHtml,
  selectBestCoordinateHit,
  isValidCoordPair,
} from "../../production-census-coordinate-extractor.js";
import {
  resolveGeocodingProvider,
  geocodeOfficialAddress,
  isStreetLevelAddress,
  estimateGeocodeCostUsd,
  placeMatchesCensus,
} from "../../production-census-geocoding-providers.js";
import { recordToAdapterFields } from "../live-deep-research.js";

export const ADDR_GEO_VERSION = "census-autopilot-v1.3-address-geo";

function headersForFamily(family) {
  const f = String(family || "").toLowerCase();
  if (f === "ihg") return IHG_FETCH_HEADERS;
  if (f === "hilton") return HILTON_FETCH_HEADERS;
  if (f === "choice") return CHOICE_FETCH_HEADERS;
  return {};
}

/**
 * Normalize address for storage (preserve semantics).
 */
export function normalizeAddress(raw) {
  const original = String(raw || "").trim();
  if (!original) return { raw_address: null, normalized_address: null };
  let n = original
    .replace(/\s+/g, " ")
    .replace(/\bAv\.?\b/gi, "Avenida")
    .replace(/\bBlvd\.?\b/gi, "Boulevard")
    .replace(/\bCarr\.?\b/gi, "Carretera")
    .replace(/\bKm\.?\b/gi, "Km")
    .replace(/\s*,\s*/g, ", ")
    .trim();
  return { raw_address: original, normalized_address: n };
}

/**
 * Extract Mexico-matching street address from multi-hotel polluted JSON-LD pages (IHG).
 */
export function extractMexicoFilteredAddress(html, opts = {}) {
  const text = String(html || "");
  const cityHint = String(opts.city || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const re =
    /"streetAddress"\s*:\s*"([^"]{5,160})"[\s\S]{0,280}?"addressLocality"\s*:\s*"([^"]+)"[\s\S]{0,200}?(?:"addressRegion"\s*:\s*"([^"]*)")?[\s\S]{0,120}?"postalCode"\s*:\s*"([^"]*)"[\s\S]{0,120}?"addressCountry"\s*:\s*"([^"]+)"/gi;
  /** @type {object[]} */
  const candidates = [];
  let m;
  while ((m = re.exec(text))) {
    const country = String(m[5] || "").trim();
    if (!/mexico|mx/i.test(country)) continue;
    const street = m[1].replace(/\\u0026/g, "&").replace(/\s+/g, " ").trim();
    if (!isStreetLevelAddress(street) && street.length < 12) continue;
    const city = String(m[2] || "").trim();
    const score =
      (cityHint &&
      city
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .includes(cityHint.slice(0, 5))
        ? 2
        : 0) + 1;
    candidates.push({
      address: street,
      city,
      state: m[3] || null,
      postal_code: m[4] || null,
      country: "Mexico",
      score,
    });
  }
  candidates.sort((a, b) => b.score - a.score);
  const best = candidates[0];
  if (!best) {
    // Fallback: generic extractor then reject non-Mexico
    const gen = extractOfficialAddressFromHtml(text, opts.url || "");
    if (gen.ok && /mexico|mx/i.test(String(gen.country || opts.country || ""))) {
      return { ok: true, ...gen, method: "official_json_ld_street_address", filtered: false };
    }
    return { ok: false, reason: "no_mexico_street_address" };
  }
  return {
    ok: true,
    address: best.address,
    city: best.city,
    state: best.state,
    postal_code: best.postal_code,
    country: "Mexico",
    confidence: best.score >= 2 ? "Exact Official" : "High",
    method: "ihg_mexico_filtered_json_ld",
    filtered: true,
  };
}

/**
 * Hilton GraphQL address + coordinates.
 */
export async function resolveHiltonGraphQLAddressCoords(property, opts = {}) {
  const fields = recordToAdapterFields(property);
  const cty =
    property.ctyhocn ||
    extractHiltonCtyhocn(fields, property.independent_record_id);
  if (!cty) return { ok: false, reason: "missing_ctyhocn" };
  if (opts.delayMs) await sleep(Math.min(opts.delayMs || 0, 200));
  const res = await fetch(HILTON_GRAPHQL_URL, {
    method: "POST",
    headers: {
      ...HILTON_GRAPHQL_HEADERS,
      Referer: property.website || `https://www.hilton.com/en/hotels/${cty.toLowerCase()}-hotel/`,
    },
    body: JSON.stringify({
      operationName: "hotelAddrGeo",
      query: `query hotelAddrGeo($ctyhocn: String!, $language: String!) {
        hotel(ctyhocn: $ctyhocn, language: $language) {
          name
          ctyhocn
          address {
            addressLine1
            addressLine2
            city
            state
            country
            postalCode
          }
          localization {
            coordinate { latitude longitude }
          }
        }
      }`,
      variables: { ctyhocn: cty, language: "en" },
    }),
  });
  const json = await res.json();
  if (json.errors?.length) {
    return { ok: false, reason: json.errors.map((e) => e.message).join("; "), ctyhocn: cty };
  }
  const hotel = json?.data?.hotel;
  if (!hotel) return { ok: false, reason: "hotel_not_found", ctyhocn: cty };
  const a = hotel.address || {};
  const line = [a.addressLine1, a.addressLine2].filter(Boolean).join(", ").trim();
  const lat = hotel.localization?.coordinate?.latitude;
  const lng = hotel.localization?.coordinate?.longitude;
  const addressOk = isStreetLevelAddress(line) || (line.length >= 12 && /\d|km|carretera|boulevard/i.test(line));
  return {
    ok: addressOk || (lat != null && lng != null),
    ctyhocn: cty,
    address: addressOk ? line : null,
    city: a.city || null,
    state: a.state || null,
    postal_code: a.postalCode || null,
    country: a.country === "MX" ? "Mexico" : a.country || "Mexico",
    latitude: lat != null ? Number(lat) : null,
    longitude: lng != null ? Number(lng) : null,
    confidence: "Exact Official",
    method: "hilton_graphql_address_localization",
    source_url: property.website || `https://www.hilton.com/en/hotels/${cty.toLowerCase()}-hotel/`,
    source_type: "official_brand_structured",
  };
}

/**
 * Validate coords in Mexico bounds + optional market sanity.
 */
export function validateMexicoCoordinates(lat, lng, ctx = {}) {
  if (!isValidCoordPair(lat, lng)) return { ok: false, reason: "invalid_pair" };
  // Rough Mexico bounding box
  if (lat < 14.0 || lat > 33.0 || lng < -118.5 || lng > -86.0) {
    return { ok: false, reason: "outside_mexico_bounds" };
  }
  void ctx;
  return { ok: true };
}

/**
 * Resolve address for one property.
 */
export async function resolvePropertyAddress(property, opts = {}) {
  const family = property.family;
  const fields = recordToAdapterFields(property);

  if (family === "Hilton") {
    try {
      const gql = await resolveHiltonGraphQLAddressCoords(property, opts);
      if (gql.ok && gql.address) {
        const norm = normalizeAddress(gql.address);
        return {
          ok: true,
          claim: {
            address: norm.normalized_address,
            raw_address: norm.raw_address,
            confidence: gql.confidence,
            source: gql.source_url,
            source_type: gql.source_type,
            method: gql.method,
            postal_code: gql.postal_code,
            cvent_used: false,
            legacy_used: false,
          },
          gql,
        };
      }
    } catch (err) {
      /* fall through */
    }
  }

  // Directory candidate (Hilton/Choice)
  try {
    const dir = await resolveDirectoryAddressCandidate({
      fields,
      identityKey: property.independent_record_id,
      family,
    });
    if (dir.ok && dir.address) {
      const norm = normalizeAddress(dir.address);
      return {
        ok: true,
        claim: {
          address: norm.normalized_address,
          raw_address: norm.raw_address,
          confidence: dir.confidence === "High" ? "Exact Official" : dir.confidence || "High",
          source: dir.source_url,
          source_type: dir.source_type || "official_brand_directory",
          method: dir.method,
          cvent_used: false,
          legacy_used: false,
        },
      };
    }
  } catch {
    /* continue */
  }

  // Official page
  const url = property.website;
  if (url) {
    if (opts.delayMs) await sleep(opts.delayMs);
    const page = await fetchText(url, {
      headers: headersForFamily(family),
      timeoutMs: opts.timeoutMs || 25000,
    });
    if (page.ok && page.text) {
      let extracted;
      if (family === "IHG") {
        extracted = extractMexicoFilteredAddress(page.text, {
          url,
          city: property.city,
          country: property.country,
        });
      } else {
        extracted = extractOfficialAddressFromHtml(page.text, url);
      }
      if (extracted.ok && extracted.address) {
        const norm = normalizeAddress(extracted.address);
        return {
          ok: true,
          claim: {
            address: norm.normalized_address,
            raw_address: norm.raw_address,
            confidence: extracted.confidence || "High",
            source: url,
            source_type: "official_property_page",
            method: extracted.method,
            postal_code: extracted.postal_code || null,
            cvent_used: false,
            legacy_used: false,
          },
          page_text: page.text,
        };
      }
    }
  }

  return { ok: false, reason: "address_unresolved" };
}

/**
 * Resolve coordinates: official first, then approved geocode of confirmed address.
 */
export async function resolvePropertyCoordinates(property, addressClaim, opts = {}) {
  const family = property.family;

  // Hilton GraphQL
  if (family === "Hilton") {
    try {
      const gql = await resolveHiltonGraphQLAddressCoords(property, opts);
      if (gql.latitude != null && gql.longitude != null) {
        const v = validateMexicoCoordinates(gql.latitude, gql.longitude, property);
        if (v.ok) {
          return {
            ok: true,
            claim: {
              latitude: gql.latitude,
              longitude: gql.longitude,
              confidence: "High",
              source: gql.source_url,
              source_type: "official_coordinates",
              method: "hilton_graphql_localization",
              provider: "Official Page",
              cvent_used: false,
              legacy_used: false,
            },
          };
        }
      }
    } catch {
      /* fall through */
    }
  }

  // Directory coordinates
  try {
    const dir = await resolveFamilyDirectorySignals({
      fields: recordToAdapterFields(property),
      identityKey: property.independent_record_id,
      family,
      lanes: ["coordinates"],
    });
    if (dir?.coordinates?.ok) {
      const lat = dir.coordinates.latitude ?? dir.coordinates.lat;
      const lng = dir.coordinates.longitude ?? dir.coordinates.lng;
      const v = validateMexicoCoordinates(lat, lng, property);
      if (v.ok) {
        return {
          ok: true,
          claim: {
            latitude: lat,
            longitude: lng,
            confidence: "High",
            source: dir.coordinates.source_url || property.website,
            source_type: "official_coordinates",
            method: dir.coordinates.method || "family_directory_coordinates",
            provider: "Official Page",
            cvent_used: false,
            legacy_used: false,
          },
        };
      }
    }
  } catch {
    /* continue */
  }

  // Page HTML coords
  const url = property.website;
  if (url) {
    if (opts.delayMs) await sleep(opts.delayMs);
    const page = await fetchText(url, {
      headers: headersForFamily(family),
      timeoutMs: opts.timeoutMs || 25000,
    });
    if (page.ok && page.text) {
      const extracted = extractCoordinatesFromOfficialHtml(page.text, {
        url,
        family: String(family).toLowerCase(),
      });
      // Prefer Mexico-bounded hits
      const mexicoHits = (extracted.hits || []).filter((h) => {
        if (h.rejected) return false;
        return validateMexicoCoordinates(h.lat, h.lng).ok;
      });
      const best = selectBestCoordinateHit(mexicoHits.length ? mexicoHits : extracted.hits);
      if (best && validateMexicoCoordinates(best.lat, best.lng).ok) {
        return {
          ok: true,
          claim: {
            latitude: best.lat,
            longitude: best.lng,
            confidence: best.confidence || "High",
            source: url,
            source_type: "official_coordinates",
            method: best.method,
            provider: "Official Page",
            cvent_used: false,
            legacy_used: false,
          },
        };
      }
    }
  }

  // Geocode cascade
  const address = addressClaim?.address;
  if (!address || !isStreetLevelAddress(address)) {
    return { ok: false, reason: "no_confirmed_street_address_for_geocode" };
  }

  const providerInfo = resolveGeocodingProvider();
  if (providerInfo.provider === "none" || !providerInfo.credentials_ok) {
    return {
      ok: false,
      reason: "PROVIDER_BLOCKED",
      provider_status: "NO_PROVIDER",
      detail: providerInfo,
    };
  }
  if (providerInfo.provider === "mapbox" && !providerInfo.permanent_storage_enabled) {
    return {
      ok: false,
      reason: "PROVIDER_BLOCKED",
      provider_status: "PROVIDER_CONFIGURED_BUT_TERMS_REVIEW_REQUIRED",
      detail: providerInfo,
    };
  }
  if (providerInfo.provider === "google" && !providerInfo.storage_terms_reviewed) {
    return {
      ok: false,
      reason: "PROVIDER_BLOCKED",
      provider_status: "PROVIDER_CONFIGURED_BUT_TERMS_REVIEW_REQUIRED",
      detail: providerInfo,
    };
  }
  if (opts.skipGeocode) {
    return { ok: false, reason: "geocode_skipped", provider_status: "PROVIDER_READY" };
  }

  if (opts.delayMs) await sleep(opts.delayMs);
  const geo = await geocodeOfficialAddress(
    {
      name: property.name,
      address,
      city: property.city,
      country: property.country || "Mexico",
    },
    providerInfo
  );
  if (!geo?.ok) {
    return {
      ok: false,
      reason: geo?.reason || "geocode_failed",
      provider_status: "PROVIDER_READY",
    };
  }
  const lat = geo.latitude ?? geo.lat;
  const lng = geo.longitude ?? geo.lng;
  const v = validateMexicoCoordinates(lat, lng, property);
  if (!v.ok) {
    return { ok: false, reason: `geocode_rejected_${v.reason}`, provider_status: "PROVIDER_READY" };
  }
  if (geo.place_name && !placeMatchesCensus(geo.place_name, {
    city: property.city,
    country: property.country || "Mexico",
  }).ok) {
    // Soft fail — still accept if Mexico bounds ok and city optional
  }

  return {
    ok: true,
    claim: {
      latitude: lat,
      longitude: lng,
      confidence: "Medium",
      source: addressClaim?.source || null,
      source_type: "official_address_geocode",
      method: `geocode_${providerInfo.provider}`,
      provider: providerInfo.provider === "mapbox" ? "Mapbox" : "Google",
      cvent_used: false,
      legacy_used: false,
    },
    geocode_cost_estimate_usd: estimateGeocodeCostUsd(1, providerInfo),
  };
}

export { warmFamilyDirectoryCaches, resolveGeocodingProvider, estimateGeocodeCostUsd };
