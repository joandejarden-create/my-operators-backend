/**
 * Google Places hotel website lookup — report-only Official URL enrichment.
 *
 * Policy: google_places = lookup / restricted_refresh_required.
 * Never writes Airtable. Never stores photos/reviews.
 * googleMapsUri is denylisted for Official Property URL — use websiteUri only.
 */

import { resolveGoogleApiKey } from "../location-verification/google-api-config.js";
import {
  evaluatePlaceMatchConfidence,
  nameSimilarity,
} from "../location-verification/place-match-confidence.js";
import { websiteHost } from "./match-current-census.js";
import { isDeniedWebsite, isWeakWebsite } from "./intake-autopilot-gates.js";

export const GOOGLE_PLACES_HOTEL_URL_LOOKUP_VERSION =
  "google-places-hotel-url-lookup-v1";

const GOOGLE_ENDPOINT = "https://places.googleapis.com/v1/places:searchText";

/** Field mask for lodging URL enrichment (no photos/reviews/ratings). */
export const HOTEL_URL_FIELD_MASK = [
  "places.id",
  "places.displayName",
  "places.formattedAddress",
  "places.location",
  "places.types",
  "places.businessStatus",
  "places.googleMapsUri",
  "places.websiteUri",
  "places.nationalPhoneNumber",
].join(",");

/**
 * @param {object} place
 */
export function normalizeHotelPlaceFromSearch(place) {
  const loc = place?.location || {};
  return {
    googlePlaceId: place?.id || "",
    googleName: place?.displayName?.text || "",
    googleLatitude: Number.isFinite(Number(loc.latitude))
      ? Number(loc.latitude)
      : null,
    googleLongitude: Number.isFinite(Number(loc.longitude))
      ? Number(loc.longitude)
      : null,
    googleFormattedAddress: place?.formattedAddress || "",
    googleMapsUri: place?.googleMapsUri || "",
    googleWebsiteUri: place?.websiteUri || "",
    googlePhone: place?.nationalPhoneNumber || "",
    googleTypes: Array.isArray(place?.types) ? place.types : [],
    googleBusinessStatus: place?.businessStatus || "",
  };
}

/**
 * Text Search (New) with lodging bias + websiteUri.
 * @param {string} query
 * @param {string} apiKey
 * @param {object} [opts]
 */
export async function googleHotelTextSearch(query, apiKey, opts = {}) {
  const maxResults = Math.max(1, Math.min(20, Number(opts.maxResults) || 5));
  const body = {
    textQuery: query,
    pageSize: maxResults,
    includedType: "lodging",
  };
  if (
    Number.isFinite(Number(opts.latitude)) &&
    Number.isFinite(Number(opts.longitude))
  ) {
    body.locationBias = {
      circle: {
        center: {
          latitude: Number(opts.latitude),
          longitude: Number(opts.longitude),
        },
        radius: Number(opts.radiusMeters) || 50000,
      },
    };
  }

  const res = await fetch(GOOGLE_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Goog-Api-Key": apiKey,
      "X-Goog-FieldMask": HOTEL_URL_FIELD_MASK,
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(
      `Google hotel Text Search HTTP ${res.status}${text ? `: ${text.slice(0, 200)}` : ""}`
    );
  }
  const data = await res.json();
  return (data.places || []).map(normalizeHotelPlaceFromSearch).slice(0, maxResults);
}

function buildHotelSearchQueries(row) {
  const name = String(row.property_name || row.name || "").trim();
  const city = String(row.city || "").trim();
  const country = String(row.country || row.payload?.Country || "").trim();
  const brand = String(row.current_brand || row.brand || "").trim();
  const queries = [];
  if (name && city && city !== "Unknown" && country) {
    queries.push(`${name} hotel, ${city}, ${country}`);
  }
  if (name && brand && country) queries.push(`${name}, ${brand}, ${country}`);
  if (name && country) queries.push(`${name} hotel, ${country}`);
  if (name) queries.push(`${name} hotel`);
  return [...new Set(queries.filter(Boolean))];
}

function pickBestPlace(candidateName, hits) {
  if (!hits?.length) return null;
  let best = hits[0];
  let bestScore = -1;
  for (const hit of hits) {
    const score = nameSimilarity(candidateName, hit.googleName || "");
    if (score > bestScore) {
      bestScore = score;
      best = hit;
    }
  }
  return best;
}

/**
 * Classify whether Google websiteUri may be proposed as Official Property URL.
 * @param {string} websiteUri
 * @param {object} confidence
 */
export function classifyGoogleWebsiteProposal(websiteUri, confidence) {
  const url = String(websiteUri || "").trim();
  if (!url) {
    return {
      propose_as_official_url: false,
      reason: "no_website_uri",
      host: "",
    };
  }
  if (isDeniedWebsite(url)) {
    return {
      propose_as_official_url: false,
      reason: "website_denylisted_ota_social_or_google",
      host: websiteHost(url) || "",
    };
  }
  const high =
    confidence?.matchConfidence === "High" &&
    confidence?.verificationStatus === "Verified";
  const medium = confidence?.matchConfidence === "Medium";
  if (!high && !medium) {
    return {
      propose_as_official_url: false,
      reason: "match_confidence_too_low",
      host: websiteHost(url) || "",
    };
  }
  return {
    propose_as_official_url: true,
    reason: high
      ? "high_confidence_non_denylist_website"
      : "medium_confidence_non_denylist_website_steward",
    host: websiteHost(url) || "",
    weak: isWeakWebsite(url),
    requires_steward: !high || isWeakWebsite(url),
  };
}

/**
 * Lookup one census intake row via Google Places (report-only).
 * @param {object} row — intake plan / dual-lane style row
 * @param {object} [options]
 */
export async function lookupHotelOfficialUrlWithGoogle(row, options = {}) {
  const apiKey = options.apiKey || resolveGoogleApiKey();
  const searchFn =
    options.searchTextFn ||
    ((q, loc) =>
      googleHotelTextSearch(q, apiKey, {
        maxResults: options.maxResults ?? 5,
        latitude: loc?.latitude,
        longitude: loc?.longitude,
        radiusMeters: options.radiusMeters,
      }));

  const propertyName = String(row.property_name || row.name || "").trim();
  const lat = Number(row.payload?.Latitude ?? row.latitude);
  const lng = Number(row.payload?.Longitude ?? row.longitude);
  const loc =
    Number.isFinite(lat) && Number.isFinite(lng)
      ? { latitude: lat, longitude: lng }
      : null;

  const base = {
    version: GOOGLE_PLACES_HOTEL_URL_LOOKUP_VERSION,
    source_record_id: row.source_record_id || row.sourceRecordId || "",
    property_name: propertyName,
    current_brand: row.current_brand || "",
    city: row.city || "",
    country: row.payload?.Country || row.country || "",
    policy: {
      source_type: "google_places",
      can_use_in_product: "restricted_refresh_required",
      airtable_write: false,
      store_photos_reviews: false,
    },
  };

  if (!apiKey && !options.searchTextFn) {
    return {
      ...base,
      status: "skipped_no_api_key",
      query_used: "",
      place: null,
      website_proposal: classifyGoogleWebsiteProposal("", null),
      notes: "GOOGLE_PLACES_API_KEY / GOOGLE_MAPS_API_KEY missing",
    };
  }

  let queryUsed = "";
  let hits = [];
  let requests = 0;
  for (const q of buildHotelSearchQueries(row)) {
    queryUsed = q;
    hits = await searchFn(q, loc);
    requests += 1;
    if (hits.length) break;
  }

  const best = pickBestPlace(propertyName, hits);
  if (!best) {
    return {
      ...base,
      status: "no_match",
      query_used: queryUsed,
      requests,
      place: null,
      website_proposal: classifyGoogleWebsiteProposal("", null),
      competing_count: 0,
    };
  }

  const confidence = evaluatePlaceMatchConfidence({
    candidateName: propertyName,
    candidateCity: row.city === "Unknown" ? "" : row.city || "",
    candidateCountry: row.payload?.Country || row.country || "",
    candidateLatitude: loc?.latitude ?? null,
    candidateLongitude: loc?.longitude ?? null,
    candidatePointType: "hotel",
    result: best,
    competingResults: hits.filter((h) => h !== best),
  });

  const websiteProposal = classifyGoogleWebsiteProposal(
    best.googleWebsiteUri,
    confidence
  );

  return {
    ...base,
    status: "matched",
    query_used: queryUsed,
    requests,
    competing_count: Math.max(0, hits.length - 1),
    place: {
      google_place_id: best.googlePlaceId,
      google_name: best.googleName,
      google_formatted_address: best.googleFormattedAddress,
      google_website_uri: best.googleWebsiteUri || "",
      google_maps_uri: best.googleMapsUri || "",
      google_phone: best.googlePhone || "",
      google_business_status: best.googleBusinessStatus || "",
      latitude: best.googleLatitude,
      longitude: best.googleLongitude,
    },
    match_confidence: confidence.matchConfidence,
    verification_status: confidence.verificationStatus,
    verification_notes: confidence.verificationNotes || "",
    website_proposal: websiteProposal,
    suggested_official_property_url: websiteProposal.propose_as_official_url
      ? best.googleWebsiteUri
      : "",
  };
}

/**
 * Batch lookup with request budget + delay.
 * @param {object[]} rows
 * @param {object} [options]
 */
export async function runGooglePlacesHotelUrlLookupBatch(rows, options = {}) {
  const maxRequests = Number(options.maxRequests) || 40;
  const delayMs = Number(options.delayMs) || 250;
  const limit = Number(options.limit) || rows.length;
  const sleep = options.sleepFn || ((ms) => new Promise((r) => setTimeout(r, ms)));

  const results = [];
  let requestCount = 0;
  let skippedBudget = 0;

  for (const row of rows.slice(0, limit)) {
    if (requestCount >= maxRequests) {
      skippedBudget += 1;
      results.push({
        version: GOOGLE_PLACES_HOTEL_URL_LOOKUP_VERSION,
        source_record_id: row.source_record_id || "",
        property_name: row.property_name || "",
        status: "skipped_request_budget",
        notes: `maxRequests=${maxRequests}`,
      });
      continue;
    }

    const remaining = maxRequests - requestCount;
    const one = await lookupHotelOfficialUrlWithGoogle(row, {
      ...options,
      maxResults: Math.min(options.maxResults ?? 5, remaining),
    });
    requestCount += Number(one.requests) || 1;
    results.push(one);
    if (delayMs > 0 && options.searchTextFn == null) await sleep(delayMs);
  }

  const proposed = results.filter((r) => r.suggested_official_property_url);
  return {
    version: GOOGLE_PLACES_HOTEL_URL_LOOKUP_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry_run_report_only",
    airtable_write: false,
    input_count: rows.length,
    processed_count: results.filter((r) => r.status !== "skipped_request_budget")
      .length,
    request_count: requestCount,
    max_requests: maxRequests,
    skipped_budget: skippedBudget,
    matched: results.filter((r) => r.status === "matched").length,
    no_match: results.filter((r) => r.status === "no_match").length,
    proposed_official_url_count: proposed.length,
    high_confidence_proposals: proposed.filter(
      (r) => r.match_confidence === "High" && !r.website_proposal?.requires_steward
    ).length,
    results,
  };
}
