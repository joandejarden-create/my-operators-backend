/**
 * Match DataForSEO Maps / local-business hits to Hotel Property Census records.
 * Candidate-only — never Census writes from this module.
 */

import { nameSimilarity, normalizeKey } from "../independent-census/match-current-census.js";
import { tokenSimilarity } from "./adapters/adapter-utils.js";

export const DATAFORSEO_LOCAL_MATCH_VERSION = "dataforseo-local-match-v1";

export const MATCH_CLASS = Object.freeze({
  MATCH_HIGH: "match_high",
  MATCH_MEDIUM: "match_medium",
  MATCH_LOW: "match_low",
  DUPLICATE_RISK: "duplicate_risk",
  NEW_HOTEL_CANDIDATE: "new_hotel_candidate",
  REJECT: "reject",
});

export const LODGING_CLASS = Object.freeze({
  HOTEL: "hotel",
  HOSTEL: "hostel",
  VACATION_RENTAL: "vacation_rental",
  APARTMENT: "apartment",
  NON_HOTEL: "non_hotel_or_wrong_category",
  CLOSED: "closed_or_permanently_closed",
  UNSUPPORTED: "unsupported_lodging_type",
});

const HOSTEL_RE = /\b(hostel|backpacker|albergue)\b/i;
const VR_RE = /\b(vacation rental|airbnb|vrbo|holiday home|casa vacacional|short[- ]term)\b/i;
const APT_RE = /\b(apartment|apartamento|condo|residences?\b(?! inn)|serviced apartment)\b/i;
const HOTEL_RE =
  /\b(hotel|resort|inn|suites|lodge|posada|boutique|marriott|hilton|hyatt|ihg|accor|wyndham|choice|radisson)\b/i;
const NON_HOTEL_RE =
  /\b(restaurant|spa(?! hotel)|bar\b|cafe|café|nightclub|gym|parking|pharmacy|tienda|store)\b/i;
const CLOSED_RE = /\b(permanently closed|closed permanently|cerrado permanentemente)\b/i;

/**
 * @param {number} lat1
 * @param {number} lon1
 * @param {number} lat2
 * @param {number} lon2
 */
export function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Classify lodging type from Maps title/category.
 * @param {{ title?: string, category?: string, business_status?: string }} item
 */
export function classifyLodgingType(item = {}) {
  const hay = `${item.title || ""} ${item.category || ""} ${item.business_status || ""}`;
  if (CLOSED_RE.test(hay) || /permanently_closed/i.test(String(item.business_status || ""))) {
    return LODGING_CLASS.CLOSED;
  }
  if (HOSTEL_RE.test(hay)) return LODGING_CLASS.HOSTEL;
  if (VR_RE.test(hay)) return LODGING_CLASS.VACATION_RENTAL;
  if (APT_RE.test(hay) && !HOTEL_RE.test(hay)) return LODGING_CLASS.APARTMENT;
  if (NON_HOTEL_RE.test(hay) && !HOTEL_RE.test(hay)) return LODGING_CLASS.NON_HOTEL;
  if (HOTEL_RE.test(hay) || /hotel/i.test(String(item.category || ""))) {
    return LODGING_CLASS.HOTEL;
  }
  if (/lodging|accommodation|hospedaje/i.test(hay)) return LODGING_CLASS.UNSUPPORTED;
  return LODGING_CLASS.NON_HOTEL;
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function brandTokenOverlap(a, b) {
  const stop = new Set([
    "hotel",
    "hotels",
    "the",
    "and",
    "by",
    "de",
    "del",
    "la",
    "el",
    "a",
    "member",
    "of",
  ]);
  const tok = (s) =>
    String(s || "")
      .toLowerCase()
      .split(/[^a-z0-9áéíóúñü]+/i)
      .filter((t) => t.length > 2 && !stop.has(t));
  const ta = new Set(tok(a));
  const tb = new Set(tok(b));
  if (!ta.size || !tb.size) return 0;
  let n = 0;
  for (const t of ta) if (tb.has(t)) n += 1;
  return n / Math.max(ta.size, tb.size);
}

/**
 * Score one Maps item against one Census record.
 * @param {object} item — maps item
 * @param {object} censusFields
 * @param {{ recordId?: string }} [opts]
 */
export function scoreLocalBusinessToCensus(item, censusFields = {}, opts = {}) {
  const censusName =
    censusFields["Canonical Property Name"] ||
    censusFields["Property Name"] ||
    "";
  const itemName = item.title || item.name || "";
  const nameSim = Math.max(
    nameSimilarity(censusName, itemName),
    tokenSimilarity(censusName, itemName)
  );

  const censusCity = String(censusFields.City || "").toLowerCase();
  const itemAddr = String(item.address || "").toLowerCase();
  const itemCityHint = itemAddr;
  const cityMatch =
    !censusCity ||
    censusCity.length < 2 ||
    itemCityHint.includes(censusCity) ||
    tokenSimilarity(censusCity, itemCityHint) >= 0.35;

  const censusCountry = String(censusFields.Country || "").toLowerCase();
  const countryMatch =
    !censusCountry ||
    itemAddr.includes(censusCountry) ||
    // Maps location_name already scoped; soft pass when country known
    true;

  const brandHay = `${censusFields["Current Brand"] || ""} ${censusFields["Brand Family"] || ""}`;
  const brandOverlap = brandTokenOverlap(brandHay, itemName);

  const censusUrl = String(
    censusFields["Official Property URL"] || censusFields["Source URL"] || ""
  );
  const itemHost = hostFromUrl(item.url || item.website);
  const censusHost = hostFromUrl(censusUrl);
  const domainMatch =
    Boolean(itemHost) &&
    Boolean(censusHost) &&
    (itemHost === censusHost ||
      itemHost.endsWith(`.${censusHost}`) ||
      censusHost.endsWith(`.${itemHost}`));

  let geoKm = null;
  const cLat = Number(censusFields.Latitude);
  const cLng = Number(censusFields.Longitude);
  const iLat = Number(item.latitude);
  const iLng = Number(item.longitude);
  if (
    Number.isFinite(cLat) &&
    Number.isFinite(cLng) &&
    Number.isFinite(iLat) &&
    Number.isFinite(iLng)
  ) {
    geoKm = haversineKm(cLat, cLng, iLat, iLng);
  }

  let score = nameSim;
  if (cityMatch) score += 0.12;
  if (countryMatch) score += 0.05;
  if (brandOverlap >= 0.3) score += 0.1;
  if (domainMatch) score += 0.2;
  if (geoKm != null) {
    if (geoKm <= 0.15) score += 0.25;
    else if (geoKm <= 0.5) score += 0.15;
    else if (geoKm <= 1.5) score += 0.05;
    else if (geoKm > 5) score -= 0.25;
  }

  score = Math.max(0, Math.min(1, score));

  /** @type {string} */
  let match_class = MATCH_CLASS.MATCH_LOW;
  const reasons = [];
  if (!cityMatch) reasons.push("city_mismatch");
  if (geoKm != null && geoKm > 5) reasons.push("geo_far");
  if (nameSim < 0.35) reasons.push("weak_name");

  if (domainMatch && nameSim >= 0.45 && cityMatch) {
    match_class = MATCH_CLASS.MATCH_HIGH;
    reasons.push("domain_and_name");
  } else if (nameSim >= 0.72 && cityMatch && (geoKm == null || geoKm <= 1.5)) {
    match_class = MATCH_CLASS.MATCH_HIGH;
    reasons.push("strong_name_city_geo");
  } else if (nameSim >= 0.55 && cityMatch && brandOverlap >= 0.25) {
    match_class = MATCH_CLASS.MATCH_HIGH;
    reasons.push("name_city_brand");
  } else if (nameSim >= 0.5 && cityMatch) {
    match_class = MATCH_CLASS.MATCH_MEDIUM;
    reasons.push("medium_name_city");
  } else if (nameSim >= 0.35) {
    match_class = MATCH_CLASS.MATCH_LOW;
  } else {
    match_class = MATCH_CLASS.REJECT;
  }

  return {
    record_id: opts.recordId || null,
    match_class,
    match_confidence: Math.round(score * 1000) / 1000,
    name_similarity: Math.round(nameSim * 1000) / 1000,
    city_match: cityMatch,
    brand_overlap: Math.round(brandOverlap * 1000) / 1000,
    domain_match: domainMatch,
    geo_km: geoKm == null ? null : Math.round(geoKm * 1000) / 1000,
    reasons,
    write_eligible_future: match_class === MATCH_CLASS.MATCH_HIGH,
    field_approval_status: "candidate_only_pending_founder_approval",
    storage_policy_flag: "candidate_only_no_census_write",
  };
}

/**
 * Best Census match for a discovery Maps item among index rows.
 * @param {object} item
 * @param {Array<{ id: string, fields: object }>} censusRows
 */
export function matchDiscoveryItemToCensus(item, censusRows = []) {
  const lodging = classifyLodgingType(item);
  if (
    lodging === LODGING_CLASS.NON_HOTEL ||
    lodging === LODGING_CLASS.CLOSED ||
    lodging === LODGING_CLASS.VACATION_RENTAL ||
    lodging === LODGING_CLASS.APARTMENT ||
    lodging === LODGING_CLASS.HOSTEL ||
    lodging === LODGING_CLASS.UNSUPPORTED
  ) {
    return {
      lodging_class: lodging,
      match_class:
        lodging === LODGING_CLASS.CLOSED
          ? MATCH_CLASS.REJECT
          : lodging === LODGING_CLASS.NON_HOTEL
            ? MATCH_CLASS.REJECT
            : MATCH_CLASS.REJECT,
      discovery_class:
        lodging === LODGING_CLASS.CLOSED
          ? "non_hotel_or_wrong_category"
          : lodging === LODGING_CLASS.HOSTEL ||
              lodging === LODGING_CLASS.APARTMENT ||
              lodging === LODGING_CLASS.VACATION_RENTAL
            ? "unsupported_lodging_type"
            : "non_hotel_or_wrong_category",
      best: null,
      near_duplicates: [],
    };
  }

  const scored = [];
  for (const row of censusRows) {
    const s = scoreLocalBusinessToCensus(item, row.fields || {}, {
      recordId: row.id,
    });
    if (s.match_class !== MATCH_CLASS.REJECT || s.name_similarity >= 0.4) {
      scored.push(s);
    }
  }
  scored.sort((a, b) => b.match_confidence - a.match_confidence);
  const best = scored[0] || null;
  const near = scored
    .filter(
      (s) =>
        s !== best &&
        s.name_similarity >= 0.5 &&
        s.city_match &&
        (s.geo_km == null || s.geo_km <= 2)
    )
    .slice(0, 3);

  if (!best || best.match_class === MATCH_CLASS.REJECT) {
    return {
      lodging_class: lodging,
      match_class: MATCH_CLASS.NEW_HOTEL_CANDIDATE,
      discovery_class: "new_hotel_candidate",
      best: null,
      near_duplicates: near,
    };
  }

  if (near.length >= 1 && best.match_confidence - near[0].match_confidence < 0.08) {
    return {
      lodging_class: lodging,
      match_class: MATCH_CLASS.DUPLICATE_RISK,
      discovery_class: "possible_duplicate",
      best,
      near_duplicates: near,
    };
  }

  if (best.match_class === MATCH_CLASS.MATCH_HIGH) {
    return {
      lodging_class: lodging,
      match_class: MATCH_CLASS.MATCH_HIGH,
      discovery_class: "already_in_census",
      best,
      near_duplicates: near,
    };
  }
  if (best.match_class === MATCH_CLASS.MATCH_MEDIUM) {
    return {
      lodging_class: lodging,
      match_class: MATCH_CLASS.MATCH_MEDIUM,
      discovery_class: "possible_duplicate",
      best,
      near_duplicates: near,
    };
  }

  return {
    lodging_class: lodging,
    match_class: MATCH_CLASS.NEW_HOTEL_CANDIDATE,
    discovery_class: "new_hotel_candidate",
    best,
    near_duplicates: near,
  };
}

/**
 * Build a normalized local enrichment candidate payload (no writes).
 */
export function buildLocalEnrichmentCandidate(item, match, meta = {}) {
  return {
    endpoint: meta.endpoint || "serp/google/maps/live/advanced",
    source: "dataforseo_google_maps",
    dataforseo_is_source_of_truth: false,
    raw: {
      title: item.title || null,
      address: item.address || null,
      phone: item.phone || null,
      website: item.website || item.url || null,
      latitude: item.latitude ?? null,
      longitude: item.longitude ?? null,
      place_id: item.place_id || null,
      category: item.category || null,
      rating: item.rating ?? null,
      cid: item.cid || null,
    },
    match_confidence: match?.match_confidence ?? null,
    match_class: match?.match_class || null,
    matched_census_record_id: match?.record_id || null,
    match_reasons: match?.reasons || [],
    field_approval_status: "candidate_only_pending_founder_approval",
    storage_policy_flag: "candidate_only_no_census_write",
    rooms_write_from_maps: false,
    fields: {
      address: item.address
        ? { value: item.address, approval: "candidate_only" }
        : null,
      phone: item.phone
        ? { value: item.phone, approval: "candidate_only" }
        : null,
      website: item.website || item.url
        ? { value: item.website || item.url, approval: "candidate_only" }
        : null,
      latitude:
        item.latitude != null
          ? { value: item.latitude, approval: "candidate_only_needs_storage_policy" }
          : null,
      longitude:
        item.longitude != null
          ? { value: item.longitude, approval: "candidate_only_needs_storage_policy" }
          : null,
      place_id: item.place_id
        ? { value: item.place_id, approval: "candidate_only" }
        : null,
    },
  };
}

export { normalizeKey };
