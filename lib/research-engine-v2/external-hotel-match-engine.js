/**
 * External hotel → Hotel Property Census match engine (read-only scoring).
 */

import {
  nameSimilarity,
  normalizeKey,
  citiesMatch,
  countriesMatch,
  distanceMeters,
  parseCoords,
} from "../independent-census/match-current-census.js";

export const EXTERNAL_HOTEL_MATCH_VERSION = "external-hotel-match-engine-v1";

export const MATCH_CONFIDENCE = Object.freeze({
  HIGH: "High",
  MEDIUM: "Medium",
  LOW: "Low",
  NONE: "None",
});

const MIN_HIGH_NAME = 0.85;
const MIN_MEDIUM_NAME = 0.7;
const MAX_GEO_METERS_HIGH = 150;
const MAX_GEO_METERS_MEDIUM = 500;

function blank(v) {
  return v == null || !String(v).trim();
}

function brandTokens(text) {
  return new Set(
    normalizeKey(text || "")
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 2)
  );
}

function brandOverlap(a, b) {
  const A = brandTokens(a);
  const B = brandTokens(b);
  if (!A.size || !B.size) return 0;
  let n = 0;
  for (const t of A) if (B.has(t)) n += 1;
  return n;
}

/**
 * Score one external candidate against one Census record.
 * @param {object} censusFields
 * @param {{
 *   name?: string,
 *   city?: string,
 *   country?: string,
 *   brand?: string,
 *   address?: string,
 *   lat?: number|string,
 *   lng?: number|string,
 *   external_id?: string,
 *   official_url?: string,
 *   is_closed?: boolean,
 *   is_residence_or_villa?: boolean,
 * }} external
 */
export function scoreExternalToCensusMatch(censusFields, external = {}) {
  const reasons = [];
  const conflicts = [];

  if (external.is_closed) {
    return {
      ok: false,
      confidence: MATCH_CONFIDENCE.NONE,
      reason: "source_property_closed_or_inactive",
      reasons,
      conflicts,
    };
  }
  if (external.is_residence_or_villa) {
    return {
      ok: false,
      confidence: MATCH_CONFIDENCE.NONE,
      reason: "source_residence_condo_villa_not_hotel_keys",
      reasons,
      conflicts,
    };
  }

  const censusName =
    censusFields["Canonical Property Name"] || censusFields["Property Name"] || "";
  const extName = external.name || "";
  if (blank(censusName) || blank(extName)) {
    return {
      ok: false,
      confidence: MATCH_CONFIDENCE.NONE,
      reason: "missing_name",
      reasons,
      conflicts,
    };
  }

  const nameSim = nameSimilarity(censusName, extName);
  reasons.push(`name_sim=${nameSim.toFixed(3)}`);

  const cityOk =
    blank(external.city) ||
    blank(censusFields.City) ||
    citiesMatch(censusFields.City, external.city) === true ||
    normalizeKey(censusFields.City) === normalizeKey(external.city);
  if (!cityOk) {
    return {
      ok: false,
      confidence: MATCH_CONFIDENCE.NONE,
      reason: "city_mismatch",
      name_sim: nameSim,
      reasons,
      conflicts,
    };
  }
  if (!blank(external.city) && !blank(censusFields.City)) {
    reasons.push("city_match");
  }

  const countryOk =
    blank(external.country) ||
    blank(censusFields.Country) ||
    countriesMatch(censusFields.Country, external.country);
  if (!countryOk) {
    return {
      ok: false,
      confidence: MATCH_CONFIDENCE.NONE,
      reason: "country_mismatch",
      name_sim: nameSim,
      reasons,
      conflicts,
    };
  }

  const censusBrand =
    censusFields["Current Brand"] || censusFields["Brand Family"] || "";
  const brandHits = brandOverlap(censusBrand, external.brand || extName);
  if (!blank(external.brand) && !blank(censusBrand) && brandHits === 0) {
    conflicts.push("brand_mismatch");
  } else if (brandHits > 0) {
    reasons.push(`brand_token_hits=${brandHits}`);
  }

  const censusCoords = parseCoords(
    censusFields.Latitude,
    censusFields.Longitude
  );
  const extCoords = parseCoords(external.lat, external.lng);
  let geoM = null;
  if (censusCoords && extCoords) {
    geoM = distanceMeters(censusCoords, extCoords);
    reasons.push(`geo_m=${geoM != null ? Math.round(geoM) : "n/a"}`);
  }

  let urlMatch = false;
  if (
    !blank(external.official_url) &&
    !blank(censusFields["Official Property URL"])
  ) {
    try {
      const a = new URL(String(external.official_url)).hostname.replace(/^www\./, "");
      const b = new URL(String(censusFields["Official Property URL"])).hostname.replace(
        /^www\./,
        ""
      );
      urlMatch = a === b;
      if (urlMatch) reasons.push("official_url_host_match");
    } catch {
      /* ignore */
    }
  }

  if (external.external_id) reasons.push(`external_id=${external.external_id}`);

  // Confidence ladder
  let confidence = MATCH_CONFIDENCE.NONE;
  let reason = "below_threshold";

  if (
    nameSim >= MIN_HIGH_NAME &&
    cityOk &&
    countryOk &&
    conflicts.length === 0 &&
    (brandHits > 0 || urlMatch || (geoM != null && geoM <= MAX_GEO_METERS_HIGH) || external.external_id)
  ) {
    confidence = MATCH_CONFIDENCE.HIGH;
    reason = "strong_name_city_plus_brand_or_geo_or_id";
  } else if (
    nameSim >= MIN_MEDIUM_NAME &&
    cityOk &&
    countryOk &&
    conflicts.length === 0 &&
    (brandHits > 0 || urlMatch || (geoM != null && geoM <= MAX_GEO_METERS_MEDIUM))
  ) {
    confidence = MATCH_CONFIDENCE.MEDIUM;
    reason = "medium_name_city_supporting_signal";
  } else if (nameSim >= MIN_MEDIUM_NAME && cityOk && countryOk && conflicts.length === 0) {
    confidence = MATCH_CONFIDENCE.LOW;
    reason = "name_city_only_ambiguous_risk";
  }

  // Same-brand multi-hotel city ambiguity flag (caller should supply siblings)
  return {
    ok: confidence === MATCH_CONFIDENCE.HIGH || confidence === MATCH_CONFIDENCE.MEDIUM,
    confidence,
    reason,
    name_sim: +nameSim.toFixed(3),
    geo_meters: geoM != null ? Math.round(geoM) : null,
    brand_hits: brandHits,
    url_match: urlMatch,
    reasons,
    conflicts,
    writable_confidence: confidence === MATCH_CONFIDENCE.HIGH || confidence === MATCH_CONFIDENCE.MEDIUM,
  };
}

/**
 * Pick best census match from a pool.
 * @param {object[]} censusRecords — { id, fields }
 * @param {object} external
 * @param {{ requireWritable?: boolean }} [opts]
 */
export function matchExternalToCensusPool(censusRecords, external, opts = {}) {
  const scored = [];
  for (const rec of censusRecords || []) {
    const m = scoreExternalToCensusMatch(rec.fields || {}, external);
    scored.push({
      census_record_id: rec.id,
      census_name:
        rec.fields?.["Canonical Property Name"] || rec.fields?.["Property Name"],
      ...m,
    });
  }
  scored.sort((a, b) => {
    const rank = { High: 3, Medium: 2, Low: 1, None: 0 };
    return (rank[b.confidence] || 0) - (rank[a.confidence] || 0) || b.name_sim - a.name_sim;
  });

  const best = scored[0] || null;
  const second = scored[1] || null;
  if (
    best &&
    second &&
    best.confidence === second.confidence &&
    Math.abs((best.name_sim || 0) - (second.name_sim || 0)) < 0.05
  ) {
    return {
      ok: false,
      reason: "ambiguous_multiple_census_matches",
      best,
      second,
      scored: scored.slice(0, 5),
    };
  }

  if (!best || best.confidence === MATCH_CONFIDENCE.NONE) {
    return { ok: false, reason: best?.reason || "no_match", best, scored: scored.slice(0, 5) };
  }

  if (opts.requireWritable && !best.writable_confidence) {
    return {
      ok: false,
      reason: "match_confidence_too_low_for_write",
      best,
      scored: scored.slice(0, 5),
    };
  }

  return {
    ok: best.ok,
    reason: best.reason,
    matched_census_record_id: best.census_record_id,
    match_confidence: best.confidence,
    best,
    scored: scored.slice(0, 5),
  };
}
