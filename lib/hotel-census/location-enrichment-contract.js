/**
 * Hotel Census Location field — STR location type contract.
 *
 * `Location` is the STR **location type** (physical setting), not city/country/Market.
 * See docs/hotel-census-location-population-rules.md
 */

import { CENSUS_FIELDS } from "./fields.js";
import { normalizeKey } from "../str-census-import/normalize.mjs";

/** STR location types observed in Hotel Census (CALA import). */
export const LOCATION_TYPE_LABELS = [
  "Urban",
  "Suburban",
  "Resort",
  "Small Metro/Town",
  "Airport",
  "Interstate",
];

export const LOCATION_TYPE_SET = new Set(LOCATION_TYPE_LABELS);

export const MAP_LOCATION_ENRICHMENT = {
  location: CENSUS_FIELDS.location,
  name: CENSUS_FIELDS.name,
  city: CENSUS_FIELDS.city,
  country: CENSUS_FIELDS.country,
  market: CENSUS_FIELDS.market,
  submarket: "Submarket",
  resortYn: "Resort (Y/N)",
  chainScale: CENSUS_FIELDS.chainScale,
  affiliation: CENSUS_FIELDS.affiliation,
};

export const LOCATION_SOURCE = {
  steward: "steward_verified",
  strLegacy: "str_legacy_import",
  nameAirport: "name_token_airport",
  nameResort: "name_token_resort",
  amenityResortYn: "amenity_resort_yn",
  peerCityCountry: "census_peer_city_country_mode",
  brandSteward: "brand_steward_plan",
};

const NAME_AIRPORT_RE = /\b(airport|aeropuerto|aeroporto|confins aeroporto)\b/i;
const NAME_RESORT_RE =
  /\b(resort|beach club|beachfront|all[- ]inclusive|all inclusive|spa resort|eco resort|nature resort|beach hotel)\b/i;

/**
 * @param {string} [value]
 */
export function isValidLocationType(value) {
  const v = String(value ?? "").trim();
  return v && LOCATION_TYPE_SET.has(v);
}

export function isBlankLocation(value) {
  if (value == null) return true;
  return String(value).trim() === "";
}

/**
 * Build city+country → location type counts from census rows that already have Location.
 * @param {Array<{ fields: object }>} records
 */
export function buildLocationPeerIndex(records) {
  /** @type {Map<string, Map<string, number>>} */
  const cityCountry = new Map();
  /** @type {Map<string, Map<string, number>>} */
  const countryOnly = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const loc = String(f[CENSUS_FIELDS.location] ?? "").trim();
    if (!isValidLocationType(loc)) continue;

    const city = String(f[CENSUS_FIELDS.city] ?? "").trim();
    const country = String(f[CENSUS_FIELDS.country] ?? "").trim();
    const cc = `${normalizeKey(city)}||${normalizeKey(country)}`;

    if (city && country) {
      if (!cityCountry.has(cc)) cityCountry.set(cc, new Map());
      const m = cityCountry.get(cc);
      m.set(loc, (m.get(loc) || 0) + 1);
    }

    if (country) {
      if (!countryOnly.has(country)) countryOnly.set(country, new Map());
      const m = countryOnly.get(country);
      m.set(loc, (m.get(loc) || 0) + 1);
    }
  }

  return { cityCountry, countryOnly };
}

function topCountedLocation(countMap) {
  let best = "";
  let bestN = 0;
  let tie = false;
  for (const [loc, n] of countMap) {
    if (n > bestN) {
      best = loc;
      bestN = n;
      tie = false;
    } else if (n === bestN && n > 0) {
      tie = true;
    }
  }
  if (!bestN) return { location: "", confidence: "none", tie: false };
  return {
    location: best,
    confidence: bestN >= 3 ? "medium" : "low",
    tie,
  };
}

/**
 * Propose STR location type for a census row (fill-blank automation).
 *
 * @param {object} row — census fields
 * @param {{ cityCountry: Map<string, Map<string, number>>, countryOnly: Map<string, Map<string, number>> }} peerIndex
 * @returns {{ location: string, source: string, confidence: string, skipped?: string[] }}
 */
export function proposeLocationType(row, peerIndex) {
  const skipped = [];
  const existing = String(row[CENSUS_FIELDS.location] ?? "").trim();
  if (existing) {
    if (isValidLocationType(existing)) {
      return { location: existing, source: LOCATION_SOURCE.steward, confidence: "high", skipped };
    }
    skipped.push(`invalid existing value: ${existing}`);
  }

  const name = String(row[CENSUS_FIELDS.name] ?? "");
  const nameKey = normalizeKey(name);
  const resortYn = String(row[MAP_LOCATION_ENRICHMENT.resortYn] ?? "").trim();

  if (NAME_AIRPORT_RE.test(name) || NAME_AIRPORT_RE.test(nameKey)) {
    return { location: "Airport", source: LOCATION_SOURCE.nameAirport, confidence: "high", skipped };
  }

  if (NAME_RESORT_RE.test(name) || NAME_RESORT_RE.test(nameKey)) {
    return { location: "Resort", source: LOCATION_SOURCE.nameResort, confidence: "high", skipped };
  }

  if (resortYn === "Y") {
    return { location: "Resort", source: LOCATION_SOURCE.amenityResortYn, confidence: "medium", skipped };
  }

  const city = String(row[CENSUS_FIELDS.city] ?? "").trim();
  const country = String(row[CENSUS_FIELDS.country] ?? "").trim();
  const cc = `${normalizeKey(city)}||${normalizeKey(country)}`;

  if (peerIndex?.cityCountry?.has(cc)) {
    const peer = topCountedLocation(peerIndex.cityCountry.get(cc));
    if (peer.location && !peer.tie) {
      return {
        location: peer.location,
        source: LOCATION_SOURCE.peerCityCountry,
        confidence: peer.confidence,
        skipped,
      };
    }
    if (peer.tie) skipped.push("city+country peer tie");
  }

  skipped.push("no rule matched — steward review");
  return { location: "", source: "", confidence: "none", skipped };
}

/**
 * @param {string} location
 */
export function validateLocationProposal(location) {
  if (!location) return { pass: false, errors: ["empty location"] };
  if (!LOCATION_TYPE_SET.has(location)) {
    return { pass: false, errors: [`"${location}" is not a valid STR location type`] };
  }
  return { pass: true, errors: [] };
}

/**
 * @param {{ location: string }} proposal
 */
export function locationProposalToAirtableFields(proposal) {
  if (!proposal.location) return {};
  return { [CENSUS_FIELDS.location]: proposal.location };
}
