/**
 * Hotel Census geography population contract.
 *
 * Governs how Region, Sub-Continent, Market, and Submarket are sourced and written.
 * See docs/hotel-census-geography-population-rules.md.
 */

import { CENSUS_FIELDS } from "./fields.js";
import {
  DEALALITY_REGION_LABELS,
  countryToDealalityRegion,
  resolveDealalityRegion,
} from "./region.js";

/** Airtable column (hyphenated). There is no separate "Sub Continent" column. */
export const CENSUS_SUB_CONTINENT_FIELD = "Sub-Continent";

export const MAP_GEOGRAPHY_ENRICHMENT = {
  region: CENSUS_FIELDS.region,
  subContinent: CENSUS_SUB_CONTINENT_FIELD,
  market: CENSUS_FIELDS.market,
  submarket: "Submarket",
  dealalityMarket: "Dealality Market",
};

/** CALA-focused sub-continent labels observed in census + STR-aligned grouping. */
export const SUB_CONTINENT_LABELS = [
  "Caribbean",
  "Central America",
  "South America",
  "North America",
];

/**
 * Country → Sub-Continent (Dealality CALA subdivision).
 * Mexico maps to North America (geographic) while Region remains CALA.
 */
export const COUNTRY_TO_SUB_CONTINENT = {
  // Caribbean
  Jamaica: "Caribbean",
  "Dominican Republic": "Caribbean",
  "Puerto Rico": "Caribbean",
  Cuba: "Caribbean",
  Bahamas: "Caribbean",
  Aruba: "Caribbean",
  "Curaçao": "Caribbean",
  Curacao: "Caribbean",
  "Cayman Islands": "Caribbean",
  "Trinidad and Tobago": "Caribbean",
  Barbados: "Caribbean",
  Haiti: "Caribbean",
  "Saint Lucia": "Caribbean",
  "Antigua and Barbuda": "Caribbean",
  Grenada: "Caribbean",
  "Saint Vincent and the Grenadines": "Caribbean",
  Dominica: "Caribbean",
  "Saint Kitts and Nevis": "Caribbean",
  "Turks and Caicos": "Caribbean",
  "Turks and Caicos Islands": "Caribbean",
  "Turks & Caicos": "Caribbean",
  "Turks & Caicos Islands": "Caribbean",
  "British Virgin Islands": "Caribbean",
  "U.S. Virgin Islands": "Caribbean",
  "US Virgin Islands": "Caribbean",
  Martinique: "Caribbean",
  Guadeloupe: "Caribbean",
  Bonaire: "Caribbean",
  Bermuda: "Caribbean",
  Anguilla: "Caribbean",
  "Sint Maarten (Dutch part)": "Caribbean",
  "Sint Maarten": "Caribbean",
  // Central America
  "Costa Rica": "Central America",
  Panama: "Central America",
  Guatemala: "Central America",
  Honduras: "Central America",
  "El Salvador": "Central America",
  Nicaragua: "Central America",
  Belize: "Central America",
  // South America
  Colombia: "South America",
  Brazil: "South America",
  Argentina: "South America",
  Chile: "South America",
  Peru: "South America",
  Ecuador: "South America",
  Venezuela: "South America",
  "Venezuela (Bolivarian Republic of)": "South America",
  Uruguay: "South America",
  Paraguay: "South America",
  Bolivia: "South America",
  "French Guiana": "South America",
  Guyana: "South America",
  Suriname: "South America",
  // North America (sub-continent within CALA census)
  Mexico: "North America",
};

export const GEOGRAPHY_SOURCE = {
  regionCountryDerived: "dealality_country_region_map",
  subContinentCountryDerived: "dealality_country_sub_continent_map",
  marketStrImport: "str_excel_str_market",
  submarketStrImport: "str_excel_str_submarket",
  dealalityMarketSteward: "dealality_steward",
  dealalitySubmarketCityMap: "dealality_city_corridor_map",
  dealalitySubmarketKeyword: "dealality_keyword_corridor_map",
  dealalitySubmarketStrNormalized: "dealality_str_submarket_normalized",
};

function normalizeCountryKey(s) {
  if (s == null || typeof s !== "string") return "";
  return s.toLowerCase().trim().replace(/\s+/g, " ").replace(/&/g, "and");
}

/**
 * @param {string} [country]
 * @returns {string}
 */
export function countryToSubContinent(country) {
  const key = normalizeCountryKey(country);
  if (!key) return "";
  for (const [name, sub] of Object.entries(COUNTRY_TO_SUB_CONTINENT)) {
    if (normalizeCountryKey(name) === key) return sub;
  }
  return "";
}

/**
 * Resolve Region for census write (fill-blank automation).
 * @param {string} [existingRegion]
 * @param {string} [country]
 */
export function resolveCensusRegion(existingRegion, country) {
  return resolveDealalityRegion(existingRegion, country);
}

/**
 * Resolve Sub-Continent for census write (fill-blank automation).
 * @param {string} [existingSubContinent]
 * @param {string} [country]
 */
export function resolveCensusSubContinent(existingSubContinent, country) {
  const existing = String(existingSubContinent ?? "").trim();
  if (existing) {
    const match = SUB_CONTINENT_LABELS.find(
      (label) => normalizeCountryKey(label) === normalizeCountryKey(existing)
    );
    if (match) return match;
  }
  return countryToSubContinent(country);
}

export function isBlankGeoValue(value) {
  if (value == null) return true;
  if (typeof value === "string") return value.trim() === "";
  return false;
}

/**
 * Propose fill-blank geography fields for a census row.
 * Market / Submarket are never derived here — STR import only.
 *
 * @param {object} row — census fields object
 * @returns {{ fields: Record<string, string>, sources: Record<string, string>, skipped: string[] }}
 */
export function proposeGeographyEnrichment(row) {
  const country = row[CENSUS_FIELDS.country] ?? row.country;
  /** @type {Record<string, string>} */
  const fields = {};
  /** @type {Record<string, string>} */
  const sources = {};
  const skipped = [];

  if (isBlankGeoValue(row[CENSUS_FIELDS.region])) {
    const region = resolveCensusRegion(row[CENSUS_FIELDS.region], country);
    if (region && region !== "Other") {
      fields[CENSUS_FIELDS.region] = region;
      sources[CENSUS_FIELDS.region] = GEOGRAPHY_SOURCE.regionCountryDerived;
    } else {
      skipped.push(CENSUS_FIELDS.region);
    }
  }

  if (isBlankGeoValue(row[CENSUS_SUB_CONTINENT_FIELD])) {
    const sub = resolveCensusSubContinent(row[CENSUS_SUB_CONTINENT_FIELD], country);
    if (sub) {
      fields[CENSUS_SUB_CONTINENT_FIELD] = sub;
      sources[CENSUS_SUB_CONTINENT_FIELD] = GEOGRAPHY_SOURCE.subContinentCountryDerived;
    } else {
      skipped.push(CENSUS_SUB_CONTINENT_FIELD);
    }
  }

  if (isBlankGeoValue(row[CENSUS_FIELDS.market])) {
    skipped.push(`${CENSUS_FIELDS.market} (requires STR import or steward)`);
  }
  if (isBlankGeoValue(row.Submarket)) {
    skipped.push("Submarket (requires STR import or steward)");
  }

  return { fields, sources, skipped };
}

export function validateGeographyProposal(fields) {
  const errors = [];
  if (fields[CENSUS_FIELDS.region]) {
    const r = fields[CENSUS_FIELDS.region];
    if (!DEALALITY_REGION_LABELS.includes(r)) {
      errors.push(`Region "${r}" is not a Dealality UI label`);
    }
  }
  if (fields[CENSUS_SUB_CONTINENT_FIELD]) {
    const s = fields[CENSUS_SUB_CONTINENT_FIELD];
    if (!SUB_CONTINENT_LABELS.includes(s)) {
      errors.push(`Sub-Continent "${s}" is not in allowed list`);
    }
  }
  return { pass: errors.length === 0, errors };
}
