/**
 * Country-scoped submarket registry — sourced from CALA radar country configs.
 */

import { COUNTRY_CONFIGS } from "./radar-buildout/country-configs.js";

const OTHER_LABEL = "Other";

/**
 * @param {string[]} values
 * @returns {string[]}
 */
export function uniqueSubmarketList(values) {
  const seen = new Set();
  const out = [];
  for (const v of values) {
    const label = String(v || "").trim();
    if (!label || label === OTHER_LABEL) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  out.sort((a, b) => a.localeCompare(b, "en", { sensitivity: "base" }));
  out.push(OTHER_LABEL);
  return out;
}

/**
 * Collect submarkets from a country config (top-level + all marketSubmarkets).
 * @param {object} config
 */
export function collectSubmarketsFromConfig(config) {
  const raw = [];
  for (const s of config?.submarkets || []) raw.push(s);
  for (const list of Object.values(config?.marketSubmarkets || {})) {
    for (const s of list || []) raw.push(s);
  }
  return uniqueSubmarketList(raw);
}

/** @type {Record<string, string[]>} */
export const COUNTRY_SUBMARKET_REGISTRY = Object.fromEntries(
  Object.entries(COUNTRY_CONFIGS).map(([country, config]) => [
    country,
    collectSubmarketsFromConfig(config),
  ])
);

const COUNTRY_KEY_BY_LOWER = new Map(
  Object.keys(COUNTRY_SUBMARKET_REGISTRY).map((c) => [c.toLowerCase(), c])
);

const CENSUS_COUNTRY_ALIASES = {
  "turks and caicos islands": "Turks & Caicos",
  "turks and caicos": "Turks & Caicos",
  "turks & caicos islands": "Turks & Caicos",
  "bonaire, sint eustatius and saba": "Aruba",
  "bonaire sint eustatius and saba": "Aruba",
  "sint maarten (dutch part)": "Aruba",
  "us virgin islands": "Puerto Rico",
  "british virgin islands": "Puerto Rico",
};

/**
 * @param {string} country
 * @returns {string[]}
 */
export function getSubmarketOptionsForCountry(country) {
  const raw = String(country || "").trim().toLowerCase();
  const aliasKey = CENSUS_COUNTRY_ALIASES[raw];
  const key =
    COUNTRY_KEY_BY_LOWER.get(raw) ||
    (aliasKey ? COUNTRY_KEY_BY_LOWER.get(aliasKey.toLowerCase()) : undefined);
  if (!key) return [OTHER_LABEL];
  return COUNTRY_SUBMARKET_REGISTRY[key] || [OTHER_LABEL];
}

/** Countries with completed radar market builds (submarket backfill targets). */
export const BUILT_RADAR_COUNTRIES = [
  "Puerto Rico",
  "Dominican Republic",
  "Colombia",
  "Mexico",
  "Panama",
  "Costa Rica",
  "Peru",
  "Chile",
  "Jamaica",
  "Bahamas",
  "Aruba",
  "Curaçao",
  "Barbados",
  "Cayman Islands",
  "Turks & Caicos",
  "Saint Lucia",
  "Antigua and Barbuda",
  "Grenada",
  "Saint Vincent and the Grenadines",
  "Dominica",
  "Saint Kitts and Nevis",
  "Trinidad and Tobago",
  "British Virgin Islands",
  "Cuba",
  "Haiti",
  "U.S. Virgin Islands",
  "Martinique",
  "Guadeloupe",
  "Bonaire",
  "Belize",
  "Guatemala",
  "Honduras",
  "Nicaragua",
  "El Salvador",
  "Argentina",
  "Ecuador",
  "Uruguay",
];

/**
 * Union of all country submarket options (for Airtable single-select schema).
 * @returns {string[]}
 */
export function buildAllSubmarketOptionsUnion() {
  const seen = new Set();
  const out = [];
  for (const list of Object.values(COUNTRY_SUBMARKET_REGISTRY)) {
    for (const opt of list) {
      if (opt === OTHER_LABEL) continue;
      const key = opt.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(opt);
    }
  }
  out.sort((a, b) => a.localeCompare(b, "en", { sensitivity: "base" }));
  out.push(OTHER_LABEL);
  return out;
}
