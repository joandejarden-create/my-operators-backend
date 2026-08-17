/**
 * CALA hotel footprint helpers for GTM Owner Target base (internal only).
 * CALA scope = countries in Hotel Census geography contract (Caribbean, Central & South America, Mexico).
 */
import { COUNTRY_TO_SUB_CONTINENT } from "../hotel-census/geography-enrichment-contract.js";
import { normalizeOwnerKey } from "./normalize.js";

/** @type {Map<string, string>} normalized country key → canonical label */
const CALA_COUNTRY_LOOKUP = new Map();
for (const country of Object.keys(COUNTRY_TO_SUB_CONTINENT)) {
  CALA_COUNTRY_LOOKUP.set(normalizeCountryKey(country), country);
}

/** CoStar / property export aliases → canonical CALA country label. */
const COUNTRY_ALIASES = new Map([
  ["caicos islands", "Turks and Caicos Islands"],
  ["turks and caicos", "Turks and Caicos Islands"],
  ["turks caicos islands", "Turks and Caicos Islands"],
  ["us virgin islands", "U.S. Virgin Islands"],
  ["u s virgin islands", "U.S. Virgin Islands"],
  ["british virgin islands", "British Virgin Islands"],
  ["sint maarten dutch part", "Sint Maarten (Dutch part)"],
  ["st maarten", "Sint Maarten (Dutch part)"],
  ["st lucia", "Saint Lucia"],
  ["st kitts and nevis", "Saint Kitts and Nevis"],
  ["st vincent and the grenadines", "Saint Vincent and the Grenadines"],
  ["trinidad tobago", "Trinidad and Tobago"],
  ["dr", "Dominican Republic"],
  ["dominican republic", "Dominican Republic"],
  ["mexico", "Mexico"],
  ["brasil", "Brazil"],
]);

/**
 * @param {string} value
 */
export function normalizeCountryKey(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

/**
 * @param {string} country
 * @returns {string | null} canonical CALA country label or null if outside CALA scope
 */
export function resolveCalaCountry(country) {
  const key = normalizeCountryKey(country);
  if (!key) return null;
  if (COUNTRY_ALIASES.has(key)) return COUNTRY_ALIASES.get(key);
  if (CALA_COUNTRY_LOOKUP.has(key)) return CALA_COUNTRY_LOOKUP.get(key);
  for (const [aliasKey, canonical] of CALA_COUNTRY_LOOKUP) {
    if (key === aliasKey || key.includes(aliasKey) || aliasKey.includes(key)) {
      return canonical;
    }
  }
  return null;
}

/**
 * @param {string} country
 */
export function isCalaCountry(country) {
  return resolveCalaCountry(country) != null;
}

/**
 * @param {object[]} properties parsed property rows with `.country`
 */
export function summarizePropertyFootprint(properties) {
  const list = properties || [];
  const countryCounts = new Map();
  let calaCount = 0;
  for (const property of list) {
    const canonical = resolveCalaCountry(property.country);
    if (canonical) {
      calaCount++;
      countryCounts.set(canonical, (countryCounts.get(canonical) || 0) + 1);
    }
  }
  const calaCountries = [...countryCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([country, count]) => ({ country, count }));

  return {
    totalPropertyCount: list.length,
    calaPropertyCount: calaCount,
    nonCalaPropertyCount: list.length - calaCount,
    hasCalaHotels: calaCount > 0,
    calaOnly: calaCount > 0 && calaCount === list.length,
    calaCountries,
    calaCountriesSummary: calaCountries.map((c) => c.country).join("; "),
  };
}

/**
 * Build owner-name index from property groups.
 * @param {{ ownerName: string, properties: object[] }[]} ownerGroups
 */
export function buildOwnerCalaFootprintIndex(ownerGroups) {
  /** @type {Map<string, { ownerName: string, ownerKey: string, footprint: ReturnType<typeof summarizePropertyFootprint> }>} */
  const byOwnerKey = new Map();

  for (const group of ownerGroups || []) {
    const ownerName = String(group.ownerName || "").trim();
    if (!ownerName) continue;
    const ownerKey = normalizeOwnerKey(ownerName);
    const footprint = summarizePropertyFootprint(group.properties);
    byOwnerKey.set(ownerKey, { ownerName, ownerKey, footprint });
  }
  return byOwnerKey;
}

/**
 * @param {string} ownerName
 * @param {Map<string, object>} ownerFootprintIndex
 */
export function lookupOwnerFootprint(ownerName, ownerFootprintIndex) {
  const candidates = splitCompoundOwnerName(ownerName);
  /** @type {{ ownerName: string, footprint: object }[]} */
  const hits = [];
  const seen = new Set();
  for (const candidate of candidates) {
    const key = normalizeOwnerKey(candidate);
    const hit = ownerFootprintIndex.get(key);
    if (!hit || seen.has(hit.ownerKey)) continue;
    seen.add(hit.ownerKey);
    hits.push(hit);
  }
  return hits;
}

function splitCompoundOwnerName(ownerName) {
  const raw = String(ownerName || "").trim();
  if (!raw) return [];
  const parts = raw
    .split(/\s*\|\s*/)
    .map((p) => p.replace(/^owner\s*\d+\s*:\s*/i, "").trim())
    .filter(Boolean);
  return parts.length ? parts : [raw];
}

/**
 * Merge multiple owner footprints (e.g. compound True Owner strings).
 * @param {{ footprint: ReturnType<summarizePropertyFootprint> }[]} hits
 */
export function mergeOwnerFootprints(hits) {
  if (!hits?.length) {
    return summarizePropertyFootprint([]);
  }
  if (hits.length === 1) return hits[0].footprint;

  const countryCounts = new Map();
  let calaCount = 0;
  let totalCount = 0;
  for (const hit of hits) {
    const fp = hit.footprint;
    totalCount += fp.totalPropertyCount;
    calaCount += fp.calaPropertyCount;
    for (const row of fp.calaCountries) {
      countryCounts.set(row.country, (countryCounts.get(row.country) || 0) + row.count);
    }
  }
  const calaCountries = [...countryCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([country, count]) => ({ country, count }));

  return {
    totalPropertyCount: totalCount,
    calaPropertyCount: calaCount,
    nonCalaPropertyCount: totalCount - calaCount,
    hasCalaHotels: calaCount > 0,
    calaOnly: calaCount > 0 && calaCount === totalCount,
    calaCountries,
    calaCountriesSummary: calaCountries.map((c) => c.country).join("; "),
  };
}
