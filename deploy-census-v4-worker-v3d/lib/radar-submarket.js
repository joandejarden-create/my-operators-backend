/**
 * Radar submarket options (Demand Anchors + Travel Infrastructure).
 * Country-scoped lists live in radar-submarket-registry.js (from country-configs).
 */

import {
  COUNTRY_SUBMARKET_REGISTRY,
  buildAllSubmarketOptionsUnion,
  getSubmarketOptionsForCountry,
} from "./radar-submarket-registry.js";

export const SUBMARKET_FIELD_NAME = "Submarket";

export const PUERTO_RICO_SUBMARKET_OPTIONS = COUNTRY_SUBMARKET_REGISTRY["Puerto Rico"] || [];

export const DOMINICAN_REPUBLIC_SUBMARKET_OPTIONS =
  COUNTRY_SUBMARKET_REGISTRY["Dominican Republic"] || [];

/** Union of all country submarkets for Airtable select + import validation. */
export const ALL_SUBMARKET_OPTIONS = buildAllSubmarketOptionsUnion();

const GLOBAL_OPTION_BY_LOWER = new Map(
  ALL_SUBMARKET_OPTIONS.map((opt) => [opt.toLowerCase(), opt])
);

/**
 * @param {string} country
 * @param {string[]} options
 */
function optionMapForCountry(country, options) {
  return new Map(options.map((opt) => [opt.toLowerCase(), opt]));
}

/**
 * Parse "Submarket: …" prefix from Notes without modifying Notes body.
 * @param {string} notes
 * @returns {string}
 */
export function extractSubmarketFromNotes(notes) {
  const text = String(notes || "").trim();
  if (!text) return "";
  const match = text.match(/Submarket:\s*([^.\n\r]+)/i);
  if (!match) return "";
  return match[1].trim();
}

/**
 * Map free text to a country submarket option (or Other).
 * @param {string} raw
 * @param {string} [country]
 * @returns {string}
 */
export function normalizeSubmarketLabel(raw, country = "") {
  const label = String(raw || "").trim();
  if (!label) return "";

  const options = country
    ? getSubmarketOptionsForCountry(country)
    : ALL_SUBMARKET_OPTIONS;
  const byLower = optionMapForCountry(country, options);

  const exact = byLower.get(label.toLowerCase());
  if (exact) return exact;

  const partial = options.find(
    (opt) =>
      opt !== "Other" &&
      (label.toLowerCase().includes(opt.toLowerCase()) ||
        opt.toLowerCase().includes(label.toLowerCase()))
  );
  if (partial) return partial;

  if (!country) {
    const globalExact = GLOBAL_OPTION_BY_LOWER.get(label.toLowerCase());
    if (globalExact) return globalExact;
  }

  return "Other";
}

/**
 * @param {string} value
 * @param {string} [country]
 * @returns {boolean}
 */
export function isValidSubmarketOption(value, country = "") {
  const v = String(value || "").trim();
  if (!v) return true;
  const options = country ? getSubmarketOptionsForCountry(country) : ALL_SUBMARKET_OPTIONS;
  return options.includes(v);
}

/**
 * Resolve submarket for Airtable import (country-aware).
 * @param {{ submarket?: string, country?: string }} item
 * @returns {string|null}
 */
export function resolveSubmarketForImport(item) {
  const raw = String(item?.submarket || "").trim();
  if (!raw) return null;
  const country = String(item?.country || "").trim();
  return normalizeSubmarketLabel(raw, country);
}

/** Fallback when Notes lack Submarket: prefix (Puerto Rico only). */
const PR_CITY_SUBMARKET_INFERENCE = {
  "san juan": "San Juan Metro",
  santurce: "San Juan Metro",
  carolina: "San Juan Metro",
  bayamón: "San Juan Metro",
  bayamon: "San Juan Metro",
  guaynabo: "San Juan Metro",
  "trujillo alto": "San Juan Metro",
  catano: "San Juan Metro",
  cataño: "San Juan Metro",
  dorado: "North Coast Resort Corridor",
  "río grande": "East Coast / Island Access",
  "rio grande": "East Coast / Island Access",
  fajardo: "East Coast / Island Access",
  ceiba: "East Coast / Island Access",
  humacao: "East Coast / Island Access",
  vieques: "Vieques / Culebra",
  culebra: "Vieques / Culebra",
  ponce: "South Coast Regional City",
  salinas: "South Coast Regional City",
  mayagüez: "West Coast / University & Surf",
  mayaguez: "West Coast / University & Surf",
  rincón: "West Coast / University & Surf",
  rincon: "West Coast / University & Surf",
  aguadilla: "Northwest Air & Leisure Corridor",
  isabela: "Northwest Air & Leisure Corridor",
  arecibo: "Northwest Air & Leisure Corridor",
  manatí: "North Coast Resort Corridor",
  manati: "North Coast Resort Corridor",
  guánica: "Southwest Nature & Beach Corridor",
  guanica: "Southwest Nature & Beach Corridor",
  "cabo rojo": "Southwest Nature & Beach Corridor",
  caguas: "Central / Inland",
  luquillo: "East Coast / Island Access",
};

/**
 * Infer submarket from city when Notes prefix is absent.
 * @param {string} city
 * @param {string} [country]
 * @returns {string}
 */
export function inferSubmarketFromCity(city, country = "Puerto Rico") {
  const key = String(city || "").trim().toLowerCase();
  if (!key) return "";

  if (String(country || "").trim().toLowerCase() === "puerto rico") {
    return PR_CITY_SUBMARKET_INFERENCE[key] || "";
  }

  const options = getSubmarketOptionsForCountry(country).filter((o) => o !== "Other");
  for (const opt of options) {
    const optKey = opt.toLowerCase();
    if (optKey === key) return opt;
    if (optKey.includes(key) || key.includes(optKey)) return opt;
  }
  return "";
}

export { getSubmarketOptionsForCountry, COUNTRY_SUBMARKET_REGISTRY };
