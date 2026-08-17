/**
 * Normalize Accor directory page titles for census matching.
 * Accor SEO titles often look like "Hotel in {city} | {brand}" while census uses property names.
 */

import { normalizeKey, normalizeText } from "../independent-census/match-current-census.js";

const SEO_LEAD_PATTERN =
  /^(hotel|resort|budget hotel|well[- ]located|comfortable|sophisticated|discover|enjoy|experience|eco|family friendly)/i;

const TRAILING_JUNK = /\s*-\s*ALL\s*$/i;

/**
 * @param {string} raw
 */
export function normalizeAccorDirectoryName(raw) {
  let s = normalizeText(raw).replace(TRAILING_JUNK, "").trim();
  if (!s) return "";

  const parts = s.split("|").map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 2) {
    if (SEO_LEAD_PATTERN.test(parts[0])) {
      s = parts.slice(1).join(" ");
    } else {
      s = parts[0];
    }
  }

  return s.replace(/\s+/g, " ").trim();
}

/** @type {string[]} */
const ACCOR_BRAND_TOKENS = [
  "pullman",
  "mercure",
  "grand mercure",
  "novotel",
  "ibis styles",
  "ibis budget",
  "ibis",
  "sofitel",
  "mgallery",
  "fairmont",
  "swissotel",
  "mama shelter",
  "adagio",
  "movenpick",
  "novotel suites",
  "greet",
  "tribe",
  "handwritten",
  "emblems",
];

/**
 * Shared brand keyword between census and directory names (Accor sub-brands).
 * @param {string} censusName
 * @param {string} directoryName
 */
export function accorBrandTokenOverlap(censusName, directoryName) {
  const censusKey = normalizeKey(censusName);
  const dirKey = normalizeKey(directoryName);
  if (!censusKey || !dirKey) return false;

  for (const brand of ACCOR_BRAND_TOKENS) {
    if (censusKey.includes(brand) && dirKey.includes(brand)) return true;
  }
  return false;
}

/**
 * @param {string} url
 */
export function accorPropertyIdFromWebsite(url) {
  const m = String(url || "").match(/\/hotel\/([0-9A-Za-z]+)\//i);
  return m ? m[1].toUpperCase() : "";
}

/**
 * Canonical English property page URL for census Website field.
 * @param {string} propertyId
 */
export function accorCanonicalPropertyUrl(propertyId) {
  const code = String(propertyId || "").trim().toUpperCase();
  if (!code) return "";
  return `https://all.accor.com/hotel/${code}/index.en.shtml`;
}
