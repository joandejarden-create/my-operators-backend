/**
 * Marriott-specific hotel name normalization for census ↔ sitemap matching.
 */

import { nameSimilarity, normalizeKey } from "./independent-census/match-current-census.js";

const STRIP_PHRASES = [
  "by marriott",
  "by elegant hotels",
  "by royalton",
  "an autograph collection",
  "autograph collection",
  "a tribute portfolio",
  "tribute portfolio",
  "a luxury collection",
  "luxury collection",
  "all inclusive",
  "all-inclusive",
  "adults only",
  "adult only",
  "resort and spa",
  "resort & spa",
  "hotel and spa",
  "hotel & spa",
  "convention center",
  "and convention center",
  "a ritz carlton reserve",
  "a ritz-carlton reserve",
  "ritz carlton reserve",
  "ritz-carlton reserve",
  "a member of design hotels",
  "member of design hotels",
  "marriott hotel",
  "marriott hotels",
  "hotels by marriott",
  "renaissance",
];

const STRIP_TOKENS = new Set([
  "by",
  "marriott",
  "the",
  "a",
  "an",
  "and",
  "at",
  "in",
  "of",
  "hotel",
  "hotels",
  "resort",
  "resorts",
  "spa",
  "suites",
  "suite",
  "collection",
  "inclusive",
  "all",
  "only",
  "adults",
  "adult",
  "beach",
  "convention",
  "center",
  "residences",
  "residence",
  "art",
  "hideaway",
  "planet",
  "hollywood",
  "scene",
  "royalton",
  "elegant",
  "project",
  "member",
  "design",
  "apartments",
  "bonvoy",
  "plus",
  "express",
  "ciudad",
  "de",
  "city",
  "highway",
  "401",
  "division",
  "street",
  "expo",
  "water",
  "edge",
  "caribbean",
]);

/**
 * @param {string} raw
 */
export function normalizeMarriottHotelNameForMatch(raw) {
  let s = normalizeKey(raw).replace(/&/g, " and ");
  for (const phrase of STRIP_PHRASES) {
    s = s.replace(new RegExp(`\\b${phrase.replace(/\s+/g, "\\s+")}\\b`, "gi"), " ");
  }
  s = s.replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
  return s;
}

/**
 * @param {string} raw
 */
export function marriottCoreTokens(raw) {
  const norm = normalizeMarriottHotelNameForMatch(raw);
  return norm
    .split(/\s+/)
    .filter((t) => t.length > 1 && !STRIP_TOKENS.has(t));
}

/**
 * @param {string} a
 * @param {string} b
 */
export function marriottDirectoryNameSimilarity(a, b) {
  const base = nameSimilarity(a, b);
  const normA = normalizeMarriottHotelNameForMatch(a);
  const normB = normalizeMarriottHotelNameForMatch(b);
  const normSim = nameSimilarity(normA, normB);

  let best = Math.max(base, normSim);
  if (normA && normB) {
    if (normA.includes(normB) || normB.includes(normA)) {
      const ratio = Math.min(normA.length, normB.length) / Math.max(normA.length, normB.length);
      best = Math.max(best, Math.max(0.72, ratio));
    }
  }

  const tokensA = marriottCoreTokens(a);
  const tokensB = marriottCoreTokens(b);
  if (tokensA.length && tokensB.length) {
    let inter = 0;
    const setB = new Set(tokensB);
    for (const t of tokensA) {
      if (setB.has(t)) inter++;
    }
    const coreSim = inter / Math.max(tokensA.length, tokensB.length);
    best = Math.max(best, coreSim);
  }

  return Math.min(1, best);
}

/**
 * @param {string} censusName
 * @param {string} directoryName
 * @param {string} [censusCity]
 */
export function marriottOpenMatchScore(censusName, directoryName, censusCity = "", censusCountry = "") {
  let score = marriottDirectoryNameSimilarity(censusName, directoryName);
  const city = normalizeKey(censusCity);
  const dir = normalizeKey(directoryName);
  if (city && dir.includes(city)) score = Math.min(1, score + 0.12);
  const country = normalizeKey(censusCountry);
  if (country && dir.includes(country)) score = Math.min(1, score + 0.15);
  return score;
}

/**
 * @param {string} name
 */
export function marriottBrandTokens(name) {
  const n = normalizeKey(name);
  /** @type {string[]} */
  const brands = [];
  if (/\bst regis\b|\bregis\b/.test(n) && !/\bla concha\b/.test(n)) brands.push("regis");
  if (/\britz\b/.test(n)) brands.push("ritz");
  if (/\bwestin\b/.test(n)) brands.push("westin");
  if (/\bsheraton\b/.test(n)) brands.push("sheraton");
  if (/\bw hotel\b/.test(n) || /^w\s/.test(n)) brands.push("w");
  if (/\baloft\b/.test(n)) brands.push("aloft");
  if (/\bac hotel\b|\bac hotels\b/.test(n)) brands.push("ac");
  if (/\bcourtyard\b/.test(n)) brands.push("courtyard");
  if (/\bfairfield\b/.test(n)) brands.push("fairfield");
  if (/\bcity express\b/.test(n)) brands.push("cityexpress");
  if (/\bplanet hollywood\b/.test(n)) brands.push("planethollywood");
  if (/\bmarriott\b/.test(n) && !brands.length) brands.push("marriott");
  return brands;
}

/**
 * @param {string} censusName
 * @param {string} directoryName
 */
export function marriottBrandsCompatible(censusName, directoryName) {
  const censusBrands = marriottBrandTokens(censusName);
  if (!censusBrands.length) return true;
  const dirBrands = marriottBrandTokens(directoryName);
  return censusBrands.every((b) => dirBrands.includes(b));
}

/**
 * @param {string} censusName
 * @param {string} directoryName
 * @param {string} censusCountry
 */
export function marriottLocationTokenOverlap(censusName, directoryName, censusCountry = "") {
  const brandOnly = new Set([
    "ritz",
    "carlton",
    "marriott",
    "westin",
    "sheraton",
    "regis",
    "hotel",
    "resort",
    "beach",
    "collection",
    "autograph",
    "tribute",
    "luxury",
    "hideaway",
    "royalton",
    "elegant",
    "hollywood",
    "planet",
    "express",
    "city",
    "suites",
    "suite",
    "residences",
    "residence",
    "reserve",
    "adults",
    "adult",
    "inclusive",
    "all",
    "only",
    "spa",
    "convention",
    "center",
    "art",
  ]);
  const censusLoc = [...marriottCoreTokens(censusName), ...marriottCoreTokens(censusCountry)].filter(
    (t) => !brandOnly.has(t)
  );
  const dirLoc = marriottCoreTokens(directoryName).filter((t) => !brandOnly.has(t));
  if (!censusLoc.length) return true;
  for (const t of censusLoc) {
    if (dirLoc.includes(t)) return true;
  }
  return false;
}
