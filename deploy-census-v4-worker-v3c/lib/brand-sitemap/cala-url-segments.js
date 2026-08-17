/**
 * CALA country/region path segments for brand sitemap URL filtering.
 * Aligns with Dealality census geography (not STR Market).
 */

import { censusCountryToSitemapSlug } from "../marriott-brand-directory-extract.js";

/** @type {Set<string>} */
export const CALA_URL_PATH_SEGMENTS = new Set([
  ...Object.keys({
    "dominican-republic": true,
    mexico: true,
    jamaica: true,
    panama: true,
    "costa-rica": true,
    colombia: true,
    peru: true,
    brazil: true,
    chile: true,
    ecuador: true,
    venezuela: true,
    guatemala: true,
    honduras: true,
    "el-salvador": true,
    bahamas: true,
    barbados: true,
    "trinidad-and-tobago": true,
    "puerto-rico": true,
    argentina: true,
    aruba: true,
    "turks-and-caicos-islands": true,
    "virgin-islands-us": true,
    guyana: true,
    curacao: true,
    "saint-lucia": true,
    belize: true,
    "antigua-and-barbuda": true,
    "cayman-islands": true,
    suriname: true,
    paraguay: true,
    uruguay: true,
    bolivia: true,
    haiti: true,
    grenada: true,
    bermuda: true,
    "sint-maarten": true,
    "saint-kitts-and-nevis": true,
  }),
]);

/** ISO-ish country codes in Accor JSON-LD addressCountry */
export const ACCOR_COUNTRY_CODE_TO_LABEL = {
  MX: "Mexico",
  BR: "Brazil",
  CO: "Colombia",
  AR: "Argentina",
  CL: "Chile",
  PE: "Peru",
  EC: "Ecuador",
  PA: "Panama",
  CR: "Costa Rica",
  DO: "Dominican Republic",
  JM: "Jamaica",
  PR: "Puerto Rico",
  GT: "Guatemala",
  HN: "Honduras",
  SV: "El Salvador",
  UY: "Uruguay",
  PY: "Paraguay",
  BO: "Bolivia",
  VE: "Venezuela",
  BS: "Bahamas",
  BB: "Barbados",
  AW: "Aruba",
  CW: "Curacao",
  TT: "Trinidad and Tobago",
  BM: "Bermuda",
  KY: "Cayman Islands",
  HT: "Haiti",
  GD: "Grenada",
  LC: "Saint Lucia",
  AG: "Antigua and Barbuda",
  KN: "Saint Kitts and Nevis",
  SX: "Sint Maarten",
  TC: "Turks and Caicos Islands",
  VI: "Virgin Islands (U.S.)",
  GY: "Guyana",
  SR: "Suriname",
  BZ: "Belize",
};

/**
 * @param {string} propertyUrl
 */
export function wyndhamUrlLooksCala(propertyUrl) {
  try {
    const segments = new URL(propertyUrl).pathname.split("/").filter(Boolean);
    if (segments.length < 4) return false;
    const region = segments[1]?.toLowerCase();
    return CALA_URL_PATH_SEGMENTS.has(region);
  } catch {
    return false;
  }
}

/**
 * @param {string} countryCode
 */
export function accorCountryCodeIsCala(countryCode) {
  const cc = String(countryCode || "").trim().toUpperCase();
  return Boolean(ACCOR_COUNTRY_CODE_TO_LABEL[cc]);
}

/**
 * @param {string} censusCountry
 */
export function censusCountryToWyndhamSegment(censusCountry) {
  return censusCountryToSitemapSlug(censusCountry);
}
