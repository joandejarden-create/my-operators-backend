/**
 * Index Choice property URLs from the CALA URL extract CSV (sitemap / census).
 * Used for diversified footprint.openings and momentum per brand.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { urlMatchesBrandSlug } from "./choice-cala-brand-url-slugs.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const EXTRACT_CSV = path.join(
  ROOT,
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv"
);

/** Country path segments treated as CALA for footprint selection (matches census CALA corridor). */
const CALA_COUNTRY_SLUGS = new Set([
  "argentina",
  "aruba",
  "bahamas",
  "barbados",
  "bolivia",
  "brazil",
  "chile",
  "colombia",
  "costa-rica",
  "dominican-republic",
  "ecuador",
  "el-salvador",
  "grenada",
  "guatemala",
  "guyana",
  "haiti",
  "honduras",
  "mexico",
  "panama",
  "paraguay",
  "peru",
  "puerto-rico",
  "suriname",
  "trinidad-and-tobago",
  "uruguay",
  "jamaica",
  "belize",
  "nicaragua",
  "cuba",
  "curacao",
  "saint-martin",
  "sint-maarten",
]);

/** @typedef {{ url: string; countrySlug: string; citySlug: string; calaStatus: string }} UrlEntry */

/** @type {Record<string, UrlEntry[]> | null} */
let cachedByBrand = null;

function hashString(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return Math.abs(h);
}

/**
 * @param {string} line
 * @returns {{ brand: string; url: string; calaStatus: string } | null}
 */
function parseExtractLine(line) {
  const urlMatch = line.match(/(https:\/\/www\.choicehotels\.com\/[^,\s]+)/);
  if (!urlMatch) return null;
  const url = urlMatch[1].trim();
  const beforeUrl = line.slice(0, line.indexOf(url));
  const brandMatch = beforeUrl.match(/^Choice Hotels International,([^,]+),/);
  if (!brandMatch) return null;
  const calaMatch = line.match(/,(included|excluded_non_cala|uncertain),Choice Hotels Sitemap,/);
  return {
    brand: brandMatch[1].trim(),
    url,
    calaStatus: calaMatch ? calaMatch[1] : "uncertain",
  };
}

function countrySlugFromUrl(url) {
  try {
    const parts = new URL(url).pathname.split("/").filter(Boolean);
    return parts[0] || "";
  } catch {
    return "";
  }
}

function citySlugFromUrl(url) {
  try {
    const parts = new URL(url).pathname.split("/").filter(Boolean);
    return parts[1] || "";
  } catch {
    return "";
  }
}

/** @returns {Record<string, UrlEntry[]>} */
export function loadUrlIndexByBrand() {
  if (cachedByBrand) return cachedByBrand;
  /** @type {Record<string, UrlEntry[]>} */
  const byBrand = {};
  if (!fs.existsSync(EXTRACT_CSV)) {
    cachedByBrand = byBrand;
    return byBrand;
  }
  const lines = fs.readFileSync(EXTRACT_CSV, "utf8").trim().split(/\r?\n/).slice(1);
  const seenPerBrand = /** @type {Record<string, Set<string>>} */ ({});

  for (const line of lines) {
    const row = parseExtractLine(line);
    if (!row) continue;
    if (row.calaStatus === "excluded_non_cala") continue;
    const countrySlug = countrySlugFromUrl(row.url);
    if (row.calaStatus === "uncertain" && countrySlug && !CALA_COUNTRY_SLUGS.has(countrySlug)) {
      continue;
    }
    if (!seenPerBrand[row.brand]) seenPerBrand[row.brand] = new Set();
    if (seenPerBrand[row.brand].has(row.url)) continue;
    seenPerBrand[row.brand].add(row.url);
    if (!byBrand[row.brand]) byBrand[row.brand] = [];
    byBrand[row.brand].push({
      url: row.url,
      countrySlug: countrySlugFromUrl(row.url),
      citySlug: citySlugFromUrl(row.url),
      calaStatus: row.calaStatus,
    });
  }

  for (const brand of Object.keys(byBrand)) {
    byBrand[brand].sort((a, b) => a.url.localeCompare(b.url));
  }

  cachedByBrand = byBrand;
  return byBrand;
}

/**
 * Pick diverse property URLs for a brand (rotated slice + one per country when possible).
 *
 * @param {string} brandKey — Matched Brand Setup Brand name from census
 * @param {number} count
 * @returns {string[]}
 */
/**
 * @param {string} brandKey
 * @param {number} count
 * @param {{ salt?: string }} [options] — varies rotation (e.g. comp cards per requesting brand)
 */
export function selectDiversifiedUrlsForBrand(brandKey, count, options = {}) {
  const index = loadUrlIndexByBrand();
  const entries = (index[brandKey] || []).filter((e) => urlMatchesBrandSlug(brandKey, e.url));
  if (!entries.length || count <= 0) return [];

  const included = entries.filter((e) => e.calaStatus === "included");
  const uncertain = entries.filter((e) => e.calaStatus === "uncertain");
  const ordered = included.length ? [...included, ...uncertain] : entries;

  const salt = options.salt ? String(options.salt) : "";
  const offset = hashString(`${brandKey}|${salt}`) % ordered.length;
  const rotated = [...ordered.slice(offset), ...ordered.slice(0, offset)];

  /** @type {string[]} */
  const picked = [];
  const usedCountries = new Set();
  const usedPlaces = new Set();
  const usedUrls = new Set();

  for (const e of rotated) {
    if (picked.length >= count) break;
    if (usedUrls.has(e.url)) continue;
    const placeKey = `${e.countrySlug}|${e.citySlug}`;
    if (e.countrySlug && usedCountries.has(e.countrySlug)) continue;
    if (e.citySlug && usedPlaces.has(placeKey)) continue;
    picked.push(e.url);
    usedUrls.add(e.url);
    if (e.countrySlug) usedCountries.add(e.countrySlug);
    if (e.citySlug) usedPlaces.add(placeKey);
  }

  for (const e of rotated) {
    if (picked.length >= count) break;
    if (usedUrls.has(e.url)) continue;
    const placeKey = `${e.countrySlug}|${e.citySlug}`;
    if (e.citySlug && usedPlaces.has(placeKey)) continue;
    picked.push(e.url);
    usedUrls.add(e.url);
    if (e.citySlug) usedPlaces.add(placeKey);
  }

  return picked;
}

/** @param {string} profileKey */
export function censusUrlCountForBrand(profileKey) {
  return (loadUrlIndexByBrand()[profileKey] || []).length;
}
