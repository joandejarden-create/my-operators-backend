/**
 * Hilton brand registry — auto-discovered from hilton.com __NEXT_DATA__ brands query.
 */

import {
  HILTON_FETCH_HEADERS,
  brandIndexUrl,
  parseNextDataFromHtml,
} from "./hilton-brand-directory-extract.js";

/** Census Affiliation strings per Hilton brandCode (fallback when alias table empty). */
export const HILTON_BRAND_CENSUS_AFFILIATION_HINTS = {
  AQ: ["Apartment Collection by Hilton"],
  CH: ["Conrad", "Conrad Hotels & Resorts"],
  DT: ["DoubleTree by Hilton", "DoubleTree"],
  ES: ["Embassy Suites by Hilton", "Embassy Suites"],
  EY: ["LivSmart Studios by Hilton"],
  GI: ["Hilton Garden Inn"],
  GU: ["Graduate by Hilton"],
  GV: ["Hilton Grand Vacations"],
  HI: ["Hilton", "Hilton Hotels & Resorts"],
  HP: ["Hampton by Hilton", "Hampton Inn by Hilton"],
  HT: ["Home2 Suites by Hilton", "Home2 Suites"],
  HW: ["Homewood Suites by Hilton", "Homewood Suites"],
  ID: ["Outset Collection by Hilton"],
  OL: ["LXR Hotels & Resorts", "LXR"],
  PE: ["Spark by Hilton"],
  PO: ["Tempo by Hilton"],
  PY: ["Canopy by Hilton", "Canopy"],
  QQ: ["Curio Collection by Hilton"],
  RU: ["Tru by Hilton"],
  SA: ["Signia by Hilton"],
  UA: ["Motto by Hilton"],
  UP: ["Tapestry Collection by Hilton", "Tapestry Collection"],
  WA: ["Waldorf Astoria", "Waldorf Astoria Hotels & Resorts"],
};

let cachedBrandList = null;

export async function fetchHiltonBrandListFromSite(seedUrl) {
  const url = seedUrl || brandIndexUrl("curio-collection");
  const res = await fetch(url, { headers: HILTON_FETCH_HEADERS, redirect: "follow" });
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching Hilton brands`);
  const html = await res.text();
  const data = parseNextDataFromHtml(html);
  const queries = data?.props?.pageProps?.dehydratedState?.queries || [];
  const brandsQuery = queries.find((q) => JSON.stringify(q.queryKey).includes('"brands"'));
  const brands = brandsQuery?.state?.data?.brands;
  if (!Array.isArray(brands)) throw new Error("Hilton brands query not found in page JSON");
  return brands;
}

export function hiltonBrandRowToConfig(raw) {
  if (!raw?.hasHotels || !raw?.code) return null;
  const slug = String(raw.canonicalSlug || raw.altSlugs?.[0] || "").trim();
  if (!slug) return null;
  return {
    canonicalBrandName: String(raw.formalName || slug).trim(),
    brandCode: String(raw.code).trim().toUpperCase(),
    locationsSlug: slug,
    parentCompany: "Hilton",
  };
}

export async function loadHiltonBrandDirectoryConfigs(opts = {}) {
  if (!cachedBrandList) {
    cachedBrandList = await fetchHiltonBrandListFromSite(opts.seedUrl);
  }
  let configs = cachedBrandList.map(hiltonBrandRowToConfig).filter(Boolean);
  if (opts.brandCodes?.length) {
    const want = new Set(opts.brandCodes.map((c) => c.toUpperCase()));
    configs = configs.filter((c) => want.has(c.brandCode));
  }
  return configs.sort((a, b) => a.canonicalBrandName.localeCompare(b.canonicalBrandName));
}

export function affiliationHintsForBrand(brandConfig) {
  const fromCode = HILTON_BRAND_CENSUS_AFFILIATION_HINTS[brandConfig.brandCode] || [];
  const canonical = brandConfig.canonicalBrandName;
  return [...new Set([canonical, ...fromCode].filter(Boolean))];
}

export function findBrandConfig(brandName, configs) {
  const key = String(brandName || "").trim();
  const byDirect =
    configs.find((c) => c.canonicalBrandName === key) ||
    configs.find((c) => c.locationsSlug === key) ||
    configs.find((c) => c.brandCode === key.toUpperCase()) ||
    null;
  if (byDirect) return byDirect;

  const keyLower = key.toLowerCase();
  for (const [code, hints] of Object.entries(HILTON_BRAND_CENSUS_AFFILIATION_HINTS)) {
    if (hints.some((h) => String(h).toLowerCase() === keyLower)) {
      return configs.find((c) => c.brandCode === code) || null;
    }
  }
  return null;
}

export function resetHiltonBrandListCache() {
  cachedBrandList = null;
}
