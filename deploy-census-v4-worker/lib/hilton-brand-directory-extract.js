/**
 * Hilton brand directory extraction from locations pages (__NEXT_DATA__ JSON).
 * Read-only public source — no hotel detail page / GraphQL required.
 */

const HILTON_ORIGIN = "https://www.hilton.com";
const HILTON_EN_PREFIX = `${HILTON_ORIGIN}/en/`;

export const HILTON_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml",
};

/** @typedef {object} HiltonBrandDirectoryConfig
 * @property {string} canonicalBrandName
 * @property {string} brandCode Hilton brand code (e.g. QQ = Curio)
 * @property {string} locationsSlug URL segment (e.g. curio-collection)
 * @property {string} [parentCompany]
 */

/** @type {Record<string, HiltonBrandDirectoryConfig>} */
export const HILTON_BRAND_DIRECTORY_REGISTRY = {
  "Curio Collection by Hilton": {
    canonicalBrandName: "Curio Collection by Hilton",
    brandCode: "QQ",
    locationsSlug: "curio-collection",
    parentCompany: "Hilton",
  },
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * @param {string} pathOrUrl locations/... or full URL
 */
export function hiltonLocationsUrl(pathOrUrl) {
  const raw = String(pathOrUrl || "").trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  const path = raw.replace(/^\//, "");
  return `${HILTON_EN_PREFIX}${path}`;
}

export function brandIndexUrl(locationsSlug) {
  return hiltonLocationsUrl(`locations/${locationsSlug}/`);
}

/**
 * @param {string} html
 */
export function parseNextDataFromHtml(html) {
  const m = String(html || "").match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  if (!m) throw new Error("__NEXT_DATA__ script not found");
  return JSON.parse(m[1]);
}

/**
 * @param {string} url
 */
export async function fetchHiltonLocationsPage(url) {
  const res = await fetch(url, { headers: HILTON_FETCH_HEADERS, redirect: "follow" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  const html = await res.text();
  const data = parseNextDataFromHtml(html);
  const pageData = data?.props?.pageProps?.pageData;
  if (!pageData || typeof pageData !== "object") {
    throw new Error(`pageData missing for ${url}`);
  }
  return { url, pageData };
}

/**
 * @param {object} pageData
 * @returns {{ name: string, uri: string, url: string }[]}
 */
export function extractCountryPageLinks(pageData) {
  const interlinks = pageData?.location?.pageInterlinks;
  if (!Array.isArray(interlinks)) return [];

  const out = [];
  const seen = new Set();
  for (const block of interlinks) {
    const links = block?.links;
    if (!Array.isArray(links)) continue;
    for (const link of links) {
      const uri = String(link?.uri || "").trim();
      if (!uri || seen.has(uri)) continue;
      seen.add(uri);
      out.push({
        name: String(link?.name || "").trim(),
        uri,
        url: hiltonLocationsUrl(uri),
      });
    }
  }
  return out;
}

/**
 * @param {object} pageData
 * @returns {object[]}
 */
export function extractHotelsFromPageData(pageData) {
  const hotels = pageData?.hotelSummaryOptions?.hotels;
  return Array.isArray(hotels) ? hotels : [];
}

function pickImageUrl(images) {
  const master = images?.master?.ratios?.[0]?.url;
  if (master) return master;
  const carousel = images?.carousel?.[0]?.ratios?.[0]?.url;
  return carousel || "";
}

/**
 * @param {object} hotel raw Hilton hotelSummaryOptions row
 * @param {{ sourceUrl?: string, countryPage?: string }} [meta]
 */
export function normalizeHiltonDirectoryHotel(hotel, meta = {}) {
  const brandCode = String(hotel?.brandCode || "").trim();
  const ctyhocn = String(hotel?.ctyhocn || "").trim().toUpperCase();
  const name = String(hotel?.name || "").trim();
  const address = hotel?.address || {};
  const coord = hotel?.localization?.coordinate || {};
  const display = hotel?.display || {};
  const open = display?.open === true;
  const openDate = String(display?.openDate || "").trim() || null;
  const website = String(hotel?.facilityOverview?.homeUrlTemplate || "").trim();
  const phone = String(hotel?.contactInfo?.phoneNumber || "").trim();

  return {
    source: "hilton_locations_directory",
    sourceUrl: meta.sourceUrl || "",
    countryPage: meta.countryPage || "",
    brandCode,
    brandPropertyCode: ctyhocn,
    ctyhocn,
    name,
    status: open ? "Open" : "Pipeline",
    openDate,
    phone,
    website,
    addressLine1: String(address.addressLine1 || "").trim(),
    addressFormatted: String(address.addressFmt || "").trim(),
    city: String(address.city || "").trim(),
    state: String(address.stateName || address.state || "").trim(),
    postalCode: String(address.postalCode || "").trim(),
    countryCode: String(address.country || "").trim(),
    country: String(address.countryName || "").trim(),
    latitude: Number(coord.latitude),
    longitude: Number(coord.longitude),
    currencyCode: String(hotel?.localization?.currencyCode || "").trim(),
    amenityIds: Array.isArray(hotel?.amenityIds) ? [...hotel.amenityIds] : [],
    heroImageUrl: pickImageUrl(hotel?.images),
    leadRateAmount: hotel?.leadRate?.lowest?.rateAmount ?? null,
    leadRateFormatted: String(hotel?.leadRate?.lowest?.rateAmountFmt || "").trim(),
    rawPayloadJson: JSON.stringify(hotel),
  };
}

/**
 * @param {object} opts
 * @param {HiltonBrandDirectoryConfig} opts.brandConfig
 * @param {number} [opts.delayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function crawlHiltonBrandDirectory(opts) {
  const { brandConfig, delayMs = 250, onProgress } = opts;
  const { brandCode, locationsSlug } = brandConfig;
  if (!brandCode || !locationsSlug) {
    throw new Error("brandConfig requires brandCode and locationsSlug");
  }

  const indexUrl = brandIndexUrl(locationsSlug);
  if (onProgress) onProgress(`Fetching index ${indexUrl}`);
  const indexPage = await fetchHiltonLocationsPage(indexUrl);

  const countryLinks = extractCountryPageLinks(indexPage.pageData);
  const urlsToFetch = [
    { url: indexUrl, countryPage: "index" },
    ...countryLinks.map((c) => ({ url: c.url, countryPage: c.name || c.uri })),
  ];

  /** @type {Map<string, ReturnType<typeof normalizeHiltonDirectoryHotel>>} */
  const byCode = new Map();
  const fetchErrors = [];

  for (const item of urlsToFetch) {
    if (delayMs > 0) await sleep(delayMs);
    try {
      if (onProgress) onProgress(`Fetching ${item.url}`);
      const page =
        item.url === indexUrl
          ? indexPage
          : await fetchHiltonLocationsPage(item.url);
      const hotels = extractHotelsFromPageData(page.pageData);
      for (const hotel of hotels) {
        if (String(hotel?.brandCode || "").trim() !== brandCode) continue;
        const normalized = normalizeHiltonDirectoryHotel(hotel, {
          sourceUrl: item.url,
          countryPage: item.countryPage,
        });
        if (!normalized.ctyhocn) continue;
        byCode.set(normalized.ctyhocn, normalized);
      }
    } catch (err) {
      fetchErrors.push({ url: item.url, error: err?.message || String(err) });
      if (onProgress) onProgress(`  ERROR ${item.url}: ${err?.message || err}`);
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    brandConfig,
    indexUrl,
    countryPagesAttempted: urlsToFetch.length,
    countryPageCount: countryLinks.length,
    hotelsFound: byCode.size,
    hotels: [...byCode.values()].sort((a, b) => a.name.localeCompare(b.name)),
    fetchErrors,
  };
}

/**
 * @param {string} brandName canonical registry key
 */
export function getHiltonBrandConfig(brandName) {
  const key = String(brandName || "").trim();
  return HILTON_BRAND_DIRECTORY_REGISTRY[key] || null;
}
