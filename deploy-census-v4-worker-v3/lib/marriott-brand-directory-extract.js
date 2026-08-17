/**
 * Marriott brand directory extraction from public country hotel-sitemap pages.
 * Overview / GraphQL endpoints are Akamai-blocked from many server IPs; sitemaps work.
 */

import { normalizeCountry } from "./independent-census/match-current-census.js";

export const MARRIOTT_ORIGIN = "https://www.marriott.com";
export const MARRIOTT_SITEMAP_INDEX =
  "https://www.marriott.com/content/dam/marriott-seo/en/marriott-tng/sitemap-hotel-sitemaps.xml";

export const MARRIOTT_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

/** Census country label → sitemap path slug (without -hotel-sitemap suffix). */
export const CENSUS_COUNTRY_TO_SITEMAP_SLUG = {
  "dominican republic": "dominican-republic",
  mexico: "mexico",
  jamaica: "jamaica",
  panama: "panama",
  "costa rica": "costa-rica",
  colombia: "colombia",
  peru: "peru",
  brazil: "brazil",
  chile: "chile",
  ecuador: "ecuador",
  venezuela: "venezuela",
  "venezuela (bolivarian republic of)": "venezuela",
  guatemala: "guatemala",
  honduras: "honduras",
  "el salvador": "el-salvador",
  bahamas: "bahamas",
  barbados: "barbados",
  "trinidad and tobago": "trinidad-and-tobago",
  "puerto rico": "puerto-rico",
  argentina: "argentina",
  aruba: "aruba",
  "turks and caicos islands": "turks-and-caicos-islands",
  "us virgin islands": "virgin-islands-us",
  "united states virgin islands": "virgin-islands-us",
  "virgin islands (u.s.)": "virgin-islands-us",
  guyana: "guyana",
  curacao: "curacao",
  "curaçao": "curacao",
  "saint lucia": "saint-lucia",
  belize: "belize",
  "antigua and barbuda": "antigua-and-barbuda",
  "cayman islands": "cayman-islands",
  suriname: "suriname",
  paraguay: "paraguay",
  uruguay: "uruguay",
  bolivia: "bolivia",
  haiti: "haiti",
  grenada: "grenada",
  bermuda: "bermuda",
  "sint maarten (dutch part)": "sint-maarten",
  "saint kitts and nevis": "saint-kitts-and-nevis",
};

/** @param {string} country */
export function censusCountryToSitemapSlug(country) {
  const key = normalizeCountry(country);
  if (!key) return "";
  return CENSUS_COUNTRY_TO_SITEMAP_SLUG[key] || "";
}

/** Human-readable label for sitemap slug (used when crawling). */
export const SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL = Object.fromEntries(
  Object.entries(CENSUS_COUNTRY_TO_SITEMAP_SLUG).map(([label, slug]) => [slug, label])
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function marshaFromMarriottWebsite(url) {
  const s = String(url || "");
  const m = s.match(/\/hotels\/([a-z0-9]+)-/i);
  return m ? m[1].toUpperCase() : "";
}

export function countrySitemapUrl(slug) {
  const s = String(slug || "")
    .trim()
    .replace(/-hotel-sitemap$/i, "")
    .replace(/^\//, "");
  return `${MARRIOTT_ORIGIN}/en-us/hotel-sitemap/${s}-hotel-sitemap`;
}

export function sitemapSlugFromUrl(url) {
  const m = String(url || "").match(/hotel-sitemap\/([a-z0-9-]+)-hotel-sitemap/i);
  return m ? m[1] : "";
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
 * @param {string} xmlOrHtml
 */
export function parseCountrySitemapUrls(xmlOrHtml) {
  return [...String(xmlOrHtml || "").matchAll(/<loc>([^<]+hotel-sitemap[^<]*)<\/loc>/gi)].map(
    (m) => m[1].trim()
  );
}

/**
 * @param {object} model AEM page model from sitemap __NEXT_DATA__
 * @returns {{ marsha: string, title: string, url: string }[]}
 */
export function extractHotelsFromSitemapModel(model) {
  /** @type {{ marsha: string, title: string, url: string }[]} */
  const out = [];
  const seen = new Set();

  function walk(obj, depth = 0) {
    if (!obj || depth > 24) return;
    if (Array.isArray(obj)) return obj.forEach((x) => walk(x, depth + 1));
    if (typeof obj !== "object") return;

    const marsha = String(obj.marsha || obj.propertyCode || "").trim().toUpperCase();
    const title = String(obj.title || obj.name || obj.hotelName || "").trim();
    const url = String(obj.url || obj.href || obj.link || "").trim();

    if (marsha && title && /\/hotels\//i.test(url)) {
      const key = marsha;
      if (!seen.has(key)) {
        seen.add(key);
        out.push({ marsha, title, url: url.replace(/\/$/, "") + (url.endsWith("/") ? "" : "/") });
      }
    }

    for (const v of Object.values(obj)) walk(v, depth + 1);
  }

  walk(model);
  return out;
}

/**
 * @param {string} url
 */
export async function fetchMarriottCountrySitemapPage(url, opts = {}) {
  const timeoutMs = Number(
    opts.timeoutMs ??
      opts.timeout_ms ??
      process.env.CENSUS_DISCOVERY_FETCH_TIMEOUT_MS ??
      20_000
  );
  let res;
  if (timeoutMs > 0 || opts.signal) {
    const { timedFetch } = await import(
      "./research-engine-v2/census-autopilot-v4/discovery-railway-safe.js"
    );
    res = await timedFetch(url, {
      headers: MARRIOTT_FETCH_HEADERS,
      redirect: "follow",
      timeoutMs,
      signal: opts.signal,
      label: `marriott:${url}`,
    });
  } else {
    res = await fetch(url, { headers: MARRIOTT_FETCH_HEADERS, redirect: "follow" });
  }
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const html = await res.text();
  if (/access denied/i.test(html)) {
    throw new Error(`Access denied for ${url}`);
  }
  const data = parseNextDataFromHtml(html);
  const model = data?.props?.pageProps?.model;
  if (!model) throw new Error(`page model missing for ${url}`);
  const hotels = extractHotelsFromSitemapModel(model);
  return { url, slug: sitemapSlugFromUrl(url), hotels, hotelCount: hotels.length };
}

/**
 * @param {{ marsha: string, title: string, url: string }} row
 * @param {{ sourceUrl?: string, countrySlug?: string, countryLabel?: string }} [meta]
 */
export function normalizeMarriottDirectoryHotel(row, meta = {}) {
  const marsha = String(row.marsha || marshaFromMarriottWebsite(row.url)).trim().toUpperCase();
  const website = String(row.url || "").trim();
  return {
    source: "marriott_country_sitemap",
    sourceUrl: meta.sourceUrl || "",
    countryPage: meta.countrySlug || "",
    country: meta.countryLabel || "",
    brandPropertyCode: marsha,
    marshaCode: marsha,
    name: String(row.title || "").trim(),
    website,
    status: "Open",
    openDate: null,
    phone: "",
    city: "",
    state: "",
    postalCode: "",
    countryCode: "",
    latitude: null,
    longitude: null,
    amenityIds: [],
    amenitiesText: "",
    description: "",
    rawPayloadJson: JSON.stringify(row),
  };
}

/**
 * @param {object} [opts]
 * @param {string[]} [opts.countrySlugs] e.g. ["dominican-republic","mexico"]
 * @param {number} [opts.delayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function crawlMarriottCountrySitemaps(opts = {}) {
  const delayMs = opts.delayMs ?? 300;
  const onProgress = opts.onProgress;

  if (onProgress) onProgress(`Fetching sitemap index ${MARRIOTT_SITEMAP_INDEX}`);
  const indexRes = await fetch(MARRIOTT_SITEMAP_INDEX, {
    headers: MARRIOTT_FETCH_HEADERS,
    redirect: "follow",
  });
  if (!indexRes.ok) throw new Error(`HTTP ${indexRes.status} fetching sitemap index`);
  const indexXml = await indexRes.text();
  let countryUrls = parseCountrySitemapUrls(indexXml);

  if (opts.countrySlugs?.length) {
    const want = new Set(opts.countrySlugs.map((s) => s.toLowerCase().replace(/-hotel-sitemap$/i, "")));
    countryUrls = countryUrls.filter((u) => want.has(sitemapSlugFromUrl(u)));
  }

  /** @type {Map<string, ReturnType<typeof normalizeMarriottDirectoryHotel>>} */
  const byMarsha = new Map();
  const fetchErrors = [];

  for (let i = 0; i < countryUrls.length; i++) {
    const url = countryUrls[i];
    if (delayMs > 0 && i > 0) await sleep(delayMs);
    try {
      if (onProgress) onProgress(`[${i + 1}/${countryUrls.length}] ${url}`);
      const page = await fetchMarriottCountrySitemapPage(url);
      for (const hotel of page.hotels) {
        const normalized = normalizeMarriottDirectoryHotel(hotel, {
          sourceUrl: url,
          countrySlug: page.slug,
          countryLabel: SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL[page.slug] || "",
        });
        if (!normalized.marshaCode) continue;
        byMarsha.set(normalized.marshaCode, normalized);
      }
    } catch (err) {
      fetchErrors.push({ url, error: err?.message || String(err) });
      if (onProgress) onProgress(`  ERROR ${url}: ${err?.message || err}`);
    }
  }

  return {
    countryPagesFetched: countryUrls.length,
    hotelsFound: byMarsha.size,
    hotels: [...byMarsha.values()],
    fetchErrors,
  };
}

/**
 * Merge richer property rows from a browser-exported Marriott search JSON file.
 * Accepts arrays or { properties: [] } / GraphQL-shaped payloads.
 *
 * @param {unknown} payload
 */
export function normalizeMarriottSearchExport(payload) {
  /** @type {object[]} */
  let rows = [];
  if (Array.isArray(payload)) rows = payload;
  else if (payload && typeof payload === "object") {
    const p = /** @type {Record<string, unknown>} */ (payload);
    rows =
      /** @type {object[]} */ (p.properties) ||
      /** @type {object[]} */ (p.hotels) ||
      /** @type {object[]} */ (p.results) ||
      [];
    if (!rows.length && p.data) {
      const str = JSON.stringify(p.data);
      const urlMatches = [
        ...str.matchAll(/https:\\\/\\\/www\.marriott\.com\\\/en-us\\\/hotels\\\/[a-z0-9-]+\\\/overview/gi),
      ];
      rows = urlMatches.map((m) => ({
        url: m[0].replace(/\\\//g, "/"),
        marsha: marshaFromMarriottWebsite(m[0].replace(/\\\//g, "/")),
      }));
    }
  }

  return rows
    .map((row) => {
      const website = String(row.url || row.hotelUrl || row.overviewUrl || row.propertyUrl || "").trim();
      const marsha = String(
        row.marsha || row.marshaCode || row.propertyCode || row.propertyId || marshaFromMarriottWebsite(website)
      )
        .trim()
        .toUpperCase();
      const name = String(row.name || row.title || row.hotelName || "").trim();
      const description = String(
        row.description || row.shortDescription || row.hotelDescription || row.overview || ""
      ).trim();
      const amenitiesText = Array.isArray(row.amenities)
        ? row.amenities.map(String).join(", ")
        : String(row.amenitiesText || row.amenities || "").trim();
      if (!marsha && !website) return null;
      const normalized = normalizeMarriottDirectoryHotel(
        { marsha, title: name, url: website },
        { sourceUrl: "marriott_search_export" }
      );
      return {
        ...normalized,
        description,
        amenitiesText,
        phone: String(row.phone || row.telephone || "").trim(),
        city: String(row.city || row.hotelCity || "").trim(),
        country: String(row.country || row.hotelCountry || "").trim(),
        latitude: Number(row.latitude ?? row.hotelLatitude) || null,
        longitude: Number(row.longitude ?? row.hotelLongitude) || null,
      };
    })
    .filter(Boolean);
}
