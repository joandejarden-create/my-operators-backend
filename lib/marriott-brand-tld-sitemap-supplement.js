/**
 * Supplement marriott.com country sitemaps with brand-TLD HWS sitemap URLs (e.g. ritzcarlton.com).
 */

import {
  marshaFromMarriottWebsite,
  normalizeMarriottDirectoryHotel,
  MARRIOTT_FETCH_HEADERS,
} from "./marriott-brand-directory-extract.js";

const RITZ_HWS_SITEMAP_URLS = Array.from({ length: 7 }, (_, i) =>
  `https://www.ritzcarlton.com/content/dam/marriott-hws/sitemap-xmls/trc-en-sitemap-hws-${i + 1}.xml`
);

/**
 * @param {string} xml
 * @returns {{ marsha: string, url: string, title: string }[]}
 */
export function extractOverviewUrlsFromHwsSitemapXml(xml) {
  /** @type {Map<string, { marsha: string, url: string, title: string }>} */
  const byMarsha = new Map();
  for (const m of String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)) {
    const url = m[1].trim();
    if (!/\/overview\/?$/i.test(url)) continue;
    const marsha = marshaFromMarriottWebsite(url);
    if (!marsha) continue;
    const slugPart = url.match(/\/hotels\/[a-z0-9]+-([^/]+)\/overview/i)?.[1] || "";
    const title = slugPart.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    if (!byMarsha.has(marsha)) {
      byMarsha.set(marsha, { marsha, url: url.replace(/\/$/, ""), title });
    }
  }
  return [...byMarsha.values()];
}

/**
 * @param {object} [opts]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function crawlRitzCarltonOverviewSupplement(opts = {}) {
  const onProgress = opts.onProgress;
  /** @type {Map<string, ReturnType<typeof normalizeMarriottDirectoryHotel>>} */
  const byMarsha = new Map();
  const fetchErrors = [];

  for (let i = 0; i < RITZ_HWS_SITEMAP_URLS.length; i++) {
    const url = RITZ_HWS_SITEMAP_URLS[i];
    try {
      if (onProgress) onProgress(`[ritz ${i + 1}/${RITZ_HWS_SITEMAP_URLS.length}] ${url}`);
      const res = await fetch(url, { headers: MARRIOTT_FETCH_HEADERS, redirect: "follow" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const rows = extractOverviewUrlsFromHwsSitemapXml(await res.text());
      for (const row of rows) {
        const normalized = normalizeMarriottDirectoryHotel(
          { marsha: row.marsha, title: row.title, url: row.url },
          { sourceUrl: url, countrySlug: "", countryLabel: "" }
        );
        normalized.source = "ritzcarlton_hws_sitemap";
        if (!byMarsha.has(normalized.marshaCode)) byMarsha.set(normalized.marshaCode, normalized);
      }
    } catch (err) {
      fetchErrors.push({ url, error: err?.message || String(err) });
    }
  }

  return {
    hotelsFound: byMarsha.size,
    hotels: [...byMarsha.values()],
    fetchErrors,
  };
}

/**
 * Merge brand-TLD supplement rows without overwriting marriott.com country sitemap rows.
 *
 * @param {ReturnType<typeof normalizeMarriottDirectoryHotel>[]} primary
 * @param {ReturnType<typeof normalizeMarriottDirectoryHotel>[]} supplement
 */
export function mergeMarriottDirectoryRows(primary, supplement) {
  const byMarsha = new Map(primary.map((h) => [h.marshaCode, h]));
  for (const row of supplement) {
    if (!row.marshaCode || byMarsha.has(row.marshaCode)) continue;
    byMarsha.set(row.marshaCode, row);
  }
  return [...byMarsha.values()];
}
