/**
 * Accor continent destination browse pages — JSON-LD ItemList discovery.
 * Names here are cleaner than hotel-page SEO titles (e.g. "ibis Sinop" vs "Hotel in…").
 */

import { ACCOR_FETCH_HEADERS, accorHotelCodeFromUrl } from "./accor-brand-directory-extract.js";
import { accorCanonicalPropertyUrl } from "./hotel-census/accor-directory-name-normalize.js";

export const ACCOR_CONTINENT_PAGES = {
  /** CALA-primary */
  southAmerica: {
    label: "South America",
    slug: "hotels-south-america-c02",
    calaRelevant: true,
  },
  centralAmerica: {
    label: "Central America",
    slug: "hotels-central-america-c10",
    calaRelevant: true,
  },
  /** Mexico/Caribbean only after country filter — page mixes US/Canada */
  northAmerica: {
    label: "North America",
    slug: "hotels-north-america-c01",
    calaRelevant: "partial",
  },
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} html
 */
export function parseAccorContinentHotelsFromHtml(html) {
  /** @type {{ propertyId: string, propertyUrl: string, inferredHotelName: string, source: string }[]} */
  const hotels = [];
  const seen = new Set();

  for (const block of String(html).match(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi) || []) {
    try {
      const json = JSON.parse(block.replace(/<\/?script[^>]*>/gi, "").trim());
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (obj?.["@type"] !== "ItemList" || !Array.isArray(obj.itemListElement)) continue;
        for (const el of obj.itemListElement) {
          const item = el.item || el;
          const rawUrl = String(item?.url || item?.["@id"] || "");
          if (!/\/hotel\//i.test(rawUrl)) continue;
          const propertyId = accorHotelCodeFromUrl(rawUrl);
          if (!propertyId || seen.has(propertyId)) continue;
          seen.add(propertyId);
          hotels.push({
            propertyId,
            propertyUrl: accorCanonicalPropertyUrl(propertyId),
            inferredHotelName: String(item?.name || "").trim(),
            source: "accor_continent_browse",
          });
        }
      }
    } catch {
      /* skip */
    }
  }
  return hotels;
}

/**
 * @param {string} html
 * @param {string} baseUrl
 */
export function maxContinentPageIndex(html, baseUrl) {
  let max = 1;
  for (const m of String(html).matchAll(/pageIndex=(\d+)/gi)) {
    const n = Number(m[1]);
    if (Number.isFinite(n) && n > max) max = n;
  }
  return max;
}

/**
 * @param {object} opts
 * @param {keyof typeof ACCOR_CONTINENT_PAGES} [opts.continent]
 * @param {number} [opts.maxPages]
 * @param {number} [opts.delayMs]
 * @param {typeof fetch} [opts.fetchFn]
 */
export async function extractAccorContinentHotels(opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const delayMs = opts.delayMs ?? 150;
  const continents = opts.continent
    ? [ACCOR_CONTINENT_PAGES[opts.continent]]
    : Object.values(ACCOR_CONTINENT_PAGES);

  /** @type {object[]} */
  const propertyRows = [];
  const seen = new Set();

  for (const continent of continents) {
    const baseUrl = `https://all.accor.com/a/en/destination/continent/${continent.slug}.html`;
    const firstRes = await fetchFn(baseUrl, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
    if (!firstRes.ok) continue;
    const firstHtml = await firstRes.text();
    const pageMax = opts.maxPages ?? maxContinentPageIndex(firstHtml, baseUrl);

    for (let pageIndex = 1; pageIndex <= pageMax; pageIndex++) {
      const url = pageIndex === 1 ? baseUrl : `${baseUrl}?pageIndex=${pageIndex}`;
      const res =
        pageIndex === 1
          ? firstRes
          : await fetchFn(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
      const html = pageIndex === 1 ? firstHtml : await res.text();
      if (!res.ok) continue;

      for (const row of parseAccorContinentHotelsFromHtml(html)) {
        if (seen.has(row.propertyId)) continue;
        seen.add(row.propertyId);
        propertyRows.push({
          ...row,
          continent: continent.label,
          continentSlug: continent.slug,
          calaRelevant: continent.calaRelevant,
          sourceName: `Accor ${continent.label} browse`,
          sourceType: "brand_directory",
        });
      }
      if (delayMs > 0 && pageIndex < pageMax) await sleep(delayMs);
    }
  }

  return {
    ok: true,
    propertyRows,
    summary: {
      total: propertyRows.length,
      byContinent: continents.map((c) => ({
        label: c.label,
        count: propertyRows.filter((r) => r.continent === c.label).length,
      })),
    },
  };
}
