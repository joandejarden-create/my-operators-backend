/**
 * Choice regional browse pages — JSON-LD Hotel discovery.
 * Property pages are often Akamai-blocked; regional listings return full hotel names + URLs.
 */

import { choicePropertyIdFromUrl } from "./choice-hotel-content-fetch.js";
import { censusCountryToSitemapSlug } from "./marriott-brand-directory-extract.js";

export const CHOICE_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml",
  "Accept-Language": "en-US,en;q=0.9",
};

/** Mexico needs explicit placeId; other countries work with slug-only regional URL. */
export const CHOICE_REGIONAL_PLACE_IDS = {
  mexico: "ChIJU1NoiDs6BIQREZgJa760ZO0",
};

/** Census country → Choice property ID prefix (sitemap + regional JSON). */
export const CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX = {
  Mexico: "MX",
  Brazil: "BR",
  Colombia: "CB",
  Chile: "CL",
  Peru: "PE",
  Ecuador: "EC",
  "Costa Rica": "CR",
  Panama: "PN",
  Guatemala: "GT",
  "Dominican Republic": "DO",
  "Trinidad and Tobago": "TT",
  Barbados: "BB",
  Bahamas: "BS",
  Aruba: "AW",
  "Puerto Rico": "PR",
  Uruguay: "UY",
  Suriname: "SR",
  Grenada: "GD",
  Honduras: "HN",
  "El Salvador": "SV",
  Argentina: "AA",
  Bolivia: "BO",
  "US Virgin Islands": "VI",
};

/** Countries with sitemap URLs but empty regional Hotel JSON (~2.8MB shell pages). */
export const CHOICE_SITEMAP_ONLY_COUNTRIES = ["Bolivia", "US Virgin Islands", "Jamaica"];

/** Choice census countries with a working regional browse page (discovery 2026-07-06). */
export const CHOICE_CALA_COUNTRIES_WITH_REGIONAL = [
  "Mexico",
  "Brazil",
  "Colombia",
  "Chile",
  "Peru",
  "Ecuador",
  "Costa Rica",
  "Panama",
  "Guatemala",
  "Dominican Republic",
  "Trinidad and Tobago",
  "Barbados",
  "Bahamas",
  "Aruba",
  "Puerto Rico",
  "Uruguay",
  "Suriname",
  "Grenada",
  "Honduras",
  "El Salvador",
  "Argentina",
];

/**
 * @param {string} countryLabel Census country field value
 */
export function buildChoiceRegionalPageForCountry(countryLabel) {
  const slug = censusCountryToSitemapSlug(countryLabel);
  if (!slug) return null;

  const placeId = CHOICE_REGIONAL_PLACE_IDS[slug];
  const query = placeId ? `?placeId=${placeId}` : "";
  return {
    label: countryLabel,
    country: countryLabel,
    slug,
    url: `https://www.choicehotels.com/en-uk/${slug}/regional-hotels${query}`,
    placeId: placeId || "",
  };
}

/**
 * @param {string[]} [countryLabels]
 */
export function buildChoiceCalaRegionalPages(countryLabels = CHOICE_CALA_COUNTRIES_WITH_REGIONAL) {
  return countryLabels
    .filter((c) => !CHOICE_SITEMAP_ONLY_COUNTRIES.includes(c))
    .map((c) => buildChoiceRegionalPageForCountry(c))
    .filter(Boolean);
}

/** @deprecated Use buildChoiceCalaRegionalPages() — kept for imports. */
export const CHOICE_CALA_REGIONAL_PAGES = buildChoiceCalaRegionalPages();

/**
 * Census Website: no locale prefix (en-uk, en-us).
 * @param {string} url
 */
export function canonicalChoicePropertyUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (raw.startsWith("/")) {
    return `https://www.choicehotels.com${raw}`.replace(
      /^(https:\/\/www\.choicehotels\.com)\/en-[a-z]{2}\//i,
      "$1/"
    );
  }
  return raw.replace(/^(https:\/\/www\.choicehotels\.com)\/en-[a-z]{2}\//i, "$1/");
}

/**
 * City slug from property URL path: /{region}/{city}/{brand}/{id}
 * @param {string} url
 */
export function choiceCitySlugFromPropertyUrl(url) {
  const path = String(url || "").replace(/^https?:\/\/[^/]+/i, "");
  const parts = path.split("/").filter(Boolean);
  if (parts[0]?.match(/^en-[a-z]{2}$/i)) parts.shift();
  return parts.length >= 2 ? parts[1] : "";
}

/**
 * Parse Hotel nodes from embedded JSON-LD / JSON blobs in regional HTML.
 * @param {string} html
 */
export function parseChoiceRegionalHotelsFromHtml(html) {
  /** @type {Map<string, { propertyId: string, name: string, propertyUrl: string, citySlug: string, source: string }>} */
  const byId = new Map();

  function ingestHotel(obj) {
    if (!obj || typeof obj !== "object") return;
    const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
    if (!types.some((t) => String(t).toLowerCase() === "hotel")) return;

    const name = String(obj.name || "").trim();
    const rawUrl = String(obj.url || "").trim();
    if (!rawUrl || !/\/[a-z]{2}\d{2,3}$/i.test(rawUrl)) return;

    const propertyUrl = canonicalChoicePropertyUrl(rawUrl);
    const propertyId = choicePropertyIdFromUrl(propertyUrl).toUpperCase();
    if (!propertyId) return;

    const prev = byId.get(propertyId);
    if (!prev || (name && name.length > (prev.name || "").length)) {
      byId.set(propertyId, {
        propertyId,
        name: name || prev?.name || propertyId,
        propertyUrl,
        citySlug: choiceCitySlugFromPropertyUrl(propertyUrl),
        source: "choice_regional_jsonld",
      });
    }
  }

  function walk(node) {
    if (!node) return;
    if (Array.isArray(node)) {
      for (const item of node) walk(item);
      return;
    }
    if (typeof node !== "object") return;
    ingestHotel(node);
    for (const value of Object.values(node)) walk(value);
  }

  for (const block of String(html).match(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi) || []) {
    try {
      const inner = block.replace(/<\/?script[^>]*>/gi, "").trim();
      walk(JSON.parse(inner));
    } catch {
      /* skip */
    }
  }

  // Choice regional pages embed Hotel nodes inline (no ld+json script tags).
  for (const m of String(html).matchAll(
    /"@type":"Hotel","name":"((?:\\.|[^"\\])*)","url":"((?:\\.|[^"\\])*)"/gi
  )) {
    const name = m[1].replace(/\\"/g, '"').replace(/\\\\/g, "\\");
    const rawUrl = m[2].replace(/\\"/g, '"').replace(/\\\\/g, "\\");
    ingestHotel({ "@type": "Hotel", name, url: rawUrl });
  }

  // Fallback: looser name/url pairs near property id tokens
  for (const m of String(html).matchAll(
    /"name":"((?:\\.|[^"\\])*)","url":"([^"]*\/([a-z]{2}\d{2,3}))"/gi
  )) {
    const name = m[1].replace(/\\"/g, '"');
    const rawUrl = m[2];
    const propertyId = m[3].toUpperCase();
    if (!byId.has(propertyId)) {
      ingestHotel({ "@type": "Hotel", name, url: rawUrl });
    }
  }

  return [...byId.values()];
}

/**
 * @param {string} regionalUrl
 * @param {{ fetchFn?: typeof fetch }} [opts]
 */
export async function fetchChoiceRegionalHotels(regionalUrl, opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const res = await fetchFn(regionalUrl, {
    redirect: "follow",
    headers: CHOICE_FETCH_HEADERS,
  });

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      regionalUrl,
      hotels: [],
      error: `http_${res.status}`,
    };
  }

  const html = await res.text();
  if (/access denied|robot check/i.test(html)) {
    return {
      ok: false,
      status: res.status,
      regionalUrl,
      hotels: [],
      error: "blocked",
      htmlLength: html.length,
    };
  }

  const hotels = parseChoiceRegionalHotelsFromHtml(html);
  return {
    ok: true,
    status: res.status,
    regionalUrl,
    htmlLength: html.length,
    hotels,
  };
}

/**
 * Fetch all configured CALA regional pages (dedupe by propertyId).
 * @param {{ pages?: typeof CHOICE_CALA_REGIONAL_PAGES, delayMs?: number, fetchFn?: typeof fetch }} [opts]
 */
export async function fetchChoiceCalaRegionalDirectory(opts = {}) {
  const pages = opts.pages || CHOICE_CALA_REGIONAL_PAGES;
  const delayMs = opts.delayMs ?? 200;
  /** @type {Map<string, object>} */
  const byId = new Map();

  for (const page of pages) {
    const result = await fetchChoiceRegionalHotels(page.url, { fetchFn: opts.fetchFn });
    for (const h of result.hotels || []) {
      byId.set(h.propertyId, { ...h, regionalLabel: page.label, regionalCountry: page.country });
    }
    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  return {
    pages: pages.length,
    hotels: [...byId.values()],
  };
}
