/**
 * IHG official directory extract — destination country pages + hoteldetail sitemaps.
 * Source of truth: ihg.com destinations + bin/*.en-us.hoteldetail.xml
 */

import { ACCOR_COUNTRY_CODE_TO_LABEL } from "./brand-sitemap/cala-url-segments.js";
import { COUNTRY_CONFIG_LIST } from "./radar-buildout/country-configs.js";

export const IHG_ORIGIN = "https://www.ihg.com";
export const IHG_BIN_SITEMAP_INDEX = `${IHG_ORIGIN}/bin/sitemapindex.xml`;
export const IHG_CONTENT_SOURCE = "ihg_destination_directory";

export const IHG_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

/** Explicit destination slug overrides for Dealality CALA country labels. */
export const IHG_CALA_DESTINATION_SLUGS = {
  "Puerto Rico": "puerto-rico",
  "Dominican Republic": "dominican-republic",
  Colombia: "colombia",
  Mexico: "mexico",
  Panama: "panama",
  "Costa Rica": "costa-rica",
  Peru: "peru",
  Chile: "chile",
  Jamaica: "jamaica",
  Brazil: "brazil",
  Argentina: "argentina",
  Bahamas: "bahamas",
  Aruba: "aruba",
  Barbados: "barbados",
  "Cayman Islands": "cayman-islands",
  Grenada: "grenada",
  Dominica: "dominica",
  "Trinidad and Tobago": "trinidad-and-tobago",
  Guatemala: "guatemala",
  "El Salvador": "el-salvador",
  Honduras: "honduras",
  Nicaragua: "nicaragua",
  Ecuador: "ecuador",
  Uruguay: "uruguay",
};

export const IHG_COUNTRY_CODE_TO_LABEL = {
  ...ACCOR_COUNTRY_CODE_TO_LABEL,
  CW: "Curaçao",
  AN: "Curaçao",
  BQ: "Bonaire",
  MQ: "Martinique",
  GP: "Guadeloupe",
  CU: "Cuba",
  NI: "Nicaragua",
  DM: "Dominica",
  VG: "British Virgin Islands",
  MF: "Saint Martin",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} xml
 */
export function extractSitemapLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

/**
 * @param {string} name
 */
export function slugifyIhgCountry(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Canonical hoteldetail URL (strip reviews/subpaths).
 * @param {string} url
 */
export function canonicalIhgHoteldetailUrl(url) {
  const s = String(url || "").trim();
  if (!s) return "";
  try {
    const u = new URL(s.startsWith("http") ? s : `${IHG_ORIGIN}${s.startsWith("/") ? "" : "/"}${s}`);
    if (!/ihg\.com$/i.test(u.hostname) && !/\.ihg\.com$/i.test(u.hostname)) return "";
    const path = u.pathname.replace(/\/$/, "");
    const m = path.match(/^(.*?\/hoteldetail)(?:\/.*)?$/i);
    if (!m) return "";
    return `${u.origin}${m[1]}`;
  } catch {
    return "";
  }
}

/**
 * IHG property mnemonic (Property ID) from hoteldetail URL.
 * @param {string} url
 */
export function ihgPropertyIdFromUrl(url) {
  const canon = canonicalIhgHoteldetailUrl(url) || String(url || "");
  const m = canon.match(/\/([a-z0-9]{4,6})\/hoteldetail\/?$/i);
  return m ? m[1].toUpperCase() : "";
}

/**
 * Brand path segment from hoteldetail URL.
 * @param {string} url
 */
export function ihgBrandFromUrl(url) {
  try {
    const u = new URL(canonicalIhgHoteldetailUrl(url) || url);
    const parts = u.pathname.split("/").filter(Boolean);
    return parts[0] || "";
  } catch {
    return "";
  }
}

/**
 * City slug from hoteldetail URL.
 * @param {string} url
 */
export function ihgCitySlugFromUrl(url) {
  try {
    const u = new URL(canonicalIhgHoteldetailUrl(url) || url);
    const parts = u.pathname.split("/").filter(Boolean);
    // brand / hotels / region / locale / city / mnemonic / hoteldetail
    const hi = parts.indexOf("hoteldetail");
    if (hi >= 2) return parts[hi - 2] || "";
    return "";
  } catch {
    return "";
  }
}

/**
 * Parse hotel cards from an IHG destination/country HTML page.
 * @param {string} html
 * @param {{ countryLabel?: string, sourceUrl?: string }} [meta]
 */
export function parseIhgDestinationHotelCards(html, meta = {}) {
  /** @type {object[]} */
  const rows = [];
  const re =
    /<div[^>]*data-hotel-mnemonic="([A-Z0-9]+)"[^>]*data-hotel-countryCode="([A-Z]{2})"[^>]*[\s\S]*?<\/address>/gi;
  let m;
  while ((m = re.exec(html))) {
    const block = m[0];
    const mnemonic = m[1].toUpperCase();
    const countryCode = m[2].toUpperCase();
    const titleMatch = block.match(
      /<a[^>]*class="[^"]*cmp-card__title-link[^"]*"[^>]*href="([^"]+)"[^>]*>([^<]+)<\/a>/i
    );
    const href = titleMatch ? titleMatch[1] : "";
    const name = titleMatch ? decodeHtmlEntities(titleMatch[2]).trim() : "";
    const addressMatch = block.match(/<address[^>]*class="[^"]*cmp-card__address[^"]*"[^>]*>([\s\S]*?)<\/address>/i);
    const addressText = addressMatch
      ? decodeHtmlEntities(addressMatch[1].replace(/<br\s*\/?>/gi, ", ").replace(/<[^>]+>/g, " "))
          .replace(/\s+/g, " ")
          .trim()
      : "";
    const cityCountry = parseIhgAddressCityCountry(addressText, countryCode);
    const propertyUrl = canonicalIhgHoteldetailUrl(href);
    if (!mnemonic || !propertyUrl || !name) continue;
    rows.push({
      propertyId: mnemonic,
      mnemonic,
      name,
      inferredHotelName: name,
      city: cityCountry.city,
      country: meta.countryLabel || cityCountry.country || IHG_COUNTRY_CODE_TO_LABEL[countryCode] || "",
      countryCode,
      citySlug: ihgCitySlugFromUrl(propertyUrl),
      brand: ihgBrandFromUrl(propertyUrl),
      propertyUrl,
      website: propertyUrl,
      addressText,
      source: IHG_CONTENT_SOURCE,
      sourceUrl: meta.sourceUrl || "",
      calaFilterStatus: "included",
    });
  }
  return rows;
}

/**
 * @param {string} addressText
 * @param {string} countryCode
 */
export function parseIhgAddressCityCountry(addressText, countryCode) {
  const text = String(addressText || "").trim();
  const countryFromCode = IHG_COUNTRY_CODE_TO_LABEL[String(countryCode || "").toUpperCase()] || "";
  // Typical: "City,  10148, Dominican Republic" or "City, ST 12345, Country"
  const parts = text.split(",").map((p) => p.trim()).filter(Boolean);
  if (!parts.length) return { city: "", country: countryFromCode };
  let country = countryFromCode;
  let city = "";
  if (parts.length >= 2) {
    const last = parts[parts.length - 1];
    if (/[A-Za-z]/.test(last) && !/^\d/.test(last)) country = last;
    // city is usually second-to-last non-postal segment, or first line after street
    // Address lines: street, then "City, postal, Country"
    if (parts.length >= 3) {
      // find part that looks like city (not mostly digits)
      for (let i = parts.length - 2; i >= 0; i--) {
        const p = parts[i].replace(/\d+/g, "").trim();
        if (p && !/^[A-Z]{2}$/.test(p)) {
          city = parts[i].replace(/\s+\d+.*$/, "").trim();
          break;
        }
      }
    }
    if (!city) city = parts[0];
  } else {
    city = parts[0];
  }
  return { city, country: country || countryFromCode };
}

function decodeHtmlEntities(s) {
  return String(s || "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}

/**
 * @param {string} [xml]
 */
export function listIhgEnUsHoteldetailSitemaps(xml) {
  return extractSitemapLocs(xml).filter((u) => /\.en-us\.hoteldetail\.xml$/i.test(u));
}

/**
 * @param {string} url
 */
export function parseIhgHoteldetailSitemapUrl(url) {
  const canon = canonicalIhgHoteldetailUrl(url);
  if (!canon) return null;
  const propertyId = ihgPropertyIdFromUrl(canon);
  if (!propertyId) return null;
  return {
    propertyId,
    mnemonic: propertyId,
    propertyUrl: canon,
    website: canon,
    brand: ihgBrandFromUrl(canon),
    citySlug: ihgCitySlugFromUrl(canon),
    name: "",
    inferredHotelName: "",
    city: String(ihgCitySlugFromUrl(canon) || "").replace(/-/g, " "),
    country: "",
    countryCode: "",
    source: "ihg_hoteldetail_sitemap",
    calaFilterStatus: "uncertain",
  };
}

/**
 * Build destination page URL for a Dealality CALA country label.
 * @param {string} countryLabel
 */
export function ihgDestinationUrlForCountry(countryLabel) {
  const slug =
    IHG_CALA_DESTINATION_SLUGS[countryLabel] || slugifyIhgCountry(countryLabel);
  if (!slug) return "";
  return `${IHG_ORIGIN}/destinations/us/en/${slug}-hotels`;
}

/**
 * Crawl official IHG CALA destination pages into directory rows.
 * @param {object} [opts]
 * @param {string[]} [opts.countries] Dealality country labels
 * @param {number} [opts.delayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function extractIhgCalaDestinationDirectory(opts = {}) {
  const countries = opts.countries?.length ? opts.countries : COUNTRY_CONFIG_LIST;
  const delayMs = opts.delayMs ?? 200;
  /** @type {object[]} */
  const propertyRows = [];
  /** @type {Map<string, object>} */
  const byId = new Map();
  /** @type {object[]} */
  const pageResults = [];
  /** @type {object[]} */
  const fetchErrors = [];

  for (const country of countries) {
    const url = ihgDestinationUrlForCountry(country);
    if (!url) {
      pageResults.push({ country, status: "no_slug", cards: 0 });
      continue;
    }
    if (opts.onProgress) opts.onProgress(`Fetching ${country}…`);
    try {
      const res = await fetch(url, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
      const html = await res.text();
      if (!res.ok) {
        pageResults.push({
          country,
          url,
          status: res.status,
          finalUrl: res.url,
          cards: 0,
          ok: false,
        });
        if (delayMs) await sleep(delayMs);
        continue;
      }
      const parsed = parseIhgDestinationHotelCards(html, {
        countryLabel: country,
        sourceUrl: res.url || url,
      });
      let added = 0;
      for (const row of parsed) {
        // Prefer destination country label (Dealality geography)
        row.country = country;
        if (!byId.has(row.propertyId)) {
          byId.set(row.propertyId, row);
          propertyRows.push(row);
          added++;
        }
      }
      pageResults.push({
        country,
        url,
        status: res.status,
        finalUrl: res.url,
        cards: parsed.length,
        added,
        ok: parsed.length > 0,
      });
    } catch (err) {
      fetchErrors.push({ country, url, error: String(err?.message || err) });
      pageResults.push({ country, url, ok: false, error: String(err?.message || err) });
    }
    if (delayMs) await sleep(delayMs);
  }

  return {
    ok: true,
    source: IHG_CONTENT_SOURCE,
    countriesRequested: countries.length,
    pagesOk: pageResults.filter((p) => p.ok).length,
    propertyRows,
    pageResults,
    fetchErrors,
  };
}

/**
 * Load all en-us hoteldetail sitemap hotels (global). Used for URL completeness.
 * @param {object} [opts]
 */
export async function crawlIhgHoteldetailSitemaps(opts = {}) {
  const delayMs = opts.delayMs ?? 100;
  const idx = await fetch(IHG_BIN_SITEMAP_INDEX, { headers: IHG_FETCH_HEADERS });
  const idxXml = await idx.text();
  if (!idx.ok) {
    return { ok: false, error: `sitemap index HTTP ${idx.status}`, hotels: [] };
  }
  const sitemaps = listIhgEnUsHoteldetailSitemaps(idxXml);
  /** @type {Map<string, object>} */
  const byId = new Map();
  for (const sm of sitemaps) {
    if (opts.onProgress) opts.onProgress(`Sitemap ${sm.split("/").pop()}`);
    try {
      const r = await fetch(sm, { headers: IHG_FETCH_HEADERS });
      const xml = await r.text();
      for (const loc of extractSitemapLocs(xml)) {
        if (/\/hoteldetail\/.+/i.test(loc.replace(/\/$/, ""))) continue;
        const parsed = parseIhgHoteldetailSitemapUrl(loc);
        if (!parsed) continue;
        if (!byId.has(parsed.propertyId)) byId.set(parsed.propertyId, parsed);
      }
    } catch (err) {
      if (opts.onProgress) opts.onProgress(`Sitemap error ${sm}: ${err.message}`);
    }
    if (delayMs) await sleep(delayMs);
  }
  return { ok: true, sitemaps: sitemaps.length, hotels: [...byId.values()] };
}

/**
 * Merge destination rows with sitemap canonical URLs when present.
 * @param {object[]} destinationRows
 * @param {object[]} sitemapHotels
 */
export function mergeIhgDirectoryWithSitemap(destinationRows, sitemapHotels) {
  const byId = new Map(sitemapHotels.map((h) => [h.propertyId, h]));
  return destinationRows.map((row) => {
    const sm = byId.get(row.propertyId);
    if (!sm) return row;
    return {
      ...row,
      propertyUrl: sm.propertyUrl || row.propertyUrl,
      website: sm.propertyUrl || row.website,
      brand: sm.brand || row.brand,
      citySlug: sm.citySlug || row.citySlug,
    };
  });
}

/** CALA country path suffixes used on ihg.com destination city pages. */
export const IHG_CALA_DEST_PATH_SUFFIXES = [
  "mexico",
  "jalisco",
  "nuevo-leon",
  "quintana-roo",
  "yucatan",
  "sonora",
  "veracruz",
  "guanajuato",
  "michoacan-de-ocampo",
  "estado-de-mexico",
  "campeche",
  "zacatecas",
  "hidalgo",
  "baja-california-nort",
  "baja-california-sur",
  "aguascalientes",
  "durango",
  "queretaro",
  "chiapas",
  "tabasco",
  "oaxaca",
  "puebla",
  "morelos",
  "tamaulipas",
  "coahuila",
  "sinaloa",
  "nayarit",
  "colima",
  "tlaxcala",
  "colombia",
  "brazil",
  "chile",
  "ecuador",
  "panama",
  "costa-rica",
  "dominican-republic",
  "puerto-rico",
  "argentina",
  "bahamas",
  "cayman-islands",
  "cayman",
  "grenada",
  "jamaica",
  "guatemala",
  "honduras",
  "nicaragua",
  "el-salvador",
  "uruguay",
  "peru",
  "aruba",
  "barbados",
  "trinidad-and-tobago",
  "dominica",
];

const IHG_DEST_PATH_BLOCKLIST =
  /(united-states|florida|california|texas|arizona|nevada|colorado|georgia|ohio|illinois|new-york|carolina|virginia|pennsylvania|new-mexico|spain|france|italy|germany|china|japan|india|thailand|united-kingdom|morocco|portugal|canada)/i;

/**
 * Infer Dealality country label from an ihg.com destination path slug.
 * @param {string} pathSlug e.g. morelia-michoacan-de-ocampo
 */
export function ihgCountryLabelFromDestPath(pathSlug) {
  const path = String(pathSlug || "")
    .replace(/^https:\/\/www\.ihg\.com\//i, "")
    .toLowerCase()
    .replace(/\/.*$/, "");
  if (!path) return "";
  if (path.endsWith("-mexico") || path === "mexico" || /-(jalisco|nuevo-leon|quintana-roo|yucatan|sonora|veracruz|guanajuato|michoacan-de-ocampo|estado-de-mexico|campeche|zacatecas|hidalgo|baja-california-nort|baja-california-sur|aguascalientes|durango|queretaro|chiapas|tabasco|oaxaca|puebla|morelos|tamaulipas|coahuila|sinaloa|nayarit|colima|tlaxcala)$/.test(path)) {
    return "Mexico";
  }
  const pairs = [
    ["dominican-republic", "Dominican Republic"],
    ["puerto-rico", "Puerto Rico"],
    ["costa-rica", "Costa Rica"],
    ["el-salvador", "El Salvador"],
    ["cayman-islands", "Cayman Islands"],
    ["trinidad-and-tobago", "Trinidad and Tobago"],
    ["colombia", "Colombia"],
    ["brazil", "Brazil"],
    ["chile", "Chile"],
    ["ecuador", "Ecuador"],
    ["panama", "Panama"],
    ["argentina", "Argentina"],
    ["bahamas", "Bahamas"],
    ["grenada", "Grenada"],
    ["jamaica", "Jamaica"],
    ["guatemala", "Guatemala"],
    ["honduras", "Honduras"],
    ["nicaragua", "Nicaragua"],
    ["uruguay", "Uruguay"],
    ["peru", "Peru"],
    ["aruba", "Aruba"],
    ["barbados", "Barbados"],
    ["dominica", "Dominica"],
  ];
  for (const [suffix, label] of pairs) {
    if (path === suffix || path.endsWith("-" + suffix)) return label;
  }
  return "";
}

/**
 * Official hotel name from hoteldetail HTML (h1 / JSON-LD / og:title).
 * @param {string} html
 */
export function extractIhgHotelNameFromHoteldetailHtml(html) {
  const h1 = [...String(html || "").matchAll(/<h1[^>]*>([\s\S]*?)<\/h1>/gi)]
    .map((m) =>
      decodeHtmlEntities(m[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim())
    )
    .find((t) => t && !/hotels$/i.test(t) && t.length > 3);
  if (h1) return h1.replace(/\s+by IHG\s*$/i, "").trim();
  const jsonName =
    String(html || "").match(/"hotelName"\s*:\s*"([^"]+)"/i)?.[1] ||
    String(html || "").match(/"@type"\s*:\s*"Hotel"[\s\S]{0,200}?"name"\s*:\s*"([^"]+)"/i)?.[1];
  if (jsonName) {
    return decodeHtmlEntities(jsonName).replace(/\s+by IHG\s*$/i, "").trim();
  }
  const og =
    String(html || "").match(/property=["']og:title["'][^>]*content=["']([^"']+)["']/i)?.[1] ||
    String(html || "").match(/content=["']([^"']+)["'][^>]*property=["']og:title["']/i)?.[1] ||
    "";
  if (og) {
    return decodeHtmlEntities(og)
      .replace(/\s*[|\-–].*$/, "")
      .replace(/\s+by IHG\s*$/i, "")
      .replace(/\s*\*+\s*$/, "")
      .trim();
  }
  return "";
}

/**
 * List CALA city destination page URLs from destinations.en.sitemap.xml.
 * @param {string} xml
 */
export function listIhgCalaCityDestinationUrls(xml) {
  return extractSitemapLocs(xml).filter((u) => {
    if (/\/(pool|pet-friendly|beach)-hotels$/i.test(u)) return false;
    if (/hotels-near-/i.test(u)) return false;
    const path = u.replace(/^https:\/\/www\.ihg\.com\//i, "").toLowerCase();
    if (!path.includes("-")) return false;
    if (IHG_DEST_PATH_BLOCKLIST.test(path)) return false;
    return IHG_CALA_DEST_PATH_SUFFIXES.some(
      (s) => path === s || path.endsWith("-" + s) || path.includes("-" + s)
    );
  });
}

function countriesRoughlyAlign(dealalityLabel, codeLabel, countryCode) {
  const a = String(dealalityLabel || "").toLowerCase();
  const b = String(codeLabel || "").toLowerCase();
  if (!a || !b) return true;
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  // US cards on MX border pages
  if (countryCode === "US" || b === "united states") return false;
  return false;
}

/**
 * Harvest hotel cards from CALA city destination pages (fills country-page gaps).
 * @param {object} [opts]
 */
export async function harvestIhgCalaCityDestinationPages(opts = {}) {
  const delayMs = opts.delayMs ?? 100;
  const knownIds = new Set(opts.knownPropertyIds || []);
  /** @type {object[]} */
  const propertyRows = [];
  /** @type {object[]} */
  const pageResults = [];

  let locs = opts.urls || [];
  if (!locs.length) {
    const res = await fetch(`${IHG_ORIGIN}/services/sitemaps/destinations.en.sitemap.xml`, {
      headers: IHG_FETCH_HEADERS,
    });
    const xml = await res.text();
    if (!res.ok) {
      return { ok: false, error: `destinations sitemap HTTP ${res.status}`, propertyRows, pageResults };
    }
    locs = listIhgCalaCityDestinationUrls(xml);
  }

  for (const url of locs) {
    if (opts.onProgress) opts.onProgress(`City page ${url.replace(IHG_ORIGIN + "/", "")}`);
    try {
      const res = await fetch(url, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
      const html = await res.text();
      if (!res.ok || /\/explore\/?$/i.test(res.url) || /\/explore(\/|$)/i.test(res.url)) {
        pageResults.push({
          url,
          status: res.status,
          finalUrl: res.url,
          cards: 0,
          added: 0,
          skipped: "explore_or_http",
        });
        if (delayMs) await sleep(delayMs);
        continue;
      }
      const countryLabel = ihgCountryLabelFromDestPath(url) || ihgCountryLabelFromDestPath(res.url);
      const parsed = parseIhgDestinationHotelCards(html, {
        countryLabel,
        sourceUrl: res.url || url,
      });
      let added = 0;
      for (const row of parsed) {
        // Border destination pages often list nearby US hotels — keep card countryCode aligned.
        if (row.countryCode) {
          const codeLabel = IHG_COUNTRY_CODE_TO_LABEL[row.countryCode];
          if (countryLabel && codeLabel && !countriesRoughlyAlign(countryLabel, codeLabel, row.countryCode)) {
            continue;
          }
          if (countryLabel === "Mexico" && row.countryCode !== "MX") continue;
        }
        if (countryLabel) row.country = countryLabel;
        row.source = "ihg_city_destination_page";
        if (knownIds.has(row.propertyId)) continue;
        knownIds.add(row.propertyId);
        propertyRows.push(row);
        added++;
      }
      pageResults.push({
        url,
        status: res.status,
        finalUrl: res.url,
        cards: parsed.length,
        added,
        countryLabel,
        ok: added > 0,
      });
    } catch (err) {
      pageResults.push({ url, error: String(err?.message || err) });
    }
    if (delayMs) await sleep(delayMs);
  }

  return {
    ok: true,
    pagesScanned: locs.length,
    propertyRows,
    pageResults,
  };
}

/**
 * Enrich sitemap hoteldetail URLs with official h1 names (CALA gap fill).
 * Only keeps hotels with a usable name that did not redirect to /explore.
 * @param {object[]} sitemapHotels
 * @param {object} [opts]
 */
export async function enrichIhgSitemapHotelsWithNames(sitemapHotels, opts = {}) {
  const delayMs = opts.delayMs ?? 120;
  const knownIds = new Set(opts.knownPropertyIds || []);
  const citySlugAllow = new Set(
    (opts.citySlugs || []).map((s) => String(s || "").toLowerCase()).filter(Boolean)
  );
  const countryBySlug = opts.countryByCitySlug || {};
  /** @type {object[]} */
  const propertyRows = [];
  /** @type {object[]} */
  const fetchResults = [];

  for (const hotel of sitemapHotels) {
    if (!hotel?.propertyId || knownIds.has(hotel.propertyId)) continue;
    const slug = String(hotel.citySlug || "").toLowerCase();
    if (citySlugAllow.size && !citySlugAllow.has(slug)) continue;
    if (opts.onProgress) opts.onProgress(`Hoteldetail ${hotel.propertyId}`);
    try {
      const res = await fetch(hotel.propertyUrl, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
      const html = await res.text();
      if (!res.ok || /\/explore\/?$/i.test(res.url) || /\/explore(\/|$)/i.test(res.url)) {
        fetchResults.push({ propertyId: hotel.propertyId, skipped: "explore_or_http", status: res.status });
        if (delayMs) await sleep(delayMs);
        continue;
      }
      const idLower = String(hotel.propertyId).toLowerCase();
      if (!new RegExp(`/${idLower}/hoteldetail`, "i").test(res.url)) {
        fetchResults.push({
          propertyId: hotel.propertyId,
          skipped: "redirected_away_from_hoteldetail",
          finalUrl: res.url,
        });
        if (delayMs) await sleep(delayMs);
        continue;
      }
      const name = extractIhgHotelNameFromHoteldetailHtml(html);
      if (!name || /^(hotels?|boutique hotel|extended stay hotel)\b/i.test(name)) {
        fetchResults.push({ propertyId: hotel.propertyId, skipped: "weak_name", name });
        if (delayMs) await sleep(delayMs);
        continue;
      }
      const country = countryBySlug[slug] || hotel.country || "";
      const row = {
        ...hotel,
        name,
        inferredHotelName: name,
        country,
        source: "ihg_hoteldetail_sitemap_enriched",
        calaFilterStatus: country ? "included" : "uncertain",
      };
      propertyRows.push(row);
      knownIds.add(hotel.propertyId);
      fetchResults.push({ propertyId: hotel.propertyId, name, country, ok: true });
    } catch (err) {
      fetchResults.push({ propertyId: hotel.propertyId, error: String(err?.message || err) });
    }
    if (delayMs) await sleep(delayMs);
  }

  return { ok: true, propertyRows, fetchResults };
}

/**
 * Union destination rows with additional directory rows (by propertyId).
 * @param {object[]} baseRows
 * @param {object[]} extraRows
 */
export function unionIhgDirectoryRows(baseRows, extraRows) {
  const byId = new Map();
  for (const row of baseRows || []) {
    if (row?.propertyId) byId.set(row.propertyId, row);
  }
  for (const row of extraRows || []) {
    if (!row?.propertyId) continue;
    if (!byId.has(row.propertyId)) byId.set(row.propertyId, row);
  }
  return [...byId.values()];
}
