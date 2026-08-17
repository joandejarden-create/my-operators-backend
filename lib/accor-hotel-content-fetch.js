/**
 * Fetch verified amenities from all.accor.com hotel pages (JSON-LD amenityFeature).
 */

import { load as loadCheerio } from "cheerio";
import { ACCOR_FETCH_HEADERS, parseAccorHotelMetadataFromHtml, accorAmenityFeatures } from "./accor-brand-directory-extract.js";

export const ACCOR_CONTENT_SOURCE = "accor_hotel_page";

function cleanText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeLabels(labels) {
  const seen = new Set();
  /** @type {string[]} */
  const out = [];
  for (const raw of labels) {
    const label = cleanText(raw);
    if (!label || label.length < 2 || label.length > 120) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out.sort((a, b) => a.localeCompare(b));
}

/**
 * @param {string} html
 */
export function parseAccorAmenitiesFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  const $ = loadCheerio(html);

  $("script[type='application/ld+json']").each((_, el) => {
    try {
      const json = JSON.parse($(el).html() || "");
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;
        for (const feat of accorAmenityFeatures(obj.amenityFeature)) {
          const name = cleanText(feat?.name);
          const val = String(feat?.value ?? "true").toLowerCase();
          if (name && val !== "false") labels.push(name);
        }
      }
    } catch {
      /* skip */
    }
  });

  const amenities = dedupeLabels(labels);
  return {
    amenities,
    amenitiesText: amenities.join("; "),
    metadata: parseAccorHotelMetadataFromHtml(html),
    parseErrors: amenities.length ? [] : ["no_amenities_parsed"],
  };
}

/**
 * @param {string} propertyUrl
 * @param {object} [opts]
 */
export async function fetchAccorHotelAmenities(propertyUrl, opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const url = String(propertyUrl || "").trim();
  if (!url) {
    return { ok: false, status: 0, amenities: [], amenitiesText: "", source: null, parseErrors: ["empty_url"] };
  }

  let res;
  try {
    res = await fetchFn(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
  } catch (err) {
    return {
      ok: false,
      status: 0,
      amenities: [],
      amenitiesText: "",
      source: ACCOR_CONTENT_SOURCE,
      parseErrors: [`fetch_error:${err?.message || err}`],
    };
  }
  const html = await res.text();
  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      amenities: [],
      amenitiesText: "",
      source: ACCOR_CONTENT_SOURCE,
      parseErrors: [`http_${res.status}`],
    };
  }

  const parsed = parseAccorAmenitiesFromHtml(html);
  return {
    ok: parsed.amenities.length > 0,
    status: res.status,
    ...parsed,
    source: ACCOR_CONTENT_SOURCE,
  };
}
