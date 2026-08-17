/**
 * Fetch verified amenities from wyndhamhotels.com property overview pages.
 */

import { load as loadCheerio } from "cheerio";
import { WYNDHAM_FETCH_HEADERS } from "./wyndham-brand-directory-extract.js";

export const WYNDHAM_CONTENT_SOURCE = "wyndham_services_amenities";

function wyndhamServicesAmenitiesUrl(propertyUrl) {
  const raw = String(propertyUrl || "").trim();
  if (!raw) return "";
  if (/\/services-amenities/i.test(raw)) return raw.split("?")[0];
  return raw.replace(/\/overview\/?$/i, "/services-amenities");
}

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
export function parseWyndhamAmenitiesFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  const $ = loadCheerio(html);

  $("script[type='application/ld+json']").each((_, el) => {
    try {
      const json = JSON.parse($(el).html() || "");
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        for (const feat of obj.amenityFeature || []) {
          const name = cleanText(feat?.name);
          const val = String(feat?.value ?? "true").toLowerCase();
          if (name && val !== "false") labels.push(name);
        }
        for (const place of obj.containsPlace || []) {
          const type = String(place?.["@type"] || "");
          const name = cleanText(place?.name);
          if (type === "ExerciseGym") labels.push("Fitness center");
          else if (/pool/i.test(type) || /pool/i.test(name)) labels.push("Pool");
          else if (type === "Restaurant") labels.push("Restaurant");
          else if (name) labels.push(name);
        }
      }
    } catch {
      /* skip */
    }
  });

  $("[itemprop='name']").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && !/^featured amenities$/i.test(t)) labels.push(t);
  });

  $(".hotel-policies-amenities__list li, .amenities-list li").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && t.length < 80) labels.push(t);
  });

  $("[class*='amenity' i] h3, [class*='amenity' i] h4, [class*='amenity' i] span").each((_, el) => {
    const t = cleanText($(el).text());
    if (t && t.length < 80 && !/^featured amenities$|^all amenities$/i.test(t)) labels.push(t);
  });

  const amenities = dedupeLabels(labels);
  return {
    amenities,
    amenitiesText: amenities.join("; "),
    parseErrors: amenities.length ? [] : ["no_amenities_parsed"],
  };
}

/**
 * @param {string} propertyUrl
 * @param {object} [opts]
 */
export async function fetchWyndhamHotelAmenities(propertyUrl, opts = {}) {
  const fetchFn = opts.fetchFn || globalThis.fetch;
  const overviewUrl = String(propertyUrl || "").trim().replace(/\/services-amenities\/?$/i, "/overview");
  const amenitiesUrl = wyndhamServicesAmenitiesUrl(overviewUrl) || overviewUrl;
  if (!overviewUrl) {
    return { ok: false, status: 0, amenities: [], amenitiesText: "", source: null, parseErrors: ["empty_url"] };
  }

  /** Prefer /services-amenities; fall back to overview when body empty (common soft-block). */
  const tryUrls = [amenitiesUrl, overviewUrl].filter(
    (u, i, arr) => u && arr.indexOf(u) === i
  );

  /** @type {string[]} */
  const parseErrors = [];
  for (const fetchUrl of tryUrls) {
    const res = await fetchFn(fetchUrl, { headers: WYNDHAM_FETCH_HEADERS, redirect: "follow" });
    const html = await res.text();
    if (!res.ok) {
      parseErrors.push(`http_${res.status}:${fetchUrl.includes("services-amenities") ? "amenities" : "overview"}`);
      continue;
    }
    if (!html || html.length < 200) {
      parseErrors.push(`empty_body:${fetchUrl.includes("services-amenities") ? "amenities" : "overview"}`);
      continue;
    }
    const parsed = parseWyndhamAmenitiesFromHtml(html);
    if (parsed.amenities.length) {
      return {
        ok: true,
        status: res.status,
        ...parsed,
        source: fetchUrl.includes("services-amenities")
          ? WYNDHAM_CONTENT_SOURCE
          : "wyndham_overview_html",
        fetchUrl,
      };
    }
    parseErrors.push(
      ...(parsed.parseErrors || ["no_amenities_parsed"]),
      fetchUrl.includes("services-amenities") ? "amenities_page" : "overview_page"
    );
  }

  return {
    ok: false,
    status: 200,
    amenities: [],
    amenitiesText: "",
    source: WYNDHAM_CONTENT_SOURCE,
    parseErrors: [...new Set(parseErrors)],
    softBlocked: true,
  };
}
