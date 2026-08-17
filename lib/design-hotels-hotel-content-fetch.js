/**
 * Fetch and parse Design Hotels property pages (designhotels.com/hotels/...).
 * Overview HTML includes fact sheet, amenities, room count, and Marriott HWS link.
 */

import { load as loadCheerio } from "cheerio";
import { formatMarriottAmenitiesText } from "./marriott-amenity-format.js";
import { marshaFromMarriottWebsite } from "./marriott-brand-directory-extract.js";

export const DESIGN_HOTELS_CONTENT_SOURCE = "designhotels_overview_html";

const FACT_SHEET_CATEGORIES = new Set([
  "Hotel Features",
  "General",
  "Room",
  "Wellness",
  "Features",
  "Meeting Rooms",
  "Dining",
  "Bathroom Features",
  "Accessibility Features",
]);

function cleanText(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function liText($, el) {
  return cleanText(
    $(el)
      .text()
      .replace(/\s*Service comes at an extra charge\s*/gi, "")
      .replace(/Selected rooms only/gi, "")
  );
}

/**
 * @param {string} url
 */
export function designHotelsOverviewUrl(url) {
  const raw = String(url || "").trim();
  if (!raw.includes("designhotels.com/hotels/")) return "";
  try {
    const u = new URL(raw.endsWith("/") ? raw : `${raw}/`);
    const parts = u.pathname.split("/").filter(Boolean);
    if (parts[0] !== "hotels" || parts.length < 4) return "";
    const last = parts[parts.length - 1].toLowerCase();
    const subpages = new Set([
      "rooms-rates",
      "location-details",
      "gallery",
      "dining",
      "wellness",
      "experiences",
      "offers",
    ]);
    const baseParts = subpages.has(last) ? parts.slice(0, -1) : parts;
    return `https://www.designhotels.com/${baseParts.join("/")}/`;
  } catch {
    return "";
  }
}

/**
 * @param {string} html
 */
export function parseDesignHotelsOverviewHtml(html) {
  const text = String(html || "");
  /** @type {string[]} */
  const parseErrors = [];
  if (!text || text.length < 1000) {
    return emptyParse(["empty_html"]);
  }

  const $ = loadCheerio(text);

  /** @type {string[]} */
  const descriptionParts = [];
  const lead = cleanText($("#overview .pdp-usp .first h2").first().text());
  if (lead.length >= 40) descriptionParts.push(lead);
  $("#overview .pdp-usp .last p").each((_, el) => {
    const p = cleanText($(el).text());
    if (p.length >= 30) descriptionParts.push(p);
  });
  if (!descriptionParts.length) {
    $("#overview h2").each((_, el) => {
      const p = cleanText($(el).text());
      if (p.length >= 40 && !/^gallery$/i.test(p)) descriptionParts.push(p);
    });
  }
  const description = descriptionParts.join(" ").slice(0, 2000);

  /** @type {string[]} */
  let amenities = [];
  $("p.category.strong").each((_, el) => {
    const category = cleanText($(el).text());
    if (!FACT_SHEET_CATEGORIES.has(category)) return;
    $(el)
      .nextAll("ul")
      .first()
      .find("li")
      .each((_, li) => {
        const label = liText($, li);
        if (label) amenities.push(label);
      });
  });

  let rooms = parseRoomsFromHtml($, text);
  let finalDescription = description;

  if (!finalDescription || !amenities.length || rooms == null) {
    const legacy = parseLegacyDesignHotelsContent($);
    if (!finalDescription && legacy.description) finalDescription = legacy.description;
    if (!amenities.length && legacy.amenities.length) amenities = legacy.amenities;
    if (rooms == null && legacy.rooms != null) rooms = legacy.rooms;
  }

  if (!finalDescription) {
    const meta =
      $("meta[name='description']").attr("content") || $("meta[property='og:description']").attr("content");
    const metaText = cleanText(meta);
    if (metaText.length >= 40) finalDescription = metaText;
  }

  const marriottUrlMatch = text.match(
    /https:\/\/www\.marriott\.com\/en-us\/hotels\/[a-z0-9-]+(?:\/overview\/)?/i
  );
  const marriottUrl = marriottUrlMatch
    ? marriottUrlMatch[0].replace(/\/overview\/?$/i, "/overview/")
    : "";
  const marshaCode = marshaFromMarriottWebsite(marriottUrl);

  const amenitiesText = formatMarriottAmenitiesText(amenities);
  if (!finalDescription) parseErrors.push("missing_description");
  if (!amenities.length) parseErrors.push("missing_amenities");

  return {
    description: finalDescription,
    amenities,
    amenitiesText,
    rooms,
    marriottUrl,
    marshaCode,
    parseErrors,
  };
}

function parseRoomsFromHtml($, text) {
  const basicText = cleanText(
    $("p.category.strong")
      .filter((_, el) => /^Basic Information$/i.test($(el).text()))
      .next("ul")
      .text()
  );
  const roomsMatch =
    basicText.match(/Number of rooms:\s*(\d+)/i) || text.match(/Number of rooms:\s*(\d+)/i);
  if (roomsMatch) {
    const n = Number(roomsMatch[1]);
    if (Number.isFinite(n) && n > 0) return n;
  }

  let legacyRooms = null;
  $("p.category.strong").each((_, el) => {
    if (!/^Rooms$/i.test(cleanText($(el).text()))) return;
    const val = cleanText($(el).next("h3").text());
    const n = Number(val.replace(/[^\d]/g, ""));
    if (Number.isFinite(n) && n > 0) legacyRooms = n;
  });
  return legacyRooms;
}

const LEGACY_AMENITY_CATEGORIES = new Set([
  "Food & Drink",
  "Spa & Recreation",
  "Conference & Meetings",
  "Architecture",
  "Interior design",
  "Themes",
  "Hotel Features",
  "Wellness",
  "Dining",
]);

const LEGACY_SKIP_CATEGORIES = new Set(["Location", "Rooms", "Available with", "The Originals"]);

/**
 * Legacy Umbraco template (#boxes layout, pre-fact-sheet masonry).
 * @param {import('cheerio').CheerioAPI} $
 */
function parseLegacyDesignHotelsContent($) {
  /** @type {string[]} */
  const descriptionParts = [];
  $("#boxes .box19 h2").each((_, el) => {
    const t = cleanText($(el).text());
    if (t.length >= 40) descriptionParts.push(t);
  });
  $("#boxes .box").each((_, box) => {
    $(box)
      .find("h2, p")
      .each((_, el) => {
        if ($(el).closest(".media-holder, .hero-footer, .base-caption").length) return;
        const tag = el.tagName?.toLowerCase();
        const t = cleanText($(el).text());
        if (t.length < 40) return;
        if (/^(rooms & suites|gallery|related articles)$/i.test(t)) return;
        if (tag === "h2" || tag === "p") descriptionParts.push(t);
      });
  });

  /** @type {string[]} */
  const amenities = [];
  $("li").each((_, li) => {
    const category = cleanText($(li).find("p.category.strong").first().text());
    if (!category || LEGACY_SKIP_CATEGORIES.has(category)) return;
    if (!LEGACY_AMENITY_CATEGORIES.has(category) && !FACT_SHEET_CATEGORIES.has(category)) return;
    const detail = cleanText($(li).find("h3").first().text());
    if (!detail || /marriott bonvoy/i.test(detail)) return;
    amenities.push(detail.includes(":") ? detail : `${category}: ${detail}`);
  });

  let rooms = null;
  $("p.category.strong").each((_, el) => {
    if (!/^Rooms$/i.test(cleanText($(el).text()))) return;
    const n = Number(cleanText($(el).next("h3").text()).replace(/[^\d]/g, ""));
    if (Number.isFinite(n) && n > 0) rooms = n;
  });

  const description = [...new Set(descriptionParts)]
    .filter((p) => !/design hotels is partnered with marriott/i.test(p))
    .filter((p) => !/available with marriott bonvoy/i.test(p))
    .join(" ")
    .slice(0, 2000);
  return { description, amenities, rooms };
}

function emptyParse(errors) {
  return {
    description: "",
    amenities: [],
    amenitiesText: "",
    rooms: null,
    marriottUrl: "",
    marshaCode: "",
    parseErrors: errors,
  };
}

/**
 * @param {string} propertyUrl
 * @param {object} [opts]
 */
export async function fetchDesignHotelsHotelContent(propertyUrl, opts = {}) {
  const overviewUrl = designHotelsOverviewUrl(propertyUrl);
  if (!overviewUrl) {
    return {
      overviewUrl: "",
      source: "",
      fetchStatus: 0,
      accessDenied: true,
      description: "",
      amenities: [],
      amenitiesText: "",
      rooms: null,
      marriottUrl: "",
      marshaCode: "",
      errors: ["invalid_design_hotels_url"],
    };
  }

  if (opts.html) {
    const parsed = parseDesignHotelsOverviewHtml(String(opts.html));
    return {
      overviewUrl,
      source: DESIGN_HOTELS_CONTENT_SOURCE,
      fetchStatus: 200,
      accessDenied: false,
      ...parsed,
      errors: parsed.parseErrors,
    };
  }

  const res = await (opts.fetchFn || globalThis.fetch)(overviewUrl, {
    headers: {
      "User-Agent": opts.userAgent || "Mozilla/5.0 (compatible; DealalityAudit/1.0)",
      Accept: "text/html",
    },
  });
  const html = await res.text();
  const accessDenied = res.status >= 400 || /access denied/i.test(html);
  const parsed = parseDesignHotelsOverviewHtml(html);
  return {
    overviewUrl,
    source: DESIGN_HOTELS_CONTENT_SOURCE,
    fetchStatus: res.status,
    accessDenied,
    ...parsed,
    errors: [...(accessDenied ? ["access_denied"] : []), ...parsed.parseErrors],
  };
}
