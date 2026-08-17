/**
 * Fetch hotel description + amenities from marriott.com HWS subpages when /overview/ is blocked.
 * Subpages (/experiences/, /dining/, /events/, /rooms/) load server-side and embed FAQ + JSON-LD.
 */

import { formatMarriottAmenitiesText } from "./marriott-amenity-format.js";
import { load as loadCheerio } from "cheerio";
import { MARRIOTT_FETCH_HEADERS, MARRIOTT_ORIGIN } from "./marriott-brand-directory-extract.js";
import { fetchMarriottBazaarvoiceProduct } from "./marriott-bazaarvoice-content-fetch.js";

export const MARRIOTT_CONTENT_SOURCE_SUBPAGES = "marriott_subpages";

const SUBPAGES = ["experiences", "dining", "events", "rooms"];

/** @type {{ re: RegExp, chip: string }[]} */
const FAQ_CHIP_RULES = [
  { re: /fitness center on-site|has a fitness center/i, chip: "Fitness center" },
  { re: /outdoor pool on-site|has one outdoor pool|swimming pool/i, chip: "Pool" },
  { re: /room service is available|offer room service/i, chip: "Room service" },
  { re: /has \d+ event rooms|meeting or event/i, chip: "Meeting event space" },
  { re: /wireless internet, complimentary|high-speed internet|high-speed wi-fi|free wi-fi/i, chip: "Free high-speed internet" },
  { re: /coffee\/tea maker/i, chip: "Coffee/tea in-room" },
  { re: /wedding services/i, chip: "Wedding services" },
  { re: /breakfast is available/i, chip: "Breakfast available (fee)" },
  { re: /offers accessible rooms|mobility accessible rooms/i, chip: "Mobility accessible rooms" },
  { re: /non-smoking/i, chip: "All public areas non-smoking" },
];

function cleanText(value) {
  return String(value || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeLabels(labels) {
  const seen = new Set();
  /** @type {string[]} */
  const out = [];
  for (const raw of labels) {
    const label = cleanText(raw);
    if (!label) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out;
}

/**
 * @param {string} slug e.g. pujac-ac-hotel-punta-cana
 */
export function marriottHotelSlugFromOverviewUrl(websiteOrSlug) {
  const raw = String(websiteOrSlug || "").trim();
  const m = raw.match(/\/hotels\/([a-z0-9]+(?:-[a-z0-9-]+)*)\//i);
  if (m) return m[1].toLowerCase();
  if (/^[a-z0-9-]+$/i.test(raw)) return raw.toLowerCase();
  return "";
}

/**
 * @param {object} hotelSchema
 */
function chipsFromHotelSchema(hotelSchema) {
  /** @type {string[]} */
  const chips = [];
  for (const place of hotelSchema?.containsPlace || []) {
    const type = String(place?.["@type"] || "");
    const name = cleanText(place?.name);
    if (type === "ExerciseGym") chips.push("Fitness center");
    if (type === "SportsActivityLocation" && /pool/i.test(name)) chips.push("Pool");
    if (type === "Restaurant") chips.push("Restaurant");
    if (type === "BarOrPub") chips.push("Bar");
    if (type === "MeetingRoom") chips.push("Meeting event space");
  }

  for (const place of hotelSchema?.containsPlace || []) {
    if (place?.["@type"] !== "HotelRoom") continue;
    for (const feat of place?.amenityFeature || []) {
      const name = cleanText(feat?.name);
      if (/wireless internet, complimentary/i.test(name)) chips.push("Free high-speed internet");
      if (/coffee\/tea maker/i.test(name)) chips.push("Coffee/tea in-room");
      if (/offers accessible rooms with roll-in showers|offers mobility accessible rooms/i.test(name)) {
        chips.push("Mobility accessible rooms");
      }
    }
  }
  return chips;
}

/**
 * @param {object} faqItem
 */
function chipsFromFaqItem(faqItem) {
  const text = `${cleanText(faqItem?.name)} ${cleanText(faqItem?.acceptedAnswer?.text)}`;
  /** @type {string[]} */
  const chips = [];
  for (const rule of FAQ_CHIP_RULES) {
    if (rule.re.test(text)) chips.push(rule.chip);
  }
  return chips;
}

/**
 * @param {string} html
 */
export function parseMarriottSubpageHtml(html) {
  const $ = loadCheerio(html);
  /** @type {string[]} */
  const amenities = [];
  let description = "";

  $("script[type='application/ld+json']").each((_, el) => {
    try {
      const data = JSON.parse($(el).html() || "{}");
      if (data["@type"] === "FAQPage") {
        for (const item of data.mainEntity || []) {
          amenities.push(...chipsFromFaqItem(item));
        }
      }
      if (data["@type"] === "Hotel") {
        amenities.push(...chipsFromHotelSchema(data));
        if (!description && typeof data.description === "string" && data.description.length >= 40) {
          description = cleanText(data.description);
        }
      }
    } catch {
      /* ignore malformed JSON-LD */
    }
  });

  const bodyText = cleanText($("body").text() || html);
  if (/high-speed internet|complimentary wi-fi|free wi-fi|wireless internet, complimentary/i.test(bodyText)) {
    amenities.push("Free high-speed internet");
  }
  if (/coffee\/tea maker/i.test(bodyText)) amenities.push("Coffee/tea in-room");

  const deduped = dedupeLabels(amenities);
  return {
    description,
    amenities: deduped,
    amenitiesText: formatMarriottAmenitiesText(deduped),
  };
}

/**
 * @param {string} slug
 */
export async function fetchMarriottSubpageHtml(slug, subpage) {
  const url = `${MARRIOTT_ORIGIN}/en-us/hotels/${slug}/${subpage}/`;
  const res = await fetch(url, {
    headers: {
      ...MARRIOTT_FETCH_HEADERS,
      Accept: "text/html,application/xhtml+xml",
    },
    redirect: "follow",
  });
  const html = await res.text();
  return { url, status: res.status, html, accessDenied: res.status === 403 || /access denied/i.test(html) };
}

/**
 * @param {string} slugOrWebsite
 * @param {object} [opts]
 * @param {string} [opts.marshaCode]
 */
export async function fetchMarriottSubpageContent(slugOrWebsite, opts = {}) {
  const slug = marriottHotelSlugFromOverviewUrl(slugOrWebsite);
  if (!slug) {
    return {
      slug: "",
      description: "",
      amenities: [],
      amenitiesText: "",
      source: MARRIOTT_CONTENT_SOURCE_SUBPAGES,
      parseErrors: ["missing_slug"],
      subpages: [],
    };
  }

  /** @type {string[]} */
  const amenities = [];
  let description = "";
  /** @type {object[]} */
  const subpages = [];
  /** @type {string[]} */
  const parseErrors = [];

  for (const subpage of SUBPAGES) {
    try {
      const fetched = await fetchMarriottSubpageHtml(slug, subpage);
      subpages.push({ subpage, status: fetched.status, accessDenied: fetched.accessDenied });
      if (fetched.accessDenied || fetched.status !== 200) {
        parseErrors.push(`${subpage}:http_${fetched.status}`);
        continue;
      }
      const parsed = parseMarriottSubpageHtml(fetched.html);
      amenities.push(...parsed.amenities);
      if (!description && parsed.description) description = parsed.description;
    } catch (err) {
      parseErrors.push(`${subpage}:${err?.message || err}`);
    }
  }

  const marsha = String(opts.marshaCode || "").trim().toUpperCase();
  if (!description && marsha) {
    try {
      const bv = await fetchMarriottBazaarvoiceProduct(marsha);
      if (bv?.description) description = bv.description;
    } catch (err) {
      parseErrors.push(`bazaarvoice:${err?.message || err}`);
    }
  }

  const deduped = dedupeLabels(amenities);
  return {
    slug,
    marshaCode: marsha,
    description,
    amenities: deduped,
    amenitiesText: formatMarriottAmenitiesText(deduped),
    source: MARRIOTT_CONTENT_SOURCE_SUBPAGES,
    parseErrors,
    subpages,
  };
}
