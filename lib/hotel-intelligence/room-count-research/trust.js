/**
 * Source trust hierarchy for room-count research.
 * Higher base_score = more authority for TOTAL PROPERTY ROOM COUNT / KEYS.
 */

export const ROOM_COUNT_TRUST_VERSION = "room-count-trust-v1";

/** Normalized source categories (user-facing). */
export const SOURCE_CATEGORIES = Object.freeze({
  OFFICIAL_HOTEL: "Official Hotel",
  OFFICIAL_BRAND: "Official Brand",
  OFFICIAL_OWNER: "Official Owner",
  OFFICIAL_OPERATOR: "Official Operator",
  GOVERNMENT_TOURISM: "Tourism Authority",
  CONVENTION_BUREAU: "Convention Bureau",
  DESTINATION_MARKETING: "Destination Marketing",
  TRUSTED_DIRECTORY: "Trusted Directory",
  PRESS_RELEASE: "Historic Press Release",
  NEWS: "News",
  OTHER: "Other",
});

/**
 * Base confidence by source category (before agreement / wording / identity modifiers).
 */
export const SOURCE_CATEGORY_BASE = Object.freeze({
  [SOURCE_CATEGORIES.OFFICIAL_HOTEL]: 0.92,
  [SOURCE_CATEGORIES.OFFICIAL_BRAND]: 0.94,
  [SOURCE_CATEGORIES.OFFICIAL_OWNER]: 0.9,
  [SOURCE_CATEGORIES.OFFICIAL_OPERATOR]: 0.88,
  [SOURCE_CATEGORIES.GOVERNMENT_TOURISM]: 0.86,
  [SOURCE_CATEGORIES.CONVENTION_BUREAU]: 0.84,
  [SOURCE_CATEGORIES.DESTINATION_MARKETING]: 0.82,
  [SOURCE_CATEGORIES.TRUSTED_DIRECTORY]: 0.72,
  [SOURCE_CATEGORIES.PRESS_RELEASE]: 0.78,
  [SOURCE_CATEGORIES.NEWS]: 0.62,
  [SOURCE_CATEGORIES.OTHER]: 0.45,
});

const BRAND_HOST_RE =
  /\b(marriott\.com|hilton\.com|ihg\.com|hyatt\.com|accor\.com|choicehotels\.com|wyndhamhotels\.com|radissonhotels\.com|melia\.com|barcelo\.com|riu\.com|karismahotels\.com|fourseasons\.com|rosewoodhotels\.com|mandarinoriental\.com|ritzcarlton\.com|stregis\.com|westin\.com|sheraton\.com|jwmarriott\.com|courtyard\.com|aloft\.com|element\.com|kimptonhotels\.com|intercontinental\.com|crowneplaza\.com|holidayinn\.com|indigohotels\.com|viceroyhotelsandresorts\.com|aman\.com|sixsenses\.com|oneandonlyresorts\.com)\b/i;

const TOURISM_HOST_RE =
  /\b(visitmexico|sectur\.gob|gob\.mx|procolombia|mincit\.gov|ict\.go\.cr|gob\.do|mitur\.gob|atp\.gob\.pa|visitpanama|visitcostarica|colombia\.travel|godominicanrepublic|visitjamaica|puertoricotourism|barbados\.org)\b/i;

const CVB_HOST_RE =
  /\b(cvb|convention|meetings|destination|tourismboard|visit[a-z]+)\b/i;

const PRESS_HOST_RE =
  /\b(prnewswire|businesswire|globenewswire|newswire|pressrelease|newsroom)\b/i;

const NEWS_HOST_RE =
  /\b(reuters|bloomberg|hotelnewsnow|skift|travelweekly|hospitalitynet|costar|forbes|nytimes|bbc\.|cnn\.|eluniversal|reforma|elfinanciero|larepublica|eltiempo|listindiario)\b/i;

const DIRECTORY_HOST_RE =
  /\b(tripadvisor|booking\.com|expedia|hotels\.com|agoda|trivago|kayak|google\.com|maps\.google)\b/i;

/**
 * Classify a URL into a source category.
 * @param {string} url
 * @param {{ hotelWebsite?: string|null, brand?: string|null }} [ctx]
 */
export function classifySourceUrl(url, ctx = {}) {
  const u = String(url || "").toLowerCase();
  if (!u) return SOURCE_CATEGORIES.OTHER;

  if (DIRECTORY_HOST_RE.test(u)) return SOURCE_CATEGORIES.OTHER; // OTA ≠ census room source

  const hotelHost = safeHost(ctx.hotelWebsite);
  const pageHost = safeHost(u);
  if (hotelHost && pageHost && (pageHost === hotelHost || pageHost.endsWith(`.${hotelHost}`) || hotelHost.endsWith(`.${pageHost}`))) {
    return SOURCE_CATEGORIES.OFFICIAL_HOTEL;
  }

  if (BRAND_HOST_RE.test(u)) return SOURCE_CATEGORIES.OFFICIAL_BRAND;
  if (PRESS_HOST_RE.test(u)) return SOURCE_CATEGORIES.PRESS_RELEASE;
  if (TOURISM_HOST_RE.test(u)) return SOURCE_CATEGORIES.GOVERNMENT_TOURISM;
  if (CVB_HOST_RE.test(u) && !NEWS_HOST_RE.test(u)) {
    return SOURCE_CATEGORIES.CONVENTION_BUREAU;
  }
  if (NEWS_HOST_RE.test(u)) return SOURCE_CATEGORIES.NEWS;

  // Property-looking independent sites
  if (/hotel|resort|inn|lodge|spa/i.test(pageHost || "")) {
    return SOURCE_CATEGORIES.OFFICIAL_HOTEL;
  }

  return SOURCE_CATEGORIES.OTHER;
}

function safeHost(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return null;
  }
}

/**
 * Whether a URL is eligible for a follow-up fetch (not OTA / social / PDF crawl).
 * @param {string} url
 */
export function isFetchEligibleUrl(url) {
  const u = String(url || "").toLowerCase();
  if (!/^https?:\/\//i.test(u)) return false;
  if (DIRECTORY_HOST_RE.test(u)) return false;
  if (/\b(facebook|instagram|twitter|x\.com|tiktok|youtube|linkedin)\b/i.test(u)) {
    return false;
  }
  if (/\.(pdf|docx?|xlsx?)(\?|$)/i.test(u)) return false;
  return true;
}

/**
 * Base score for a classified source.
 * @param {string} category
 */
export function baseConfidenceForCategory(category) {
  return SOURCE_CATEGORY_BASE[category] ?? SOURCE_CATEGORY_BASE[SOURCE_CATEGORIES.OTHER];
}
