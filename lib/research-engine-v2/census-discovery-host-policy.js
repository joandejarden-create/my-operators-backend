/**
 * Provider-independent discovery host policy for Hotel Property Census.
 * Used by HBX, shell insert, coordinate completion, and optional enrichment lanes.
 * Not tied to any single SERP/Maps vendor.
 */

export const CENSUS_DISCOVERY_HOST_POLICY_VERSION =
  "census-discovery-host-policy-v1";

/** Domains never accepted as official hotel / rooms evidence SoT. */
export const REJECTED_SOURCE_HOSTS = Object.freeze([
  "booking.com",
  "expedia.com",
  "hotels.com",
  "hoteles.com",
  "tripadvisor.com",
  "tripadvisor.co",
  "agoda.com",
  "kayak.com",
  "trivago.com",
  "hotelscombined.com",
  "orbitz.com",
  "travelocity.com",
  "priceline.com",
  "makemytrip.com",
  "tripadvisor.in",
  "facebook.com",
  "instagram.com",
  "twitter.com",
  "x.com",
  "youtube.com",
  "wikipedia.org",
  "reddit.com",
  "yelp.com",
  "airbnb.com",
  "vrbo.com",
  "rooms.aero",
  "tripadvisor.es",
  "tripadvisor.com.mx",
  "tripadvisor.com.br",
  "tripadvisor.com.co",
  "despegar.com",
  "despegar.com.mx",
  "despegar.com.co",
  "google.com",
  "google.com.mx",
  "google.com.br",
  "bing.com",
]);

/** Venue / directory aggregators not approved for validated rooms/URL writes. */
export const REJECTED_DIRECTORY_HOSTS = Object.freeze([
  "cvent.com",
  "venues.cvent.com",
  "meetingbrokers.com",
  "eventective.com",
]);

/** Affiliate / mirror host patterns (not official brand property sites). */
const REJECTED_HOST_SUFFIX_RE =
  /(^|\.)([a-z0-9-]+-)?hotels?\.(com|net|org)$|(^|\.)hotels?-[a-z0-9.-]+\.(com|net|org)$/i;

export const BRAND_OFFICIAL_HOST_HINTS = Object.freeze([
  "marriott.com",
  "hilton.com",
  "ihg.com",
  "hyatt.com",
  "accor.com",
  "all.accor.com",
  "choicehotels.com",
  "wyndhamhotels.com",
  "radissonhotels.com",
  "bestwestern.com",
  "melia.com",
  "riu.com",
  "barcelo.com",
  "minorhotels.com",
  "fourseasons.com",
  "rosewoodhotels.com",
  "preferredhotels.com",
  "designhotels.com",
  "kimptonhotels.com",
  "sixsenses.com",
  "sofitel.com",
  "fairmont.com",
  "novotel.com",
  "ibis.com",
  "pullmanhotels.com",
  "mgallery.com",
  "swissotel.com",
  "movenpick.com",
]);

/**
 * @param {string} host
 */
export function isBrandOfficialHost(host) {
  if (!host) return false;
  return BRAND_OFFICIAL_HOST_HINTS.some(
    (d) => host === d || host.endsWith(`.${d}`)
  );
}

/**
 * True when a hostname must not be treated as discovery / official evidence.
 * @param {string} host
 */
export function isRejectedDiscoveryHost(host) {
  if (!host) return true;
  if (REJECTED_SOURCE_HOSTS.some((d) => host === d || host.endsWith(`.${d}`))) {
    return true;
  }
  if (REJECTED_DIRECTORY_HOSTS.some((d) => host === d || host.endsWith(`.${d}`))) {
    return true;
  }
  if (REJECTED_HOST_SUFFIX_RE.test(host) && !isBrandOfficialHost(host)) {
    return true;
  }
  if (/rooms\.aero|hoteles\.com|hotels\.com/i.test(host)) return true;
  return false;
}
