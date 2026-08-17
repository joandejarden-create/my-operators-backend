/**
 * DataForSEO validated-write policy v1 — gates + validators.
 * DataForSEO is never source of truth. Writes require underlying URL fetch + match.
 */

import {
  isBrandOfficialHost,
  isTrustedSecondaryHost,
  REJECTED_SOURCE_HOSTS,
  SOURCE_TIER,
  nameMatchScore,
} from "./dataforseo-candidate-classifier.js";
import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
  isFalsePositiveRoomCount,
} from "./production-census-rooms-keys-extractor.js";
import {
  ROOMS_EVIDENCE_TIER,
  resolveRoomsSourceTypeForAirtable,
  buildRoomsProvenanceNotes,
  MAP_ROOMS_SOURCE_TYPE,
} from "./census-secondary-hotel-data-policy.js";
import { mapEvidenceTierCodeToSelect } from "./production-census-rooms-evidence-tier-schema.js";

export const DATAFORSEO_VALIDATED_WRITE_POLICY_VERSION =
  "dataforseo-validated-write-policy-v1";

const REJECTED_HOST_SUFFIX_RE =
  /(^|\.)([a-z0-9-]+-)?hotels?\.(com|net|org)$|(^|\.)hotels?-[a-z0-9.-]+\.(com|net|org)$/i;

/** Venue / directory aggregators not approved for validated rooms/URL writes. */
const REJECTED_DIRECTORY_HOSTS = Object.freeze([
  "cvent.com",
  "venues.cvent.com",
  "meetingbrokers.com",
  "eventective.com",
]);

const AFFILIATE_WORDING_RE =
  /\b(book now|best rate guarantee|compare prices|affiliate|sponsored listing|partner hotel|ota)\b/i;

const MULTI_HOTEL_DIRECTORY_RE =
  /\b(hotels? in .{0,40}(city|área|area)|browse hotels|find hotels|hotel deals|all hotels)\b/i;

/**
 * @param {NodeJS.ProcessEnv} [env]
 */
export function resolveDataForSeoValidatedWriteGates(env = process.env) {
  const enabled = String(env.DATAFORSEO_ENABLED || "0").trim() === "1";
  const candidatesOnly =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0").trim() === "1";
  const validated =
    String(env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0").trim() === "1";
  const urlWrites = String(env.ENABLE_DATAFORSEO_URL_WRITES || "0").trim() === "1";
  const roomsWrites =
    String(env.ENABLE_DATAFORSEO_ROOMS_WRITES || "0").trim() === "1";
  const addressWrites =
    String(env.ENABLE_DATAFORSEO_ADDRESS_WRITES || "0").trim() === "1";
  const phoneWrites =
    String(env.ENABLE_DATAFORSEO_PHONE_WRITES || "0").trim() === "1";
  const coordWrites =
    String(env.ENABLE_DATAFORSEO_COORDINATE_WRITES || "0").trim() === "1";

  const blockers = [];
  if (!enabled) blockers.push("DATAFORSEO_ENABLED_not_1");
  if (candidatesOnly) {
    blockers.push("DATAFORSEO_WRITE_CANDIDATES_ONLY_must_be_0_for_validated_writes");
  }
  if (!validated) blockers.push("ENABLE_DATAFORSEO_VALIDATED_WRITES_must_be_1");
  if (!urlWrites && !roomsWrites) {
    blockers.push("no_url_or_rooms_write_flags_enabled");
  }
  if (addressWrites) blockers.push("ENABLE_DATAFORSEO_ADDRESS_WRITES_must_be_0");
  if (phoneWrites) blockers.push("ENABLE_DATAFORSEO_PHONE_WRITES_must_be_0");
  if (coordWrites) blockers.push("ENABLE_DATAFORSEO_COORDINATE_WRITES_must_be_0");

  return {
    ok: blockers.length === 0,
    blockers,
    dataforseo_is_source_of_truth: false,
    serp_snippet_writes_allowed: false,
    url_writes: urlWrites,
    rooms_writes: roomsWrites,
    address_writes: false,
    phone_writes: false,
    coordinate_writes: false,
    maps_writes: false,
    travel_weekly_direct_writes: false,
    candidates_only: candidatesOnly,
    validated_writes: validated,
  };
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

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

/**
 * Brand URL must be property-specific (not city brand listing).
 * @param {string} url
 */
export function isPropertySpecificBrandUrl(url) {
  const u = String(url || "");
  const host = hostFromUrl(u);
  if (!host) return false;
  if (/choicehotels\.com/i.test(host)) {
    // Require property code e.g. mx063 / mx224
    return /\/[a-z]{2}\d{2,4}(?:\?|#|$|\/)/i.test(u);
  }
  if (/marriott\.com/i.test(host)) {
    return /\/hotels\/[a-z0-9]+-/i.test(u);
  }
  if (/ihg\.com/i.test(host)) {
    return /hoteldetail|\/hotels\/[^?\s]+\/[a-z0-9]{4,}\//i.test(u);
  }
  if (/hilton\.com/i.test(host)) {
    return /\/hotels\/[^?\s]+\/[^?\s]+/i.test(u);
  }
  if (/accor\.com|all\.accor\.com/i.test(host)) {
    return /\/hotel\/|\/[a-z]{1,2}\/[a-z0-9-]+\/index/i.test(u);
  }
  if (/wyndhamhotels\.com/i.test(host)) {
    return /\/hotels\/|hotel-id|property/i.test(u);
  }
  if (/preferredhotels\.com|hyatt\.com|radissonhotels\.com/i.test(host)) {
    return /\/hotels?\/|\/hotel\//i.test(u);
  }
  // Other brand hosts: require hotel path segment
  if (isBrandOfficialHost(host)) {
    return /hotel|property|resort|inn|suites/i.test(u);
  }
  return true;
}

/**
 * Strip tags → text for match checks.
 * @param {string} html
 */
export function htmlToPlainText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function extractPageTitle(html) {
  const m = String(html || "").match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m ? m[1].replace(/\s+/g, " ").trim() : "";
}

/**
 * Stricter hotel_official validation after fetch.
 * @param {{
 *   url: string,
 *   html: string,
 *   hotelName?: string,
 *   city?: string,
 *   country?: string,
 *   source_tier?: string,
 * }} input
 */
export function validateHotelOfficialStrict(input = {}) {
  const url = String(input.url || "").trim();
  const host = hostFromUrl(url);
  const html = String(input.html || "");
  const title = extractPageTitle(html);
  const text = htmlToPlainText(html).slice(0, 50000);
  const hay = `${title} ${text}`.toLowerCase();

  if (!url || !html) {
    return { ok: false, reason: "missing_url_or_html", write_eligible: false };
  }
  if (isRejectedDiscoveryHost(host)) {
    return { ok: false, reason: "rejected_host", write_eligible: false, host };
  }
  if (isTrustedSecondaryHost(host)) {
    return {
      ok: false,
      reason: "trusted_secondary_not_approved_for_direct_write",
      write_eligible: false,
      host,
    };
  }
  if (AFFILIATE_WORDING_RE.test(hay) && !isBrandOfficialHost(host)) {
    return { ok: false, reason: "affiliate_wording", write_eligible: false, host };
  }
  if (MULTI_HOTEL_DIRECTORY_RE.test(hay) && !isBrandOfficialHost(host)) {
    return {
      ok: false,
      reason: "multi_hotel_generic_page",
      write_eligible: false,
      host,
    };
  }
  // Booking-widget-only shells without property copy
  if (
    /booking\.com|expedia|tripadvisor/i.test(html) &&
    !isBrandOfficialHost(host) &&
    text.length < 400
  ) {
    return {
      ok: false,
      reason: "booking_affiliate_only_structure",
      write_eligible: false,
      host,
    };
  }

  const nameCheck = nameMatchScore(input.hotelName, `${title} ${text.slice(0, 2000)}`);
  if (input.hotelName && !nameCheck.ok) {
    return {
      ok: false,
      reason: "property_name_not_on_page",
      write_eligible: false,
      host,
      name_match: nameCheck,
    };
  }

  const city = String(input.city || "").toLowerCase();
  const country = String(input.country || "").toLowerCase();
  const cityHit = city.length > 2 && hay.includes(city);
  const countryHit = country.length > 3 && hay.includes(country);
  const brandOfficial = isBrandOfficialHost(host);
  if (!brandOfficial && !cityHit && !countryHit) {
    return {
      ok: false,
      reason: "city_or_country_not_on_page",
      write_eligible: false,
      host,
      name_match: nameCheck,
    };
  }

  if (brandOfficial && !isPropertySpecificBrandUrl(url)) {
    return {
      ok: false,
      reason: "brand_url_not_property_specific",
      write_eligible: false,
      host,
    };
  }

  const tier = brandOfficial
    ? SOURCE_TIER.brand_official
    : SOURCE_TIER.hotel_official;

  return {
    ok: true,
    reason: null,
    write_eligible: true,
    host,
    title,
    source_tier: tier,
    name_match: nameCheck,
    geo_match: { city: cityHit, country: countryHit },
  };
}

/**
 * Validate brand_official candidate (still requires fetch + property match).
 */
export function validateBrandOfficialUrl(input = {}) {
  const url = String(input.url || "").trim();
  const host = hostFromUrl(url);
  if (!isBrandOfficialHost(host)) {
    return { ok: false, reason: "not_brand_official_host", write_eligible: false };
  }
  if (isRejectedDiscoveryHost(host)) {
    return { ok: false, reason: "rejected_host", write_eligible: false };
  }
  if (!isPropertySpecificBrandUrl(url)) {
    return {
      ok: false,
      reason: "brand_url_not_property_specific",
      write_eligible: false,
      host,
    };
  }
  // Reuse strict page checks
  return validateHotelOfficialStrict({ ...input, source_tier: SOURCE_TIER.brand_official });
}

/**
 * When brand official HTML is bot-blocked (403), allow URL write only if
 * property-specific brand path + name tokens appear in URL and/or SERP title.
 * Never used for rooms extraction (needs body).
 */
export function validateBrandOfficialUrlBotBlocked(input = {}) {
  const url = String(input.url || "").trim();
  const host = hostFromUrl(url);
  const title = String(input.title || "");
  if (!isBrandOfficialHost(host)) {
    return { ok: false, reason: "not_brand_official_host", write_eligible: false };
  }
  if (!isPropertySpecificBrandUrl(url)) {
    return {
      ok: false,
      reason: "brand_url_not_property_specific",
      write_eligible: false,
      host,
    };
  }
  const nameCheck = nameMatchScore(
    input.hotelName,
    `${title} ${url.replace(/[-_/]/g, " ")}`
  );
  if (input.hotelName && !nameCheck.ok) {
    return {
      ok: false,
      reason: "property_name_not_in_url_or_title",
      write_eligible: false,
      host,
      name_match: nameCheck,
    };
  }
  const city = String(input.city || "").toLowerCase();
  const urlHay = url.toLowerCase().replace(/[-_/]/g, " ");
  const cityHit =
    !city ||
    city.length <= 2 ||
    urlHay.includes(city) ||
    title.toLowerCase().includes(city);
  if (!cityHit) {
    return {
      ok: false,
      reason: "city_not_in_url_or_title_bot_blocked",
      write_eligible: false,
      host,
    };
  }
  return {
    ok: true,
    reason: "bot_blocked_brand_url_path_validated",
    write_eligible: true,
    host,
    source_tier: SOURCE_TIER.brand_official,
    name_match: nameCheck,
    bot_blocked: true,
  };
}

/**
 * Fetch underlying candidate URL. Never write from SERP snippet alone.
 * @param {string} url
 * @param {{ fetchImpl?: typeof fetch, timeoutMs?: number }} [opts]
 */
export async function fetchUnderlyingCandidatePage(url, opts = {}) {
  const fetchImpl = opts.fetchImpl || fetch;
  const timeoutMs = opts.timeoutMs ?? 12000;
  const u = String(url || "").trim();
  if (!u || !/^https?:\/\//i.test(u)) {
    return { ok: false, reason: "invalid_url", html: "", status: 0 };
  }
  const host = hostFromUrl(u);
  if (isRejectedDiscoveryHost(host) || isTrustedSecondaryHost(host)) {
    return {
      ok: false,
      reason: isTrustedSecondaryHost(host)
        ? "trusted_secondary_fetch_skipped"
        : "rejected_host_fetch_skipped",
      html: "",
      status: 0,
      host,
    };
  }
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetchImpl(u, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        Accept: "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
        "User-Agent": "DealalityCensusBot/1.0 (+dataforseo-validated-write-policy-v1)",
      },
    });
    const html = await res.text();
    return {
      ok: res.ok,
      reason: res.ok ? null : `http_${res.status}`,
      html: html || "",
      status: res.status,
      final_url: String(res.url || u),
      host,
    };
  } catch (err) {
    return {
      ok: false,
      reason: `fetch_error:${err?.name || "err"}`,
      html: "",
      status: 0,
      host,
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Extract rooms from fetched official/tourism/factsheet HTML.
 * Never from SERP snippet alone.
 */
export function extractValidatedRoomsFromHtml(html, url, ctx = {}) {
  const host = hostFromUrl(url);
  if (isRejectedDiscoveryHost(host) || isTrustedSecondaryHost(host)) {
    return { ok: false, reason: "rooms_source_host_not_approved" };
  }
  const extracted = extractRoomsKeysFromOfficialHtml(html, {
    url,
    propertyName: ctx.hotelName,
  });
  const best = selectBestRoomsHit(extracted.hits || []);
  if (!best || best.rejected || best.count == null) {
    return { ok: false, reason: "no_rooms_on_page" };
  }
  if (isFalsePositiveRoomCount(html, best.count, best.method || "official")) {
    return { ok: false, reason: "rooms_false_positive_rejected" };
  }
  if (best.count === 25 && /choicehotels\.com/i.test(url)) {
    return { ok: false, reason: "choice_sitewide_rooms_default_25" };
  }
  if (best.confidence !== "High" && best.confidence !== "Medium") {
    return { ok: false, reason: "rooms_confidence_too_low" };
  }

  const brandOfficial = isBrandOfficialHost(host);
  const tourism =
    /gob\.|sectur|mincit|rnt\.|turismo|visit|ict\.go\.cr|embratur/i.test(host);
  const factsheet =
    /\.pdf(\?|$)/i.test(url) ||
    /fact-?sheet|ficha t[eé]cnica/i.test(html.slice(0, 5000));
  const press = /\/(press|newsroom|media|noticias|prensa)\b/i.test(url);

  let category = "official_hotel_website";
  let evidence_tier = ROOMS_EVIDENCE_TIER.OFFICIAL_HOTEL_WEBSITE;
  let confidence = best.confidence === "High" ? "High" : "Medium";

  if (brandOfficial) {
    category = "official_parent_brand_source";
    evidence_tier = ROOMS_EVIDENCE_TIER.OFFICIAL_HIGH;
    confidence = "High";
  } else if (factsheet) {
    category = "official_factsheet";
    evidence_tier = ROOMS_EVIDENCE_TIER.OFFICIAL_HIGH;
    confidence = "High";
  } else if (press) {
    category = "official_press_release_or_opening_announcement";
    evidence_tier = "official_press_release";
    confidence = "High";
  } else if (tourism) {
    category = "tourism_board_convention_bureau_destination_authority";
    evidence_tier = ROOMS_EVIDENCE_TIER.SECONDARY_TOURISM_BOARD;
    confidence = "Medium";
  }

  // Policy: only High or approved Medium (tourism)
  if (confidence === "Medium" && category !== "tourism_board_convention_bureau_destination_authority") {
    // Allow Medium from extractor on hotel_official only if page validated
    if (!ctx.page_validated) {
      return { ok: false, reason: "medium_rooms_requires_page_validation" };
    }
  }

  return {
    ok: true,
    rooms: best.count,
    confidence,
    category,
    evidence_tier,
    evidence_tier_select: mapEvidenceTierCodeToSelect(evidence_tier),
    source_type_airtable: resolveRoomsSourceTypeForAirtable({
      is_official: brandOfficial || factsheet || press || !tourism,
      category,
    }),
    source_url: url,
    method: best.method,
    notes: buildRoomsProvenanceNotes({
      evidence_tier,
      category,
      adapter: "dataforseo_validated_write_policy_v1",
      note: best.method || "html_extract",
    }),
  };
}

/**
 * Classify whether a discovery candidate may enter URL/rooms validation.
 * @param {object} candidate
 */
export function classifyCandidateForValidatedWrite(candidate = {}) {
  const url = String(candidate.url || "").trim();
  const host = hostFromUrl(url) || String(candidate.host || "");
  const tier = String(candidate.source_tier || "");
  const cats = candidate.categories || [];

  if (!url) {
    return { action: "skip", reason: "no_url", field: null };
  }
  if (
    cats.includes("google_maps_local_candidate") &&
    !cats.includes("official_hotel_url_candidate") &&
    !cats.includes("rooms_evidence_page_candidate")
  ) {
    return { action: "hold", reason: "maps_candidate_only", field: "maps" };
  }
  if (tier === SOURCE_TIER.hospitality_trade_secondary || isTrustedSecondaryHost(host)) {
    return {
      action: "hold",
      reason: "travel_weekly_or_trusted_secondary_not_approved",
      field: "secondary",
    };
  }
  if (isRejectedDiscoveryHost(host)) {
    return { action: "reject", reason: "rejected_ota_or_affiliate_host", field: null };
  }
  if (cats.includes("address_candidate") || cats.includes("phone_candidate")) {
    // Contact signals held — may still validate URL/rooms from same candidate
  }
  if (
    cats.includes("official_hotel_url_candidate") &&
    (tier === SOURCE_TIER.brand_official || tier === SOURCE_TIER.hotel_official)
  ) {
    return {
      action: "validate_url",
      reason: null,
      field: "official_property_url",
      requires_strict_hotel_official: tier === SOURCE_TIER.hotel_official,
    };
  }
  if (
    cats.includes("rooms_evidence_page_candidate") &&
    [
      SOURCE_TIER.brand_official,
      SOURCE_TIER.hotel_official,
      SOURCE_TIER.tourism_registry,
      SOURCE_TIER.tourism_board,
      SOURCE_TIER.convention_bureau,
      SOURCE_TIER.factsheet_pdf,
      SOURCE_TIER.official_press,
    ].includes(tier)
  ) {
    return {
      action: "validate_rooms",
      reason: null,
      field: "rooms",
      requires_strict_hotel_official: tier === SOURCE_TIER.hotel_official,
    };
  }
  return { action: "skip", reason: "not_in_approved_write_categories", field: null };
}

export { MAP_ROOMS_SOURCE_TYPE, SOURCE_TIER, hostFromUrl };
