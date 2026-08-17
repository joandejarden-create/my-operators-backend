/**
 * Safe Tripadvisor start-URL builders.
 * Prefer: Hotel_Review URL → Hotels-g URL → per-hotel Search?q=
 * NEVER broad destination free-text ("hotels in Bogotá").
 */

import { MATCH_CONFIG } from "./constants.js";

export const TRIPADVISOR_QUERY_URLS_VERSION = "tripadvisor-query-urls-v1";

function enc(s) {
  return encodeURIComponent(String(s || "").trim());
}

/**
 * @param {string} query
 * @returns {{ ok: boolean, reason?: string }}
 */
export function assertNotBannedDestinationQuery(query) {
  const q = String(query || "").trim();
  for (const re of MATCH_CONFIG.bannedQueryPatterns) {
    if (re.test(q)) {
      return {
        ok: false,
        reason: `banned_destination_free_text:${q.slice(0, 80)}`,
      };
    }
  }
  return { ok: true };
}

/**
 * Build ordered resolution strategies for one hotel.
 * @param {{
 *   name: string,
 *   city?: string|null,
 *   country?: string|null,
 *   tripadvisor_hotel_review_url?: string|null,
 *   tripadvisor_hotels_g_url?: string|null,
 *   tripadvisor_id?: string|null,
 * }} hotel
 */
export function buildTripadvisorResolutionPlan(hotel = {}) {
  /** @type {Array<{ priority: number, kind: string, url: string }>} */
  const steps = [];

  const reviewUrl = String(hotel.tripadvisor_hotel_review_url || "").trim();
  if (/tripadvisor\.com\/Hotel_Review/i.test(reviewUrl)) {
    steps.push({ priority: 1, kind: "hotel_review_url", url: reviewUrl });
  }

  const hotelsG = String(hotel.tripadvisor_hotels_g_url || "").trim();
  if (/tripadvisor\.com\/Hotels-g/i.test(hotelsG)) {
    steps.push({ priority: 2, kind: "hotels_g_url", url: hotelsG });
  }

  // Reconstruct Hotel_Review if we only have an id (geo geo id unknown — Search fallback)
  const taId = String(hotel.tripadvisor_id || "").trim();
  if (taId && /^\d+$/.test(taId) && !steps.some((s) => s.kind === "hotel_review_url")) {
    // Without geo slug we cannot build a stable Hotel_Review URL; keep Search.
  }

  const name = String(hotel.name || "").trim();
  const city = String(hotel.city || "").trim();
  const country = String(hotel.country || "").trim();
  if (name) {
    const qParts = [name, city, country].filter(Boolean);
    const q = qParts.join(" ");
    const ban = assertNotBannedDestinationQuery(q);
    if (!ban.ok) {
      return { ok: false, reason: ban.reason, steps: [] };
    }
    // Guard: query must include hotel name tokens, not only a destination
    if (!/hotel|resort|inn|lodge|suite|marriott|hilton|ibis|radisson|wyndham|choice|hyatt|accor|riu|barcelo|iberostar/i.test(name) &&
        name.split(/\s+/).length < 2) {
      // still allow if city+country present with multi-word name
    }
    steps.push({
      priority: 3,
      kind: "per_hotel_search_q",
      url: `https://www.tripadvisor.com/Search?q=${enc(q)}`,
      query: q,
    });
  }

  steps.sort((a, b) => a.priority - b.priority);
  return { ok: steps.length > 0, steps, reason: steps.length ? null : "no_resolution_inputs" };
}

/**
 * Actor input for maxcopell/tripadvisor — hotels only, no nearby, no photos.
 * @param {Array<{url:string}>} startUrls
 * @param {object} [opts]
 */
export function buildTripadvisorActorInput(startUrls, opts = {}) {
  const urls = (startUrls || [])
    .map((u) => (typeof u === "string" ? { url: u } : u))
    .filter((u) => u?.url && /^https?:\/\/(www\.)?tripadvisor\.com\//i.test(u.url));

  for (const u of urls) {
    // Reject destination free-text Search?q=hotels+in+...
    try {
      const parsed = new URL(u.url);
      if (parsed.pathname.includes("/Search")) {
        const q = parsed.searchParams.get("q") || "";
        const ban = assertNotBannedDestinationQuery(q);
        if (!ban.ok) {
          throw new Error(ban.reason);
        }
      }
    } catch (err) {
      if (String(err?.message || "").startsWith("banned_")) throw err;
    }
  }

  return {
    startUrls: urls,
    includeHotels: true,
    includeRestaurants: false,
    includeAttractions: false,
    includeNearbyResults: false,
    includeTags: false,
    maxItemsPerQuery: Number(opts.maxItemsPerQuery ?? 3),
    maxPhotosPerPlace: 0,
    language: opts.language || "en",
    currency: opts.currency || "USD",
  };
}
