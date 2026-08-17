/**
 * SerpApi Google Hotels search wrapper.
 */

import { serpapiSearch, safeErrorMessage } from "./client.js";
import { normalizeGoogleHotelProperty } from "./normalize.js";

function defaultCheckIn() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + 45);
  return d.toISOString().slice(0, 10);
}
function defaultCheckOut() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + 46);
  return d.toISOString().slice(0, 10);
}

/**
 * Search Google Hotels via SerpApi.
 * @param {{ q: string, gl?: string, hl?: string, currency?: string, check_in_date?: string, check_out_date?: string, adults?: number, timeoutMs?: number }} params
 * @param {{ tracker?: import('./credit-tracker.js').SerpApiCreditTracker, hotelId?: string }} [ctx]
 */
export async function searchGoogleHotels(params, ctx = {}) {
  if (ctx.tracker && !ctx.tracker.canSpend(1)) {
    return {
      ok: false,
      blocked: true,
      reason: ctx.tracker.blockReason || "search_ceiling",
      candidates: [],
      creditsCharged: 0,
    };
  }

  const res = await serpapiSearch(
    {
      engine: "google_hotels",
      q: params.q,
      gl: params.gl || "mx",
      hl: params.hl || "en",
      currency: params.currency || "USD",
      check_in_date: params.check_in_date || defaultCheckIn(),
      check_out_date: params.check_out_date || defaultCheckOut(),
      adults: params.adults || 2,
    },
    { timeoutMs: params.timeoutMs || 90000 }
  );

  const credits = res.creditsCharged || 0;
  if (ctx.tracker) {
    ctx.tracker.record({
      endpoint: "google_hotels/search",
      hotelId: ctx.hotelId,
      purpose: "property_search",
      credits,
      result: res.ok ? "ok" : safeErrorMessage(res.error?.message || "search_failed"),
      useful: false,
      searchId: res.search_id,
    });
  }

  if (!res.ok) {
    return {
      ok: false,
      reason: res.error?.message || `http_${res.status}`,
      candidates: [],
      creditsCharged: credits,
      raw: res.raw,
    };
  }

  const props = Array.isArray(res.data?.properties) ? res.data.properties : [];
  const ads = Array.isArray(res.data?.ads) ? res.data.ads : [];

  // Google Hotels often resolves a specific hotel query to property-details shape at ROOT
  // (name, address, phone, property_token) with empty properties[]. Treat that as the primary hit.
  const rootLooksLikeProperty =
    Boolean(res.data?.property_token) && Boolean(res.data?.name) && props.length === 0;

  const candidates = [];
  if (rootLooksLikeProperty) {
    const direct = normalizeGoogleHotelProperty(res.data, { source: "search_direct_property" });
    if (direct) candidates.push(direct);
  }
  for (const p of props) {
    const n = normalizeGoogleHotelProperty(p, { source: "search_properties" });
    if (n) candidates.push(n);
  }

  return {
    ok: true,
    candidates,
    response_shape: rootLooksLikeProperty ? "direct_property" : props.length ? "properties_list" : "empty_or_ads_only",
    ads_count: ads.length,
    creditsCharged: credits,
    search_id: res.search_id,
    raw_count: candidates.length,
    search_parameters: res.data?.search_parameters || null,
  };
}

export { defaultCheckIn, defaultCheckOut };
