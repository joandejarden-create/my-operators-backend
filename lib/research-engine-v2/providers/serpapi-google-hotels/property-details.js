/**
 * SerpApi Google Hotels property details (same engine + property_token).
 */

import { serpapiSearch, safeErrorMessage } from "./client.js";
import { normalizeGoogleHotelProperty } from "./normalize.js";
import { defaultCheckIn, defaultCheckOut } from "./search.js";

/**
 * @param {{ property_token: string, q?: string, gl?: string, hl?: string, currency?: string, check_in_date?: string, check_out_date?: string, adults?: number, timeoutMs?: number }} params
 * @param {{ tracker?: import('./credit-tracker.js').SerpApiCreditTracker, hotelId?: string, useful?: boolean }} [ctx]
 */
export async function getGoogleHotelDetails(params, ctx = {}) {
  if (!params?.property_token) {
    return { ok: false, reason: "missing_property_token", candidate: null, creditsCharged: 0 };
  }
  if (ctx.tracker && !ctx.tracker.canSpend(1)) {
    return {
      ok: false,
      blocked: true,
      reason: ctx.tracker.blockReason || "search_ceiling",
      candidate: null,
      creditsCharged: 0,
    };
  }

  const res = await serpapiSearch(
    {
      engine: "google_hotels",
      property_token: params.property_token,
      q: params.q || "hotel",
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
      endpoint: "google_hotels/property_details",
      hotelId: ctx.hotelId,
      purpose: "property_details",
      credits,
      result: res.ok ? "ok" : safeErrorMessage(res.error?.message || "details_failed"),
      useful: Boolean(ctx.useful),
      searchId: res.search_id,
    });
  }

  if (!res.ok) {
    return {
      ok: false,
      reason: res.error?.message || `http_${res.status}`,
      candidate: null,
      creditsCharged: credits,
      raw: res.raw,
    };
  }

  // Property details: fields are at root (name, address, phone, …), not under properties[]
  const root = res.data || {};
  const candidate = normalizeGoogleHotelProperty(root, { source: "property_details" });

  return {
    ok: true,
    candidate,
    creditsCharged: credits,
    search_id: res.search_id,
    has_amenities: Array.isArray(root.amenities) && root.amenities.length > 0,
    has_excluded_amenities: Array.isArray(root.excluded_amenities) && root.excluded_amenities.length > 0,
    has_phone: Boolean(root.phone),
    has_address: Boolean(root.address),
    has_gps: Boolean(root.gps_coordinates?.latitude != null),
    // expose observed rooms arrays length only for capability proof — never as Keys
    _observed_featured_price_room_types: Array.isArray(root.featured_prices)
      ? root.featured_prices.reduce((n, p) => n + (Array.isArray(p.rooms) ? p.rooms.length : 0), 0)
      : null,
    _observed_essential_info: root.essential_info || null,
  };
}
