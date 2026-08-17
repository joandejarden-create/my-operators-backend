/**
 * StayingAPI search wrapper.
 */

import { stayingRequest, pollJob, safeErrorMessage } from "./client.js";
import { normalizeProperty } from "./normalize.js";

function defaultCheckIn() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + 60);
  return d.toISOString().slice(0, 10);
}
function defaultCheckOut() {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + 62);
  return d.toISOString().slice(0, 10);
}

/**
 * @param {{ location: string, platforms?: string[], limit?: number, timeoutMs?: number, checkIn?: string, checkOut?: string, adults?: number }} params
 * @param {{ tracker?: import('./credit-tracker.js').StayingCreditTracker, hotelId?: string }} [ctx]
 */
export async function searchProperties(params, ctx = {}) {
  const platforms = (params.platforms || ["booking"]).join(",");
  const limit = params.limit ?? 5;
  const estimate = 8; // conservative before knowing result count
  if (ctx.tracker && !ctx.tracker.canSpend(estimate)) {
    return {
      ok: false,
      blocked: true,
      reason: ctx.tracker.blockReason || "credit_ceiling",
      candidates: [],
      creditsCharged: 0,
    };
  }

  let res = await stayingRequest("/search", {
    query: {
      location: params.location,
      platforms,
      limit,
      // dates improve hotel inventory hits without implying Rooms/Keys
      checkIn: params.checkIn || defaultCheckIn(),
      checkOut: params.checkOut || defaultCheckOut(),
      adults: params.adults || 2,
    },
    timeoutMs: params.timeoutMs || 90000,
  });

  // Free plan rate limit — retry a few times (failed calls are free)
  for (let attempt = 0; attempt < 4 && !res.ok && (res.status === 429 || /rate limit/i.test(String(res.error?.message || ""))); attempt++) {
    const waitSec = res.retryAfterSec || 12;
    await new Promise((r) => setTimeout(r, (waitSec + 1) * 1000));
    res = await stayingRequest("/search", {
      query: {
        location: params.location,
        platforms,
        limit,
        checkIn: params.checkIn || defaultCheckIn(),
        checkOut: params.checkOut || defaultCheckOut(),
        adults: params.adults || 2,
      },
      timeoutMs: params.timeoutMs || 90000,
    });
  }

  if (res.async && res.jobId) {
    res = await pollJob(res.jobId, { timeoutMs: params.timeoutMs || 90000 });
  }

  const credits = res.creditsCharged || 0;
  if (ctx.tracker) {
    ctx.tracker.record({
      endpoint: "/v1/search",
      hotelId: ctx.hotelId,
      purpose: "property_search",
      credits,
      result: res.ok ? "ok" : safeErrorMessage(res.error?.message || "search_failed"),
      useful: false,
      requestId: res.requestId,
    });
  }

  if (!res.ok) {
    return {
      ok: false,
      reason: res.error?.message || `http_${res.status}`,
      candidates: [],
      creditsCharged: credits,
      meta: res.meta,
    };
  }

  const rows = Array.isArray(res.data) ? res.data : [];
  const candidates = rows.map(normalizeProperty).filter(Boolean);

  return {
    ok: true,
    candidates,
    creditsCharged: credits,
    meta: res.meta,
    raw_count: rows.length,
  };
}
