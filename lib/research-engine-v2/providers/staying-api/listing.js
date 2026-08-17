/**
 * StayingAPI listing detail wrapper.
 */

import { stayingRequest, pollJob, safeErrorMessage } from "./client.js";
import { normalizeProperty } from "./normalize.js";

/**
 * @param {string} platform
 * @param {string} id
 * @param {{ timeoutMs?: number }} [params]
 * @param {{ tracker?: import('./credit-tracker.js').StayingCreditTracker, hotelId?: string, useful?: boolean }} [ctx]
 */
export async function getListing(platform, id, params = {}, ctx = {}) {
  if (ctx.tracker && !ctx.tracker.canSpend(3)) {
    return {
      ok: false,
      blocked: true,
      reason: ctx.tracker.blockReason || "credit_ceiling",
      candidate: null,
      creditsCharged: 0,
    };
  }

  let res = await stayingRequest(
    `/listing/${encodeURIComponent(platform)}/${encodeURIComponent(id)}`,
    { timeoutMs: params.timeoutMs || 90000 }
  );

  if (res.async && res.jobId) {
    res = await pollJob(res.jobId, { timeoutMs: params.timeoutMs || 90000 });
  }

  const credits = res.creditsCharged || 0;
  if (ctx.tracker) {
    ctx.tracker.record({
      endpoint: "/v1/listing",
      hotelId: ctx.hotelId,
      purpose: "listing_detail",
      credits,
      result: res.ok ? "ok" : safeErrorMessage(res.error?.message || "listing_failed"),
      useful: Boolean(ctx.useful),
      requestId: res.requestId,
    });
  }

  if (!res.ok) {
    return {
      ok: false,
      reason: res.error?.message || `http_${res.status}`,
      candidate: null,
      creditsCharged: credits,
    };
  }

  const candidate = normalizeProperty(res.data);
  return {
    ok: Boolean(candidate),
    candidate,
    creditsCharged: credits,
    meta: res.meta,
  };
}
