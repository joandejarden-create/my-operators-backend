/**
 * Hotel Intelligence — Apify usage / cost tracking (operational metadata).
 *
 * Auth notes (readiness):
 * - Interactive Cursor agent runs: MCP plugin-apify-apify session is sufficient.
 * - Server-side / automated Node jobs using apify-client: require local
 *   APIFY_TOKEN or APIFY_API_TOKEN (MCP does not inject into local scripts).
 *
 * Cost source of truth for finished runs:
 *   GET /v2/actor-runs/{runId} → usageTotalUsd (+ chargedEventCounts)
 *   Prefer MCP resource http://api.apify.internal:3333/v2/actor-runs/{runId}
 *   over get-actor-run tool summary (cost fields often omitted there).
 */

import {
  APIFY_AUTH_METHODS,
  APIFY_USE_CASES,
  DEFAULT_TRIPADVISOR_ACTOR,
} from "./constants.js";

export {
  APIFY_USAGE_VERSION,
  APIFY_USE_CASES,
  APIFY_AUTH_METHODS,
  APIFY_COST_SOURCE,
  DEFAULT_TRIPADVISOR_ACTOR,
} from "./constants.js";

export {
  buildApifyUsageRecord,
  fromApifyRunPayload,
  extractApifyRunCost,
  summarizeApifyUsage,
  scrubSecrets,
} from "./normalize.js";

export {
  createApifyUsageStore,
  resolveApifyUsageDir,
} from "./store.js";

/**
 * Convenience: record a Tripadvisor room-count Actor run outcome.
 * @param {{ recordRun: Function }} store
 * @param {object} input
 */
export function recordTripadvisorRoomCountRun(store, input = {}) {
  return store.recordRun({
    use_case: APIFY_USE_CASES.ROOM_COUNT,
    actor_id: input.actor_id || DEFAULT_TRIPADVISOR_ACTOR.actor_id,
    actor_name: input.actor_name || DEFAULT_TRIPADVISOR_ACTOR.actor_name,
    auth_method: input.auth_method || APIFY_AUTH_METHODS.MCP,
    ...input,
  });
}
