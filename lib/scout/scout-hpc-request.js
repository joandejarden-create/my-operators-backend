/**
 * P8.8 — Attach Scout HPC gate result onto Scout API query objects.
 */

import { shouldUseHpcScoutCensus } from "../hotel-census/brand-presence-hpc-request.js";

/**
 * @param {object} req
 * @param {Record<string, unknown>} [query]
 * @returns {Record<string, unknown>}
 */
export function withScoutHpcQuery(req, query = {}) {
  const q = { ...(query || req?.query || {}) };
  q._scoutHpc = shouldUseHpcScoutCensus(req);
  return q;
}
