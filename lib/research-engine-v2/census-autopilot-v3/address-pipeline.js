/**
 * Address pipeline — normalize + staging claim shape (no Cvent/legacy).
 */

import { normalizeAddress } from "../census-autopilot-v1/golden-gap-v13/address-coordinate-resolvers.js";

export { normalizeAddress };

/**
 * Build staging address from official evidence only.
 */
export function buildAddressStaging(opts = {}) {
  const raw = opts.raw_address || opts.address || null;
  if (!raw) {
    return { ok: false, raw_address: null, normalized_address: null, claim: null };
  }
  const { raw_address, normalized_address } = normalizeAddress(raw);
  const sourceType = opts.source_type || "official_property_page";
  const serpapi = /serpapi/i.test(sourceType) || opts.serpapi_used === true;
  return {
    ok: true,
    raw_address,
    normalized_address,
    claim: {
      value: normalized_address,
      source: opts.source || sourceType,
      source_type: sourceType,
      source_url: opts.source_url || null,
      retrieved_at: opts.retrieved_at || new Date().toISOString(),
      confidence: opts.confidence || "High",
      match_confidence: opts.match_confidence || "High",
      research_run: opts.research_run || null,
      serpapi_used: serpapi,
      cvent_used_as_production_evidence: false,
      legacy_used_as_production_evidence: false,
      status: "active",
      raw_address,
    },
  };
}
