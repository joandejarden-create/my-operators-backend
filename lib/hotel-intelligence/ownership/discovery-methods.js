/**
 * Ownership discovery method catalog + telemetry helpers (P1.6).
 * Records which research lane produced a candidate — not chain-of-thought.
 */

export const DISCOVERY_METHODS_VERSION = "ownership-discovery-methods-v1";

/** @typedef {typeof OWNERSHIP_DISCOVERY_METHODS[number]} OwnershipDiscoveryMethod */

export const OWNERSHIP_DISCOVERY_METHODS = Object.freeze([
  "tourism_registry_identity",
  "hotel_name_to_company",
  "company_identifier_resolution",
  "cnpj_validation",
  "branch_to_matrix",
  "qsa_partner_climb",
  "related_entity_discovery",
  "owner_portfolio",
  "transaction_evidence",
  "franchisor_financial_disclosure",
  "lease_disclosure",
  "development_announcement",
  "condo_structure_detection",
  "corporate_parent_resolution",
  "government_registry",
  "gleif_corporate_graph",
  "generic_web",
  "webhound_escalation",
  "manual_review",
  "other",
]);

/**
 * @param {string} method
 */
export function isDiscoveryMethod(method) {
  return OWNERSHIP_DISCOVERY_METHODS.includes(String(method || "").trim());
}

/**
 * @param {object} [partial]
 */
export function createMethodAttempt(partial = {}) {
  return {
    method: partial.method || "other",
    attempted: partial.attempted !== false,
    success: Boolean(partial.success),
    candidate_count: Number(partial.candidate_count || 0),
    relationship_count: Number(partial.relationship_count || 0),
    verified_high_count: Number(partial.verified_high_count || 0),
    false_positive: Boolean(partial.false_positive),
    source_class: partial.source_class || null,
    country: partial.country || null,
    searches: Number(partial.searches || 0),
    pages_fetched: Number(partial.pages_fetched || 0),
    latency_ms: Number(partial.latency_ms || 0),
    cost_usd: partial.cost_usd != null ? Number(partial.cost_usd) : null,
    stop_reason: partial.stop_reason || null,
    notes: Array.isArray(partial.notes) ? partial.notes : [],
  };
}

/**
 * Aggregate method attempts into a performance table row map.
 * @param {object[]} attempts
 */
export function aggregateMethodPerformance(attempts = []) {
  /** @type {Map<string, object>} */
  const byMethod = new Map();
  for (const a of attempts || []) {
    const key = a.method || "other";
    const row =
      byMethod.get(key) ||
      {
        method: key,
        attempts: 0,
        successes: 0,
        useful_candidates: 0,
        verified_high_relationships: 0,
        false_positives: 0,
        searches: 0,
        pages_fetched: 0,
        cost_usd: 0,
        latency_ms: 0,
      };
    row.attempts += 1;
    if (a.success) row.successes += 1;
    row.useful_candidates += Number(a.candidate_count || 0);
    row.verified_high_relationships += Number(a.verified_high_count || 0);
    if (a.false_positive) row.false_positives += 1;
    row.searches += Number(a.searches || 0);
    row.pages_fetched += Number(a.pages_fetched || 0);
    row.cost_usd += Number(a.cost_usd || 0);
    row.latency_ms += Number(a.latency_ms || 0);
    byMethod.set(key, row);
  }
  return [...byMethod.values()].sort((a, b) => b.attempts - a.attempts);
}
