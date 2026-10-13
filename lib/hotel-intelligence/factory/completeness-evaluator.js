/**
 * Packet 2.8 — HotelIntelligenceCompletenessEvaluator
 * Domain-by-domain status — never a misleading single % alone.
 */

export const COMPLETENESS_ENGINE_VERSION = "hotel-intelligence-completeness-v1";

export const DOMAIN_STATUSES = Object.freeze([
  "COMPLETE",
  "STRONG",
  "PARTIAL",
  "MISSING",
  "CONFLICTED",
  "STALE",
  "RESEARCH_REQUIRED",
]);

export const COMPLETENESS_DOMAINS = Object.freeze([
  "IDENTITY",
  "PROPERTY_FUNDAMENTALS",
  "OWNERSHIP",
  "OPERATOR",
  "BRAND",
  "ORGANIZATION",
  "PORTFOLIO",
  "PEOPLE",
  "RELATIONSHIPS",
  "DEVELOPMENT",
  "TRANSACTIONS",
  "MARKET",
  "AREA_HOTELS",
  "DEMAND",
  "ACCESS",
  "SOURCES",
  "RESEARCH",
]);

export const COMPLETENESS_TIERS = Object.freeze({
  TIER_A_FOUNDATIONAL: [
    "IDENTITY",
    "PROPERTY_FUNDAMENTALS",
    "BRAND",
    "OPERATOR",
    "MARKET",
    "AREA_HOTELS",
    "DEMAND",
    "ACCESS",
  ],
  TIER_B_INTELLIGENCE: [
    "OWNERSHIP",
    "ORGANIZATION",
    "RELATIONSHIPS",
    "PEOPLE",
    "SOURCES",
  ],
  TIER_C_DEEP: ["TRANSACTIONS", "DEVELOPMENT", "RESEARCH", "PORTFOLIO"],
});

function hasText(v) {
  return v != null && String(v).trim() !== "" && String(v).trim().toLowerCase() !== "unknown";
}

function domainStatusFromEvidence(domain, evidence = {}) {
  const d = evidence[domain] || evidence[domain.toLowerCase()] || {};
  if (d.status && DOMAIN_STATUSES.includes(d.status)) return d;
  const available = d.data_available === true || hasText(d.value) || (Array.isArray(d.items) && d.items.length);
  const conflicted = d.conflicted === true || d.status === "CONFLICTED";
  const researchRequired = d.research_required === true;
  let status = "MISSING";
  if (conflicted) status = "CONFLICTED";
  else if (researchRequired) status = "RESEARCH_REQUIRED";
  else if (d.complete === true) status = "COMPLETE";
  else if (d.strong === true || (available && d.confidence === "HIGH")) status = "STRONG";
  else if (available) status = "PARTIAL";
  return {
    domain,
    status,
    what_is_missing: d.what_is_missing || (status === "MISSING" || status === "PARTIAL" ? d.gap || null : null),
    why_it_matters: d.why_it_matters || null,
    best_source: d.best_source || null,
    research_method: d.research_method || null,
    estimated_cost_class: d.estimated_cost_class || null,
    reusable_at: d.reusable_at || null, // PROPERTY | ORGANIZATION | BRAND | OPERATOR | MARKET
    escalation_required: Boolean(d.escalation_required),
    confidence: d.confidence || null,
    provenance: d.provenance || null,
  };
}

/**
 * Evaluate completeness for a hotel from domain evidence map.
 * @param {{ hotel_id: string, hotel_name?: string, domains?: object }} input
 */
export function evaluateHotelIntelligenceCompleteness(input = {}) {
  const domains = {};
  for (const domain of COMPLETENESS_DOMAINS) {
    domains[domain] = domainStatusFromEvidence(domain, input.domains || {});
  }

  const tierComplete = (tierDomains) =>
    tierDomains.every((d) => ["COMPLETE", "STRONG"].includes(domains[d].status));

  const actionable = Object.values(domains)
    .filter((d) =>
      ["MISSING", "PARTIAL", "CONFLICTED", "STALE", "RESEARCH_REQUIRED"].includes(d.status)
    )
    .map((d) => ({
      domain: d.domain,
      status: d.status,
      what_is_missing: d.what_is_missing,
      why_it_matters: d.why_it_matters,
      best_source: d.best_source,
      research_method: d.research_method,
      estimated_cost_class: d.estimated_cost_class,
      reusable_at: d.reusable_at,
      escalation_required: d.escalation_required,
    }));

  return {
    engine_version: COMPLETENESS_ENGINE_VERSION,
    hotel_id: input.hotel_id || null,
    hotel_name: input.hotel_name || null,
    evaluated_at: new Date().toISOString(),
    domains,
    tiers: {
      TIER_A_FOUNDATIONAL_COMPLETE: tierComplete(COMPLETENESS_TIERS.TIER_A_FOUNDATIONAL),
      TIER_B_INTELLIGENCE_COMPLETE: tierComplete(COMPLETENESS_TIERS.TIER_B_INTELLIGENCE),
      TIER_C_DEEP_COMPLETE: tierComplete(COMPLETENESS_TIERS.TIER_C_DEEP),
    },
    actionable_gaps: actionable,
    note: "Do not report 100% completeness from UI text alone. Unknown/Probable/Unverified ≠ verified complete.",
  };
}
