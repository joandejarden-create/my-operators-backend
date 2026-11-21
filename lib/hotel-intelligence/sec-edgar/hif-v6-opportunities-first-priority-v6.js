/**
 * HIF V6 — Founder priority lock: scale opportunities first, enrich ownership second.
 * CUSTOMER_BLOCKED. Evidence standards never lowered for volume.
 */

export const HIF_V6_OPPORTUNITIES_FIRST_PRIORITY =
  "hif-v6-opportunities-first-priority-v6";

export const OPPORTUNITY_MILESTONE_V6 = Object.freeze({
  targetDistinctEvidenceBackedOpportunities: 1000,
  ownershipRequiredForDiscovery: false,
  historicalExpirationIsNotAutomaticallyActiveOpportunity: true,
  listsOnly: Object.freeze([
    "BRAND_CONVERSION_TARGETS",
    "MANAGEMENT_CONTRACT_TARGETS",
  ]),
});

export const RESEARCH_PRIORITY_ORDER_V6 = Object.freeze([
  "LARGE_SCALE_OPPORTUNITY_DISCOVERY",
  "VERIFICATION_AND_DEDUPLICATION",
  "DEFERRED_OWNERSHIP_ENRICHMENT",
]);

export const TRACK_EMPHASIS_UNDER_OPPORTUNITIES_FIRST = Object.freeze({
  TRACK_B: Object.freeze([
    "INTERNATIONAL_HOTEL_SPECIFIC_CONTRACTUAL_DISCLOSURES",
    "CVM_EDINET_SGX_HKEX_SEDAR_AND_OTHER_PRODUCTIVE_REGULATORS",
    "COMPANIES_HOUSE_AND_HMLR_SUPPORTING_VERIFICATION_ONLY",
    "DO_NOT_BLOCK_ON_MISSING_COMPANIES_HOUSE_KEY",
  ]),
  ownershipEnrichment: "DEFERRED_EXCEPT_TARGETED_DISAMBIGUATION",
});

export function progressTowardOpportunityMilestone({
  distinctEvidenceBackedOpportunities = 0,
  target = OPPORTUNITY_MILESTONE_V6.targetDistinctEvidenceBackedOpportunities,
} = {}) {
  const n = Math.max(0, Number(distinctEvidenceBackedOpportunities) || 0);
  const t = Math.max(1, Number(target) || 1000);
  return {
    target: t,
    current: n,
    remaining: Math.max(0, t - n),
    pct: Math.round((1000 * n) / t) / 10,
    milestoneMet: n >= t,
  };
}
