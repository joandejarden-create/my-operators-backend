/**
 * HIF V6 — Founder priority lock: scale opportunities first, enrich ownership second.
 * CUSTOMER_BLOCKED. Evidence standards never lowered for volume.
 */

export const HIF_V6_OPPORTUNITIES_FIRST_PRIORITY =
  "hif-v6-opportunities-first-priority-v6";

/** Immediate commercial milestone (research inventory, not outreach publication). */
export const OPPORTUNITY_MILESTONE_V6 = Object.freeze({
  targetDistinctEvidenceBackedOpportunities: 1000,
  ownershipRequiredForDiscovery: false,
  ownershipRequiredForOutreachReady: false, // deferred enrichment; outreach-ready may still prefer contacts later
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

export const OPPORTUNITY_QUALIFICATION_GATES_V6 = Object.freeze([
  "DISTINCT_IDENTIFIABLE_PHYSICAL_HOTEL",
  "HOTEL_SPECIFIC_FRANCHISE_MANAGEMENT_OR_COMMERCIAL_EVENT_EVIDENCE",
  "CORRECT_AGREEMENT_OR_SIGNAL_CLASSIFICATION",
  "ATTRIBUTABLE_SOURCE",
  "DEFENSIBLE_COMMERCIAL_THESIS",
  "DEDUPE_AGAINST_EXISTING_HOTELS_AND_EVENTS",
]);

/** Separate tracking buckets — do not collapse into one active count. */
export const OPPORTUNITY_TRACKING_BUCKET_V6 = Object.freeze({
  VERIFIED_CONTRACT_EXPIRATIONS: "VERIFIED_CONTRACT_EXPIRATIONS",
  VERIFIED_NOTICE_PROVISIONS: "VERIFIED_NOTICE_PROVISIONS",
  EVIDENCE_BACKED_COMMERCIAL_SIGNALS_WITHOUT_DATES:
    "EVIDENCE_BACKED_COMMERCIAL_SIGNALS_WITHOUT_DATES",
  QUALIFIED_ACTIONABLE_OPPORTUNITIES: "QUALIFIED_ACTIONABLE_OPPORTUNITIES",
  HISTORICAL_CLOSED_SUPERSEDED: "HISTORICAL_CLOSED_SUPERSEDED",
  RESEARCH_CANDIDATES: "RESEARCH_CANDIDATES",
  INVALID_OR_QUARANTINED: "INVALID_OR_QUARANTINED",
});

export const TRACK_EMPHASIS_UNDER_OPPORTUNITIES_FIRST = Object.freeze({
  TRACK_A: Object.freeze([
    "SCALE_CMBS_EX10_FRANCHISE_MANAGEMENT_SCHEDULES",
    "NET_NEW_EXPIRATIONS_AND_LIFECYCLE_EVENTS",
    "ORIGINAL_AGREEMENT_AND_MGMT_POINTER_RECOVERY",
    "DO_NOT_REPEAT_EXHAUSTED_APPRAISAL_RESEARCH",
  ]),
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
