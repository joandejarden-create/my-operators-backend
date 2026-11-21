/**
 * HIF V6 — Founder amendment operating contract (industrial-scale discovery).
 *
 * Aspirational milestones ≠ minimum verification quotas.
 * Never lower evidence standards to meet a numerical target.
 * Eligibility ≠ coverage. Dual commercial lists only. Lease deferred.
 */

export const HIF_INDUSTRIAL_SCALE_OPERATING_CONTRACT_V6 =
  "hif-industrial-scale-operating-contract-v6";

/** Replaces prior 250 / 50 volume ambitions. */
export const SCALE_AMBITIONS_V6 = Object.freeze({
  initialIntensiveRegions: [
    "NORTH_AMERICA",
    "CENTRAL_AMERICA",
    "CARIBBEAN",
    "SOUTH_AMERICA",
    "WESTERN_EUROPE",
    "NORTHERN_EUROPE",
    "SOUTHERN_EUROPE",
    "CENTRAL_EUROPE",
    "EASTERN_EUROPE",
  ],
  geographicExclusions: "NONE",
  otherRegionsEligible: true,
  aspirationalMilestones: Object.freeze({
    distinctResearchedHotelsInitialScale: 10000,
    verifiedOwnerPropertyRelationships: 2000,
    hotelSpecificVerifiedFranchiseOrManagementExpirations: 1000,
    evidenceBackedCommercialResearchProspects: 2000,
    /** Founder priority (2026-11-21): opportunity universe before ownership enrichment */
    distinctEvidenceBackedHotelDevelopmentOpportunities: 1000,
  }),
  priorityOrder: Object.freeze([
    "LARGE_SCALE_OPPORTUNITY_DISCOVERY",
    "VERIFICATION_AND_DEDUPLICATION",
    "DEFERRED_OWNERSHIP_ENRICHMENT",
  ]),
  ownershipEnrichmentDefault: "DEFERRED_EXCEPT_TARGETED_DISAMBIGUATION",
  longerTermDesignCapacity: Object.freeze({
    hotels: "TENS_OF_THOUSANDS",
    contractualEvents: "THOUSANDS_PLUS",
    redesignRequired: false,
  }),
  supersededTargets: Object.freeze({
    priorVerifiedContractExpirations: 250,
    priorManagementExpirations: 50,
    status: "REPLACED_BY_INDUSTRIAL_SCALE_AMBITIONS",
  }),
  evidenceRule: "NEVER_LOWER_EVIDENCE_STANDARDS_TO_MEET_NUMERICAL_TARGET",
});

/**
 * Coverage vs contractual verification — must remain separate metrics.
 */
export const HOTEL_INTELLIGENCE_METRIC = Object.freeze({
  DISCOVERED_HOTELS: "DISCOVERED_HOTELS",
  IDENTITY_VERIFIED_HOTELS: "IDENTITY_VERIFIED_HOTELS",
  OWNER_LINKED_HOTELS: "OWNER_LINKED_HOTELS",
  AGREEMENT_IDENTIFIED_HOTELS: "AGREEMENT_IDENTIFIED_HOTELS",
  VERIFIED_EXPIRATION_HOTELS: "VERIFIED_EXPIRATION_HOTELS",
  VERIFIED_NOTICE_HOTELS: "VERIFIED_NOTICE_HOTELS",
  CURRENT_STATUS_VERIFIED_HOTELS: "CURRENT_STATUS_VERIFIED_HOTELS",
  QUALIFIED_COMMERCIAL_TARGETS: "QUALIFIED_COMMERCIAL_TARGETS",
});

export const COMMERCIAL_PROSPECT_LISTS_V6 = Object.freeze({
  BRAND_CONVERSION_TARGETS: "BRAND_CONVERSION_TARGETS",
  MANAGEMENT_CONTRACT_TARGETS: "MANAGEMENT_CONTRACT_TARGETS",
  thirdListAuthorized: false,
  leaseDedicatedProduct: false,
  leaseClassificationRetained: true,
});

export const DISCOVERY_CONSTRAINTS_V6 = Object.freeze({
  researchHorizonFilter: "NONE",
  geographicEligibilityExclusions: "NONE",
  fixedIssuerListIsCeiling: false,
  preEnumeratedBatchIsCeiling: false,
  customerBlockedDefault: true,
  hpcCreatesUnauthorized: false,
  metaSchemaApplyUnauthorized: false,
  exaAgentRouting: "MANUAL_ESCALATION_ONLY",
  jevRouting: "DETERMINISTIC_ONLY_SHADOW_OPTIONAL",
});

export const RESEARCH_SEQUENCING_AFTER_V5_MERGE = Object.freeze([
  "COMPLETE_62_EXHIBIT_EX10_HARVEST",
  "EXPAND_US_FRANCHISE_AND_NAMED_MANAGEMENT_SOURCES",
  "LAUNCH_REAL_NON_US_DISCOVERY_AMERICAS_EUROPE",
  "EXPAND_SOURCES_BY_MEASURED_YIELD",
  "RECONCILE_INTO_HOTEL_EXPLORER_INTELLIGENCE",
  "REPEAT_CONTROLLED_INCREMENTAL_BATCHES",
]);

/**
 * Empty metric bag — callers fill from verified artifacts only.
 */
export function emptyHotelIntelligenceScorecard(asOf = null) {
  const zeros = {};
  for (const k of Object.values(HOTEL_INTELLIGENCE_METRIC)) zeros[k] = 0;
  return {
    version: HIF_INDUSTRIAL_SCALE_OPERATING_CONTRACT_V6,
    asOf: asOf || new Date().toISOString().slice(0, 10),
    metrics: zeros,
    notes: {
      eligibilityIsNotCoverage: true,
      identityIsNotContractOpportunity: true,
      aspirationalMilestonesAreNotQuotas: true,
    },
  };
}

/**
 * Assert metric keys remain distinct (no collapsing coverage into verification).
 */
export function assertMetricTaxonomyIntegrity(scorecard = {}) {
  const metrics = scorecard.metrics || scorecard;
  const missing = Object.values(HOTEL_INTELLIGENCE_METRIC).filter((k) => !(k in metrics));
  if (missing.length) {
    return { ok: false, error: "MISSING_METRIC_KEYS", missing };
  }
  // Soft invariant: qualified targets cannot exceed discovered hotels when both known
  const discovered = Number(metrics.DISCOVERED_HOTELS) || 0;
  const qualified = Number(metrics.QUALIFIED_COMMERCIAL_TARGETS) || 0;
  const verifiedExp = Number(metrics.VERIFIED_EXPIRATION_HOTELS) || 0;
  if (discovered > 0 && qualified > discovered) {
    return {
      ok: false,
      error: "QUALIFIED_EXCEEDS_DISCOVERED",
      discovered,
      qualified,
    };
  }
  if (discovered > 0 && verifiedExp > discovered) {
    return {
      ok: false,
      error: "VERIFIED_EXPIRATION_EXCEEDS_DISCOVERED",
      discovered,
      verifiedExp,
    };
  }
  return { ok: true };
}

export default {
  HIF_INDUSTRIAL_SCALE_OPERATING_CONTRACT_V6,
  SCALE_AMBITIONS_V6,
  HOTEL_INTELLIGENCE_METRIC,
  COMMERCIAL_PROSPECT_LISTS_V6,
  DISCOVERY_CONSTRAINTS_V6,
  RESEARCH_SEQUENCING_AFTER_V5_MERGE,
  emptyHotelIntelligenceScorecard,
  assertMetricTaxonomyIntegrity,
};
