/**
 * Governed learning process — hotel findings become shared, versioned improvements.
 */

export const LEARNING_POLICY_VERSION = "dealality-learning-regression-v1";

export const LEARNING_CLASS = Object.freeze({
  BUG: "bug",
  MISSING_SOURCE_STRATEGY: "missing_source_strategy",
  EVIDENCE_MODEL_ISSUE: "evidence_model_issue",
  MARKET_SPECIFIC_VARIATION: "market_specific_variation",
  PRODUCT_IMPROVEMENT: "genuine_product_improvement",
});

export const LEARNING_ADOPTION_STATUS = Object.freeze({
  RECORDED: "RECORDED",
  PROPOSED: "PROPOSED",
  IMPLEMENTED: "IMPLEMENTED",
  TESTED: "TESTED",
  SHADOW_COMPARED: "SHADOW_COMPARED",
  VERSIONED: "VERSIONED",
  CANARY_PASSED: "CANARY_PASSED",
  ADOPTED: "ADOPTED",
  REJECTED: "REJECTED",
});

/**
 * Permanent regression hotel suite (cases, not hard-coded customer exceptions).
 */
export const HISTORICAL_HOTEL_REGRESSION_MATRIX = Object.freeze({
  version: LEARNING_POLICY_VERSION,
  gdi: Object.freeze([
    {
      hotel: "Wyndham San Diego Bayside",
      cases: [
        "manual_reviewed_package_vs_autonomous_full_e2e",
        "preserve_historical_records",
        "contact_buyer_resolution",
      ],
    },
    {
      hotel: "Charlotte Marriott City Center",
      hpcId: "recbRIFhro6tLmSrO",
      cases: [
        "rows_handoff_collapse",
        "ready_ok_integrity",
        "17_technical_ready_vs_14_ready_hf",
        "four_commercially_approved_staged",
        "empty_customer_store_before_publication",
      ],
    },
    {
      hotel: "Bethesda Marriott",
      hpcId: "recLuxvwwxID7U2B8",
      cases: [
        "preserve_existing_visible_records",
        "correct_hotel_geography",
        "staged_incremental_discovery",
        "eleven_independent_segment_coverage",
        "commercial_quality_vs_technical_ready",
      ],
    },
    {
      hotel: "Nobu Hotel London Portman Square",
      cases: [
        "correct_authorization_ledger",
        "public_research_continuation",
        "candidate_exhaustion_vs_segment_sufficiency",
        "airtable_visibility_vs_fs_counts",
      ],
    },
  ]),
  adp: Object.freeze([
    {
      hotel: "Charlotte Marriott City Center",
      hpcId: "recbRIFhro6tLmSrO",
      adpPropertyId: "adp_charlotte_marriott_city_center",
      cases: [
        "252_observations",
        "waterstone_boca_contamination_prevented",
        "charlotte_market_registry",
        "reality_gap_not_measured",
      ],
    },
    {
      hotel: "Waterstone Boca Raton",
      adpPropertyId: "adp_waterstone_boca_raton",
      cases: ["legitimate_south_florida_entity_resolution"],
    },
    {
      hotel: "Tampa Airport Marriott",
      hpcId: "recSM9ARs8U9UZ3AZ",
      adpPropertyId: "adp_tampa_airport_marriott",
      cases: ["independent_tampa_market"],
    },
    {
      hotel: "Falls Church Marriott Fairview Park",
      hpcId: "recRkGGNKslJptFIL",
      adpPropertyId: "adp_falls_church_marriott_fairview_park",
      cases: ["northern_virginia_market_identity"],
    },
    {
      hotel: "Renaissance Arlington Capital View",
      hpcId: "recyZciNlXq5KulnD",
      adpPropertyId: "adp_renaissance_arlington_capital_view",
      cases: ["independent_of_fairview"],
    },
  ]),
});

export function createLearningRecord({
  id,
  hotelId = null,
  product,
  classification,
  finding,
  proposedSharedChange = null,
  affectedStages = [],
  regressionTests = [],
} = {}) {
  return Object.freeze({
    policyVersion: LEARNING_POLICY_VERSION,
    id,
    hotelId,
    product,
    classification,
    finding,
    proposedSharedChange,
    affectedStages: Object.freeze([...(affectedStages || [])]),
    regressionTests: Object.freeze([...(regressionTests || [])]),
    status: LEARNING_ADOPTION_STATUS.RECORDED,
    recordedAt: new Date().toISOString(),
    adoptionChecklist: Object.freeze([
      "Record finding",
      "Classify",
      "Propose shared change",
      "Identify affected stages",
      "Create regression tests",
      "Reprocess historical fixtures",
      "Shadow compare existing hotel reports",
      "Check budget and publication effects",
      "Approve versioned workflow change",
      "Canary hotels",
      "Expand after regression pass",
    ]),
    forbidden: Object.freeze([
      "Silently mutate rules of active hotel runs",
      "Add hotel-specific exceptions where shared contract solves it",
      "Force market-specific knowledge into universal rules",
      "Claim adoption without implemented+tested+versioned+invoked",
    ]),
  });
}

export function isLearningAdopted(record) {
  return record?.status === LEARNING_ADOPTION_STATUS.ADOPTED;
}
