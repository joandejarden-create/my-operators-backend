/**
 * GDI 2.0 Phase 3A — Corporate Meetings & Training segment constants.
 */

export const CORPORATE_MEETINGS_TRAINING_V1 = "gdi-corporate-meetings-training-v1";

export const CMT_RESEARCH_SEGMENT = "corporate_meetings_training";

export const CMT_DEMAND_CATEGORY = Object.freeze({
  LEADERSHIP_EXECUTIVE_MEETINGS: "LEADERSHIP_EXECUTIVE_MEETINGS",
  ANNUAL_SALES_KICKOFFS: "ANNUAL_SALES_KICKOFFS",
  CORPORATE_TRAINING_ACADEMIES: "CORPORATE_TRAINING_ACADEMIES",
  NEW_HIRE_ONBOARDING_COHORTS: "NEW_HIRE_ONBOARDING_COHORTS",
  REGIONAL_MANAGEMENT_CONFERENCES: "REGIONAL_MANAGEMENT_CONFERENCES",
  CORPORATE_RETREATS_OFFSITES: "CORPORATE_RETREATS_OFFSITES",
  DISTRIBUTOR_PARTNER_TRAINING: "DISTRIBUTOR_PARTNER_TRAINING",
  MA_INTEGRATION_MEETINGS: "MA_INTEGRATION_MEETINGS",
  PRODUCT_SERVICE_TRAINING: "PRODUCT_SERVICE_TRAINING",
  RECURRING_DEPARTMENTAL_MEETINGS: "RECURRING_DEPARTMENTAL_MEETINGS",
});

export const CMT_FULL_E2E_STAGES = Object.freeze([
  "hotel_profile",
  "segment_discovery",
  "candidate_identity_resolution",
  "demand_investigation",
  "buyer_research",
  "hotel_matching",
  "shared_qualification",
  "research_continuation_or_shadow_sales_ready",
  "evidence_backed_opportunity_brief",
  "checkpoint_persist_local_only",
]);

export const CMT_PILOT_LIMITS = Object.freeze({
  maxHotels: 5,
  maxCandidatesPerHotel: 10,
  maxExternalRetrievalsTotal: 60,
  authorizePaidResearch: false,
  authorizePublication: false,
  authorizeProductionWrites: false,
  allowRender: false,
});

/** Hiring/growth alone must never establish hotel demand. */
export const CMT_INSUFFICIENT_DEMAND_INDICATORS = Object.freeze([
  "hiring_volume_alone",
  "headcount_growth_alone",
  "organization_size_alone",
  "generic_corporate_growth_without_meeting_or_training_program",
]);
