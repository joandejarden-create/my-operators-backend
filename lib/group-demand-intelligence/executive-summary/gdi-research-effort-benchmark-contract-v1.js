/**
 * GDI Manual Research Effort Benchmark Contract V1
 *
 * Configurable, versioned activity-specific assumptions for estimating the
 * manual research effort a hotel sales team would otherwise perform to produce
 * equivalent commercial intelligence.
 *
 * Classification: PROVISIONAL until validated by timed manual exercises or
 * credible customer feedback. Never present as measured industry facts.
 *
 * Scope: GDI Research Receipt / Executive Summary only.
 * Future ADP / Hotel Explorer / Deal Opportunities cycles should adopt a
 * product-specific benchmark contract — do not treat this as a cross-product engine.
 *
 * Contract: gdi-research-effort-benchmark-contract-v1
 */

export const GDI_RESEARCH_EFFORT_BENCHMARK_CONTRACT_ID =
  "gdi-research-effort-benchmark-contract-v1";

export const GDI_RESEARCH_EFFORT_BENCHMARK_VERSION = "v1.0.0-provisional";

export const BENCHMARK_VALIDATION_STATUS = Object.freeze({
  PROVISIONAL: "PROVISIONAL",
  VALIDATED_CONSERVATIVE: "VALIDATED_CONSERVATIVE",
  SUPERSEDED: "SUPERSEDED",
});

/** Explicit working-day assumption for customer-facing day conversion. */
export const WORKING_DAY_ASSUMPTION = Object.freeze({
  hoursPerWorkingDay: 8,
  label: "8-hour working day",
  note: "Equivalent working days = estimated research hours saved ÷ 8.",
});

/**
 * Residual hotel research/verification effort retained after Dealality delivery.
 * Excludes ordinary commercial pursuit the hotel would perform regardless.
 * Provisional fraction of equivalent manual hours.
 */
export const RESIDUAL_HOTEL_RESEARCH_FRACTION = Object.freeze({
  base: 0.2,
  low: 0.15,
  high: 0.3,
  validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
  note:
    "Hotel still spends time reviewing, verifying, and preparing outreach. Ordinary pursuit/RFP work is excluded from savings.",
});

/**
 * Activity benchmarks — hours are manual analyst effort per counting unit.
 * low / base / high are provisional ranges.
 */
export const RESEARCH_EFFORT_BENCHMARKS = Object.freeze([
  {
    activityKey: "demand_discovery_and_screening",
    researchActivity: "Demand Discovery And Screening",
    countingUnit: "candidate_opportunity",
    hours: { low: 0.35, base: 0.6, high: 1.0 },
    benchmarkSource:
      "Founder provisional estimate for market scan + screen to hotel-relevant candidate",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Finding New Opportunities",
    salesActivityKey: "finding_new_opportunities",
    dedupeGroup: "discovery",
  },
  {
    activityKey: "deep_opportunity_research",
    researchActivity: "Deep Opportunity Research",
    countingUnit: "opportunity_deep_dossier",
    hours: { low: 1.5, base: 2.5, high: 4.0 },
    benchmarkSource:
      "Founder provisional estimate for organization/event dossier equivalent to GDI deep research",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Researching Organizations And Events",
    salesActivityKey: "researching_organizations_and_events",
    dedupeGroup: "deep",
  },
  {
    activityKey: "competitive_account_discovery",
    researchActivity: "Competitive Account Discovery",
    countingUnit: "competitive_opportunity",
    hours: { low: 0.75, base: 1.25, high: 2.0 },
    benchmarkSource:
      "Founder provisional estimate for competitor overflow / competitive demand scan",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Finding New Opportunities",
    salesActivityKey: "finding_new_opportunities",
    dedupeGroup: "competitive",
  },
  {
    activityKey: "plp_referral_research",
    researchActivity: "PLP Referral Research",
    countingUnit: "plp_opportunity",
    hours: { low: 0.75, base: 1.25, high: 2.0 },
    benchmarkSource:
      "Founder provisional estimate for preferred lodging / referral pathway research",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Evaluating Hotel Fit",
    salesActivityKey: "evaluating_hotel_fit",
    dedupeGroup: "plp",
  },
  {
    activityKey: "leisure_demand_research",
    researchActivity: "Leisure Demand Research",
    countingUnit: "leisure_opportunity",
    hours: { low: 0.75, base: 1.25, high: 2.0 },
    benchmarkSource:
      "Founder provisional estimate for leisure / sports demand diligence",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Researching Organizations And Events",
    salesActivityKey: "researching_organizations_and_events",
    dedupeGroup: "leisure",
  },
  {
    activityKey: "who_how_resolution",
    researchActivity: "WHO/HOW Resolution",
    countingUnit: "opportunity_with_who_how",
    hours: { low: 0.5, base: 0.9, high: 1.5 },
    benchmarkSource:
      "Founder provisional estimate for decision-maker identification per opportunity",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Identifying Decision-Makers",
    salesActivityKey: "identifying_decision_makers",
    dedupeGroup: "who_how",
  },
  {
    activityKey: "alternative_entry_point_research",
    researchActivity: "Alternative Entry-Point Research",
    countingUnit: "opportunity_with_alt_entry",
    hours: { low: 0.35, base: 0.6, high: 1.0 },
    benchmarkSource:
      "Founder provisional estimate for influencer / alternate path research beyond primary buyer",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Identifying Decision-Makers",
    salesActivityKey: "identifying_decision_makers",
    dedupeGroup: "alt_entry",
  },
  {
    activityKey: "professional_contact_verification",
    researchActivity: "Professional Contact Verification",
    countingUnit: "verified_outreach_channel",
    hours: { low: 0.25, base: 0.4, high: 0.75 },
    benchmarkSource:
      "Founder provisional estimate to confirm email / phone / professional channel",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Establishing Professional Contact Channels",
    salesActivityKey: "establishing_professional_contact_channels",
    dedupeGroup: "contact_verify",
  },
  {
    activityKey: "hotel_specific_commercial_qualification",
    researchActivity: "Hotel-Specific Commercial Qualification",
    countingUnit: "qualified_opportunity",
    hours: { low: 0.4, base: 0.75, high: 1.25 },
    benchmarkSource:
      "Founder provisional estimate for hotel-fit / commercial qualification write-up",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Evaluating Hotel Fit",
    salesActivityKey: "evaluating_hotel_fit",
    dedupeGroup: "qualification",
  },
  {
    activityKey: "sales_pursuit_strategy_preparation",
    researchActivity: "Sales Pursuit Strategy Preparation",
    countingUnit: "opportunity_with_sales_action",
    hours: { low: 0.25, base: 0.45, high: 0.75 },
    benchmarkSource:
      "Founder provisional estimate for recommended next-action / pursuit brief (not CRM pursuit itself)",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Evaluating Hotel Fit",
    salesActivityKey: "evaluating_hotel_fit",
    dedupeGroup: "pursuit_prep",
  },
  {
    activityKey: "weekly_research_revalidation",
    researchActivity: "Weekly Research Revalidation",
    countingUnit: "materially_changed_opportunity",
    hours: { low: 0.2, base: 0.35, high: 0.6 },
    benchmarkSource:
      "Founder provisional estimate to re-check changed opportunities in a weekly cycle",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Screening Out Irrelevant Demand",
    salesActivityKey: "screening_out_irrelevant_demand",
    dedupeGroup: "weekly",
    requiresComparableWeeklyHistory: true,
  },
  {
    activityKey: "executive_report_preparation",
    researchActivity: "Executive Report Preparation",
    countingUnit: "research_package_delivery",
    hours: { low: 2.0, base: 3.5, high: 6.0 },
    benchmarkSource:
      "Founder provisional estimate to assemble an equivalent executive commercial briefing manually",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Evaluating Hotel Fit",
    salesActivityKey: "evaluating_hotel_fit",
    dedupeGroup: "report",
  },
  {
    activityKey: "opportunities_screened_out",
    researchActivity: "Opportunities Screened Out",
    countingUnit: "screened_out_opportunity",
    hours: { low: 0.2, base: 0.35, high: 0.6 },
    benchmarkSource:
      "Founder provisional estimate for diligence that protects sales time by rejecting poor-fit demand",
    validationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    effectiveDate: "2026-11-27",
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    customerFacingActivityLabel: "Screening Out Irrelevant Demand",
    salesActivityKey: "screening_out_irrelevant_demand",
    dedupeGroup: "screened_out",
    requiresScreenedOutTelemetry: true,
  },
]);

export function getResearchEffortBenchmark(activityKey) {
  return (
    RESEARCH_EFFORT_BENCHMARKS.find((b) => b.activityKey === activityKey) ||
    null
  );
}

export function listProvisionalBenchmarks() {
  return RESEARCH_EFFORT_BENCHMARKS.filter(
    (b) => b.validationStatus === BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
  );
}

export default {
  GDI_RESEARCH_EFFORT_BENCHMARK_CONTRACT_ID,
  GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
  BENCHMARK_VALIDATION_STATUS,
  WORKING_DAY_ASSUMPTION,
  RESIDUAL_HOTEL_RESEARCH_FRACTION,
  RESEARCH_EFFORT_BENCHMARKS,
  getResearchEffortBenchmark,
  listProvisionalBenchmarks,
};
