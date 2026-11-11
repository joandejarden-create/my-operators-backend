/**
 * GDI Need-Date Deep Discovery V6 — contract.
 *
 * Philosophy: research depth and commercial opportunity yield first.
 * Budget discipline remains mandatory; arbitrary low allocations must not
 * undermine discovery quality. Zero-paid by default — paid Exa requires
 * separate founder authorization.
 *
 * PowerShell #15 = orchestrator only (not a 15th demand segment).
 * Canonical demand segments = 12; research workflows = 14 (#1–14).
 */

export const NEED_DATE_DEEP_DISCOVERY_V6 = "gdi-need-date-deep-discovery-v6";

export const NEED_DATE_V6_SUCCESS = Object.freeze({
  targetQualifiedMin: 5,
  targetQualifiedMax: 10,
  note: "Discovery objective — not a publication quota. Do not relax standards to hit the target.",
  evaluatePerNeedMonth: true,
});

/** Multi-wave investigation lifecycle (durable). */
export const NEED_DATE_V6_WAVE = Object.freeze({
  WAVE_A_EXISTING_INTELLIGENCE: "WAVE_A_EXISTING_INTELLIGENCE",
  WAVE_B_BROAD_DIVERSIFIED_DISCOVERY: "WAVE_B_BROAD_DIVERSIFIED_DISCOVERY",
  WAVE_C_CANDIDATE_QUALIFICATION: "WAVE_C_CANDIDATE_QUALIFICATION",
  WAVE_D_DEEP_RESEARCH: "WAVE_D_DEEP_RESEARCH",
  WAVE_E_ADAPTIVE_GAP_RESOLUTION: "WAVE_E_ADAPTIVE_GAP_RESOLUTION",
  WAVE_F_COMMERCIAL_CERTIFICATION: "WAVE_F_COMMERCIAL_CERTIFICATION",
  WAVE_G_COVERAGE_ASSESSMENT: "WAVE_G_COVERAGE_ASSESSMENT",
});

export const NEED_DATE_V6_WAVE_META = Object.freeze({
  [NEED_DATE_V6_WAVE.WAVE_A_EXISTING_INTELLIGENCE]: {
    paid: false,
    objective:
      "Review hotel GDI inventory, research memory, known accounts, historical candidates and prior Need-Date investigations for need-period matches.",
  },
  [NEED_DATE_V6_WAVE.WAVE_B_BROAD_DIVERSIFIED_DISCOVERY]: {
    paid: true,
    requiresFounderApproval: true,
    objective:
      "Diversified discovery across PRIMARY/SECONDARY workflows — organizations, programmes, planners, venues and buyer-owned demand; not conference calendars alone.",
  },
  [NEED_DATE_V6_WAVE.WAVE_C_CANDIDATE_QUALIFICATION]: {
    paid: false,
    objective:
      "Screen for verified org/programme, activity, timing, hotel fit, accessibility, buyer/referral — reject thin source pages early.",
  },
  [NEED_DATE_V6_WAVE.WAVE_D_DEEP_RESEARCH]: {
    paid: true,
    requiresFounderApproval: true,
    objective:
      "Exa Deep Research on promising candidates; allow follow-up when material questions remain unresolved.",
  },
  [NEED_DATE_V6_WAVE.WAVE_E_ADAPTIVE_GAP_RESOLUTION]: {
    paid: true,
    requiresFounderApproval: true,
    objective:
      "Resolve blocker-ledger gaps with specific research questions; reuse saved findings; no identical query burns.",
  },
  [NEED_DATE_V6_WAVE.WAVE_F_COMMERCIAL_CERTIFICATION]: {
    paid: false,
    objective:
      "Confirm evidence, identity, buyer relevance, accessibility and actionable next steps before staging.",
  },
  [NEED_DATE_V6_WAVE.WAVE_G_COVERAGE_ASSESSMENT]: {
    paid: false,
    objective:
      "Assess unmet months, underresearched segments and unresolved high-potential opportunities; recommend targeted follow-up only when justified.",
  },
});

/** Workflow priority classes for campaign coverage plans. */
export const NEED_DATE_V6_WORKFLOW_PRIORITY = Object.freeze({
  PRIMARY: "PRIMARY",
  SECONDARY: "SECONDARY",
  CONDITIONAL: "CONDITIONAL",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

/** Shared commercial-accessibility classifications (V6). */
export const NEED_DATE_V6_ACCESSIBILITY = Object.freeze({
  DIRECT_ADDRESSABLE_DEMAND: "DIRECT_ADDRESSABLE_DEMAND",
  CREDIBLY_CONTESTABLE_SOURCING: "CREDIBLY_CONTESTABLE_SOURCING",
  VERIFIED_PARTNERSHIP_REFERRAL: "VERIFIED_PARTNERSHIP_REFERRAL",
  EVIDENCED_OVERFLOW: "EVIDENCED_OVERFLOW",
  TIMING_UNCONFIRMED_RELATIONSHIP: "TIMING_UNCONFIRMED_RELATIONSHIP",
  SPECULATIVE_OVERFLOW: "SPECULATIVE_OVERFLOW",
  COMPETITOR_COMMITTED: "COMPETITOR_COMMITTED",
});

/** Sales-ready eligible accessibility classes. */
export const NEED_DATE_V6_SALES_READY_ACCESSIBILITY = Object.freeze([
  NEED_DATE_V6_ACCESSIBILITY.DIRECT_ADDRESSABLE_DEMAND,
  NEED_DATE_V6_ACCESSIBILITY.CREDIBLY_CONTESTABLE_SOURCING,
  NEED_DATE_V6_ACCESSIBILITY.VERIFIED_PARTNERSHIP_REFERRAL,
  NEED_DATE_V6_ACCESSIBILITY.EVIDENCED_OVERFLOW,
]);

/** Research Continuation eligible (not verified need-month yield alone). */
export const NEED_DATE_V6_CONTINUATION_ACCESSIBILITY = Object.freeze([
  NEED_DATE_V6_ACCESSIBILITY.TIMING_UNCONFIRMED_RELATIONSHIP,
  NEED_DATE_V6_ACCESSIBILITY.CREDIBLY_CONTESTABLE_SOURCING,
  NEED_DATE_V6_ACCESSIBILITY.VERIFIED_PARTNERSHIP_REFERRAL,
]);

/** Never sales-ready without new evidence. */
export const NEED_DATE_V6_WATCH_ONLY_ACCESSIBILITY = Object.freeze([
  NEED_DATE_V6_ACCESSIBILITY.SPECULATIVE_OVERFLOW,
  NEED_DATE_V6_ACCESSIBILITY.COMPETITOR_COMMITTED,
]);

export const NEED_DATE_V6_BLOCKER = Object.freeze({
  BUYER_NOT_VERIFIED: "BUYER_NOT_VERIFIED",
  DEMAND_TIMING_UNKNOWN: "DEMAND_TIMING_UNKNOWN",
  VENUE_SOURCING_UNCLEAR: "VENUE_SOURCING_UNCLEAR",
  HOTEL_FIT_INCOMPLETE: "HOTEL_FIT_INCOMPLETE",
  GROUP_LODGING_MECHANISM_UNCLEAR: "GROUP_LODGING_MECHANISM_UNCLEAR",
  EVENT_EDITION_UNCONFIRMED: "EVENT_EDITION_UNCONFIRMED",
  COMPETING_HOTEL_COMMITMENT_UNRESOLVED: "COMPETING_HOTEL_COMMITMENT_UNRESOLVED",
  DEMAND_OWNER_UNCLEAR: "DEMAND_OWNER_UNCLEAR",
  ACCESSIBILITY_UNCLASSIFIED: "ACCESSIBILITY_UNCLASSIFIED",
  THIN_SOURCE_ONLY: "THIN_SOURCE_ONLY",
});

export const NEED_DATE_V6_CERT_GATE = Object.freeze({
  ALL_WORKFLOW_RELEVANCE_COVERAGE: "ALL_WORKFLOW_RELEVANCE_COVERAGE",
  ACTUAL_DISCOVERY_DEPTH: "ACTUAL_DISCOVERY_DEPTH",
  RESEARCH_STRATEGY_DIVERSITY: "RESEARCH_STRATEGY_DIVERSITY",
  WEDDING_SPECIFIC_RESEARCH: "WEDDING_SPECIFIC_RESEARCH",
  NAMED_BUYER_VERIFICATION: "NAMED_BUYER_VERIFICATION",
  COMMERCIAL_ACCESSIBILITY: "COMMERCIAL_ACCESSIBILITY",
  EVIDENCE_SUPPORTED_TIMING: "EVIDENCE_SUPPORTED_TIMING",
  EXISTING_INVENTORY_DEDUPLICATION: "EXISTING_INVENTORY_DEDUPLICATION",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  DEEP_RESEARCH_OUTPUT_COMPLETENESS: "DEEP_RESEARCH_OUTPUT_COMPLETENESS",
  RESEARCH_TO_CIB_PRESERVATION: "RESEARCH_TO_CIB_PRESERVATION",
  MONTH_SPECIFIC_COMMERCIAL_YIELD: "MONTH_SPECIFIC_COMMERCIAL_YIELD",
  BUDGET_SAFETY: "BUDGET_SAFETY",
  PUBLICATION_AUTHORIZATION: "PUBLICATION_AUTHORIZATION",
});

export const NEED_DATE_V6_QUALIFIED_STANDARDS = Object.freeze([
  "Verified demand owner",
  "Evidence-grounded business activity",
  "Relevant demand timing OR clearly defined timing investigation (Continuation)",
  "Realistic hotel-specific revenue thesis",
  "Commercial accessibility (sales-ready class or explicit Continuation)",
  "Relevant named buyer or credible referral",
  "Practical next sales action",
  "Source-backed research",
  "Full V2 CIB + ten-section drawer when staged/published",
  "Contact research completeness tracked (name alone ≠ PASS)",
  "Account-level contact reuse without collapsing distinct demand mechanisms",
  "Hotel-identity integrity (no foreign-hotel sales copy)",
]);

/** Binding weekly commercial standards (14 workflows + PowerShell #15). */
export const NEED_DATE_V6_WEEKLY_COMMERCIAL_STANDARDS_MODULE =
  "lib/group-demand-intelligence/research/weekly-research-commercial-standards-v1.js";

/**
 * Structural drawer certification must never override commercial eligibility.
 * Every FullE2E / Need-Date / weekly run must distinguish these layers:
 */
export const NEED_DATE_V6_READINESS_LAYERS = Object.freeze({
  RESEARCH_COMPLETE: "researchComplete",
  CONTACT_RESEARCH_COMPLETE: "contactResearchComplete",
  COMMERCIAL_QUALIFICATION_COMPLETE: "commercialQualificationComplete",
  CUSTOMER_READY: "customerReady",
  MODULE:
    "lib/group-demand-intelligence/commercial-customer-readiness-gate-v1.js",
  rule: "Ten-section drawer PASS ≠ customer-ready / sales-ready",
});

export const WATERSTONE_V6_HOTEL = Object.freeze({
  hotelId: "recgMYovrrZDJMqzX",
  name: "Waterstone Resort & Marina Boca Raton",
  needMonths: ["2027-08", "2027-09", "2027-10"],
  priorCampaignSpentUsd: 6.9,
  priorCampaignRemainingUsd: 11.1,
  priorAggregateCeilingUsd: 18,
  priorStagedCompetitorAssociated: 3,
  inventoryBaseline: 45,
  weddingResearchStatus: "NOT_RESEARCHED",
});
