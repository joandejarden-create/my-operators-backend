/**
 * GDI Need-Date Demand Intelligence V1 — contract constants.
 *
 * Cross-segment targeted discovery orchestrator (PowerShell workflow #15).
 * NOT a fifteenth canonical demand segment / research engine.
 *
 * Contract: GDI_NEED_DATE_DEMAND_INTELLIGENCE_V1
 * Zero-paid by default. Airtable mutation + paid providers require separate founder GO.
 */

export const NEED_DATE_DEMAND_INTELLIGENCE_V1 =
  "gdi-need-date-demand-intelligence-v1";

/** Dynamic need-period lifecycle + versioning (extends V1; same PowerShell #15). */
export const NEED_DATE_DEMAND_INTELLIGENCE_V2 =
  "gdi-need-date-demand-intelligence-v2";

export const NEED_DATE_POWERSHELL_WORKFLOW_NUMBER = 15;

/** Waterstone Boca Raton — controlled V2 pilot hotel (HPC = GDI hotelId). */
export const WATERSTONE_NEED_DATE_PILOT_HOTEL_ID = "recgMYovrrZDJMqzX";

export const NEED_DATE_MODE = Object.freeze({
  PREVIEW: "preview",
  FULL_E2E: "full-e2e",
  CERTIFY_ONLY: "certify-only",
  WEEKLY_INCREMENTAL: "weekly-incremental",
});

export const PERIOD_GRANULARITY = Object.freeze({
  MONTH: "Month",
  DATE_RANGE: "Date Range",
});

export const NEED_PERIOD_STATUS = Object.freeze({
  DRAFT: "Draft",
  ACTIVE: "Active",
  RESEARCHING: "Researching",
  REVIEW: "Review",
  /** Hotel still needs some but not all of the period / revenue objective. */
  PARTIALLY_FILLED: "Partially Filled",
  /** Commercial gap satisfied — keep history; stop need-date prioritization. */
  FILLED: "Filled",
  COMPLETED: "Completed",
  CLOSED: "Closed",
  /** Withdrawn without claiming the gap was filled. */
  CANCELLED: "Cancelled",
  /** Superseded by another period (REPLACE). */
  REPLACED: "Replaced",
  /** Isolated research test — not a verified hotel occupancy deficiency. */
  TEST_SCENARIO: "TEST_SCENARIO",
});

/**
 * Append-only change types for dynamic need-period lifecycle.
 * Filled ≠ Cancelled ≠ Replaced.
 */
export const NEED_PERIOD_CHANGE_TYPE = Object.freeze({
  ADD: "ADD",
  MODIFY: "MODIFY",
  SPLIT: "SPLIT",
  PARTIALLY_FILLED: "PARTIALLY_FILLED",
  FILLED: "FILLED",
  CLOSED: "CLOSED",
  REOPEN: "REOPEN",
  REPLACE: "REPLACE",
  CANCEL: "CANCEL",
  PRIORITY_CHANGED: "PRIORITY_CHANGED",
  REVENUE_OBJECTIVE_CHANGED: "REVENUE_OBJECTIVE_CHANGED",
});

/** Minimum research response implied by a change (orchestrator guidance). */
export const NEED_PERIOD_RESEARCH_IMPLICATION = Object.freeze({
  MATCH_EXISTING_THEN_GAP: "MATCH_EXISTING_THEN_GAP",
  STOP_PERIOD_DISCOVERY: "STOP_PERIOD_DISCOVERY",
  REPRIORITIZE_GAPS: "REPRIORITIZE_GAPS",
  REACTIVATE_AND_REMATCH: "REACTIVATE_AND_REMATCH",
  SUPERSEDE_AND_ACTIVATE_SUCCESSOR: "SUPERSEDE_AND_ACTIVATE_SUCCESSOR",
  PRESERVE_ONLY: "PRESERVE_ONLY",
  NO_PAID_RESEARCH: "NO_PAID_RESEARCH",
});

/** Hotel revenue need types (guestrooms optional for catering/meetings). */
export const NEED_REVENUE_TYPE = Object.freeze({
  GROUP_GUESTROOMS: "GROUP_GUESTROOMS",
  CORPORATE_TRANSIENT_GUESTROOMS: "CORPORATE_TRANSIENT_GUESTROOMS",
  MEETING_SPACE: "MEETING_SPACE",
  LOCAL_CATERING: "LOCAL_CATERING",
  BANQUETS: "BANQUETS",
  PRIVATE_DINING: "PRIVATE_DINING",
  EXTENDED_STAY: "EXTENDED_STAY",
  WEDDING_GROUP_GUESTROOMS: "WEDDING_GROUP_GUESTROOMS",
  RESORT_GROUP: "RESORT_GROUP",
  OTHER_ANCILLARY: "OTHER_ANCILLARY",
});

export const NEED_REVENUE_TYPE_LABEL = Object.freeze({
  [NEED_REVENUE_TYPE.GROUP_GUESTROOMS]: "Group guestrooms",
  [NEED_REVENUE_TYPE.CORPORATE_TRANSIENT_GUESTROOMS]:
    "Corporate transient guestrooms",
  [NEED_REVENUE_TYPE.MEETING_SPACE]: "Meeting space",
  [NEED_REVENUE_TYPE.LOCAL_CATERING]: "Local catering",
  [NEED_REVENUE_TYPE.BANQUETS]: "Banquets",
  [NEED_REVENUE_TYPE.PRIVATE_DINING]: "Private dining",
  [NEED_REVENUE_TYPE.EXTENDED_STAY]: "Extended-stay accommodation",
  [NEED_REVENUE_TYPE.WEDDING_GROUP_GUESTROOMS]: "Wedding group guestrooms",
  [NEED_REVENUE_TYPE.RESORT_GROUP]: "Resort group business",
  [NEED_REVENUE_TYPE.OTHER_ANCILLARY]: "Other ancillary revenue",
});

/**
 * Timing match vs hotel need period (distinct from universal TIMING_TYPE).
 * Never treat sourcing window alone as demand-date overlap.
 */
export const NEED_TIMING_OVERLAP = Object.freeze({
  CONFIRMED_PERIOD_OVERLAP: "CONFIRMED_PERIOD_OVERLAP",
  SUPPORTED_PROBABLE_OVERLAP: "SUPPORTED_PROBABLE_OVERLAP",
  TIMING_UNCONFIRMED: "TIMING_UNCONFIRMED",
  OUTSIDE_PERIOD: "OUTSIDE_PERIOD",
  ONGOING_ACCOUNT_POTENTIAL: "ONGOING_ACCOUNT_POTENTIAL",
});

export const NEED_TIMING_OVERLAP_LABEL = Object.freeze({
  [NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP]: "Confirmed period overlap",
  [NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP]: "Supported probable overlap",
  [NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED]: "Timing unconfirmed",
  [NEED_TIMING_OVERLAP.OUTSIDE_PERIOD]: "Outside period",
  [NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL]: "Ongoing account potential",
});

/** Demand-date kinds (sourcing ≠ guestroom/meeting revenue timing). */
export const DEMAND_DATE_KIND = Object.freeze({
  EVENT: "EVENT",
  GUEST_ARRIVAL_DEPARTURE: "GUEST_ARRIVAL_DEPARTURE",
  MEETING: "MEETING",
  CATERING: "CATERING",
  PROGRAMME: "PROGRAMME",
  PROJECT_DEPLOYMENT: "PROJECT_DEPLOYMENT",
  SOURCING_PLANNING: "SOURCING_PLANNING",
  UNKNOWN: "UNKNOWN",
});

export const NEED_OUTPUT_CATEGORY = Object.freeze({
  EXISTING_MATCH: "EXISTING_MATCH",
  EXISTING_ENRICHED: "EXISTING_OPPORTUNITY_RESEARCH_ENRICHED",
  NEW_UNIQUE: "NEW_UNIQUE_OPPORTUNITY",
});

export const NEED_OUTPUT_CATEGORY_LABEL = Object.freeze({
  [NEED_OUTPUT_CATEGORY.EXISTING_MATCH]: "Existing match",
  [NEED_OUTPUT_CATEGORY.EXISTING_ENRICHED]: "Existing opportunity — research enriched",
  [NEED_OUTPUT_CATEGORY.NEW_UNIQUE]: "New unique opportunity",
});

/** Permanent certification gates (PASS / FAIL / NOT_VERIFIED). */
export const NEED_DATE_CERT_GATE = Object.freeze({
  NEED_PERIOD_IDENTITY_PASS: "NEED_PERIOD_IDENTITY_PASS",
  NEED_PERIOD_DATE_VALIDATION_PASS: "NEED_PERIOD_DATE_VALIDATION_PASS",
  TIMING_MATCH_EVIDENCE_PASS: "TIMING_MATCH_EVIDENCE_PASS",
  EXISTING_INVENTORY_MATCH_PASS: "EXISTING_INVENTORY_MATCH_PASS",
  CROSS_WORKFLOW_DEDUP_PASS: "CROSS_WORKFLOW_DEDUP_PASS",
  REVENUE_TYPE_MATCH_PASS: "REVENUE_TYPE_MATCH_PASS",
  BUYER_ELIGIBILITY_PASS: "BUYER_ELIGIBILITY_PASS",
  RESEARCH_PRESERVATION_PASS: "RESEARCH_PRESERVATION_PASS",
  GOLD_DRAWER_PASS: "GOLD_DRAWER_PASS",
  BROWSER_PASS: "BROWSER_PASS",
  BUDGET_PASS: "BUDGET_PASS",
  PUBLICATION_GATE_PASS: "PUBLICATION_GATE_PASS",
});

export const CERT_OUTCOME = Object.freeze({
  PASS: "PASS",
  FAIL: "FAIL",
  NOT_VERIFIED: "NOT_VERIFIED",
});

/**
 * PowerShell #1–14 research strategies (selective; never launch all blindly).
 * Maps to existing Invoke-Gdi*.ps1 entry points / shared research modules.
 */
export const NEED_DATE_RESEARCH_STRATEGY = Object.freeze({
  ASSOCIATION_EVENTS: "association_events",
  SCALING_COMPANIES: "scaling_companies",
  MEETINGS_INCENTIVES: "meetings_incentives",
  LOCAL_CATERING_DAY_MEETINGS: "local_catering_day_meetings",
  EDUCATION_SCIENTIFIC: "education_scientific",
  GOVERNMENT_PUBLIC_SECTOR: "government_public_sector",
  SPORTS_ENTERTAINMENT: "sports_entertainment",
  SOCIAL_SPECIAL_EVENTS: "social_special_events",
  TRAVEL_TRADE: "travel_trade",
  PROJECTS_INFRASTRUCTURE: "projects_infrastructure",
  RELOCATION_EXTENDED_STAY: "relocation_extended_stay",
  CORPORATE_CELEBRATIONS: "corporate_celebrations",
  PREFERRED_LODGING: "preferred_lodging_partnerships",
  PROFESSIONAL_EVENT_PLANNERS: "professional_event_planners",
});

export const NEED_DATE_STRATEGY_META = Object.freeze({
  [NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS]: {
    powershell: "Invoke-GdiAssociationEvents.ps1",
    number: 1,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.BANQUETS,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES]: {
    powershell: "Invoke-GdiScalingCompanies.ps1",
    number: 2,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.CORPORATE_TRANSIENT_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.EXTENDED_STAY,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES]: {
    powershell: "Invoke-GdiMeetingsIncentives.ps1",
    number: 3,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.BANQUETS,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.LOCAL_CATERING_DAY_MEETINGS]: {
    powershell: "Invoke-GdiLocalCateringDayMeetings.ps1",
    number: 4,
    revenueFit: [
      NEED_REVENUE_TYPE.LOCAL_CATERING,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.PRIVATE_DINING,
      NEED_REVENUE_TYPE.BANQUETS,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.EDUCATION_SCIENTIFIC]: {
    powershell: "Invoke-GdiEducationScientific.ps1",
    number: 5,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.GOVERNMENT_PUBLIC_SECTOR]: {
    powershell: "Invoke-GdiGovernmentPublicSector.ps1",
    number: 6,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.SPORTS_ENTERTAINMENT]: {
    powershell: "Invoke-GdiSportsEntertainment.ps1",
    number: 7,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS]: {
    powershell: "Invoke-GdiSocialSpecialEvents.ps1",
    number: 8,
    revenueFit: [
      NEED_REVENUE_TYPE.WEDDING_GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.BANQUETS,
      NEED_REVENUE_TYPE.PRIVATE_DINING,
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.TRAVEL_TRADE]: {
    powershell: "Invoke-GdiTravelTradeIntermediaries.ps1",
    number: 9,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.RESORT_GROUP,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.PROJECTS_INFRASTRUCTURE]: {
    powershell: "Invoke-GdiProjectsInfrastructure.ps1",
    number: 10,
    revenueFit: [
      NEED_REVENUE_TYPE.EXTENDED_STAY,
      NEED_REVENUE_TYPE.CORPORATE_TRANSIENT_GUESTROOMS,
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.RELOCATION_EXTENDED_STAY]: {
    powershell: "Invoke-GdiRelocationExtendedStay.ps1",
    number: 11,
    revenueFit: [NEED_REVENUE_TYPE.EXTENDED_STAY],
  },
  [NEED_DATE_RESEARCH_STRATEGY.CORPORATE_CELEBRATIONS]: {
    powershell: "Invoke-GdiCorporateCelebrations.ps1",
    number: 12,
    revenueFit: [
      NEED_REVENUE_TYPE.PRIVATE_DINING,
      NEED_REVENUE_TYPE.BANQUETS,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
      NEED_REVENUE_TYPE.MEETING_SPACE,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.PREFERRED_LODGING]: {
    powershell: "Invoke-GdiPreferredLodgingPartnerships.ps1",
    number: 13,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.RESORT_GROUP,
    ],
  },
  [NEED_DATE_RESEARCH_STRATEGY.PROFESSIONAL_EVENT_PLANNERS]: {
    powershell: "Invoke-GdiProfessionalEventPlanners.ps1",
    number: 14,
    revenueFit: [
      NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      NEED_REVENUE_TYPE.MEETING_SPACE,
      NEED_REVENUE_TYPE.LOCAL_CATERING,
      NEED_REVENUE_TYPE.BANQUETS,
    ],
  },
});

/** Customer filter key — scoped, not a permanent fourth tile pill. */
export const NEED_DATE_CUSTOMER_FILTER = Object.freeze({
  MATCHES_NEED_DATES: "MATCHES_NEED_DATES",
});

export const NEED_DATE_CUSTOMER_FILTER_LABEL = Object.freeze({
  [NEED_DATE_CUSTOMER_FILTER.MATCHES_NEED_DATES]: "Matches Need Dates",
});

export const AIRTABLE_MUTATION_BLOCKED = Object.freeze({
  code: "NEED_DATE_AIRTABLE_MUTATION_BLOCKED",
  message:
    "Airtable table/field creation and production writes require separate explicit founder authorization.",
});

export const PAID_PROVIDER_BLOCKED = Object.freeze({
  code: "NEED_DATE_PAID_PROVIDER_BLOCKED",
  message:
    "Live Exa / Firecrawl / Surfe / paid mapping requires separate founder spend authorization. No prior research balances may be reused.",
});
