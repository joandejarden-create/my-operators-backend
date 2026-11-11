/**
 * Airtable field map — Need-Date Demand Intelligence V3
 * Base: AI Demand Positioning (appa2cE7FTRmIbB32)
 */

export const NEED_DATE_ADP_BASE_ID_DEFAULT = "appa2cE7FTRmIbB32";
export const NEED_DATE_HPC_BASE_ID_DEFAULT = "appCCUsuGsE1ifoLk";
export const NEED_DATE_HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const NEED_DATE_HOTEL_COMMERCIAL_PROFILES_TABLE_ID = "tblBYJtKj6oi4J0Ow";
export const NEED_DATE_GDI_OPPORTUNITIES_TABLE_ID = "tblRuReslJMwsfRQj";

export const TABLE_GDI_HOTEL_NEED_PERIODS = "GDI Hotel Need Periods";
export const TABLE_GDI_NEED_PERIOD_CHANGE_HISTORY =
  "GDI Need Period Change History";
export const TABLE_GDI_NEED_DATE_CAMPAIGN_RUNS = "GDI Need-Date Campaign Runs";
export const TABLE_GDI_NEED_PERIOD_OPPORTUNITY_MATCHES =
  "GDI Need Period Opportunity Matches";

/** Primary + writable fields on GDI Hotel Need Periods */
export const F_NEED_PERIOD = Object.freeze({
  needPeriodId: "Need Period ID",
  hotelId: "Hotel ID",
  hotelCommercialProfile: "Hotel Commercial Profile",
  periodName: "Period Name",
  startDate: "Start Date",
  endDate: "End Date",
  calendarYear: "Calendar Year",
  revenueNeed: "Revenue Need",
  priority: "Priority",
  status: "Status",
  targetRoomNights: "Target Room Nights",
  targetRevenue: "Target Revenue",
  targetMeetings: "Target Meetings",
  minGroupSize: "Minimum Group Size",
  maxGroupSize: "Maximum Group Size",
  preferredDays: "Preferred Days",
  targetMarkets: "Target Markets",
  restrictions: "Restrictions",
  salesNotes: "Sales Notes",
  lastConfirmedByHotel: "Last Confirmed By Hotel",
  lastResearchDate: "Last Research Date",
  nextResearchReview: "Next Research Review",
  researchStatus: "Research Status",
  currentVersion: "Current Version",
  changeReason: "Change Reason",
  supersededBy: "Superseded By",
  relatedNeedPeriods: "Related Need Periods",
  isTestScenario: "Is Test Scenario",
  source: "Source",
  idempotencyKey: "Idempotency Key",
  schemaVersion: "Schema Version",
  periodPreferenceOverrides: "Period Preference Overrides",
});

/** GDI Hotel Commercial Profiles (selection-first portal) — distinct from ADP facility profiles */
export const NEED_DATE_GDI_COMMERCIAL_PROFILES_TABLE_ID = "tbl8KSNUCRbIzEv15";
export const NEED_DATE_GDI_COMMERCIAL_PROFILE_HISTORY_TABLE_ID = "tblrFnRPOoUz6oNCn";


export const F_CHANGE_HISTORY = Object.freeze({
  changeId: "Change ID",
  needPeriod: "Need Period",
  hotelId: "Hotel ID",
  previousVersion: "Previous Version",
  newVersion: "New Version",
  changeType: "Change Type",
  previousValues: "Previous Values",
  newValues: "New Values",
  changedBy: "Changed By",
  changedAt: "Changed At",
  changeReason: "Change Reason",
  researchImpact: "Research Impact",
  correlationId: "Correlation / Request ID",
  schemaVersion: "Schema Version",
});

export const F_CAMPAIGN_RUN = Object.freeze({
  campaignId: "Campaign ID",
  hotelId: "Hotel ID",
  linkedNeedPeriods: "Linked Need Periods",
  researchMode: "Research Mode",
  selectedStrategies: "Selected Strategies",
  researchStatus: "Research Status",
  approvedBudget: "Approved Budget",
  actualSpend: "Actual Spend",
  existingMatches: "Existing Matches",
  enrichedExisting: "Enriched Existing Opportunities",
  newQualified: "New Qualified Opportunities",
  researchContinuation: "Research Continuation",
  duplicatesRejected: "Duplicates Rejected",
  certificationResults: "Certification Results",
  publicationStatus: "Publication Status",
  startedAt: "Started At",
  completedAt: "Completed At",
  errorDetails: "Error Details",
  schemaVersion: "Schema Version",
});

export const F_MATCH = Object.freeze({
  matchKey: "Match Key",
  needPeriod: "Need Period",
  needPeriodId: "Need Period ID",
  opportunityId: "Canonical Opportunity ID",
  gdiOpportunity: "GDI Opportunity",
  matchingEvidence: "Matching Evidence",
  timingOverlap: "Timing Overlap Classification",
  revenueRelevance: "Revenue Relevance",
  matchClass: "Match Classification",
  matchStatus: "Match Status",
  lastVerifiedAt: "Last Verification Date",
  schemaVersion: "Schema Version",
});

export const VAL_PRIORITY = Object.freeze([
  "Critical",
  "High",
  "Medium",
  "Low",
]);

export const VAL_STATUS = Object.freeze([
  "Test Scenario",
  "Active",
  "Partially Filled",
  "Filled",
  "Paused",
  "Reopened",
  "Replaced",
  "Withdrawn",
]);

export const VAL_REVENUE_NEED = Object.freeze([
  "Group guestrooms",
  "Corporate guestrooms",
  "Meetings",
  "Local catering",
  "Banquets",
  "Private dining",
  "Wedding groups",
  "Extended stay",
  "Resort group business",
]);

export const VAL_RESEARCH_STATUS = Object.freeze([
  "Not Researched",
  "Matching Existing Opportunities",
  "Research Recommended",
  "Awaiting Authorization",
  "Researching",
  "Results Available",
  "Research Blocked / Failed",
]);

export const VAL_CHANGE_TYPE = Object.freeze([
  "ADD",
  "MODIFY",
  "SPLIT",
  "PARTIAL",
  "FILLED",
  "CANCEL",
  "REOPEN",
  "REPLACE",
  "PAUSE",
  "PRIORITY_CHANGED",
  "REVENUE_OBJECTIVE_CHANGED",
]);

export const VAL_MATCH_CLASS = Object.freeze([
  "Existing Match",
  "Existing Enriched",
  "New Opportunity",
  "Research Continuation",
]);

export const VAL_PUBLICATION_STATUS = Object.freeze([
  "Not Published",
  "Staging",
  "Published",
  "Blocked",
]);

export const NEED_DATE_SCHEMA_VERSION = "gdi-need-date-demand-intelligence-v3";

/** Map internal enum → Airtable select labels */
export function toAirtablePriority(v) {
  const s = String(v || "").toUpperCase();
  if (s === "CRITICAL") return "Critical";
  if (s === "HIGH") return "High";
  if (s === "LOW") return "Low";
  return "Medium";
}

export function fromAirtablePriority(v) {
  const s = String(v || "");
  if (s === "Critical") return "CRITICAL";
  if (s === "High") return "HIGH";
  if (s === "Low") return "LOW";
  return "MEDIUM";
}

export function toAirtableStatus(v) {
  const s = String(v || "");
  const map = {
    TEST_SCENARIO: "Test Scenario",
    Active: "Active",
    ACTIVE: "Active",
    "Partially Filled": "Partially Filled",
    PARTIALLY_FILLED: "Partially Filled",
    Filled: "Filled",
    FILLED: "Filled",
    Paused: "Paused",
    PAUSED: "Paused",
    REVIEW: "Paused",
    Reopened: "Reopened",
    REOPENED: "Reopened",
    Replaced: "Replaced",
    REPLACED: "Replaced",
    Withdrawn: "Withdrawn",
    Cancelled: "Withdrawn",
    CANCELLED: "Withdrawn",
    Closed: "Filled",
    CLOSED: "Filled",
    Completed: "Filled",
    Draft: "Active",
    DRAFT: "Active",
    Researching: "Active",
    RESEARCHING: "Active",
  };
  return map[s] || map[s.replace(/\s+/g, "_").toUpperCase()] || "Active";
}

export function fromAirtableStatus(v) {
  const s = String(v || "");
  if (s === "Test Scenario") return "TEST_SCENARIO";
  if (s === "Partially Filled") return "Partially Filled";
  if (s === "Withdrawn") return "Cancelled";
  return s || "Active";
}

const REVENUE_TO_AT = {
  GROUP_GUESTROOMS: "Group guestrooms",
  CORPORATE_TRANSIENT_GUESTROOMS: "Corporate guestrooms",
  MEETING_SPACE: "Meetings",
  LOCAL_CATERING: "Local catering",
  BANQUETS: "Banquets",
  PRIVATE_DINING: "Private dining",
  WEDDING_GROUP_GUESTROOMS: "Wedding groups",
  EXTENDED_STAY: "Extended stay",
  RESORT_GROUP: "Resort group business",
};

const REVENUE_FROM_AT = Object.fromEntries(
  Object.entries(REVENUE_TO_AT).map(([k, v]) => [v, k]),
);

export function toAirtableRevenueNeed(list = []) {
  return (list || [])
    .map((x) => REVENUE_TO_AT[x] || (VAL_REVENUE_NEED.includes(x) ? x : null))
    .filter(Boolean);
}

export function fromAirtableRevenueNeed(list = []) {
  return (list || [])
    .map((x) => REVENUE_FROM_AT[x] || null)
    .filter(Boolean);
}

export function toAirtableResearchStatus(v) {
  const map = {
    NOT_RESEARCHED: "Not Researched",
    MATCHING_EXISTING: "Matching Existing Opportunities",
    RESEARCH_RECOMMENDED: "Research Recommended",
    AWAITING_AUTHORIZATION: "Awaiting Authorization",
    RESEARCHING: "Researching",
    RESULTS_AVAILABLE: "Results Available",
    RESEARCH_BLOCKED: "Research Blocked / Failed",
    RESEARCH_FAILED: "Research Blocked / Failed",
  };
  return map[String(v || "")] || String(v || "Not Researched");
}
