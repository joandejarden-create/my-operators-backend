/**
 * Scout Opportunity Signals watchlist — Airtable table + field names (Deal Capture Platform).
 * Writes only to this table; never Hotel Census.
 */

export const SCOUT_OPPORTUNITY_SIGNALS_TABLE =
  process.env.SCOUT_OPPORTUNITY_SIGNALS_TABLE || "Scout Opportunity Signals";

export const SIGNAL_TYPE_OPTIONS = [
  "parent_company_market_gap",
  "brand_market_gap",
  "independent_conversion_cluster",
  "large_independent_asset",
  "pipeline_activity",
  "rebrand_candidate",
  "operator_opportunity_market",
];

export const CONFIDENCE_OPTIONS = ["High", "Medium", "Low"];
export const ACTIONABILITY_OPTIONS = ["High", "Medium", "Low"];

export const REVIEW_STATUS_OPTIONS = [
  "New",
  "Watchlist",
  "Researching",
  "Ready for Outreach",
  "Dismissed",
  "Deal Created",
];

export const WATCHLIST_FIELDS = {
  signalId: "Signal ID",
  signalType: "Signal Type",
  signalTitle: "Signal Title",
  country: "Country",
  market: "Market",
  submarket: "Submarket",
  city: "City",
  linkedHotelCensusRecordId: "Linked Hotel Census Record ID",
  hotelName: "Hotel Name",
  parentCompany: "Parent Company",
  brand: "Brand",
  priorityScore: "Priority Score",
  confidence: "Confidence",
  actionability: "Actionability",
  reason: "Reason",
  supportingMetricsJson: "Supporting Metrics JSON",
  recommendedAction: "Recommended Action",
  reviewStatus: "Review Status",
  assignedTo: "Assigned To",
  internalNotes: "Internal Notes",
  source: "Source",
  generatedAt: "Generated At",
  lastReviewedAt: "Last Reviewed At",
  createDeal: "Create Deal?",
};

/** Field specs for Metadata API table creation / ensure script. */
export function watchlistFieldSpecs() {
  const singleSelect = (name, optionNames) => ({
    name,
    type: "singleSelect",
    options: { choices: optionNames.map((n) => ({ name: n })) },
  });

  const dateTime = (name) => ({
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  });

  return [
    { name: WATCHLIST_FIELDS.signalId, type: "singleLineText" },
    singleSelect(WATCHLIST_FIELDS.signalType, SIGNAL_TYPE_OPTIONS),
    { name: WATCHLIST_FIELDS.signalTitle, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.country, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.market, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.submarket, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.city, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.linkedHotelCensusRecordId, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.hotelName, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.parentCompany, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.brand, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.priorityScore, type: "number", options: { precision: 0 } },
    singleSelect(WATCHLIST_FIELDS.confidence, CONFIDENCE_OPTIONS),
    singleSelect(WATCHLIST_FIELDS.actionability, ACTIONABILITY_OPTIONS),
    { name: WATCHLIST_FIELDS.reason, type: "multilineText" },
    { name: WATCHLIST_FIELDS.supportingMetricsJson, type: "multilineText" },
    { name: WATCHLIST_FIELDS.recommendedAction, type: "multilineText" },
    singleSelect(WATCHLIST_FIELDS.reviewStatus, REVIEW_STATUS_OPTIONS),
    { name: WATCHLIST_FIELDS.assignedTo, type: "singleLineText" },
    { name: WATCHLIST_FIELDS.internalNotes, type: "multilineText" },
    { name: WATCHLIST_FIELDS.source, type: "singleLineText" },
    dateTime(WATCHLIST_FIELDS.generatedAt),
    dateTime(WATCHLIST_FIELDS.lastReviewedAt),
    {
      name: WATCHLIST_FIELDS.createDeal,
      type: "checkbox",
      options: { icon: "check", color: "greenBright" },
    },
  ];
}
