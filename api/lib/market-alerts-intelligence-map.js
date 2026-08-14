/**
 * Central Airtable field map for Market Alerts actionable intelligence (V1).
 * Enrichment lives on the same MarketAlerts row — no separate events table.
 */

export const MAP_INTEL = {
  eventType: "Event Type",
  whatChanged: "What Changed",
  hotelProject: "Hotel / Project",
  ownerDeveloper: "Owner / Developer",
  brandInvolved: "Brand Involved",
  operatorInvolved: "Operator Involved",
  rooms: "Rooms",
  assetProjectStage: "Asset / Project Stage",
  entityKey: "Entity Key",
  intelligenceStatus: "Intelligence Status",
  intelligenceTreatment: "Intelligence Treatment",
  intelligenceUpdatedAt: "Intelligence Updated At",
  worthReviewingOwner: "Worth Reviewing — Owner",
  worthReviewingBrand: "Worth Reviewing — Brand",
  worthReviewingOperator: "Worth Reviewing — Operator",
  signalTypeOwner: "Signal Type — Owner",
  signalTypeBrand: "Signal Type — Brand",
  signalTypeOperator: "Signal Type — Operator",
  decisionStageOwner: "Decision Stage — Owner",
  decisionStageBrand: "Decision Stage — Brand",
  decisionStageOperator: "Decision Stage — Operator",
  whyItMattersOwner: "Why It Matters — Owner",
  whyItMattersBrand: "Why It Matters — Brand",
  whyItMattersOperator: "Why It Matters — Operator",
  recommendedActionOwner: "Recommended Action — Owner",
  recommendedActionBrand: "Recommended Action — Brand",
  recommendedActionOperator: "Recommended Action — Operator",
  actionableOwner: "Actionable — Owner",
  actionableBrand: "Actionable — Brand",
  actionableOperator: "Actionable — Operator",
  signalTiming: "Signal Timing",
  projectDirection: "Project Direction",
};

export const EVENT_TYPES = [
  "Hotel For Sale",
  "Acquisition",
  "Sale",
  "Portfolio Acquisition",
  "JV",
  "Recapitalization",
  "Distress",
  "Site Acquisition",
  "Planning Application",
  "Development Proposal",
  "Adaptive Reuse Proposal",
  "New Development",
  "Planning Approval",
  "Construction Start",
  "Brand Signing",
  "Reflag",
  "Conversion",
  "Brand Exit",
  "Operator Appointment",
  "Operator Change",
  "Operator Exit",
  "Management Agreement",
  "Financing",
  "Refinancing",
  "Major Renovation",
  "Repositioning",
];

export const ASSET_PROJECT_STAGES = [
  "Planning",
  "Construction",
  "Operating",
  "Distressed",
  "Unknown",
];

export const INTELLIGENCE_STATUSES = ["Pending", "Ready", "Skipped", "Error"];
export const INTELLIGENCE_TREATMENTS = ["STANDARD", "REVIEW", "IGNORE"];
export const DECISION_STAGES = ["Early", "Forming", "Active", "Likely Decided"];

export const SIGNAL_TYPES_OWNER = [
  "Competitive Change",
  "Strategic Market Change",
  "Capital / Transaction Signal",
  "Repositioning Activity",
  "New Competitive Supply",
];

export const SIGNAL_TYPES_BRAND = [
  "Potential Conversion Opportunity",
  "Potential Development Opportunity",
  "Brand White-Space Signal",
  "Owner Expansion Signal",
  "Reflag Opportunity",
  "Competitive Brand Move",
];

export const SIGNAL_TYPES_OPERATOR = [
  "Potential Management Opportunity",
  "New Development Opportunity",
  "Operator Review Signal",
  "Owner Expansion Signal",
  "Turnaround / Repositioning Opportunity",
  "Competitive Operator Move",
  "Management Agreement Announced",
];

function singleSelect(choices) {
  return {
    type: "singleSelect",
    options: {
      choices: choices.map((name) => ({ name })),
    },
  };
}

function checkboxField(description) {
  return {
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
    description,
  };
}

/** Field specs for Meta API ensure script. */
export function buildMarketAlertsIntelligenceFieldSpecs() {
  return [
    {
      name: MAP_INTEL.eventType,
      type: "singleSelect",
      options: { choices: EVENT_TYPES.map((name) => ({ name })) },
      description: "Deterministic commercial event type",
    },
    {
      name: MAP_INTEL.whatChanged,
      type: "singleLineText",
      description: "Short factual change statement",
    },
    { name: MAP_INTEL.hotelProject, type: "singleLineText" },
    { name: MAP_INTEL.ownerDeveloper, type: "singleLineText" },
    { name: MAP_INTEL.brandInvolved, type: "singleLineText" },
    { name: MAP_INTEL.operatorInvolved, type: "singleLineText" },
    { name: MAP_INTEL.rooms, type: "number", options: { precision: 0 } },
    {
      name: MAP_INTEL.assetProjectStage,
      ...singleSelect(ASSET_PROJECT_STAGES),
    },
    {
      name: MAP_INTEL.entityKey,
      type: "singleLineText",
      description: "Stable key for cross-alert correlation",
    },
    {
      name: MAP_INTEL.intelligenceStatus,
      ...singleSelect(INTELLIGENCE_STATUSES),
    },
    {
      name: MAP_INTEL.intelligenceTreatment,
      ...singleSelect(INTELLIGENCE_TREATMENTS),
    },
    {
      name: MAP_INTEL.intelligenceUpdatedAt,
      type: "dateTime",
      options: {
        dateFormat: { name: "iso" },
        timeFormat: { name: "24hour" },
        timeZone: "utc",
      },
    },
    {
      name: MAP_INTEL.worthReviewingOwner,
      ...checkboxField("Worth reviewing for Owner audience"),
    },
    {
      name: MAP_INTEL.worthReviewingBrand,
      ...checkboxField("Worth reviewing for Brand audience"),
    },
    {
      name: MAP_INTEL.worthReviewingOperator,
      ...checkboxField("Worth reviewing for Operator audience"),
    },
    {
      name: MAP_INTEL.signalTypeOwner,
      ...singleSelect(SIGNAL_TYPES_OWNER),
    },
    {
      name: MAP_INTEL.signalTypeBrand,
      ...singleSelect(SIGNAL_TYPES_BRAND),
    },
    {
      name: MAP_INTEL.signalTypeOperator,
      ...singleSelect(SIGNAL_TYPES_OPERATOR),
    },
    {
      name: MAP_INTEL.decisionStageOwner,
      ...singleSelect(DECISION_STAGES),
    },
    {
      name: MAP_INTEL.decisionStageBrand,
      ...singleSelect(DECISION_STAGES),
    },
    {
      name: MAP_INTEL.decisionStageOperator,
      ...singleSelect(DECISION_STAGES),
    },
    { name: MAP_INTEL.whyItMattersOwner, type: "multilineText" },
    { name: MAP_INTEL.whyItMattersBrand, type: "multilineText" },
    { name: MAP_INTEL.whyItMattersOperator, type: "multilineText" },
    { name: MAP_INTEL.recommendedActionOwner, type: "multilineText" },
    { name: MAP_INTEL.recommendedActionBrand, type: "multilineText" },
    { name: MAP_INTEL.recommendedActionOperator, type: "multilineText" },
    {
      name: MAP_INTEL.actionableOwner,
      ...checkboxField("Actionable open decision window for Owner audience"),
    },
    {
      name: MAP_INTEL.actionableBrand,
      ...checkboxField("Actionable open decision window for Brand audience"),
    },
    {
      name: MAP_INTEL.actionableOperator,
      ...checkboxField("Actionable open decision window for Operator audience"),
    },
    {
      name: MAP_INTEL.signalTiming,
      type: "singleSelect",
      options: {
        choices: [
          { name: "Pre-Decision" },
          { name: "Decision Forming" },
          { name: "Decision Announced" },
          { name: "Post-Decision" },
        ],
      },
    },
    {
      name: MAP_INTEL.projectDirection,
      type: "singleSelect",
      options: {
        choices: [
          { name: "Advancing" },
          { name: "Under Review" },
          { name: "Challenged" },
          { name: "Delayed" },
          { name: "Rejected / Blocked" },
        ],
      },
    },
  ];
}

export function audienceActionableField(audience) {
  if (audience === "owner") return MAP_INTEL.actionableOwner;
  if (audience === "brand") return MAP_INTEL.actionableBrand;
  if (audience === "operator") return MAP_INTEL.actionableOperator;
  return null;
}

export function audienceWorthField(audience) {
  if (audience === "owner") return MAP_INTEL.worthReviewingOwner;
  if (audience === "brand") return MAP_INTEL.worthReviewingBrand;
  if (audience === "operator") return MAP_INTEL.worthReviewingOperator;
  return null;
}
