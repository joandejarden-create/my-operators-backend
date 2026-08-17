/**
 * Pilot Target List — Airtable field mapping (GTM internal base).
 *
 * Table: AIRTABLE_GTM_PILOT_TARGET_LIST_TABLE or "Pilot Target List"
 * Base: AIRTABLE_GTM_BASE_ID (same as GTM Owner Targets base)
 *
 * Contact-level curated pilot outreach list — not CoStar rollup (Owner Targets).
 */

export const GTM_PILOT_TARGET_LIST_TABLE =
  process.env.AIRTABLE_GTM_PILOT_TARGET_LIST_TABLE || "Pilot Target List";

/** Existing + new outreach fields — Airtable column names. */
export const MAP_PILOT_TARGET_LIST = {
  // Identity / segmentation (existing)
  name: "Name",
  company: "Company",
  role: "Role",
  category: "Category",
  region: "Region",
  pilotRegion: "Pilot Region",
  whyTheyMatter: "Why They Matter",
  warmIntro: "Warm Intro?",
  status: "Status",
  relationshipStrength: "Relationship Strength",
  pilotRelevance: "Pilot Relevance",
  likelyContribution: "Likely Contribution",
  nextAction: "Next Action",
  lastContactDate: "Last Contact Date",
  nextFollowUpDate: "Next Follow-Up Date",
  outreachMessageAngle: "Outreach Message Angle",
  priority: "Priority",

  // Contact channels (may be missing on base — setup script adds)
  email: "Email",
  linkedInUrl: "LinkedIn URL",
  notes: "Notes",

  // New outreach segmentation
  outreachSegment: "Outreach Segment",
  pilotFit: "Pilot Fit",

  // Messaging (messageAngle alias documented in PILOT_FIELD_EQUIVALENTS)
  messageAngle: "Message Angle",
  personalizationLine: "Personalization Line",
  emailSubject: "Email Subject",
  emailDraft: "Email Draft",
  finalApprovedEmail: "Final Approved Email",
  linkedInDmDraft: "LinkedIn DM Draft",
  followUpDraft: "Follow-Up Draft",

  // Workflow
  outreachStatus: "Outreach Status",
  lastContactedDate: "Last Contacted Date",
  replyNotes: "Reply Notes",

  // Mail merge
  readyForMailMerge: "Ready for Mail Merge",
  mailMergeBatch: "Mail Merge Batch",
  sendChannel: "Send Channel",

  // Optional guardrails
  introSource: "Intro Source",
  warmIntroContact: "Warm Intro Contact",
  doNotContact: "Do Not Contact",
  doNotContactReason: "Do Not Contact Reason",
};

/** Recommended Outreach Segment options (pilot wave). */
export const VAL_PILOT_OUTREACH_SEGMENT = [
  "Owner / Investor",
  "Advisor / Consultant / Broker",
  "Operator",
  "Brand / Referral Source",
  "Capital Partner",
  "Other",
];

export const VAL_PILOT_REGION = [
  "CALA",
  "Mexico",
  "Caribbean",
  "Central America",
  "South America",
  "Latin America",
  "United States / Canada",
  "Europe / Spain",
  "Global / Multi-Region",
  "Other",
  "Unknown / TBD",
];

/**
 * Region (multi-select) is broader geographic context.
 * Pilot Region is the primary dropdown for pilot targeting.
 */
export const VAL_PILOT_REGION_CONTEXT = [
  "Caribbean & Latin America",
  "Canada",
  "United States",
  "Europe",
  "Middle East & Africa",
  "Asia Pacific",
  "Global / Multi-Region",
  "Unknown / TBD",
  "South America",
  "Central America",
  "Caribbean",
  "Mexico",
];

export const VAL_CALA_FIRST_PILOT_REGIONS = [
  "CALA",
  "Mexico",
  "Caribbean",
  "Central America",
  "South America",
  "Latin America",
];

export const VAL_NON_CALA_FEEDBACK_REGIONS = [
  "United States / Canada",
  "Europe / Spain",
  "Global / Multi-Region",
  "Other",
];

export const VAL_PILOT_FIT = [
  "Strong Pilot Candidate",
  "Possible Pilot Candidate",
  "Feedback / Referral Only",
  "Follow-Up Later",
  "Weak Fit",
  "Not A Fit",
];

export const VAL_PILOT_PRIORITY = ["P1", "P2", "P3"];

export const VAL_PILOT_OUTREACH_STATUS = [
  "Not Started",
  "Draft Needed",
  "Drafted",
  "Needs Review",
  "Approved",
  "Sent",
  "Replied",
  "Follow-Up Needed",
  "Follow-Up Later",
  "Not Interested",
  "Converted To Pilot",
  "Archived",
];

export const VAL_PILOT_SEND_CHANNEL = [
  "Email",
  "LinkedIn",
  "WhatsApp",
  "Warm Intro",
  "Manual Call",
  "Other",
];

export const VAL_PILOT_OUTREACH_MESSAGE_ANGLE = [
  "Owner Pilot",
  "Advisor Partner",
  "Feedback / Perspective",
  "Warm Intro / Referral",
  "Brand Criteria Input",
  "Operator Perspective",
  "Owner-Opt-In Referral Only",
  "Pilot Update / Re-Engage Later",
  "Non-CALA Workflow Feedback",
  "Other",
];

export const VAL_PILOT_RELATIONSHIP_STRENGTH = [
  "Strong Warm Relationship",
  "Known Contact",
  "Met Once",
  "LinkedIn / Light Connection",
  "Cold",
  "Dormant",
];

export const VAL_PILOT_RELEVANCE = ["High", "Medium", "Low", "Unknown"];

/** Fields the setup script may create (never renames existing). */
export const PILOT_OUTREACH_FIELDS_TO_ENSURE = [
  "email",
  "linkedInUrl",
  "notes",
  "pilotRegion",
  "outreachSegment",
  "pilotFit",
  "personalizationLine",
  "emailSubject",
  "emailDraft",
  "finalApprovedEmail",
  "linkedInDmDraft",
  "followUpDraft",
  "outreachStatus",
  "replyNotes",
  "readyForMailMerge",
  "mailMergeBatch",
  "sendChannel",
  "introSource",
  "warmIntroContact",
  "doNotContact",
  "doNotContactReason",
];

/**
 * Existing fields that satisfy required model — do not duplicate.
 * @type {Record<string, { existingKey: keyof typeof MAP_PILOT_TARGET_LIST, notes: string }>}
 */
export const PILOT_FIELD_EQUIVALENTS = {
  messageAngle: {
    existingKey: "outreachMessageAngle",
    notes: "Reuse Outreach Message Angle; do not add Message Angle.",
  },
  lastContactedDate: {
    existingKey: "lastContactDate",
    notes: "Reuse Last Contact Date; do not add Last Contacted Date.",
  },
};
