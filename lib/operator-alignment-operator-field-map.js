/**
 * Operator Setup Phase 5C — Airtable field → table mapping for backfill writes.
 */

export const OPERATOR_TABLE_KEYS = {
  master: "master",
  profile: "profile",
  platform: "platform",
  commercial: "commercial",
  governance: "governance",
};

export const OPERATOR_TABLE_NAMES = {
  master: "Operator Setup - Master",
  profile: "Operator Setup - Profile & Positioning",
  platform: "Operator Setup - Platform & Markets",
  commercial: "Operator Setup - Commercial Fit & Terms",
  governance: "Operator Setup - Governance, Delivery & Diligence",
};

/** @type {Record<string, keyof typeof OPERATOR_TABLE_KEYS>} */
export const OPERATOR_FIELD_TO_TABLE_KEY = {
  "Data Confidence Level": "master",
  "Source Type": "master",
  "Last Updated Date": "master",
  "Service Models Supported": "profile",
  chainScalesSupported: "profile", // Airtable field id-style name on Profile table
  "Brand Families Operated": "profile",
  "Active Countries": "platform",
  "Active Markets / Cities": "platform",
  "Market Presence Type": "platform",
  "Management Structures Supported": "commercial",
  "New-Build Opening Experience": "commercial",
  "Pre-Opening Support Capability": "commercial",
  "Offered Services": "governance",
  "Owner Reporting Level": "governance",
  "F&B Capability Level": "governance",
  "Revenue Management Capability": "governance",
  "Sales Platform": "governance",
  "Governance Cadence": "governance",
};

export const OPERATOR_BACKFILL_PRIORITY_FIELDS = [
  "Active Countries",
  "Active Markets / Cities",
  "Market Presence Type",
  "Management Structures Supported",
  "Offered Services",
  "Service Models Supported",
  "chainScalesSupported",
  "Data Confidence Level",
  "Source Type",
];
