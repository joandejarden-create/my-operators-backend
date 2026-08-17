/**
 * Conversion Financing Package — MVP select option sets.
 * Extends Capital Setup financing needs; do not invent options outside this module.
 */

export const CAPITAL_NEED_TYPE_OPTIONS = [
  "Brand Conversion / Reflagging",
  "PIP / Renovation Financing",
  "Refinance Plus Capex",
  "Acquisition Plus Conversion",
  "Repositioning Capital",
  "Operator Transition Financing",
  "Development / Construction Financing",
  "Working Capital / Stabilization",
  "Other",
];

export const CAPITAL_USE_OF_PROCEEDS_OPTIONS = [
  "PIP / Brand Standards",
  "Guestroom Renovation",
  "Public Area Renovation",
  "FF&E",
  "Signage / Exterior",
  "Systems / Technology",
  "Soft Costs",
  "Working Capital",
  "Interest Reserve",
  "Existing Debt Payoff",
  "Acquisition Closing",
  "Contingency",
  "Other",
];

export const CAPITAL_UNLOCK_OPTIONS = [
  "Brand Conversion",
  "Renovation Completion",
  "Higher Positioning",
  "Operator Transition",
  "Stabilization",
  "Acquisition Closing",
  "Refinance / Maturity Solution",
  "Sale Preparation",
  "Ownership Preservation",
  "Other",
];

export const CAPITAL_ASSET_STATUS_OPTIONS = [
  "Operating Hotel",
  "Closed Hotel",
  "Under Renovation",
  "Under Contract / Acquisition Target",
  "Development Site",
  "Mixed-Use Project",
  "Distressed / Underperforming",
  "Other",
];

export const CAPITAL_BRAND_STATUS_OPTIONS = [
  "Independent",
  "Currently Branded",
  "Brand Agreement Expiring",
  "Brand Conversion Under Review",
  "Brand Contacted",
  "LOI / Application Stage",
  "Franchise Approval Received",
  "Unknown / Not Applicable",
];

export const CAPITAL_PIP_ESTIMATE_STATUS_OPTIONS = [
  "Not Estimated",
  "Owner Estimate",
  "Brand-Provided PIP",
  "Architect / Contractor Estimate",
  "Final Budget Available",
];

export const CAPITAL_OPERATOR_STATUS_OPTIONS = [
  "Existing Operator Staying",
  "Existing Operator Under Review",
  "New Operator Being Evaluated",
  "Operator Not Selected",
  "Owner-Operated",
  "Not Applicable",
];

export const CAPITAL_FINANCIALS_AVAILABILITY_OPTIONS = [
  "Yes, TTM Available",
  "Yes, 2–3 Years Available",
  "Partial Financials",
  "Not Available",
  "Not Yet Uploaded",
];

export const CAPITAL_DEBT_STATUS_OPTIONS = [
  "No Existing Debt",
  "Existing Debt in Place",
  "Debt Maturity Approaching",
  "Refinance Required",
  "Unknown / Not Disclosed",
];

export const CAPITAL_EQUITY_CONTRIBUTION_OPTIONS = [
  "Available",
  "Partially Available",
  "Not Yet Determined",
  "Not Disclosed",
];

export const CAPITAL_TIMING_OPTIONS = [
  "Immediate",
  "30–60 Days",
  "3–6 Months",
  "6–12 Months",
  "Early Planning",
];

export const CAPITAL_SHARING_PREFERENCE_OPTIONS = [
  "Save Internally Only",
  "Share Anonymized Summary With Matched Capital Providers",
  "Share Named Opportunity Only After Owner Approval",
  "Share Documents Only After NDA / Permission",
];

/** Default: private — no provider visibility. */
export const CAPITAL_SHARING_STATUS_OPTIONS = [
  "Draft",
  "Internal Review",
  "Owner Approved Summary",
  "Shared Anonymously",
  "Intro Approved",
  "NDA / Confidential Review",
  "Archived",
];

export const CAPITAL_SUPPORTING_DOCUMENTS_OPTIONS = [
  "TTM P&L",
  "2–3 Years P&L",
  "Capex / PIP Budget",
  "Existing Debt Summary",
  "Property Photos",
  "Brand Correspondence",
  "Operator Proposal",
  "Appraisal / Valuation",
  "STR / Market Data",
  "Purchase Agreement",
  "Construction Budget",
  "None Yet",
];

export const CAPITAL_CURRENCY_OPTIONS = [
  "USD",
  "EUR",
  "MXN",
  "DOP",
  "COP",
  "BRL",
  "CLP",
  "PEN",
  "Local Currency",
  "Other",
];

/** Sharing statuses that may expose anonymized provider-facing summary. */
export const PROVIDER_VISIBLE_SHARING_STATUSES = new Set([
  "Owner Approved Summary",
  "Shared Anonymously",
  "Intro Approved",
  "NDA / Confidential Review",
]);

export const DEFAULT_CAPITAL_SHARING_STATUS = "Draft";
