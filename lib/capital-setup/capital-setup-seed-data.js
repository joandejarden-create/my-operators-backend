/**
 * Capital Setup — seed rows for Financing Document Categories reference table.
 */
import { DOCUMENT_CATEGORY_FIELDS } from "./airtable-capital-setup-fields.js";

export const FINANCING_DOCUMENT_CATEGORY_SEED_ROWS = [
  {
    categoryName: "Deal Summary",
    sortOrder: 1,
    requiredForReadiness: true,
    categoryDescription: "High-level deal overview and financing request summary.",
    ownerFacingHelperCopy: "Provide a concise overview of the property, sponsor, and financing request.",
  },
  {
    categoryName: "Financing Request",
    sortOrder: 2,
    requiredForReadiness: true,
    categoryDescription: "Requested loan amount, structure, and use of proceeds.",
    ownerFacingHelperCopy: "Describe the financing amount, structure, and timing you are seeking.",
  },
  {
    categoryName: "Sources & Uses",
    sortOrder: 3,
    requiredForReadiness: true,
    categoryDescription: "Project sources and uses of funds.",
    ownerFacingHelperCopy: "Show how the project will be funded and where capital will be deployed.",
  },
  {
    categoryName: "Property Financials",
    sortOrder: 4,
    requiredForReadiness: true,
    categoryDescription: "Historical and trailing property financial performance.",
    ownerFacingHelperCopy: "Upload TTM and historical P&L, balance sheet, and forecasts where available.",
  },
  {
    categoryName: "Market Data",
    sortOrder: 5,
    requiredForReadiness: false,
    categoryDescription: "STR, market study, and competitive set context.",
    ownerFacingHelperCopy: "Include STR reports, market studies, and competitive positioning data.",
  },
  {
    categoryName: "Sponsor Information",
    sortOrder: 6,
    requiredForReadiness: true,
    categoryDescription: "Sponsor background, experience, and financial capacity.",
    ownerFacingHelperCopy: "Provide sponsor bio, track record, and financial strength indicators.",
  },
  {
    categoryName: "Brand Information",
    sortOrder: 7,
    requiredForReadiness: false,
    categoryDescription: "Brand flag, franchise, and conversion details.",
    ownerFacingHelperCopy: "Include brand LOI, franchise terms, or conversion rationale if applicable.",
  },
  {
    categoryName: "Operator Information",
    sortOrder: 8,
    requiredForReadiness: false,
    categoryDescription: "Operator selection, management agreement, and operating history.",
    ownerFacingHelperCopy: "Provide operator bio, agreement status, and relevant operating experience.",
  },
  {
    categoryName: "Capex / PIP",
    sortOrder: 9,
    requiredForReadiness: false,
    categoryDescription: "Renovation, PIP, and capital expenditure plans.",
    ownerFacingHelperCopy: "Upload PIP budgets, capex schedules, and scope summaries.",
  },
  {
    categoryName: "Legal / Ownership",
    sortOrder: 10,
    requiredForReadiness: false,
    categoryDescription: "Ownership structure, title, and entity documentation.",
    ownerFacingHelperCopy: "Provide ownership chart, title documents, and entity formation details.",
  },
  {
    categoryName: "Existing Debt",
    sortOrder: 11,
    requiredForReadiness: false,
    categoryDescription: "Current loan statements, maturity, and payoff details.",
    ownerFacingHelperCopy: "Include existing loan statements and payoff or assumption terms.",
  },
  {
    categoryName: "Development / Construction",
    sortOrder: 12,
    requiredForReadiness: false,
    categoryDescription: "Development budgets, schedules, and contractor information.",
    ownerFacingHelperCopy: "Upload construction budget, timeline, and contractor details for development deals.",
  },
  {
    categoryName: "Environmental / Technical",
    sortOrder: 13,
    requiredForReadiness: false,
    categoryDescription: "Environmental, engineering, and technical diligence materials.",
    ownerFacingHelperCopy: "Provide environmental reports and technical diligence where available.",
  },
  {
    categoryName: "Insurance",
    sortOrder: 14,
    requiredForReadiness: false,
    categoryDescription: "Insurance policies and coverage summaries.",
    ownerFacingHelperCopy: "Include current insurance policies or broker summaries.",
  },
  {
    categoryName: "Tax / Compliance",
    sortOrder: 15,
    requiredForReadiness: false,
    categoryDescription: "Tax returns, compliance, and regulatory filings.",
    ownerFacingHelperCopy: "Provide tax and compliance documentation as requested by lenders.",
  },
  {
    categoryName: "Other",
    sortOrder: 16,
    requiredForReadiness: false,
    categoryDescription: "Additional supporting documents not captured elsewhere.",
    ownerFacingHelperCopy: "Upload any other documents relevant to your financing request.",
  },
];

/** Sample document names for future provider-linked seeding (requires Capital Provider records). */
export const SAMPLE_REQUIRED_DOCUMENT_NAMES = [
  "Executive Summary",
  "Financing Request",
  "Sources & Uses",
  "TTM P&L",
  "Historical Financials",
  "STR Report",
  "Appraisal",
  "PIP Budget",
  "Construction Budget",
  "Sponsor Bio",
  "Ownership Structure",
  "Brand LOI / Franchise Agreement",
  "Management Agreement / Operator Bio",
  "Existing Loan Statement",
  "Property Photos",
  "Business Plan",
  "Market Study",
  "Environmental Report",
  "Title / Ownership Documents",
];

export function buildCategorySeedFields(row) {
  const F = DOCUMENT_CATEGORY_FIELDS;
  return {
    [F.categoryName]: row.categoryName,
    [F.categoryDescription]: row.categoryDescription || "",
    [F.ownerFacingHelperCopy]: row.ownerFacingHelperCopy || "",
    [F.sortOrder]: row.sortOrder,
    [F.requiredForReadiness]: !!row.requiredForReadiness,
  };
}
