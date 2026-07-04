/**
 * Capital Provider Explorer — Airtable field mapping (live Capital Setup tables).
 */
import {
  CAPITAL_PROVIDER_FIELDS as PF,
  REPRESENTATIVE_FINANCING_FIELDS as RF,
  TABLE_CAPITAL_PROVIDERS,
  TABLE_CRITERIA,
  TABLE_REQUIRED_DOCUMENTS,
  TABLE_CONTACTS,
  TABLE_REPRESENTATIVE_FINANCINGS,
  TABLE_SOURCE_REFERENCES,
} from "../../lib/capital-setup/airtable-capital-setup-fields.js";

export const TABLES = {
  providers: TABLE_CAPITAL_PROVIDERS,
  criteria: TABLE_CRITERIA,
  requiredDocuments: TABLE_REQUIRED_DOCUMENTS,
  contacts: TABLE_CONTACTS,
  representativeFinancings: TABLE_REPRESENTATIVE_FINANCINGS,
  sourceReferences: TABLE_SOURCE_REFERENCES,
};

/** Owner-safe provider fields (Airtable column name). */
export const PROVIDER_AT = {
  name: PF.name,
  institutionType: PF.institutionType,
  profileStatus: PF.profileStatus,
  visibilityLevel: PF.visibilityLevel,
  shortDescription: PF.shortDescription,
  institutionOverview: PF.institutionOverview,
  hotelLendingFocus: PF.hotelLendingFocus,
  headquarters: PF.headquarters,
  website: PF.website,
  logoUrl: PF.logoUrl,
  primaryRegion: PF.primaryRegion,
  geographicCoverage: PF.geographicCoverage,
  preferredMarkets: PF.preferredMarkets,
  typicalDealTypes: PF.typicalDealTypes,
  loanProductsOffered: PF.loanProductsOffered,
  preferredAssetTypes: PF.preferredAssetTypes,
  projectStageAppetite: PF.projectStageAppetite,
  minimumLoanSize: PF.minimumLoanSize,
  maximumLoanSize: PF.maximumLoanSize,
  typicalLoanSizeSummary: PF.typicalLoanSizeSummary,
  brandPreference: PF.brandPreference,
  operatorPreference: PF.operatorPreference,
  sponsorPreference: PF.sponsorPreference,
  currentLendingAppetite: PF.currentLendingAppetite,
  contactPathway: PF.contactPathway,
  requiredInformationSummary: PF.requiredInformationSummary,
  processOverview: PF.processOverview,
  ownerFacingNotes: PF.ownerFacingNotes,
  sourceType: PF.sourceType,
  sourceConfidence: PF.sourceConfidence,
  lastVerifiedDate: PF.lastVerifiedDate,
  featuredProvider: PF.featuredProvider,
  sortOrder: PF.sortOrder,
  explorerHeroVerification: PF.explorerHeroVerification,
  explorerHeroDataSource: PF.explorerHeroDataSource,
};

/** Fields that must never appear in owner-facing API responses. */
export const INTERNAL_PROVIDER_AT_FIELDS = new Set([
  PF.internalCreditBoxNotes,
  PF.pricingGuidance,
  PF.leverageGuidance,
  PF.riskLimits,
  PF.relationshipSensitivity,
  PF.dealDeclinePatterns,
  PF.internalRelationshipOwner,
  PF.internalNotes,
  PF.marketsExcludedPaused,
  PF.createdBySeedSource,
]);

export const CRITERIA_AT = {
  name: "Criteria Name",
  provider: "Capital Provider",
  loanProduct: "Loan Product",
  dealTypes: "Deal Type Applicability",
  minLoan: "Minimum Loan Size",
  maxLoan: "Maximum Loan Size",
  minProjectCost: "Minimum Total Project Cost",
  maxProjectCost: "Maximum Total Project Cost",
  termRange: "Term Range",
  recourse: "Recourse Preference",
  rateType: "Rate Type",
  currency: "Currency",
  sponsorReq: "Sponsor Requirements",
  equityReq: "Equity Requirements",
  collateralReq: "Collateral Requirements",
  brandReq: "Brand / Flag Requirements",
  operatorReq: "Operator Requirements",
  marketReq: "Market Requirements",
  appetite: "Appetite Status",
  ownerSummary: "Owner-Visible Summary",
  sourceConfidence: "Source Confidence",
  lastVerified: "Last Verified Date",
};

export const INTERNAL_CRITERIA_AT_FIELDS = new Set([
  "Internal Criteria Notes",
  "Minimum LTV",
  "Maximum LTV",
  "Minimum LTC",
  "Maximum LTC",
  "Excluded Markets",
]);

export const DOCUMENT_AT = {
  reqName: "Document Requirement Name",
  provider: "Capital Provider",
  docName: "Document Name",
  category: "Document Category",
  requiredLevel: "Required Level",
  dealTypes: "Applies To Deal Types",
  description: "Description",
  ownerInstructions: "Owner-Facing Instructions",
  visibility: "Visibility Level",
  sortOrder: "Sort Order",
  internalNotes: "Internal Notes",
};

export const CONTACT_AT = {
  name: "Contact Name",
  provider: "Capital Provider",
  title: "Title",
  department: "Department / Group",
  email: "Email",
  phone: "Phone",
  linkedIn: "LinkedIn URL",
  regionCoverage: "Region Coverage",
  contactRole: "Contact Role",
  preferredMethod: "Preferred Contact Method",
  contactStatus: "Contact Status",
  contactNotes: "Contact Notes",
  internalOnly: "Internal Only",
};

export const FINANCING_AT = {
  name: RF.name,
  provider: RF.provider,
  projectName: RF.projectName,
  location: RF.location,
  dealType: RF.dealType,
  loanAmountLabel: RF.loanAmountLabel,
  loanAmountUsd: RF.loanAmountUsd,
  transactionYear: RF.transactionYear,
  ownerSummary: RF.ownerSummary,
  sourceName: RF.sourceName,
  sourceUrl: RF.sourceUrl,
  imageUrl: RF.imageUrl,
  visibility: RF.visibilityLevel,
  sortOrder: RF.sortOrder,
  internalNotes: RF.internalNotes,
};

export const SOURCE_AT = {
  name: "Source Name",
  provider: "Capital Provider",
  sourceType: "Source Type",
  sourceUrl: "Source URL",
  sourceDate: "Source Date",
  retrievedDate: "Retrieved / Reviewed Date",
  summary: "Source Summary",
  relevantFields: "Relevant Fields Supported",
  confidence: "Confidence Level",
  internalNotes: "Internal Notes",
};

export const OWNER_PROFILE_STATUSES = new Set(["Active", "Needs Review"]);

export const OWNER_VISIBILITY_LEVELS = new Set(["Public", "Limited"]);

export const ADMIN_EXTRA_VISIBILITY_LEVELS = new Set([
  "Private",
  "Admin Only",
  "Invite Only",
]);

export const OWNER_DOCUMENT_VISIBILITY = new Set(["Owner Visible", ""]);

export const OWNER_FINANCING_VISIBILITY = new Set(["Owner Visible", ""]);

export const LOAN_SIZE_RANGE_OPTIONS = [
  "Under $10M",
  "$10M – $25M",
  "$25M – $50M",
  "$50M – $100M",
  "$100M+",
];
