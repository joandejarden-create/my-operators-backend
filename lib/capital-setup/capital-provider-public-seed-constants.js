/**
 * Shared constants for public-source capital provider seeding.
 */
export const SEED_SOURCE_TAG = "scripts/seed-capital-providers-public-data.mjs";
export const LAST_VERIFIED = "2026-06-17";

export const OWNER_DISCLAIMER =
  "Capital provider information is based on public sources and is for organizational and informational purposes only. Financing availability, terms, and approval are determined solely by the capital provider.";

/** Default hero trust labels for public-source seeded providers. */
export const PUBLIC_SEED_EXPLORER_HERO_VERIFICATION =
  "Public Source — Not Capital Provider-Verified";
export const PUBLIC_SEED_EXPLORER_HERO_DATA_SOURCE = "Curated Public Sources";

export const PROVIDER_FIELD = {
  name: "Capital Provider Name",
  institutionType: "Institution Type",
  profileStatus: "Profile Status",
  visibilityLevel: "Visibility Level",
  shortDescription: "Short Description",
  institutionOverview: "Institution Overview",
  hotelLendingFocus: "Hotel Lending Focus",
  headquarters: "Headquarters / Main Office",
  website: "Website",
  logoUrl: "Logo URL",
  primaryRegion: "Primary Region",
  geographicCoverage: "Geographic Coverage",
  preferredMarkets: "Preferred Markets",
  typicalDealTypes: "Typical Deal Types",
  loanProductsOffered: "Loan Products Offered",
  preferredAssetTypes: "Preferred Asset Types",
  projectStageAppetite: "Project Stage Appetite",
  minimumLoanSize: "Minimum Loan Size",
  maximumLoanSize: "Maximum Loan Size",
  typicalLoanSizeSummary: "Typical Loan Size Summary",
  brandPreference: "Brand Preference",
  operatorPreference: "Operator Preference",
  sponsorPreference: "Sponsor Preference",
  currentLendingAppetite: "Current Lending Appetite",
  contactPathway: "Contact Pathway",
  requiredInformationSummary: "Required Information Summary",
  processOverview: "Process Overview",
  ownerFacingNotes: "Owner-Facing Notes",
  sourceType: "Source Type",
  sourceConfidence: "Source Confidence",
  lastVerifiedDate: "Last Verified Date",
  createdBySeedSource: "Created By / Seed Source",
  explorerHeroVerification: "Explorer Hero Verification",
  explorerHeroDataSource: "Explorer Hero Data Source",
};

export const SOURCE_FIELD = {
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

export const CRITERIA_FIELD = {
  name: "Criteria Name",
  provider: "Capital Provider",
  loanProduct: "Loan Product",
  dealTypes: "Deal Type Applicability",
  minLoan: "Minimum Loan Size",
  maxLoan: "Maximum Loan Size",
  termRange: "Term Range",
  recourse: "Recourse Preference",
  rateType: "Rate Type",
  currency: "Currency",
  sponsorReq: "Sponsor Requirements",
  marketReq: "Market Requirements",
  appetite: "Appetite Status",
  ownerSummary: "Owner-Visible Summary",
  sourceConfidence: "Source Confidence",
  lastVerified: "Last Verified Date",
};

export const DOCUMENT_FIELD = {
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

export const CONTACT_FIELD = {
  name: "Contact Name",
  provider: "Capital Provider",
  title: "Title",
  department: "Department / Group",
  email: "Email",
  phone: "Phone",
  regionCoverage: "Region Coverage",
  contactRole: "Contact Role",
  preferredMethod: "Preferred Contact Method",
  contactStatus: "Contact Status",
  contactNotes: "Contact Notes",
  internalOnly: "Internal Only",
};

export const FINANCING_FIELD = {
  name: "Representative Financing Name",
  provider: "Capital Provider",
  projectName: "Project Name",
  location: "Location / Market",
  dealType: "Deal Type",
  loanAmountLabel: "Loan Amount Label",
  loanAmountUsd: "Loan Amount (USD)",
  transactionYear: "Transaction Year",
  ownerSummary: "Owner Summary",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  imageUrl: "Image URL",
  visibility: "Visibility Level",
  sortOrder: "Sort Order",
};
