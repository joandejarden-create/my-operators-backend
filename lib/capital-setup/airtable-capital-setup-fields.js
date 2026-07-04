/**
 * Capital Setup — Airtable table names and field spec builders (Financing Hub).
 */
import * as OPT from "./airtable-capital-setup-options.js";

export const TABLE_CAPITAL_PROVIDERS = "Capital Setup - Capital Providers";
export const TABLE_CRITERIA = "Capital Setup - Capital Provider Criteria";
export const TABLE_REQUIRED_DOCUMENTS = "Capital Setup - Capital Provider Required Documents";
export const TABLE_CONTACTS = "Capital Setup - Capital Provider Contacts";
export const TABLE_ACTIVITY = "Capital Setup - Capital Provider Activity";
export const TABLE_FINANCING_NEEDS = "Capital Setup - Deal Financing Needs";
export const TABLE_DEAL_PROVIDER_LIST = "Capital Setup - Deal Capital Provider List";
export const TABLE_VISIBILITY_RULES = "Capital Setup - Capital Provider Visibility Rules";
export const TABLE_SOURCE_REFERENCES = "Capital Setup - Capital Provider Source References";
export const TABLE_REPRESENTATIVE_FINANCINGS =
  "Capital Setup - Representative Financings";
export const TABLE_DOCUMENT_CATEGORIES = "Capital Setup - Financing Document Categories";

export const CAPITAL_SETUP_TABLES = [
  TABLE_CAPITAL_PROVIDERS,
  TABLE_CRITERIA,
  TABLE_REQUIRED_DOCUMENTS,
  TABLE_CONTACTS,
  TABLE_REPRESENTATIVE_FINANCINGS,
  TABLE_ACTIVITY,
  TABLE_FINANCING_NEEDS,
  TABLE_DEAL_PROVIDER_LIST,
  TABLE_VISIBILITY_RULES,
  TABLE_SOURCE_REFERENCES,
  TABLE_DOCUMENT_CATEGORIES,
];

export const CAPITAL_PROVIDER_FIELDS = {
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
  marketsExcludedPaused: "Markets Excluded / Paused",
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
  internalCreditBoxNotes: "Internal Credit Box Notes",
  pricingGuidance: "Pricing Guidance",
  leverageGuidance: "Leverage Guidance",
  riskLimits: "Risk Limits",
  relationshipSensitivity: "Relationship Sensitivity",
  dealDeclinePatterns: "Deal Decline Patterns",
  sourceType: "Source Type",
  sourceConfidence: "Source Confidence",
  lastVerifiedDate: "Last Verified Date",
  internalRelationshipOwner: "Internal Relationship Owner",
  internalNotes: "Internal Notes",
  sortOrder: "Sort Order",
  featuredProvider: "Featured Provider",
  createdBySeedSource: "Created By / Seed Source",
  explorerHeroVerification: "Explorer Hero Verification",
  explorerHeroDataSource: "Explorer Hero Data Source",
};

export const REPRESENTATIVE_FINANCING_FIELDS = {
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
  visibilityLevel: "Visibility Level",
  sortOrder: "Sort Order",
  internalNotes: "Internal Notes",
};

/** Explorer favorites + deal financing list rows (same table). */
export const DEAL_PROVIDER_LIST_FIELDS = {
  listItemName: "List Item Name",
  userId: "User_ID",
  relatedDeal: "Related Deal",
  financingNeed: "Financing Need",
  capitalProvider: "Capital Provider",
  listStatus: "List Status",
  dateAdded: "Date Added",
};

/** List rows with this status and no Related Deal = Capital Explorer favorites. */
export const EXPLORER_FAVORITE_LIST_STATUS = "Saved";

export const DOCUMENT_CATEGORY_FIELDS = {
  categoryName: "Category Name",
  categoryDescription: "Category Description",
  typicalDocuments: "Typical Documents",
  appliesToDealTypes: "Applies To Deal Types",
  ownerFacingHelperCopy: "Owner-Facing Helper Copy",
  sortOrder: "Sort Order",
  requiredForReadiness: "Required for Readiness",
  internalNotes: "Internal Notes",
};

/**
 * @param {object} ctx
 * @param {string|null} ctx.providersId
 * @param {string|null} ctx.dealsId
 * @param {string|null} ctx.financingNeedsId
 * @param {string|null} ctx.contactsId
 */
export function buildTableFieldSpecs(tableName, ctx = {}) {
  switch (tableName) {
    case TABLE_DOCUMENT_CATEGORIES:
      return documentCategoriesFields();
    case TABLE_CAPITAL_PROVIDERS:
      return capitalProvidersFields();
    case TABLE_CRITERIA:
      return criteriaFields(ctx);
    case TABLE_REQUIRED_DOCUMENTS:
      return requiredDocumentsFields(ctx);
    case TABLE_CONTACTS:
      return contactsFields(ctx);
    case TABLE_ACTIVITY:
      return activityFields(ctx);
    case TABLE_FINANCING_NEEDS:
      return financingNeedsFields(ctx);
    case TABLE_DEAL_PROVIDER_LIST:
      return dealProviderListFields(ctx);
    case TABLE_VISIBILITY_RULES:
      return visibilityRulesFields(ctx);
    case TABLE_SOURCE_REFERENCES:
      return sourceReferencesFields(ctx);
    case TABLE_REPRESENTATIVE_FINANCINGS:
      return representativeFinancingsFields(ctx);
    default:
      return [];
  }
}

function capitalProvidersFields() {
  const F = CAPITAL_PROVIDER_FIELDS;
  return [
    { name: F.name, type: "singleLineText" },
    singleSelect(F.institutionType, OPT.INSTITUTION_TYPE_OPTIONS),
    singleSelect(F.profileStatus, OPT.PROFILE_STATUS_OPTIONS),
    singleSelect(F.visibilityLevel, OPT.VISIBILITY_LEVEL_OPTIONS),
    { name: F.shortDescription, type: "multilineText" },
    { name: F.institutionOverview, type: "multilineText" },
    { name: F.hotelLendingFocus, type: "multilineText" },
    { name: F.headquarters, type: "singleLineText" },
    { name: F.website, type: "url" },
    { name: F.logoUrl, type: "url" },
    singleSelect(F.primaryRegion, OPT.PRIMARY_REGION_OPTIONS),
    multiSelect(F.geographicCoverage, OPT.GEOGRAPHY_COVERAGE_OPTIONS),
    multiSelect(F.preferredMarkets, OPT.GEOGRAPHY_COVERAGE_OPTIONS),
    multiSelect(F.marketsExcludedPaused, OPT.GEOGRAPHY_COVERAGE_OPTIONS),
    multiSelect(F.typicalDealTypes, OPT.TYPICAL_DEAL_TYPE_OPTIONS),
    multiSelect(F.loanProductsOffered, OPT.LOAN_PRODUCT_OPTIONS),
    multiSelect(F.preferredAssetTypes, OPT.PREFERRED_ASSET_TYPE_OPTIONS),
    multiSelect(F.projectStageAppetite, OPT.PROJECT_STAGE_APPETITE_OPTIONS),
    currencyField(F.minimumLoanSize),
    currencyField(F.maximumLoanSize),
    { name: F.typicalLoanSizeSummary, type: "singleLineText" },
    singleSelect(F.brandPreference, OPT.BRAND_PREFERENCE_OPTIONS),
    singleSelect(F.operatorPreference, OPT.OPERATOR_PREFERENCE_OPTIONS),
    singleSelect(F.sponsorPreference, OPT.SPONSOR_PREFERENCE_OPTIONS),
    singleSelect(F.currentLendingAppetite, OPT.LENDING_APPETITE_OPTIONS),
    singleSelect(F.contactPathway, OPT.CONTACT_PATHWAY_OPTIONS),
    { name: F.requiredInformationSummary, type: "multilineText" },
    { name: F.processOverview, type: "multilineText" },
    { name: F.ownerFacingNotes, type: "multilineText" },
    { name: F.internalCreditBoxNotes, type: "multilineText" },
    { name: F.pricingGuidance, type: "multilineText" },
    { name: F.leverageGuidance, type: "multilineText" },
    { name: F.riskLimits, type: "multilineText" },
    singleSelect(F.relationshipSensitivity, OPT.RELATIONSHIP_SENSITIVITY_OPTIONS),
    { name: F.dealDeclinePatterns, type: "multilineText" },
    singleSelect(F.sourceType, OPT.SOURCE_TYPE_OPTIONS),
    singleSelect(F.sourceConfidence, OPT.CONFIDENCE_OPTIONS),
    dateField(F.lastVerifiedDate),
    { name: F.internalRelationshipOwner, type: "singleLineText" },
    { name: F.internalNotes, type: "multilineText" },
    numberField(F.sortOrder, 0),
    checkboxField(F.featuredProvider),
    { name: F.createdBySeedSource, type: "singleLineText" },
    {
      name: F.explorerHeroVerification,
      type: "singleLineText",
      description:
        "Capital Provider Explorer hero badge text (e.g. Verified by capital provider, Public Source — Not Capital Provider-Verified).",
    },
    {
      name: F.explorerHeroDataSource,
      type: "singleLineText",
      description:
        "Capital Provider Explorer hero muted line after the badge (e.g. Live Airtable / Capital Setup data).",
    },
  ];
}

function documentCategoriesFields() {
  const F = DOCUMENT_CATEGORY_FIELDS;
  return [
    { name: F.categoryName, type: "singleLineText" },
    { name: F.categoryDescription, type: "multilineText" },
    { name: F.typicalDocuments, type: "multilineText" },
    multiSelect(F.appliesToDealTypes, OPT.DEAL_TYPE_APPLICABILITY_OPTIONS),
    { name: F.ownerFacingHelperCopy, type: "multilineText" },
    numberField(F.sortOrder, 0),
    checkboxField(F.requiredForReadiness),
    { name: F.internalNotes, type: "multilineText" },
  ];
}

function criteriaFields(ctx) {
  return [
    { name: "Criteria Name", type: "singleLineText" },
    providerLink(ctx, "Capital Provider"),
    singleSelect("Loan Product", OPT.LOAN_PRODUCT_OPTIONS),
    multiSelect("Deal Type Applicability", OPT.DEAL_TYPE_APPLICABILITY_OPTIONS),
    currencyField("Minimum Loan Size"),
    currencyField("Maximum Loan Size"),
    currencyField("Minimum Total Project Cost"),
    currencyField("Maximum Total Project Cost"),
    percentField("Minimum LTV"),
    percentField("Maximum LTV"),
    percentField("Minimum LTC"),
    percentField("Maximum LTC"),
    { name: "Term Range", type: "singleLineText" },
    { name: "Amortization", type: "singleLineText" },
    singleSelect("Recourse Preference", OPT.RECOURSE_OPTIONS),
    multiSelect("Rate Type", OPT.RATE_TYPE_OPTIONS),
    multiSelect("Currency", OPT.CURRENCY_OPTIONS),
    { name: "Sponsor Requirements", type: "multilineText" },
    { name: "Equity Requirements", type: "multilineText" },
    { name: "Collateral Requirements", type: "multilineText" },
    { name: "Brand / Flag Requirements", type: "multilineText" },
    { name: "Operator Requirements", type: "multilineText" },
    { name: "Market Requirements", type: "multilineText" },
    multiSelect("Excluded Markets", OPT.GEOGRAPHY_COVERAGE_OPTIONS),
    singleSelect("Appetite Status", OPT.APPETITE_STATUS_OPTIONS),
    { name: "Owner-Visible Summary", type: "multilineText" },
    { name: "Internal Criteria Notes", type: "multilineText" },
    singleSelect("Source Confidence", OPT.CONFIDENCE_OPTIONS),
    dateField("Last Verified Date"),
  ];
}

function requiredDocumentsFields(ctx) {
  return [
    { name: "Document Requirement Name", type: "singleLineText" },
    providerLink(ctx, "Capital Provider"),
    { name: "Document Name", type: "singleLineText" },
    singleSelect("Document Category", OPT.DOCUMENT_CATEGORY_OPTIONS),
    singleSelect("Required Level", OPT.REQUIRED_LEVEL_OPTIONS),
    multiSelect("Applies To Deal Types", OPT.DEAL_TYPE_APPLICABILITY_OPTIONS),
    { name: "Description", type: "multilineText" },
    { name: "Owner-Facing Instructions", type: "multilineText" },
    { name: "Owner Upload Field Mapping", type: "singleLineText" },
    singleSelect("Visibility Level", OPT.DOCUMENT_VISIBILITY_OPTIONS),
    numberField("Sort Order", 0),
    { name: "Internal Notes", type: "multilineText" },
  ];
}

function contactsFields(ctx) {
  return [
    { name: "Contact Name", type: "singleLineText" },
    providerLink(ctx, "Capital Provider"),
    { name: "Title", type: "singleLineText" },
    { name: "Department / Group", type: "singleLineText" },
    { name: "Email", type: "email" },
    { name: "Phone", type: "phoneNumber" },
    { name: "LinkedIn URL", type: "url" },
    multiSelect("Region Coverage", OPT.CONTACT_REGION_OPTIONS),
    singleSelect("Contact Role", OPT.CONTACT_ROLE_OPTIONS),
    { name: "Relationship Owner", type: "singleLineText" },
    singleSelect("Relationship Strength", OPT.RELATIONSHIP_STRENGTH_OPTIONS),
    singleSelect("Preferred Contact Method", OPT.PREFERRED_CONTACT_METHOD_OPTIONS),
    singleSelect("Contact Status", OPT.CONTACT_STATUS_OPTIONS),
    dateField("Last Contacted"),
    dateField("Next Follow-Up Date"),
    { name: "Contact Notes", type: "multilineText" },
    checkboxField("Internal Only"),
  ];
}

function activityFields(ctx) {
  return [
    { name: "Activity Name", type: "singleLineText" },
    providerLink(ctx, "Capital Provider"),
    relatedDealField(ctx, "Related Deal"),
    contactLink(ctx, "Related Contact"),
    singleSelect("Activity Type", OPT.ACTIVITY_TYPE_OPTIONS),
    singleSelect("Outcome", OPT.ACTIVITY_OUTCOME_OPTIONS),
    multiSelect("Decline Reason", OPT.DECLINE_REASON_OPTIONS),
    { name: "Feedback Notes", type: "multilineText" },
    { name: "Owner-Facing Feedback", type: "multilineText" },
    { name: "Internal Notes", type: "multilineText" },
    dateField("Activity Date"),
    { name: "Entered By", type: "singleLineText" },
    checkboxField("Follow-Up Needed"),
    dateField("Follow-Up Date"),
  ];
}

function financingNeedsFields(ctx) {
  return [
    { name: "Financing Need Name", type: "singleLineText" },
    relatedDealField(ctx, "Related Deal"),
    singleSelect("Financing Need Status", OPT.FINANCING_NEED_STATUS_OPTIONS),
    multiSelect("Financing Type Needed", OPT.TYPICAL_DEAL_TYPE_OPTIONS),
    currencyField("Loan Amount Requested"),
    currencyField("Total Project Cost"),
    currencyField("Existing Debt Amount"),
    percentField("Target Leverage"),
    currencyField("Equity Committed"),
    multiSelect("Use of Proceeds", OPT.USE_OF_PROCEEDS_OPTIONS),
    singleSelect("Timing Need", OPT.TIMING_NEED_OPTIONS),
    singleSelect("Collateral Type", OPT.COLLATERAL_TYPE_OPTIONS),
    singleSelect("Asset Status", OPT.ASSET_STATUS_OPTIONS),
    singleSelect("Brand Status", OPT.BRAND_STATUS_OPTIONS),
    singleSelect("Operator Status", OPT.OPERATOR_STATUS_OPTIONS),
    singleSelect("Sponsor Experience", OPT.SPONSOR_EXPERIENCE_OPTIONS),
    multiSelect("Financials Available", OPT.FINANCIALS_AVAILABLE_OPTIONS),
    { name: "Financing Readiness Notes", type: "multilineText" },
    { name: "Missing Information", type: "multilineText" },
    { name: "Internal Notes", type: "multilineText" },
  ];
}

function dealProviderListFields(ctx) {
  const F = DEAL_PROVIDER_LIST_FIELDS;
  return [
    { name: F.listItemName, type: "singleLineText" },
    userLink(ctx, F.userId),
    relatedDealField(ctx, F.relatedDeal),
    financingNeedLink(ctx, F.financingNeed),
    providerLink(ctx, F.capitalProvider),
    singleSelect(F.listStatus, OPT.LIST_STATUS_OPTIONS),
    singleSelect("Fit Band", OPT.FIT_BAND_OPTIONS),
    numberField("Fit Score", 0),
    { name: "Fit Rationale", type: "multilineText" },
    multiSelect("Key Fit Signals", OPT.FIT_SIGNAL_OPTIONS),
    multiSelect("Key Gaps", OPT.FIT_GAP_OPTIONS),
    { name: "Owner Notes", type: "multilineText" },
    { name: "Internal Notes", type: "multilineText" },
    dateField(F.dateAdded),
    dateField("Last Status Change"),
    { name: "Next Action", type: "singleLineText" },
    dateField("Next Action Date"),
  ];
}

function visibilityRulesFields(ctx) {
  return [
    { name: "Rule Name", type: "singleLineText" },
    providerLink(ctx, "Capital Provider"),
    singleSelect("Visibility Scope", OPT.VISIBILITY_SCOPE_OPTIONS),
    multiSelect("User Role", OPT.USER_ROLE_OPTIONS),
    multiSelect("Visible Sections", OPT.VISIBLE_SECTION_OPTIONS),
    multiSelect("Hidden Sections", OPT.VISIBLE_SECTION_OPTIONS),
    checkboxField("Requires NDA"),
    checkboxField("Requires Owner Approval"),
    checkboxField("Requires Admin Approval"),
    singleSelect("Rule Status", OPT.RULE_STATUS_OPTIONS),
    { name: "Notes", type: "multilineText" },
  ];
}

function sourceReferencesFields(ctx) {
  return [
    { name: "Source Name", type: "singleLineText" },
    providerLink(ctx, "Capital Provider"),
    singleSelect("Source Type", OPT.SOURCE_REFERENCE_TYPE_OPTIONS),
    { name: "Source URL", type: "url" },
    dateField("Source Date"),
    dateField("Retrieved / Reviewed Date"),
    { name: "Source Summary", type: "multilineText" },
    multiSelect("Relevant Fields Supported", OPT.RELEVANT_FIELDS_SUPPORTED_OPTIONS),
    singleSelect("Confidence Level", OPT.CONFIDENCE_OPTIONS),
    { name: "Internal Notes", type: "multilineText" },
  ];
}

function representativeFinancingsFields(ctx) {
  const F = REPRESENTATIVE_FINANCING_FIELDS;
  return [
    { name: F.name, type: "singleLineText" },
    providerLink(ctx, F.provider),
    { name: F.projectName, type: "singleLineText" },
    { name: F.location, type: "singleLineText" },
    singleSelect(F.dealType, OPT.DEAL_TYPE_APPLICABILITY_OPTIONS),
    { name: F.loanAmountLabel, type: "singleLineText" },
    currencyField(F.loanAmountUsd),
    { name: F.transactionYear, type: "singleLineText" },
    { name: F.ownerSummary, type: "multilineText" },
    { name: F.sourceName, type: "singleLineText" },
    { name: F.sourceUrl, type: "url" },
    { name: F.imageUrl, type: "url" },
    singleSelect(F.visibilityLevel, OPT.DOCUMENT_VISIBILITY_OPTIONS),
    numberField(F.sortOrder, 0),
    { name: F.internalNotes, type: "multilineText" },
  ];
}

function providerLink(ctx, name) {
  if (ctx.providersId) return linkField(name, ctx.providersId);
  return {
    name,
    type: "singleLineText",
    description: "TODO: convert to linked record when Capital Providers table exists",
  };
}

function contactLink(ctx, name) {
  if (ctx.contactsId) return linkField(name, ctx.contactsId);
  return {
    name,
    type: "singleLineText",
    description: "TODO: convert to linked record when Capital Provider Contacts table exists",
  };
}

function financingNeedLink(ctx, name) {
  if (ctx.financingNeedsId) return linkField(name, ctx.financingNeedsId);
  return {
    name,
    type: "singleLineText",
    description: "TODO: convert to linked record when Deal Financing Needs table exists",
  };
}

function relatedDealField(ctx, name) {
  if (ctx.dealsId) return linkField(name, ctx.dealsId);
  return {
    name,
    type: "singleLineText",
    description: "TODO: convert to linked record when Deals table is resolvable",
  };
}

function userLink(ctx, name) {
  if (ctx.usersId) {
    return {
      ...linkField(name, ctx.usersId),
      description:
        "User who saved this row (Capital Explorer favorites use Saved status with no Related Deal).",
    };
  }
  return {
    name,
    type: "singleLineText",
    description: "TODO: convert to linked record when Users table is resolvable",
  };
}

export function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

export function singleSelect(name, optionNames) {
  return { name, type: "singleSelect", options: { choices: choices(optionNames) } };
}

export function multiSelect(name, optionNames) {
  return { name, type: "multipleSelects", options: { choices: choices(optionNames) } };
}

export function dateField(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}

export function numberField(name, precision = 0) {
  return { name, type: "number", options: { precision } };
}

export function currencyField(name) {
  return { name, type: "currency", options: { precision: 2, symbol: "$" } };
}

export function percentField(name) {
  return { name, type: "percent", options: { precision: 2 } };
}

export function checkboxField(name) {
  return { name, type: "checkbox", options: { icon: "check", color: "greenBright" } };
}

export function linkField(name, linkedTableId) {
  return {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
}
