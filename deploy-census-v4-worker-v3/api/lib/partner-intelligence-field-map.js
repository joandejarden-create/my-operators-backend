/**
 * Partner Intelligence Repository — central Airtable field mapping.
 * Tables: Source Library, Extracted Facts, Published Explorer Fields, Helena Intake.
 *
 * Status: Schema stub (Phase 2). No API handlers yet.
 * Validate select options against Meta API before any write (see apply-choice-fee-structure-batch.mjs).
 */

/** @typedef {'Brand'|'Operator'|'Parent Company'|'Service Provider'|'Other'} ProfileType */
/** @typedef {'Brand Explorer'|'Operator Explorer'|'Internal Intelligence'|'Other'} ExplorerType */
/** @typedef {'Public'|'Internal Only'|'Restricted'} VisibilityLevel */
/** @typedef {'High'|'Medium'|'Low'} QualityLevel */
/** @typedef {'Pending'|'Approved'|'Edited'|'Rejected'|'Needs More Source'} HumanReviewStatus */

export const PARTNER_INTELLIGENCE_TABLES = {
  sourceLibrary:
    process.env.PARTNER_INTELLIGENCE_SOURCE_TABLE ||
    "Partner Intelligence - Source Library",
  extractedFacts:
    process.env.PARTNER_INTELLIGENCE_FACTS_TABLE ||
    "Partner Intelligence - Extracted Facts",
  publishedFields:
    process.env.PARTNER_INTELLIGENCE_PUBLISHED_TABLE ||
    "Partner Intelligence - Published Explorer Fields",
  helenaIntake:
    process.env.PARTNER_INTELLIGENCE_HELENA_TABLE ||
    "Partner Intelligence - Helena Outreach Intake",
};

/** Linked Airtable tables (existing) */
export const PARTNER_INTELLIGENCE_LINKS = {
  brandBasics: "Brand Setup - Brand Basics",
  operatorMaster: "Operator Setup - Master",
  users: process.env.AIRTABLE_ME_USERS_TABLE || "Users",
};

/** Brand Setup - Brand Basics: branded residences capability */
export const MAP_BRAND_RESIDENCES = {
  status: "Branded Residences Status",
  notes: "Branded Residences Notes",
  sourceUrl: "Branded Residences Source URL",
  reviewStatus: "Branded Residences Review Status",
};

/**
 * Source Library — API/form key → Airtable column name.
 */
export const MAP_PARTNER_SOURCE = {
  profileType: "Profile Type",
  parentCompany: "Parent Company",
  brand: "Brand",
  operator: "Operator / Management Company",
  region: "Region",
  countryMarket: "Country / Market",
  sourceTitle: "Source Title",
  sourceType: "Source Type",
  sourceUrl: "Source URL",
  sourceFile: "Source File",
  fileType: "File Type",
  sourceDate: "Source Date",
  captureDate: "Capture Date",
  sourceOrigin: "Source Origin",
  visibility: "Public / Private / Restricted",
  verifiedSource: "Verified Source?",
  sourceQuality: "Source Quality",
  status: "Status",
  notes: "Notes",
  lastReviewed: "Last Reviewed",
  reviewedBy: "Reviewed By",
  approvedForExtraction: "Approved for Extraction?",
  approvedForExplorerUse: "Approved for Explorer Use?",
  confidentialityNotes: "Confidentiality Notes",
  permissionVisibilityNotes: "Permission / Visibility Notes",
  relatedContact: "Related Contact",
  extractionRunId: "Extraction Run ID",
  duplicateOf: "Duplicate Of",
  localFilePath: "Local File Path",
};

/** Allowed select options — confirm against base before write */
export const VAL_PARTNER_SOURCE_SELECTS = {
  profileType: ["Brand", "Operator", "Parent Company", "Service Provider", "Other"],
  sourceOrigin: [
    "Public Web",
    "Brand Provided",
    "Operator Provided",
    "Internal Upload",
    "FDD Library",
    "Press Release",
    "RFP Response",
    "Other",
  ],
  visibility: ["Public", "Private", "Restricted"],
  verifiedSource: ["Yes", "No"],
  sourceQuality: ["High", "Medium", "Low"],
  status: [
    "Found",
    "Captured",
    "Classified",
    "Extracted",
    "Needs Review",
    "Approved",
    "Rejected",
    "Stale",
  ],
  approvedForExtraction: ["Yes", "No"],
  approvedForExplorerUse: ["Yes", "No"],
};

/**
 * Extracted Facts — API/form key → Airtable column name.
 */
export const MAP_PARTNER_FACT = {
  profileType: "Profile Type",
  parentCompany: "Parent Company",
  brand: "Brand",
  operator: "Operator / Management Company",
  sourceRecord: "Source Record",
  explorerType: "Explorer Type",
  explorerSection: "Explorer Section",
  fieldName: "Field Name",
  extractedValue: "Extracted Value",
  normalizedValue: "Normalized Value",
  evidenceText: "Evidence Text",
  pageSectionAnchor: "Page Number / Section / URL Anchor",
  sourceType: "Source Type",
  sourceQuality: "Source Quality",
  confidenceScore: "Confidence Score",
  confidenceLevel: "Confidence Level",
  extractionType: "Extraction Type",
  publicVisibility: "Public Visibility",
  humanReviewStatus: "Human Review Status",
  approvedValue: "Approved Value",
  reviewerNotes: "Reviewer Notes",
  dataGap: "Data Gap?",
  followUpQuestion: "Follow-up Question",
  lastUpdated: "Last Updated",
  extractionRunId: "Extraction Run ID",
  reviewedBy: "Reviewed By",
  reviewedAt: "Reviewed At",
};

export const VAL_PARTNER_FACT_SELECTS = {
  explorerType: ["Brand Explorer", "Operator Explorer", "Internal Intelligence", "Other"],
  confidenceLevel: ["High", "Medium", "Low"],
  extractionType: ["Directly Stated", "Inferred", "Needs Confirmation"],
  publicVisibility: ["Public", "Internal Only", "Restricted"],
  humanReviewStatus: ["Pending", "Approved", "Edited", "Rejected", "Needs More Source"],
  dataGap: ["Yes", "No"],
};

/**
 * Published Explorer Fields — live approved values.
 */
export const MAP_PARTNER_PUBLISHED = {
  profileType: "Profile Type",
  brand: "Brand",
  operator: "Operator / Management Company",
  supportingFacts: "Supporting Facts",
  primarySource: "Primary Source",
  explorerType: "Explorer Type",
  explorerSection: "Explorer Section",
  fieldName: "Field Name",
  approvedValue: "Approved Value",
  displayLabel: "Display Label",
  publicVisibility: "Public Visibility",
  overallSourceConfidence: "Overall Source Confidence",
  lastReviewedDate: "Last Reviewed Date",
  reviewedBy: "Reviewed By",
  publishStatus: "Publish Status",
  publishedAt: "Published At",
  stale: "Stale?",
  dataGap: "Data Gap?",
  reviewerNotes: "Reviewer Notes",
  registryVersion: "Registry Version",
};

export const VAL_PARTNER_PUBLISHED_SELECTS = {
  publishStatus: ["Draft", "Published", "Superseded", "Withdrawn"],
  overallSourceConfidence: ["High", "Medium", "Low"],
};

/**
 * Helena Outreach Intake
 */
export const MAP_PARTNER_HELENA = {
  profileType: "Profile Type",
  parentCompany: "Parent Company",
  brand: "Brand",
  operator: "Operator / Management Company",
  contactName: "Contact Name",
  contactTitle: "Contact Title",
  contactEmail: "Contact Email",
  company: "Company",
  region: "Region",
  requestedMaterials: "Requested Materials",
  receivedMaterials: "Received Materials",
  dateRequested: "Date Requested",
  dateReceived: "Date Received",
  sourceOrigin: "Source Origin",
  permissionVisibilityNotes: "Permission / Visibility Notes",
  confidentialityNotes: "Confidentiality Notes",
  followUpNeeded: "Follow-up Needed",
  suggestedFollowUpDate: "Suggested Follow-up Date",
  uploadedToSourceLibrary: "Uploaded to Partner Source Library?",
  extractionStatus: "Extraction Status",
  linkedSourceRecord: "Linked Source Record",
  notes: "Notes",
};

export const VAL_PARTNER_HELENA_SELECTS = {
  profileType: ["Brand", "Operator"],
  sourceOrigin: ["Brand Provided", "Operator Provided"],
  followUpNeeded: ["Yes", "No"],
  uploadedToSourceLibrary: ["Yes", "No"],
  extractionStatus: ["Not Started", "Ready for Extraction", "Extracted", "Needs Review"],
};

/** Standard copy when source does not confirm a field */
export const PARTNER_INTELLIGENCE_GAP_COPY =
  "Not confirmed in available sources.";

/** Feature flags — default off until Phase 5+ */
export const PARTNER_INTELLIGENCE_FLAGS = {
  extractionEnabled: process.env.PARTNER_INTELLIGENCE_EXTRACTION_ENABLED === "1",
  publishEnabled: process.env.PARTNER_INTELLIGENCE_PUBLISH_ENABLED === "1",
  publishOverlay: process.env.PARTNER_INTELLIGENCE_PUBLISH_OVERLAY === "1",
  llmExtraction: process.env.PARTNER_INTELLIGENCE_LLM_EXTRACTION_ENABLED === "1",
  referenceRoot:
    process.env.PARTNER_REFERENCE_ROOT ||
    "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material",
  operatorReferenceRoot:
    process.env.OPERATOR_REFERENCE_ROOT ||
    "G:\\My Drive\\Dealality™\\Platform Design & Build\\Operator Reference Material",
};
