/**
 * Airtable column names for P1 profile-level governance (read path).
 * @see docs/data-intelligence/governance-read-path-trust-label-plan.md
 */

/** Canonical P1 governance columns → API camelCase keys */
export const MAP_PROFILE_GOVERNANCE_AIRTABLE = {
  validationStatus: "Validation Status",
  usagePermission: "Usage Permission",
  sourceType: "Source Type",
  sourceRegion: "Source Region",
  lastReviewedDate: "Last Reviewed Date",
  refreshDueDate: "Refresh Due Date",
  confidenceLevel: "Confidence Level",
  evidenceNotes: "Evidence Notes",
  missingDataFlags: "Missing Data Flags",
  companyValidated: "Company Validated",
  companyValidationDate: "Company Validation Date",
  reviewedBy: "Reviewed By",
  externalDisplayStatus: "External Display Status",
  internalNotes: "Internal Notes",
};

/** Aliases when reading live Airtable rows */
export const MAP_PROFILE_GOVERNANCE_ALIASES = {
  lastReviewedDate: ["Profile Last Reviewed", "Last Reviewed"],
  confidenceLevel: ["Data Confidence Level"],
};

/** Brand Basics legacy hero fields — not used for external displayLabel in Phase 1 */
export const BRAND_GOVERNANCE_LEGACY_FALLBACKS = {
  explorerHeroVerification: "Explorer Hero Verification",
  explorerHeroDataSource: "Explorer Hero Data Source",
};

/** Operator Master legacy / partial equivalents */
export const OPERATOR_GOVERNANCE_LEGACY_FALLBACKS = {
  dataConfidenceLevel: "Data Confidence Level",
  lastUpdatedDate: "Last Updated Date",
  sourceType: "Source Type",
};

export const GOVERNANCE_VALIDATION_STATUS = {
  companyValidated: "Company Validated",
  companyReviewed: "Company Reviewed",
  companyPublished: "Company Published",
  sourceInformed: "Source-Informed",
  ownerProvided: "Owner-Provided",
  aiAssisted: "AI-Assisted",
  needsReview: "Needs Review",
  staleRefreshNeeded: "Stale / Refresh Needed",
  doNotUse: "Do Not Use",
};

/**
 * Explorer trust-chip labels — conservative; never mirror raw Validation Status verbatim.
 * @see docs/data-intelligence/governance-read-path-trust-label-plan.md
 */
export const GOVERNANCE_EXTERNAL_DISPLAY_LABEL = {
  companyValidated: "Company-Validated Profile",
  companyReviewed: "Company-Reviewed Profile",
  companyPublished: "AI-Assisted Profile",
  sourceInformed: "Source-Informed Profile",
  aiAssisted: "AI-Assisted Profile",
};

/** Subtitle source-basis line when external label is shown (Explorer only). */
export const GOVERNANCE_EXTERNAL_SOURCE_BASIS = {
  companyPublished: "Company Materials",
  sourceInformed: "Reviewed Sources",
  aiAssisted: "AI-assisted research",
};

export const GOVERNANCE_USAGE_PERMISSION = {
  internalOnly: "Internal Only",
  platformDisplayAllowed: "Platform Display Allowed",
  scoringAllowed: "Scoring Allowed",
  externalSnapshotAllowed: "External Snapshot Allowed",
  companyValidated: "Company Validated",
  doNotUse: "Do Not Use",
};

export const GOVERNANCE_EXTERNAL_DISPLAY = {
  showTrustLabel: "Show Trust Label",
  hideTrustLabel: "Hide Trust Label",
  internalOnly: "Internal Only",
  needsReview: "Needs Review",
  doNotDisplay: "Do Not Display",
};
