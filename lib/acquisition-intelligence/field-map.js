/**
 * Acquisition Intelligence — Airtable field maps (GTM internal base).
 *
 * Hybrid model:
 *   GTM Contacts = global person identity
 *   Acquisition Network Relationships = user ↔ person (scoped)
 *   Acquisition Import Batches = CSV/API import audit
 *
 * Base: AIRTABLE_GTM_BASE_ID
 * SoT: docs/acquisition-intelligence.md
 */

import { GTM_CONTACT_TABLE } from "../gtm-owner-target/contact-field-map.js";

export const GTM_ACQUISITION_RELATIONSHIPS_TABLE =
  process.env.AIRTABLE_GTM_ACQUISITION_RELATIONSHIPS_TABLE ||
  "Acquisition Network Relationships";

export const GTM_ACQUISITION_IMPORT_BATCHES_TABLE =
  process.env.AIRTABLE_GTM_ACQUISITION_IMPORT_BATCHES_TABLE ||
  "Acquisition Import Batches";

export const GTM_ACQUISITION_LINKED_TABLES = {
  contacts: GTM_CONTACT_TABLE,
  relationships: GTM_ACQUISITION_RELATIONSHIPS_TABLE,
  importBatches: GTM_ACQUISITION_IMPORT_BATCHES_TABLE,
};

/** Provenance constant for LinkedIn Connections CSV exports. */
export const SOURCE_LINKEDIN_CONNECTIONS_EXPORT = "LINKEDIN_CONNECTIONS_EXPORT";

/** Future ingestion methods — CSV first; LinkedIn API later. */
export const VAL_INGESTION_METHOD = ["CSV_EXPORT", "LINKEDIN_API", "MANUAL"];

/** @type {Record<string, string>} */
export const MAP_ACQUISITION_RELATIONSHIP = {
  relationshipName: "Relationship Name",
  sourceUserId: "Source User Id",
  contact: "Contact",
  linkedInUrl: "LinkedIn URL",
  connectedOn: "Connected On",
  relationshipStrength: "Relationship Strength",
  acquisitionRole: "Acquisition Role",
  personCompanyClass: "Person / Company Class",
  importSource: "Import Source",
  ingestionMethod: "Ingestion Method",
  importBatch: "Import Batch",
  sourceFileName: "Source File Name",
  importedAt: "Imported At",
  lastLinkedInSyncAt: "Last LinkedIn Sync At",
  firstName: "First Name",
  lastName: "Last Name",
  email: "Email",
  company: "Company",
  position: "Position",
  status: "Status",
  researchStatus: "Research Status",
  relationshipDedupeKey: "Relationship Dedupe Key",
  notes: "Notes",
  visibility: "Visibility",
  // Stage 2 classification
  directProspectPotential: "Direct Prospect Potential",
  connectorPotential: "Connector Potential",
  decisionVisibility: "Decision Visibility",
  calaRelevance: "CALA Relevance",
  classificationConfidence: "Classification Confidence",
  scoreExplanation: "Score Explanation",
  researchQueueEligibility: "Research Queue Eligibility",
  classificationSource: "Classification Source",
  classifierVersion: "Classifier Version",
  classifiedAt: "Classified At",
  existingOwnerTargetMatch: "Existing Owner Target Match",
  existingOwnerTargetName: "Existing Owner Target Name",
  existingOwnerTarget: "Existing Owner Target",
};

/** @type {Record<string, string>} */
export const MAP_ACQUISITION_IMPORT_BATCH = {
  batchLabel: "Batch Label",
  sourceUserId: "Source User Id",
  sourceFileName: "Source File Name",
  importSource: "Import Source",
  ingestionMethod: "Ingestion Method",
  importedAt: "Imported At",
  status: "Status",
  rowsDetected: "Rows Detected",
  createdCount: "Created Count",
  updatedCount: "Updated Count",
  skippedCount: "Skipped Count",
  invalidCount: "Invalid Count",
  duplicateCount: "Duplicate Count",
  withCompanyCount: "With Company Count",
  withPositionCount: "With Position Count",
  withLinkedInCount: "With LinkedIn Count",
  withEmailCount: "With Email Count",
  earliestConnectedOn: "Earliest Connected On",
  latestConnectedOn: "Latest Connected On",
  previewReportPath: "Preview Report Path",
  notes: "Notes",
  visibility: "Visibility",
};

export const VAL_RELATIONSHIP_STRENGTH = [
  "Unknown",
  "1 — LinkedIn Connection Only",
  "2 — Weak Relationship",
  "3 — Know / Comfortable Contacting",
  "4 — Know Well",
  "5 — Strong / Would Readily Call",
];

export const VAL_ACQUISITION_ROLE = [
  "Unclassified",
  "Direct Prospect",
  "Owner Connector",
  "Decision-Signal Source",
  "Strategic Relationship",
  "Low Relevance",
];

export const VAL_PERSON_COMPANY_CLASS = [
  "Unclassified",
  "Unknown",
  "Hotel Owner",
  "Developer",
  "Family Office",
  "Real Estate Investor",
  "Asset Manager",
  "Brand",
  "Operator / Management Company",
  "Attorney",
  "Architect",
  "Broker",
  "Lender",
  "Capital Provider",
  "Consultant",
  "Feasibility / Advisory",
  "Construction / Project Management",
  "Tourism / Government",
  "Other",
];

export const VAL_RESEARCH_QUEUE_ELIGIBILITY = [
  "Research Priority",
  "Research Candidate",
  "No Research Yet",
];

export const VAL_CLASSIFICATION_SOURCE = [
  "Automated",
  "Manual",
  "Existing GTM Match",
];

export const VAL_EXISTING_OWNER_TARGET_MATCH = ["Yes", "No", "Uncertain"];

/** Classifier version stamped on automated classifications. */
export const ACQUISITION_CLASSIFIER_VERSION = "acquisition-classify-v1";

export const VAL_ACQUISITION_RELATIONSHIP_STATUS = [
  "Active",
  "Archived",
  "Low Relevance",
];

export const VAL_RESEARCH_STATUS = [
  "Not Researched",
  "Queued",
  "Researching",
  "Researched",
  "Needs Review",
  "Failed",
];

export const VAL_POTENTIAL_BAND = ["Unknown", "Low", "Medium", "High"];

export const VAL_CALA_RELEVANCE = [
  "Unknown",
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Colombia",
  "Guatemala",
  "Wider CALA",
  "Warm Europe",
  "Other",
];

export const VAL_CLASSIFICATION_CONFIDENCE = ["Low", "Medium", "High"];

export const VAL_IMPORT_BATCH_STATUS = [
  "Previewed",
  "Applied",
  "Failed",
  "Partially Applied",
];

export const VAL_ACQUISITION_VISIBILITY = ["internal_only"];

/** Official LinkedIn Connections CSV column names (exact export labels). */
export const LINKEDIN_CONNECTIONS_COLUMNS = {
  firstName: "First Name",
  lastName: "Last Name",
  url: "URL",
  email: "Email Address",
  company: "Company",
  position: "Position",
  connectedOn: "Connected On",
};

/** Minimum columns required to accept a file as LinkedIn Connections export. */
export const LINKEDIN_CONNECTIONS_REQUIRED_HEADERS = [
  LINKEDIN_CONNECTIONS_COLUMNS.firstName,
  LINKEDIN_CONNECTIONS_COLUMNS.lastName,
];

export const LINKEDIN_CONNECTIONS_PREFERRED_HEADERS = [
  LINKEDIN_CONNECTIONS_COLUMNS.url,
  LINKEDIN_CONNECTIONS_COLUMNS.email,
  LINKEDIN_CONNECTIONS_COLUMNS.company,
  LINKEDIN_CONNECTIONS_COLUMNS.position,
  LINKEDIN_CONNECTIONS_COLUMNS.connectedOn,
];
