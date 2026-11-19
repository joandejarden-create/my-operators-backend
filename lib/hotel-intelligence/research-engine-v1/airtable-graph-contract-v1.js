/**
 * HE-INTEL-001 P2 — Airtable intelligence graph contract.
 * Five approved tables + Evidence entity-type extension.
 * Schema creation authorized; bulk research data writes are not.
 */

import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION } from "./contract.js";

export const GRAPH_CONTRACT_ID = "HOTEL_INTELLIGENCE_AIRTABLE_GRAPH_V1";
export const GRAPH_CONTRACT_VERSION = "1.0.0-p2";
export const GRAPH_SCHEMA_VERSION = "he_intel_airtable_graph_v1";

/** Canonical intelligence / ADP base (shared HI commercial tables). */
export const GRAPH_BASE_ID_CANONICAL = "appa2cE7FTRmIbB32";

export const P2_POLICY = Object.freeze({
  airtable_schema_create_authorized: true,
  bulk_research_data_writes: false,
  paid_providers_enabled: false,
  surfe_enabled: false,
  customer_publication: "BLOCKED",
  hpc_owner_operator_overwrite: false,
  radar_status_mutation: false,
  name_only_auto_merge: false,
});

/**
 * Publication governance — creating Airtable rows ≠ customer publication.
 */
export const PUBLICATION_LAYER = Object.freeze({
  RESEARCH_DISCOVERY: "RESEARCH_DISCOVERY",
  EVIDENCE_VALIDATION: "EVIDENCE_VALIDATION",
  CANONICAL_RECORD_APPROVAL: "CANONICAL_RECORD_APPROVAL",
  AIRTABLE_PERSISTENCE: "AIRTABLE_PERSISTENCE",
  INTERNAL_HE_PUBLICATION: "INTERNAL_HE_PUBLICATION",
  CUSTOMER_FACING_PUBLICATION: "CUSTOMER_FACING_PUBLICATION",
});

export const RECORD_PUBLICATION_STATE = Object.freeze({
  INTERNAL_ONLY: "INTERNAL_ONLY",
  STAGED: "STAGED",
  APPROVED_INTERNAL: "APPROVED_INTERNAL",
  CUSTOMER_BLOCKED: "CUSTOMER_BLOCKED",
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
});

export const HI_GRAPH_TABLES = Object.freeze({
  organizations: "HI Organizations",
  hotelOrgRelationships: "HI Hotel–Org Relationships",
  orgContacts: "HI Org Contacts",
  contactChannels: "HI Contact Channels",
  ownershipResearchRuns: "HI Ownership Research Runs",
  evidence: "Hotel Intelligence Evidence",
});

export const ORG_TYPES = Object.freeze([
  "Legal Owner",
  "Economic Owner",
  "Investor",
  "Asset Manager",
  "Operator",
  "Franchisee",
  "Brand / Franchisor",
  "Parent Company",
  "Holding Company",
  "REIT",
  "Family Office",
  "Government",
  "Other",
  "Unknown",
]);

export const RELATIONSHIP_TYPES = Object.freeze([
  "Legal Owner",
  "Economic Owner",
  "Investor",
  "Asset Manager",
  "Operator",
  "Franchisee",
  "Brand",
  "Management Company",
  "Developer",
  "Lender",
  "Other",
]);

export const VERIFICATION_STATES = Object.freeze([
  "Unverified",
  "Evidence Collected",
  "Validated",
  "Confirmed",
  "Disputed",
  "Superseded",
  "Needs Review",
]);

export const CONFIDENCE_LEVELS = Object.freeze(["HIGH", "MEDIUM", "LOW", "UNVERIFIED"]);

export const CHANNEL_KINDS = Object.freeze(["Email", "Telephone", "LinkedIn", "Other"]);

export const CHANNEL_CLASSIFICATIONS = Object.freeze([
  "Public",
  "Direct Personal",
  "General / Switchboard",
  "Inferred",
  "Unknown",
]);

export const CHANNEL_VERIFICATION = Object.freeze([
  "Unverified",
  "Inferred",
  "Public Listed",
  "Verified Direct",
  "Do Not Publish",
]);

export const RUN_STATUSES = Object.freeze([
  "INIT",
  "IN_PROGRESS",
  "STAGED",
  "VALIDATION_REQUIRED",
  "APPROVED_FOR_UPSERT",
  "UPSERTED",
  "STOPPED",
  "FAILED",
]);

/** Evidence Entity Type additions (extend existing HI Evidence table). */
export const EVIDENCE_ENTITY_TYPE_EXTENSIONS = Object.freeze([
  "Organization",
  "Hotel–Org Relationship",
  "Org Contact",
  "Contact Channel",
  "Ownership Research Run",
]);

export const MAP_HI_ORGANIZATION = Object.freeze({
  orgId: "Organization ID",
  legalName: "Legal Name",
  tradingName: "Trading Name",
  organizationType: "Organization Type",
  jurisdiction: "Jurisdiction",
  registryId: "Registry / Identifier",
  officialWebsite: "Official Website",
  parentOrgId: "Parent Organization ID",
  parentOrgLink: "Parent Organization",
  verificationState: "Verification State",
  confidence: "Confidence",
  dedupeKey: "Dedupe Key",
  provenanceSummary: "Provenance Summary",
  publicationState: "Publication State",
  evidenceRefs: "Evidence Refs",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const MAP_HI_HOTEL_ORG_RELATIONSHIP = Object.freeze({
  relationshipId: "Relationship ID",
  hpcHotelId: "HPC Hotel ID",
  organizationId: "Organization ID",
  organizationLink: "Organization",
  relationshipType: "Relationship Type",
  current: "Current?",
  effectiveFrom: "Effective From",
  effectiveTo: "Effective To",
  confidence: "Confidence",
  verificationState: "Verification State",
  conflictStatus: "Conflict Status",
  dedupeKey: "Dedupe Key",
  evidenceRefs: "Evidence Refs",
  publicationState: "Publication State",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const MAP_HI_ORG_CONTACT = Object.freeze({
  personId: "Person ID",
  displayName: "Display Name",
  organizationId: "Organization ID",
  organizationLink: "Organization",
  title: "Title",
  functionRole: "Function / Role",
  hotelResponsibility: "Hotel / Commercial Responsibility",
  affiliationVerified: "Affiliation Verified?",
  roleCurrency: "Role Currency",
  confidence: "Confidence",
  verificationState: "Verification State",
  personDedupeKey: "Person Dedupe Key",
  affiliationDedupeKey: "Affiliation Dedupe Key",
  evidenceRefs: "Evidence Refs",
  publicationState: "Publication State",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const MAP_HI_CONTACT_CHANNEL = Object.freeze({
  channelId: "Channel ID",
  subjectType: "Subject Type",
  subjectId: "Subject ID",
  personLink: "Person",
  organizationLink: "Organization",
  channelKind: "Channel Kind",
  channelValue: "Channel Value",
  classification: "Classification",
  verificationStatus: "Verification Status",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  observedAt: "Observed At",
  provider: "Provider",
  usageProvenance: "Usage Provenance",
  accessRestriction: "Access / Publication Restriction",
  dedupeKey: "Dedupe Key",
  publicationState: "Publication State",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const MAP_HI_OWNERSHIP_RESEARCH_RUN = Object.freeze({
  runId: "Run ID",
  caseId: "Case ID",
  contractId: "Contract ID",
  contractVersion: "Contract Version",
  hpcHotelId: "HPC Hotel ID",
  organizationScope: "Organization Scope",
  executionMode: "Execution Mode",
  currentStage: "Current Stage",
  runStatus: "Run Status",
  checkpointRef: "Checkpoint / Resume Ref",
  evidenceVersion: "Evidence Version",
  providerOperations: "Provider Operations JSON",
  costUsd: "Cost USD",
  costUsdStatus: "Cost USD Status",
  credits: "Credits",
  validationState: "Validation State",
  publicationState: "Publication State",
  failureInfo: "Failure / Retry Info",
  inputFingerprint: "Input Fingerprint",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const CONFLICT_STATUS = Object.freeze([
  "None",
  "Held For Review",
  "Stronger Evidence Wins",
  "Superseded",
]);

export const SUBJECT_TYPES = Object.freeze(["Person", "Organization"]);

export function writeGuaranteesP2() {
  return {
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    graph_contract: GRAPH_CONTRACT_ID,
    graph_contract_version: GRAPH_CONTRACT_VERSION,
    canonical_writes: false,
    bulk_research_data_writes: false,
    airtable_schema_create: P2_POLICY.airtable_schema_create_authorized,
    paid_providers: false,
    surfe: false,
    customer_publication: "BLOCKED",
    hpc_owner_operator_overwrite: false,
    radar_status_mutation: false,
    research_complete_claim_allowed: false,
  };
}
