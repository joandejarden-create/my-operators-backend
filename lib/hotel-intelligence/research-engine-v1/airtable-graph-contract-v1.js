/**
 * HE-INTEL-001 P2 — Airtable intelligence graph contract.
 * Five approved tables + HI Graph Evidence on the HPC / census base.
 *
 * Canonical base: appCCUsuGsE1ifoLk (same as Hotel Property Census).
 * Wrong base (do not use for HI graph): appa2cE7FTRmIbB32
 */

import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION } from "./contract.js";

export const GRAPH_CONTRACT_ID = "HOTEL_INTELLIGENCE_AIRTABLE_GRAPH_V1";
export const GRAPH_CONTRACT_VERSION = "1.0.1-p2-hpc-base";
export const GRAPH_SCHEMA_VERSION = "he_intel_airtable_graph_v1";

/** Canonical HPC / census base — HI graph MUST live here. */
export const GRAPH_BASE_ID_CANONICAL = "appCCUsuGsE1ifoLk";

/** Misplaced ADP/intelligence base — HI graph tables must not live here. */
export const GRAPH_BASE_ID_FORBIDDEN = "appa2cE7FTRmIbB32";

/** Hotel Property Census on the canonical base. */
export const HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const HPC_TABLE_NAME = "Hotel Property Census";

/** Reciprocal linked-record field on HPC for HI Hotel–Org Relationships. */
export const HPC_RECIPROCAL_RELATIONSHIP_FIELD = "HI Organization Relationships";

/**
 * ADP/intelligence evidence table — DO NOT delete or mutate without founder auth.
 * Ownership/graph evidence for HE-INTEL uses HI Graph Evidence on the HPC base.
 */
export const ADP_HI_EVIDENCE_TABLE_ID = "tblHxS8x0niDEGVsh";
export const ADP_HI_EVIDENCE_TABLE_NAME = "Hotel Intelligence Evidence";

export const P2_POLICY = Object.freeze({
  airtable_schema_create_authorized: true,
  bulk_research_data_writes: false,
  paid_providers_enabled: false,
  surfe_enabled: false,
  customer_publication: "BLOCKED",
  hpc_owner_operator_overwrite: false,
  radar_status_mutation: false,
  name_only_auto_merge: false,
  require_canonical_base_id: true,
  forbidden_base_id: GRAPH_BASE_ID_FORBIDDEN,
});

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
  graphEvidence: "HI Graph Evidence",
});

/** Populated after schema ensure on HPC base — fail-closed when writing. */
export const HI_GRAPH_TABLE_IDS = Object.freeze({
  // Filled by live registry file / ensure apply report; defaults null until verified.
  organizations: null,
  hotelOrgRelationships: null,
  orgContacts: null,
  contactChannels: null,
  ownershipResearchRuns: null,
  graphEvidence: null,
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

export const GRAPH_EVIDENCE_ENTITY_TYPES = Object.freeze([
  "Organization",
  "Hotel–Org Relationship",
  "Org Contact",
  "Contact Channel",
  "Ownership Research Run",
  "Other",
]);

/** @deprecated ADP evidence extensions — prefer HI Graph Evidence on HPC base. */
export const EVIDENCE_ENTITY_TYPE_EXTENSIONS = GRAPH_EVIDENCE_ENTITY_TYPES;

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
  hpcHotelLink: "HPC Hotel",
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
  /** Linked HI Contact Channels (read-through hydration — no Surfe persistence). */
  contactChannelsLink: "HI Contact Channels",
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
  /** Additive enrichment licensing fields (Contact Enrichment Airtable Schema V1). */
  lastCheckedAt: "Last Checked At",
  licenseRetentionClass: "License / Retention Class",
  retentionExpiresAt: "Retention Expires At",
  providerRequestId: "Provider Request ID",
});

export const MAP_HI_OWNERSHIP_RESEARCH_RUN = Object.freeze({
  runId: "Run ID",
  caseId: "Case ID",
  contractId: "Contract ID",
  contractVersion: "Contract Version",
  hpcHotelId: "HPC Hotel ID",
  hpcHotelLink: "Hotel Property Census",
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

export const MAP_HI_GRAPH_EVIDENCE = Object.freeze({
  evidenceId: "Evidence ID",
  hpcHotelId: "HPC Hotel ID",
  hpcHotelLink: "Hotel Property Census",
  entityType: "Entity Type",
  entityRecordId: "Entity Record ID",
  fieldName: "Field Name",
  valueObserved: "Value Observed",
  sourceName: "Source Name",
  sourceType: "Source Type",
  sourceUrl: "Source URL",
  sourcePublishedDate: "Source Published Date",
  retrievedAt: "Retrieved At",
  evidenceSnippet: "Evidence Snippet",
  confidence: "Confidence",
  evidenceStrength: "Evidence Strength",
  current: "Current?",
  supersededBy: "Superseded By",
  notes: "Notes",
  schemaVersion: "Schema Version",
});

export const CONFLICT_STATUS = Object.freeze([
  "None",
  "Held For Review",
  "Stronger Evidence Wins",
  "Superseded",
]);

export const SUBJECT_TYPES = Object.freeze(["Person", "Organization"]);

export function assertCanonicalGraphBase(baseId) {
  const id = String(baseId || "").trim();
  if (!id) {
    return { ok: false, error: "GRAPH_BASE_ID_MISSING" };
  }
  if (id === GRAPH_BASE_ID_FORBIDDEN) {
    return {
      ok: false,
      error: "GRAPH_BASE_FORBIDDEN",
      message: "HI graph must not use ADP/intelligence base appa2cE7FTRmIbB32",
      expected: GRAPH_BASE_ID_CANONICAL,
      actual: id,
    };
  }
  if (P2_POLICY.require_canonical_base_id && id !== GRAPH_BASE_ID_CANONICAL) {
    return {
      ok: false,
      error: "GRAPH_BASE_MISMATCH",
      expected: GRAPH_BASE_ID_CANONICAL,
      actual: id,
    };
  }
  return { ok: true, baseId: id };
}

export function resolveGraphBaseId(env = process.env) {
  // Prefer explicit graph/HPC envs; never silently fall back to ADP intelligence base.
  const candidates = [
    env.HE_INTEL_GRAPH_AIRTABLE_BASE_ID,
    env.AIRTABLE_BASE_ID_ALT,
    env.AIRTABLE_HPC_BASE_ID,
  ]
    .map((v) => String(v || "").trim())
    .filter(Boolean);
  for (const c of candidates) {
    const gate = assertCanonicalGraphBase(c);
    if (gate.ok) return c;
  }
  return GRAPH_BASE_ID_CANONICAL;
}

export function writeGuaranteesP2() {
  return {
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    graph_contract: GRAPH_CONTRACT_ID,
    graph_contract_version: GRAPH_CONTRACT_VERSION,
    graph_base_id: GRAPH_BASE_ID_CANONICAL,
    forbidden_base_id: GRAPH_BASE_ID_FORBIDDEN,
    hpc_table_id: HPC_TABLE_ID,
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
