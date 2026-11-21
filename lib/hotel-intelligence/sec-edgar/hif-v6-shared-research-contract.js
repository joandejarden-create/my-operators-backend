/**
 * HIF V6 — Shared research contract for dual-track discovery.
 *
 * Track A (commercial/contract/property) and Track B (government/regulatory)
 * feed the same Hotel Explorer identities and dual prospect lists.
 * Wave execution gated on Integration Controller merge of PR #503.
 */

import {
  COMMERCIAL_PROSPECT_LISTS_V6,
  DISCOVERY_CONSTRAINTS_V6,
  HIF_INDUSTRIAL_SCALE_OPERATING_CONTRACT_V6,
  HOTEL_INTELLIGENCE_METRIC,
  RESEARCH_SEQUENCING_AFTER_V5_MERGE,
  SCALE_AMBITIONS_V6,
} from "./hif-industrial-scale-operating-contract-v6.js";

export const HIF_V6_SHARED_RESEARCH_CONTRACT = "hif-v6-shared-research-contract";

export const DISCOVERY_TRACK = Object.freeze({
  TRACK_A_COMMERCIAL_CONTRACT_PROPERTY: "TRACK_A_COMMERCIAL_CONTRACT_PROPERTY",
  TRACK_B_GOVERNMENT_REGULATORY_PUBLIC: "TRACK_B_GOVERNMENT_REGULATORY_PUBLIC",
});

/** Shared canonical identity kinds — both tracks must reconcile into these. */
export const SHARED_IDENTITY_KIND = Object.freeze({
  HOTEL_PROPERTY: "HOTEL_PROPERTY",
  AGREEMENT: "AGREEMENT",
  CONTRACTUAL_EVENT: "CONTRACTUAL_EVENT",
  OWNER: "OWNER",
  OPERATOR: "OPERATOR",
  FRANCHISOR: "FRANCHISOR",
  FRANCHISEE: "FRANCHISEE",
  ASSET_MANAGER: "ASSET_MANAGER",
  DECISION_MAKER: "DECISION_MAKER",
  SOURCE_DOCUMENT: "SOURCE_DOCUMENT",
  COMMERCIAL_OPPORTUNITY: "COMMERCIAL_OPPORTUNITY",
});

/**
 * Commercial signal classes (Phase 10) — independent of metric taxonomy.
 * Do not treat every identified hotel as a commercial opportunity.
 */
export const COMMERCIAL_SIGNAL_CLASS = Object.freeze({
  VERIFIED_CONTRACTUAL_OPPORTUNITY: "VERIFIED_CONTRACTUAL_OPPORTUNITY",
  VERIFIED_COMMERCIAL_SIGNAL: "VERIFIED_COMMERCIAL_SIGNAL",
  RESEARCH_CANDIDATE: "RESEARCH_CANDIDATE",
  HISTORICAL_CLOSED_OR_SUPERSEDED: "HISTORICAL_CLOSED_OR_SUPERSEDED",
  HOLD: "HOLD",
});

/** Court / insolvency evidence classes (Phase 4). */
export const COURT_EVIDENCE_CLASS = Object.freeze({
  ALLEGATION: "ALLEGATION",
  EXECUTED_CONTRACTUAL_INSTRUMENT: "EXECUTED_CONTRACTUAL_INSTRUMENT",
  COURT_FINDING: "COURT_FINDING",
  FINAL_JUDGMENT: "FINAL_JUDGMENT",
  SETTLEMENT_TERMS: "SETTLEMENT_TERMS",
  HISTORICAL_EVENT: "HISTORICAL_EVENT",
  CURRENT_VERIFIED_CONTRACTUAL_STATUS: "CURRENT_VERIFIED_CONTRACTUAL_STATUS",
});

/** Ownership role distinctions (Phase 7) — never collapse interchangeably. */
export const OWNERSHIP_ROLE = Object.freeze({
  PHYSICAL_HOTEL_PROPERTY: "PHYSICAL_HOTEL_PROPERTY",
  LEGAL_PROPERTY_OWNER: "LEGAL_PROPERTY_OWNER",
  TAX_RECORD_OWNER: "TAX_RECORD_OWNER",
  PARENT_OWNERSHIP_COMPANY: "PARENT_OWNERSHIP_COMPANY",
  ASSET_MANAGER: "ASSET_MANAGER",
  FRANCHISEE: "FRANCHISEE",
  OPERATING_COMPANY: "OPERATING_COMPANY",
  MANAGEMENT_COMPANY: "MANAGEMENT_COMPANY",
  FRANCHISOR: "FRANCHISOR",
});

export const MANDATORY_EVIDENCE_FIELDS = Object.freeze([
  "originalSource",
  "sourceJurisdiction",
  "hotelLocation",
  "retrievalDate",
  "agreementType",
  "documentExecutionClass",
  "propertyAttribution",
  "contractualDateSemantics",
  "applicableNoticeClauses",
  "historicalVersusCurrentStatus",
  "verificationConfidence",
  "sourceLineage",
  "discoveryTrack",
  "discoverySourcePath",
]);

/**
 * Parallel-track orchestration inside the registered HE workspace.
 * Product chats must not create unregistered worktrees or edit worktree-registry.json.
 */
export const PARALLEL_TRACK_ORCHESTRATION_V6 = Object.freeze({
  registeredWorkspace: "C:/Dev/dealality/ws-hotel-explorer",
  stream: "he",
  separateUnregisteredWorktrees: false,
  newRegistryEntriesRequireIntegrationController: true,
  trackABranchPattern: "feat/hif-v6-track-a-*",
  trackBBranchPattern: "feat/hif-v6-track-b-*",
  sharedContractOwnership: "lib/hotel-intelligence/sec-edgar/hif-v6-shared-research-contract.js",
  sharedCoreOwnership: [
    "lib/hotel-intelligence/sec-edgar/commercial-target-discovery-v1.js",
    "lib/hotel-intelligence/sec-edgar/hif-industrial-scale-operating-contract-v6.js",
    "lib/hotel-intelligence/sec-edgar/hif-regional-coverage-yield-register-v6.js",
  ],
  mergeOwner: "Integration Controller",
  sequentialMergeRequired: true,
  independentQueues: true,
  independentCheckpoints: true,
  separateCostLedgers: true,
  noDuplicateHifCore: true,
  noDuplicateProspectEngines: true,
});

export const RESEARCH_ROUTING_V6 = Object.freeze({
  defaultPath: [
    "DETERMINISTIC_ROUTING",
    "FREE_OFFICIAL_RECORDS_OR_EXA_SEARCH",
    "INDEPENDENT_VERIFICATION",
  ],
  exaAgent: "MANUAL_ESCALATION_ONLY",
  jev: "SHADOW_ONLY",
  noAutomaticAgentSpend: true,
  noAutomaticJevApply: true,
  providerMayNotCertifyWithoutEvidence: true,
  appraisalStandaloneAdapter: "DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER",
  appraisalPaidResearchAuthorized: false,
  appraisalAssumptionsAreNotContractualTerms: true,
  appraisalSourcePriorityPointersRouteTo: Object.freeze([
    "FRANCHISE_AGREEMENT_DISCOVERY",
    "MANAGEMENT_CONTRACT_DISCOVERY",
  ]),
  spendLedgers: Object.freeze([
    "SEARCH_AND_RETRIEVAL",
    "COURT_DOCUMENT_ACCESS",
    "EXA_AGENT",
    "JEV",
    "OTHER_PAID_RESEARCH",
  ]),
});

export const GOVERNANCE_V6 = Object.freeze({
  customerBlocked: true,
  hpcCreatesUnauthorized: true,
  hpcUpdatesUnauthorized: true,
  metaSchemaChangesUnauthorized: true,
  customerPublicationUnauthorized: true,
  paidPersonalContactEnrichmentUnauthorized: true,
  requireDeduplication: true,
  requireSchemaVerification: true,
  requireEvidenceValidation: true,
  requireIdempotentWrites: true,
  requireIndependentReadBack: true,
  requireHistoricalEventPreservation: true,
  requireSourceLineage: true,
});

/**
 * Phase 0 gate — wave execution unauthorized until V5 PR #503 is on main.
 */
export function buildV6ExecutionGate(pr503 = {}) {
  const merged = Boolean(pr503.merged || pr503.mergeSha);
  return {
    version: HIF_V6_SHARED_RESEARCH_CONTRACT,
    operatingContract: HIF_INDUSTRIAL_SCALE_OPERATING_CONTRACT_V6,
    v5Pr503Merged: merged,
    mergeSha: pr503.mergeSha || null,
    v6WaveExecutionAuthorized: merged,
    competingImplementationPrsAllowed: false,
    designAndInventoryAllowedWhileBlocked: true,
    blocker: merged
      ? null
      : "AWAITING_INTEGRATION_CONTROLLER — PR #503 must merge before Track A/B research waves",
    sequencingAfterMerge: RESEARCH_SEQUENCING_AFTER_V5_MERGE,
    trackAInitialWaves: [
      "EXTRACT_62_EX10_EXHIBITS",
      "EXPAND_HIGH_YIELD_CMBS_CONTRACTUAL_TABLES",
      "PILOT_RECORDED_MANAGEMENT_AGREEMENT_MEMORANDA",
      "PILOT_COURT_INSOLVENCY_CONTRACT_EXHIBITS",
      "RESEARCH_APPRAISALS_AND_UNDERWRITING",
      "ENRICH_OWNERS_OPERATORS_FOR_VERIFIED_EXPIRATIONS",
      "INVESTIGATE_HISTORICAL_RENEWALS_AND_REFLAGGING",
    ],
    trackBInitialWaves: [
      "VERIFY_TEST_BRAZIL_CVM",
      "VERIFY_TEST_JAPAN_EDINET",
      "VERIFY_TEST_UK_COMPANIES_HOUSE",
      "VERIFY_TEST_SGX_AND_HKEX",
      "ASSESS_NA_LATAM_EU_PROPERTY_REGISTRATION_SOURCES",
      "RETRIEVE_HOTEL_RELATED_DISCLOSURES",
      "EXTRACT_AND_VERIFY_AGREEMENT_OWNERSHIP_FINDINGS",
      "IDENTIFY_PORTALS_FOR_LARGER_SCALE_INGESTION",
    ],
    constraints: DISCOVERY_CONSTRAINTS_V6,
    routing: RESEARCH_ROUTING_V6,
    governance: GOVERNANCE_V6,
    commercialLists: COMMERCIAL_PROSPECT_LISTS_V6,
    metricTaxonomy: HOTEL_INTELLIGENCE_METRIC,
    scaleAmbitions: SCALE_AMBITIONS_V6.aspirationalMilestones,
    parallelOrchestration: PARALLEL_TRACK_ORCHESTRATION_V6,
  };
}

export function assertSharedContractIntegrity(contract = {}) {
  const errors = [];
  if (contract.commercialLists?.thirdListAuthorized) {
    errors.push("THIRD_PROSPECT_LIST_NOT_AUTHORIZED");
  }
  if (contract.commercialLists?.leaseDedicatedProduct) {
    errors.push("LEASE_DEDICATED_PRODUCT_NOT_AUTHORIZED");
  }
  if (contract.routing?.exaAgent !== "MANUAL_ESCALATION_ONLY") {
    errors.push("EXA_AGENT_MUST_BE_MANUAL_ESCALATION_ONLY");
  }
  if (contract.governance && contract.governance.customerBlocked !== true) {
    errors.push("CUSTOMER_BLOCKED_REQUIRED");
  }
  if (contract.parallelOrchestration?.separateUnregisteredWorktrees === true) {
    errors.push("UNREGISTERED_WORKTREES_PROHIBITED");
  }
  return { ok: errors.length === 0, errors };
}
