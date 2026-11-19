/**
 * HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1 — versioned E2E research contract.
 * P1 foundation: staging only. No Airtable apply. No paid providers by default.
 */

export const ENGINE_CONTRACT_ID = "HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1";
export const ENGINE_CONTRACT_VERSION = "1.0.0-p1";

/** Publication / research lifecycle (engine-level; distinct from stage graph). */
export const PUBLICATION_STATE = Object.freeze({
  UNRESEARCHED: "UNRESEARCHED",
  RESEARCH_IN_PROGRESS: "RESEARCH_IN_PROGRESS",
  EVIDENCE_COLLECTED: "EVIDENCE_COLLECTED",
  VALIDATION_REQUIRED: "VALIDATION_REQUIRED",
  VERIFIED_STAGED: "VERIFIED_STAGED",
  APPROVED_FOR_CANONICAL_UPSERT: "APPROVED_FOR_CANONICAL_UPSERT",
  PUBLISHED_INTERNAL: "PUBLISHED_INTERNAL",
  REFRESH_DUE: "REFRESH_DUE",
  STOPPED: "STOPPED",
});

/** P1 hard policy — apply disabled until P2 schema + founder auth. */
export const P1_POLICY = Object.freeze({
  airtable_apply_enabled: false,
  paid_providers_enabled: false,
  surfe_enabled: false,
  customer_publication: "BLOCKED",
  golden_demo_as_production: false,
  fixture_traversal_is_not_research_complete: true,
});

export const FAILURE_CLASS = Object.freeze({
  INPUT_INVALID: "INPUT_INVALID",
  IDENTITY_UNRESOLVED: "IDENTITY_UNRESOLVED",
  IDENTITY_AMBIGUOUS: "IDENTITY_AMBIGUOUS",
  NOT_IN_PILOT_MANIFEST: "NOT_IN_PILOT_MANIFEST",
  GOLDEN_DEMO_ISOLATION: "GOLDEN_DEMO_ISOLATION",
  BUDGET_EXHAUSTED: "BUDGET_EXHAUSTED",
  PROVIDER_UNAUTHORIZED: "PROVIDER_UNAUTHORIZED",
  PROVIDER_FAILED: "PROVIDER_FAILED",
  VALIDATION_FAILED: "VALIDATION_FAILED",
  APPLY_DISABLED: "APPLY_DISABLED",
  CROSS_HOTEL_LEAKAGE: "CROSS_HOTEL_LEAKAGE",
  CASE_CORRUPT: "CASE_CORRUPT",
  CASE_IDENTITY_MISMATCH: "CASE_IDENTITY_MISMATCH",
  INCOMPLETE: "INCOMPLETE",
  UNKNOWN: "UNKNOWN",
});

export function writeGuaranteesP1() {
  return {
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    canonical_writes: false,
    airtable_apply: false,
    paid_providers: false,
    surfe: false,
    customer_publication: "BLOCKED",
    research_complete_claim_allowed: false,
  };
}
