/**
 * GDI Full E2E — Complete Intelligence Lifecycle Contract V1
 *
 * Founder correction (2026-11-19): weekly examples illustrated accumulation,
 * NOT separate research phases. Every authorized Full E2E run for a hotel must
 * attempt the complete applicable intelligence lifecycle in that same run.
 *
 * Distinguish:
 *   - Development phases: build/certify order (Slice 1 → 2A → 2B → …)
 *   - Production execution: one integrated Full E2E process
 *
 * Evaluating a dimension ≠ unlimited search. Record explicit outcomes.
 * Persistence failure ⇒ durable completion must not be claimed.
 *
 * No production writes / paid research / publication from this module alone.
 */

export const GDI_FULL_E2E_COMPLETE_INTELLIGENCE_LIFECYCLE_CONTRACT_V1 =
  "gdi-full-e2e-complete-intelligence-lifecycle-contract-v1";

/**
 * Explicit per-dimension outcomes (research attempted vs verified).
 */
export const INTELLIGENCE_DIMENSION_OUTCOME = Object.freeze({
  VERIFIED: "VERIFIED",
  REUSED: "REUSED",
  NOT_FOUND: "NOT_FOUND",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  BLOCKED_BY_BUDGET: "BLOCKED_BY_BUDGET",
  BLOCKED_BY_AUTHORIZATION: "BLOCKED_BY_AUTHORIZATION",
  PERSISTENCE_FAILED: "PERSISTENCE_FAILED",
  NOT_YET_WIRED: "NOT_YET_WIRED",
});

/**
 * Lifecycle dimensions that every applicable Full E2E run must evaluate.
 * Order matches founder requirement list; ids are stable for telemetry.
 */
export const FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS = Object.freeze([
  Object.freeze({
    id: "discovery_fourteen_segments",
    label: "Discovery across all 14 demand segments",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "reuse_existing_durable_intelligence",
    label: "Lookup and reuse of existing Airtable and durable intelligence",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "demand_evidence_and_qualification",
    label: "Demand evidence and commercial qualification research",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "verified_named_contacts",
    label: "Verified named contacts and commercial entry points",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "partners_planners_intermediaries",
    label: "Partners, planners and intermediaries where relevant",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "placements_recurring_competitive",
    label:
      "Previous hotel placements, recurring events and competitive intelligence",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "forward_looking_sourcing_windows",
    label: "Forward-looking demand and plausible sourcing windows",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "hotel_fit_pursuit_recommendations",
    label: "Individual hotel fit, commercial opportunity and pursuit recommendations",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "commercial_intelligence_brief",
    label: "Evidence-backed Commercial Intelligence Brief generation",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "durable_persistence_create_or_update",
    label:
      "Durable creation or update of research, organization, opportunity, contact and partner intelligence",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "weekly_change_classification",
    label: "Weekly Change classification against prior durable state",
    requiredInRun: true,
  }),
  Object.freeze({
    id: "customer_publication_readiness",
    label: "Customer publication-readiness assessment under authorization gates",
    requiredInRun: true,
  }),
]);

/**
 * Target production architecture (not a separate weekly pipeline).
 */
export const FULL_E2E_PRODUCTION_EXECUTION_TARGET = Object.freeze({
  model: "ONE_HOTEL_ONE_INTEGRATED_FULL_E2E_RUN",
  principles: Object.freeze([
    "Complete applicable intelligence in the same authorized run",
    "Reuse before re-research; budget-bounded depth",
    "Persist permissible intelligence before claiming durable completion",
    "Weekly Change computed every applicable run (baseline on first run)",
    "No independent weekly pipelines for competitive / contacts / partners",
    "Development phases remain for build order; production is integrated",
  ]),
  weeklyChangeRules: Object.freeze({
    computedEveryApplicableRun: true,
    firstRunEstablishesBaseline: true,
    subsequentRunsCompareToBaseline: true,
    mustBeEvidenceBasedAndStable: true,
    repeatedWithoutMaterialChangeMustNotBeNewOrUpdated: true,
  }),
  persistenceRules: Object.freeze({
    lookupBeforeResearch: true,
    persistAfterResearch: true,
    doNotDeferAvailableContactsPartnersCompetitors: true,
    durableCompletionRequiresSuccessfulPersistence: true,
    recordPersistenceFailuresVisibly: true,
  }),
  researchDepthRules: Object.freeze({
    evaluateAllApplicableDimensions: true,
    unlimitedSearchProhibited: true,
    reuseVerifiedEvidenceFirst: true,
    targetUnresolvedCommercialQuestions: true,
    recordAttemptedVsVerified: true,
  }),
});

/**
 * Slice 2A applicability: honesty + contract surface only.
 * Does not unlock production writes or claim all dimensions are wired.
 */
export const SLICE_2A_LIFECYCLE_CONTRACT_SCOPE = Object.freeze({
  expandsCurrentPrIntoAllPillars: false,
  requiresLifecycleContractDocumentation: true,
  requiresCapabilityBacklogForUnwiredDimensions: true,
  persistenceHonestyRequired: true,
  productionWritesAuthorized: false,
  paidResearchAuthorized: false,
  customerPublicationAuthorized: false,
});

/**
 * Build an empty per-dimension outcome ledger for a run (telemetry scaffold).
 */
export function buildEmptyIntelligenceLifecycleLedger({
  hotelId = null,
  runId = null,
  at = new Date().toISOString(),
} = {}) {
  const dimensions = {};
  for (const dim of FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS) {
    dimensions[dim.id] = {
      id: dim.id,
      label: dim.label,
      requiredInRun: dim.requiredInRun,
      attempted: false,
      outcome: INTELLIGENCE_DIMENSION_OUTCOME.NOT_YET_WIRED,
      verified: false,
      notes: null,
    };
  }
  return {
    version: GDI_FULL_E2E_COMPLETE_INTELLIGENCE_LIFECYCLE_CONTRACT_V1,
    hotelId,
    runId,
    at,
    productionExecutionTarget: FULL_E2E_PRODUCTION_EXECUTION_TARGET.model,
    dimensions,
    durableCompletionClaimAllowed: false,
    durableCompletionBlockers: [
      "Lifecycle ledger not yet fully populated by canonical Full E2E wiring",
    ],
  };
}

/**
 * Durable completion may be claimed only when required dimensions are settled
 * and persistence did not fail. Unwired dimensions block the claim.
 */
export function evaluateDurableCompletionClaim(ledger = {}) {
  const dims = ledger.dimensions || {};
  const blockers = [];
  for (const dim of FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS) {
    const row = dims[dim.id];
    if (!row) {
      blockers.push({ dimensionId: dim.id, code: "DIMENSION_MISSING" });
      continue;
    }
    if (row.outcome === INTELLIGENCE_DIMENSION_OUTCOME.NOT_YET_WIRED) {
      blockers.push({ dimensionId: dim.id, code: "NOT_YET_WIRED" });
      continue;
    }
    if (row.outcome === INTELLIGENCE_DIMENSION_OUTCOME.PERSISTENCE_FAILED) {
      blockers.push({ dimensionId: dim.id, code: "PERSISTENCE_FAILED" });
      continue;
    }
    if (
      dim.id === "durable_persistence_create_or_update" &&
      row.outcome !== INTELLIGENCE_DIMENSION_OUTCOME.VERIFIED &&
      row.outcome !== INTELLIGENCE_DIMENSION_OUTCOME.REUSED
    ) {
      // Persistence dimension must succeed (or reuse existing durable row).
      if (
        row.outcome !== INTELLIGENCE_DIMENSION_OUTCOME.BLOCKED_BY_AUTHORIZATION
      ) {
        blockers.push({
          dimensionId: dim.id,
          code: "PERSISTENCE_NOT_SUCCESSFUL",
          outcome: row.outcome,
        });
      }
    }
  }
  return {
    durableCompletionClaimAllowed: blockers.length === 0,
    blockers,
  };
}

export default {
  GDI_FULL_E2E_COMPLETE_INTELLIGENCE_LIFECYCLE_CONTRACT_V1,
  INTELLIGENCE_DIMENSION_OUTCOME,
  FULL_E2E_INTELLIGENCE_LIFECYCLE_DIMENSIONS,
  FULL_E2E_PRODUCTION_EXECUTION_TARGET,
  SLICE_2A_LIFECYCLE_CONTRACT_SCOPE,
  buildEmptyIntelligenceLifecycleLedger,
  evaluateDurableCompletionClaim,
};
