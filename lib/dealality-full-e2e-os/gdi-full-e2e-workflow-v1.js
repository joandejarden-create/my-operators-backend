/**
 * GDI_FULL_E2E_V1 — versioned operating-system wrapper over the existing
 * gdi-full-e2e-contract-v1.5 + research supervisors.
 *
 * Does not replace the coordinator; pins inputs/outputs and next-action rules.
 */

import {
  FULL_E2E_CONTRACT_VERSION,
  E2E_STAGE,
  COORDINATOR_STAGE,
  FULL_E2E_COORDINATOR_STAGES,
  E2E_SEGMENT_TAXONOMY,
  buildFullE2eContractDocument,
} from "../group-demand-intelligence/workflow/full-e2e-contract-v1.js";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "../group-demand-intelligence/research/research-constants-v1.js";
import {
  PRODUCT,
  RUN_MODE,
  PUBLICATION_LIFECYCLE,
  OVERALL_RUN_STATUS,
} from "./execution-states.js";

export const GDI_FULL_E2E_WORKFLOW_ID = "GDI_FULL_E2E_V1";
export const GDI_FULL_E2E_WORKFLOW_VERSION = "gdi-full-e2e-os-v1";
/** Pins the underlying product contract this OS revision wraps. */
export const GDI_UNDERLYING_CONTRACT_VERSION = FULL_E2E_CONTRACT_VERSION;

export const GDI_REQUIRED_SEGMENTS = Object.freeze([
  "associations_government",
  "medical_academic",
  "exhibitor_sponsor_teams",
  "corporate_meetings_training",
  "scaling_companies",
  "sports",
  "entertainment_production",
  "weddings_social_private_events",
  "professional_planners_dmc",
  "local_meetings_catering",
  "tours_educational_specialist",
]);

export const GDI_WORKFLOW_MODES = Object.freeze({
  [RUN_MODE.NEW]: {
    description: "First complete research/report creation for a hotel",
    allowPaidDiscovery: true,
    allowPublication: true,
    requirePublicationAuth: true,
  },
  [RUN_MODE.REFRESH]: {
    description:
      "Update research while preserving customer experience until approved",
    allowPaidDiscovery: true,
    allowPublication: true,
    requirePublicationAuth: true,
    preserveCustomerVisible: true,
  },
  [RUN_MODE.VALIDATE]: {
    description: "Recheck evidence/reports without unnecessary new measurement",
    allowPaidDiscovery: false,
    allowPublication: false,
  },
  [RUN_MODE.REPAIR]: {
    description: "Recompute affected stages after a verified shared defect fix",
    allowPaidDiscovery: false,
    allowPublication: false,
    recomputeOnly: true,
  },
  [RUN_MODE.STATUS]: {
    description: "Read-only status / next-action inspection",
    allowPaidDiscovery: false,
    allowPublication: false,
  },
  [RUN_MODE.DRY_RUN]: {
    description: "Plan-only; blocks paid and production writes",
    allowPaidDiscovery: false,
    allowPublication: false,
  },
});

/**
 * OS-level stages mapped onto existing coordinator stages.
 * Hotel config may change market/segment strategy — never omit mandatory stages.
 */
export const GDI_OS_STAGES = Object.freeze([
  {
    id: "hotel_identity",
    required: true,
    coordinatorStages: [COORDINATOR_STAGE.VALIDATE_CONFIG],
    e2eStages: [E2E_STAGE.A_RESOLVE_HOTEL, E2E_STAGE.B_VERIFY_PROFILE],
  },
  {
    id: "inventory_reuse",
    required: true,
    coordinatorStages: [COORDINATOR_STAGE.REUSE_EVIDENCE],
    e2eStages: [E2E_STAGE.C_FREEZE_INVENTORY],
  },
  {
    id: "segment_applicability",
    required: true,
    coordinatorStages: [COORDINATOR_STAGE.DISCOVER_GAPS],
    e2eStages: [E2E_STAGE.D_SEGMENT_SCOPE],
  },
  {
    id: "multisegment_research",
    required: true,
    coordinatorStages: [COORDINATOR_STAGE.PAID_MULTISEGMENT],
    e2eStages: [E2E_STAGE.E_SUBSTANTIVE_RESEARCH],
    authorization: "authorize-paid-discovery",
  },
  {
    id: "opportunity_maturation",
    required: true,
    coordinatorStages: [COORDINATOR_STAGE.OPPORTUNITY_RESEARCH_MATURATION],
    e2eStages: [E2E_STAGE.F_QUALIFY_DEDUPE, E2E_STAGE.G_CONTACT_DEPTH],
  },
  {
    id: "fit_eligibility_cards",
    required: true,
    coordinatorStages: [
      COORDINATOR_STAGE.ASSESS_FIT,
      COORDINATOR_STAGE.IDENTIFY_PEOPLE,
      COORDINATOR_STAGE.ENRICH_CONTACTS,
      COORDINATOR_STAGE.BUYER_CONTACT_ENRICHMENT,
      COORDINATOR_STAGE.SYNTHESIZE_HOTEL_RELEVANCE,
      COORDINATOR_STAGE.EVALUATE_ELIGIBILITY,
      COORDINATOR_STAGE.GENERATE_CARDS,
    ],
    e2eStages: [E2E_STAGE.H_HOTEL_FIT, E2E_STAGE.I_CARDS_DRAWERS],
  },
  {
    id: "research_persist_staged",
    required: true,
    coordinatorStages: [
      COORDINATOR_STAGE.PREPARE_MANIFEST,
      COORDINATOR_STAGE.PREVIEW_VERIFY,
      COORDINATOR_STAGE.APPLY_CORPORATE,
      COORDINATOR_STAGE.APPLY_PE,
    ],
    e2eStages: [E2E_STAGE.J_PUBLICATION_DIFF, E2E_STAGE.K_CANONICAL_PERSIST],
    publicationLifecycle: PUBLICATION_LIFECYCLE.RESEARCH_STAGED,
    note: "Research persistence does not require customer publication auth",
  },
  {
    id: "controlled_publication",
    required: true,
    coordinatorStages: [COORDINATOR_STAGE.APPLY_PUBLISH],
    e2eStages: [E2E_STAGE.K_CANONICAL_PERSIST],
    authorization: "authorize-publish",
    publicationLifecycle: PUBLICATION_LIFECYCLE.PUBLISHED,
  },
  {
    id: "delivery_verification",
    required: true,
    coordinatorStages: [
      COORDINATOR_STAGE.LOCAL_DELIVERY_VERIFY,
      COORDINATOR_STAGE.PRODUCTION_DELIVERY_VERIFY,
    ],
    e2eStages: [E2E_STAGE.M_LOCAL_VERIFY, E2E_STAGE.N_PRODUCTION_VERIFY],
    publicationLifecycle: PUBLICATION_LIFECYCLE.DELIVERY_VERIFIED,
  },
  {
    id: "reconcile_lessons_evaluate",
    required: true,
    coordinatorStages: [
      COORDINATOR_STAGE.RECONCILE_RUN,
      COORDINATOR_STAGE.CAPTURE_LESSONS,
      COORDINATOR_STAGE.EVALUATE_COMPLETION,
    ],
    e2eStages: [E2E_STAGE.O_RECONCILE, E2E_STAGE.P_LESSONS_CAPTURE],
  },
]);

export function getGdiWorkflowDefinition() {
  const underlying = buildFullE2eContractDocument();

  return Object.freeze({
    workflowId: GDI_FULL_E2E_WORKFLOW_ID,
    workflowVersion: GDI_FULL_E2E_WORKFLOW_VERSION,
    product: PRODUCT.GDI,
    underlyingContractVersion: GDI_UNDERLYING_CONTRACT_VERSION,
    underlyingManifest: underlying,
    requiredInputs: Object.freeze([
      "hotelId", // HPC record id
      "mode",
    ]),
    optionalConfiguration: Object.freeze([
      "authorizationRef",
      "budgetUsd",
      "authorizePaidDiscovery",
      "authorizeSurfe",
      "authorizePublish",
      "resumeRun",
      "targetEnv",
      "dryRun",
    ]),
    requiredStages: GDI_OS_STAGES,
    coordinatorStages: FULL_E2E_COORDINATOR_STAGES,
    requiredSegments: GDI_REQUIRED_SEGMENTS,
    supervisorExpectedSegments: SUPERVISOR_EXPECTED_SEGMENTS,
    e2eSegmentTaxonomy: E2E_SEGMENT_TAXONOMY,
    modes: GDI_WORKFLOW_MODES,
    terminalStates: Object.freeze([
      OVERALL_RUN_STATUS.FULL_E2E_COMPLETE,
      OVERALL_RUN_STATUS.FAILED,
      OVERALL_RUN_STATUS.BLOCKED,
      OVERALL_RUN_STATUS.WAITING_PUBLICATION_AUTHORIZATION,
    ]),
    qualityGates: Object.freeze([
      "segment_sufficiency_independent",
      "ready_ok_vs_hotel_facing",
      "handoff_rows_collapse_forbidden",
      "publication_auth_required",
      "airtable_readback_required",
      "delivery_verification_required_for_full_e2e",
      "blocked_segment_not_counted_as_sufficient",
    ]),
    budgetPolicy: Object.freeze({
      productScoped: true,
      hotelScoped: true,
      noCrossHotelTransfer: true,
      reuseProviderJobs: true,
      dryRunBlocksPaid: true,
    }),
    publicationRequirements: Object.freeze({
      researchPersistsWithoutPublicationAuth: true,
      customerVisibleRequiresAuthorizePublish: true,
      neverAutoPublishOnReady: true,
      preserveCustomerFeedback: true,
      versionedReplacementPreferred: true,
    }),
    cli: Object.freeze({
      canonical: "scripts/dealality-gdi-full-e2e.mjs",
      underlying: "scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e",
      status: "scripts/gdi-full-e2e-status-v1.mjs",
      npm: "dealality:gdi-full-e2e",
    }),
    invariants: Object.freeze([
      "FULL_E2E_COMPLETE is evaluator-owned only",
      "PUBLIC_PATH_EXHAUSTED is not a commercial disposition",
      "Technical Ready is not commercial Sales Ready",
      "hotelSpecificRepairScriptsForbidden",
      "All 11 segments independently assessed",
    ]),
  });
}

export function validateGdiRequiredSegments(presentSegmentIds = []) {
  const present = new Set(
    (presentSegmentIds || []).map((s) => String(s || "").trim()).filter(Boolean)
  );
  const missing = GDI_REQUIRED_SEGMENTS.filter((s) => !present.has(s));
  return {
    ok: missing.length === 0,
    required: GDI_REQUIRED_SEGMENTS,
    present: [...present],
    missing,
  };
}
