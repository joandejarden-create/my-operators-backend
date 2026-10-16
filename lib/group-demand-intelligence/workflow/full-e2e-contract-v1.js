/**
 * Full GDI E2E Contract V1 — versioned execution + completion rules.
 *
 * Distinguishes: task execution ≠ research coverage ≠ publication ≠ customer delivery.
 * Association-only publication while applicable segments remain plan-only ≠ COMPLETE.
 *
 * Wired into scripts/gdi-research-to-page-workflow-v1.mjs via --mode full-e2e.
 */

import {
  RESEARCH_SEGMENT,
  RESEARCH_SEGMENT_LABEL,
  APPLICABILITY,
} from "./hotel-segment-research-plan-v1.js";
import {
  EXECUTION_MODE,
  SEGMENT_COVERAGE_STATUS,
  TASK_EXECUTION_STATUS,
} from "./research-scorecard-v1.js";
import { COVERAGE_STATUS } from "./shared-demand-model-v1.js";

export const FULL_E2E_CONTRACT_VERSION = "gdi-full-e2e-contract-v1";

/** Overall run status derived by the completion evaluator. */
export const E2E_OVERALL_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  RUNNING: "RUNNING",
  PARTIAL: "PARTIAL",
  BLOCKED: "BLOCKED",
  COMPLETE: "COMPLETE",
  FAILED: "FAILED",
});

/** Run modes — weekly refresh remains distinct. */
export const E2E_RUN_MODE = Object.freeze({
  FULL_E2E: "full_e2e",
  STANDARD_COORDINATOR: "standard_coordinator",
  WEEKLY_REFRESH: "weekly_refresh",
  STATUS_ONLY: "status_only",
  DRY_RUN_PREFLIGHT: "dry_run_preflight",
});

/**
 * Contract stages A–O. Each maps to one or more coordinator stages
 * and lists evidence required to pass.
 */
export const E2E_STAGE = Object.freeze({
  A_RESOLVE_HOTEL: "A_resolve_hotel_identity",
  B_VERIFY_PROFILE: "B_verify_hotel_profile",
  C_FREEZE_INVENTORY: "C_freeze_inventory_reuse",
  D_SEGMENT_SCOPE: "D_segment_applicability_scope",
  E_SUBSTANTIVE_RESEARCH: "E_substantive_multisegment_research",
  F_QUALIFY_DEDUPE: "F_qualify_deduplicate",
  G_CONTACT_DEPTH: "G_contact_depth",
  H_HOTEL_FIT: "H_hotel_fit_narratives",
  I_CARDS_DRAWERS: "I_cards_drawers_quality",
  J_PUBLICATION_DIFF: "J_publication_diff_backups",
  K_CANONICAL_PERSIST: "K_canonical_persistence",
  L_REPORTS_LINKS: "L_reports_external_links",
  M_LOCAL_VERIFY: "M_local_delivery_verify",
  N_PRODUCTION_VERIFY: "N_production_delivery_verify",
  O_RECONCILE: "O_reconcile_budget_history_backlog",
});

/** Coordinator stage ids used by gdi-research-to-page-workflow-v1.mjs */
export const COORDINATOR_STAGE = Object.freeze({
  VALIDATE_CONFIG: "validate_config",
  REUSE_EVIDENCE: "reuse_evidence",
  WEEKLY_REFRESH: "weekly_refresh",
  DISCOVER_GAPS: "discover_gaps",
  PAID_MULTISEGMENT: "paid_multisegment",
  ASSESS_FIT: "assess_fit",
  IDENTIFY_PEOPLE: "identify_people",
  ENRICH_CONTACTS: "enrich_contacts",
  BUYER_CONTACT_ENRICHMENT: "buyer_contact_enrichment",
  SYNTHESIZE_HOTEL_RELEVANCE: "synthesize_hotel_relevance",
  EVALUATE_ELIGIBILITY: "evaluate_eligibility",
  GENERATE_CARDS: "generate_cards",
  PREPARE_MANIFEST: "prepare_manifest",
  PREVIEW_VERIFY: "preview_verify",
  APPLY_CORPORATE: "apply_corporate_packages",
  APPLY_PE: "apply_pe_canonical",
  APPLY_PUBLISH: "apply_publish",
  /** New stages added for Full E2E contract */
  LOCAL_DELIVERY_VERIFY: "local_delivery_verify",
  PRODUCTION_DELIVERY_VERIFY: "production_delivery_verify",
  RECONCILE_RUN: "reconcile_run",
  EVALUATE_COMPLETION: "evaluate_completion",
});

/**
 * Mapping: contract stage → coordinator stage(s) + pass evidence keys.
 */
export const E2E_STAGE_DEFINITIONS = Object.freeze([
  {
    id: E2E_STAGE.A_RESOLVE_HOTEL,
    letter: "A",
    title: "Resolve canonical hotel identity",
    coordinatorStages: [COORDINATOR_STAGE.VALIDATE_CONFIG],
    inputs: ["--hotel canonicalHotelId", "config/group-demand-intelligence/hotels/<id>.json"],
    outputs: ["resolved hotelConfig", "canonical hotelId"],
    evidenceRequired: ["hotel_config_present", "canonical_hotel_id_match"],
    executionModes: ["live", "dry_run"],
    sideEffects: "none",
    authorization: "none",
    failurePartial: "missing config → FAILED/BLOCKED",
    retryResume: "re-run validate_config",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.B_VERIFY_PROFILE,
    letter: "B",
    title: "Verify hotel profile, facilities, positioning, catchment, constraints",
    coordinatorStages: [COORDINATOR_STAGE.VALIDATE_CONFIG, COORDINATOR_STAGE.REUSE_EVIDENCE],
    inputs: ["hotelConfig.capabilityProfile", "market/geography profile"],
    outputs: ["profile verification record"],
    evidenceRequired: ["profile_fields_present_or_gaps_recorded"],
    executionModes: ["reuse", "fresh_research", "dry_run"],
    sideEffects: "none (read/verify)",
    authorization: "none",
    failurePartial: "critical profile gaps → PARTIAL/BLOCKED",
    retryResume: "resume after profile repair",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.C_FREEZE_INVENTORY,
    letter: "C",
    title: "Freeze existing inventory and reuse prior intelligence/feedback",
    coordinatorStages: [COORDINATOR_STAGE.REUSE_EVIDENCE],
    inputs: ["Airtable/filesystem opportunities", "feedback/actions"],
    outputs: ["reuse ledger", "inventory freeze snapshot"],
    evidenceRequired: ["inventory_freeze_artifact", "reuse_ledger"],
    executionModes: ["reuse", "dry_run"],
    sideEffects: "read-only freeze artifact write under run dir",
    authorization: "none",
    failurePartial: "missing freeze → cannot claim COMPLETE",
    retryResume: "idempotent re-freeze",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.D_SEGMENT_SCOPE,
    letter: "D",
    title: "Determine segment applicability and research scope",
    coordinatorStages: [COORDINATOR_STAGE.REUSE_EVIDENCE, COORDINATOR_STAGE.DISCOVER_GAPS],
    inputs: ["hotel profile", "segment research plan"],
    outputs: ["frozen segment scope", "briefs for applicable segments"],
    evidenceRequired: ["segment_scope_freeze", "applicability_rationales"],
    executionModes: ["plan", "dry_run"],
    sideEffects: "scope freeze write under run dir",
    authorization: "none",
    failurePartial: "unsupported NOT_APPLICABLE without rationale rejected",
    retryResume: "scope change requires recorded before/after",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.E_SUBSTANTIVE_RESEARCH,
    letter: "E",
    title: "Execute substantive research for every applicable segment",
    coordinatorStages: [
      COORDINATOR_STAGE.DISCOVER_GAPS,
      COORDINATOR_STAGE.PAID_MULTISEGMENT,
    ],
    inputs: ["frozen briefs", "budget ledger", "authorization"],
    outputs: ["provider run IDs", "scorecard rows", "candidates"],
    evidenceRequired: [
      "every_applicable_segment_not_plan_only",
      "coverage_status_per_segment",
      "depth_checks_or_partial_reason",
    ],
    executionModes: ["reuse", "fresh_research", "replay", "live"],
    sideEffects: "paid provider calls when authorized",
    authorization: "--authorize-paid-discovery / paid-MS envelope",
    failurePartial: "plan_only applicable segment → cannot COMPLETE",
    retryResume: "resume skips completed provider jobs by jobKey",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.F_QUALIFY_DEDUPE,
    letter: "F",
    title: "Qualify and deduplicate candidates",
    coordinatorStages: [
      COORDINATOR_STAGE.EVALUATE_ELIGIBILITY,
      COORDINATOR_STAGE.ASSESS_FIT,
    ],
    inputs: ["candidates", "use-case contracts"],
    outputs: ["qualification dispositions", "change ledger"],
    evidenceRequired: ["disposition_per_candidate", "dedupe_identity_keys"],
    executionModes: ["reuse", "fresh_research", "dry_run"],
    sideEffects: "internal artifacts",
    authorization: "none",
    failurePartial: "unreconciled dispositions → PARTIAL",
    retryResume: "idempotent by opportunity identity",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.G_CONTACT_DEPTH,
    letter: "G",
    title: "Execute contact-depth research",
    coordinatorStages: [
      COORDINATOR_STAGE.IDENTIFY_PEOPLE,
      COORDINATOR_STAGE.ENRICH_CONTACTS,
      COORDINATOR_STAGE.BUYER_CONTACT_ENRICHMENT,
    ],
    inputs: ["credible candidates", "Surfe auth flag"],
    outputs: ["named research status", "channel provenance"],
    evidenceRequired: [
      "contact_depth_attempt_records",
      "no_silent_not_attempted_to_exhausted",
    ],
    executionModes: ["public", "surfe_authorized", "dry_run"],
    sideEffects: "Surfe spend when --authorize-surfe",
    authorization: "--authorize-surfe",
    failurePartial: "NOT_ATTEMPTED remaining on required queue → PARTIAL",
    retryResume: "reuse accepted Surfe results by person identity",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.H_HOTEL_FIT,
    letter: "H",
    title: "Synthesize independent hotel fit and sales narratives",
    coordinatorStages: [
      COORDINATOR_STAGE.SYNTHESIZE_HOTEL_RELEVANCE,
      COORDINATOR_STAGE.ASSESS_FIT,
    ],
    inputs: ["qualified opportunities", "hotel profile"],
    outputs: ["hotel-fit assessments", "why-this-hotel prose"],
    evidenceRequired: ["fit_assessment_or_insufficient_evidence", "no_invented_scores"],
    executionModes: ["reuse", "fresh_research", "dry_run"],
    sideEffects: "artifact writes",
    authorization: "none",
    failurePartial: "missing narratives on published set → PARTIAL",
    retryResume: "re-run synthesize stage",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.I_CARDS_DRAWERS,
    letter: "I",
    title: "Generate and quality-check cards/drawers",
    coordinatorStages: [COORDINATOR_STAGE.GENERATE_CARDS],
    inputs: ["opportunities", "drawer contract"],
    outputs: ["drawer completeness report"],
    evidenceRequired: ["drawer_completeness_report"],
    executionModes: ["dry_run", "live"],
    sideEffects: "artifact writes",
    authorization: "none",
    failurePartial: "quality review cannot replace missing research",
    retryResume: "re-run generate_cards",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.J_PUBLICATION_DIFF,
    letter: "J",
    title: "Prepare publication diff and backups",
    coordinatorStages: [COORDINATOR_STAGE.PREPARE_MANIFEST, COORDINATOR_STAGE.PREVIEW_VERIFY],
    inputs: ["package dir", "production snapshot"],
    outputs: ["diff", "backups"],
    evidenceRequired: ["publication_diff", "backup_paths"],
    executionModes: ["dry_run", "live"],
    sideEffects: "backup writes under run dir",
    authorization: "none for prep",
    failurePartial: "missing backups → cannot COMPLETE publish path",
    retryResume: "idempotent backup",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.K_CANONICAL_PERSIST,
    letter: "K",
    title: "Persist canonical entities and qualified opportunities",
    coordinatorStages: [
      COORDINATOR_STAGE.APPLY_CORPORATE,
      COORDINATOR_STAGE.APPLY_PE,
      COORDINATOR_STAGE.APPLY_PUBLISH,
    ],
    inputs: ["qualified package", "--authorize-publish"],
    outputs: ["Airtable/filesystem persistence proof", "read-back"],
    evidenceRequired: ["persist_readback", "idempotency_check"],
    executionModes: ["dry_run_blocks_writes", "authorized_apply"],
    sideEffects: "production writes when authorized",
    authorization: "--authorize-publish / --authorize-pe-persist",
    failurePartial: "qualified publish OK while other segments partial → overall PARTIAL",
    retryResume: "idempotent upsert by opportunity id",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.L_REPORTS_LINKS,
    letter: "L",
    title: "Generate/update required reports and external links",
    coordinatorStages: [COORDINATOR_STAGE.RECONCILE_RUN, COORDINATOR_STAGE.EVALUATE_COMPLETION],
    inputs: ["persisted inventory", "share registry"],
    outputs: ["report/PDF meta", "hotel-scoped share reference (no raw token in git)"],
    evidenceRequired: ["deliverable_inventory_checked", "share_path_or_gap_recorded"],
    executionModes: ["verify", "generate"],
    sideEffects: "report artifacts; no token commit",
    authorization: "share mint uses production secret when issuing",
    failurePartial: "missing required deliverable without gap record → not COMPLETE",
    retryResume: "re-verify links",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.M_LOCAL_VERIFY,
    letter: "M",
    title: "Verify local delivery",
    coordinatorStages: [COORDINATOR_STAGE.LOCAL_DELIVERY_VERIFY, COORDINATOR_STAGE.PREVIEW_VERIFY],
    inputs: [":8080 or declared local process", "flags/allowlist", "data root"],
    outputs: ["local verification proof"],
    evidenceRequired: ["local_serving_process", "local_list_detail_check"],
    executionModes: ["browser", "api", "asset"],
    sideEffects: "none",
    authorization: "none",
    failurePartial: "auth-blocked browser check → BLOCKED/NOT_RUN (not pass)",
    retryResume: "re-run after local restart",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.N_PRODUCTION_VERIFY,
    letter: "N",
    title: "Deploy when needed and verify production delivery",
    coordinatorStages: [COORDINATOR_STAGE.PRODUCTION_DELIVERY_VERIFY],
    inputs: ["deployed sha", "production URL", "canonical data"],
    outputs: ["production verification proof"],
    evidenceRequired: [
      "deployed_code_or_data_revision",
      "production_list_detail_share_check",
      "no_provider_metadata_leak",
    ],
    executionModes: ["api", "browser", "deploy_then_verify"],
    sideEffects: "deploy only when code requires it",
    authorization: "normal clean-main deploy process",
    failurePartial: "Airtable write alone ≠ production delivery",
    retryResume: "invalidate stale proofs on later mutation",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.O_RECONCILE,
    letter: "O",
    title: "Reconcile results, budget, execution history, refresh backlog",
    coordinatorStages: [
      COORDINATOR_STAGE.RECONCILE_RUN,
      COORDINATOR_STAGE.EVALUATE_COMPLETION,
    ],
    inputs: ["ledgers", "scorecards", "change ledger"],
    outputs: ["reconciliation", "computed overall status", "backlog seed"],
    evidenceRequired: ["budget_reconciled", "overall_status_from_evaluator"],
    executionModes: ["always"],
    sideEffects: "artifact writes under run dir",
    authorization: "none",
    failurePartial: "unresolved reservations/holds recorded; may force PARTIAL",
    retryResume: "idempotent reconcile",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
]);

/** Segment taxonomy for Full E2E (maps to RESEARCH_SEGMENT + planner axis). */
export const E2E_SEGMENT_TAXONOMY = Object.freeze([
  {
    id: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.SCALING_COMPANIES,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.SCALING_COMPANIES],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING],
    primary: true,
    includesSubsegments: ["corporate_room_rental", "social_celebrations", "nonprofit_assoc_local"],
  },
  {
    id: "professional_planners_dmc",
    label: "Professional planners / DMCs / intermediaries",
    primary: true,
    mapsToResearchSegment: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
    coverageAxis: "planner_intermediary_discovery",
    note: "Association contacts do not satisfy planner-discovery coverage",
  },
  {
    id: RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS],
    primary: true,
    includesSubsegments: ["venue_lodging_referral"],
  },
  {
    id: RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.SPORTS,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.SPORTS],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.MEDICAL_ACADEMIC],
    primary: true,
  },
  {
    id: RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
    label: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST],
    primary: true,
  },
]);

/**
 * Forbidden rationales for NOT_APPLICABLE.
 * Applicability must come from hotel profile / catchment / demand use cases.
 */
export const FORBIDDEN_NOT_APPLICABLE_REASONS = Object.freeze([
  "research_inconvenient",
  "provider_returned_nothing_useful",
  "budget_exhausted",
  "no_cards_published_yet",
  "skipped_for_time",
  "will_do_later",
]);

/**
 * Depth inadequacy patterns — evidence-based, not card quotas.
 */
export const DEPTH_INADEQUACY_RULES = Object.freeze([
  {
    id: "single_broad_competing_hotels",
    description:
      "A single broad query returning competing hotels does not complete buyer discovery",
  },
  {
    id: "directory_only_unverified",
    description: "Directory-only results require source checking before qualification",
  },
  {
    id: "assoc_contact_not_planner_coverage",
    description:
      "An association contact does not establish professional-planner coverage",
  },
  {
    id: "venue_list_not_activity",
    description:
      "A venue list does not establish venue activity, lodging/referral potential, or hotel fit",
  },
  {
    id: "funding_alone_not_scaling",
    description: "Funding alone does not qualify a scaling company",
  },
  {
    id: "local_sports_calendar_not_travel",
    description: "A local sports calendar does not establish traveling demand",
  },
  {
    id: "plan_only_not_execution",
    description: "A brief or query plan is not execution",
  },
]);

/** Customer deliverables that must be checked (or gap-recorded). */
export const E2E_REQUIRED_DELIVERABLES = Object.freeze([
  {
    id: "customer_list_drawers",
    label: "Customer list and drawers",
    required: true,
  },
  {
    id: "gdi_report_pdf",
    label: "GDI report/PDF where supported",
    required: true,
    allowGapIfUnsupported: true,
  },
  {
    id: "exports",
    label: "Appropriate exports",
    required: true,
    allowGapIfUnsupported: true,
  },
  {
    id: "hotel_scoped_share_link",
    label: "Valid hotel-scoped external share link",
    required: true,
  },
  {
    id: "admin_report_share_refs",
    label: "Correct Admin report/share references",
    required: true,
  },
  {
    id: "research_publication_history",
    label: "Research and publication history",
    required: true,
  },
]);

/**
 * Coordinator stage order for Full E2E mode (extends standard STAGES).
 */
export const FULL_E2E_COORDINATOR_STAGES = Object.freeze([
  COORDINATOR_STAGE.VALIDATE_CONFIG,
  COORDINATOR_STAGE.REUSE_EVIDENCE,
  COORDINATOR_STAGE.DISCOVER_GAPS,
  COORDINATOR_STAGE.PAID_MULTISEGMENT,
  COORDINATOR_STAGE.ASSESS_FIT,
  COORDINATOR_STAGE.IDENTIFY_PEOPLE,
  COORDINATOR_STAGE.ENRICH_CONTACTS,
  COORDINATOR_STAGE.BUYER_CONTACT_ENRICHMENT,
  COORDINATOR_STAGE.SYNTHESIZE_HOTEL_RELEVANCE,
  COORDINATOR_STAGE.EVALUATE_ELIGIBILITY,
  COORDINATOR_STAGE.GENERATE_CARDS,
  COORDINATOR_STAGE.PREPARE_MANIFEST,
  COORDINATOR_STAGE.PREVIEW_VERIFY,
  COORDINATOR_STAGE.APPLY_CORPORATE,
  COORDINATOR_STAGE.APPLY_PE,
  COORDINATOR_STAGE.APPLY_PUBLISH,
  COORDINATOR_STAGE.LOCAL_DELIVERY_VERIFY,
  COORDINATOR_STAGE.PRODUCTION_DELIVERY_VERIFY,
  COORDINATOR_STAGE.RECONCILE_RUN,
  COORDINATOR_STAGE.EVALUATE_COMPLETION,
]);

export function isForbiddenNotApplicableReason(reason) {
  const r = String(reason || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  return FORBIDDEN_NOT_APPLICABLE_REASONS.some(
    (f) => r === f || r.includes(f) || f.includes(r)
  );
}

/**
 * Plan-only / brief-only execution cannot satisfy substantive research.
 */
export function isPlanOnlyExecution(mode) {
  const m = String(mode || "").toLowerCase();
  return (
    m === EXECUTION_MODE.PLAN_ONLY ||
    m === "plan_only" ||
    m === "plan" ||
    m === "dry_run_plan" ||
    m === "brief_only" ||
    m === "query_plan_only"
  );
}

/**
 * Build a machine-readable contract document for artifacts / reports.
 */
export function buildFullE2eContractDocument() {
  return {
    version: FULL_E2E_CONTRACT_VERSION,
    generatedFor: "Full GDI E2E",
    distinctions: {
      taskExecution: "Coordinator stage completed or failed",
      researchCoverage: "Declared segment scope satisfied with evidence",
      publication: "Qualified opportunities persisted for hotel-facing use",
      customerDelivery: "Local + production verification of list/drawers/reports/links",
    },
    overallStatuses: E2E_OVERALL_STATUS,
    runModes: E2E_RUN_MODE,
    stages: E2E_STAGE_DEFINITIONS,
    segmentTaxonomy: E2E_SEGMENT_TAXONOMY,
    depthInadequacyRules: DEPTH_INADEQUACY_RULES,
    forbiddenNotApplicableReasons: FORBIDDEN_NOT_APPLICABLE_REASONS,
    requiredDeliverables: E2E_REQUIRED_DELIVERABLES,
    coordinatorStagesFullE2e: FULL_E2E_COORDINATOR_STAGES,
    weeklyRefreshDistinct: true,
    weeklyRefreshNote:
      "Weekly refresh uses the same quality/persistence rules but does not imply full initial market coverage COMPLETE",
    zeroQualifiedProspectsValid: true,
    cardQuotasForbidden: true,
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  };
}

export {
  RESEARCH_SEGMENT,
  RESEARCH_SEGMENT_LABEL,
  APPLICABILITY,
  EXECUTION_MODE,
  SEGMENT_COVERAGE_STATUS,
  TASK_EXECUTION_STATUS,
  COVERAGE_STATUS,
};
