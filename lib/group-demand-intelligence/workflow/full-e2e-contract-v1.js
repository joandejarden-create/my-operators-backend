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

export const FULL_E2E_CONTRACT_VERSION = "gdi-full-e2e-contract-v1.5";
/** Prior released versions — evaluators accept during transition. */
export const FULL_E2E_CONTRACT_VERSION_PRIOR = "gdi-full-e2e-contract-v1.4";
export const FULL_E2E_CONTRACT_VERSION_V1_3 = "gdi-full-e2e-contract-v1.3";
export const FULL_E2E_CONTRACT_VERSION_V1 = "gdi-full-e2e-contract-v1";

/**
 * Lesson incorporation status — only IMPLEMENTED_AND_TESTED counts as
 * part of the proven standard process. Do NOT mark IMPLEMENTED_AND_TESTED
 * solely because evaluator fixtures reject explicit failure flags.
 */
export const LESSON_STATUS = Object.freeze({
  IMPLEMENTED_AND_TESTED: "IMPLEMENTED_AND_TESTED",
  IMPLEMENTED_NOT_VERIFIED: "IMPLEMENTED_NOT_VERIFIED",
  PARTIALLY_WIRED: "PARTIALLY_WIRED",
  DOCUMENTED_ONLY: "DOCUMENTED_ONLY",
  BLOCKED: "BLOCKED",
});

/**
 * Per-lesson proof axes. Track separately (closeout five-axis names in
 * full-e2e-lessons-capture-v1.js):
 *   1. Documented
 *   2. Implemented in the shared path
 *   3. Tested
 *   4. Executed successfully
 *   5. Production-verified (where applicable)
 *
 * Acceptance is a working evidenced process — not a register where every
 * row shares the same status. Each lesson must reference code, test,
 * execution evidence, and release.
 *
 * @typedef {{
 *   documented?: boolean,
 *   ruleImplemented: boolean,
 *   executionPathWired: boolean,
 *   evidenceGeneratedAutomatically: boolean,
 *   regressionIntegrationVerified: boolean,
 *   productionVerified: boolean | null,
 *   notes?: string,
 * }} LessonProofAxes
 */

/**
 * Roll up proof axes → lesson status.
 * productionVerified=null means N/A (fixture/heuristic rule only).
 * Do not claim full incorporation from a status label alone — inspect axes.
 */
export function rollupLessonStatus(proof = {}) {
  const documented =
    proof.documented !== undefined ? proof.documented === true : true;
  const ruleImplemented =
    proof.ruleImplemented === true || proof.implementedSharedPath === true;
  const executionPathWired = proof.executionPathWired === true;
  const evidenceGeneratedAutomatically =
    proof.evidenceGeneratedAutomatically === true ||
    proof.executedSuccessfully === true;
  const regressionIntegrationVerified =
    proof.regressionIntegrationVerified === true || proof.tested === true;
  const productionVerified =
    proof.productionVerified === undefined ? null : proof.productionVerified;

  if (!documented && !ruleImplemented) return LESSON_STATUS.DOCUMENTED_ONLY;
  if (!ruleImplemented) return LESSON_STATUS.DOCUMENTED_ONLY;
  if (!executionPathWired) return LESSON_STATUS.PARTIALLY_WIRED;
  const prodOk = productionVerified === null || productionVerified === true;
  if (
    evidenceGeneratedAutomatically &&
    regressionIntegrationVerified &&
    prodOk
  ) {
    return LESSON_STATUS.IMPLEMENTED_AND_TESTED;
  }
  return LESSON_STATUS.IMPLEMENTED_NOT_VERIFIED;
}

function lesson(row) {
  const proof = {
    documented: true,
    ...(row.proof || {}),
  };
  const status = row.status || rollupLessonStatus(proof);
  return Object.freeze({
    ...row,
    proof: Object.freeze(proof),
    status,
    executionEvidence: row.executionEvidence || null,
  });
}

/**
 * Mayflower / San Diego / Renaissance → standard Full E2E lessons.
 * Keep ids stable; proof axes updated when wiring/evidence/production land.
 */
export const FULL_E2E_LESSON_REGISTER = Object.freeze([
  lesson({
    id: "applicable_segment_declared_depth",
    title: "Research every applicable segment to a declared depth",
    defect: "Plan-only / label-only researched treated as coverage",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "assessApplicableSegmentCompletion + DEPTH_INADEQUACY_RULES + E2E_SEGMENT_TAXONOMY",
    test: "test-gdi-full-e2e-contract-v1.mjs (plan_only, researched label, depth evidence)",
    release: "PR #119 / v1.1+",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Wired into coordinator evaluate_completion; Ren/Mayflower/SD runs produce segment axes",
    },
  }),
  lesson({
    id: "missing_and_plan_only_block_complete",
    title: "Missing segments and plan-only stages prevent COMPLETE",
    defect: "Empty/omitted axes or plan_only still COMPLETE",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation: "validateSegmentCoverageAxes + isPlanOnlyExecution",
    test: "empty/omitted/plan_only adversarial cases",
    release: "PR #119 / v1.1+",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Evaluator blocks COMPLETE; live Ren remains PARTIAL when axes incomplete",
    },
  }),
  lesson({
    id: "assoc_contacts_not_planner",
    title: "Association contacts do not satisfy planner discovery",
    defect: "Association named contacts counted as planner coverage",
    hotels: ["mayflower", "renaissance"],
    implementation: "planner axis + assoc_contact_not_planner_coverage rule",
    test: "planner via association contacts → PARTIAL",
    release: "PR #119 / v1.1+",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: null,
      notes: "Fixture + evaluator; production planner depth still open on many accounts",
    },
  }),
  lesson({
    id: "sports_edu_independent",
    title: "Sports and educational coverage require independent assessment",
    defect:
      "Proxy satisfaction OR silent non-admission of borrowed segment research",
    hotels: ["renaissance"],
    implementation:
      "hasSupportedIndependentSegmentAssessment + evaluateIndependentSegmentAxes",
    test: "proxy / missing independent evidence → incomplete; dedicated provider runs pass",
    release: "v1.2 / corrected v1.4",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Requires supported independent evidence (not anti-proxy silence). Completion-input stamps independentAssessment from dedicated provider runs. Ren sports/edu gap-fill ran; production Full E2E COMPLETE not claimed",
    },
  }),
  lesson({
    id: "contact_depth_with_org_fallback",
    title: "Contact depth is mandatory even when an organization fallback exists",
    defect: "Org route skipped contact-depth attempt",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "CONTACT_QUEUE_DISPOSITIONS + evaluateContactDepthOrgRoutePolicy + buyer-contact-enrichment-v1",
    test: "org fallback after documented attempt; skip-for-org blocked; false EXHAUSTED without sources rejected",
    release: "PR #119 + v1.2 / Ren continuation",
    executionEvidence:
      "reports/gdi-six-hotel-e2e/renaissance/continuation/complete_v1/04b-false-exhaust-revert.json",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Queue dispositions wired; false EXHAUSTED-without-sources fixed in shared enrichment. Named-buyer depth still open for majority of Ren accounts — org route ≠ named research complete",
    },
  }),
  lesson({
    id: "hotel_fit_drawers_before_publish",
    title: "Structured hotel-fit assessments and complete drawers before publication",
    defect: "Lean packages published without fit tiles / incomplete drawers; unknown facts treated as unfinished work",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "assertPublicationWriteReadiness + evaluatePublicationQuality + apply_publish + PE promote gate",
    test: "publication without fit/drawers blocked; never_executed ≠ provisional/insufficient_evidence",
    release: "v1.2 / hardened v1.4–v1.5",
    executionEvidence:
      "reports/gdi-six-hotel-e2e/renaissance/continuation/complete_v1/03-hotel-fit-batch.json",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: false,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Readiness distinguishes unfinished assessment from honest unknown/provisional after assessment. Ren 42/42 facing have structured fit; some lean drawer prose still thin",
    },
  }),
  lesson({
    id: "canonical_writers_no_post_import_repair",
    title: "Canonical writers must work without post-import eligibility repairs",
    defect:
      "upsert omitted eligibility fields; prose whyThisMatters/proposedHotelUse treated as evidence",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "assertCanonicalWriteEligibilityFields → evaluateProspectSuitability + corporate-package-persist",
    test: "prose-only fails; use-case suitability required; repair-required blocks COMPLETE",
    release: "v1.2 / corrected v1.4",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: false,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Prose-presence shortcut removed; use-case contract enforced on corporate persist. Not all persist paths emit eligibilityWriteComplete automatically yet",
    },
  }),
  lesson({
    id: "dedupe_preserve_history",
    title: "New records are not necessarily new opportunities; reconcile duplicates",
    defect: "Create counted as net-new without identity reconcile; lineage alone left duplicate customer cards",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "derivePublicationQualityFromPersistOutputs + supersession/merge + evaluatePublicationQuality identityDedupeReconciled",
    test: "unreconciled identity → not COMPLETE; supersession preserves history",
    release: "v1.2 / hardened v1.4 / Ren SSSP",
    executionEvidence:
      "reports/gdi-six-hotel-e2e/renaissance/continuation/complete_v1/02-sssp-duplicate-resolution.json",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: false,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "SSSP/76th used shared supersession (legacy withdrawn, canon linked). Dedupe prefers change-ledger/import outputs. Facing count ≠ unique sales opportunity count — report both",
    },
  }),
  lesson({
    id: "delivery_checks_required",
    title: "Required reports, PDFs, links, local and production checks are delivery",
    defect: "gapRecorded / not_run verification still COMPLETE",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation: "E2E_REQUIRED_DELIVERABLES + verification adapters",
    test: "gap deliverable + not_run / stale verification adversarial",
    release: "PR #119 / v1.1+",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes: "Adapters wired; clean-main deploy of v1.5 landed (PR #123 / c8278607 / Railway 34cf42a7). Full machine delivery verify still PARTIAL on Ren (share crypto, PDF route).",
    },
  }),
  lesson({
    id: "completion_from_evidence",
    title: "Completion derives from current execution evidence, not manual status",
    defect: "operator passed overrode failed machine checks",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation: "normalizeVerification + operator override blockers",
    test: "manual passed vs failed machine → FAILED",
    release: "PR #119 / v1.1+",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: null,
      notes: "Machine-wins merge in completion-input; fixture-proven",
    },
  }),
  lesson({
    id: "resume_without_paid_repeat",
    title: "Resume revisits unfinished/stale stages without repeating paid research",
    defect: "status=completed skipped stale verify; or re-paid on resume",
    hotels: ["renaissance"],
    implementation: "verificationNeedsRerun + fingerprint invalidation",
    test: "verificationNeedsRerun + buildCompletionInputFromRun read-only",
    release: "PR #119 / v1.1+",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Ren gap-fill resumed under same $12 auth without re-paying completed MS jobs",
    },
  }),
  lesson({
    id: "hotel_product_budget_isolation",
    title: "Hotel/product budgets remain separate",
    defect: "Copied/combined budget fields marked reconciled",
    hotels: ["renaissance"],
    implementation: "reconcileAuthoritativeBudget + evaluateBudgetIsolation",
    test: "non-authoritative budget + cross-scope combined blocked",
    release: "PR #119 + v1.2",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Ren auth_gdi_renaissance_ts_12usd_v1 ledger-scoped; cross-product blocked in evaluator",
    },
  }),
  lesson({
    id: "delivery_verify_fail_closed",
    title: "Delivery verification fails closed on schema, hotel, manifest, leaks, and build identity",
    defect:
      "HTTP 200 / empty JSON fallback / caller-supplied SHA / noted leaks treated as passed",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "runLocalDeliveryVerification + runProductionDeliveryVerification (parseJsonBodyStrict, publication reconcile, list/detail, observeRuntimeBuildIdentity)",
    test:
      "test-gdi-full-e2e-contract-v1.mjs cases 35–40, 43",
    release: "v1.3 / fix/gdi-full-e2e-verify-harden",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Rule + adapters + fixture regressions land on this branch. Code on production (Railway 34cf42a7). Full delivery-adapter productionVerified still open until share/PDF machine proofs pass.",
    },
  }),
  lesson({
    id: "auth_reconcile_ledger_identity_first",
    title: "Budget reconcile reads ledger authorization before comparing the request",
    defect:
      "authorizationId || ledger.authorization made mismatch checks ineffective; missing identity could pass",
    hotels: ["renaissance"],
    implementation:
      "reconcileAuthoritativeBudget ledger-first identity + hotel/product/purpose/cap/amount fail-closed",
    test: "test-gdi-full-e2e-contract-v1.mjs cases 41–42",
    release: "v1.3 / fix/gdi-full-e2e-verify-harden",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Ledger-first reconcile used for Ren $12 auth; mismatch fixtures pass",
    },
  }),
  lesson({
    id: "geo_local_connection_preserved",
    title: "Geography checks preserve localConnection; hotel catchment defines foreign markets",
    defect:
      "Disposition geo probe dropped localConnection; generic foreign patterns treated hotel catchment tokens as foreign",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "blobOf + disposition geo probe preserve localConnection; hotel-specific local/foreign patterns",
    test: "test-gdi-full-e2e-contract-v1.mjs cases 44–45",
    release: "v1.3 / fix/gdi-full-e2e-verify-harden",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Mayflower DC / SD / Ren Midtown catchment patterns; Ren redispose preserved localConnection",
    },
  }),
  lesson({
    id: "pe_schema_link_persistence",
    title: "Private-events schema and hotel-link persistence survive canonical apply",
    defect: "PE venue/fit/opportunity links dropped or duplicate-created on rerun",
    hotels: ["mayflower", "san_diego"],
    implementation: "apply_pe_canonical + persistVenuePartnershipPackages + drawerProof",
    test: "PE rerun UPDATE-only; peVenueId + VENUE_PARTNERSHIP detailLayout",
    release: "Mayflower / SD operational baseline",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Mayflower PE canonical persist + drawer proof; Ren PE packages optional/absent",
    },
  }),
  lesson({
    id: "publication_metadata_preservation",
    title: "Publication basis and identity metadata preserved across enrichment writes",
    defect: "Partial enrichment cleared REVIEWED_PACKAGE_V1 / facing flags",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation: "preservePublicationMetadata + REVIEWED_PACKAGE_V1 allowlist",
    test: "buyer-contact enrichment preservePublicationMetadata regressions",
    release: "prospect publication eligibility path",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes: "Mayflower/SD/Ren on REVIEWED_PROSPECT_HOTEL_IDS; apply_publish enables reviewed flag",
    },
  }),
  lesson({
    id: "contact_ownership_hotel_scoped",
    title: "Contact ownership stays hotel-scoped; no cross-hotel contact bleed",
    defect: "Contacts reused across hotels without ownership scoping",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation: "hotelId-scoped candidate storage + buyer enrichment queue",
    test: "buyer enrichment hotelId filter; candidate dir under hotel run",
    release: "contact depth / buyer enrichment",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes: "Run-scoped candidate dirs wired; cross-hotel bleed audit not fully automated in Full E2E",
    },
  }),
  lesson({
    id: "local_production_parity",
    title: "Local preview and production customer surfaces stay parity-checked",
    defect: "Local FS facing set diverged from production Airtable list",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "local_delivery_verify + production_delivery_verify + gdi-local-storage-parity-diagnose",
    test: "delivery adapters list/detail; parity diagnose script",
    release: "v1.1+ / verify-harden v1.3",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: false,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Adapters + diagnose script exist. Continuous parity evidence still operator-run; clean-main deploy of verify-harden pending for Ren production verify",
    },
  }),
  lesson({
    id: "provider_leak_customer_sanitize",
    title: "Internal provider metadata never reaches customer list/detail/share DTOs",
    defect:
      "evidence.researchProvider=webhound, webhoundUsed, WEBHOUND_* channel sources leaked on customer detail",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "customer-presentation-sanitize-v3 (strip provider keys/enums; preserve source URLs) + detectProviderMetadataLeaks",
    test: "test:gdi-customer-provider-leak-sanitize-v3; delivery verify fail-closed on leaks",
    release: "v1.5 / fix/gdi-full-e2e-verify-harden",
    executionEvidence:
      "PR #123 merge c8278607; Railway 34cf42a7 SUCCESS; 10-postdeploy-leak-scan.json LEAKS=0",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: true,
      notes:
        "Shared sanitize on authenticated list/detail path. Ren production detail re-scan after PR #123 / c8278607 / Railway 34cf42a7: 0 leaks (was 7). Evidence: 10-postdeploy-leak-scan.json",
    },
  }),
  lesson({
    id: "false_contact_exhaustion_forbidden",
    title: "Do not mark contact research EXHAUSTED without documented attempts/sources",
    defect: "Buyer enrichment stamped EXHAUSTED for all remaining accounts to close the stage",
    hotels: ["renaissance", "mayflower", "san_diego"],
    implementation:
      "buyer-contact-enrichment-v1 + buyer-contact-enrichment-contract-v1 (researchAttempted gate)",
    test: "false exhaust revert + contract axes; silent not_attempted→exhausted blocks COMPLETE",
    release: "v1.4–v1.5 / Ren continuation",
    executionEvidence:
      "reports/gdi-six-hotel-e2e/renaissance/continuation/complete_v1/04b-false-exhaust-revert.json",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes: "Shared enrichment no longer invents exhaustion; evaluator already fails silent transitions",
    },
  }),
  lesson({
    id: "inventory_facing_vs_unique_count",
    title: "Report stored, facing, and unique sales opportunities separately",
    defect: "API count / enriched rows treated as net-new discovery or unique opportunities",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "inventory freeze + change ledger + closeout metrics (stored ≠ facing ≠ unique keys)",
    test: "identity_count_inconsistent blocker; Ren freeze/reconcile artifacts",
    release: "v1.5 / Ren continuation closeout",
    executionEvidence:
      "reports/gdi-six-hotel-e2e/renaissance/CLOSEOUT_A_K.md §F; INVENTORY_FREEZE_CONTINUATION.json",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: false,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes:
        "Contract/evaluator identity count checks exist; closeout discipline required. Continuous auto evidence from every freeze still partial",
    },
  }),
  lesson({
    id: "assessment_unknown_vs_unfinished",
    title: "Unknown facts after assessment are not unfinished assessment work",
    defect:
      "Provisional/insufficient_evidence/unknown room volume blocked otherwise-qualified publication",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation: "assertPublicationWriteReadiness never_executed vs provisional path",
    test: "test-gdi-customer-provider-leak-sanitize-v3 readiness cases; full-e2e readiness fixtures",
    release: "v1.5",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: false,
      notes: "Shared readiness gate; production publication verify after clean-main deploy still open",
    },
  }),
  lesson({
    id: "lessons_capture_required",
    title: "Lessons capture is a required Full E2E completion stage",
    defect:
      "Defect fixes left only in hotel reports or deferred to another founder prompt",
    hotels: ["mayflower", "san_diego", "renaissance"],
    implementation:
      "full-e2e-lessons-capture-v1 + stage P_LESSONS_CAPTURE + evaluateLessonsCaptureCompleteness",
    test: "test-gdi-full-e2e-contract-v1.mjs lessons-capture cases; assertDefectFixIncludesLessonCapture",
    release: "v1.5",
    proof: {
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: true,
      regressionIntegrationVerified: true,
      productionVerified: null,
      notes:
        "Capture required on completion path. Hotel-specific repair ≠ process improvement until shared path prevents/detects recurrence",
    },
  }),
]);

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
  P_LESSONS_CAPTURE: "P_lessons_capture",
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
  CAPTURE_LESSONS: "capture_lessons",
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
    coordinatorStages: [COORDINATOR_STAGE.RECONCILE_RUN],
    inputs: ["ledgers", "scorecards", "change ledger"],
    outputs: ["reconciliation", "backlog seed"],
    evidenceRequired: ["budget_reconciled"],
    executionModes: ["always"],
    sideEffects: "artifact writes under run dir",
    authorization: "none",
    failurePartial: "unresolved reservations/holds recorded; may force PARTIAL",
    retryResume: "idempotent reconcile",
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  },
  {
    id: E2E_STAGE.P_LESSONS_CAPTURE,
    letter: "P",
    title: "Capture and reconcile process lessons into the shared standard",
    coordinatorStages: [
      COORDINATOR_STAGE.CAPTURE_LESSONS,
      COORDINATOR_STAGE.EVALUATE_COMPLETION,
    ],
    inputs: [
      "defects found this run",
      "FULL_E2E_LESSON_REGISTER",
      "shared code/test/runbook/prompt diffs",
    ],
    outputs: [
      "lessons-capture artifact",
      "closeout: incorporated / remaining gaps / next-hotel inheritance",
    ],
    evidenceRequired: [
      "lessons_capture_artifact",
      "lesson_register_updated_for_defects",
      "next_hotel_inheritance_declared",
    ],
    executionModes: ["always"],
    sideEffects: "artifact writes under run dir; register/docs updates in same PR",
    authorization: "none",
    failurePartial:
      "missing lessons capture or hotel-only repair claimed as process improvement → cannot COMPLETE",
    retryResume: "idempotent capture from register + incorporatedThisRunIds",
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
  {
    id: "sports_proxy_not_independent",
    description:
      "Exhibitor or association proxy does not complete independent sports coverage",
  },
  {
    id: "educational_proxy_not_independent",
    description:
      "Sports/medical/association proxy does not complete independent educational/tour coverage",
  },
  {
    id: "org_route_skips_contact_depth",
    description:
      "An organization contact route does not skip required contact-depth research",
  },
]);

/**
 * Expected coverage axes for Full E2E — every axis needs exactly one
 * applicability decision. Empty coverage ≠ no research needed.
 */
export const E2E_EXPECTED_SEGMENT_IDS = Object.freeze(
  E2E_SEGMENT_TAXONOMY.map((s) => s.id)
);

export function isKnownSegmentId(id) {
  return E2E_EXPECTED_SEGMENT_IDS.includes(String(id || "").trim());
}

/** Valid dispositions for required contact-depth queue items. */
export const CONTACT_QUEUE_DISPOSITIONS = Object.freeze([
  "named_relevant_contact_found",
  "referral_lead_found",
  "public_research_exhausted_org_fallback",
  "further_enrichment_pending",
  "blocked",
  "not_attempted",
]);

/**
 * Customer deliverables that must be verified.
 * gap/unsupported/missing/broken/not_run never satisfy COMPLETE.
 * allowNotApplicable: status=not_applicable + supported reason only.
 */
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
    allowNotApplicable: true,
    allowGapIfUnsupported: true, // legacy alias — evaluator requires not_applicable+reason
  },
  {
    id: "exports",
    label: "Appropriate exports",
    required: true,
    allowNotApplicable: true,
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
  COORDINATOR_STAGE.CAPTURE_LESSONS,
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
    lessonRegister: FULL_E2E_LESSON_REGISTER,
    lessonStatusEnum: LESSON_STATUS,
    lessonRegisterSummary: summarizeLessonRegister(),
    lessonsCaptureRequired: true,
    lessonsCaptureModule: "full-e2e-lessons-capture-v1",
    lessonProofAxesRequired: [
      "documented",
      "implementedSharedPath",
      "tested",
      "executedSuccessfully",
      "productionVerified",
    ],
    weeklyRefreshDistinct: true,
    weeklyRefreshNote:
      "Weekly refresh uses the same quality/persistence rules but does not imply full initial market coverage COMPLETE",
    zeroQualifiedProspectsValid: true,
    cardQuotasForbidden: true,
    standardEntryPoint:
      "node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <id> --mode full-e2e …",
    hotelSpecificRepairScriptsForbidden: true,
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  };
}

/** Lessons that count as incorporated into the proven standard process. */
export function listProvenLessons() {
  return FULL_E2E_LESSON_REGISTER.filter(
    (l) => l.status === LESSON_STATUS.IMPLEMENTED_AND_TESTED
  );
}

/** Honest lesson summary — do not collapse all rows to one status. */
export function summarizeLessonRegister(register = FULL_E2E_LESSON_REGISTER) {
  const byStatus = {};
  for (const l of register) {
    byStatus[l.status] = (byStatus[l.status] || 0) + 1;
  }
  return {
    total: register.length,
    proven: listProvenLessons().length,
    byStatus,
    incomplete: register
      .filter((l) => l.status !== LESSON_STATUS.IMPLEMENTED_AND_TESTED)
      .map((l) => ({
        id: l.id,
        status: l.status,
        proof: l.proof || null,
        notes: l.proof?.notes || null,
      })),
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
