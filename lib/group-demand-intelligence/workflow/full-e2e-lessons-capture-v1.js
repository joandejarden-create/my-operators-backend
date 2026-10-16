/**
 * Full GDI E2E — required lessons capture (Mayflower / San Diego / Renaissance → standard).
 *
 * Defect fixes must update, in the same task:
 *   1. Shared implementation (where applicable)
 *   2. Regression/integration test
 *   3. Lesson register
 *   4. Full E2E runbook + reusable Cursor prompt
 *
 * Track separately (do not claim full incorporation from a status label alone):
 *   Documented → Implemented (shared path) → Tested → Executed successfully → Production-verified
 *
 * A hotel-specific repair is not a completed process improvement until the shared
 * path prevents recurrence or explicitly detects and routes it.
 *
 * Empty incorporatedThisRun is valid when the coordinator declares noNewLessonsThisRun
 * with execution proof (runId + artifact path + revision). A checklist alone is insufficient.
 */

import {
  FULL_E2E_CONTRACT_VERSION,
  FULL_E2E_LESSON_REGISTER,
  LESSON_STATUS,
  rollupLessonStatus,
  summarizeLessonRegister,
} from "./full-e2e-contract-v1.js";

export const LESSONS_CAPTURE_VERSION = "gdi-full-e2e-lessons-capture-v1";

/** Five-axis tracking required for every lesson (user-facing names). */
export const LESSON_PROOF_AXIS = Object.freeze({
  DOCUMENTED: "documented",
  IMPLEMENTED_SHARED_PATH: "implementedSharedPath",
  TESTED: "tested",
  EXECUTED_SUCCESSFULLY: "executedSuccessfully",
  PRODUCTION_VERIFIED: "productionVerified",
});

/**
 * Next-hotel inheritance modes — do not collapse these into a single "yes".
 */
export const NEXT_HOTEL_INHERITANCE_MODE = Object.freeze({
  AUTOMATICALLY_PERFORMS: "automatically_performs",
  AUTOMATICALLY_VALIDATES: "automatically_validates",
  DETECTS_GAP_REQUIRES_OPERATOR: "detects_gap_requires_operator",
  NOT_YET_RELEASED: "not_yet_released",
});

/** Lessons whose shared path actively performs the correction on every run. */
const PERFORMS_IDS = new Set([
  "provider_leak_customer_sanitize",
  "geo_local_connection_preserved",
  "resume_without_paid_repeat",
  "hotel_product_budget_isolation",
  "auth_reconcile_ledger_identity_first",
  "publication_metadata_preservation",
  "pe_schema_link_persistence",
  "false_contact_exhaustion_forbidden",
  "assessment_unknown_vs_unfinished",
  "applicable_segment_declared_depth",
  "contact_depth_with_org_fallback",
  "canonical_writers_no_post_import_repair",
]);

/** Lessons that primarily fail-closed validate / block COMPLETE. */
const VALIDATES_IDS = new Set([
  "missing_and_plan_only_block_complete",
  "completion_from_evidence",
  "delivery_verify_fail_closed",
  "lessons_capture_required",
  "assoc_contacts_not_planner",
  "sports_edu_independent",
  "dedupe_preserve_history",
  "hotel_fit_drawers_before_publish",
]);

/**
 * Shared path detects incompleteness but still needs an operator step
 * (freeze discipline, share mint, PDF visual, continuous parity run).
 */
const OPERATOR_STEP_IDS = new Set([
  "inventory_facing_vs_unique_count",
  "delivery_checks_required",
  "local_production_parity",
  "contact_ownership_hotel_scoped",
]);

/**
 * Normalize internal proof object ↔ five-axis closeout view.
 * Accepts either legacy (ruleImplemented/…) or five-axis field names.
 */
export function normalizeLessonProofAxes(proof = {}) {
  const documented =
    proof.documented !== undefined
      ? proof.documented === true
      : proof.ruleImplemented === true || proof.documented !== false;
  const implementedSharedPath =
    proof.implementedSharedPath !== undefined
      ? proof.implementedSharedPath === true
      : proof.ruleImplemented === true;
  const tested =
    proof.tested !== undefined
      ? proof.tested === true
      : proof.regressionIntegrationVerified === true;
  const executedSuccessfully =
    proof.executedSuccessfully !== undefined
      ? proof.executedSuccessfully === true
      : proof.evidenceGeneratedAutomatically === true &&
        proof.executionPathWired === true;
  const productionVerified =
    proof.productionVerified === undefined ? null : proof.productionVerified;

  const legacy = {
    ruleImplemented: implementedSharedPath,
    executionPathWired:
      proof.executionPathWired !== undefined
        ? proof.executionPathWired === true
        : executedSuccessfully || implementedSharedPath,
    evidenceGeneratedAutomatically:
      proof.evidenceGeneratedAutomatically !== undefined
        ? proof.evidenceGeneratedAutomatically === true
        : executedSuccessfully,
    regressionIntegrationVerified: tested,
    productionVerified,
    notes: proof.notes || null,
  };

  return {
    ...legacy,
    documented,
    implementedSharedPath,
    tested,
    executedSuccessfully,
    productionVerified,
    fiveAxis: {
      [LESSON_PROOF_AXIS.DOCUMENTED]: documented,
      [LESSON_PROOF_AXIS.IMPLEMENTED_SHARED_PATH]: implementedSharedPath,
      [LESSON_PROOF_AXIS.TESTED]: tested,
      [LESSON_PROOF_AXIS.EXECUTED_SUCCESSFULLY]: executedSuccessfully,
      [LESSON_PROOF_AXIS.PRODUCTION_VERIFIED]: productionVerified,
    },
  };
}

/**
 * Next hotel inherits the shared correction when documented + shared impl +
 * wired path + regression test exist — even if productionVerified is still open.
 * Hotel-only repair scripts do NOT set this true.
 */
export function nextHotelInheritsAutomatically(lesson = {}) {
  if (lesson.hotelSpecificRepairOnly === true) return false;
  const p = normalizeLessonProofAxes(lesson.proof || {});
  return (
    p.documented === true &&
    p.implementedSharedPath === true &&
    p.executionPathWired === true &&
    p.tested === true
  );
}

/**
 * Classify how the next hotel receives the correction.
 * Do not claim "no bespoke work" when mode is detects_gap_requires_operator.
 */
export function classifyNextHotelInheritance(lesson = {}) {
  if (lesson.hotelSpecificRepairOnly === true) {
    return {
      mode: NEXT_HOTEL_INHERITANCE_MODE.DETECTS_GAP_REQUIRES_OPERATOR,
      inheritsSharedCorrection: false,
      operatorStep:
        "Do not copy hotel-specific repair scripts; port the defect into the shared path first.",
    };
  }
  const inherits = nextHotelInheritsAutomatically(lesson);
  if (!inherits) {
    return {
      mode: NEXT_HOTEL_INHERITANCE_MODE.NOT_YET_RELEASED,
      inheritsSharedCorrection: false,
      operatorStep:
        "Shared implementation and/or regression test incomplete — next hotel does not inherit yet.",
    };
  }
  const id = String(lesson.id || "");
  if (OPERATOR_STEP_IDS.has(id)) {
    return {
      mode: NEXT_HOTEL_INHERITANCE_MODE.DETECTS_GAP_REQUIRES_OPERATOR,
      inheritsSharedCorrection: true,
      operatorStep: operatorStepForLesson(id),
    };
  }
  if (VALIDATES_IDS.has(id)) {
    return {
      mode: NEXT_HOTEL_INHERITANCE_MODE.AUTOMATICALLY_VALIDATES,
      inheritsSharedCorrection: true,
      operatorStep: null,
    };
  }
  if (PERFORMS_IDS.has(id)) {
    return {
      mode: NEXT_HOTEL_INHERITANCE_MODE.AUTOMATICALLY_PERFORMS,
      inheritsSharedCorrection: true,
      operatorStep: null,
    };
  }
  return {
    mode: NEXT_HOTEL_INHERITANCE_MODE.AUTOMATICALLY_VALIDATES,
    inheritsSharedCorrection: true,
    operatorStep: null,
  };
}

function operatorStepForLesson(id) {
  switch (id) {
    case "inventory_facing_vs_unique_count":
      return "Operator must run inventory freeze + reconcile stored/facing/unique counts into the run ledger (blockers alone do not assemble the freeze artifact).";
    case "delivery_checks_required":
      return "Operator must mint hotel-scoped share capability (crypto secret) and obtain PDF/report surfaces when product routes exist; list/CSV alone ≠ full delivery proof.";
    case "local_production_parity":
      return "Operator must run local↔production parity diagnose against the declared local process and production URL for this hotel.";
    case "contact_ownership_hotel_scoped":
      return "Operator must confirm hotel-scoped candidate dirs for the run; cross-hotel bleed audit is not fully automated.";
    default:
      return "Operator step required — see lesson register notes.";
  }
}

/**
 * Enrich a register row for closeout / capture artifacts.
 */
export function enrichLessonForCloseout(lesson = {}) {
  const proof = normalizeLessonProofAxes(lesson.proof || {});
  const status = lesson.status || rollupLessonStatus(proof);
  const inherits = nextHotelInheritsAutomatically({ ...lesson, proof });
  const inheritance = classifyNextHotelInheritance({ ...lesson, proof });
  return {
    id: lesson.id,
    title: lesson.title,
    defect: lesson.defect || null,
    hotels: lesson.hotels || [],
    implementation: lesson.implementation || null,
    test: lesson.test || null,
    release: lesson.release || null,
    executionEvidence: lesson.executionEvidence || null,
    status,
    proof,
    fiveAxis: proof.fiveAxis,
    nextHotelInheritsAutomatically: inherits,
    nextHotelInheritanceMode: inheritance.mode,
    nextHotelOperatorStep: inheritance.operatorStep,
    processImprovementComplete:
      status === LESSON_STATUS.IMPLEMENTED_AND_TESTED &&
      inherits &&
      inheritance.mode !==
        NEXT_HOTEL_INHERITANCE_MODE.DETECTS_GAP_REQUIRES_OPERATOR,
  };
}

/**
 * Build the required Full E2E closeout lesson section.
 *
 * @param {object} [opts]
 * @param {string[]} [opts.incorporatedThisRunIds]
 * @param {boolean} [opts.noNewLessonsThisRun]
 * @param {object} [opts.coordinatorExecutionProof]
 * @param {object[]} [opts.register]
 * @param {string} [opts.runId]
 * @param {string} [opts.hotelId]
 * @param {string} [opts.releaseRef]
 */
export function buildLessonCloseoutSection(opts = {}) {
  const register = (opts.register || FULL_E2E_LESSON_REGISTER).map(
    enrichLessonForCloseout
  );
  const incorporatedIds = new Set(
    (opts.incorporatedThisRunIds || []).map((id) => String(id))
  );
  const incorporatedThisRun = register.filter((l) => incorporatedIds.has(l.id));
  const remainingGaps = register.filter(
    (l) => l.status !== LESSON_STATUS.IMPLEMENTED_AND_TESTED
  );
  const byMode = {
    [NEXT_HOTEL_INHERITANCE_MODE.AUTOMATICALLY_PERFORMS]: [],
    [NEXT_HOTEL_INHERITANCE_MODE.AUTOMATICALLY_VALIDATES]: [],
    [NEXT_HOTEL_INHERITANCE_MODE.DETECTS_GAP_REQUIRES_OPERATOR]: [],
    [NEXT_HOTEL_INHERITANCE_MODE.NOT_YET_RELEASED]: [],
  };
  for (const l of register) {
    const mode =
      l.nextHotelInheritanceMode ||
      NEXT_HOTEL_INHERITANCE_MODE.NOT_YET_RELEASED;
    if (!byMode[mode]) byMode[mode] = [];
    byMode[mode].push({
      id: l.id,
      operatorStep: l.nextHotelOperatorStep || null,
    });
  }

  const noNew =
    opts.noNewLessonsThisRun === true ||
    (incorporatedThisRun.length === 0 && opts.noNewLessonsThisRun !== false);

  return {
    version: LESSONS_CAPTURE_VERSION,
    contractVersion: FULL_E2E_CONTRACT_VERSION,
    at: new Date().toISOString(),
    runId: opts.runId || null,
    hotelId: opts.hotelId || null,
    releaseRef: opts.releaseRef || null,
    noNewLessonsThisRun: noNew && incorporatedThisRun.length === 0,
    coordinatorExecutionProof: opts.coordinatorExecutionProof || null,
    summary: summarizeLessonRegister(
      (opts.register || FULL_E2E_LESSON_REGISTER).map((l) => ({
        ...l,
        status: enrichLessonForCloseout(l).status,
      }))
    ),
    lessonsIncorporatedThisRun: incorporatedThisRun.map((l) => ({
      id: l.id,
      title: l.title,
      status: l.status,
      implementation: l.implementation,
      test: l.test,
      release: l.release,
      executionEvidence: l.executionEvidence,
      fiveAxis: l.fiveAxis,
      nextHotelInheritsAutomatically: l.nextHotelInheritsAutomatically,
      nextHotelInheritanceMode: l.nextHotelInheritanceMode,
    })),
    remainingImplementationOrVerificationGaps: remainingGaps.map((l) => ({
      id: l.id,
      status: l.status,
      fiveAxis: l.fiveAxis,
      notes: l.proof?.notes || null,
      nextHotelInheritsAutomatically: l.nextHotelInheritsAutomatically,
      nextHotelInheritanceMode: l.nextHotelInheritanceMode,
      nextHotelOperatorStep: l.nextHotelOperatorStep,
      missingAxes: Object.entries(l.fiveAxis || {})
        .filter(([, v]) => v !== true && v !== null)
        .map(([k]) => k)
        .concat(
          l.fiveAxis?.productionVerified === false
            ? [LESSON_PROOF_AXIS.PRODUCTION_VERIFIED]
            : []
        ),
    })),
    nextHotelInheritance: {
      inheritsAutomatically: register
        .filter((l) => l.nextHotelInheritsAutomatically === true)
        .map((l) => l.id),
      byMode,
      doesNotInheritYet: register
        .filter((l) => l.nextHotelInheritsAutomatically !== true)
        .map((l) => ({
          id: l.id,
          mode: l.nextHotelInheritanceMode,
          reason:
            l.hotelSpecificRepairOnly === true
              ? "hotel_specific_repair_only"
              : "shared_path_or_test_incomplete",
          status: l.status,
        })),
      rule:
        "Distinguish automatically_performs vs automatically_validates vs detects_gap_requires_operator vs not_yet_released. Shared correction inheritance ≠ zero operator evidence assembly.",
    },
    register,
  };
}

function hasCoordinatorExecutionProof(proof) {
  if (!proof || typeof proof !== "object") return false;
  return Boolean(
    proof.runId &&
      proof.hotelId &&
      proof.artifactPath &&
      proof.coordinatorStage === "capture_lessons" &&
      (proof.gitSha || proof.releaseRef || proof.contractVersion)
  );
}

/**
 * Evaluate whether lessons capture for this run is complete enough to pass
 * the required Full E2E stage / gate.
 *
 * Incomplete capture blocks COMPLETE (not a soft warning).
 */
export function evaluateLessonsCaptureCompleteness(capture = {}) {
  const blockers = [];
  const reasons = [];

  if (!capture || typeof capture !== "object") {
    blockers.push({ code: "lessons_capture_missing" });
    reasons.push(
      "Lessons capture artifact missing — required on Full E2E completion path"
    );
    return { ok: false, incomplete: true, blockers, reasons };
  }

  if (
    capture.version !== LESSONS_CAPTURE_VERSION &&
    !String(capture.version || "").startsWith("gdi-full-e2e-lessons-capture")
  ) {
    blockers.push({ code: "lessons_capture_version_unknown" });
    reasons.push(`Unknown lessons-capture version: ${capture.version}`);
  }

  if (!Array.isArray(capture.register) || capture.register.length === 0) {
    blockers.push({ code: "lessons_capture_register_empty" });
    reasons.push("Lessons capture must include the canonical lesson register");
  }

  if (
    !capture.nextHotelInheritance ||
    !Array.isArray(capture.nextHotelInheritance.inheritsAutomatically)
  ) {
    blockers.push({ code: "lessons_capture_inheritance_missing" });
    reasons.push(
      "Lessons capture must state which corrections next hotel inherits automatically"
    );
  }

  if (
    !Array.isArray(capture.remainingImplementationOrVerificationGaps) &&
    !Array.isArray(capture.remainingGaps)
  ) {
    blockers.push({ code: "lessons_capture_gaps_missing" });
    reasons.push(
      "Lessons capture must list remaining implementation/verification gaps"
    );
  }

  const incorporated = capture.lessonsIncorporatedThisRun || [];
  const noNew = capture.noNewLessonsThisRun === true;

  if (incorporated.length === 0) {
    if (!noNew) {
      blockers.push({ code: "lessons_capture_no_new_undeclared" });
      reasons.push(
        "No lessons incorporated this run — must set noNewLessonsThisRun=true with coordinator execution proof (do not invent lessons)"
      );
    }
    if (!hasCoordinatorExecutionProof(capture.coordinatorExecutionProof)) {
      blockers.push({ code: "lessons_capture_execution_proof_missing" });
      reasons.push(
        "Lessons capture requires coordinatorExecutionProof (runId, hotelId, artifactPath, capture_lessons stage, revision) — a checklist alone is insufficient"
      );
    }
  }

  // Hotel-specific repairs claimed as process improvements without shared path.
  for (const row of capture.hotelSpecificRepairsClaimedComplete || []) {
    if (row?.sharedPathPreventsRecurrence !== true) {
      blockers.push({
        code: "hotel_repair_not_process_improvement",
        lessonId: row?.id || null,
      });
      reasons.push(
        `Hotel-specific repair ${row?.id || "(unknown)"} claimed complete without shared-path prevention/detection`
      );
    }
  }

  // Defects fixed this run must be listed with code/test/register refs.
  for (const row of incorporated) {
    if (!row?.implementation || !row?.test) {
      blockers.push({
        code: "lesson_missing_code_or_test_ref",
        lessonId: row?.id || null,
      });
      reasons.push(
        `Lesson ${row?.id || "(unknown)"} incorporated this run without implementation and test references`
      );
    }
  }

  return {
    ok: blockers.length === 0,
    incomplete: blockers.length > 0,
    blockers,
    reasons,
  };
}

/**
 * Build the durable run artifact for stage P / capture_lessons.
 */
export function buildLessonsCaptureArtifact(opts = {}) {
  const closeout = buildLessonCloseoutSection(opts);
  const evaluation = evaluateLessonsCaptureCompleteness(closeout);
  return {
    ...closeout,
    evaluation,
    requiredOnCompletionPath: true,
    hotelSpecificRepairNote:
      "A hotel-specific repair is not a completed process improvement until the shared path prevents recurrence or explicitly detects and routes it.",
  };
}

/**
 * Assert a defect-fix task payload includes the four required surfaces.
 */
export function assertDefectFixIncludesLessonCapture(fix = {}) {
  const missing = [];
  if (!fix.sharedImplementation && fix.sharedImplementationApplicable !== false) {
    missing.push("sharedImplementation");
  }
  if (!fix.regressionOrIntegrationTest) missing.push("regressionOrIntegrationTest");
  if (!fix.lessonRegisterUpdated) missing.push("lessonRegisterUpdated");
  if (!fix.runbookAndCursorPromptUpdated) {
    missing.push("runbookAndCursorPromptUpdated");
  }
  return {
    ok: missing.length === 0,
    missing,
    reasons:
      missing.length === 0
        ? []
        : [
            `Defect fix incomplete for lessons capture — missing: ${missing.join(", ")}. Do not defer to another founder prompt or hotel-only report.`,
          ],
  };
}
