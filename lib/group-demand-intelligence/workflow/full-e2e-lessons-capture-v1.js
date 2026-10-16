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
 * Next hotel inherits automatically when the shared path implements + wires +
 * tests the correction — even if this hotel's production verify is still open.
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
 * Enrich a register row for closeout / capture artifacts.
 */
export function enrichLessonForCloseout(lesson = {}) {
  const proof = normalizeLessonProofAxes(lesson.proof || {});
  const status = lesson.status || rollupLessonStatus(proof);
  const inherits = nextHotelInheritsAutomatically({ ...lesson, proof });
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
    processImprovementComplete:
      status === LESSON_STATUS.IMPLEMENTED_AND_TESTED && inherits,
  };
}

/**
 * Build the required Full E2E closeout lesson section.
 *
 * @param {object} [opts]
 * @param {string[]} [opts.incorporatedThisRunIds] — lesson ids touched/fixed this run
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
  const inheritedByNextHotel = register.filter(
    (l) => l.nextHotelInheritsAutomatically === true
  );
  const notInherited = register.filter(
    (l) => l.nextHotelInheritsAutomatically !== true
  );

  return {
    version: LESSONS_CAPTURE_VERSION,
    contractVersion: FULL_E2E_CONTRACT_VERSION,
    at: new Date().toISOString(),
    runId: opts.runId || null,
    hotelId: opts.hotelId || null,
    releaseRef: opts.releaseRef || null,
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
    })),
    remainingImplementationOrVerificationGaps: remainingGaps.map((l) => ({
      id: l.id,
      status: l.status,
      fiveAxis: l.fiveAxis,
      notes: l.proof?.notes || null,
      nextHotelInheritsAutomatically: l.nextHotelInheritsAutomatically,
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
      inheritsAutomatically: inheritedByNextHotel.map((l) => l.id),
      doesNotInheritYet: notInherited.map((l) => ({
        id: l.id,
        reason:
          l.hotelSpecificRepairOnly === true
            ? "hotel_specific_repair_only"
            : "shared_path_or_test_incomplete",
        status: l.status,
      })),
      rule:
        "Next hotel inherits when documented + shared implementation + wired path + regression test exist. ProductionVerified may still be open for the current hotel.",
    },
    register,
  };
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
  for (const row of capture.lessonsIncorporatedThisRun || []) {
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
 * Used by regressions and by coordinators when recording incorporated lessons.
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
