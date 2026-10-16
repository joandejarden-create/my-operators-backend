/**
 * Full GDI E2E completion evaluator V1.2 — fail closed.
 *
 * COMPLETE requires verified stage outcomes, complete segment applicability
 * coverage, required deliverables, reconciled contact-depth queue, fresh
 * local+production verification, authoritative budget reconciliation, and
 * publication-quality gates (fit/drawers/eligibility/dedupe/independent axes).
 *
 * Association-only publication + incomplete applicable segments → PARTIAL.
 * Empty coverage / omitted segments / gap-recorded deliverables → never COMPLETE.
 */

import {
  FULL_E2E_CONTRACT_VERSION,
  E2E_OVERALL_STATUS,
  E2E_STAGE_DEFINITIONS,
  E2E_REQUIRED_DELIVERABLES,
  E2E_EXPECTED_SEGMENT_IDS,
  isForbiddenNotApplicableReason,
  isPlanOnlyExecution,
  SEGMENT_COVERAGE_STATUS,
  isKnownSegmentId,
  CONTACT_QUEUE_DISPOSITIONS,
} from "./full-e2e-contract-v1.js";
import {
  evaluatePublicationQuality,
  evaluateIndependentSegmentAxes,
  evaluateContactDepthOrgRoutePolicy,
  evaluateBudgetIsolation,
} from "./full-e2e-publication-quality-v1.js";
import { evaluateLessonsCaptureCompleteness } from "./full-e2e-lessons-capture-v1.js";

const APPLICABILITY = Object.freeze({
  APPLICABLE: "applicable",
  NOT_APPLICABLE: "not_applicable",
  UNRESOLVED: "unresolved",
});

const PASSING_STAGE = new Set(["PASSED", "PASS", "COMPLETED_OK", "N_A", "NOT_APPLICABLE"]);
const BLOCKING_STAGE = new Set([
  "NOT_STARTED",
  "BLOCKED",
  "FAILED",
  "RUNNING",
  "SKIPPED",
  "MISSING",
]);

/**
 * Evaluate Full GDI E2E completion from run artifacts.
 * @param {object} input
 * @returns {object}
 */
export function evaluateFullE2eCompletion(input = {}) {
  const {
    runId = null,
    hotelId = null,
    runMode = "full_e2e",
    ledger = {},
    segmentCoverage = [],
    expectedSegmentIds = null,
    contactDepth = {},
    publication = {},
    localVerification = {},
    productionVerification = {},
    deliverables = [],
    budget = {},
    stageResults = null,
    currentFingerprints = {},
    weeklyRefreshCompleted = false,
    initialCoverageClaimedCompleteFromWeeklyOnly = false,
    operatorEvidence = null,
    lessonsCapture = null,
  } = input;

  const reasons = [];
  const blockers = [];

  // --- Expected segment coverage axes ---
  const expectedIds = Array.isArray(expectedSegmentIds) && expectedSegmentIds.length
    ? [...expectedSegmentIds]
    : [...E2E_EXPECTED_SEGMENT_IDS];

  const coverageValidation = validateSegmentCoverageAxes(
    segmentCoverage || [],
    expectedIds,
    { blockers, reasons }
  );

  const applicable = coverageValidation.applicable;
  const notApplicable = coverageValidation.notApplicable;

  let applicablePlanOnly = 0;
  let applicableIncompleteCoverage = 0;
  let applicableComplete = 0;
  let applicableResearchedZero = 0;

  for (const s of applicable) {
    const assessment = assessApplicableSegmentCompletion(s);
    if (assessment.planOnly) {
      applicablePlanOnly += 1;
      reasons.push(assessment.reason);
      continue;
    }
    if (assessment.complete) {
      if (s.researchedZeroYield) applicableResearchedZero += 1;
      applicableComplete += 1;
      continue;
    }
    applicableIncompleteCoverage += 1;
    reasons.push(assessment.reason);
  }

  // Association contacts ≠ planner coverage
  const plannerAxis = applicable.find(
    (s) =>
      s.segmentId === "professional_planners_dmc" ||
      s.coverageAxis === "planner_intermediary_discovery"
  );
  if (plannerAxis) {
    const claimedViaAssoc =
      plannerAxis.satisfiedByAssociationContacts === true ||
      plannerAxis.executionMode === "association_contacts_proxy";
    if (claimedViaAssoc) {
      applicableIncompleteCoverage += 1;
      applicableComplete = Math.max(0, applicableComplete - 1);
      reasons.push(
        "Named association contacts do not satisfy professional-planner discovery coverage"
      );
    }
  }

  // Sports / educational axes must be independently assessed
  const indep = evaluateIndependentSegmentAxes(applicable, { blockers, reasons });
  if (indep.incomplete > 0) {
    applicableIncompleteCoverage += indep.incomplete;
    applicableComplete = Math.max(0, applicableComplete - indep.incomplete);
  }

  // --- Contact depth queue reconciliation ---
  const contact = evaluateContactDepth(contactDepth, { blockers, reasons });
  const orgSkip = evaluateContactDepthOrgRoutePolicy(contactDepth, {
    blockers,
    reasons,
  });
  if (orgSkip.incomplete) contact.incomplete = true;

  // --- Publication (distinct from overall COMPLETE) ---
  const pub = {
    qualifiedPublished: publication.qualifiedPublished === true,
    associationOnly: publication.associationOnly === true,
    readbackOk: publication.readbackOk === true,
    backupsPresent: publication.backupsPresent === true,
    otherSegmentsPartial: publication.otherSegmentsPartial === true,
    zeroQualifiedEmptyStateOk: publication.zeroQualifiedEmptyStateOk === true,
  };
  if (pub.associationOnly && applicablePlanOnly + applicableIncompleteCoverage > 0) {
    reasons.push(
      "Association-only publication while other applicable segments remain incomplete — overall cannot be COMPLETE"
    );
  }

  const pubQuality = evaluatePublicationQuality(publication, {
    blockers,
    reasons,
    qualifiedPublishedHint: pub.qualifiedPublished,
  });
  if (pubQuality.incomplete) {
    reasons.push("Publication quality gates incomplete — cannot COMPLETE");
  }

  // --- Deliverables (gap ≠ satisfied) ---
  evaluateDeliverables(deliverables, publication, { blockers, reasons });

  // --- Local / production verification + freshness ---
  const local = normalizeVerification(localVerification, "local", {
    hotelId,
    runId,
    currentFingerprints,
    blockers,
    reasons,
    operatorEvidence,
  });
  const prod = normalizeVerification(productionVerification, "production", {
    hotelId,
    runId,
    currentFingerprints,
    blockers,
    reasons,
    operatorEvidence,
  });

  if (local.status === "passed" && prod.status !== "passed") {
    reasons.push(
      "Local verification passed but production proof missing/failed — not COMPLETE"
    );
  }

  // --- Budget (authoritative + hotel/product isolation) ---
  const budgetEval = evaluateBudget(budget, { blockers, reasons });
  const budgetIso = evaluateBudgetIsolation(budget, {
    blockers,
    reasons,
    hotelId,
  });
  if (!budgetIso.ok) budgetEval.ok = false;
  if (
    budget.exhausted === true &&
    (applicablePlanOnly > 0 || applicableIncompleteCoverage > 0)
  ) {
    reasons.push(
      "Budget exhausted with incomplete applicable research — PARTIAL (preserve completed work)"
    );
  }

  // --- Lessons capture (required — not deferrable to a later founder prompt) ---
  const lessonsEval = evaluateLessonsCaptureCompleteness(lessonsCapture);
  if (!lessonsEval.ok) {
    for (const b of lessonsEval.blockers || []) blockers.push(b);
    reasons.push(...(lessonsEval.reasons || []));
  }

  // --- Stage evaluation — every required stage must pass or N/A ---
  const stages = stageResults || deriveStagesFromLedger(ledger);
  const stageEval = [];
  let stagesIncomplete = false;
  for (const def of E2E_STAGE_DEFINITIONS) {
    const hit = (stages || []).find(
      (s) => s.id === def.id || s.letter === def.letter
    );
    let status = String(hit?.status || "NOT_STARTED").toUpperCase();
    if (status === "COMPLETED") status = "PASSED"; // ledger alias only if outcome verified
    if (hit?.outcomeVerified === false && status === "PASSED") {
      status = "NOT_STARTED";
      reasons.push(
        `Stage ${def.letter}: ledger status=completed without verified outcome`
      );
    }
    // Free-form "passed" cannot override FAILED machine outcome
    if (hit?.machineStatus === "FAILED" && status === "PASSED") {
      status = "FAILED";
      blockers.push({ code: "operator_override_of_failed_stage", stageId: def.id });
      reasons.push(
        `Stage ${def.letter}: operator/manual passed conflicts with failed machine evidence`
      );
    }
    const naOk =
      status === "N_A" ||
      status === "NOT_APPLICABLE";
    if (naOk && !hit?.notApplicableEvidence) {
      status = "NOT_STARTED";
      blockers.push({ code: "stage_na_without_evidence", stageId: def.id });
      reasons.push(`Stage ${def.letter}: NOT_APPLICABLE without evidence`);
    }
    stageEval.push({
      id: def.id,
      letter: def.letter,
      title: def.title,
      status,
      detail: hit?.detail || null,
    });
    if (!PASSING_STAGE.has(status)) {
      stagesIncomplete = true;
      if (BLOCKING_STAGE.has(status) || status === "NOT_STARTED") {
        reasons.push(
          `Required stage ${def.letter} (${def.id}) status=${status} — prevents COMPLETE`
        );
      }
    }
  }

  // --- Weekly refresh must not imply full initial COMPLETE ---
  if (weeklyRefreshCompleted && initialCoverageClaimedCompleteFromWeeklyOnly) {
    blockers.push({ code: "weekly_refresh_cannot_imply_full_e2e" });
    reasons.push(
      "Weekly refresh completion does not imply full initial market coverage COMPLETE"
    );
  }

  // --- Dry-run isolation ---
  if (ledger.dryRun === true && ledger.paidCallsAttempted === true) {
    blockers.push({ code: "dry_run_paid_violation" });
    reasons.push("Dry-run attempted paid calls");
  }
  if (ledger.dryRun === true && ledger.productionWritesAttempted === true) {
    blockers.push({ code: "dry_run_write_violation" });
    reasons.push("Dry-run attempted production writes");
  }

  // --- Overall status ---
  const hardFail = blockers.some((b) =>
    [
      "dry_run_paid_violation",
      "dry_run_write_violation",
      "unsupported_not_applicable",
      "operator_override_of_failed_stage",
      "operator_override_of_failed_verification",
      "wrong_artifact_identity",
    ].includes(b.code)
  );
  const hardBlock = blockers.some((b) =>
    [
      "airtable_ok_share_broken",
      "stale_production_verification",
      "stale_local_verification",
      "airtable_write_only_as_production",
      "silent_not_attempted_to_exhausted",
      "deliverable_missing",
      "deliverable_unsatisfied",
      "deliverable_gap_unrecorded",
      "weak_local_substituted",
      "weekly_refresh_cannot_imply_full_e2e",
      "empty_segment_coverage",
      "omitted_expected_segment",
      "unknown_segment_id",
      "duplicate_segment_id",
      "unresolved_applicability",
      "invalid_applicability",
      "contact_queue_unreconciled",
      "contact_summary_missing",
      "budget_not_authoritative",
      "stage_na_without_evidence",
      "verification_not_run",
      "verification_blocked",
      "verification_failed",
      "publication_without_structured_hotel_fit",
      "publication_hotel_fit_unverified",
      "publication_without_complete_drawers",
      "publication_drawers_unverified",
      "post_import_eligibility_repair_required",
      "eligibility_write_incomplete",
      "identity_dedupe_unreconciled",
      "identity_dedupe_unverified",
      "identity_count_inconsistent",
      "independent_segment_assessment_missing",
      "contact_depth_skipped_for_org_route",
      "budget_cross_scope_combined",
      "budget_wrong_hotel",
      "lessons_capture_missing",
      "lessons_capture_version_unknown",
      "lessons_capture_register_empty",
      "lessons_capture_inheritance_missing",
      "lessons_capture_gaps_missing",
      "hotel_repair_not_process_improvement",
      "lesson_missing_code_or_test_ref",
    ].includes(b.code)
  );

  const anyRunning = stageEval.some((s) => s.status === "RUNNING");
  const anyFailed = stageEval.some((s) => s.status === "FAILED") || hardFail;
  const researchIncomplete =
    applicablePlanOnly > 0 ||
    applicableIncompleteCoverage > 0 ||
    !coverageValidation.axesComplete;
  const contactIncomplete = contact.incomplete === true;
  const deliveryIncomplete =
    local.status !== "passed" || prod.status !== "passed";
  const started =
    stageEval.some((s) => s.status !== "NOT_STARTED") ||
    (segmentCoverage || []).length > 0 ||
    Boolean(ledger.startedAt);

  let overall = E2E_OVERALL_STATUS.NOT_STARTED;

  if (anyFailed) {
    overall = E2E_OVERALL_STATUS.FAILED;
  } else if (
    hardBlock ||
    local.status === "blocked" ||
    prod.status === "blocked" ||
    local.status === "not_run" ||
    prod.status === "not_run"
  ) {
    // not_run on required verification → BLOCKED (cannot COMPLETE)
    if (
      local.status === "not_run" ||
      prod.status === "not_run" ||
      local.status === "blocked" ||
      prod.status === "blocked"
    ) {
      if (!blockers.some((b) => b.code === "verification_not_run" || b.code === "verification_blocked")) {
        blockers.push({
          code:
            local.status === "not_run" || prod.status === "not_run"
              ? "verification_not_run"
              : "verification_blocked",
        });
        reasons.push("Required verification BLOCKED/NOT_RUN — cannot COMPLETE");
      }
    }
    overall = E2E_OVERALL_STATUS.BLOCKED;
  } else if (!started) {
    overall = E2E_OVERALL_STATUS.NOT_STARTED;
  } else if (anyRunning) {
    overall = E2E_OVERALL_STATUS.RUNNING;
  } else if (
    !researchIncomplete &&
    !contactIncomplete &&
    !deliveryIncomplete &&
    !stagesIncomplete &&
    budgetEval.ok &&
    blockers.length === 0 &&
    pub.readbackOk !== false &&
    !(pub.associationOnly && researchIncomplete)
  ) {
    const allApplicableDone =
      applicable.length > 0
        ? applicableComplete === applicable.length && applicablePlanOnly === 0
        : false;
    // Empty applicable with complete N/A axes is OK (all not_applicable with rationales)
    const allAxesNaOrComplete =
      coverageValidation.axesComplete &&
      applicablePlanOnly === 0 &&
      applicableIncompleteCoverage === 0 &&
      (applicable.length === 0 || allApplicableDone);

    if (allAxesNaOrComplete) {
      overall = E2E_OVERALL_STATUS.COMPLETE;
    } else {
      overall = E2E_OVERALL_STATUS.PARTIAL;
      reasons.push("Not all expected coverage axes completed for declared scope");
    }
  } else {
    overall = E2E_OVERALL_STATUS.PARTIAL;
  }

  // Final safety: never COMPLETE with incomplete research / stages / contacts / lessons
  if (
    overall === E2E_OVERALL_STATUS.COMPLETE &&
    (researchIncomplete ||
      contactIncomplete ||
      stagesIncomplete ||
      deliveryIncomplete ||
      !budgetEval.ok ||
      !lessonsEval.ok ||
      blockers.length > 0)
  ) {
    overall = E2E_OVERALL_STATUS.PARTIAL;
    reasons.push(
      "COMPLETE overridden to PARTIAL: incomplete research, stages, contacts, delivery, budget, or lessons capture"
    );
  }

  return {
    version: FULL_E2E_CONTRACT_VERSION,
    runId,
    hotelId,
    runMode,
    overallStatus: overall,
    computedAt: new Date().toISOString(),
    distinctions: {
      taskExecution: { stageEval },
      researchCoverage: {
        expectedCount: expectedIds.length,
        axesComplete: coverageValidation.axesComplete,
        applicableCount: applicable.length,
        applicableComplete,
        applicablePlanOnly,
        applicableIncompleteCoverage,
        applicableResearchedZeroYield: applicableResearchedZero,
      },
      publication: pub,
      customerDelivery: { local, production: prod },
    },
    segmentSummary: {
      expected: expectedIds.length,
      axesComplete: coverageValidation.axesComplete,
      applicable: applicable.length,
      notApplicable: notApplicable.length,
      planOnlyApplicable: applicablePlanOnly,
      incompleteCoverage: applicableIncompleteCoverage,
      completedForDeclaredScope: applicableComplete,
      researchedZeroYield: applicableResearchedZero,
    },
    contactDepth: contact,
    deliverables: E2E_REQUIRED_DELIVERABLES.map((r) => ({
      ...r,
      result:
        (deliverables || []).find((d) => d.id === r.id) || { status: "missing" },
    })),
    budget: budgetEval.summary,
    lessonsCapture: {
      ok: lessonsEval.ok,
      incomplete: lessonsEval.incomplete === true,
      blockers: lessonsEval.blockers || [],
      reasons: lessonsEval.reasons || [],
      incorporatedThisRun: lessonsCapture?.lessonsIncorporatedThisRun || [],
      remainingGaps:
        lessonsCapture?.remainingImplementationOrVerificationGaps ||
        lessonsCapture?.remainingGaps ||
        [],
      nextHotelInheritance: lessonsCapture?.nextHotelInheritance || null,
    },
    stageEval,
    blockers,
    reasons,
    canClaimComplete: overall === E2E_OVERALL_STATUS.COMPLETE,
    note:
      overall === E2E_OVERALL_STATUS.COMPLETE
        ? "All required stages passed or evidence-backed N/A; expected coverage axes complete; contact depth reconciled; deliverables verified; local+production fresh; budget authoritatively reconciled; lessons capture complete."
        : "Do not independently write COMPLETE in founder/Admin reports — consume overallStatus.",
  };
}

function validateSegmentCoverageAxes(rows, expectedIds, { blockers, reasons }) {
  const applicable = [];
  const notApplicable = [];
  const seen = new Map();

  if (!Array.isArray(rows) || rows.length === 0) {
    blockers.push({ code: "empty_segment_coverage" });
    reasons.push(
      "Empty segment coverage is missing evidence — not proof that no research is needed"
    );
    return { applicable, notApplicable, axesComplete: false };
  }

  for (const s of rows) {
    const id = String(s.segmentId || "").trim();
    if (!id) {
      blockers.push({ code: "invalid_applicability", segmentId: null });
      reasons.push("Segment row missing segmentId");
      continue;
    }
    if (!isKnownSegmentId(id) && !expectedIds.includes(id)) {
      blockers.push({ code: "unknown_segment_id", segmentId: id });
      reasons.push(`Unknown segment ID: ${id}`);
    }
    if (seen.has(id)) {
      blockers.push({ code: "duplicate_segment_id", segmentId: id });
      reasons.push(`Duplicate segment ID: ${id}`);
      continue;
    }
    seen.set(id, s);

    const app = String(s.applicability || "")
      .toLowerCase()
      .trim();
    if (app === APPLICABILITY.APPLICABLE) {
      applicable.push(s);
    } else if (app === APPLICABILITY.NOT_APPLICABLE) {
      notApplicable.push(s);
      if (isForbiddenNotApplicableReason(s.applicabilityReason)) {
        blockers.push({
          code: "unsupported_not_applicable",
          segmentId: id,
          reason: s.applicabilityReason,
        });
        reasons.push(
          `Segment ${id}: NOT_APPLICABLE rationale forbidden (${s.applicabilityReason})`
        );
      }
      if (!s.applicabilityReason || !String(s.applicabilityReason).trim()) {
        blockers.push({
          code: "missing_not_applicable_rationale",
          segmentId: id,
        });
        reasons.push(`Segment ${id}: NOT_APPLICABLE missing rationale`);
      }
    } else if (app === APPLICABILITY.UNRESOLVED || !app) {
      blockers.push({ code: "unresolved_applicability", segmentId: id });
      reasons.push(
        `Segment ${id}: applicability unresolved — prevents COMPLETE`
      );
    } else {
      blockers.push({
        code: "invalid_applicability",
        segmentId: id,
        value: app,
      });
      reasons.push(`Segment ${id}: unrecognized applicability "${app}"`);
    }
  }

  for (const id of expectedIds) {
    if (!seen.has(id)) {
      blockers.push({ code: "omitted_expected_segment", segmentId: id });
      reasons.push(`Expected coverage axis omitted: ${id}`);
    }
  }

  const axesComplete =
    expectedIds.every((id) => seen.has(id)) &&
    !blockers.some((b) =>
      [
        "empty_segment_coverage",
        "omitted_expected_segment",
        "unknown_segment_id",
        "duplicate_segment_id",
        "unresolved_applicability",
        "invalid_applicability",
        "unsupported_not_applicable",
        "missing_not_applicable_rationale",
      ].includes(b.code)
    );

  return { applicable, notApplicable, axesComplete };
}

/**
 * Applicable segment completion requires evidence — not labels alone.
 */
function assessApplicableSegmentCompletion(s) {
  const id = s.segmentId;
  const mode = s.executionMode || s.mode || null;
  const cov = String(s.coverageStatus || "").toLowerCase();
  const depthFails = s.depthFailures || [];

  if (isPlanOnlyExecution(mode)) {
    return {
      planOnly: true,
      complete: false,
      reason: `Applicable segment ${id} remains plan_only — not substantive research`,
    };
  }

  if (depthFails.length > 0 && !hasQualifyingReuseEvidence(s)) {
    return {
      planOnly: false,
      complete: false,
      reason: `Segment ${id}: depth inadequacy [${depthFails.join(", ")}]`,
    };
  }

  // Label "researched" alone is insufficient
  if (cov === "researched") {
    return {
      planOnly: false,
      complete: false,
      reason: `Segment ${id}: coverage label "researched" alone is insufficient without scope/artifacts/depth/stopping reason`,
    };
  }

  // reuseSatisfiesScope boolean alone is insufficient
  if (s.reuseSatisfiesScope === true && !hasQualifyingReuseEvidence(s)) {
    return {
      planOnly: false,
      complete: false,
      reason: `Segment ${id}: reuseSatisfiesScope boolean alone is insufficient`,
    };
  }

  const hasScope =
    s.declaredScope &&
    typeof s.declaredScope === "object" &&
    (s.declaredScope.version || s.scopeVersion || s.declaredScope.horizon);
  const hasArtifacts =
    hasExecutionArtifacts(s) || hasQualifyingReuseEvidence(s);
  const hasFreshness = Boolean(
    s.sourceFreshness || s.sourceDate || s.providerRunIds?.length
  );
  const hasDispositions =
    Array.isArray(s.candidateDispositions) ||
    s.researchedZeroYield === true ||
    s.dispositionsVerifiedEmpty === true;
  const hasDepth =
    s.depthAssessment ||
    (Array.isArray(s.depthFailures) && s.depthFailures.length === 0) ||
    s.depthOk === true;
  const hasStopping = Boolean(s.stoppingReason);

  const coverageOk =
    cov === SEGMENT_COVERAGE_STATUS.COMPLETED_FOR_DECLARED_SCOPE ||
    cov === "completed_for_declared_scope";

  if (
    coverageOk &&
    hasScope &&
    hasArtifacts &&
    hasFreshness &&
    hasDispositions &&
    hasDepth &&
    hasStopping
  ) {
    return { planOnly: false, complete: true, reason: null };
  }

  if (cov === "blocked" || cov === SEGMENT_COVERAGE_STATUS.NOT_STARTED || !cov) {
    return {
      planOnly: false,
      complete: false,
      reason: `Applicable segment ${id}: coverage=${cov || "missing"}`,
    };
  }

  const missing = [];
  if (!coverageOk) missing.push("coverageStatus");
  if (!hasScope) missing.push("declaredScope+version");
  if (!hasArtifacts) missing.push("executionOrReuseArtifacts");
  if (!hasFreshness) missing.push("sourceFreshness");
  if (!hasDispositions) missing.push("candidateDispositions");
  if (!hasDepth) missing.push("depthAssessment");
  if (!hasStopping) missing.push("stoppingReason");

  return {
    planOnly: false,
    complete: false,
    reason: `Applicable segment ${id}: incomplete evidence [${missing.join(", ")}] (coverage=${cov || "missing"})`,
  };
}

function hasExecutionArtifacts(s) {
  return Boolean(
    s.executionArtifacts ||
      s.scorecardRef ||
      s.briefRef ||
      (Array.isArray(s.providerRunIds) && s.providerRunIds.length) ||
      s.manifestRef
  );
}

function hasQualifyingReuseEvidence(s) {
  return (
    s.reuseSatisfiesScope === true &&
    s.declaredScope &&
    (s.reuseProvenance ||
      s.reuseArtifactRef ||
      s.reuseLedgerRef ||
      (Array.isArray(s.reuseEvidenceIds) && s.reuseEvidenceIds.length > 0))
  );
}

function evaluateContactDepth(contactDepth = {}, { blockers, reasons }) {
  const required = contactDepth.required !== false;
  const summaryMissing =
    contactDepth.summaryMissing === true ||
    contactDepth.missing === true ||
    (required &&
      contactDepth.queueVerifiedEmpty !== true &&
      !Array.isArray(contactDepth.queue) &&
      contactDepth.attempted == null &&
      contactDepth.notAttempted == null &&
      !contactDepth.status);

  const queue = Array.isArray(contactDepth.queue) ? contactDepth.queue : null;
  const queueIds = queue
    ? queue.map((q) => q.id || q.opportunityId || q.candidateId).filter(Boolean)
    : contactDepth.requiredQueueIds || null;

  const contact = {
    required,
    attempted: Number(contactDepth.attempted || 0),
    notAttempted: Number(contactDepth.notAttempted || 0),
    namedComplete: Number(contactDepth.namedComplete || 0),
    exhaustedOrgFallback: Number(contactDepth.exhaustedOrgFallback || 0),
    silentNotAttemptedToExhausted:
      contactDepth.silentNotAttemptedToExhausted === true,
    status: contactDepth.status || null,
    queueSize: queueIds ? queueIds.length : null,
    queueVerifiedEmpty: contactDepth.queueVerifiedEmpty === true,
    incomplete: false,
  };

  if (!required) {
    return contact;
  }

  if (summaryMissing && !(queueIds && queueIds.length === 0 && contact.queueVerifiedEmpty)) {
    blockers.push({ code: "contact_summary_missing" });
    reasons.push(
      "Contact depth summary missing while contact research is required — zero-default does not imply completion"
    );
    contact.incomplete = true;
  }

  if (contact.silentNotAttemptedToExhausted) {
    blockers.push({ code: "silent_not_attempted_to_exhausted" });
    reasons.push(
      "Contact depth: NOT_ATTEMPTED silently converted to exhausted fallback"
    );
    contact.incomplete = true;
  }

  // Explicit empty verified queue → OK (no-candidate run)
  if (contact.queueVerifiedEmpty && (!queueIds || queueIds.length === 0)) {
    contact.status = contact.status || "empty_verified_queue";
    contact.incomplete = false;
    return contact;
  }

  // Legacy skipped_no_candidates without verified empty queue → incomplete
  if (contact.status === "skipped_no_candidates" && !contact.queueVerifiedEmpty) {
    reasons.push(
      "Contact depth: skipped_no_candidates without verified empty queue — not sufficient"
    );
    contact.incomplete = true;
  }

  if (queueIds && queueIds.length > 0) {
    const dispositions = contactDepth.dispositionsById || {};
    const results = contactDepth.resultIds || [];
    const attemptIds = contactDepth.attemptIds || [];
    let missingDisp = 0;
    for (const qid of queueIds) {
      const d =
        dispositions[qid] ||
        (queue || []).find(
          (q) => (q.id || q.opportunityId || q.candidateId) === qid
        )?.disposition;
      if (!d || !CONTACT_QUEUE_DISPOSITIONS.includes(String(d))) {
        missingDisp += 1;
      }
    }
    if (missingDisp > 0) {
      blockers.push({ code: "contact_queue_unreconciled", missing: missingDisp });
      reasons.push(
        `Contact depth: ${missingDisp} required queue items lack a valid disposition`
      );
      contact.incomplete = true;
    }
    // Reconcile unique IDs across queue → attempts → results when provided
    if (
      Array.isArray(contactDepth.attemptIds) &&
      Array.isArray(contactDepth.resultIds)
    ) {
      const attemptSet = new Set(attemptIds);
      for (const qid of queueIds) {
        if (!attemptSet.has(qid) && !dispositions[qid]) {
          contact.incomplete = true;
        }
      }
    }
    contact.notAttempted = Math.max(contact.notAttempted, missingDisp);
  } else if (contact.notAttempted > 0) {
    reasons.push(
      `Contact depth: ${contact.notAttempted} required queue items still NOT_ATTEMPTED`
    );
    contact.incomplete = true;
  }

  return contact;
}

function evaluateDeliverables(deliverables, publication, { blockers, reasons }) {
  const deliverableMap = new Map((deliverables || []).map((d) => [d.id, d]));
  for (const req of E2E_REQUIRED_DELIVERABLES) {
    const got = deliverableMap.get(req.id);
    if (!got) {
      blockers.push({ code: "deliverable_missing", id: req.id });
      reasons.push(`Required deliverable missing: ${req.id}`);
      continue;
    }
    const st = String(got.status || "").toLowerCase();
    if (st === "verified" || st === "passed") continue;

    if (st === "not_applicable") {
      if (!req.allowNotApplicable && !req.allowGapIfUnsupported) {
        blockers.push({ code: "deliverable_unsatisfied", id: req.id });
        reasons.push(
          `Deliverable ${req.id}: NOT_APPLICABLE not permitted by contract`
        );
      } else if (!got.reason || !String(got.reason).trim()) {
        blockers.push({ code: "deliverable_unsatisfied", id: req.id });
        reasons.push(
          `Deliverable ${req.id}: NOT_APPLICABLE missing supported reason`
        );
      }
      continue;
    }

    // gap / unsupported / missing / broken / not_run — never satisfy COMPLETE
    if (
      ["gap", "unsupported", "missing", "broken", "failed", "not_run"].includes(
        st
      )
    ) {
      blockers.push({
        code:
          st === "missing" ? "deliverable_missing" : "deliverable_unsatisfied",
        id: req.id,
        status: st,
      });
      reasons.push(
        `Deliverable ${req.id}: status=${st}${
          got.gapRecorded ? " (gapRecorded does not satisfy delivery)" : ""
        }`
      );
      continue;
    }

    reasons.push(`Deliverable ${req.id}: status=${st || "unknown"}`);
    blockers.push({ code: "deliverable_unsatisfied", id: req.id, status: st });
  }

  const share = deliverableMap.get("hotel_scoped_share_link");
  if (
    publication.airtableWriteOk === true &&
    share &&
    (share.status === "broken" || share.status === "failed")
  ) {
    blockers.push({ code: "airtable_ok_share_broken" });
    reasons.push(
      "Airtable write succeeded but external share link is broken — not COMPLETE"
    );
  }
}

function evaluateBudget(budget = {}, { blockers, reasons }) {
  const summary = {
    reconciled: false,
    authoritative: budget.authoritative === true,
    exhausted: budget.exhausted === true,
    outstandingReservationsUsd: budget.outstandingReservationsUsd ?? null,
    estimatedIncurredUsd: budget.estimatedIncurredUsd ?? null,
    actualBilledUsd:
      budget.actualBilledUsd === undefined ? null : budget.actualBilledUsd,
    accountingHoldUsd: budget.accountingHoldUsd ?? null,
    authorizationId: budget.authorizationId || null,
    capUsd: budget.capUsd ?? null,
    availableUsd: budget.availableUsd ?? null,
  };

  // Copied fields without authoritative ledger inspection ≠ reconciled
  if (budget.reconciled === true && budget.authoritative !== true) {
    blockers.push({ code: "budget_not_authoritative" });
    reasons.push(
      "Budget fields copied without authoritative ledger reconciliation"
    );
    return { ok: false, summary };
  }

  if (budget.reconciled !== true) {
    reasons.push("Budget not reconciled");
    return { ok: false, summary };
  }

  if (!budget.authorizationId && budget.requireAuthorizationId !== false) {
    // Allow fixtures to set requireAuthorizationId:false only when authoritative+reconciled
    if (budget.authoritative === true && budget.authorizationId) {
      /* ok */
    } else if (!budget.authorizationId) {
      blockers.push({ code: "budget_not_authoritative" });
      reasons.push("Budget reconciliation missing authorization identity");
      return { ok: false, summary };
    }
  }

  const outstandingOk =
    budget.outstandingReservationsUsd == null ||
    Number(budget.outstandingReservationsUsd) === 0 ||
    budget.reservationsAccounted === true;

  if (!outstandingOk) {
    reasons.push("Budget has unaccounted outstanding reservations");
    return { ok: false, summary: { ...summary, reconciled: false } };
  }

  summary.reconciled = true;
  return { ok: true, summary };
}

function normalizeVerification(v = {}, kind, ctx) {
  const {
    hotelId,
    runId,
    currentFingerprints = {},
    blockers,
    reasons,
    operatorEvidence,
  } = ctx;
  let status = String(v.status || "missing").toLowerCase();

  if (status === "asset_only_as_pass") {
    blockers.push({ code: "weak_local_substituted" });
    reasons.push(
      "Local browser check cannot be replaced by a weaker asset check and called passed"
    );
    status = "failed";
  }
  if (status === "airtable_write_only") {
    blockers.push({ code: "airtable_write_only_as_production" });
    reasons.push("Airtable write alone does not satisfy production delivery");
    status = "failed";
  }

  // Operator cannot override failed machine check
  const machine = v.machineStatus || v.machineResult;
  if (
    machine &&
    ["failed", "blocked", "not_run"].includes(String(machine).toLowerCase()) &&
    status === "passed"
  ) {
    blockers.push({ code: "operator_override_of_failed_verification", kind });
    reasons.push(
      `${kind} verification: operator passed conflicts with failed/blocked machine check`
    );
    status = String(machine).toLowerCase();
  }

  // Artifact identity binding
  if (status === "passed") {
    if (v.hotelId && hotelId && String(v.hotelId) !== String(hotelId)) {
      blockers.push({ code: "wrong_artifact_identity", kind });
      reasons.push(`${kind} verification hotelId mismatch`);
      status = "failed";
    }
    if (v.runId && runId && String(v.runId) !== String(runId)) {
      blockers.push({ code: "wrong_artifact_identity", kind });
      reasons.push(`${kind} verification runId mismatch`);
      status = "failed";
    }
    if (
      currentFingerprints.dataRevision &&
      v.dataRevision &&
      String(v.dataRevision) !== String(currentFingerprints.dataRevision)
    ) {
      blockers.push({
        code:
          kind === "production"
            ? "stale_production_verification"
            : "stale_local_verification",
        kind,
      });
      reasons.push(
        `${kind} verification dataRevision stale vs current artifacts`
      );
      status = "stale";
    }
    if (
      currentFingerprints.buildSha &&
      v.buildSha &&
      String(v.buildSha) !== String(currentFingerprints.buildSha)
    ) {
      blockers.push({
        code:
          kind === "production"
            ? "stale_production_verification"
            : "stale_local_verification",
        kind,
      });
      reasons.push(`${kind} verification buildSha stale vs current build`);
      status = "stale";
    }
    if (v.status === "stale" || status === "stale") {
      blockers.push({
        code:
          kind === "production"
            ? "stale_production_verification"
            : "stale_local_verification",
      });
      reasons.push(
        `${kind === "production" ? "Production" : "Local"} verification is stale relative to current artifacts`
      );
      status = "stale";
    }
  }

  if (status === "stale") {
    blockers.push({
      code:
        kind === "production"
          ? "stale_production_verification"
          : "stale_local_verification",
    });
  }

  // Manual evidence must reference existing proof
  if (operatorEvidence && v.manualReview) {
    const m = v.manualReview;
    if (!m.reviewer || !m.timestamp || !m.evidenceRef) {
      reasons.push(`${kind}: manual review missing reviewer/timestamp/evidenceRef`);
      if (status === "passed") status = "blocked";
    }
  }

  return {
    kind,
    status,
    verifiedAt: v.verifiedAt || null,
    buildSha: v.buildSha || null,
    dataRevision: v.dataRevision || null,
    runId: v.runId || runId || null,
    hotelId: v.hotelId || hotelId || null,
    notes: v.notes || null,
  };
}

function deriveStagesFromLedger(ledger = {}) {
  const stages = ledger.stages || {};
  const map = {
    validate_config: "A_resolve_hotel_identity",
    reuse_evidence: "C_freeze_inventory_reuse",
    discover_gaps: "D_segment_applicability_scope",
    paid_multisegment: "E_substantive_multisegment_research",
    evaluate_eligibility: "F_qualify_deduplicate",
    buyer_contact_enrichment: "G_contact_depth",
    synthesize_hotel_relevance: "H_hotel_fit_narratives",
    generate_cards: "I_cards_drawers_quality",
    prepare_manifest: "J_publication_diff_backups",
    apply_publish: "K_canonical_persistence",
    local_delivery_verify: "M_local_delivery_verify",
    production_delivery_verify: "N_production_delivery_verify",
    reconcile_run: "O_reconcile_budget_history_backlog",
    evaluate_completion: "O_reconcile_budget_history_backlog",
  };
  const out = [];
  for (const [coord, e2eId] of Object.entries(map)) {
    const st = stages[coord];
    if (!st) continue;
    const status = String(st.status || "").toUpperCase();
    const verificationStatus = String(st.verificationStatus || "").toLowerCase();
    // completed + not_run verification → NOT_STARTED for E2E purposes
    let mapped =
      status === "COMPLETED"
        ? "PASSED"
        : status === "FAILED"
          ? "FAILED"
          : status === "BLOCKED"
            ? "BLOCKED"
            : status === "RUNNING"
              ? "RUNNING"
              : status || "NOT_STARTED";
    if (
      (coord === "local_delivery_verify" ||
        coord === "production_delivery_verify") &&
      status === "COMPLETED" &&
      verificationStatus &&
      verificationStatus !== "passed" &&
      verificationStatus !== "verified"
    ) {
      mapped =
        verificationStatus === "blocked"
          ? "BLOCKED"
          : verificationStatus === "failed"
            ? "FAILED"
            : "NOT_STARTED";
    }
    out.push({
      id: e2eId,
      status: mapped,
      outcomeVerified:
        coord.startsWith("local_") || coord.startsWith("production_")
          ? verificationStatus === "passed"
          : status === "COMPLETED" || status === "FAILED",
      detail: st,
      machineStatus: st.machineStatus || null,
    });
  }
  return out;
}

export function evaluateFixtureCase(fixture = {}) {
  return evaluateFullE2eCompletion(fixture);
}

/** Test helper: all contract stages marked PASSED with verified outcomes. */
export function allStagesPassed(extra = {}) {
  return E2E_STAGE_DEFINITIONS.map((d) => ({
    id: d.id,
    letter: d.letter,
    status: "PASSED",
    outcomeVerified: true,
    notApplicableEvidence: null,
    ...((extra[d.id] || {})),
  }));
}
