#!/usr/bin/env node
/**
 * Full GDI E2E contract + fail-closed completion evaluator regressions.
 * Fixture-only — no paid spend.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import {
  FULL_E2E_CONTRACT_VERSION,
  E2E_OVERALL_STATUS,
  E2E_REQUIRED_DELIVERABLES,
  E2E_EXPECTED_SEGMENT_IDS,
  DEPTH_INADEQUACY_RULES,
  FORBIDDEN_NOT_APPLICABLE_REASONS,
  FULL_E2E_LESSON_REGISTER,
  LESSON_STATUS,
  listProvenLessons,
  summarizeLessonRegister,
  rollupLessonStatus,
  isForbiddenNotApplicableReason,
  isPlanOnlyExecution,
  buildFullE2eContractDocument,
  FULL_E2E_COORDINATOR_STAGES,
  E2E_STAGE,
} from "../lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js";
import {
  buildLessonsCaptureArtifact,
  assertDefectFixIncludesLessonCapture,
  nextHotelInheritsAutomatically,
  evaluateLessonsCaptureCompleteness,
  LESSONS_CAPTURE_VERSION,
} from "../lib/group-demand-intelligence/workflow/full-e2e-lessons-capture-v1.js";
import {
  evaluateFullE2eCompletion,
  allStagesPassed,
} from "../lib/group-demand-intelligence/workflow/full-e2e-completion-evaluator-v1.js";
import {
  buildCompletionInputFromRun,
  writeFullE2eArtifacts,
  mergeVerification,
} from "../lib/group-demand-intelligence/workflow/full-e2e-completion-input-v1.js";
import {
  reconcileAuthoritativeBudget,
  verificationNeedsRerun,
  runLocalDeliveryVerification,
  parseJsonBodyStrict,
  detectProviderMetadataLeaks,
  computeOpportunityListFingerprint,
} from "../lib/group-demand-intelligence/workflow/full-e2e-verification-adapters-v1.js";
import {
  assertCanonicalWriteEligibilityFields,
  hasSupportedIndependentSegmentAssessment,
  assertPublicationWriteReadiness,
  derivePublicationQualityFromPersistOutputs,
} from "../lib/group-demand-intelligence/workflow/full-e2e-publication-quality-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function baseDeliverables(overrides = {}) {
  return E2E_REQUIRED_DELIVERABLES.map((d) => ({
    id: d.id,
    status: "verified",
    gapRecorded: false,
    ...((overrides[d.id] || {})),
  }));
}

function completeSegment(id, extra = {}) {
  return {
    segmentId: id,
    applicability: "applicable",
    executionMode: "fresh_research",
    coverageStatus: "completed_for_declared_scope",
    declaredScope: { version: "v1", horizon: "18m", geography: "catchment" },
    executionArtifacts: { briefRef: `briefs/${id}.json` },
    sourceFreshness: "2026-10-16",
    providerRunIds: [`trun_test_${id}`],
    candidateDispositions: [],
    dispositionsVerifiedEmpty: true,
    researchedZeroYield: true,
    depthAssessment: "bounded_scope_complete",
    depthFailures: [],
    depthOk: true,
    stoppingReason: "declared_scope_complete",
    ...extra,
  };
}

function allAxesNaExcept(applicableRows = []) {
  const appIds = new Set(applicableRows.map((r) => r.segmentId));
  return E2E_EXPECTED_SEGMENT_IDS.map((id) => {
    const hit = applicableRows.find((r) => r.segmentId === id);
    if (hit) return hit;
    return {
      segmentId: id,
      applicability: "not_applicable",
      applicabilityReason: "outside_catchment_no_traveling_demand",
    };
  });
}

function okBudget(extra = {}) {
  return {
    reconciled: true,
    authoritative: true,
    authorizationId: "auth_test_v1",
    capUsd: 12,
    estimatedIncurredUsd: 1,
    actualBilledUsd: null,
    outstandingReservationsUsd: 0,
    reservationsAccounted: true,
    availableUsd: 11,
    ...extra,
  };
}

function okContact(extra = {}) {
  return {
    required: true,
    queueVerifiedEmpty: true,
    requiredQueueIds: [],
    attempted: 0,
    notAttempted: 0,
    status: "empty_verified_queue",
    ...extra,
  };
}

function assertPartial(result, msg) {
  assert.equal(
    result.overallStatus,
    E2E_OVERALL_STATUS.PARTIAL,
    msg || `expected PARTIAL got ${result.overallStatus}: ${result.reasons?.join("; ")}`
  );
  assert.equal(result.canClaimComplete, false);
}

function assertBlocked(result, msg) {
  assert.equal(
    result.overallStatus,
    E2E_OVERALL_STATUS.BLOCKED,
    msg || `expected BLOCKED got ${result.overallStatus}: ${result.reasons?.join("; ")}`
  );
}

function assertComplete(result, msg) {
  assert.equal(
    result.overallStatus,
    E2E_OVERALL_STATUS.COMPLETE,
    msg || `expected COMPLETE got ${result.overallStatus}: ${JSON.stringify(result.reasons)}`
  );
  assert.equal(result.canClaimComplete, true);
}

function assertFailed(result, msg) {
  assert.equal(
    result.overallStatus,
    E2E_OVERALL_STATUS.FAILED,
    msg || `expected FAILED got ${result.overallStatus}`
  );
}

// --- Contract document ---
{
  const doc = buildFullE2eContractDocument();
  assert.equal(doc.version, FULL_E2E_CONTRACT_VERSION);
  assert.ok(doc.version.startsWith("gdi-full-e2e-contract-v1."));
  assert.ok(doc.stages.length >= 16);
  assert.ok(doc.lessonsCaptureRequired === true);
  assert.ok(FULL_E2E_COORDINATOR_STAGES.includes("capture_lessons"));
  assert.ok(doc.stages.some((s) => s.id === E2E_STAGE.P_LESSONS_CAPTURE));
  assert.equal(E2E_EXPECTED_SEGMENT_IDS.length, 11);
  assert.ok(DEPTH_INADEQUACY_RULES.length >= 6);
  assert.ok(FORBIDDEN_NOT_APPLICABLE_REASONS.includes("budget_exhausted"));
  assert.ok(FULL_E2E_COORDINATOR_STAGES.includes("evaluate_completion"));
  assert.ok(isPlanOnlyExecution("plan_only"));
  assert.ok(
    FULL_E2E_LESSON_REGISTER.some((l) => l.id === "lessons_capture_required")
  );
  assert.ok(
    FULL_E2E_LESSON_REGISTER.some((l) => l.id === "provider_leak_customer_sanitize")
  );
  assert.ok(
    FULL_E2E_LESSON_REGISTER.some((l) => l.id === "false_contact_exhaustion_forbidden")
  );
  assert.ok(isForbiddenNotApplicableReason("provider returned nothing useful"));
  assert.ok(Array.isArray(doc.lessonRegister));
  assert.equal(doc.lessonRegister.length, FULL_E2E_LESSON_REGISTER.length);
  // Honest register: not every lesson is IMPLEMENTED_AND_TESTED.
  const summary = summarizeLessonRegister();
  assert.ok(summary.total === FULL_E2E_LESSON_REGISTER.length);
  assert.ok(listProvenLessons().length < FULL_E2E_LESSON_REGISTER.length);
  assert.ok(summary.incomplete.length > 0);
  assert.ok(
    listProvenLessons().every((l) => l.status === LESSON_STATUS.IMPLEMENTED_AND_TESTED)
  );
  assert.ok(
    FULL_E2E_LESSON_REGISTER.every(
      (l) =>
        l.proof &&
        typeof l.proof.ruleImplemented === "boolean" &&
        typeof l.proof.executionPathWired === "boolean" &&
        typeof l.proof.evidenceGeneratedAutomatically === "boolean" &&
        typeof l.proof.regressionIntegrationVerified === "boolean"
    )
  );
  assert.equal(
    rollupLessonStatus({
      ruleImplemented: true,
      executionPathWired: true,
      evidenceGeneratedAutomatically: false,
      regressionIntegrationVerified: true,
      productionVerified: false,
    }),
    LESSON_STATUS.IMPLEMENTED_NOT_VERIFIED
  );
  assert.ok(doc.hotelSpecificRepairScriptsForbidden === true);
  assert.ok(String(doc.standardEntryPoint).includes("--mode full-e2e"));
}

const baseOk = {
  hotelId: "recTEST",
  runId: "run_test_1",
  contactDepth: okContact(),
  publication: {
    qualifiedPublished: true,
    readbackOk: true,
    backupsPresent: true,
    structuredHotelFitOk: true,
    drawersComplete: true,
    eligibilityWriteComplete: true,
    identityDedupeReconciled: true,
    postImportEligibilityRepairRequired: false,
    newRecordCount: 1,
    newOpportunityCount: 1,
  },
  localVerification: {
    status: "passed",
    hotelId: "recTEST",
    runId: "run_test_1",
    dataRevision: "rev1",
    buildSha: "sha1",
  },
  productionVerification: {
    status: "passed",
    hotelId: "recTEST",
    runId: "run_test_1",
    dataRevision: "rev1",
    buildSha: "sha1",
  },
  currentFingerprints: { dataRevision: "rev1", buildSha: "sha1" },
  deliverables: baseDeliverables(),
  budget: okBudget(),
  stageResults: allStagesPassed(),
  ledger: { startedAt: "2026-10-16T00:00:00Z", stages: {} },
  lessonsCapture: buildLessonsCaptureArtifact({
    runId: "run_test_1",
    hotelId: "recTEST",
    incorporatedThisRunIds: ["lessons_capture_required"],
  }),
};

// 1) Association-only + plan-only applicable → PARTIAL
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      completeSegment("associations_government", { researchedZeroYield: false, candidateDispositions: [{ id: "a" }] }),
      {
        segmentId: "corporate_meetings_training",
        applicability: "applicable",
        executionMode: "plan_only",
        coverageStatus: "not_started",
      },
    ]),
    publication: {
      ...baseOk.publication,
      associationOnly: true,
      otherSegmentsPartial: true,
    },
  });
  assertPartial(r, "association-only + plan_only must be PARTIAL");
}

// 2) Empty segment coverage cannot COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: [],
  });
  assert.ok(r.blockers.some((b) => b.code === "empty_segment_coverage"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 3) Omitted expected segment cannot COMPLETE
{
  const rows = allAxesNaExcept([completeSegment("associations_government")]);
  const filtered = rows.filter((s) => s.segmentId !== "sports");
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: filtered,
  });
  assert.ok(r.blockers.some((b) => b.code === "omitted_expected_segment"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 4) Unknown / duplicate segment IDs
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: [
      ...allAxesNaExcept([]),
      { segmentId: "not_a_real_segment", applicability: "not_applicable", applicabilityReason: "x" },
      {
        segmentId: "sports",
        applicability: "not_applicable",
        applicabilityReason: "outside_catchment_no_traveling_demand",
      },
    ],
  });
  assert.ok(r.blockers.some((b) => b.code === "unknown_segment_id"));
  assert.ok(r.blockers.some((b) => b.code === "duplicate_segment_id"));
}

// 5) Mandatory stage NOT_STARTED / BLOCKED prevents COMPLETE
{
  const stages = allStagesPassed();
  stages.find((s) => s.letter === "G").status = "NOT_STARTED";
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    stageResults: stages,
  });
  assertPartial(r, "missing contact-depth stage prevents COMPLETE");
}

// 6) Required share/report gapRecorded=true but unavailable → not COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    deliverables: baseDeliverables({
      hotel_scoped_share_link: { status: "gap", gapRecorded: true },
      gdi_report_pdf: { status: "unsupported", gapRecorded: true },
    }),
  });
  assert.ok(r.blockers.some((b) => b.code === "deliverable_unsatisfied"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 7) Missing contact summary with nonempty required queue
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    contactDepth: {
      required: true,
      summaryMissing: true,
      requiredQueueIds: ["opp1", "opp2"],
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "contact_summary_missing"));
  assertBlocked(r);
}

// 8) Manual passed conflicting with failed execution evidence
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    localVerification: {
      status: "passed",
      machineStatus: "failed",
      hotelId: "recTEST",
      runId: "run_test_1",
      dataRevision: "rev1",
      buildSha: "sha1",
    },
  });
  assert.ok(
    r.blockers.some((b) => b.code === "operator_override_of_failed_verification")
  );
  assertFailed(r);
}

// 9) Proof for wrong hotel/run
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    productionVerification: {
      status: "passed",
      hotelId: "recOTHER",
      runId: "run_other",
      dataRevision: "rev1",
      buildSha: "sha1",
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "wrong_artifact_identity"));
  assertFailed(r);
}

// 10) Old verification after data/build change → stale
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    productionVerification: {
      status: "passed",
      hotelId: "recTEST",
      runId: "run_test_1",
      dataRevision: "old",
      buildSha: "sha1",
    },
    currentFingerprints: { dataRevision: "new", buildSha: "sha1" },
  });
  assert.ok(r.blockers.some((b) => b.code === "stale_production_verification"));
  assertBlocked(r);
}

// 11) Production verification not_run recorded by prior execution
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    productionVerification: { status: "not_run" },
    ledger: {
      startedAt: "2026-10-16T00:00:00Z",
      stages: {
        production_delivery_verify: {
          status: "completed",
          verificationStatus: "not_run",
        },
      },
    },
  });
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
  assert.ok(
    r.overallStatus === E2E_OVERALL_STATUS.BLOCKED ||
      r.overallStatus === E2E_OVERALL_STATUS.PARTIAL
  );
}

// 12) Budget fields copied without authoritative reconciliation
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    budget: { reconciled: true, estimatedIncurredUsd: 1 },
  });
  assert.ok(r.blockers.some((b) => b.code === "budget_not_authoritative"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 13) Valid researched-zero-yield can COMPLETE with correct deliverables
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      completeSegment("sports", { researchedZeroYield: true }),
    ]),
    contactDepth: okContact(),
    publication: {
      qualifiedPublished: false,
      zeroQualifiedEmptyStateOk: true,
      readbackOk: true,
      backupsPresent: true,
    },
  });
  assertComplete(r, "researched zero-yield with empty verified contact queue");
  assert.equal(r.segmentSummary.researchedZeroYield, 1);
}

// 14) Adequate reuse with provenance avoids needing paid (COMPLETE)
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "reuse",
        coverageStatus: "completed_for_declared_scope",
        reuseSatisfiesScope: true,
        declaredScope: { version: "v1", horizon: "18m" },
        reuseProvenance: { ledgerRef: "reuse-ledger.json" },
        sourceFreshness: "2026-10-01",
        candidateDispositions: [{ id: "x" }],
        depthAssessment: "reuse_adequate",
        depthOk: true,
        depthFailures: [],
        stoppingReason: "reuse_satisfies_declared_scope",
      },
    ]),
  });
  assertComplete(r, "reuse with provenance may COMPLETE");
}

// 15) reuseSatisfiesScope boolean alone insufficient
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "reuse",
        coverageStatus: "completed_for_declared_scope",
        reuseSatisfiesScope: true,
        declaredScope: { horizon: "18m" },
      },
    ]),
  });
  assertPartial(r, "reuse boolean alone insufficient");
}

// 16) Org fallback after documented contact-depth work can leave research COMPLETE path
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    contactDepth: {
      required: true,
      requiredQueueIds: ["opp1"],
      queue: [{ opportunityId: "opp1" }],
      dispositionsById: {
        opp1: "public_research_exhausted_org_fallback",
      },
      attemptIds: ["opp1"],
      resultIds: ["opp1"],
      attempted: 1,
      notAttempted: 0,
      exhaustedOrgFallback: 1,
      status: "completed",
    },
    publication: { ...baseOk.publication, qualifiedPublished: true },
  });
  assertComplete(r, "org fallback after documented attempt OK");
}

// 17) Association-only publication remains PARTIAL when other segments incomplete
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      completeSegment("associations_government"),
      {
        segmentId: "corporate_meetings_training",
        applicability: "applicable",
        executionMode: "plan_only",
        coverageStatus: "not_started",
      },
    ]),
    publication: { ...baseOk.publication, associationOnly: true },
  });
  assertPartial(r);
}

// 18) Planner via association contacts ≠ coverage
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      completeSegment("associations_government"),
      {
        ...completeSegment("professional_planners_dmc"),
        coverageAxis: "planner_intermediary_discovery",
        satisfiedByAssociationContacts: true,
      },
    ]),
  });
  assertPartial(r, "assoc contacts ≠ planner");
}

// 19) Dry-run paid/write violations → FAILED
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([]),
    ledger: {
      dryRun: true,
      paidCallsAttempted: true,
      productionWritesAttempted: true,
      startedAt: "2026-10-16T00:00:00Z",
    },
  });
  assertFailed(r);
}

// 20) Weekly refresh cannot imply full E2E COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    weeklyRefreshCompleted: true,
    initialCoverageClaimedCompleteFromWeeklyOnly: true,
  });
  assertBlocked(r);
}

// 21) Resume invalidation: verificationNeedsRerun
{
  assert.equal(
    verificationNeedsRerun(
      { status: "completed", verificationStatus: "not_run" },
      { dataRevision: "a" }
    ),
    true
  );
  assert.equal(
    verificationNeedsRerun(
      {
        status: "completed",
        verificationStatus: "passed",
        dataRevision: "old",
      },
      { dataRevision: "new" }
    ),
    true
  );
  assert.equal(
    verificationNeedsRerun(
      {
        status: "completed",
        verificationStatus: "passed",
        dataRevision: "same",
        buildSha: "s",
      },
      { dataRevision: "same", buildSha: "s" }
    ),
    false
  );
}

// 22) mergeVerification: machine failure wins over operator passed
{
  const m = mergeVerification(
    { status: "failed", machineStatus: "failed" },
    { status: "passed" }
  );
  assert.equal(m.status, "failed");
}

// 23) Authoritative budget reconcile from ledger doc
{
  const auth = reconcileAuthoritativeBudget({
    ledgerDoc: {
      authorization: "auth_x",
      hotelId: "recH",
      budgetUsd: 12,
      estimatedSpentUsd: 2,
      accountingHoldUsd: 0,
      entries: [
        { jobKey: "a", status: "spent", estimatedUsd: 2 },
        { jobKey: "b", status: "reserved", estimatedUsd: 0 },
      ],
    },
    authorizationId: "auth_x",
    hotelId: "recH",
  });
  assert.equal(auth.reconciled, true);
  assert.equal(auth.authoritative, true);
  assert.equal(auth.availableUsd, 10);
}

// 24) buildCompletionInputFromRun is read-only on ledger
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-e2e-resume-"));
  const ledger = {
    runId: "run_resume_1",
    hotelId: "recTEST",
    dryRun: true,
    startedAt: "2026-10-16T00:00:00Z",
    stages: {
      validate_config: { status: "completed" },
      local_delivery_verify: {
        status: "completed",
        verificationStatus: "not_run",
      },
    },
    budget: { reconciled: true },
  };
  fs.writeFileSync(
    path.join(tmp, "stage-completion-ledger.json"),
    JSON.stringify(ledger, null, 2)
  );
  const input1 = buildCompletionInputFromRun({ ledger, runDir: tmp, overlay: {} });
  assert.equal(input1.localVerification.status, "not_run");
  const ledgerAfter = JSON.parse(
    fs.readFileSync(path.join(tmp, "stage-completion-ledger.json"), "utf8")
  );
  assert.deepEqual(ledgerAfter.stages, ledger.stages);
}

// 25) Label "researched" alone insufficient
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "researched",
      },
    ]),
  });
  assertPartial(r);
}

// Artifact write helpers
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-e2e-art-"));
  const completion = evaluateFullE2eCompletion({
    segmentCoverage: [],
    ledger: {},
  });
  const paths = writeFullE2eArtifacts({
    outDir: tmp,
    contractDoc: buildFullE2eContractDocument(),
    completion,
    inputSnapshot: { note: "fixture" },
  });
  assert.ok(fs.existsSync(paths.contractPath));
  assert.ok(fs.existsSync(paths.completionPath));
}

// Coordinator source wiring smoke
{
  const src = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "utf8"
  );
  assert.ok(src.includes("full-e2e"));
  assert.ok(src.includes("evaluate_completion"));
  assert.ok(src.includes("evaluateFullE2eCompletion"));
  assert.ok(src.includes("runLocalDeliveryVerification") || src.includes("local_delivery_verify"));
  assert.ok(
    fs.existsSync(path.join(ROOT, "scripts/gdi-full-e2e-status-v1.mjs"))
  );
}

// Documented command parser smoke — flags present
{
  const src = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "utf8"
  );
  for (const flag of [
    "--mode",
    "--authorize-publish",
    "--authorize-paid-discovery",
    "--authorize-surfe",
    "--dry-run",
    "--resume-run",
    "--authorization-ref",
  ]) {
    assert.ok(src.includes(flag), `missing flag ${flag}`);
  }
}

// --- Renaissance lessons institutionalized (v1.2) ---

// 26) Sports proxy / non-independent assessment blocks COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      {
        ...completeSegment("sports"),
        satisfiedByExhibitorProxy: true,
        independentAssessment: false,
      },
    ]),
  });
  assert.ok(r.blockers.some((b) => b.code === "independent_segment_assessment_missing"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 27) Educational proxy from sports blocks COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      {
        ...completeSegment("tours_educational_specialist"),
        satisfiedBySportsProxy: true,
      },
    ]),
  });
  assert.ok(r.blockers.some((b) => b.code === "independent_segment_assessment_missing"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 28) Contact depth skipped because org route exists → blocked
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    contactDepth: {
      ...okContact(),
      skippedBecauseOrgRouteExists: true,
      status: "skipped_org_route_exists",
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "contact_depth_skipped_for_org_route"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 29) Publication without structured hotel-fit / drawers blocked
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    publication: {
      ...baseOk.publication,
      structuredHotelFitOk: false,
      drawersComplete: false,
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "publication_without_structured_hotel_fit"));
  assert.ok(r.blockers.some((b) => b.code === "publication_without_complete_drawers"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 30) Post-import eligibility repair required → not COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    publication: {
      ...baseOk.publication,
      postImportEligibilityRepairRequired: true,
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "post_import_eligibility_repair_required"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 31) Identity dedupe unreconciled → not COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    publication: {
      ...baseOk.publication,
      identityDedupeReconciled: false,
      newRecordCount: 5,
      newOpportunityCount: 5,
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "identity_dedupe_unreconciled"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 32) Cross hotel/product budget combination blocked
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    budget: { ...okBudget(), crossProductCombined: true },
  });
  assert.ok(r.blockers.some((b) => b.code === "budget_cross_scope_combined"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 33) Canonical write eligibility uses use-case contract — not prose presence
{
  const bad = assertCanonicalWriteEligibilityFields({
    title: "x",
    prospectHotelFacing: true,
  });
  assert.equal(bad.ok, false);
  assert.ok(bad.missing.includes("hotelFitRationale"));
  assert.ok(bad.missing.includes("localConnection"));

  // whyThisMatters / proposedHotelUse alone must not establish eligibility
  const proseOnly = assertCanonicalWriteEligibilityFields({
    organizationName: "Acme Co",
    title: "Important account",
    hotelFitRationale: "Walkable to Midtown training venues for overnight stays",
    localConnection: "HQ within 2 miles",
    whyThisMatters: "Strategic Midtown account for Renaissance",
    proposedHotelUse: "Could host meetings at the hotel",
  });
  assert.equal(proseOnly.ok, false);

  const good = assertCanonicalWriteEligibilityFields({
    organizationName: "Acme Training Co",
    title: "2027 Midtown Trainer Summit",
    eventStartDate: "2027-03-15",
    hotelFitRationale:
      "Walkable to Midtown training venues and overnight room blocks for regional trainers",
    localConnection: "HQ within 2 miles of Times Square",
    travelEvidenceNote: "Regional trainers overnight",
    roomDemandStatus: "EVIDENCED",
    sourceUrl: "https://example.com/summit-2027",
  });
  assert.equal(good.ok, true);
}

// 33b) Independent segment requires supported assessment, not anti-proxy silence
{
  assert.equal(
    hasSupportedIndependentSegmentAssessment({
      segmentId: "sports",
      executionMode: "fresh_research",
      coverageStatus: "completed_for_declared_scope",
      // no provider runs, no independentAssessment flag
    }),
    false
  );
  assert.equal(
    hasSupportedIndependentSegmentAssessment(completeSegment("sports")),
    true
  );
  const silentSports = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      {
        segmentId: "sports",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
        declaredScope: { version: "v1" },
        depthOk: true,
        researchedZeroYield: true,
        // deliberately omit providerRunIds + independentAssessment
      },
    ]),
  });
  assert.ok(
    silentSports.blockers.some((b) => b.code === "independent_segment_assessment_missing")
  );
}

// 34) Standard entry point is shared coordinator (not Renaissance repair script)
{
  const src = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "utf8"
  );
  assert.ok(src.includes("--mode") && src.includes("full-e2e"));
  assert.ok(src.includes("evaluateFullE2eCompletion"));
  assert.ok(
    !fs.existsSync(
      path.join(ROOT, "scripts/gdi-renaissance-eligibility-repair-v1.mjs")
    ),
    "must not require a Renaissance-specific eligibility repair entry point"
  );
  const persist = fs.readFileSync(
    path.join(ROOT, "lib/group-demand-intelligence/workflow/corporate-package-persist-v1.js"),
    "utf8"
  );
  assert.ok(persist.includes("assertCanonicalWriteEligibilityFields"));
}

// --- Delivery verification + auth reconcile hardening (v1.3) ---

function mockFetchSequence(handlers) {
  let i = 0;
  return async (url, init) => {
    const h = handlers[Math.min(i, handlers.length - 1)];
    i += 1;
    return h(String(url), init);
  };
}

function jsonResponse(status, body, headers = {}) {
  const headerMap = new Map(
    Object.entries(headers).map(([k, v]) => [k.toLowerCase(), String(v)])
  );
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (k) => headerMap.get(String(k).toLowerCase()) || null },
    text: async () =>
      typeof body === "string" ? body : JSON.stringify(body),
    json: async () => {
      if (typeof body === "string") return JSON.parse(body);
      return body;
    },
  };
}

// 35) HTTP 200 with invalid JSON must fail (empty fallback not accepted)
{
  assert.equal(parseJsonBodyStrict("not-json{").ok, false);
  const r = await runLocalDeliveryVerification({
    hotelId: "recH",
    requireBuildIdentity: false,
    fetchImpl: mockFetchSequence([
      () => jsonResponse(200, "not-json{"),
    ]),
  });
  assert.equal(r.status, "failed");
  assert.equal(r.checks.parseReason, "invalid_json");
}

// 36) Wrong-hotel response fails
{
  const r = await runLocalDeliveryVerification({
    hotelId: "recExpected",
    requireBuildIdentity: false,
    fetchImpl: mockFetchSequence([
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recOther",
          updatedAt: "2026-10-16T00:00:00Z",
          opportunities: [{ id: "opp1" }],
        }),
    ]),
  });
  assert.equal(r.status, "failed");
  assert.equal(r.checks.hotelIdentityOk, false);
}

// 37) Missing expected published IDs fails
{
  const r = await runLocalDeliveryVerification({
    hotelId: "recH",
    requireBuildIdentity: false,
    expectedPublishedIds: ["oppA", "oppB"],
    fetchImpl: mockFetchSequence([
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          updatedAt: "2026-10-16T00:00:00Z",
          opportunities: [{ id: "oppA" }],
        }),
    ]),
  });
  assert.equal(r.status, "failed");
  assert.equal(
    r.checks.publicationReconcile.reason,
    "missing_expected_published_ids"
  );
}

// 38) List/detail mismatch fails
{
  const r = await runLocalDeliveryVerification({
    hotelId: "recH",
    requireBuildIdentity: false,
    expectedPublishedIds: ["oppA"],
    detailSampleLimit: 1,
    fetchImpl: mockFetchSequence([
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          updatedAt: "2026-10-16T00:00:00Z",
          opportunities: [{ id: "oppA" }],
        }),
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          opportunity: { id: "oppOTHER", hotelId: "recH" },
        }),
    ]),
  });
  assert.equal(r.status, "failed");
  assert.equal(r.checks.listDetail.status, "failed");
}

// 39) Provider metadata leakage fails (not merely noted)
{
  assert.equal(
    detectProviderMetadataLeaks({
      opportunities: [{ id: "x", note: "trun_abc123deadbeef" }],
    }).leaked,
    true
  );
  const r = await runLocalDeliveryVerification({
    hotelId: "recH",
    requireBuildIdentity: false,
    fetchImpl: mockFetchSequence([
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          updatedAt: "2026-10-16T00:00:00Z",
          opportunities: [
            { id: "oppA", evidence: "trun_567f3335d0c140adbc64e005916c92ac" },
          ],
        }),
    ]),
  });
  assert.equal(r.status, "failed");
  assert.equal(r.checks.providerMetadataLeaks, 1);
}

// 40) Intended build SHA without observed identity → blocked (caller SHA ≠ proof)
{
  const r = await runLocalDeliveryVerification({
    hotelId: "recH",
    intendedBuildSha: "abc123deadbeef",
    requireBuildIdentity: true,
    detailSampleLimit: 1,
    fetchImpl: mockFetchSequence([
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          updatedAt: "2026-10-16T00:00:00Z",
          opportunities: [{ id: "oppA" }],
        }),
      // list/detail sample
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          opportunity: { id: "oppA", hotelId: "recH" },
        }),
      // health probes — no sha
      () => jsonResponse(200, { ok: true }),
      () => jsonResponse(200, { ok: true }),
      () => jsonResponse(200, { ok: true }),
    ]),
  });
  assert.equal(r.status, "blocked");
  assert.equal(r.checks.buildIdentityOk, false);
}

// 41) Authorization mismatch despite supplied authorizationId (ledger identity first)
{
  const mismatch = reconcileAuthoritativeBudget({
    authorizationId: "auth_requested_wrong",
    hotelId: "recH",
    ledgerDoc: {
      authorization: "auth_ledger_real",
      hotelId: "recH",
      budgetUsd: 12,
      estimatedSpentUsd: 1,
      accountingHoldUsd: 0,
      entries: [{ jobKey: "a", status: "spent", estimatedUsd: 1 }],
    },
  });
  assert.equal(mismatch.reconciled, false);
  assert.equal(mismatch.reason, "authorization_mismatch");
  assert.equal(mismatch.authorizationId, "auth_ledger_real");
  assert.equal(mismatch.requestedAuthorizationId, "auth_requested_wrong");
}

// 42) Missing / malformed / wrong-scope ledger fail closed
{
  assert.equal(
    reconcileAuthoritativeBudget({ authorizationId: "x" }).reason,
    "ledger_missing"
  );
  assert.equal(
    reconcileAuthoritativeBudget({
      authorizationId: "auth_x",
      hotelId: "recH",
      ledgerDoc: {
        // no authorization identity on ledger
        hotelId: "recH",
        budgetUsd: 12,
        estimatedSpentUsd: 0,
        entries: [],
      },
    }).reason,
    "authorization_identity_missing"
  );
  assert.equal(
    reconcileAuthoritativeBudget({
      authorizationId: "auth_x",
      hotelId: "recOTHER",
      ledgerDoc: {
        authorization: "auth_x",
        hotelId: "recH",
        budgetUsd: 12,
        estimatedSpentUsd: 0,
        entries: [],
      },
    }).reason,
    "hotel_mismatch"
  );
  assert.equal(
    reconcileAuthoritativeBudget({
      authorizationId: "auth_x",
      hotelId: "recH",
      ledgerDoc: {
        authorization: "auth_x",
        hotelId: "recH",
        budgetUsd: "not-a-number",
        estimatedSpentUsd: 0,
        entries: [],
      },
    }).reason,
    "cap_invalid"
  );
}

// 43) Happy-path delivery with manifest + list/detail + observed build header
{
  const listBody = {
    ok: true,
    hotelId: "recH",
    updatedAt: "2026-10-16T00:00:00Z",
    opportunities: [{ id: "oppA" }, { id: "oppLegacy" }],
  };
  const fp = computeOpportunityListFingerprint(listBody);
  const r = await runLocalDeliveryVerification({
    hotelId: "recH",
    intendedBuildSha: "shippedsha1",
    publicationManifest: {
      publishedIds: ["oppA"],
      legacyPreservedIds: ["oppLegacy"],
    },
    fetchImpl: mockFetchSequence([
      () =>
        jsonResponse(200, listBody, {
          "x-dealality-build-sha": "shippedsha1",
        }),
      () =>
        jsonResponse(200, {
          ok: true,
          hotelId: "recH",
          opportunity: { id: "oppA", hotelId: "recH", title: "A" },
        }),
    ]),
  });
  assert.equal(r.status, "passed");
  assert.equal(r.buildSha, "shippedsha1");
  assert.equal(r.checks.observedDataRevision, fp);
  assert.equal(r.checks.publicationReconcile.status, "passed");
}

// 44) NYC Midtown geo: Times Square / New York are local for Renaissance, not foreign
{
  const {
    assessGeographicRelevance,
  } = await import(
    "../lib/group-demand-intelligence/workflow/geographic-relevance-gate-v1.js"
  );
  const cfg = {
    hotelId: "recG66DQJKP2c0UNh",
    demandTerritory: {
      label: "Manhattan Midtown / Times Square",
      includes: ["Times Square / Theater District", "Midtown Manhattan"],
    },
  };
  const geo = assessGeographicRelevance(
    {
      organizationName: "Sports Traveler",
      localConnection:
        "2026 US Open finals package includes three nights at the Marriott Marquis New York Times Square",
    },
    cfg,
    { hotelId: "recG66DQJKP2c0UNh" }
  );
  assert.equal(geo.quarantine, false);
  assert.ok(geo.localHits.length > 0);
}

// 45) Disposition geo probe must not drop localConnection (generic segments)
{
  const {
    dispositionPaidCandidates,
  } = await import(
    "../lib/group-demand-intelligence/workflow/paid-multisegment-research-v1.js"
  );
  const rows = dispositionPaidCandidates(
    {
      hotelId: "recG66DQJKP2c0UNh",
      segment: "sports",
      kind: "discovery",
      candidates: [
        {
          organizationName: "Example Sports Travel Co",
          localConnection:
            "Midtown Manhattan overnight packages near Times Square for race weekends",
          proposedHotelUse: "Group lodging blocks for visiting runners",
          hotelFitRationale:
            "317-room Midtown hotel can host race-weekend overflow lodging",
          contactEmail: "groups@example.com",
          sourceUrls: ["https://example.com/nyc"],
        },
      ],
    },
    {
      hotelConfig: {
        hotelId: "recG66DQJKP2c0UNh",
        demandTerritory: {
          label: "Manhattan Midtown / Times Square",
          includes: ["Times Square / Theater District"],
        },
      },
    }
  );
  assert.equal(rows.length, 1);
  assert.notEqual(rows[0].disposition, "quarantined_wrong_market");
  assert.notEqual(rows[0].reason, "unhandled_segment_or_kind");
  assert.ok(rows[0].package?.localConnection);
}

// 46) Lessons capture missing blocks COMPLETE
{
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([
      completeSegment("associations_government"),
    ]),
    lessonsCapture: null,
  });
  assert.ok(r.blockers.some((b) => b.code === "lessons_capture_missing"));
  assert.notEqual(r.overallStatus, E2E_OVERALL_STATUS.COMPLETE);
}

// 47) Hotel-specific repair claimed complete without shared path is blocked
{
  const capture = buildLessonsCaptureArtifact({
    runId: "run_test_1",
    hotelId: "recTEST",
    incorporatedThisRunIds: ["provider_leak_customer_sanitize"],
  });
  capture.hotelSpecificRepairsClaimedComplete = [
    { id: "ren_only_payload_rewrite", sharedPathPreventsRecurrence: false },
  ];
  const evalCap = evaluateLessonsCaptureCompleteness(capture);
  assert.equal(evalCap.ok, false);
  assert.ok(
    evalCap.blockers.some((b) => b.code === "hotel_repair_not_process_improvement")
  );
  const r = evaluateFullE2eCompletion({
    ...baseOk,
    segmentCoverage: allAxesNaExcept([completeSegment("associations_government")]),
    lessonsCapture: capture,
  });
  assert.ok(
    r.blockers.some((b) => b.code === "hotel_repair_not_process_improvement")
  );
}

// 48) Defect-fix checklist requires shared path + test + register + runbook/prompt
{
  const bad = assertDefectFixIncludesLessonCapture({
    sharedImplementation: "customer-presentation-sanitize-v3",
    // missing test / register / prompt
  });
  assert.equal(bad.ok, false);
  assert.ok(bad.missing.includes("regressionOrIntegrationTest"));
  assert.ok(bad.missing.includes("lessonRegisterUpdated"));
  assert.ok(bad.missing.includes("runbookAndCursorPromptUpdated"));

  const good = assertDefectFixIncludesLessonCapture({
    sharedImplementation: "customer-presentation-sanitize-v3",
    regressionOrIntegrationTest: "test:gdi-customer-provider-leak-sanitize-v3",
    lessonRegisterUpdated: true,
    runbookAndCursorPromptUpdated: true,
  });
  assert.equal(good.ok, true);
}

// 49) Next hotel inherits when shared path + test exist (prod may still be open)
{
  const leak = FULL_E2E_LESSON_REGISTER.find(
    (l) => l.id === "provider_leak_customer_sanitize"
  );
  assert.ok(leak);
  assert.equal(nextHotelInheritsAutomatically(leak), true);
  assert.equal(leak.proof.productionVerified, false);
  const capture = buildLessonsCaptureArtifact({
    incorporatedThisRunIds: ["provider_leak_customer_sanitize"],
  });
  assert.equal(capture.version, LESSONS_CAPTURE_VERSION);
  assert.ok(
    capture.nextHotelInheritance.inheritsAutomatically.includes(
      "provider_leak_customer_sanitize"
    )
  );
  assert.ok(Array.isArray(capture.remainingImplementationOrVerificationGaps));
}

console.log("test-gdi-full-e2e-contract-v1: PASS");
