#!/usr/bin/env node
/**
 * Full GDI E2E contract + completion evaluator regressions (fixture-only; no paid spend).
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
  DEPTH_INADEQUACY_RULES,
  FORBIDDEN_NOT_APPLICABLE_REASONS,
  isForbiddenNotApplicableReason,
  isPlanOnlyExecution,
  buildFullE2eContractDocument,
  FULL_E2E_COORDINATOR_STAGES,
} from "../lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js";
import { evaluateFullE2eCompletion } from "../lib/group-demand-intelligence/workflow/full-e2e-completion-evaluator-v1.js";
import {
  buildCompletionInputFromRun,
  writeFullE2eArtifacts,
} from "../lib/group-demand-intelligence/workflow/full-e2e-completion-input-v1.js";

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

function applicableSegments(rows) {
  return rows;
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
    msg || `expected BLOCKED got ${result.overallStatus}`
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

// --- Contract document ---
{
  const doc = buildFullE2eContractDocument();
  assert.equal(doc.version, FULL_E2E_CONTRACT_VERSION);
  assert.ok(doc.stages.length >= 15);
  assert.ok(doc.segmentTaxonomy.length >= 8);
  assert.ok(DEPTH_INADEQUACY_RULES.length >= 6);
  assert.ok(FORBIDDEN_NOT_APPLICABLE_REASONS.includes("budget_exhausted"));
  assert.ok(FULL_E2E_COORDINATOR_STAGES.includes("evaluate_completion"));
  assert.ok(FULL_E2E_COORDINATOR_STAGES.includes("buyer_contact_enrichment"));
  assert.ok(isPlanOnlyExecution("plan_only"));
  assert.ok(isForbiddenNotApplicableReason("provider returned nothing useful"));
  assert.equal(isForbiddenNotApplicableReason("outside_catchment_no_traveling_demand"), false);
}

// 1) Association-only publication + plan-only applicable segments → PARTIAL
{
  const r = evaluateFullE2eCompletion({
    hotelId: "recTEST_ASSOC",
    segmentCoverage: applicableSegments([
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
      {
        segmentId: "corporate_meetings_training",
        applicability: "applicable",
        executionMode: "plan_only",
        coverageStatus: "not_started",
      },
      {
        segmentId: "professional_planners_dmc",
        applicability: "applicable",
        executionMode: "plan_only",
        coverageStatus: "not_started",
      },
    ]),
    contactDepth: { required: true, notAttempted: 0, attempted: 3, status: "completed" },
    publication: {
      qualifiedPublished: true,
      associationOnly: true,
      readbackOk: true,
      backupsPresent: true,
      otherSegmentsPartial: true,
    },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertPartial(r, "association-only + plan_only must be PARTIAL");
  assert.ok(
    r.reasons.some((x) => /plan_only|Association-only/i.test(x)),
    "must explain association-only / plan_only"
  );
}

// 2) Named association contacts do not satisfy planner-discovery coverage
{
  const r = evaluateFullE2eCompletion({
    hotelId: "recTEST_PLANNER",
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
      {
        segmentId: "professional_planners_dmc",
        applicability: "applicable",
        coverageAxis: "planner_intermediary_discovery",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
        satisfiedByAssociationContacts: true,
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 2 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertPartial(r, "assoc contacts ≠ planner coverage");
  assert.ok(r.reasons.some((x) => /planner/i.test(x)));
}

// 3) Irrelevant provider results do not satisfy research depth
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "corporate_meetings_training",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
        depthFailures: ["single_broad_competing_hotels"],
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertPartial(r, "depth inadequacy must block COMPLETE");
  assert.ok(r.reasons.some((x) => /depth inadequacy/i.test(x)));
}

// 4) Adequate current reused evidence can satisfy scope without new paid calls
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "reuse",
        coverageStatus: "completed_for_declared_scope",
        reuseSatisfiesScope: true,
        declaredScope: { horizon: "18m", geography: "catchment" },
      },
      {
        segmentId: "corporate_meetings_training",
        applicability: "applicable",
        executionMode: "reuse",
        reuseSatisfiesScope: true,
        declaredScope: { horizon: "18m" },
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 2, status: "completed" },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertComplete(r, "reuse-satisfied scope may COMPLETE without new paid");
}

// 5) Researched zero-yield segment can pass with proper evidence
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "sports",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
        researchedZeroYield: true,
      },
    ],
    contactDepth: {
      required: true,
      notAttempted: 0,
      attempted: 0,
      status: "skipped_no_candidates",
    },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertComplete(r, "researched zero-yield may COMPLETE");
  assert.equal(r.segmentSummary.researchedZeroYield, 1);
}

// 6) Unsupported NOT_APPLICABLE does not bypass required research
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "scaling_companies",
        applicability: "not_applicable",
        applicabilityReason: "budget_exhausted",
      },
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assert.ok(
    r.blockers.some((b) => b.code === "unsupported_not_applicable"),
    "forbidden N/A must block"
  );
  assert.equal(r.overallStatus, E2E_OVERALL_STATUS.FAILED);
}

// 7) Org fallback permits eligible publication but does not skip contact-depth attempts
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: {
      required: true,
      notAttempted: 2,
      attempted: 0,
      exhaustedOrgFallback: 2,
      silentNotAttemptedToExhausted: true,
      status: "completed",
    },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assert.ok(
    r.blockers.some((b) => b.code === "silent_not_attempted_to_exhausted")
  );
  assertBlocked(r, "silent NOT_ATTEMPTED→exhausted must BLOCK");
}

// 8) Duplicate representations do not inflate discovery totals (evaluator segment counts are set-based by input)
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assert.equal(r.segmentSummary.applicable, 1);
  assert.equal(r.distinctions.researchCoverage.applicableCount, 1);
}

// 9) Local pass + missing production proof cannot return COMPLETE
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "missing" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertPartial(r, "local without production ≠ COMPLETE");
  assert.ok(r.reasons.some((x) => /production/i.test(x)));
}

// 10) Airtable write + broken external link cannot return COMPLETE
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: {
      qualifiedPublished: true,
      airtableWriteOk: true,
      readbackOk: true,
      backupsPresent: true,
    },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables({
      hotel_scoped_share_link: { status: "broken" },
    }),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assert.ok(r.blockers.some((b) => b.code === "airtable_ok_share_broken"));
  assertBlocked(r, "broken share + airtable write ≠ COMPLETE");
}

// 11) Stale build/data verification is rejected
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "stale" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assert.ok(r.blockers.some((b) => b.code === "stale_production_verification"));
  assertBlocked(r);
}

// 12) Budget exhaustion preserves completed work and returns honest partial
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
      {
        segmentId: "corporate_meetings_training",
        applicability: "applicable",
        executionMode: "plan_only",
        coverageStatus: "not_started",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, associationOnly: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true, exhausted: true },
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { validate_config: { status: "completed" } } },
  });
  assertPartial(r, "budget exhaustion with incomplete research → PARTIAL");
  assert.ok(r.reasons.some((x) => /Budget exhausted/i.test(x)));
  assert.equal(r.segmentSummary.completedForDeclaredScope, 1);
}

// 13) Dry-run has no paid or production-write side effects (violation → FAILED)
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [],
    contactDepth: { required: false },
    publication: {},
    localVerification: { status: "not_run" },
    productionVerification: { status: "not_run" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    ledger: {
      dryRun: true,
      paidCallsAttempted: true,
      productionWritesAttempted: true,
      startedAt: "2026-10-16T00:00:00Z",
      stages: { validate_config: { status: "completed" } },
    },
  });
  assert.ok(r.blockers.some((b) => b.code === "dry_run_paid_violation"));
  assert.ok(r.blockers.some((b) => b.code === "dry_run_write_violation"));
  assert.equal(r.overallStatus, E2E_OVERALL_STATUS.FAILED);
}

// 14) Resume does not duplicate — evaluator is pure; input assembler is read-only
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-e2e-resume-"));
  const ledger = {
    runId: "run_resume_1",
    hotelId: "recTEST",
    dryRun: true,
    startedAt: "2026-10-16T00:00:00Z",
    stages: {
      validate_config: { status: "completed" },
      evaluate_completion: { status: "completed" },
    },
    budget: { reconciled: true },
  };
  fs.writeFileSync(
    path.join(tmp, "stage-completion-ledger.json"),
    JSON.stringify(ledger, null, 2)
  );
  const input1 = buildCompletionInputFromRun({ ledger, runDir: tmp, overlay: {} });
  const input2 = buildCompletionInputFromRun({ ledger, runDir: tmp, overlay: {} });
  assert.equal(input1.runId, input2.runId);
  assert.equal(input1.hotelId, "recTEST");
  // ledger file unchanged
  const ledgerAfter = JSON.parse(
    fs.readFileSync(path.join(tmp, "stage-completion-ledger.json"), "utf8")
  );
  assert.deepEqual(ledgerAfter.stages, ledger.stages);
}

// 15) Weekly refresh completion does not imply full initial coverage
{
  const r = evaluateFullE2eCompletion({
    segmentCoverage: [
      {
        segmentId: "associations_government",
        applicability: "applicable",
        executionMode: "fresh_research",
        coverageStatus: "completed_for_declared_scope",
      },
    ],
    contactDepth: { required: true, notAttempted: 0, attempted: 1 },
    publication: { qualifiedPublished: true, readbackOk: true, backupsPresent: true },
    localVerification: { status: "passed" },
    productionVerification: { status: "passed" },
    deliverables: baseDeliverables(),
    budget: { reconciled: true },
    weeklyRefreshCompleted: true,
    initialCoverageClaimedCompleteFromWeeklyOnly: true,
    ledger: { startedAt: "2026-10-16T00:00:00Z", stages: { weekly_refresh: { status: "completed" } } },
  });
  assert.ok(
    r.blockers.some((b) => b.code === "weekly_refresh_cannot_imply_full_e2e")
  );
  assertBlocked(r, "weekly refresh ≠ full E2E COMPLETE");
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

// Coordinator source wiring smoke (no execution)
{
  const src = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "utf8"
  );
  assert.ok(src.includes("--mode full-e2e") || src.includes('full-e2e'));
  assert.ok(src.includes("evaluate_completion"));
  assert.ok(src.includes("evaluateFullE2eCompletion"));
  assert.ok(src.includes("dry_run_blocks_paid_calls") || src.includes("explicitDryRun"));
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "scripts/gdi-full-e2e-status-v1.mjs")
    )
  );
}

console.log("PASS test-gdi-full-e2e-contract-v1");
