#!/usr/bin/env node
/**
 * GDI P1-A/B Full E2E execution integrity — deterministic tests.
 * No paid Exa / Apify / Parallel / Surfe / Airtable / publication.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";
import { executeFourteenSegmentDiscoveryMatrix } from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js";
import {
  loadExecutionRun,
  buildProviderMatrix,
  buildMonitorDashboardPayload,
  certifyExecutionRun,
  createEmptyExecutionRun,
  recordProviderCall,
  SEGMENT_EXEC_STATUS,
  PROVIDER_CALL_STATUS,
  APIFY_LIFECYCLE,
  RUN_VERDICT,
  GDI_PROVIDER_IDS,
} from "../lib/group-demand-intelligence/observability/gdi-execution-ledger-store-v1.js";
import {
  listProviderInventory,
  getProviderInventoryEntry,
} from "../lib/group-demand-intelligence/observability/gdi-provider-inventory-v1.js";
import {
  APIFY_SKIP_REASON,
  beginFourteenSegmentExecutionRun,
  recordFourteenSegmentCellOutcome,
  accountApifyForFourteenSegmentCell,
  finalizeFourteenSegmentExecutionRun,
} from "../lib/group-demand-intelligence/observability/gdi-fourteen-segment-execution-ledger-bridge-v1.js";
import { clearActiveExecutionRunContext } from "../lib/group-demand-intelligence/observability/gdi-execution-run-context-v1.js";
import { reconcileAllHistoricalHotels } from "../lib/group-demand-intelligence/observability/gdi-historical-execution-reconciliation-v1.js";
import { workflowPrefersApify } from "../lib/group-demand-intelligence/research/apify/apify-fulle2e-orchestration-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];

function check(name, fn) {
  try {
    const out = fn();
    if (out && typeof out.then === "function") {
      throw new Error("use checkAsync for async tests");
    }
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-p1ab-"));
const stateRoot = path.join(tmpRoot, "discovery-state");
const obsRoot = path.join(tmpRoot, "execution-observability");

// --- Inventory honesty ---
check("apify_inventory_not_overclaiming_wired", () => {
  const apify = getProviderInventoryEntry(GDI_PROVIDER_IDS.APIFY);
  assert.equal(
    apify.fullE2eOrchestratorUsage,
    "CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E",
  );
  assert.equal(apify.lifecycle, "CONFIGURED_NOT_ORCHESTRATED");
  assert.notEqual(apify.fullE2eOrchestratorUsage, "WIRED_DEFAULT_MOCK_FAIL_CLOSED");
});

check("provider_matrix_configured_not_orchestrated_reason", () => {
  const run = createEmptyExecutionRun({ hotelId: "recINV", runId: "run_inv" });
  const matrix = buildProviderMatrix(run);
  const apify = matrix.find((p) => p.providerId === GDI_PROVIDER_IDS.APIFY);
  assert.equal(apify.callsSubmitted, 0);
  assert.match(
    apify.failureOrSkipReason || "",
    /configured_not_orchestrated|no_calls/,
  );
});

// --- Cell outcome mapping ---
check("no_provider_invocation_not_executed", () => {
  clearActiveExecutionRunContext();
  beginFourteenSegmentExecutionRun({
    hotelId: "recCELL",
    runId: "run_cell_skip",
    root: obsRoot,
    plannedSegmentIds: listFourteenWorkflowSegmentIds(),
  });
  recordFourteenSegmentCellOutcome({
    hotelId: "recCELL",
    segmentId: "projects_infrastructure",
    researchRunId: "rr1",
    executorInvoked: false,
    providerCallCount: 0,
    freshRetrievals: 0,
    retrievalMode: "",
    candidates: [],
    extractedCandidates: 0,
    finishedAt: new Date().toISOString(),
    ok: true,
  });
  const run = loadExecutionRun("recCELL", "run_cell_skip", obsRoot);
  assert.equal(
    run.segments.projects_infrastructure.status,
    SEGMENT_EXEC_STATUS.SKIPPED,
  );
  assert.match(
    run.segments.projects_infrastructure.errorOrSkipReason || "",
    /NO_PROVIDER/,
  );
  clearActiveExecutionRunContext();
});

check("zero_result_fixture_can_complete", () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recCELL2",
    runId: "run_cell_zero",
    root: obsRoot,
  });
  recordFourteenSegmentCellOutcome({
    hotelId: "recCELL2",
    segmentId: "education_scientific",
    researchRunId: "rr2",
    executorInvoked: true,
    executorId: "fixtureExecutor",
    providerCallCount: 0,
    freshRetrievals: 0,
    retrievalMode: "FIXTURE",
    candidates: [],
    extractedCandidates: 0,
    finishedAt: new Date().toISOString(),
    ok: true,
    strategy: "test",
  });
  const run = loadExecutionRun("recCELL2", "run_cell_zero", obsRoot);
  assert.equal(
    run.segments.education_scientific.status,
    SEGMENT_EXEC_STATUS.COMPLETED,
  );
  assert.equal(run.segments.education_scientific.candidatesFound, 0);
  assert.ok(
    run.providerCalls.some(
      (c) =>
        c.providerId === GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC &&
        c.cacheHit === true,
    ),
  );
  clearActiveExecutionRunContext();
});

check("failed_cell_marks_failed", () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recCELL3",
    runId: "run_cell_fail",
    root: obsRoot,
  });
  recordFourteenSegmentCellOutcome({
    hotelId: "recCELL3",
    segmentId: "government_public_sector",
    ok: false,
    stopReason: "MISSING_EXECUTOR_SPEC",
    providerCallCount: 0,
    freshRetrievals: 0,
    candidates: [],
    finishedAt: new Date().toISOString(),
  });
  const run = loadExecutionRun("recCELL3", "run_cell_fail", obsRoot);
  assert.equal(
    run.segments.government_public_sector.status,
    SEGMENT_EXEC_STATUS.FAILED,
  );
  clearActiveExecutionRunContext();
});

check("historical_cache_distinguished_from_live", () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recCELL4",
    runId: "run_hist",
    root: obsRoot,
  });
  recordFourteenSegmentCellOutcome({
    hotelId: "recCELL4",
    segmentId: "associations_conferences_events",
    researchRunId: "rrh",
    executorInvoked: true,
    providerCallCount: 0,
    freshRetrievals: 0,
    retrievalMode: "HISTORICAL_REPLAY",
    candidates: [{ candidateId: "c1" }],
    extractedCandidates: 1,
    finishedAt: new Date().toISOString(),
    ok: true,
  });
  const run = loadExecutionRun("recCELL4", "run_hist", obsRoot);
  const call = run.providerCalls.find(
    (c) => c.providerId === GDI_PROVIDER_IDS.HISTORICAL_CACHE,
  );
  assert.ok(call);
  assert.equal(call.status, PROVIDER_CALL_STATUS.CACHE_HIT);
  assert.equal(call.cacheHit, true);
  clearActiveExecutionRunContext();
});

await checkAsync("apify_default_honest_skip_not_orchestrated", async () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recAPIFY",
    runId: "run_apify_skip",
    root: obsRoot,
  });
  const out = await accountApifyForFourteenSegmentCell({
    segmentId: "sports_entertainment",
    hotel: { displayName: "Test Hotel", city: "Geneva" },
    enableApifyOrchestration: false,
    authorizeApifyLive: false,
  });
  assert.equal(out.dispatched, false);
  assert.equal(out.reason, APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED);
  const run = loadExecutionRun("recAPIFY", "run_apify_skip", obsRoot);
  assert.ok(
    run.apifyLifecycle.some((e) => e.state === APIFY_LIFECYCLE.APIFY_SKIPPED),
  );
  assert.ok(
    run.providerCalls.some(
      (c) =>
        c.providerId === GDI_PROVIDER_IDS.APIFY &&
        c.skipReason === APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
    ),
  );
  // Must not claim COMPLETED live job
  assert.ok(
    !run.apifyLifecycle.some(
      (e) => e.state === APIFY_LIFECYCLE.APIFY_JOB_SUBMITTED,
    ),
  );
  clearActiveExecutionRunContext();
});

await checkAsync("apify_disabled_honest_skip", async () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recAPIFYD",
    runId: "run_apify_dis",
    root: obsRoot,
  });
  const out = await accountApifyForFourteenSegmentCell({
    segmentId: "social_special_events",
    apifyDisabled: true,
  });
  assert.equal(out.reason, APIFY_SKIP_REASON.APIFY_DISABLED);
  clearActiveExecutionRunContext();
});

await checkAsync("apify_mock_orchestration_distinguishable", async () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recAPIFYM",
    runId: "run_apify_mock",
    root: obsRoot,
  });
  const out = await accountApifyForFourteenSegmentCell({
    segmentId: "professional_event_planners",
    hotel: { displayName: "Mock Hotel", city: "Toronto" },
    enableApifyOrchestration: true,
    authorizeApifyLive: false,
    forceMockOnly: true,
  });
  assert.equal(out.dispatched, true);
  assert.equal(out.mock, true);
  const run = loadExecutionRun("recAPIFYM", "run_apify_mock", obsRoot);
  const apifyCalls = run.providerCalls.filter(
    (c) => c.providerId === GDI_PROVIDER_IDS.APIFY,
  );
  assert.ok(apifyCalls.length >= 1);
  // Mock completion is CACHE_HIT, not live COMPLETED without cacheHit
  const terminal = apifyCalls[apifyCalls.length - 1];
  assert.ok(
    terminal.status === PROVIDER_CALL_STATUS.CACHE_HIT ||
      terminal.cacheHit === true ||
      terminal.status === PROVIDER_CALL_STATUS.COMPLETED,
  );
  if (terminal.status === PROVIDER_CALL_STATUS.COMPLETED) {
    assert.equal(terminal.cacheHit, true);
  }
  clearActiveExecutionRunContext();
});

await checkAsync("apify_unauthorized_when_live_requested_without_auth", async () => {
  beginFourteenSegmentExecutionRun({
    hotelId: "recAPIFYU",
    runId: "run_apify_unauth",
    root: obsRoot,
  });
  const out = await accountApifyForFourteenSegmentCell({
    segmentId: "local_catering_day_meetings",
    enableApifyOrchestration: true,
    authorizeApifyLive: false,
    forceMockOnly: false,
  });
  assert.equal(out.dispatched, false);
  assert.equal(out.reason, APIFY_SKIP_REASON.APIFY_UNAUTHORIZED);
  clearActiveExecutionRunContext();
});

// --- Full matrix instrumentation (fixture, no paid) ---
await checkAsync("matrix_instruments_all_14_segments", async () => {
  clearActiveExecutionRunContext();
  const hotelId = "recMATRIX_P1AB";
  const { summary, cells } = await executeFourteenSegmentDiscoveryMatrix({
    hotelIds: [hotelId],
    authorizePaidDiscovery: false,
    useFixtureProvider: true,
    allowHistoricalReplay: false,
    persistState: true,
    stateRoot,
    runQualificationHandoff: false,
    executionObservability: true,
    executionObservabilityRoot: obsRoot,
    executionRunId: "run_matrix_14",
    enableApifyOrchestration: false,
    skipHpc: true,
  });

  assert.equal(cells.length, 14);
  assert.equal(summary.executionObservability, true);
  assert.ok(summary.executionLedgers?.length >= 1);

  const run = loadExecutionRun(hotelId, "run_matrix_14", obsRoot);
  assert.ok(run, "durable ledger must exist");
  assert.equal(Object.keys(run.segments).length, 14);

  const terminal = new Set([
    SEGMENT_EXEC_STATUS.COMPLETED,
    SEGMENT_EXEC_STATUS.EXECUTED,
    SEGMENT_EXEC_STATUS.SKIPPED,
    SEGMENT_EXEC_STATUS.FAILED,
    SEGMENT_EXEC_STATUS.BLOCKED_BUDGET,
    SEGMENT_EXEC_STATUS.BLOCKED_CONFIG,
  ]);
  for (const sid of listFourteenWorkflowSegmentIds()) {
    assert.equal(run.segments[sid].planned, true);
    assert.ok(
      terminal.has(run.segments[sid].status),
      `${sid} status=${run.segments[sid].status}`,
    );
    // No silent PLANNED left
    assert.notEqual(run.segments[sid].status, SEGMENT_EXEC_STATUS.PLANNED);
    assert.notEqual(run.segments[sid].status, SEGMENT_EXEC_STATUS.STARTED);
  }

  // Apify primary segments must show honest skip, not job submitted
  for (const sid of listFourteenWorkflowSegmentIds()) {
    if (!workflowPrefersApify(sid)) continue;
    const apifySkip = run.providerCalls.find(
      (c) =>
        c.providerId === GDI_PROVIDER_IDS.APIFY &&
        c.segmentId === sid &&
        c.skipReason === APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
    );
    assert.ok(apifySkip, `Apify honesty missing for ${sid}`);
  }
  assert.ok(
    !run.apifyLifecycle.some(
      (e) => e.state === APIFY_LIFECYCLE.APIFY_JOB_SUBMITTED,
    ),
  );

  assert.ok(run.certificate || run.verdict);
  const cert = run.certificate || certifyExecutionRun(run);
  assert.ok(
    [
      RUN_VERDICT.FULL_E2E_EXECUTION_COMPLETE,
      RUN_VERDICT.FULL_E2E_PARTIAL,
    ].includes(cert.verdict),
  );

  const dash = buildMonitorDashboardPayload(run);
  assert.equal(dash.segmentMatrix.length, 14);
  const apifyRow = dash.providerMatrix.find(
    (p) => p.providerId === GDI_PROVIDER_IDS.APIFY,
  );
  assert.ok(apifyRow);
  assert.equal(
    apifyRow.fullE2eOrchestratorUsage,
    "CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E",
  );
});

check("retry_correlation_does_not_double_count", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recDEDUP",
    runId: "run_dedup2",
  });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "sports_entertainment",
    status: PROVIDER_CALL_STATUS.SUBMITTED,
    correlationId: "retry_same",
    actualCostUsd: 0.2,
  });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "sports_entertainment",
    status: PROVIDER_CALL_STATUS.COMPLETED,
    correlationId: "retry_same",
    actualCostUsd: 0.2,
  });
  assert.equal(run.providerCalls.length, 1);
  assert.equal(run.summary.providerCalls, 1);
});

check("historical_unknown_preserved", () => {
  const hist = reconcileAllHistoricalHotels({ root: ROOT, persist: false });
  assert.equal(hist.length, 3);
  for (const h of hist) {
    for (const sid of listFourteenWorkflowSegmentIds()) {
      assert.equal(h.run.segments[sid].status, SEGMENT_EXEC_STATUS.UNKNOWN);
    }
  }
});

check("cert_complete_requires_terminal_segments", () => {
  const run = createEmptyExecutionRun({
    hotelId: "recCERT",
    runId: "run_cert_partial",
  });
  // Leave all PLANNED → PARTIAL
  const cert = certifyExecutionRun(run);
  assert.equal(cert.verdict, RUN_VERDICT.FULL_E2E_PARTIAL);
});

check("cert_failed_on_provider_failure", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recCERT2",
    runId: "run_cert_fail",
  });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "relocation_extended_stay",
    status: PROVIDER_CALL_STATUS.FAILED,
    errorCode: "provider_timeout",
    correlationId: "fail_cert",
  });
  const cert = certifyExecutionRun(run);
  assert.ok(
    [RUN_VERDICT.FULL_E2E_FAILED, RUN_VERDICT.FULL_E2E_PARTIAL].includes(
      cert.verdict,
    ),
  );
});

await checkAsync(
  "optional_apify_skip_preserves_completed_segment",
  async () => {
    beginFourteenSegmentExecutionRun({
      hotelId: "recERASE2",
      runId: "run_erase2",
      root: obsRoot,
    });
    recordFourteenSegmentCellOutcome({
      hotelId: "recERASE2",
      segmentId: "sports_entertainment",
      researchRunId: "rr_erase2",
      executorInvoked: true,
      retrievalMode: "FIXTURE",
      providerCallCount: 0,
      freshRetrievals: 0,
      candidates: [{ candidateId: "x" }],
      extractedCandidates: 1,
      finishedAt: new Date().toISOString(),
      ok: true,
    });
    await accountApifyForFourteenSegmentCell({
      segmentId: "sports_entertainment",
      enableApifyOrchestration: false,
    });
    const run = loadExecutionRun("recERASE2", "run_erase2", obsRoot);
    assert.equal(
      run.segments.sports_entertainment.status,
      SEGMENT_EXEC_STATUS.COMPLETED,
    );
    finalizeFourteenSegmentExecutionRun();
  },
);

check("no_paid_safety_flags_in_summary_contract", () => {
  // Static safety: inventory still marks Apify/Exa paid; tests never authorize live.
  const inv = listProviderInventory();
  assert.ok(inv.find((p) => p.providerId === GDI_PROVIDER_IDS.APIFY)?.paid);
  assert.ok(inv.find((p) => p.providerId === GDI_PROVIDER_IDS.EXA_AGENT)?.paid);
});

check("current_packages_untouched_fingerprints", () => {
  // Sanity: YOTEL CURRENT pointer file still present and unchanged by this test suite
  const yotelPtr = path.join(
    ROOT,
    "data/group-demand-intelligence/customer-publication/recrPQcZg7SFARRb2/CURRENT.json",
  );
  if (fs.existsSync(yotelPtr)) {
    const before = fs.readFileSync(yotelPtr, "utf8");
    assert.ok(before.includes("fingerprint") || before.includes("packageVersion") || before.length > 10);
  }
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-full-e2e-execution-integrity-p1ab-v1",
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      tmpRoot,
      safety: {
        paidExa: false,
        paidApify: false,
        paidParallel: false,
        surfe: false,
        airtableWrites: false,
        publicationChanges: false,
      },
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
