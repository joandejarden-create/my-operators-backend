#!/usr/bin/env node
/**
 * GDI Full E2E all-provider execution observability V1 — mocked tests.
 * No paid Exa / Apify / Parallel / Surfe / Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";
import {
  GDI_PROVIDER_IDS,
  listProviderInventory,
  createEmptyExecutionRun,
  recordProviderCall,
  recordApifyLifecycle,
  updateSegmentStatus,
  updateStageStatus,
  saveExecutionRun,
  loadExecutionRun,
  listExecutionRuns,
  buildProviderMatrix,
  detectSilentSegmentSkips,
  certifyExecutionRun,
  buildMonitorDashboardPayload,
  SEGMENT_EXEC_STATUS,
  PROVIDER_CALL_STATUS,
  APIFY_LIFECYCLE,
  STAGE_STATUS,
  E2E_STAGE,
  RUN_VERDICT,
} from "../lib/group-demand-intelligence/observability/gdi-execution-ledger-store-v1.js";
import {
  setActiveExecutionRunContext,
  clearActiveExecutionRunContext,
  observeProviderCall,
} from "../lib/group-demand-intelligence/observability/gdi-execution-run-context-v1.js";
import {
  reconcileAllHistoricalHotels,
  HOTEL_SPECS,
} from "../lib/group-demand-intelligence/observability/gdi-historical-execution-reconciliation-v1.js";
import { orchestrateApifyDiscoveryForWorkflow } from "../lib/group-demand-intelligence/research/apify/apify-fulle2e-orchestration-v1.js";
import {
  buildExaSegmentRequestHeading,
  GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-exa-segment-request-titles-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-exec-obs-"));

check("all_14_segments_represented", () => {
  const run = createEmptyExecutionRun({
    hotelId: "recTEST",
    runId: "run_test_1",
    root: tmpRoot,
  });
  assert.equal(Object.keys(run.segments).length, 14);
  for (const id of listFourteenWorkflowSegmentIds()) {
    assert.ok(run.segments[id]);
    assert.equal(
      run.segments[id].displayTitle,
      GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[id],
    );
  }
});

check("actual_calls_not_config_determine_execution", () => {
  let run = createEmptyExecutionRun({ hotelId: "recTEST", runId: "run_calls" });
  const matrix0 = buildProviderMatrix(run);
  const apify0 = matrix0.find((p) => p.providerId === GDI_PROVIDER_IDS.APIFY);
  assert.equal(apify0.callsSubmitted, 0);
  assert.match(apify0.failureOrSkipReason || "", /no_calls|configured/);

  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "education_scientific",
    researchStage: "DISCOVERY",
    operation: "DISCOVERY",
    status: PROVIDER_CALL_STATUS.COMPLETED,
    actualCostUsd: 0.5,
    providerRequestId: "exa_mock_1",
    candidateCount: 3,
    correlationId: "c1",
  });
  assert.equal(run.segments.education_scientific.requestCount, 1);
  assert.equal(
    run.segments.education_scientific.discoveryStatus,
    SEGMENT_EXEC_STATUS.COMPLETED,
  );
  assert.ok(
    run.segments.education_scientific.providersUsed.includes(
      GDI_PROVIDER_IDS.EXA_AGENT,
    ),
  );
  // Associations must not be marked executed just because inventory exists
  assert.equal(
    run.segments.associations_conferences_events.requestCount,
    0,
  );
});

check("apify_lifecycle_tracking", () => {
  let run = createEmptyExecutionRun({ hotelId: "recTEST", runId: "run_apify" });
  run = recordApifyLifecycle(run, {
    state: APIFY_LIFECYCLE.APIFY_CONFIGURED,
  });
  run = recordApifyLifecycle(run, {
    state: APIFY_LIFECYCLE.APIFY_SKIPPED,
    reason: "live_not_authorized",
    segmentId: "sports_entertainment",
  });
  assert.equal(run.apifyLifecycle.length, 2);
  assert.equal(run.apifyLifecycle[1].state, APIFY_LIFECYCLE.APIFY_SKIPPED);
});

check("exa_stage_a_b_headings_preserved", () => {
  assert.equal(
    buildExaSegmentRequestHeading({
      segmentId: "projects_infrastructure",
      stage: "DISCOVERY",
    }),
    "# PROJECTS / INFRASTRUCTURE — EXA AGENT DISCOVERY CONTRACT V1",
  );
  assert.equal(
    buildExaSegmentRequestHeading({
      segmentId: "relocation_extended_stay",
      stage: "DEEP_RESEARCH",
    }),
    "# RELOCATION / EXTENDED STAY — EXA AGENT DEEP RESEARCH CONTRACT V1",
  );
});

check("cache_only_vs_paid_distinguished", () => {
  let run = createEmptyExecutionRun({ hotelId: "recTEST", runId: "run_cache" });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.HISTORICAL_CACHE,
    segmentId: "government_public_sector",
    operation: "REPLAY",
    status: PROVIDER_CALL_STATUS.CACHE_HIT,
    cacheHit: true,
    correlationId: "cache1",
  });
  const skips = detectSilentSegmentSkips(run);
  assert.ok(skips.some((s) => s.kind === "CACHE_ONLY"));
});

check("scheduled_never_started_detection", () => {
  const run = createEmptyExecutionRun({ hotelId: "recTEST", runId: "run_skip" });
  const skips = detectSilentSegmentSkips(run);
  assert.ok(skips.some((s) => s.kind === "SCHEDULED_NEVER_STARTED"));
});

check("failed_provider_and_partial_verdict", () => {
  let run = createEmptyExecutionRun({ hotelId: "recTEST", runId: "run_fail" });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "local_catering_day_meetings",
    status: PROVIDER_CALL_STATUS.FAILED,
    errorCode: "timeout",
    correlationId: "fail1",
  });
  const cert = certifyExecutionRun(run);
  assert.ok(
    [RUN_VERDICT.FULL_E2E_FAILED, RUN_VERDICT.FULL_E2E_PARTIAL].includes(
      cert.verdict,
    ),
  );
});

check("dedupe_retries_by_correlation_id", () => {
  let run = createEmptyExecutionRun({ hotelId: "recTEST", runId: "run_dedupe" });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "sports_entertainment",
    status: PROVIDER_CALL_STATUS.SUBMITTED,
    correlationId: "same",
    actualCostUsd: 0.1,
  });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "sports_entertainment",
    status: PROVIDER_CALL_STATUS.COMPLETED,
    correlationId: "same",
    actualCostUsd: 0.1,
  });
  assert.equal(run.providerCalls.length, 1);
  assert.equal(run.providerCalls[0].status, PROVIDER_CALL_STATUS.COMPLETED);
});

check("persistence_survives_reload", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recPERSIST",
    runId: "run_persist",
  });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC,
    segmentId: "professional_event_planners",
    status: PROVIDER_CALL_STATUS.COMPLETED,
    correlationId: "p1",
  });
  saveExecutionRun(run, tmpRoot);
  const loaded = loadExecutionRun("recPERSIST", "run_persist", tmpRoot);
  assert.ok(loaded);
  assert.equal(loaded.providerCalls.length, 1);
  const listed = listExecutionRuns("recPERSIST", tmpRoot);
  assert.equal(listed[0].runId, "run_persist");
});

check("run_context_noop_without_active_run", () => {
  clearActiveExecutionRunContext();
  assert.equal(observeProviderCall({ providerId: "exa_agent" }), null);
});

check("run_context_records_when_active", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recCTX",
    runId: "run_ctx",
  });
  saveExecutionRun(run, tmpRoot);
  setActiveExecutionRunContext({
    hotelId: "recCTX",
    runId: "run_ctx",
    root: tmpRoot,
  });
  observeProviderCall({
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "corporate_meetings_incentives",
    status: PROVIDER_CALL_STATUS.COMPLETED,
    correlationId: "ctx1",
    actualCostUsd: 0.25,
  });
  clearActiveExecutionRunContext();
  const loaded = loadExecutionRun("recCTX", "run_ctx", tmpRoot);
  assert.equal(loaded.providerCalls.length, 1);
  assert.equal(
    loaded.segments.corporate_meetings_incentives.requestCount,
    1,
  );
});

check("historical_reconciliation_marks_unknown_not_fabricated", () => {
  const results = reconcileAllHistoricalHotels({
    root: ROOT,
    persist: false,
  });
  assert.equal(results.length, 3);
  for (const r of results) {
    assert.equal(r.run.historical, true);
    assert.equal(r.run.historicalConfidence, "PARTIAL_EVIDENCE");
    assert.notEqual(
      r.run.certificate.verdict,
      RUN_VERDICT.FULL_E2E_EXECUTION_COMPLETE,
    );
    // Must not invent 14 executed segments from taxonomy alone
    const executed = Object.values(r.run.segments).filter(
      (s) => s.requestCount > 0 && s.segmentId !== "CROSS_SEGMENT",
    );
    // Aggregate Exa call is CROSS_SEGMENT only — segment rows stay UNKNOWN
    for (const sid of listFourteenWorkflowSegmentIds()) {
      assert.equal(r.run.segments[sid].status, SEGMENT_EXEC_STATUS.UNKNOWN);
    }
    assert.ok(r.run.apifyLifecycle.some((e) => e.state === APIFY_LIFECYCLE.APIFY_SKIPPED));
  }
  assert.equal(HOTEL_SPECS.length, 3);
});

check("dashboard_payload_safe_no_secrets", () => {
  let run = createEmptyExecutionRun({ hotelId: "recDASH", runId: "run_dash" });
  run = recordProviderCall(run, {
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: "education_scientific",
    status: PROVIDER_CALL_STATUS.COMPLETED,
    correlationId: "d1",
  });
  run = updateStageStatus(run, E2E_STAGE.E_SUBSTANTIVE_RESEARCH, {
    status: STAGE_STATUS.COMPLETED,
  });
  const dash = buildMonitorDashboardPayload(run);
  const blob = JSON.stringify(dash);
  // Config field *names* may mention TOKEN; forbid secret *values*.
  assert.doesNotMatch(blob, /Bearer\s+[A-Za-z0-9._-]+/i);
  assert.doesNotMatch(blob, /sk-[A-Za-z0-9]{10,}/);
  assert.doesNotMatch(blob, /"password"\s*:\s*"[^"]+"/i);
  assert.equal(dash.segmentMatrix.length, 14);
  assert.ok(dash.providerMatrix.length >= 8);
  assert.ok(dash.certificate.verdict);
});

check("provider_inventory_lists_configured_unused", () => {
  const inv = listProviderInventory();
  const serp = inv.find((p) => p.providerId === GDI_PROVIDER_IDS.SERPAPI);
  assert.ok(serp);
  assert.equal(serp.lifecycle, "UNUSED");
  assert.equal(serp.fullE2eOrchestratorUsage, "NOT_INVOKED");
});

check("admin_shell_wires_execution_monitor_tab", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/ai-demand-admin.html"),
    "utf8",
  );
  const shell = fs.readFileSync(
    path.join(ROOT, "public/js/admin-ai-demand-admin.js"),
    "utf8",
  );
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/admin-gdi-execution-monitor.js"),
    "utf8",
  );
  assert.match(html, /gdi-execution-monitor/);
  assert.match(html, /adaPanelGdiExecutionMonitor/);
  assert.match(shell, /gdi-execution-monitor/);
  assert.match(js, /execution-monitor\/reconcile-historical/);
  assert.match(js, /gemApifyLifecycle/);
});

check("server_routes_admin_only_pattern", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.match(server, /execution-monitor\/hotels/);
  assert.match(server, /requireAdminAccess/);
  assert.match(server, /getGdiExecutionMonitorRun/);
});

{
  const name = "apify_orchestration_mock_records_when_context_set";
  try {
    let run = createEmptyExecutionRun({
      hotelId: "recAPIFY2",
      runId: "run_apify_ctx2",
    });
    saveExecutionRun(run, tmpRoot);
    setActiveExecutionRunContext({
      hotelId: "recAPIFY2",
      runId: "run_apify_ctx2",
      root: tmpRoot,
    });
    await orchestrateApifyDiscoveryForWorkflow({
      workflow: { id: "sports_entertainment" },
      hotel: { displayName: "Test", city: "Geneva" },
      forceMockOnly: true,
      authorizeApifyLive: false,
    });
    clearActiveExecutionRunContext();
    const loaded = loadExecutionRun("recAPIFY2", "run_apify_ctx2", tmpRoot);
    assert.ok(loaded.apifyLifecycle.length >= 1);
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-full-e2e-all-provider-execution-observability-v1",
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      tmpRoot,
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
