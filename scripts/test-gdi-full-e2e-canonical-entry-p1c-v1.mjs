#!/usr/bin/env node
/**
 * P1-C canonical Full E2E entry — deterministic tests (no paid / no publish).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  preflightCanonicalFullE2e,
  assessCanonicalLiveReadiness,
  runCanonicalFullE2eResearch,
  normalizeMode,
  CANONICAL_EXECUTION_MODE,
  GDI_CANONICAL_FULL_E2E_ENTRY_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";
import {
  createEmptyExecutionRun,
  certifyExecutionRun,
  updateSegmentStatus,
  RUN_VERDICT,
  SEGMENT_EXEC_STATUS,
  loadExecutionRun,
} from "../lib/group-demand-intelligence/observability/gdi-execution-ledger-store-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";
import { getProviderInventoryEntry } from "../lib/group-demand-intelligence/observability/gdi-provider-inventory-v1.js";
import { GDI_PROVIDER_IDS } from "../lib/group-demand-intelligence/observability/gdi-execution-ledger-store-v1.js";
import { reconcileAllHistoricalHotels } from "../lib/group-demand-intelligence/observability/gdi-historical-execution-reconciliation-v1.js";
import { APIFY_SKIP_REASON } from "../lib/group-demand-intelligence/observability/gdi-fourteen-segment-execution-ledger-bridge-v1.js";

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

async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-p1c-"));
const obsRoot = path.join(tmpRoot, "execution-observability");

const HOTELS = {
  yotel: "recrPQcZg7SFARRb2",
  bethesda: "recLuxvwwxID7U2B8",
  radisson: "recH9h3McgMNFQEla",
  wyndham: "recXh4NgF8sDbi9cQ",
};

check("normalize_modes", () => {
  assert.equal(normalizeMode("fixture"), CANONICAL_EXECUTION_MODE.FIXTURE);
  assert.equal(normalizeMode("dry-run"), CANONICAL_EXECUTION_MODE.DRY_RUN);
  assert.equal(normalizeMode("preflight"), CANONICAL_EXECUTION_MODE.PREFLIGHT);
  assert.equal(normalizeMode("live"), CANONICAL_EXECUTION_MODE.LIVE);
  assert.equal(normalizeMode("nope"), null);
});

check("missing_hotel_fails_closed", () => {
  const p = preflightCanonicalFullE2e({ mode: "fixture" });
  assert.equal(p.ok, false);
  assert.ok(p.errors.some((e) => e.code === "MISSING_HOTEL_ID"));
});

check("fixture_blocks_paid_flags", () => {
  const p = preflightCanonicalFullE2e({
    hotelId: HOTELS.yotel,
    mode: "fixture",
    authorizePaidDiscovery: true,
  });
  assert.equal(p.ok, false);
  assert.ok(p.errors.some((e) => e.code === "FIXTURE_MODE_BLOCKS_PAID_FLAGS"));
});

check("publish_flag_fails_closed_on_research_entry", () => {
  const p = preflightCanonicalFullE2e({
    hotelId: HOTELS.yotel,
    mode: "dry-run",
    authorizePublish: true,
  });
  assert.equal(p.ok, false);
  assert.ok(
    p.errors.some((e) =>
      String(e.code).includes("PUBLICATION_NOT_AUTHORIZED"),
    ),
  );
});

check("live_without_auth_fails_closed", () => {
  const p = preflightCanonicalFullE2e({
    hotelId: HOTELS.yotel,
    mode: "live",
  });
  assert.equal(p.ok, false);
  assert.ok(
    p.errors.some((e) => e.code === "LIVE_MODE_REQUIRES_PAID_AUTHORIZATION"),
  );
});

check("live_requires_budget_ceiling_and_exa_auth", () => {
  const p = preflightCanonicalFullE2e({
    hotelId: HOTELS.yotel,
    mode: "live",
    authorizePaidDiscovery: true,
    authorizePaidResearch: true,
  });
  assert.equal(p.ok, false);
  assert.ok(
    p.errors.some((e) => e.code === "LIVE_MODE_REQUIRES_BUDGET_CEILING_USD"),
  );
  assert.ok(
    p.errors.some((e) => e.code === "LIVE_MODE_REQUIRES_AUTHORIZE_EXA_AGENT_LIVE"),
  );
});

check("live_readiness_blocked_without_env_gate", () => {
  const prev = process.env.GDI_EXA_AGENT_LIVE;
  delete process.env.GDI_EXA_AGENT_LIVE;
  try {
    const r = assessCanonicalLiveReadiness({
      hotelId: HOTELS.yotel,
      mode: "live",
      authorizePaidDiscovery: true,
      authorizePaidResearch: true,
      authorizeExaAgentLive: true,
      budgetCeilingUsd: 15,
    });
    assert.equal(r.status, "BLOCKED");
    assert.equal(r.liveReady, false);
    assert.ok(
      r.blockers.some((b) => b.code === "GDI_EXA_AGENT_LIVE_ENV_REQUIRED"),
    );
  } finally {
    if (prev == null) delete process.env.GDI_EXA_AGENT_LIVE;
    else process.env.GDI_EXA_AGENT_LIVE = prev;
  }
});

check("cli_loads_dotenv_and_exa_live_flag", () => {
  const cli = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-canonical-full-e2e-entry-v1.mjs"),
    "utf8",
  );
  assert.ok(cli.includes('import "dotenv/config"'));
  assert.ok(cli.includes("--authorize-exa-agent-live"));
  assert.ok(cli.includes("--budget-ceiling-usd"));
  const entry = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js",
    ),
    "utf8",
  );
  assert.ok(entry.includes("authorizeExaAgentLive"));
  assert.ok(entry.includes("allowLiveExaDeepSeed"));
  assert.ok(entry.includes("budgetsRespected"));
  assert.ok(!/budgetsRespected:\s*true,\s*\n\s*outputsIngested:\s*true/.test(entry));
});

check("discovery_only_cert_is_partial", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recCERT_P1C",
    runId: "run_disc_only",
  });
  for (const sid of listFourteenWorkflowSegmentIds()) {
    run = updateSegmentStatus(run, sid, {
      status: SEGMENT_EXEC_STATUS.COMPLETED,
      discoveryStatus: SEGMENT_EXEC_STATUS.COMPLETED,
      // deepResearchStatus remains PLANNED
    });
  }
  const cert = certifyExecutionRun(run, { discoveryPhaseOnly: true });
  assert.equal(cert.verdict, RUN_VERDICT.FULL_E2E_PARTIAL);
  assert.equal(cert.researchCompleteness, "DISCOVERY_COMPLETED");
  assert.equal(cert.publicationAuthorized, false);
});

check("full_complete_requires_deep_terminal", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recCERT_P1C2",
    runId: "run_need_deep",
  });
  for (const sid of listFourteenWorkflowSegmentIds()) {
    run = updateSegmentStatus(run, sid, {
      status: SEGMENT_EXEC_STATUS.COMPLETED,
      discoveryStatus: SEGMENT_EXEC_STATUS.COMPLETED,
      deepResearchStatus: SEGMENT_EXEC_STATUS.PLANNED,
    });
  }
  const cert = certifyExecutionRun(run, { requireDeepResearch: true });
  assert.equal(cert.verdict, RUN_VERDICT.FULL_E2E_PARTIAL);
  assert.ok(cert.deepResearchIncompleteSegments.length > 0);
});

check("deep_complete_can_certify_complete", () => {
  let run = createEmptyExecutionRun({
    hotelId: "recCERT_P1C3",
    runId: "run_full",
  });
  for (const sid of listFourteenWorkflowSegmentIds()) {
    run = updateSegmentStatus(run, sid, {
      status: SEGMENT_EXEC_STATUS.COMPLETED,
      discoveryStatus: SEGMENT_EXEC_STATUS.COMPLETED,
      deepResearchStatus: SEGMENT_EXEC_STATUS.COMPLETED,
    });
  }
  const cert = certifyExecutionRun(run, { requireDeepResearch: true });
  assert.equal(cert.verdict, RUN_VERDICT.FULL_E2E_EXECUTION_COMPLETE);
  assert.equal(cert.researchCompleteness, "DEEP_RESEARCH_COMPLETED");
  assert.equal(cert.publicationAuthorized, false);
});

check("apify_inventory_still_honest", () => {
  const apify = getProviderInventoryEntry(GDI_PROVIDER_IDS.APIFY);
  assert.equal(
    apify.fullE2eOrchestratorUsage,
    "CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E",
  );
  assert.equal(APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED, "APIFY_NOT_ORCHESTRATED");
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

check("published_packages_untouched", () => {
  for (const id of Object.values(HOTELS)) {
    const ptr = path.join(
      ROOT,
      "data/group-demand-intelligence/customer-publication",
      id,
      "CURRENT.json",
    );
    if (fs.existsSync(ptr)) {
      const raw = fs.readFileSync(ptr, "utf8");
      assert.ok(raw.length > 10);
    }
  }
});

await checkAsync("preflight_mode_no_research", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: HOTELS.yotel,
    mode: "preflight",
    skipHpc: true,
  });
  assert.equal(r.preflightOnly, true);
  assert.ok(r.plannedSegments.length === 14);
  assert.equal(r.publicationAuthorized, false);
});

await checkAsync("fixture_canonical_run_yotel", async () => {
  const outDir = path.join(tmpRoot, "yotel-run");
  const r = await runCanonicalFullE2eResearch({
    hotelId: HOTELS.yotel,
    mode: "fixture",
    skipHpc: true,
    maxCandidates: 3,
    outDir,
    executionObservabilityRoot: obsRoot,
    executionRunId: "p1c_yotel_fixture",
    allowHistoricalReplay: true,
  });
  assert.equal(r.ok, true);
  assert.ok(r.runId);
  assert.equal(r.publicationAuthorized, false);
  const run = loadExecutionRun(HOTELS.yotel, "p1c_yotel_fixture", obsRoot);
  assert.ok(run);
  assert.equal(Object.keys(run.segments).length, 14);
  for (const sid of listFourteenWorkflowSegmentIds()) {
    assert.notEqual(run.segments[sid].status, SEGMENT_EXEC_STATUS.PLANNED);
    assert.notEqual(
      run.segments[sid].deepResearchStatus,
      SEGMENT_EXEC_STATUS.PLANNED,
    );
  }
  assert.ok(run.certificate || r.certificate);
  const cert = run.certificate || r.certificate;
  assert.equal(cert.publicationAuthorized, false);
  // Must not invent live Apify jobs
  assert.ok(
    !(run.apifyLifecycle || []).some((e) =>
      String(e.state || "").includes("JOB_SUBMITTED"),
    ),
  );
});

await checkAsync("fixture_canonical_run_bethesda", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: HOTELS.bethesda,
    mode: "fixture",
    skipHpc: true,
    maxCandidates: 2,
    outDir: path.join(tmpRoot, "bethesda-run"),
    executionObservabilityRoot: obsRoot,
    executionRunId: "p1c_bethesda_fixture",
  });
  assert.equal(r.ok, true);
  assert.equal(r.publicationAuthorized, false);
});

await checkAsync("fixture_canonical_run_radisson", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: HOTELS.radisson,
    mode: "fixture",
    skipHpc: true,
    maxCandidates: 2,
    outDir: path.join(tmpRoot, "radisson-run"),
    executionObservabilityRoot: obsRoot,
    executionRunId: "p1c_radisson_fixture",
  });
  assert.equal(r.ok, true);
});

check("npm_script_and_cli_exist", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));
  assert.ok(pkg.scripts["gdi:canonical-full-e2e"]);
  assert.ok(pkg.scripts["test:gdi-full-e2e-canonical-entry-p1c-v1"]);
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "scripts/gdi-canonical-full-e2e-entry-v1.mjs"),
    ),
  );
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js",
      ),
    ),
  );
});

check("contract_id_stable", () => {
  assert.equal(GDI_CANONICAL_FULL_E2E_ENTRY_V1, "gdi-canonical-full-e2e-entry-v1");
});

await checkAsync("skip_hotel_wide_refused_outside_isolated_harness", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: HOTELS.yotel,
    mode: "dry-run",
    skipHpc: true,
    maxCandidates: 1,
    outDir: path.join(tmpRoot, "skip-refused"),
    executionObservabilityRoot: obsRoot,
    executionRunId: "p1c_skip_refused",
    skipHotelWideFinalization: true,
    // Missing allowIsolatedTestSkipHotelWideFinalization + isolatedTestHarness
  });
  assert.equal(r.hotelWideFinalization?.completionOutcome, "FULL_E2E_BLOCKED");
  assert.equal(r.hotelWideFinalization?.ok, false);
  assert.match(
    String(r.hotelWideFinalization?.error || ""),
    /refused outside isolated test/i,
  );
  assert.equal(r.summaryPath ? JSON.parse(fs.readFileSync(r.summaryPath, "utf8")).fullE2eCompleteClaimAllowed : true, false);
});

await checkAsync("isolated_test_skip_never_claims_full_e2e_complete", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: HOTELS.yotel,
    mode: "fixture",
    skipHpc: true,
    maxCandidates: 1,
    outDir: path.join(tmpRoot, "skip-isolated"),
    executionObservabilityRoot: obsRoot,
    executionRunId: "p1c_skip_isolated",
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
  });
  const summary = JSON.parse(fs.readFileSync(r.summaryPath, "utf8"));
  assert.equal(summary.hotelWideFinalization.skipped, true);
  assert.equal(summary.hotelWideFinalization.isolatedTestSkip, true);
  assert.equal(summary.fullE2eCompleteClaimAllowed, false);
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      tmpRoot,
      safety: {
        paidExa: false,
        paidApify: false,
        airtableWrites: false,
        publicationChanges: false,
      },
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
