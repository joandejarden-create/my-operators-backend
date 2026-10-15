/**
 * Focused regressions for customer research-history eligibility + semantics.
 * Uses an isolated temp GDI_DATA_ROOT — does not mutate production ledgers.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  ACTIVITY_KIND,
  CUSTOMER_HISTORY_EXCLUSION,
  appendHotelActivityEvent,
  buildHotelActivityHistory,
  classifyCustomerHistoryEligibility,
  formatRunStatusForCustomer,
  loadHotelActivityLedger,
  recoverHotelActivityLedgerFromManifests,
  reannotateHotelActivityLedgerEligibility,
} from "../lib/group-demand-intelligence/hotel-activity-history-v1.js";

const HOTEL = "recHistTestHotel01";
const NOW = "2026-10-15T18:00:00.000Z";

function mkRoot() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-hist-elig-"));
  process.env.GDI_DATA_ROOT = root;
  process.env.GDI_HISTORY_NOW = NOW;
  const weeklyDir = path.join(root, "hotels", HOTEL, "weekly-refresh", "runs");
  fs.mkdirSync(weeklyDir, { recursive: true });
  return { root, weeklyDir };
}

function writeManifest(weeklyDir, name, body) {
  fs.writeFileSync(path.join(weeklyDir, name), JSON.stringify(body, null, 2) + "\n");
}

function testFutureSimulatedExcluded() {
  const { weeklyDir } = mkRoot();
  writeManifest(weeklyDir, "future.json", {
    runId: "gdi_weekly_recHistT_2026-10-29T12-00-00-000Z_c2ef69",
    hotelId: HOTEL,
    dryRun: true,
    clockLabel: "isolated_future_clock_2026-10-29_not_second_live_week",
    status: "completed",
    startedAt: "2026-10-29T12:00:00.000Z",
    completedAt: "2026-10-29T12:00:00.000Z",
    runMode: "combined",
  });
  writeManifest(weeklyDir, "realish-dry.json", {
    runId: "gdi_weekly_recHistT_2026-10-15T10-30-36-069Z_e2f5eb",
    hotelId: HOTEL,
    dryRun: true,
    clockLabel: null,
    status: "completed",
    startedAt: "2026-10-15T10:30:36.069Z",
    completedAt: "2026-10-15T10:30:36.069Z",
    runMode: "combined",
  });

  recoverHotelActivityLedgerFromManifests(HOTEL);
  const hist = buildHotelActivityHistory(HOTEL, {
    opportunities: [],
    opportunityDoc: {},
    now: NOW,
  });

  assert.equal(hist.lastResearchAt, null, "future/dry manifests must not set Last Research");
  assert.notEqual(hist.runStatus, "completed");
  assert.notEqual(hist.runStatusLabel, "Completed");
  const ledger = loadHotelActivityLedger(HOTEL);
  assert.ok(ledger.events.length >= 2, "audit trail retained");
  const future = ledger.events.find((e) => /2026-10-29/.test(e.runId || ""));
  assert.ok(future);
  assert.equal(future.at, "2026-10-29T12:00:00.000Z", "do not clamp timestamp");
  assert.equal(future.customerHistoryEligible, false);
  assert.ok(
    future.customerHistoryExclusionReasons.includes(CUSTOMER_HISTORY_EXCLUSION.DRY_RUN) ||
      future.customerHistoryExclusionReasons.includes(CUSTOMER_HISTORY_EXCLUSION.TEST_CLOCK) ||
      future.customerHistoryExclusionReasons.includes(CUSTOMER_HISTORY_EXCLUSION.FUTURE_TIMESTAMP)
  );
  console.log("ok future simulated excluded");
}

function testQueueOnlyNotCompletedResearch() {
  const { weeklyDir } = mkRoot();
  writeManifest(weeklyDir, "queue-only.json", {
    runId: "gdi_weekly_recHistT_2026-10-15T08-00-00-000Z_queue1",
    hotelId: HOTEL,
    dryRun: false,
    clockLabel: null,
    status: "completed",
    startedAt: "2026-10-15T08:00:00.000Z",
    completedAt: "2026-10-15T08:00:05.000Z",
    runMode: "queue_only",
    queues: { runMode: "queue_only", coverageBacklog: [{ segmentId: "sports" }] },
  });
  const hist = buildHotelActivityHistory(HOTEL, {
    opportunities: [],
    opportunityDoc: {},
    now: NOW,
  });
  assert.equal(hist.lastResearchAt, null);
  assert.notEqual(hist.runStatusLabel, "Completed");
  const c = classifyCustomerHistoryEligibility({
    dryRun: false,
    status: "completed",
    at: "2026-10-15T08:00:05.000Z",
    queueOnly: true,
    runMode: "queue_only",
  }, { now: NOW });
  assert.equal(c.eligible, false);
  assert.ok(c.reasons.includes(CUSTOMER_HISTORY_EXCLUSION.PLAN_OR_QUEUE_ONLY));
  console.log("ok queue-only not completed research");
}

function testRunIdWithoutCompletion() {
  mkRoot();
  const hist = buildHotelActivityHistory(HOTEL, {
    opportunities: [
      {
        id: "opp1",
        runId: "gdi_weekly_recHistT_2026-10-15T09-24-33-542Z_e21d45",
        lastVerifiedAt: "2026-10-14T12:00:00.000Z",
        createdAt: "2026-10-13T08:00:00.000Z",
      },
    ],
    opportunityDoc: {},
    now: NOW,
  });
  assert.equal(hist.lastResearchAt, null, "verification alone is not Last Research");
  assert.ok(hist.lastSourceVerificationAt);
  assert.equal(hist.lastSourceVerificationAt.slice(0, 10), "2026-10-14");
  assert.equal(hist.runStatus, "STATUS_UNAVAILABLE");
  assert.equal(
    formatRunStatusForCustomer(hist.runStatus, hist.runStatusLabel),
    "Status unavailable"
  );
  console.log("ok runId without completion");
}

function testGenuineCompletedAndPartial() {
  mkRoot();
  appendHotelActivityEvent(HOTEL, {
    kind: ACTIVITY_KIND.WEEKLY_REFRESH,
    status: "completed",
    at: "2026-10-15T11:00:00.000Z",
    runId: "gdi_weekly_recHistT_2026-10-15T11-00-00-000Z_live01",
    source: "weekly-refresh-orchestrator",
    provenance: "live_write",
    dryRun: false,
  });
  appendHotelActivityEvent(HOTEL, {
    kind: ACTIVITY_KIND.RESEARCH_RUN,
    status: "partial",
    at: "2026-10-14T09:00:00.000Z",
    runId: "gdi_paid_recHistT_2026-10-14T09-00-00-000Z_p1",
    source: "paid-multisegment",
    provenance: "live_write",
    dryRun: false,
  });
  const hist = buildHotelActivityHistory(HOTEL, {
    opportunities: [],
    opportunityDoc: {},
    now: NOW,
  });
  assert.equal(hist.lastResearchAt, "2026-10-15T11:00:00.000Z");
  assert.equal(hist.runStatus, "completed");
  assert.equal(hist.runStatusLabel, "Completed");
  assert.equal(hist.latestRunId, "gdi_weekly_recHistT_2026-10-15T11-00-00-000Z_live01");
  console.log("ok genuine completed and partial");
}

function testPublicationPrepVsApply() {
  mkRoot();
  const prepOnly = buildHotelActivityHistory(HOTEL, {
    opportunities: [
      {
        id: "prep",
        updatedAt: "2026-10-15T17:00:00.000Z",
        // preparation basis without successful publish stamp
        publicationPrepAt: "2026-10-15T17:00:00.000Z",
      },
    ],
    opportunityDoc: { updatedAt: "2026-10-15T17:00:00.000Z" },
    now: NOW,
  });
  assert.equal(prepOnly.lastPublishedUpdateAt, null);

  const applied = buildHotelActivityHistory(HOTEL, {
    opportunities: [
      {
        id: "pub",
        prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
        canonicalPublishedAt: "2026-10-12T10:00:00.000Z",
        updatedAt: "2026-10-15T17:00:00.000Z",
      },
    ],
    opportunityDoc: {
      reviewedProspectImport: { importedAt: "2026-10-12T10:00:00.000Z" },
    },
    now: NOW,
  });
  assert.equal(applied.lastPublishedUpdateAt.slice(0, 10), "2026-10-12");
  console.log("ok publication prep vs apply");
}

function testDurableHistoryAfterRestart() {
  const { root } = mkRoot();
  appendHotelActivityEvent(HOTEL, {
    kind: ACTIVITY_KIND.WEEKLY_REFRESH,
    status: "completed",
    at: "2026-10-15T12:30:00.000Z",
    runId: "gdi_weekly_recHistT_2026-10-15T12-30-00-000Z_live02",
    source: "weekly-refresh-orchestrator",
    provenance: "live_write",
    dryRun: false,
  });
  // Also plant an ineligible future event that must stay out after "restart"
  appendHotelActivityEvent(HOTEL, {
    kind: ACTIVITY_KIND.WEEKLY_REFRESH,
    status: "completed",
    at: "2026-10-29T12:00:00.000Z",
    runId: "gdi_weekly_recHistT_2026-10-29T12-00-00-000Z_sim",
    source: "weekly-refresh/runs",
    provenance: "recovered_weekly_refresh_manifest",
    recovered: true,
    dryRun: true,
    clockLabel: "isolated_future_clock_2026-10-29_not_second_live_week",
    forceAuditWrite: true,
  });

  const before = buildHotelActivityHistory(HOTEL, {
    opportunities: [],
    opportunityDoc: {},
    now: NOW,
  });
  assert.equal(before.lastResearchAt, "2026-10-15T12:30:00.000Z");

  // Simulate process restart: new import against same durable root
  process.env.GDI_DATA_ROOT = root;
  reannotateHotelActivityLedgerEligibility(HOTEL);
  const after = buildHotelActivityHistory(HOTEL, {
    opportunities: [],
    opportunityDoc: {
      hotelActivitySummary: {
        lastResearchAt: "2026-10-29T12:00:00.000Z",
        runStatus: "completed",
        runStatusLabel: "Completed",
        latestRunId: "gdi_weekly_recHistT_2026-10-29T12-00-00-000Z_sim",
        customerHistoryEligible: false,
      },
    },
    now: NOW,
  });
  assert.equal(after.lastResearchAt, "2026-10-15T12:30:00.000Z");
  assert.notEqual(after.lastResearchAt?.slice(0, 10), "2026-10-29");
  assert.equal(after.runStatusLabel, "Completed");
  console.log("ok durable history after restart");
}

function testLiveWriteBlocksSimulated() {
  mkRoot();
  const blocked = appendHotelActivityEvent(HOTEL, {
    kind: ACTIVITY_KIND.WEEKLY_REFRESH,
    status: "completed",
    at: "2026-10-29T12:00:00.000Z",
    runId: "gdi_weekly_block_sim",
    provenance: "live_write",
    dryRun: true,
    clockLabel: "isolated_future_clock_2026-10-29",
  });
  assert.equal(blocked.skipped, true);
  assert.equal(loadHotelActivityLedger(HOTEL).events.length, 0);
  console.log("ok live write blocks simulated");
}

testFutureSimulatedExcluded();
testQueueOnlyNotCompletedResearch();
testRunIdWithoutCompletion();
testGenuineCompletedAndPartial();
testPublicationPrepVsApply();
testDurableHistoryAfterRestart();
testLiveWriteBlocksSimulated();
console.log("PASS test-gdi-hotel-activity-history-eligibility-v1");
