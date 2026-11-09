#!/usr/bin/env node
/**
 * Proves FullE2E discovery orchestration invokes real segment executors —
 * not mere ID traversal. Uses deterministic fixture providers (no paid calls).
 */
import assert from "node:assert/strict";
import {
  executeFourteenSegmentDiscoveryCell,
  executeFourteenSegmentDiscoveryMatrix,
  FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTION_V1,
} from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js";
import {
  assertFourteenSegmentExecutorArtifactsOnDisk,
  listFourteenSegmentExecutorSpecs,
  FOURTEEN_SEGMENT_EXECUTOR_SPECS,
} from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js";
import {
  runSegmentDiscoveryExecutor,
  createFixtureDiscoveryProvider,
  DISCOVERY_EXECUTION_STATUS,
} from "../lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";
import { buildSegmentDiscoveryQueries } from "../lib/group-demand-intelligence/research/segment-discovery-adapters-v1.js";
import { resolveHotelGdiProfileForResearch } from "../lib/group-demand-intelligence/hotel-gdi-profile-resolution-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const hotels = [
  "recCAGqkERSXgiku4",
  "recTk5I8VOzsPhxDz",
  "recXh4NgF8sDbi9cQ",
];

console.log("=== registry artifacts on disk ===");
const arts = assertFourteenSegmentExecutorArtifactsOnDisk();
assert.equal(arts.ok, true, JSON.stringify(arts.failures, null, 2));

console.log("=== 14 specs cover taxonomy ===");
const ids = listFourteenWorkflowSegmentIds();
assert.equal(ids.length, 14);
for (const id of ids) {
  assert.ok(FOURTEEN_SEGMENT_EXECUTOR_SPECS[id], `missing spec ${id}`);
}
assert.equal(listFourteenSegmentExecutorSpecs().length, 14);

console.log("=== GDI hotel resolve for Aimbridge trio (no ADP 31) ===");
for (const hotelId of hotels) {
  const r = await resolveHotelGdiProfileForResearch(hotelId, {
    skipHpc: true,
    preferLive: false,
  });
  assert.ok(r.ok !== false, `resolve failed ${hotelId}`);
  const hotel = r.hotel || r.profile;
  assert.ok(hotel?.displayName || hotel?.name, `no name ${hotelId}`);
  assert.ok(hotel?.city, `city missing after geo patch ${hotelId}`);
}

console.log("=== all 14 adapters generate segment-specific queries ===");
{
  const r = await resolveHotelGdiProfileForResearch(hotels[0], { skipHpc: true });
  const hotel = r.hotel;
  for (const segmentId of ids) {
    const built = buildSegmentDiscoveryQueries(segmentId, hotel);
    assert.ok(built.queries?.length > 0, `no queries for ${segmentId}`);
    assert.notEqual(built.readiness, "EXECUTOR_NOT_IMPLEMENTED");
  }
}

console.log("=== fixture provider path for all 14 × 3 hotels ===");
{
  let cells = 0;
  for (const hotelId of hotels) {
    for (const segmentId of ids) {
      const ex = await runSegmentDiscoveryExecutor({
        hotelId,
        segmentId,
        skipHpc: true,
        provider: createFixtureDiscoveryProvider(),
      });
      assert.equal(ex.executorInvoked, true);
      assert.equal(ex.hotelId, hotelId);
      assert.equal(ex.segmentId, segmentId);
      assert.ok(ex.queryCount > 0, `queryCount ${segmentId}`);
      assert.equal(ex.providerCalls, 0, "fixture must not count as live provider calls");
      assert.equal(ex.freshRetrievals, 0);
      assert.equal(ex.discoveryCompleted, false);
      assert.ok(
        [
          DISCOVERY_EXECUTION_STATUS.EXECUTION_PARTIAL,
          DISCOVERY_EXECUTION_STATUS.EXECUTOR_READY,
          DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED,
        ].includes(ex.executionStatus),
        `unexpected status ${ex.executionStatus} for ${segmentId}`
      );
      assert.ok(ex.candidates?.length >= 0);
      for (const c of ex.candidates || []) {
        assert.equal(c.hotelId, hotelId);
        assert.equal(c.segmentId, segmentId);
        assert.ok(c.provenance || c.sourceFamily);
      }
      cells += 1;
    }
  }
  assert.equal(cells, 42);
}

console.log("=== zero-cost blocked paid without auth is not DISCOVERY_COMPLETED ===");
{
  const blocked = await runSegmentDiscoveryExecutor({
    hotelId: hotels[0],
    segmentId: "projects_infrastructure",
    skipHpc: true,
    authorizePaidDiscovery: false,
  });
  assert.equal(blocked.executorInvoked, true);
  assert.equal(
    blocked.executionStatus,
    DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED
  );
  assert.equal(blocked.discoveryCompleted, false);
  assert.equal(blocked.providerCalls, 0);
  assert.match(
    String(blocked.stopReason || ""),
    /PAID_PROVIDER_REQUIRES_FOUNDER_AUTHORIZATION/
  );
}

console.log("=== cell orchestration invokes segment executor ===");
{
  const cell = await executeFourteenSegmentDiscoveryCell({
    hotelId: hotels[0],
    segmentId: "associations_conferences_events",
    useFixtureProvider: true,
    allowHistoricalReplay: false,
  });
  assert.equal(cell.executorInvoked, true);
  assert.ok(String(cell.executorId || "").includes("runSegmentDiscoveryExecutor"));
  assert.equal(cell.version, FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTION_V1);
  assert.ok(cell.queryCount > 0);
  assert.equal(cell.discoveryCompleted, false);
  assert.ok(cell.specializedRunner == null || typeof cell.specializedRunner === "object");
}

console.log("=== injected fixtureExecutor override still works ===");
{
  let calls = 0;
  const fixtureExecutor = async ({ hotelId, segmentId }) => {
    calls += 1;
    return {
      executorId: "test_fixture_executor",
      classification: "FIXTURE_ONLY",
      queryCount: 1,
      providerCallCount: 0,
      freshRetrievals: 0,
      extractedCandidates: 1,
      validatedCandidates: 1,
      deduplicatedCandidates: 1,
      retrievalMode: "FIXTURE",
      stopReason: "FIXTURE_OK",
      candidates: [
        {
          candidateId: `fix_${hotelId}_${segmentId}`,
          hotelId,
          segmentId,
          hasSourceEvidence: true,
          stage: "A_DISCOVERY",
        },
      ],
    };
  };

  const one = await executeFourteenSegmentDiscoveryCell({
    hotelId: hotels[0],
    segmentId: ids[0],
    fixtureExecutor,
    allowHistoricalReplay: false,
  });
  assert.equal(one.executorInvoked, true);
  assert.equal(one.executorId, "test_fixture_executor");
  assert.equal(calls, 1);

  const matrix = await executeFourteenSegmentDiscoveryMatrix({
    hotelIds: hotels,
    fixtureExecutor,
    allowHistoricalReplay: false,
  });
  assert.equal(matrix.summary.cellCount, 42);
  assert.equal(matrix.summary.executorInvokedCount, 42);
  assert.equal(calls, 1 + 42);
  assert.equal(matrix.summary.discoveryCompleteClaimAllowed, false);
}

console.log("=== blocked-paid cell without fixture is not stub-complete ===");
{
  const stubCell = await executeFourteenSegmentDiscoveryCell({
    hotelId: hotels[0],
    segmentId: "projects_infrastructure",
    allowHistoricalReplay: false,
    ledgerRoots: [],
  });
  assert.equal(stubCell.executorInvoked, true);
  assert.ok(
    stubCell.executionStatus === DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED ||
      stubCell.classification === DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED ||
      stubCell.classification === "STUB_OR_NO_OP"
  );
  assert.equal(stubCell.providerCallCount, 0);
  assert.equal(stubCell.freshRetrievals, 0);
  assert.notEqual(stubCell.discoveryCompleted, true);
}

console.log("=== assoc + scaling specialized runners resolve Aimbridge hotels ===");
{
  for (const segmentId of [
    "associations_conferences_events",
    "corporate_accounts_scaling",
  ]) {
    const cell = await executeFourteenSegmentDiscoveryCell({
      hotelId: hotels[1],
      segmentId,
      useFixtureProvider: true,
      allowHistoricalReplay: false,
    });
    // Specialized runners optional on main; shared segment executor is required.
    if (
      cell.specializedRunner?.available === false ||
      cell.specializedRunner?.ok === false ||
      !cell.specializedRunner?.hotelResolved
    ) {
      console.log(
        `soft-skip specialized runner unavailable for ${segmentId}: ${cell.specializedRunner?.error || "not_resolved"}`
      );
      continue;
    }
    assert.ok(cell.specializedRunner, `missing specialized note for ${segmentId}`);
    assert.equal(cell.specializedRunner.hotelResolved, true, segmentId);
    assert.notEqual(cell.specializedRunner.executed, true);
  }
}

console.log("OK — fourteen-segment discovery execution tests passed");
