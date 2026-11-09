#!/usr/bin/env node
/**
 * Durable discovery-state persistence + resume/idempotency tests (fixtures only).
 * No paid Exa. No Airtable. No publication.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createEmptyDiscoveryState,
  saveDiscoveryStateAtomic,
  loadDiscoveryState,
  persistDiscoveryTrace,
  mergeDiscoveryTraceIntoState,
  shouldSkipLiveProviderInvocation,
  buildDiscoveryResumePlan,
} from "../lib/group-demand-intelligence/research/full-e2e-fourteen-segment-discovery-state-v1.js";
import { DISCOVERY_EXECUTION_STATUS } from "../lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";
import {
  executeFourteenSegmentDiscoveryCell,
} from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js";
import { inspectExaLiveDiscoveryReadiness } from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import { resolveHotelGdiProfileForResearch } from "../lib/group-demand-intelligence/hotel-gdi-profile-resolution-v1.js";

const tmpRoot = fs.mkdtempSync(
  path.join(os.tmpdir(), "gdi-discovery-state-")
);
const hotelId = "recCAGqkERSXgiku4";
const segmentId = "associations_conferences_events";

console.log("=== atomic persist + reload ===");
{
  const empty = createEmptyDiscoveryState({ hotelId, segmentId });
  empty.candidateIds = ["cand_stable_a"];
  const saved = saveDiscoveryStateAtomic(empty, { root: tmpRoot });
  assert.ok(fs.existsSync(saved.path));
  const loaded = loadDiscoveryState(hotelId, segmentId, { root: tmpRoot });
  assert.equal(loaded.hotelId, hotelId);
  assert.equal(loaded.segmentId, segmentId);
  assert.deepEqual(loaded.candidateIds, ["cand_stable_a"]);
  assert.equal(loaded.publicationOccurred, false);
  assert.equal(loaded.airtableWrites, 0);
}

console.log("=== idempotent cost accounting ===");
{
  const trace = {
    hotelId,
    segmentId: "corporate_accounts_scaling",
    researchRunId: "run_test_1",
    executionStatus: DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED,
    retrievalMode: "FRESH_RETRIEVAL",
    providerCallCount: 2,
    freshRetrievals: 2,
    researchCostUsd: 0.2,
    stopReason: "LIVE_PROVIDER_EXECUTED",
    querySpecs: [{ id: "q1", query: "scaling companies Cancun" }],
    candidates: [
      {
        candidateId: "cand_live_1",
        hasSourceEvidence: true,
        evidenceUrls: ["https://example.com/a"],
        provenance: { queryId: "q1" },
      },
    ],
    rejections: [],
  };
  const first = persistDiscoveryTrace(trace, { root: tmpRoot });
  assert.equal(first.state.providerUsage.costUsd, 0.2);
  assert.equal(first.state.providerUsage.liveCalls, 2);
  const second = persistDiscoveryTrace(trace, { root: tmpRoot });
  assert.equal(second.state.providerUsage.costUsd, 0.2, "cost must not double");
  assert.equal(second.state.providerUsage.liveCalls, 2, "live calls must not double");
  assert.deepEqual(second.state.candidateIds, ["cand_live_1"]);
  assert.equal(
    second.state.lastSuccessfulCheckpoint.kind,
    "LIVE_PROVIDER_COMPLETE"
  );
}

console.log("=== resume skips completed live provider ===");
{
  const state = loadDiscoveryState(hotelId, "corporate_accounts_scaling", {
    root: tmpRoot,
  });
  const skip = shouldSkipLiveProviderInvocation(state);
  assert.equal(skip.skip, true);
  assert.match(skip.reason, /LIVE_PROVIDER|CHECKPOINT/);
  const forced = shouldSkipLiveProviderInvocation(state, { forceRerun: true });
  assert.equal(forced.skip, false);
}

console.log("=== cell persist under temp root + stable IDs on rerun ===");
{
  const cell1 = await executeFourteenSegmentDiscoveryCell({
    hotelId,
    segmentId,
    useFixtureProvider: true,
    allowHistoricalReplay: false,
    persistState: true,
    stateRoot: tmpRoot,
    researchRunId: "run_fixture_persist",
  });
  assert.ok(cell1.durableStatePath);
  assert.ok(cell1.persistedResearchCandidates >= 1);
  const ids1 = (cell1.candidates || []).map((c) => c.candidateId).sort();

  const cell2 = await executeFourteenSegmentDiscoveryCell({
    hotelId,
    segmentId,
    useFixtureProvider: true,
    allowHistoricalReplay: false,
    persistState: true,
    stateRoot: tmpRoot,
    researchRunId: "run_fixture_persist",
  });
  const ids2 = (cell2.candidates || []).map((c) => c.candidateId).sort();
  assert.deepEqual(ids2, ids1, "stable candidate IDs across reruns");
  const st = loadDiscoveryState(hotelId, segmentId, { root: tmpRoot });
  assert.equal(st.providerUsage.costUsd, 0);
  assert.ok(st.lastSuccessfulCheckpoint);
  assert.equal(st.publicationOccurred, false);
}

console.log("=== resume plan for pilot hotels ===");
{
  const plan = buildDiscoveryResumePlan({
    hotelIds: ["recCAGqkERSXgiku4", "recTk5I8VOzsPhxDz"],
    segmentIds: [
      "associations_conferences_events",
      "corporate_accounts_scaling",
    ],
    root: tmpRoot,
  });
  assert.equal(plan.cellCount, 4);
  assert.ok(plan.needsLiveCount + plan.skipLiveCount === 4);
}

console.log("=== Exa readiness inspection (no paid calls) ===");
{
  const resolved = await resolveHotelGdiProfileForResearch(hotelId, {
    skipHpc: true,
  });
  const hotel = resolved.hotel || resolved.profile;
  const ready = inspectExaLiveDiscoveryReadiness({
    hotel,
    segmentId: "associations_conferences_events",
    effort: "medium",
    authorizeExaAgentLive: false,
  });
  assert.equal(ready.liveAuthorized, false);
  assert.equal(ready.authenticatedDoesNotProveLiveRetrieval, true);
  assert.ok(ready.payload.queryChars > 100);
  assert.equal(ready.payload.hotelContextPresent, true);
  assert.equal(ready.quote.bounded, true);
  assert.ok(ready.quote.maxUsd <= 0.2);
}

console.log("=== merge preserves historical evidence refs ===");
{
  const existing = createEmptyDiscoveryState({
    hotelId: "recTk5I8VOzsPhxDz",
    segmentId: "associations_conferences_events",
  });
  existing.cacheReferences = ["historical://ledger"];
  existing.candidateIds = ["hist_1"];
  const merged = mergeDiscoveryTraceIntoState(existing, {
    hotelId: "recTk5I8VOzsPhxDz",
    segmentId: "associations_conferences_events",
    retrievalMode: "HISTORICAL_REPLAY",
    providerCallCount: 0,
    freshRetrievals: 0,
    researchCostUsd: 0,
    candidates: [
      {
        candidateId: "hist_1",
        hasSourceEvidence: true,
        evidenceUrls: ["https://example.com/hist"],
      },
      { candidateId: "hist_2", hasSourceEvidence: false },
    ],
    executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTION_PARTIAL,
  });
  assert.ok(merged.candidateIds.includes("hist_1"));
  assert.ok(merged.candidateIds.includes("hist_2"));
  assert.ok(merged.cacheReferences.includes("historical://ledger"));
  assert.equal(merged.providerUsage.costUsd, 0);
}

// cleanup
fs.rmSync(tmpRoot, { recursive: true, force: true });
console.log("OK — discovery state resume/idempotency tests passed");
