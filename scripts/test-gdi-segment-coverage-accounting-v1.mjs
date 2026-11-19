#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildSegmentCoverageAccounting,
  COVERAGE_STATUS,
  COVERAGE_PARTIAL_REASON,
} from "../lib/group-demand-intelligence/workflow/gdi-segment-coverage-accounting-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const planned = listFourteenWorkflowSegmentIds();
assert.equal(planned.length, 14);

const fixtureCells = planned.map((segmentId) => ({
  segmentId,
  retrievalMode: "FIXTURE",
  executorInvoked: true,
  candidates: [],
  discoveryYieldCounts: { FIXTURE: 1 },
}));

const fixtureCoverage = buildSegmentCoverageAccounting({
  plannedSegmentIds: planned,
  discoveryCells: fixtureCells,
  retainedCandidates: [],
  selectionRegister: { selectedCount: 0, deferredCount: 0, register: [] },
  processed: [],
  maxCandidates: 8,
  allowPartial: true,
});
assert.equal(fixtureCoverage.coverageStatus, COVERAGE_STATUS.PARTIAL_COVERAGE);
assert.equal(fixtureCoverage.fullCoverageClaimAllowed, false);
assert.ok(
  fixtureCoverage.partialReasons.includes(
    COVERAGE_PARTIAL_REASON.FIXTURE_OR_HISTORICAL_ONLY,
  ),
);
assert.equal(fixtureCoverage.totals.plannedSegments, 14);
assert.equal(fixtureCoverage.runOkWhenPartialAuthorized, true);

const capped = buildSegmentCoverageAccounting({
  plannedSegmentIds: planned,
  discoveryCells: planned.map((segmentId) => ({
    segmentId,
    retrievalMode: "FRESH_RETRIEVAL",
    executorInvoked: true,
    freshRetrievals: 1,
    candidates: [{ candidateId: `c_${segmentId}` }],
  })),
  retainedCandidates: planned.map((segmentId) => ({
    id: `c_${segmentId}`,
    researchSegment: segmentId,
  })),
  selectionRegister: {
    selectedCount: 8,
    deferredCount: 6,
    segmentSelected: Object.fromEntries(planned.slice(0, 8).map((s) => [s, 1])),
    register: [
      ...planned.slice(0, 8).map((segmentId) => ({
        segmentId,
        decision: "SELECTED_FOR_DEEP",
      })),
      ...planned.slice(8).map((segmentId) => ({
        segmentId,
        decision: "DEFERRED_LOWER_RANK",
      })),
    ],
  },
  processed: planned.slice(0, 8).map((segmentId) => ({
    opportunityId: `c_${segmentId}`,
    segmentId,
  })),
  maxCandidates: 8,
  allowPartial: false,
});
assert.equal(capped.coverageStatus, COVERAGE_STATUS.PARTIAL_COVERAGE);
assert.equal(capped.fullCoverageClaimAllowed, false);
assert.ok(capped.partialReasons.includes(COVERAGE_PARTIAL_REASON.CAP_TRUNCATED));
assert.equal(capped.runOkWhenPartialAuthorized, false);

const syntheticBlocked = buildSegmentCoverageAccounting({
  plannedSegmentIds: planned,
  discoveryCells: planned.map((segmentId) => ({
    segmentId,
    retrievalMode: "FRESH_RETRIEVAL",
    executorInvoked: true,
    freshRetrievals: 1,
    candidates: [],
  })),
  retainedCandidates: [],
  selectionRegister: { selectedCount: 0, deferredCount: 0, register: [] },
  processed: [],
  maxCandidates: 14,
  allowPartial: false,
  syntheticCorpusCount: 2,
});
assert.equal(syntheticBlocked.fullCoverageClaimAllowed, false);
assert.ok(
  syntheticBlocked.partialReasons.includes(
    COVERAGE_PARTIAL_REASON.SYNTHETIC_CORPUS_EVIDENCE,
  ),
);

const liveFull = buildSegmentCoverageAccounting({
  plannedSegmentIds: planned,
  discoveryCells: planned.map((segmentId) => ({
    segmentId,
    retrievalMode: "FRESH_RETRIEVAL",
    executorInvoked: true,
    freshRetrievals: 1,
    candidates: [{ candidateId: `live_${segmentId}` }],
  })),
  retainedCandidates: planned.map((segmentId) => ({
    id: `live_${segmentId}`,
    researchSegment: segmentId,
  })),
  selectionRegister: {
    selectedCount: 14,
    deferredCount: 0,
    segmentSelected: Object.fromEntries(planned.map((s) => [s, 1])),
    register: planned.map((segmentId) => ({
      segmentId,
      decision: "SELECTED_FOR_DEEP",
    })),
  },
  processed: planned.map((segmentId) => ({
    opportunityId: `live_${segmentId}`,
    segmentId,
  })),
  maxCandidates: 14,
  allowPartial: false,
  syntheticCorpusCount: 0,
});
assert.equal(liveFull.coverageStatus, COVERAGE_STATUS.FULL_COVERAGE);
assert.equal(liveFull.fullCoverageClaimAllowed, true);

console.log("test-gdi-segment-coverage-accounting-v1: PASS");
