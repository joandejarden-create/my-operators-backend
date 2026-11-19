/**
 * Fourteen-segment Full E2E coverage accounting V1.
 *
 * Records planned / discovered / selected / skipped / unexecuted segments.
 * Never claims FULL_COVERAGE for fixture/historical-only or deep-cap truncated runs.
 * Does not invent discovery results. Does not spend or publish.
 */

export const GDI_SEGMENT_COVERAGE_ACCOUNTING_V1 =
  "gdi-segment-coverage-accounting-v1";

export const COVERAGE_STATUS = Object.freeze({
  FULL_COVERAGE: "FULL_COVERAGE",
  PARTIAL_COVERAGE: "PARTIAL_COVERAGE",
  NO_COVERAGE: "NO_COVERAGE",
});

export const COVERAGE_PARTIAL_REASON = Object.freeze({
  CAP_TRUNCATED: "CAP_TRUNCATED",
  DISCOVERY_NOT_LIVE: "DISCOVERY_NOT_LIVE",
  BUDGET_BLOCKED: "BUDGET_BLOCKED",
  SEGMENT_UNEXECUTED: "SEGMENT_UNEXECUTED",
  FIXTURE_OR_HISTORICAL_ONLY: "FIXTURE_OR_HISTORICAL_ONLY",
  SYNTHETIC_CORPUS_EVIDENCE: "SYNTHETIC_CORPUS_EVIDENCE",
  ZERO_YIELD_EVALUATED: "ZERO_YIELD_EVALUATED",
  AUTHORIZED_PARTIAL_RUN: "AUTHORIZED_PARTIAL_RUN",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isLiveRetrievalMode(mode) {
  const m = text(mode).toUpperCase();
  return m === "FRESH_RETRIEVAL" || m.startsWith("RESUMED_FRESH");
}

function isFixtureOrHistoricalMode(mode) {
  const m = text(mode).toUpperCase();
  return (
    m.includes("FIXTURE") ||
    m.includes("HISTORICAL") ||
    m === "NO_RETRIEVAL" ||
    m === "" ||
    m.includes("BLOCKED")
  );
}

/**
 * Build per-segment and run-level coverage honesty report.
 */
export function buildSegmentCoverageAccounting({
  plannedSegmentIds = [],
  discoveryCells = [],
  retainedCandidates = [],
  selectionRegister = null,
  processed = [],
  maxCandidates = null,
  allowPartial = false,
  syntheticCorpusCount = 0,
} = {}) {
  const planned = [...new Set((plannedSegmentIds || []).map(text).filter(Boolean))];
  const cellsBySeg = new Map();
  for (const cell of discoveryCells || []) {
    const sid = text(cell.segmentId);
    if (!sid) continue;
    cellsBySeg.set(sid, cell);
  }

  const retainedBySeg = new Map();
  for (const c of retainedCandidates || []) {
    const sid = text(c.researchSegment || c.segmentId || c.demandType);
    if (!sid) continue;
    if (!retainedBySeg.has(sid)) retainedBySeg.set(sid, []);
    retainedBySeg.get(sid).push(c);
  }

  const processedBySeg = new Map();
  for (const p of processed || []) {
    const sid = text(p.segmentId || p.researchSegment);
    if (!sid) continue;
    if (!processedBySeg.has(sid)) processedBySeg.set(sid, []);
    processedBySeg.get(sid).push(p);
  }

  const registerRows = Array.isArray(selectionRegister?.register)
    ? selectionRegister.register
    : [];
  const selectedBySeg = new Map();
  const deferredBySeg = new Map();
  const budgetBlockedBySeg = new Map();
  for (const row of registerRows) {
    const sid = text(row.segmentId || row.researchSegment);
    if (!sid) continue;
    const decision = text(row.decision || row.selectionDecision).toUpperCase();
    if (decision.startsWith("SELECTED")) {
      selectedBySeg.set(sid, (selectedBySeg.get(sid) || 0) + 1);
    } else if (decision.includes("DEFERRED") || decision.includes("BUDGET")) {
      deferredBySeg.set(sid, (deferredBySeg.get(sid) || 0) + 1);
      if (decision.includes("BUDGET") || /BUDGET|FROZEN/i.test(text(row.reason))) {
        budgetBlockedBySeg.set(sid, true);
      }
    }
  }

  // Fallback when register lacks per-row decisions but has segmentSelected.
  const segmentSelected = selectionRegister?.segmentSelected || {};
  for (const [sid, n] of Object.entries(segmentSelected)) {
    if (!selectedBySeg.has(sid)) selectedBySeg.set(sid, Number(n) || 0);
  }

  const deferredCount = Number(selectionRegister?.deferredCount || 0);
  const selectedCount = Number(
    selectionRegister?.selectedCount != null
      ? selectionRegister.selectedCount
      : [...selectedBySeg.values()].reduce((a, b) => a + b, 0),
  );

  const partialReasons = new Set();
  if (allowPartial) partialReasons.add(COVERAGE_PARTIAL_REASON.AUTHORIZED_PARTIAL_RUN);
  if (Number(syntheticCorpusCount) > 0) {
    partialReasons.add(COVERAGE_PARTIAL_REASON.SYNTHETIC_CORPUS_EVIDENCE);
  }
  if (
    maxCandidates != null &&
    Number.isFinite(Number(maxCandidates)) &&
    Number(maxCandidates) > 0 &&
    deferredCount > 0
  ) {
    partialReasons.add(COVERAGE_PARTIAL_REASON.CAP_TRUNCATED);
  }

  const perSegment = {};
  let segmentsFullyCovered = 0;
  let segmentsWithLiveDiscovery = 0;
  let segmentsZeroYieldEvaluated = 0;
  let segmentsUnexecuted = 0;

  for (const sid of planned) {
    const cell = cellsBySeg.get(sid) || null;
    const retained = retainedBySeg.get(sid) || [];
    const selected = selectedBySeg.get(sid) || 0;
    const deferred = deferredBySeg.get(sid) || 0;
    const deepProcessed = (processedBySeg.get(sid) || []).length;
    const reasons = [];
    const retrievalMode = cell?.retrievalMode || null;
    const yieldCounts = cell?.discoveryYieldCounts || cell?.yieldCounts || null;
    const cellExecuted =
      cell != null &&
      (cell.executorInvoked !== false ||
        Number(cell.providerCallCount || 0) > 0 ||
        Number(cell.freshRetrievals || 0) > 0 ||
        Array.isArray(cell.candidates));

    if (!cellExecuted) {
      segmentsUnexecuted += 1;
      reasons.push(COVERAGE_PARTIAL_REASON.SEGMENT_UNEXECUTED);
      partialReasons.add(COVERAGE_PARTIAL_REASON.SEGMENT_UNEXECUTED);
    } else if (isLiveRetrievalMode(retrievalMode)) {
      segmentsWithLiveDiscovery += 1;
    } else if (isFixtureOrHistoricalMode(retrievalMode)) {
      reasons.push(COVERAGE_PARTIAL_REASON.FIXTURE_OR_HISTORICAL_ONLY);
      partialReasons.add(COVERAGE_PARTIAL_REASON.FIXTURE_OR_HISTORICAL_ONLY);
      partialReasons.add(COVERAGE_PARTIAL_REASON.DISCOVERY_NOT_LIVE);
    } else {
      reasons.push(COVERAGE_PARTIAL_REASON.DISCOVERY_NOT_LIVE);
      partialReasons.add(COVERAGE_PARTIAL_REASON.DISCOVERY_NOT_LIVE);
    }

    if (cellExecuted && retained.length === 0 && selected === 0) {
      segmentsZeroYieldEvaluated += 1;
      reasons.push(COVERAGE_PARTIAL_REASON.ZERO_YIELD_EVALUATED);
    }
    if (deferred > 0) {
      reasons.push(COVERAGE_PARTIAL_REASON.CAP_TRUNCATED);
      partialReasons.add(COVERAGE_PARTIAL_REASON.CAP_TRUNCATED);
    }
    if (budgetBlockedBySeg.get(sid)) {
      reasons.push(COVERAGE_PARTIAL_REASON.BUDGET_BLOCKED);
      partialReasons.add(COVERAGE_PARTIAL_REASON.BUDGET_BLOCKED);
    }

    const segmentFull =
      cellExecuted &&
      isLiveRetrievalMode(retrievalMode) &&
      deferred === 0 &&
      !budgetBlockedBySeg.get(sid) &&
      (selected === 0 || deepProcessed >= selected);

    if (segmentFull) segmentsFullyCovered += 1;

    perSegment[sid] = {
      discoveryCellStatus: cellExecuted
        ? isLiveRetrievalMode(retrievalMode)
          ? "LIVE_EXECUTED"
          : "EXECUTED_NON_LIVE"
        : "UNEXECUTED",
      retrievalMode,
      yieldCounts,
      retained: retained.length,
      selected,
      deferred,
      deepProcessed,
      blocked: budgetBlockedBySeg.get(sid) === true,
      skipped: deferred,
      coverageStatus: segmentFull
        ? COVERAGE_STATUS.FULL_COVERAGE
        : cellExecuted
          ? COVERAGE_STATUS.PARTIAL_COVERAGE
          : COVERAGE_STATUS.NO_COVERAGE,
      reasons,
      zeroRealDiscoveryEvaluated:
        cellExecuted && retained.length === 0 && selected === 0,
    };
  }

  const allPlannedLiveAndUntruncated =
    planned.length > 0 &&
    segmentsFullyCovered === planned.length &&
    deferredCount === 0 &&
    Number(syntheticCorpusCount) === 0;

  const coverageStatus = allPlannedLiveAndUntruncated
    ? COVERAGE_STATUS.FULL_COVERAGE
    : planned.length === 0 || segmentsUnexecuted === planned.length
      ? COVERAGE_STATUS.NO_COVERAGE
      : COVERAGE_STATUS.PARTIAL_COVERAGE;

  const fullCoverageClaimAllowed = coverageStatus === COVERAGE_STATUS.FULL_COVERAGE;

  return {
    version: GDI_SEGMENT_COVERAGE_ACCOUNTING_V1,
    coverageStatus,
    allowPartial: allowPartial === true,
    maxCandidates:
      maxCandidates == null || !Number.isFinite(Number(maxCandidates))
        ? null
        : Number(maxCandidates),
    perSegment,
    totals: {
      plannedSegments: planned.length,
      segmentsFullyCovered,
      segmentsWithLiveDiscovery,
      segmentsZeroYieldEvaluated,
      segmentsUnexecuted,
      retained: (retainedCandidates || []).length,
      selected: selectedCount,
      deferred: deferredCount,
      processed: (processed || []).length,
      syntheticCorpusCount: Number(syntheticCorpusCount) || 0,
    },
    partialReasons: [...partialReasons],
    fullCoverageClaimAllowed,
    // ok for run continuation when partial is authorized; never upgrades claim.
    runOkWhenPartialAuthorized:
      fullCoverageClaimAllowed || allowPartial === true,
    note:
      "FULL_COVERAGE requires live discovery evaluation for every planned segment with no deep-cap truncation and no synthetic corpus evidence.",
  };
}

export default {
  GDI_SEGMENT_COVERAGE_ACCOUNTING_V1,
  COVERAGE_STATUS,
  COVERAGE_PARTIAL_REASON,
  buildSegmentCoverageAccounting,
};
