/**
 * Full E2E fourteen-segment traversal contract V1.
 *
 * Ensures PowerShell / Node FullE2E orchestration evaluates every canonical
 * PowerShell FullE2E workflow segment (listFourteenWorkflowSegmentIds) for each
 * selected hotel — without silent stop after a lead-count target, and without
 * reporting COMPLETE while applicable segments remain NOT_RUN.
 *
 * Does not invent opportunities. Does not call paid providers.
 */

import {
  listFourteenWorkflowSegmentIds,
} from "../research/universal-contact-research-contract-v1.js";

export const FULL_E2E_FOURTEEN_SEGMENT_TRAVERSAL_V1 =
  "gdi-full-e2e-fourteen-segment-traversal-v1";

export const TRAVERSAL_CELL_STATUS = Object.freeze({
  NOT_RUN: "NOT_RUN",
  PARTIAL: "PARTIAL",
  COMPLETED: "COMPLETED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  BLOCKED: "BLOCKED",
});

export function getCanonicalFourteenSegmentIds() {
  return Object.freeze([...listFourteenWorkflowSegmentIds()]);
}

/**
 * Deterministic traversal plan: hotel × 14 segments.
 * Preserves prior cell state when provided; defaults to NOT_RUN.
 */
export function buildFourteenSegmentTraversalPlan({
  hotelIds = [],
  priorCells = [],
} = {}) {
  const segments = getCanonicalFourteenSegmentIds();
  if (segments.length !== 14) {
    throw new Error(
      `fourteen_segment_taxonomy_invalid_count_${segments.length}`
    );
  }
  const prior = new Map(
    (priorCells || []).map((c) => [`${c.hotelId}::${c.segmentId}`, c])
  );
  const cells = [];
  for (const hotelId of hotelIds) {
    for (const segmentId of segments) {
      const key = `${hotelId}::${segmentId}`;
      const prev = prior.get(key);
      cells.push({
        hotelId,
        segmentId,
        status: prev?.status || TRAVERSAL_CELL_STATUS.NOT_RUN,
        skipped: Boolean(prev?.skipped),
        blocked: prev?.status === TRAVERSAL_CELL_STATUS.BLOCKED,
        opportunityIds: Array.isArray(prev?.opportunityIds)
          ? [...prev.opportunityIds]
          : [],
        reason: prev?.reason || null,
      });
    }
  }
  return {
    version: FULL_E2E_FOURTEEN_SEGMENT_TRAVERSAL_V1,
    segmentIds: segments,
    hotelIds: [...hotelIds],
    cellCount: cells.length,
    cells,
  };
}

/**
 * Deduplicate opportunities across segments (same demand counted once).
 * originatingSegment retained; additionalMatchingSegments recorded.
 */
export function dedupeCrossSegmentOpportunities(rows = []) {
  const byId = new Map();
  for (const row of rows || []) {
    const id = row.candidateId || row.id || row.opportunityId;
    if (!id) continue;
    if (!byId.has(id)) {
      byId.set(id, {
        ...row,
        id,
        originatingSegment:
          row.originatingSegment ||
          row.strategy ||
          row.primarySegment ||
          null,
        additionalMatchingSegments: [
          ...new Set(
            [
              ...(Array.isArray(row.additionalMatchingSegments)
                ? row.additionalMatchingSegments
                : []),
            ].filter(Boolean)
          ),
        ],
      });
      continue;
    }
    const existing = byId.get(id);
    const extra = row.originatingSegment || row.strategy || null;
    if (
      extra &&
      extra !== existing.originatingSegment &&
      !existing.additionalMatchingSegments.includes(extra)
    ) {
      existing.additionalMatchingSegments.push(extra);
    }
  }
  return [...byId.values()];
}

/**
 * Hotel accounting: cannot claim COMPLETE while any applicable segment is NOT_RUN.
 */
export function evaluateFourteenSegmentHotelCompletion(cells = [], hotelId) {
  const hotelCells = (cells || []).filter((c) => c.hotelId === hotelId);
  const notRun = hotelCells.filter(
    (c) => c.status === TRAVERSAL_CELL_STATUS.NOT_RUN
  );
  const blocked = hotelCells.filter(
    (c) => c.status === TRAVERSAL_CELL_STATUS.BLOCKED
  );
  const partial = hotelCells.filter(
    (c) => c.status === TRAVERSAL_CELL_STATUS.PARTIAL
  );
  const completed = hotelCells.filter(
    (c) =>
      c.status === TRAVERSAL_CELL_STATUS.COMPLETED ||
      c.status === TRAVERSAL_CELL_STATUS.NOT_APPLICABLE
  );
  const okCompleteClaim =
    notRun.length === 0 &&
    hotelCells.length === 14 &&
    blocked.every((c) => Boolean(c.reason));
  return {
    hotelId,
    segmentCount: hotelCells.length,
    expected: 14,
    notRun: notRun.map((c) => c.segmentId),
    blocked: blocked.map((c) => ({ segmentId: c.segmentId, reason: c.reason })),
    partial: partial.map((c) => c.segmentId),
    completedOrNa: completed.map((c) => c.segmentId),
    canClaimFullE2eDiscoveryComplete: okCompleteClaim && partial.length === 0,
    incompleteBecauseNotRun: notRun.length > 0,
  };
}

/**
 * Resume helper: next cells to execute (NOT_RUN then PARTIAL).
 * Does not stop after N leads.
 */
export function nextSegmentsToExecute(cells = [], { hotelId = null } = {}) {
  let list = cells || [];
  if (hotelId) list = list.filter((c) => c.hotelId === hotelId);
  const notRun = list.filter((c) => c.status === TRAVERSAL_CELL_STATUS.NOT_RUN);
  const partial = list.filter((c) => c.status === TRAVERSAL_CELL_STATUS.PARTIAL);
  return [...notRun, ...partial];
}

export function assertNoSilentLeadCap(options = {}) {
  const cap = options.discoveryLeadCap;
  if (cap != null && Number(cap) > 0) {
    return {
      ok: false,
      code: "DISCOVERY_LEAD_CAP_FORBIDDEN",
      message:
        "FullE2E fourteen-segment discovery must not silently stop after a target number of leads.",
      cap: Number(cap),
    };
  }
  return { ok: true };
}
