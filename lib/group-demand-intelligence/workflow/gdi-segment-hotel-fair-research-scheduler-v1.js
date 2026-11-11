/**
 * Segment-fair + hotel-fair deep-research queue scheduler V1.
 *
 * Prevents early taxonomy segments from consuming the entire authorized budget
 * before later segments receive any deep research.
 *
 * Strategy:
 *  1) Bucket incomplete opportunities by hotelId × segmentId.
 *  2) Round-robin across non-empty cells (hotel×segment).
 *  3) Within each cell, prefer higher commercial-priority scores.
 *  4) Multi-pass until queues empty — every non-empty cell gets coverage
 *     before any cell receives its second item in a pass cycle.
 *
 * Does not invent opportunities. Does not spend. Does not publish.
 */

import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";

export const GDI_SEGMENT_HOTEL_FAIR_RESEARCH_SCHEDULER_V1 =
  "gdi-segment-hotel-fair-research-scheduler-v1";

export const LATE_SEGMENTS = Object.freeze([
  "local_catering_day_meetings",
  "corporate_celebrations_private_events",
  "travel_trade_group_intermediaries",
  "relocation_extended_stay",
  "preferred_lodging_partnerships",
  "professional_event_planners",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function scoreOpportunityForFairQueue(opp = {}) {
  let score = 0;
  if (opp.primaryContact?.name || opp.contactName) score += 20;
  if ((opp.namedStakeholders || []).length) score += 10;
  if ((opp.seedUrls || []).length || (opp.evidence || []).length) score += 15;
  if (text(opp.hotelOpportunityThesis) || text(opp.whyThisHotel)) score += 20;
  if (text(opp.potentialHotelBusiness) || text(opp.proposedHotelUse)) score += 10;
  const seg = String(opp.researchSegment || "");
  if (LATE_SEGMENTS.includes(seg)) score += 8; // mild anti-starvation bias, not reverse-order
  const d = String(opp.internalDisposition || "").toUpperCase();
  if (d.includes("WATCH")) score += 5;
  if (d.includes("EXCLUDE") || d.includes("REJECT")) score -= 30;
  return score;
}

/**
 * Build a fair deep-research queue.
 * @param {object[]} opportunities
 * @param {object} opts
 * @param {Set<string>|string[]} [opts.completedOpportunityIds] — skip already settled
 * @param {string[]} [opts.hotelIds]
 * @param {string[]} [opts.segmentIds]
 * @param {Function} [opts.scoreFn]
 */
export function buildSegmentHotelFairDeepResearchQueue(opportunities = [], opts = {}) {
  const segmentIds = opts.segmentIds || listFourteenWorkflowSegmentIds();
  const completed = new Set(
    [...(opts.completedOpportunityIds || [])].map((id) => String(id))
  );
  const scoreFn = opts.scoreFn || scoreOpportunityForFairQueue;

  const pending = [];
  const seen = new Set();
  for (const opp of opportunities) {
    const id = String(opp.id || opp.opportunityId || "");
    if (!id || seen.has(id)) continue;
    seen.add(id);
    if (completed.has(id)) continue;
    pending.push(opp);
  }

  const hotelOrder = opts.hotelIds || [
    ...new Set(pending.map((o) => String(o.hotelId || "_unknown"))),
  ];

  /** @type {Map<string, object[]>} */
  const buckets = new Map();
  for (const hotelId of hotelOrder) {
    for (const segmentId of segmentIds) {
      buckets.set(`${hotelId}|${segmentId}`, []);
    }
  }
  // Unknown segments still scheduled (appended keys)
  for (const opp of pending) {
    const hotelId = String(opp.hotelId || "_unknown");
    const segmentId = String(opp.researchSegment || "_unknown");
    const key = `${hotelId}|${segmentId}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(opp);
  }

  for (const [, list] of buckets) {
    list.sort((a, b) => scoreFn(b) - scoreFn(a));
  }

  const cellKeys = [];
  // Hotel-fair outer, segment-fair inner: for each segment, walk hotels;
  // then next segment — then multi-pass. This guarantees late segments
  // receive a turn in pass 1 before early segments get pass 2 items.
  for (const segmentId of [...segmentIds, "_unknown"]) {
    for (const hotelId of [...hotelOrder, "_unknown"]) {
      const key = `${hotelId}|${segmentId}`;
      if (buckets.has(key) && buckets.get(key).length) cellKeys.push(key);
    }
  }
  // De-dupe while preserving order
  const orderedCells = [...new Set(cellKeys)];

  const queue = [];
  const cellStats = orderedCells.map((key) => {
    const [hotelId, segmentId] = key.split("|");
    return {
      hotelId,
      segmentId,
      pendingAtSchedule: buckets.get(key)?.length || 0,
    };
  });

  let progressed = true;
  let pass = 0;
  while (progressed) {
    progressed = false;
    pass += 1;
    for (const key of orderedCells) {
      const list = buckets.get(key);
      if (!list?.length) continue;
      const next = list.shift();
      queue.push({
        ...next,
        _fairSchedule: {
          version: GDI_SEGMENT_HOTEL_FAIR_RESEARCH_SCHEDULER_V1,
          pass,
          cellKey: key,
          score: scoreFn(next),
        },
      });
      progressed = true;
    }
  }

  const pendingByHotelSegment = cellStats.map((c) => ({
    ...c,
    remainingAfterScheduleBuild: 0, // all moved into queue
  }));

  return {
    version: GDI_SEGMENT_HOTEL_FAIR_RESEARCH_SCHEDULER_V1,
    queue,
    totalQueued: queue.length,
    skippedCompleted: completed.size,
    passes: pass,
    cellStats,
    pendingByHotelSegment,
    lateSegments: [...LATE_SEGMENTS],
    firstOccurrenceBySegment: Object.fromEntries(
      segmentIds.map((seg) => {
        const idx = queue.findIndex((o) => String(o.researchSegment) === seg);
        return [seg, idx];
      })
    ),
  };
}

export function summarizeFairQueueCoverage(schedule) {
  const bySeg = {};
  for (const opp of schedule.queue || []) {
    const seg = String(opp.researchSegment || "_unknown");
    bySeg[seg] = (bySeg[seg] || 0) + 1;
  }
  return {
    version: GDI_SEGMENT_HOTEL_FAIR_RESEARCH_SCHEDULER_V1,
    totalQueued: schedule.totalQueued,
    bySegment: bySeg,
    firstOccurrenceBySegment: schedule.firstOccurrenceBySegment,
    lateSegmentsCoveredInFirstPass: LATE_SEGMENTS.filter((seg) => {
      const idx = schedule.firstOccurrenceBySegment?.[seg];
      return idx != null && idx >= 0 && idx < (schedule.totalQueued || 0);
    }),
  };
}

export default {
  GDI_SEGMENT_HOTEL_FAIR_RESEARCH_SCHEDULER_V1,
  LATE_SEGMENTS,
  scoreOpportunityForFairQueue,
  buildSegmentHotelFairDeepResearchQueue,
  summarizeFairQueueCoverage,
};
