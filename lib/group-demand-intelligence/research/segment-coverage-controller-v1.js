/**
 * Hotel-level demand-segment coverage + advisory hotel completion pre-gate.
 *
 * Full E2E `evaluateFullE2eCompletion` remains the SoT for COMPLETE.
 * Depth uses meaningful sufficiency — not hard-coded task count >= 2.
 */

import {
  SEGMENT_RUN_STATUS,
  SEGMENT_TERMINAL_OK,
  RESEARCH_TERMINAL,
  DEFAULT_MINIMUM_DEPTH,
  canonicalizeSegmentId,
  SUPERVISOR_EXPECTED_SEGMENTS,
} from "./research-constants-v1.js";
import { classifySegmentRunStatus } from "./segment-research-sufficiency-v1.js";
import { isActionableContactRoute } from "./opportunity-research-supervisor-v1.js";

export { SEGMENT_RUN_STATUS, SEGMENT_TERMINAL_OK };

export function segmentCoverageGate({
  expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS,
  segmentRuns = [],
  minimumDepth = {},
} = {}) {
  const bySegment = new Map(
    segmentRuns.map((r) => [canonicalizeSegmentId(r.segment || r.segmentId), r])
  );

  const rows = expectedSegments.map((raw) => {
    const segment = canonicalizeSegmentId(raw);
    const run = bySegment.get(segment);
    if (!run) {
      return {
        segment,
        status: SEGMENT_RUN_STATUS.PENDING,
        reason: "NO_RUN",
      };
    }

    // Optional soft floors can still be passed for diagnostics; sufficiency owns the gate
    const floors = {
      ...DEFAULT_MINIMUM_DEPTH,
      ...(minimumDepth[segment] || minimumDepth[raw] || {}),
    };
    const classified = classifySegmentRunStatus(run);
    return {
      segment,
      status: classified.status,
      reason: classified.reason,
      metrics: { ...(classified.metrics || {}), floors },
    };
  });

  const pass = rows.every((r) => SEGMENT_TERMINAL_OK.includes(r.status));
  const missingForcesPartial = rows.some(
    (r) =>
      r.status === SEGMENT_RUN_STATUS.PENDING ||
      r.status === SEGMENT_RUN_STATUS.INSUFFICIENT_RESEARCH_DEPTH ||
      r.status === SEGMENT_RUN_STATUS.IN_PROGRESS ||
      r.status === SEGMENT_RUN_STATUS.FAILED ||
      r.status === SEGMENT_RUN_STATUS.BLOCKED
  );

  return { pass, missingForcesPartial, rows };
}

/**
 * Advisory hotel pre-gate. Does not replace evaluateFullE2eCompletion.
 */
export function hotelCompletionGate({
  coverage,
  opportunities = [],
  minHotelFacingProspects = 10,
  requireProductionDelivery = true,
  delivery = {},
} = {}) {
  const hotelFacing = opportunities.filter((x) =>
    [
      RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT,
      RESEARCH_TERMINAL.EXPLORATORY_PROSPECT,
    ].includes(x.researchStatus)
  );
  const publishable = opportunities.filter(
    (x) => x.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT
  );
  const contactUsable = hotelFacing.filter((x) =>
    isActionableContactRoute(x.contact || {})
  );

  const deliveryChecks = requireProductionDelivery
    ? {
        canonicalPersistence: Boolean(delivery.canonicalPersistence),
        drawersRender: Boolean(delivery.drawersRender),
        pdfVerified: Boolean(delivery.pdfVerified),
        externalLinkVerified: Boolean(delivery.externalLinkVerified),
        adminRowVerified: Boolean(delivery.adminRowVerified),
        productionVerified: Boolean(delivery.productionVerified),
      }
    : {};

  const deliveryPass =
    !requireProductionDelivery || Object.values(deliveryChecks).every(Boolean);

  const reasons = [];
  if (!coverage?.pass) reasons.push("SEGMENT_COVERAGE_INCOMPLETE");
  if (coverage?.missingForcesPartial) reasons.push("MISSING_OR_SHALLOW_SEGMENT");
  if (hotelFacing.length < minHotelFacingProspects) {
    reasons.push("INSUFFICIENT_HOTEL_FACING_PROSPECTS");
  }
  if (contactUsable.length < hotelFacing.length) {
    reasons.push("UNUSABLE_CONTACT_ROUTE_PRESENT");
  }
  if (requireProductionDelivery && !deliveryPass) {
    reasons.push("CUSTOMER_DELIVERY_INCOMPLETE");
  }

  return {
    status: reasons.length ? "PARTIAL" : "COMPLETE",
    reasons,
    metrics: {
      hotelFacingProspects: hotelFacing.length,
      publishableProspects: publishable.length,
      exploratoryProspects: hotelFacing.length - publishable.length,
      contactUsable: contactUsable.length,
      segmentsOk: (coverage?.rows || []).filter((r) =>
        SEGMENT_TERMINAL_OK.includes(r.status)
      ).length,
      segmentsExpected: (coverage?.rows || []).length,
      minHotelFacingProspects,
    },
    deliveryChecks,
    note: "Advisory pre-gate — Full E2E evaluator remains SoT for COMPLETE",
  };
}
