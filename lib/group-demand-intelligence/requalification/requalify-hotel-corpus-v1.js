/**
 * Requalify existing GDI hotel corpus against HI-enriched hotel profiles.
 * Does not invent demand. Does not lower readiness gates.
 */

import { extractMarketOpportunityPacket } from "../market-opportunity-graph/market-opportunity-packet-v1.js";
import { evaluateHotelGeographicApplicability } from "../market-opportunity-graph/geographic-applicability-v1.js";
import { evaluateCrossHotelFit } from "../market-opportunity-graph/cross-hotel-fit-v1.js";
import { decideSupportingDataNextAction } from "../market-opportunity-graph/jev-supporting-data-router-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import {
  isCustomerFacingOpportunity,
  isGdiTestOrFixtureOpportunity,
} from "../customer-visibility.js";
import {
  classifyDominantBlocker,
  wasHeldDueToIncompleteHi,
  isHardTerminalBlocker,
  GDI_BLOCKER,
} from "./blocker-taxonomy-v1.js";

function snapshotOppLite(opp) {
  return {
    id: opp.id || opp.opportunityId,
    title: opp.title || opp.opportunityName || null,
    organizationName: opp.organizationName || opp.company || null,
    customerFacingState: opp.customerFacingState || null,
    priority: opp.priority || null,
    hotelFitScore: opp.hotelFitScore ?? null,
    venueStatus: opp.venueStatus || null,
    opportunityType: opp.opportunityType || null,
    eventStartDate: opp.eventStartDate || null,
    estimatedPeakRooms: opp.estimatedPeakRooms ?? opp.peakRooms ?? null,
    marketOpportunityId: opp.marketOpportunityId || null,
    eventSeriesId: opp.eventSeriesId || null,
    eventCycleId: opp.eventCycleId || null,
    subEventId: opp.subEventId || null,
  };
}

export function summarizeHotelCorpus(opps = [], opts = {}) {
  const rows = (opps || []).filter((o) => !isGdiTestOrFixtureOpportunity(o));
  const counts = {
    total: rows.length,
    customerVisible: 0,
    strictReady: 0,
    watch: 0,
    futureWatch: 0,
    held: 0,
    rejected: 0,
    actionable: 0,
    active: 0,
    internalOnly: 0,
    unknown: 0,
    priority: {},
  };
  const ids = {
    customerVisible: [],
    strictReady: [],
    watch: [],
    rejected: [],
    futureWatch: [],
  };

  for (const o of rows) {
    const ready = isGdiCustomerOpportunityReady(o, opts);
    const visible = isCustomerFacingOpportunity(o, opts);
    const state = String(o.customerFacingState || "UNKNOWN").toUpperCase();
    const pri = String(o.priority || "UNKNOWN").toUpperCase();
    counts.priority[pri] = (counts.priority[pri] || 0) + 1;

    if (ready.ok) {
      counts.strictReady += 1;
      ids.strictReady.push(o.id || o.opportunityId);
    }
    if (visible) {
      counts.customerVisible += 1;
      ids.customerVisible.push(o.id || o.opportunityId);
    }
    if (state === "WATCH") {
      counts.watch += 1;
      ids.watch.push(o.id || o.opportunityId);
    } else if (state === "FUTURE_WATCH") {
      counts.futureWatch += 1;
      ids.futureWatch.push(o.id || o.opportunityId);
    } else if (state === "CLOSED" || pri === "DISQUALIFIED") {
      counts.rejected += 1;
      ids.rejected.push(o.id || o.opportunityId);
    } else if (state === "ACTIONABLE_NOW") {
      counts.actionable += 1;
    } else if (state === "ACTIVE") {
      counts.active += 1;
    } else if (state === "INTERNAL_ONLY") {
      counts.internalOnly += 1;
    } else if (state === "UNKNOWN" || !o.customerFacingState) {
      counts.unknown += 1;
    }

    if (!ready.ok && !["CLOSED"].includes(state) && pri !== "DISQUALIFIED") {
      counts.held += 1;
    }
  }

  return { counts, ids, rows };
}

/**
 * Re-evaluate one opportunity against HI-enriched profile.
 */
export function requalifyOpportunity({
  opp,
  hotelProfile,
  hotelProfileBeforeHi = null,
} = {}) {
  if (isGdiTestOrFixtureOpportunity(opp)) {
    return { skipped: true, reason: "test_or_fixture" };
  }

  const readinessBefore = isGdiCustomerOpportunityReady(opp);
  const packet = extractMarketOpportunityPacket(opp, hotelProfile?.hotelId);
  const geoBefore = hotelProfileBeforeHi
    ? evaluateHotelGeographicApplicability(opp, hotelProfileBeforeHi)
    : null;
  const fitBefore = hotelProfileBeforeHi
    ? evaluateCrossHotelFit({
        marketPacket: packet,
        hotelProfile: hotelProfileBeforeHi,
        geographicApplicability: geoBefore,
        seedOpp: opp,
      })
    : {
        hotelFitScore: opp.hotelFitScore ?? null,
        missingData: [],
        finalState: null,
        productConstraint: null,
        reasons: ["pre_hi_score_from_row"],
      };

  const classBefore = classifyDominantBlocker({
    opp,
    hotelProfile: hotelProfileBeforeHi || hotelProfile,
    readiness: readinessBefore,
    fit: fitBefore,
  });

  const geoAfter = evaluateHotelGeographicApplicability(opp, hotelProfile);
  const fitAfter = evaluateCrossHotelFit({
    marketPacket: packet,
    hotelProfile,
    geographicApplicability: geoAfter,
    seedOpp: opp,
  });
  const classAfter = classifyDominantBlocker({
    opp: {
      ...opp,
      hotelFitScore: fitAfter.hotelFitScore ?? opp.hotelFitScore,
    },
    hotelProfile,
    readiness: readinessBefore,
    fit: fitAfter,
  });

  const oldFit = fitBefore.hotelFitScore;
  const newFit = fitAfter.hotelFitScore;
  const fitDelta =
    oldFit != null && newFit != null ? Number(newFit) - Number(oldFit) : null;

  const hiRecovered =
    wasHeldDueToIncompleteHi(classBefore) &&
    !isHardTerminalBlocker(classAfter.blocker) &&
    (classAfter.blocker !== classBefore.blocker ||
      (fitDelta != null && fitDelta >= 8) ||
      (hotelProfile?.hiEnriched &&
        hotelProfile?.hiOverlay?.meetingSqFt != null &&
        (hotelProfileBeforeHi?.meetingSqFt == null ||
          hotelProfileBeforeHi?.meetingSqFt === 0)));

  const jev = decideSupportingDataNextAction({
    marketPacket: packet,
    geographicApplicability: geoAfter,
    hotelFit: fitAfter,
  });

  const shouldContinueResearch =
    hiRecovered &&
    !isHardTerminalBlocker(classAfter.blocker) &&
    classAfter.blocker !== GDI_BLOCKER.READY &&
    jev?.action &&
    jev.action !== "STOP_NO_FURTHER_EVIDENCE";

  return {
    skipped: false,
    snapshot: snapshotOppLite(opp),
    marketOpportunityId: packet.marketOpportunityId,
    oldFit,
    newFit,
    fitDelta,
    oldBlocker: classBefore.blocker,
    newBlocker: classAfter.blocker,
    blockerChanged: classBefore.blocker !== classAfter.blocker,
    hiRelatedBefore: Boolean(classBefore.hiRelated),
    hiRecovered,
    shouldContinueResearch,
    hiFactThatChanged: describeHiChange(hotelProfileBeforeHi, hotelProfile),
    fitAfter,
    geoAfter: geoAfter.applicability,
    jev,
    readinessBefore: {
      ok: readinessBefore.ok,
      state: readinessBefore.state,
      failed: readinessBefore.failed,
    },
    reasons: fitAfter.reasons || [],
  };
}

function describeHiChange(before, after) {
  const parts = [];
  if ((before?.meetingSqFt == null || before?.meetingSqFt === 0) && after?.meetingSqFt) {
    parts.push(`meetingSqFt ${before?.meetingSqFt ?? "null"}→${after.meetingSqFt}`);
  }
  if ((before?.rooms == null || before?.rooms === 0) && after?.rooms) {
    parts.push(`rooms ${before?.rooms ?? "null"}→${after.rooms}`);
  }
  if (
    before?.eventSpaceDomainStatus !== after?.eventSpaceDomainStatus &&
    after?.eventSpaceDomainStatus
  ) {
    parts.push(
      `eventStatus ${before?.eventSpaceDomainStatus || "?"}→${after.eventSpaceDomainStatus}`
    );
  }
  if (after?.meetingRoomCount != null && before?.meetingRoomCount == null) {
    parts.push(`meetingRooms→${after.meetingRoomCount}`);
  }
  return parts.length ? parts.join("; ") : null;
}

export function requalifyHotelCorpus({
  hotelId,
  opportunities,
  hotelProfile,
  hotelProfileBeforeHi = null,
} = {}) {
  const beforeSummary = summarizeHotelCorpus(opportunities);
  const evaluations = [];
  const hiRecovered = [];

  for (const opp of beforeSummary.rows) {
    const ev = requalifyOpportunity({
      opp,
      hotelProfile,
      hotelProfileBeforeHi,
    });
    if (ev.skipped) continue;
    evaluations.push(ev);
    if (ev.hiRecovered) hiRecovered.push(ev);
  }

  return {
    hotelId,
    hotelName: hotelProfile?.displayName || hotelId,
    before: beforeSummary.counts,
    beforeIds: beforeSummary.ids,
    hiProfile: {
      rooms: hotelProfile?.rooms,
      meetingSqFt: hotelProfile?.meetingSqFt,
      meetingRoomCount: hotelProfile?.meetingRoomCount,
      eventStatus: hotelProfile?.eventSpaceDomainStatus,
      hiEnriched: hotelProfile?.hiEnriched,
      hiOverlay: hotelProfile?.hiOverlay,
    },
    evaluations,
    hiRecovered,
    hiRecoveredCount: hiRecovered.length,
    previouslyHeldDueToIncompleteHi: evaluations.filter((e) => e.hiRelatedBefore)
      .length,
  };
}
