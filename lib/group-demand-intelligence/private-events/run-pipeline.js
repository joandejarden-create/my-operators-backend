/**
 * Offline Private Events pipeline — venue graph → priority → signals → qualify.
 * Dry-run / fixture driven. No live writes. No contact-provider spend.
 */

import { buildHotelContext, buildHotelVenueRelationship } from "./hotel-context.js";
import { createVenueGraph } from "./venue-entity.js";
import { classifyVenuePriority } from "./venue-priority.js";
import {
  qualifySpecificEvent,
  qualifyVenuePartnership,
  LIFECYCLE,
} from "./qualify.js";
import { VENUE_PRIORITY } from "./constants.js";
import { PRIVATE_EVENTS_V1 } from "./constants.js";

/**
 * Run PE module for one hotel against a venue universe + optional event signals.
 * Venues may be shared across hotels via the same venue graph instance.
 */
export function runPrivateEventsForHotel({
  hotel,
  venues = [],
  eventSignals = [],
  venueGraph = null,
  asOf = new Date("2026-09-24T12:00:00Z"),
  costTracker = null,
} = {}) {
  const started = Date.now();
  const costs = costTracker || {
    venueDiscoveryQueries: 0,
    venueFetches: 0,
    eventSignalQueries: 0,
    officialPageFetches: 0,
    candidates: 0,
    qualified: 0,
    trueCount: 0,
    costUsd: 0,
    cacheReuse: 0,
  };

  const hotelCtx = buildHotelContext(hotel);
  const graph = venueGraph || createVenueGraph();

  const venueAudits = [];
  const partnershipOpps = [];
  const specificOpps = [];

  for (const raw of venues) {
    costs.candidates += 1;
    const { venue, reused } = graph.upsert(raw);
    if (reused) costs.cacheReuse += 1;

    const relationship = buildHotelVenueRelationship(hotelCtx, venue, {
      distanceMiles: raw.distanceMiles,
      driveTimeMinutes: raw.driveTimeMinutes,
      existingRelationshipStatus: raw.existingRelationshipStatus,
    });
    const priority = classifyVenuePriority(venue, relationship);

    venueAudits.push({
      venueId: venue.venueId,
      venueName: venue.venueName,
      venueType: venue.venueType,
      distanceMiles: relationship.distanceMiles,
      capacity: venue.maxCapacity,
      lodgingStatus: venue.onSiteLodgingStatus,
      weddingsAdvertised: venue.weddingsAdvertised,
      privateEventsAdvertised: venue.privateEventsAdvertised,
      estimatedAnnualPrivateEvents: venue.estimatedAnnualPrivateEvents,
      hotelPartners: venue.hotelPartners,
      exclusiveHotelRelationship: venue.exclusiveHotelRelationship,
      eventContact: venue.eventContact,
      eventContactRole: venue.eventContactRole,
      priority: priority.priority,
      priorityFactors: priority.factors,
      sourceUrls: venue.sourceUrls,
      website: venue.website,
      lodgingCatchmentFit: relationship.lodgingCatchmentFit,
      productFit: relationship.productFit,
    });

    const partnership = qualifyVenuePartnership({
      venue,
      hotelCtx,
      relationship,
    });
    costs.qualified += 1;

    if (
      partnership.actionable ||
      partnership.lifecycle === LIFECYCLE.WATCH ||
      partnership.lifecycle === LIFECYCLE.ACTIONABLE_NOW
    ) {
      partnershipOpps.push({
        venueId: venue.venueId,
        venueName: venue.venueName,
        venueType: venue.venueType,
        capacity: venue.maxCapacity,
        onSiteLodging: venue.onSiteLodgingStatus,
        onSiteGuestrooms: venue.onSiteGuestrooms,
        distanceMiles: relationship.distanceMiles,
        privateEventActivity: {
          weddingsAdvertised: venue.weddingsAdvertised,
          privateEventsAdvertised: venue.privateEventsAdvertised,
          estimatedAnnualPrivateEvents: venue.estimatedAnnualPrivateEvents,
        },
        existingHotelPartner: venue.hotelPartners?.length
          ? venue.hotelPartners
          : venue.preferredHotelListed
            ? ["PREFERRED_LISTED"]
            : [],
        contactPath: partnership.buyerPath,
        whyOpportunityExists: partnership.whyOpportunityExists,
        recommendedAction: partnership.recommendedAction,
        evidence: venue.sourceUrls,
        priority: partnership.venuePriority,
        lifecycle: partnership.lifecycle,
        actionable: partnership.actionable,
        rejectReasons: partnership.rejectReasons,
      });
    }
    if (partnership.actionable) costs.trueCount += 1;
  }

  // Forward event signals only for HIGH_POTENTIAL_PARTNER + EVENT_SIGNAL_TARGET
  const signalEligible = new Set(
    venueAudits
      .filter(
        (v) =>
          v.priority === VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER ||
          v.priority === VENUE_PRIORITY.EVENT_SIGNAL_TARGET
      )
      .map((v) => v.venueId)
  );

  let rejectedSignals = 0;
  let watchSignals = 0;
  let actionableSignals = 0;

  for (const signal of eventSignals) {
    costs.eventSignalQueries += 1;
    const venue =
      (signal.venueId && graph.get(signal.venueId)) ||
      graph.list().find(
        (v) =>
          v.venueName &&
          signal.venueName &&
          v.venueName.toLowerCase() === String(signal.venueName).toLowerCase()
      ) ||
      null;

    if (venue && !signalEligible.has(venue.venueId) && signal.venueId) {
      // Still evaluate but mark as out-of-priority search scope
    }

    const relationship = venue
      ? buildHotelVenueRelationship(hotelCtx, venue, {
          distanceMiles: signal.distanceMiles,
        })
      : {
          hotelId: hotelCtx.hotelId,
          venueId: null,
          lodgingCatchmentFit: signal.lodgingCatchmentFit || "UNKNOWN",
          distanceMiles: signal.distanceMiles ?? null,
          productFit: "UNKNOWN",
        };

    const q = qualifySpecificEvent({
      signal,
      venue: venue || {
        venueId: null,
        venueName: signal.venueName,
        onSiteLodgingStatus: signal.venueLodgingStatus || "UNKNOWN",
        onSiteGuestrooms: signal.onSiteGuestrooms,
        exclusiveHotelRelationship: false,
        sourceUrls: signal.sourceUrl ? [signal.sourceUrl] : [],
      },
      hotelCtx,
      relationship,
      asOf,
    });

    if (q.rejectReasons?.length) {
      rejectedSignals += 1;
      specificOpps.push({
        eventName: signal.eventName,
        eventType: signal.eventType,
        eventDate: signal.eventDate,
        venueName: venue?.venueName || signal.venueName,
        rejected: true,
        rejectReasons: q.rejectReasons,
        lifecycle: q.lifecycle,
        actionable: false,
      });
      continue;
    }

    if (q.actionable) actionableSignals += 1;
    else watchSignals += 1;
    if (q.actionable) costs.trueCount += 1;

    specificOpps.push({
      eventName: signal.eventName,
      eventType: signal.eventType || q.demandSignalType,
      eventDate: signal.eventDate,
      venueId: venue?.venueId || null,
      venueName: venue?.venueName || signal.venueName,
      distanceMiles: relationship.distanceMiles,
      attendance: q.lodgingCapture?.attendance,
      potentialRoomsLow: q.lodgingCapture?.potentialRoomsLow,
      potentialRoomsHigh: q.lodgingCapture?.potentialRoomsHigh,
      roomDemandStatus: q.lodgingCapture?.roomDemandStatus,
      displayLabel: q.lodgingCapture?.displayLabel,
      lodgingStatus: venue?.onSiteLodgingStatus,
      buyerPath: q.lodgingCapture?.buyerPath,
      whyHotelCouldWin: q.whyHotelCouldWin,
      recommendedAction: q.recommendedAction,
      evidence: signal.sourceUrl ? [signal.sourceUrl] : signal.sourceUrls || [],
      confidence: q.actionable ? "ACTIONABLE" : q.lifecycle,
      lifecycle: q.lifecycle,
      actionable: q.actionable,
      downgradeReasons: q.downgradeReasons,
      falsePrecision: q.lodgingCapture?.falsePrecision === true,
    });
  }

  const priorityCounts = {
    HIGH_POTENTIAL_PARTNER: 0,
    EVENT_SIGNAL_TARGET: 0,
    WATCH: 0,
    LOW_HOTEL_OPPORTUNITY: 0,
  };
  for (const v of venueAudits) {
    if (priorityCounts[v.priority] != null) priorityCounts[v.priority] += 1;
  }

  const withCapacity = venueAudits.filter((v) => v.capacity != null).length;
  const withLodging = venueAudits.filter(
    (v) => v.lodgingStatus && v.lodgingStatus !== "UNKNOWN"
  ).length;
  const withActivity = venueAudits.filter(
    (v) =>
      v.weddingsAdvertised ||
      v.privateEventsAdvertised ||
      (v.estimatedAnnualPrivateEvents || 0) > 0
  ).length;
  const withPartnerStatus = venueAudits.filter(
    (v) =>
      v.exclusiveHotelRelationship ||
      (v.hotelPartners || []).length > 0 ||
      v.priority
  ).length;
  const withContact = venueAudits.filter(
    (v) => v.eventContact || v.eventContactRole
  ).length;
  const withOfficial = venueAudits.filter(
    (v) => (v.sourceUrls || []).length > 0 || v.website
  ).length;
  const n = venueAudits.length || 1;

  return {
    module: PRIVATE_EVENTS_V1,
    hotelId: hotelCtx.hotelId,
    hotelArchetype: hotelCtx.archetype,
    practicalCatchment: hotelCtx.practicalCatchment,
    preferredVenueTypes: hotelCtx.preferredVenueTypes,
    venueUniverse: {
      total: venueAudits.length,
      ...priorityCounts,
      audits: venueAudits,
    },
    venueDataQuality: {
      withCapacityPct: Math.round((withCapacity / n) * 100),
      withLodgingStatusPct: Math.round((withLodging / n) * 100),
      withEventActivityPct: Math.round((withActivity / n) * 100),
      withHotelPartnershipStatusPct: Math.round((withPartnerStatus / n) * 100),
      withContactPathPct: Math.round((withContact / n) * 100),
      withOfficialSourcePct: Math.round((withOfficial / n) * 100),
    },
    eventSignals: {
      futureEventSignals: eventSignals.length,
      specificEventCandidates: specificOpps.length,
      actionable: actionableSignals,
      watch: watchSignals,
      rejected: rejectedSignals,
    },
    venuePartnershipOpportunities: partnershipOpps.filter((p) => p.actionable),
    venuePartnershipWatch: partnershipOpps.filter((p) => !p.actionable),
    specificEventOpportunities: specificOpps.filter((o) => o.actionable),
    specificEventWatchOrRejected: specificOpps.filter((o) => !o.actionable),
    derivedDemand: {
      modeledRoomRange: specificOpps.filter(
        (o) => o.roomDemandStatus === "MODELED"
      ).length,
      confirmedRoomDemand: specificOpps.filter(
        (o) => o.roomDemandStatus === "CONFIRMED"
      ).length,
      unknown: specificOpps.filter((o) => o.roomDemandStatus === "UNKNOWN").length,
      falsePrecisionIssues: specificOpps.filter((o) => o.falsePrecision).length,
    },
    costs: {
      ...costs,
      runtimeMs: Date.now() - started,
    },
    venueGraphSize: graph.size(),
    _graph: graph,
  };
}
