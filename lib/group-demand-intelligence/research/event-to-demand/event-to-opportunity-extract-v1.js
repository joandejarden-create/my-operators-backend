/**
 * Extract organization-level opportunities from a verified event.
 * One event → multiple orgs/mechanisms only when evidence supports each.
 * Never invents dates, attendance, room blocks, or buyers.
 */

import { RESEARCH_TERMINAL } from "../research-constants-v1.js";
import {
  EVENT_DEMAND_MECHANISM,
  EVENT_TO_DEMAND_VERSION,
  primarySegmentForDomain,
  isActionableMechanism,
} from "./event-to-demand-constants-v1.js";
import {
  eventDerivedOpportunityId,
  eventDerivedOpportunityIdentityKey,
  isFutureEvent,
} from "./event-identity-v1.js";
import { assessEventHotelFit } from "./event-hotel-fit-v1.js";
import {
  buildOrganizationContactRoute,
  isExploratoryWithoutRoomBlock,
} from "./event-organization-route-v1.js";
import { normalizeOrgIdentityKey } from "../../workflow/reuse-prior-intelligence-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function orgNamesOverlap(a = "", b = "") {
  const na = text(a).toLowerCase();
  const nb = text(b).toLowerCase();
  if (!na || !nb) return false;
  if (normalizeOrgIdentityKey(a) === normalizeOrgIdentityKey(b)) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  // Strip trailing years for "London Tech Week" vs "London Tech Week 2027"
  const stripYear = (s) => s.replace(/\b20\d{2}\b/g, "").replace(/\s+/g, " ").trim();
  const sa = stripYear(na);
  const sb = stripYear(nb);
  return Boolean(sa && sb && (sa.includes(sb) || sb.includes(sa)));
}

function existingIsFacingDuplicate(existing = [], organizationName, event) {
  const eventName = text(event?.eventName || "").toLowerCase();
  return existing.find((o) => {
    const sameOrg = orgNamesOverlap(o.organizationName || "", organizationName || "");
    if (!sameOrg) return false;
    const facing =
      o.customerVisible === true ||
      o.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT ||
      o.researchStatus === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT ||
      Boolean(o.prospectPublicationBasis) ||
      // Also suppress against in-progress / stopped E2D siblings in memory
      String(o.id || "").startsWith("gdi_e2d_");
    if (!facing) return false;
    const blob = `${o.title || ""} ${o.eventName || ""} ${o.localConnection || ""} ${o.organizationName || ""}`.toLowerCase();
    const eventOverlap =
      !eventName ||
      blob.includes(eventName) ||
      eventName.includes(text(o.eventName || "").toLowerCase()) ||
      Boolean(o.eventId && event?.eventId && o.eventId === event.eventId);
    return eventOverlap;
  });
}

/**
 * @param {object} args
 * @param {object} args.event - verified event
 * @param {object} args.hotel
 * @param {object[]} [args.existingOpportunities] - for dedupe
 * @returns {{ candidates: object[], signals: object[], rejected: object[] }}
 */
export function extractOpportunitiesFromVerifiedEvent({
  event = {},
  hotel = {},
  existingOpportunities = [],
} = {}) {
  const candidates = [];
  const signals = [];
  const rejected = [];

  if (!event?.eventName && !event?.name) {
    rejected.push({ reason: "missing_event_name", event });
    return { candidates, signals, rejected };
  }
  if (event.verified !== true) {
    rejected.push({
      reason: "unverified_event_excluded_from_customer_path",
      eventId: event.eventId,
      eventName: event.eventName,
    });
    return { candidates, signals, rejected };
  }
  if (!isFutureEvent(event)) {
    rejected.push({
      reason: "past_or_undated_event",
      eventId: event.eventId,
      eventName: event.eventName,
    });
    return { candidates, signals, rejected };
  }

  const existingKeys = new Set(
    existingOpportunities.map((o) => {
      if (o.eventDerivedIdentityKey) return o.eventDerivedIdentityKey;
      return eventDerivedOpportunityIdentityKey({
        organizationName: o.organizationName,
        event: {
          eventName: o.eventName || o.title,
          eventStartDate: o.eventStartDate,
          city: o.city || hotel.city,
          venue: o.venue,
        },
        mechanism: o.eventDemandMechanism || o.demandMechanism,
      });
    })
  );

  const stakeholders = Array.isArray(event.stakeholders) ? event.stakeholders : [];
  const proposals = [];

  // Organizer path — one primary mechanism, plus optional distinct evidence-backed extras
  if (event.organizer && event.organizerDemandRationale) {
    proposals.push({
      organizationName: event.organizer,
      mechanism:
        event.proposedOrganizerMechanism ||
        EVENT_DEMAND_MECHANISM.OFFSITE_MEETING,
      evidence: {
        organizerIdentified: true,
        organizationNamed: true,
        officialHousingContracted: Boolean(event.housingArrangements),
        openDemandSupported: event.openDemandSupported === true,
        maryleboneOvernightEvidence: event.maryleboneOvernightEvidence === true,
        publishedAttendance: event.publishedAttendance,
        rationale: event.organizerDemandRationale,
      },
    });
  } else if (event.organizer && !event.organizerDemandRationale) {
    // Live-discovered events often lack curated rationale — require explicit
    // proposedMechanisms or stakeholders rather than inventing demand.
    if (!stakeholders.length && !Array.isArray(event.proposedMechanisms)) {
      rejected.push({
        reason: "organizer_without_demand_rationale",
        organizationName: event.organizer,
        eventId: event.eventId,
      });
    }
  }

  // Additional distinct mechanisms for the same organizer when evidence-backed
  for (const pm of event.proposedMechanisms || []) {
    if (!pm?.mechanism || !pm?.demandRationale) continue;
    if (!isActionableMechanism(pm.mechanism)) continue;
    proposals.push({
      organizationName: pm.organizationName || event.organizer,
      mechanism: pm.mechanism,
      evidence: {
        organizerIdentified: Boolean(event.organizer),
        organizationNamed: true,
        officialHousingContracted: Boolean(event.housingArrangements),
        openDemandSupported: pm.openDemandSupported === true,
        maryleboneOvernightEvidence:
          pm.maryleboneOvernightEvidence === true ||
          event.maryleboneOvernightEvidence === true,
        publishedAttendance: event.publishedAttendance,
        rationale: pm.demandRationale,
      },
      contact: pm.contact || null,
    });
  }

  // Explicit stakeholder extractions (exhibitors, sponsors, VIP hosts)
  for (const s of stakeholders) {
    if (!s?.organizationName) continue;
    if (!s.mechanism || !isActionableMechanism(s.mechanism)) {
      rejected.push({
        reason: "stakeholder_without_actionable_mechanism",
        organizationName: s.organizationName,
        eventId: event.eventId,
      });
      continue;
    }
    if (!s.demandRationale) {
      rejected.push({
        reason: "stakeholder_missing_demand_rationale",
        organizationName: s.organizationName,
        eventId: event.eventId,
      });
      continue;
    }
    proposals.push({
      organizationName: s.organizationName,
      mechanism: s.mechanism,
      evidence: {
        organizerIdentified: Boolean(event.organizer),
        organizationNamed: true,
        officialHousingContracted: Boolean(event.housingArrangements),
        openDemandSupported: s.openDemandSupported === true,
        maryleboneOvernightEvidence:
          s.maryleboneOvernightEvidence === true ||
          event.maryleboneOvernightEvidence === true,
        publishedAttendance: event.publishedAttendance,
        rationale: s.demandRationale,
      },
      contact: s.contact || null,
      stakeholder: s,
    });
  }

  // Destination-wide with no stakeholders → internal signal only
  if (!proposals.length) {
    const fit = assessEventHotelFit({
      event,
      hotel,
      mechanism: EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL,
      evidence: {},
    });
    signals.push({
      kind: "verified_event_market_signal",
      eventId: event.eventId,
      eventName: event.eventName,
      fitClass: fit.fitClass,
      territory: fit.territory,
      coverageCredit: true,
      note: "Verified event contributes to discovery coverage without a hotel-facing candidate",
    });
    return { candidates, signals, rejected };
  }

  const segment =
    event.segment ||
    primarySegmentForDomain(event.domain) ||
    "associations_government";

  for (const p of proposals) {
    const fit = assessEventHotelFit({
      event,
      hotel,
      mechanism: p.mechanism,
      evidence: p.evidence,
    });

    if (fit.fitClass === EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL || !fit.promoteCandidate) {
      signals.push({
        kind: "event_fit_rejected",
        eventId: event.eventId,
        organizationName: p.organizationName,
        mechanism: p.mechanism,
        fit,
      });
      rejected.push({
        reason: fit.blockers[0] || "hotel_fit_rejected",
        organizationName: p.organizationName,
        eventId: event.eventId,
        fit,
      });
      continue;
    }

    if (!p.evidence.rationale) {
      rejected.push({
        reason: "missing_evidence_backed_rationale",
        organizationName: p.organizationName,
        eventId: event.eventId,
      });
      continue;
    }

    const identityKey = eventDerivedOpportunityIdentityKey({
      organizationName: p.organizationName,
      event,
      mechanism: fit.fitClass,
    });
    if (existingKeys.has(identityKey)) {
      rejected.push({
        reason: "duplicate_org_event_mechanism",
        identityKey,
        organizationName: p.organizationName,
      });
      continue;
    }

    const priorFacing = existingIsFacingDuplicate(
      existingOpportunities,
      p.organizationName,
      event
    );
    if (priorFacing) {
      rejected.push({
        reason: "duplicate_of_existing_facing_opportunity",
        organizationName: p.organizationName,
        eventId: event.eventId,
        duplicateOf: priorFacing.id || priorFacing.opportunityId || null,
      });
      continue;
    }

    existingKeys.add(identityKey);

    const id = eventDerivedOpportunityId({
      organizationName: p.organizationName,
      event,
      mechanism: fit.fitClass,
    });

    const start = event.eventStartDate || null;
    const route = buildOrganizationContactRoute({
      event,
      stakeholder: p.stakeholder || null,
      organizationName: p.organizationName,
    });
    const contact = {
      ...(p.contact || {}),
      ...route.contact,
    };

    // Mechanism-proportionate demand scale:
    // exploratory mechanisms do NOT invent room blocks / attendance.
    // confirmed lodging (HOST / contracted exhibitor open demand) may mark supported.
    const confirmedLodging =
      fit.fitClass === EVENT_DEMAND_MECHANISM.HOST_EVENT ||
      (p.evidence.openDemandSupported === true &&
        Number(p.evidence.publishedAttendance || 0) > 0);
    const demandScale = confirmedLodging
      ? {
          supported: true,
          hotelRelevant: true,
          estimatedPeakRooms: null,
          attendeeCount: p.evidence.publishedAttendance || null,
          basis: "source_supported_lodging_or_attendance",
          sourceUrl: event.officialUrl || null,
        }
      : {
          supported: false,
          hotelRelevant: true,
          estimatedPeakRooms: null,
          attendeeCount: p.evidence.publishedAttendance || null,
          basis: isExploratoryWithoutRoomBlock(fit.fitClass)
            ? "exploratory_mechanism_without_confirmed_room_block"
            : "demand_scale_unconfirmed",
          sourceUrl: event.officialUrl || null,
        };

    candidates.push({
      id,
      opportunityId: id,
      hotelId: hotel.hotelId || hotel.canonicalHotelId || null,
      organizationName: p.organizationName,
      organizationIdentityKey: normalizeOrgIdentityKey(p.organizationName),
      title: `${p.organizationName} — ${event.eventName}`,
      segment,
      researchSegment: segment,
      eventId: event.eventId,
      eventName: event.eventName,
      eventStartDate: start,
      venue: event.venue || null,
      city: event.city || hotel.city || null,
      eventDemandMechanism: fit.fitClass,
      eventHotelFitClass: fit.fitClass,
      eventDerivedIdentityKey: identityKey,
      eventTerritoryClass: fit.territory,
      discoverySource: "event_to_demand_v1",
      researchStatus: RESEARCH_TERMINAL.IN_PROGRESS,
      futureDemand: start
        ? {
            supported: true,
            dateWindow: String(start).slice(0, 10),
            sourceUrl: event.officialUrl || null,
          }
        : { supported: false },
      hotelFit: {
        supported: true,
        viable: true,
        fitClass: fit.fitClass,
        territory: fit.territory,
        reasons: fit.reasons,
        luxuryCompatible: fit.luxury === true,
      },
      demandScale,
      localConnection: p.evidence.rationale,
      hotelFitRationale: fit.reasons.join("; "),
      proposedHotelUse: fit.fitClass,
      sources: event.sources || [],
      evidence: [
        ...(event.sources || []).map((s) => ({
          sourceFamily: s.sourceFamily || "official_event",
          url: s.url,
        })),
        {
          sourceFamily: "event_mechanism_rationale",
          note: p.evidence.rationale,
        },
        ...(route.actionable
          ? [
              {
                sourceFamily: "organization_sales_route",
                url: contact.organizationRouteUrl || contact.contactFormUrl,
              },
            ]
          : []),
      ],
      contact,
      contactRouteQuality:
        contact.contactRouteQuality || route.contactRouteQuality || null,
      notes: [
        `${EVENT_TO_DEMAND_VERSION}:${fit.fitClass}`,
        `event:${event.eventId}`,
        route.actionable
          ? `org_route:${route.reason}`
          : `org_route_pending:${route.reason}`,
      ],
      eventToDemand: {
        version: EVENT_TO_DEMAND_VERSION,
        eventId: event.eventId,
        mechanism: fit.fitClass,
        fit,
        domain: event.domain || null,
        exploratoryWithoutRoomBlock: isExploratoryWithoutRoomBlock(fit.fitClass),
        organizationRoute: route.reason,
        contactRouteQuality: route.contactRouteQuality || null,
      },
    });
  }

  return { candidates, signals, rejected };
}

/**
 * Extract across a verified event corpus; dedupe across events.
 */
export function extractOpportunitiesFromEventCorpus({
  events = [],
  hotel = {},
  existingOpportunities = [],
} = {}) {
  const allCandidates = [];
  const allSignals = [];
  const allRejected = [];
  let existing = [...existingOpportunities];
  for (const event of events) {
    const out = extractOpportunitiesFromVerifiedEvent({
      event,
      hotel,
      existingOpportunities: existing,
    });
    allCandidates.push(...out.candidates);
    allSignals.push(...out.signals);
    allRejected.push(...out.rejected);
    existing = [...existing, ...out.candidates];
  }
  return {
    candidates: allCandidates,
    signals: allSignals,
    rejected: allRejected,
    metrics: {
      eventsIn: events.length,
      candidatesOut: allCandidates.length,
      signalsOut: allSignals.length,
      rejectedOut: allRejected.length,
    },
  };
}
