/**
 * Hotel-specific fit for verified events / demand mechanisms.
 * Citywide events are signals — never automatic hotel-facing opportunities.
 */

import {
  EVENT_DEMAND_MECHANISM,
  EVENT_HOTEL_FIT_CLASS,
  isActionableMechanism,
} from "./event-to-demand-constants-v1.js";
import { isFutureEvent } from "./event-identity-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

function territoryBlob(hotel = {}) {
  return lower(
    [
      hotel.displayName,
      hotel.city,
      hotel.country,
      hotel.demandTerritory?.label,
      ...(hotel.demandTerritory?.includes || []),
      ...(hotel.commercialPriorities?.demandAnchorFocus || []),
      hotel.capabilityProfile?.classification,
      hotel.capabilityProfile?.serviceLevel,
    ].join(" ")
  );
}

function classifyTerritory(event = {}, hotel = {}) {
  const venue = lower([event.venue, event.city].filter(Boolean).join(" "));
  const keywords = hotel.demandTerritory?.classificationKeywords || {};
  // Specific stretch venues (ExCeL/Olympia/airports) before broad "london" competitive match
  const order = [
    "TERRITORY_CORE",
    "TERRITORY_NEARBY",
    "TERRITORY_STRETCH",
    "TERRITORY_COMPETITIVE",
    "OUTSIDE_REALISTIC_TERRITORY",
  ];
  for (const cls of order) {
    const kws = keywords[cls] || [];
    if (kws.some((k) => venue.includes(String(k).toLowerCase()))) return cls;
  }
  const city = lower(hotel.city || "");
  if (city && lower(event.city || "").includes(city)) return "TERRITORY_COMPETITIVE";
  return "OUTSIDE_REALISTIC_TERRITORY";
}

/**
 * Assess whether a mechanism for this event is commercially plausible for the hotel.
 * Never invents attendance, room blocks, or contracted housing status.
 */
export function assessEventHotelFit({
  event = {},
  hotel = {},
  mechanism = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL,
  evidence = {},
} = {}) {
  const reasons = [];
  const blockers = [];
  const rooms = Number(hotel.capabilityProfile?.totalGuestrooms || 0) || 0;
  const service = lower(hotel.capabilityProfile?.serviceLevel || "");
  const luxury = /luxury|lifestyle|upscale/.test(service) || /nobu|four seasons|rosewood/.test(territoryBlob(hotel));
  const territory = classifyTerritory(event, hotel);
  const future = isFutureEvent(event);

  if (!future) {
    blockers.push("event_not_future");
    return {
      fitClass: EVENT_HOTEL_FIT_CLASS.SPECULATIVE_MARKET_SIGNAL,
      promoteCandidate: false,
      territory,
      reasons: ["Event start is past or undated — not future demand"],
      blockers,
    };
  }

  if (territory === "OUTSIDE_REALISTIC_TERRITORY") {
    blockers.push("outside_realistic_territory");
    return {
      fitClass: EVENT_HOTEL_FIT_CLASS.SPECULATIVE_MARKET_SIGNAL,
      promoteCandidate: false,
      territory,
      reasons: ["Event venue/city outside hotel demand territory"],
      blockers,
    };
  }

  const housingContracted =
    evidence.officialHousingContracted === true ||
    /official\s+hotel|contracted\s+housing|housing\s+bureau/i.test(
      text(event.housingArrangements)
    );
  const attendance = Number(evidence.publishedAttendance || event.publishedAttendance || 0);
  const stretch = territory === "TERRITORY_STRETCH";

  let fitClass = String(mechanism || EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL);
  let promoteCandidate = false;

  switch (fitClass) {
    case EVENT_DEMAND_MECHANISM.HOST_EVENT: {
      const canHost =
        rooms > 0 &&
        (Number(hotel.capabilityProfile?.largestReceptionCapacity || 0) > 0 ||
          Number(hotel.capabilityProfile?.largestBallroomSqFt || 0) > 0);
      if (!canHost) {
        blockers.push("insufficient_meeting_inventory_evidence");
        fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
      } else if (attendance && attendance > rooms * 3) {
        reasons.push(
          "Published attendance exceeds plausible on-property host capacity; host-event unlikely"
        );
        fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
      } else if (territory === "TERRITORY_CORE" || territory === "TERRITORY_NEARBY") {
        promoteCandidate = true;
        reasons.push("Venue catchment supports on-property host or associated meeting use");
      } else {
        fitClass = EVENT_DEMAND_MECHANISM.OFFSITE_MEETING;
        promoteCandidate = Boolean(evidence.organizerIdentified);
        reasons.push("Stretch/competitive host unlikely; off-site meeting path if organizer identified");
      }
      break;
    }
    case EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING: {
      if (!evidence.organizationNamed) {
        blockers.push("exhibitor_org_not_identified");
        fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
      } else if (housingContracted && evidence.openDemandSupported !== true) {
        blockers.push("official_housing_contracted_no_open_demand");
        reasons.push("Official housing exists; open lodging demand not evidenced");
        fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
      } else if (stretch && !evidence.maryleboneOvernightEvidence) {
        blockers.push("stretch_venue_without_overnight_evidence");
        fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
        reasons.push("Stretch venue requires overnight/referral evidence for this hotel");
      } else {
        promoteCandidate = true;
        reasons.push("Named exhibitor/sponsor with plausible lodging mechanism");
      }
      break;
    }
    case EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION:
    case EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY:
    case EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING:
    case EVENT_DEMAND_MECHANISM.OFFSITE_MEETING:
    case EVENT_DEMAND_MECHANISM.ANCILLARY_EVENT_BUSINESS: {
      if (!evidence.organizationNamed && !evidence.organizerIdentified) {
        blockers.push("organization_not_identified");
        fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
      } else if (luxury && /vip|executive|hospitality|board|delegation/i.test(fitClass)) {
        promoteCandidate = true;
        reasons.push("Luxury positioning compatible with executive/VIP mechanism");
      } else {
        promoteCandidate = true;
        reasons.push("Organization identified with evidence-backed mechanism");
      }
      // Stretch venues: main-conference lodging often unsuitable, but executive /
      // VIP / offsite mechanisms can still be hotel-relevant for a luxury property
      // when the organization is identified (mechanism-specific threshold).
      if (stretch && !evidence.maryleboneOvernightEvidence) {
        const stretchOkWithoutOvernight =
          fitClass === EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION ||
          fitClass === EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY ||
          fitClass === EVENT_DEMAND_MECHANISM.OFFSITE_MEETING;
        if (stretchOkWithoutOvernight && luxury) {
          promoteCandidate = true;
          reasons.push(
            "Stretch venue: executive/VIP/offsite prospecting allowed for luxury hotel without requiring mass overnight evidence"
          );
        } else {
          promoteCandidate = false;
          blockers.push("stretch_without_overnight_evidence");
          fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
        }
      }
      break;
    }
    default: {
      fitClass = EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL;
      promoteCandidate = false;
      reasons.push("Market signal only — not hotel-facing without a demand mechanism");
    }
  }

  if (promoteCandidate && !isActionableMechanism(fitClass)) {
    promoteCandidate = false;
  }

  return {
    fitClass,
    promoteCandidate,
    territory,
    rooms,
    luxury,
    housingContracted,
    publishedAttendance: attendance || null,
    reasons,
    blockers,
  };
}
