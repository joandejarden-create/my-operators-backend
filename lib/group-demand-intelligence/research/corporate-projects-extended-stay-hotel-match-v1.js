/**
 * GDI 2.0 Phase 3B — Corporate Projects & Extended Stay hotel-specific matching.
 * Preserves unknowns; does not fabricate room-night estimates or confirmed stays.
 */

import { assessHotelGeographicFit } from "./commercial-spine-restoration-v41.js";
import {
  CORPORATE_PROJECTS_EXTENDED_STAY_V1,
  CPE_RESEARCH_SEGMENT,
} from "./corporate-projects-extended-stay-constants-v1.js";

export const CPE_HOTEL_MATCH_V1 = "gdi-corporate-projects-extended-stay-hotel-match-v1";

export const CPE_HOTEL_FIT_CLASS = Object.freeze({
  STRONG_MATCH: "STRONG_MATCH",
  PLAUSIBLE_MATCH: "PLAUSIBLE_MATCH",
  WEAK_OR_UNCERTAIN: "WEAK_OR_UNCERTAIN",
  NO_MEANINGFUL_FIT: "NO_MEANINGFUL_FIT",
  INCOMPLETE_HOTEL_DATA: "INCOMPLETE_HOTEL_DATA",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hotelCity(hotel = {}) {
  return (
    text(hotel.city) ||
    text(hotel.location?.city) ||
    text(hotel.demandTerritory?.label?.split("/")?.[0]) ||
    ""
  );
}

function hotelCapability(hotel = {}) {
  const cap = hotel.capabilityProfile || hotel.capabilities || {};
  return {
    guestrooms: Number(cap.totalGuestrooms || hotel.rooms || 0) || null,
    meetingSqFt: Number(cap.totalMeetingSpaceSqFt || 0) || null,
    meetingRooms: Number(cap.meetingRoomCount || cap.meetingRoomsIndoor || 0) || null,
    serviceLevel: text(cap.serviceLevel || hotel.serviceLevel),
    incomplete:
      !Number(cap.totalGuestrooms || hotel.rooms || 0) &&
      !Number(cap.totalMeetingSpaceSqFt || 0),
  };
}

function activityBlob(opportunity = {}) {
  return [
    opportunity.activityHeadline,
    opportunity.whatIsHappening,
    opportunity.hotelDemandThesis,
    opportunity.proposedHotelUse,
    opportunity.location,
    opportunity.city,
    opportunity.projectSite,
    opportunity.evidenceQuote,
  ]
    .map(text)
    .filter(Boolean)
    .join(" — ");
}

export function assessCpeHotelSpecificMatch(opportunity = {}, hotel = {}, opts = {}) {
  const city = hotelCity(hotel);
  const cap = hotelCapability(hotel);
  const blob = activityBlob(opportunity);
  const displayName = text(hotel.displayName || hotel.name || hotel.hotelId);

  const geo = assessHotelGeographicFit({
    targetHotel: displayName,
    targetCity: city,
    evidenceBlob: blob,
    organizationLocation: opportunity.organizationLocation || opportunity.hqCity || null,
    eventDestination: opportunity.projectCity || opportunity.eventCity || opportunity.city || null,
    nationalOrganization: Boolean(opportunity.nationalOrganization),
  });

  const needsExtendedStay =
    /\b(extended stay|multi-?week|multi-?month|deployment|mobilization|field (?:team|crew)|project duration)\b/i.test(
      blob,
    );
  const needsRooms =
    /\b(guestroom|room.?night|overnight|lodging|housing|accommodation|crew travel|per diem)\b/i.test(
      blob,
    ) || needsExtendedStay;
  const needsMeeting =
    /\b(meeting|kickoff|training|workshop|coordination meeting|steering committee)\b/i.test(blob);
  const nonlocalPersonnel =
    /\b(visiting|nonlocal|non-local|fly-?in|travel(?:ing)? (?:crew|team|staff|engineers)|mobiliz)\b/i.test(
      blob,
    );

  const facilityNotes = [];
  let facilitySupport = "unknown";
  if (cap.incomplete) {
    facilityNotes.push("hotel_capability_profile_incomplete");
  } else if (needsRooms) {
    if (cap.guestrooms && cap.guestrooms >= 20) {
      facilitySupport = "supported_by_profile";
      facilityNotes.push(`guestrooms_${cap.guestrooms}`);
      if (needsExtendedStay) {
        facilityNotes.push("extended_stay_pattern_inferred_not_confirmed_inventory");
      }
    } else {
      facilityNotes.push("guestroom_count_unknown_or_low");
    }
  }
  if (needsMeeting && cap.meetingRooms >= 1) {
    facilityNotes.push(`meeting_rooms_${cap.meetingRooms}`);
  }

  const territoryHit =
    city &&
    blob &&
    (new RegExp(city.split(/\s+/)[0], "i").test(blob) ||
      (hotel.demandTerritory?.includes || []).some((t) =>
        new RegExp(String(t).split(/\s+where\b/)[0].slice(0, 12), "i").test(blob),
      ));

  let fitClass = CPE_HOTEL_FIT_CLASS.WEAK_OR_UNCERTAIN;
  if (cap.incomplete && !territoryHit && !geo.eventQualifiedForTargetHotel) {
    fitClass = CPE_HOTEL_FIT_CLASS.INCOMPLETE_HOTEL_DATA;
  } else if (geo.geoFitClass === "NAMED_COMPETITOR_HOST_ELSEWHERE") {
    fitClass = CPE_HOTEL_FIT_CLASS.NO_MEANINGFUL_FIT;
  } else if (geo.eventQualifiedForTargetHotel && nonlocalPersonnel) {
    fitClass = CPE_HOTEL_FIT_CLASS.STRONG_MATCH;
  } else if (territoryHit || (geo.organizationStillAccountEligible && nonlocalPersonnel)) {
    fitClass = CPE_HOTEL_FIT_CLASS.PLAUSIBLE_MATCH;
  } else if (!city) {
    fitClass = CPE_HOTEL_FIT_CLASS.INCOMPLETE_HOTEL_DATA;
  } else if (!nonlocalPersonnel && !needsRooms) {
    fitClass = CPE_HOTEL_FIT_CLASS.NO_MEANINGFUL_FIT;
    facilityNotes.push("no_nonlocal_personnel_or_lodging_signal");
  } else {
    fitClass = CPE_HOTEL_FIT_CLASS.WEAK_OR_UNCERTAIN;
  }

  return {
    version: CPE_HOTEL_MATCH_V1,
    parentVersion: CORPORATE_PROJECTS_EXTENDED_STAY_V1,
    researchSegment: CPE_RESEARCH_SEGMENT,
    hotelId: hotel.hotelId || opts.hotelId || null,
    hotelDisplayName: displayName || null,
    fitClass,
    commerciallyAccessible:
      fitClass === CPE_HOTEL_FIT_CLASS.STRONG_MATCH ||
      fitClass === CPE_HOTEL_FIT_CLASS.PLAUSIBLE_MATCH,
    geographic: {
      city: city || null,
      geoFitClass: geo.geoFitClass,
      eventQualifiedForTargetHotel: geo.eventQualifiedForTargetHotel === true,
      note: geo.note || null,
    },
    projectTravel: {
      needsExtendedStay,
      needsRooms,
      needsMeeting,
      nonlocalPersonnel,
      fabricatedRoomNightEstimate: false,
      fabricatedConfirmedStay: false,
    },
    facilities: {
      capability: cap,
      facilitySupport,
      notes: facilityNotes,
      inventedInventory: false,
    },
    timing: {
      durationKnown: Boolean(opportunity.projectDuration || opportunity.eventDate),
      projectDuration: opportunity.projectDuration || null,
      note: opportunity.projectDuration
        ? "Duration present in evidence"
        : "Duration unknown — do not invent room-night totals",
    },
    uncertaintyPreserved: cap.incomplete || fitClass === CPE_HOTEL_FIT_CLASS.INCOMPLETE_HOTEL_DATA,
    note: "Evidence vs inference preserved. No fabricated room-night estimates or confirmed accommodation commitments.",
  };
}
