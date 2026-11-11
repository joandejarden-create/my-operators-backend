/**
 * GDI 2.0 Phase 3A — Corporate Meetings & Training hotel-specific matching.
 *
 * Uses hotel config capability/territory data only. Never invents facilities
 * or availability. Preserves uncertainty when hotel data is incomplete.
 */

import { assessHotelGeographicFit } from "./commercial-spine-restoration-v41.js";
import {
  CORPORATE_MEETINGS_TRAINING_V1,
  CMT_RESEARCH_SEGMENT,
} from "./corporate-meetings-training-constants-v1.js";

export const CMT_HOTEL_MATCH_V1 = "gdi-corporate-meetings-training-hotel-match-v1";

export const CMT_HOTEL_FIT_CLASS = Object.freeze({
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
    theaterCap: Number(cap.largestTheaterCapacity || 0) || null,
    serviceLevel: text(cap.serviceLevel || hotel.serviceLevel),
    softBrand: text(cap.softBrand || hotel.brand),
    incomplete:
      !Number(cap.totalGuestrooms || hotel.rooms || 0) &&
      !Number(cap.totalMeetingSpaceSqFt || 0),
  };
}

function activityBlob(opportunity = {}) {
  return [
    opportunity.activityHeadline,
    opportunity.eventName,
    opportunity.whatIsHappening,
    opportunity.hotelDemandThesis,
    opportunity.proposedHotelUse,
    opportunity.location,
    opportunity.city,
    opportunity.evidenceQuote,
  ]
    .map(text)
    .filter(Boolean)
    .join(" — ");
}

/**
 * Match CMT demand to a particular hotel without inventing inventory.
 */
export function assessCmtHotelSpecificMatch(opportunity = {}, hotel = {}, opts = {}) {
  const city = hotelCity(hotel);
  const cap = hotelCapability(hotel);
  const blob = activityBlob(opportunity);
  const displayName = text(hotel.displayName || hotel.name || hotel.hotelId);

  const geo = assessHotelGeographicFit({
    targetHotel: displayName,
    targetCity: city,
    evidenceBlob: blob,
    organizationLocation: opportunity.organizationLocation || opportunity.hqCity || null,
    eventDestination: opportunity.eventCity || opportunity.activityCity || null,
    nationalOrganization: Boolean(opportunity.nationalOrganization),
  });

  const needsMeeting =
    /\b(meeting|training|academy|workshop|kickoff|conference|retreat|offsite|ballroom|catering)\b/i.test(
      blob,
    );
  const needsRooms =
    /\b(guestroom|room.?block|overnight|lodging|housing|accommodation|executive accommodation)\b/i.test(
      blob,
    );
  const needsCatering = /\b(catering|banquet|f&b|food.?and.?beverage)\b/i.test(blob);

  const facilityNotes = [];
  let facilitySupport = "unknown";

  if (cap.incomplete) {
    facilityNotes.push("hotel_capability_profile_incomplete");
  } else {
    if (needsMeeting) {
      if (cap.meetingSqFt && cap.meetingSqFt >= 500) {
        facilitySupport = "supported_by_profile";
        facilityNotes.push(`meeting_space_sqft_${cap.meetingSqFt}`);
      } else if (cap.meetingRooms && cap.meetingRooms >= 1) {
        facilitySupport = "supported_by_profile";
        facilityNotes.push(`meeting_rooms_${cap.meetingRooms}`);
      } else {
        facilitySupport = "profile_lacks_meeting_inventory";
        facilityNotes.push("meeting_demand_but_no_meeting_inventory_in_profile");
      }
    }
    if (needsRooms) {
      if (cap.guestrooms && cap.guestrooms >= 20) {
        facilityNotes.push(`guestrooms_${cap.guestrooms}`);
      } else if (!cap.guestrooms) {
        facilityNotes.push("guestroom_count_unknown");
      }
    }
    if (needsCatering && !/full_service|lifestyle|luxury/i.test(cap.serviceLevel)) {
      facilityNotes.push("fnb_capability_not_explicit_in_profile");
    }
  }

  const territoryHit =
    city &&
    blob &&
    (new RegExp(city.split(/\s+/)[0], "i").test(blob) ||
      (hotel.demandTerritory?.includes || []).some((t) =>
        new RegExp(String(t).split(/\s+where\b/)[0].slice(0, 12), "i").test(blob),
      ));

  let fitClass = CMT_HOTEL_FIT_CLASS.WEAK_OR_UNCERTAIN;
  if (cap.incomplete && !territoryHit && !geo.eventQualifiedForTargetHotel) {
    fitClass = CMT_HOTEL_FIT_CLASS.INCOMPLETE_HOTEL_DATA;
  } else if (geo.geoFitClass === "NAMED_COMPETITOR_HOST_ELSEWHERE") {
    fitClass = CMT_HOTEL_FIT_CLASS.NO_MEANINGFUL_FIT;
  } else if (
    geo.eventQualifiedForTargetHotel &&
    facilitySupport !== "profile_lacks_meeting_inventory"
  ) {
    fitClass = CMT_HOTEL_FIT_CLASS.STRONG_MATCH;
  } else if (territoryHit || geo.organizationStillAccountEligible) {
    fitClass =
      facilitySupport === "profile_lacks_meeting_inventory"
        ? CMT_HOTEL_FIT_CLASS.WEAK_OR_UNCERTAIN
        : CMT_HOTEL_FIT_CLASS.PLAUSIBLE_MATCH;
  } else if (!city) {
    fitClass = CMT_HOTEL_FIT_CLASS.INCOMPLETE_HOTEL_DATA;
  } else {
    fitClass = CMT_HOTEL_FIT_CLASS.NO_MEANINGFUL_FIT;
  }

  const commerciallyAccessible =
    fitClass === CMT_HOTEL_FIT_CLASS.STRONG_MATCH ||
    fitClass === CMT_HOTEL_FIT_CLASS.PLAUSIBLE_MATCH;

  return {
    version: CMT_HOTEL_MATCH_V1,
    parentVersion: CORPORATE_MEETINGS_TRAINING_V1,
    researchSegment: CMT_RESEARCH_SEGMENT,
    hotelId: hotel.hotelId || opts.hotelId || null,
    hotelDisplayName: displayName || null,
    fitClass,
    commerciallyAccessible,
    geographic: {
      city: city || null,
      geoFitClass: geo.geoFitClass,
      eventQualifiedForTargetHotel: geo.eventQualifiedForTargetHotel === true,
      note: geo.note || null,
    },
    facilities: {
      capability: cap,
      facilitySupport,
      needsMeeting,
      needsRooms,
      needsCatering,
      notes: facilityNotes,
      inventedInventory: false,
    },
    timing: {
      leadTimeKnown: Boolean(opportunity.eventDate || opportunity.activityDate),
      eventDate: opportunity.eventDate || opportunity.activityDate || null,
      note: opportunity.eventDate
        ? "Timing present in opportunity evidence"
        : "Timing unknown — do not invent dates",
    },
    uncertaintyPreserved: cap.incomplete || fitClass === CMT_HOTEL_FIT_CLASS.INCOMPLETE_HOTEL_DATA,
    note: "Does not claim availability. Hotel claims require supporting property information.",
  };
}
