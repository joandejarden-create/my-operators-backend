/**
 * GDI Qualification Precision — venue/sourcing gate, opportunity type,
 * room-demand vs attendance, event geography, contact quality, and
 * Opportunity Qualification (separate from Hotel Fit).
 *
 * Finding an event ≠ finding a winnable hotel sales opportunity.
 * No Webhound spend in this module.
 */

import {
  BOOKING_WINDOW,
  CLAIM_KIND,
  CONTACT_QUALITY,
  CONTACT_QUALITY_LABEL,
  EVENT_LOCATION_STATUS,
  EVENT_LOCATION_STATUS_LABEL,
  OPPORTUNITY_QUALIFICATION,
  OPPORTUNITY_QUALIFICATION_LABEL,
  OPPORTUNITY_TYPE,
  OPPORTUNITY_TYPE_LABEL,
  QUALIFICATION_FAILURE_REASON,
  REACTIVATION_SIGNAL,
  REACTIVATION_SIGNAL_LABEL,
  ROOM_DEMAND_STATUS,
  ROOM_DEMAND_STATUS_LABEL,
  VENUE_SOURCING_STATUS,
  VENUE_SOURCING_STATUS_LABEL,
} from "./claim-types.js";
import { isCoreTerritory } from "./demand-territory.js";
import {
  CAPTURE_CAPACITY_STATE,
  calculateHotelGroupCaptureCapacity,
  hotelContextFromProfile,
} from "./hotel-capture-capacity.js";
import {
  detectFutureCycleEvidence,
  shouldPreservePlacedVenueAsFutureCycleWatch,
  FUTURE_CYCLE_EVIDENCE_STATE,
} from "./future-cycle-evidence.js";

const UNKNOWN_TOKEN = "UNKNOWN";

function blob(o) {
  return [
    o.title,
    o.destinationStatus,
    o.venueStatus,
    o.summaryWhat,
    o.summaryWhyMatters,
    o.summaryWhyHotel,
    o.bethesdaWinThesis,
    o.hotelOpportunityThesis,
    o.whyNow,
    o.organizationName,
    o.segment,
    o.demandType,
    ...(o.labels || []),
    ...(o.meetingHistory || []).map((h) => `${h.city || ""} ${h.venue || ""}`),
    ...(o.evidence || []).map((e) => `${e.field || ""} ${e.value || ""} ${e.extractedText || ""}`),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function isUnknown(v) {
  return v == null || v === "" || v === UNKNOWN_TOKEN || v === CLAIM_KIND.UNKNOWN;
}

function hasNamedPerson(contact) {
  if (!contact) return false;
  const name = String(contact.name || "").trim();
  if (!name || /^unknown$/i.test(name) || /^tbd$/i.test(name)) return false;
  if (
    /program office|staff|organizers|meetings contact|association|conference ops|translational science/i.test(
      name
    )
  ) {
    return false;
  }
  if (/^(nist|fiu|acts|amwa|office|team|committee)\b/i.test(name)) return false;
  // Require First Last pattern
  if (!/^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(name) && !/^[A-Z][a-z]+\s+[A-Z]\.\s*[A-Z][a-z]+/.test(name)) {
    // Allow mixed case names with 2+ tokens that look personal
    const tokens = name.split(/\s+/).filter(Boolean);
    if (tokens.length < 2) return false;
    if (tokens.some((t) => /office|staff|program|meetings|events|association|committee/i.test(t))) {
      return false;
    }
  }
  return /[A-Za-z]{2,}/.test(name);
}

function emailLooksGeneric(email) {
  const e = String(email || "").toLowerCase();
  if (!e) return false;
  return /^(info|contact|hello|admin|office|support|help|sales|events|meetings|conference|registration|housing|tournament)@/.test(
    e
  );
}

/**
 * Event geography must beat organization name/HQ.
 * Priority: confirmed venue → city → facility → published geography → historical → org HQ (weak).
 */
export function classifyEventLocation(opportunity = {}) {
  const dest = String(opportunity.destinationStatus || "").trim();
  const venue = String(opportunity.venueStatus || "").trim();
  const t = blob(opportunity);
  const hist = Array.isArray(opportunity.meetingHistory) ? opportunity.meetingHistory : [];

  const namedVenue =
    venue &&
    !/not.?announced|not named|tbd|tba|unresolved|unknown|fields-based|hotel\/venue not/i.test(venue) &&
    /hotel|marriott|hilton|hyatt|convention|center|arena|stadium|plex|campus|gaylord|ritz|renaissance|resort|marina/i.test(
      venue
    );

  if (namedVenue || /verified venue|official venue|host hotel[:\s]/i.test(t)) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.VERIFIED_VENUE,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.VERIFIED_VENUE,
      eventLocationSummary: venue || dest || "Named venue",
      eventLocationSource: "venue_status",
      organizationLocationIsWeakSignalOnly: false,
    };
  }

  // Generic city / metro pattern (portable — not Bethesda-only)
  const genericCity =
    dest.match(/\b([A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){0,3}),\s*[A-Z]{2}\b/) ||
    dest.match(
      /\b(New York|Times Square|Manhattan|Midtown|Boca Raton|Palm Beach|West Palm|Delray Beach|Fort Lauderdale|Miami|Orlando|Tampa|Atlanta|Chicago|Boston|Dallas|Houston|Los Angeles|San Francisco|Seattle|Denver|Phoenix|Philadelphia|Las Vegas|Nashville|Austin|San Diego)\b/i
    );

  // Legacy DMV city recognition (still valid; not exclusive)
  const dmvCityMatch =
    dest.match(
      /\b(Bethesda|Rockville|Gaithersburg|Silver Spring|Washington|Arlington|Alexandria|McLean|Tysons|College Park|Upper Marlboro|National Harbor|Baltimore|Fairfax|Reston)\b/i
    ) ||
    venue.match(
      /\b(Bethesda|Rockville|Gaithersburg|Silver Spring|Washington|Arlington|Alexandria|McLean|Tysons|College Park|Upper Marlboro|National Harbor|Baltimore|Fairfax|Reston)\b/i
    );

  const cityMatch = genericCity || dmvCityMatch;

  if (cityMatch && !/area|region|tbd|to be announced/i.test(dest || "")) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.VERIFIED_CITY,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.VERIFIED_CITY,
      eventLocationSummary: dest || cityMatch[0],
      eventLocationSource: "destination_status",
      organizationLocationIsWeakSignalOnly: false,
    };
  }

  if (
    /montgomery county|soccerplex|maryland soccer|prince george|fairfax county|loudoun|palm beach county|broward|manhattan|midtown|times square/i.test(
      `${dest} ${venue} ${t}`
    )
  ) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.VERIFIED_CITY,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.VERIFIED_CITY,
      eventLocationSummary: dest || venue || "Confirmed county / facility geography",
      eventLocationSource: "destination_status",
      organizationLocationIsWeakSignalOnly: false,
    };
  }

  if (
    /washington,\s*dc area|dmv|metro area|national capital|south florida|tri-state|to be announced|location tbd/i.test(
      dest || t
    )
  ) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.REGION_ONLY,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.REGION_ONLY,
      eventLocationSummary: dest || "Regional destination only",
      eventLocationSource: "published_region",
      organizationLocationIsWeakSignalOnly: true,
    };
  }

  if (hist.length && hist.some((h) => h.city)) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.ESTIMATED,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.ESTIMATED,
      eventLocationSummary: `Historical pattern: ${hist
        .map((h) => h.city)
        .filter(Boolean)
        .slice(0, 3)
        .join(", ")}`,
      eventLocationSource: "historical_event_location",
      organizationLocationIsWeakSignalOnly: true,
    };
  }

  // Title / thesis city cues (portable — not market-specific branching)
  const titleBlob = `${opportunity.title || ""} ${opportunity.hotelOpportunityThesis || ""} ${opportunity.summaryWhyHotel || ""}`;
  const titleCity = titleBlob.match(
    /\b(Boca(?:\s+Raton)?|West Palm(?:\s+Beach)?|Palm Beach|Delray(?:\s+Beach)?|Deerfield(?:\s+Beach)?|Fort Lauderdale|Times Square|Manhattan|Midtown|Bethesda|Rockville|Gaithersburg|Silver Spring|National Harbor|Arlington|Tysons)\b/i
  );
  if (titleCity) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.ESTIMATED,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.ESTIMATED,
      eventLocationSummary: titleCity[0],
      eventLocationSource: "title_or_thesis_geography",
      organizationLocationIsWeakSignalOnly: false,
    };
  }

  // Territory-locked CORE/NEARBY with a non-empty destination string → estimated geography
  if (
    dest &&
    dest.length > 2 &&
    !/tbd|tba|unknown|not announced/i.test(dest) &&
    isCoreTerritory(opportunity.demandTerritoryFit)
  ) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.ESTIMATED,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.ESTIMATED,
      eventLocationSummary: dest,
      eventLocationSource: "destination_with_core_territory",
      organizationLocationIsWeakSignalOnly: false,
    };
  }

  const org = String(opportunity.organizationName || "");
  if (/bethesda|rockville|montgomery|boca|palm beach|manhattan/i.test(org) && !cityMatch) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.UNKNOWN,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.UNKNOWN,
      eventLocationSummary:
        "Organization name implies local presence; event geography not confirmed — do not treat as strong geographic fit",
      eventLocationSource: "organization_name_weak_only",
      organizationLocationIsWeakSignalOnly: true,
    };
  }

  if (dest && dest.length > 2 && !/tbd|tba|unknown/i.test(dest)) {
    return {
      eventLocationStatus: EVENT_LOCATION_STATUS.ESTIMATED,
      eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.ESTIMATED,
      eventLocationSummary: dest,
      eventLocationSource: "destination_status_unverified",
      organizationLocationIsWeakSignalOnly: true,
    };
  }

  return {
    eventLocationStatus: EVENT_LOCATION_STATUS.UNKNOWN,
    eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.UNKNOWN,
    eventLocationSummary: "Event geography not confirmed",
    eventLocationSource: "none",
    organizationLocationIsWeakSignalOnly: true,
  };
}

/**
 * Infer venue/housing status from existing evidence text.
 * Method concept: verify_event_venue_and_housing_status (see research-methods).
 */
export function classifyVenueSourcingStatus(opportunity = {}) {
  if (opportunity.venueSourcingStatus && Object.values(VENUE_SOURCING_STATUS).includes(opportunity.venueSourcingStatus)) {
    return {
      venueSourcingStatus: opportunity.venueSourcingStatus,
      venueSourcingStatusLabel:
        VENUE_SOURCING_STATUS_LABEL[opportunity.venueSourcingStatus] || opportunity.venueSourcingStatus,
      venueSourcingRationale: opportunity.venueSourcingRationale || "Curated / prior classification retained.",
      venueVerificationMethodId: opportunity.venueVerificationMethodId || "GDI-VENUE-HOUSING-01",
    };
  }

  const t = blob(opportunity);
  const venue = String(opportunity.venueStatus || "").toLowerCase();
  const labels = (opportunity.labels || []).map((x) => String(x).toLowerCase());

  if (/already contracted|fully booked|host hotel announced|official host hotel|sold out housing/i.test(t)) {
    // Require affirmative overflow evidence — "no overflow" must not match.
    if (
      /(?:^|[^a-z])overflow(?:[^a-z]|$)|housing list|secondary lodging|room block bid|stay-to-play|hbc event|onpeak|passtkey/i.test(
        t
      ) &&
      !/\bno overflow\b|\bno housing\b|without overflow|no overflow evidence/i.test(t)
    ) {
      return {
        venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
        venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
        venueSourcingRationale:
          "Primary venue/host appears selected, but housing / overflow / stay-to-play evidence remains.",
        venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
      };
    }
    return {
      venueSourcingStatus: VENUE_SOURCING_STATUS.FULLY_PLACED,
      venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.FULLY_PLACED,
      venueSourcingRationale: "Evidence indicates the primary hotel/venue is already placed.",
      venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.VENUE_ALREADY_SELECTED,
    };
  }

  if (
    /fields-based|soccerplex|tournament fields|stay-to-play|official housing partner|housing via|hbc event/i.test(
      t
    ) ||
    labels.includes("overflow") ||
    labels.includes("overflow_only") ||
    /overflow/i.test(String(opportunity.demandType || ""))
  ) {
    return {
      venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
      venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
      venueSourcingRationale:
        "Primary program is field/venue-based or host path differs; hotel path is housing / overflow / stay-to-play.",
      venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
    };
  }

  if (
    /not.?announced|not named|hotel\/venue not|location to be announced|venue tbd|hotel tbd|unresolved/i.test(
      `${venue} ${t}`
    )
  ) {
    if (/rfp|sourcing|bid|hotel selection|call for hotels/i.test(t)) {
      return {
        venueSourcingStatus: VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING,
        venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.RFP_ACTIVE_SOURCING,
        venueSourcingRationale: "Hotel/venue not named and public sourcing / RFP / housing process signals exist.",
        venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
      };
    }
    return {
      venueSourcingStatus: VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
      venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.HOTEL_VENUE_TBD,
      venueSourcingRationale: "Official materials leave hotel/venue unnamed or TBA.",
      venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
    };
  }

  if (/partial(?:ly)? placed|destination selected|city confirmed.*hotel/i.test(t)) {
    return {
      venueSourcingStatus: VENUE_SOURCING_STATUS.PARTIALLY_PLACED,
      venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.PARTIALLY_PLACED,
      venueSourcingRationale: "Destination or part of the program is set; hotel path may still be open.",
      venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
    };
  }

  if (/too early|future cycle|watch only|dates not set/i.test(t) && /not.?announced|tbd/i.test(venue)) {
    return {
      venueSourcingStatus: VENUE_SOURCING_STATUS.OPEN_UNRESOLVED,
      venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.OPEN_UNRESOLVED,
      venueSourcingRationale: "Cycle appears open but early — monitor rather than assume active sourcing.",
      venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
    };
  }

  if (/gaylord|washington hilton|already booked elsewhere|convention center host/i.test(t)) {
    return {
      venueSourcingStatus: VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
      venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
      venueSourcingRationale: "Named primary venue elsewhere without overflow/housing evidence for this hotel.",
      venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.NO_OVERFLOW_OPPORTUNITY,
    };
  }

  return {
    venueSourcingStatus: VENUE_SOURCING_STATUS.UNKNOWN,
    venueSourcingStatusLabel: VENUE_SOURCING_STATUS_LABEL.UNKNOWN,
    venueSourcingRationale:
      "Insufficient public evidence to confirm whether hotel/venue selection is open — treat as unresolved pending verification.",
    venueVerificationMethodId: "GDI-VENUE-HOUSING-01",
  };
}

export function classifyRoomDemand(opportunity = {}) {
  const t = blob(opportunity);
  const publishedPeakRooms = opportunity.publishedPeakRooms ?? null;
  const publishedRoomNights = opportunity.publishedRoomNights ?? null;
  const publishedAttendance = opportunity.publishedAttendance ?? null;
  const estimatedPeakRooms = isUnknown(opportunity.estimatedPeakRooms)
    ? null
    : opportunity.estimatedPeakRooms;
  const estimatedRoomNights = isUnknown(opportunity.estimatedRoomNights)
    ? null
    : opportunity.estimatedRoomNights;
  const estimatedAttendance = isUnknown(opportunity.estimatedAttendance)
    ? null
    : opportunity.estimatedAttendance;

  let roomDemandStatus = ROOM_DEMAND_STATUS.UNKNOWN;
  let roomDemandConfidence = 25;
  let rationale = "Room demand not published; attendance alone is not treated as guestrooms demand.";

  if (/official housing|housing partner|room block|passtkey|onpeak|hbc|stay-to-play/i.test(t)) {
    roomDemandStatus = ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM;
    roomDemandConfidence = 80;
    rationale =
      "Verified housing / stay-to-play / room-block program evidence — guestrooms path exists independent of attendance headline.";
  } else if (publishedPeakRooms != null || /verified room block|contracted peak rooms/i.test(t)) {
    roomDemandStatus = ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK;
    roomDemandConfidence = 90;
    rationale = "Published or verified peak-room / room-block figure.";
  } else if (
    (/(?:^|[^a-z])overflow(?:[^a-z]|$)|secondary lodging|housing list/i.test(t) ||
      (opportunity.labels || []).some((l) => /overflow/i.test(String(l)))) &&
    !/\bno overflow\b|without overflow|no overflow evidence/i.test(t)
  ) {
    roomDemandStatus = ROOM_DEMAND_STATUS.OVERFLOW_ONLY;
    roomDemandConfidence = 65;
    rationale = "Opportunity is overflow / housing-list participation, not primary host block.";
  } else if (estimatedPeakRooms != null && opportunity.estimatedPeakRoomsClaimKind === CLAIM_KIND.FACT) {
    roomDemandStatus = ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE;
    roomDemandConfidence = 70;
    rationale = "Peak-room estimate backed by factual evidence rows.";
  } else if (estimatedPeakRooms != null) {
    roomDemandStatus = ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND;
    roomDemandConfidence = 45;
    rationale =
      "Peak rooms estimated from event structure — not derived from attendance alone; validate before aggressive pursuit.";
  } else if (/local attendees|commuter|day meeting|mostly local|no overnight/i.test(t)) {
    roomDemandStatus = ROOM_DEMAND_STATUS.LOCAL_LIMITED_ROOM_DEMAND;
    roomDemandConfidence = 55;
    rationale = "Evidence suggests limited overnight demand despite possible attendance.";
    return {
      publishedAttendance,
      estimatedAttendance,
      publishedPeakRooms,
      estimatedPeakRooms,
      publishedRoomNights,
      estimatedRoomNights,
      roomDemandStatus,
      roomDemandStatusLabel: ROOM_DEMAND_STATUS_LABEL[roomDemandStatus],
      roomDemandConfidence,
      roomDemandRationale: rationale,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.MOST_ATTENDEES_LOCAL,
    };
  } else if (
    estimatedAttendance != null &&
    /conference|expo|annual meeting|symposium|summit|multi-?day|jun |apr |mar |three-day|2-day|3-day/i.test(
      t
    ) &&
    !/day meeting only|commuter only|local only/i.test(t)
  ) {
    roomDemandStatus = ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND;
    roomDemandConfidence = 40;
    rationale =
      "Multi-day conference/meeting structure supports cautious overnight room-demand estimate; attendance is context only — not a room-block proof.";
  } else if (estimatedAttendance != null && estimatedPeakRooms == null) {
    roomDemandStatus = ROOM_DEMAND_STATUS.UNKNOWN;
    roomDemandConfidence = 20;
    rationale =
      "Attendance known but peak rooms not evidenced — do not assume a large guestrooms opportunity from headcount.";
  }

  return {
    publishedAttendance,
    estimatedAttendance,
    publishedPeakRooms,
    estimatedPeakRooms,
    publishedRoomNights,
    estimatedRoomNights,
    roomDemandStatus,
    roomDemandStatusLabel: ROOM_DEMAND_STATUS_LABEL[roomDemandStatus],
    roomDemandConfidence,
    roomDemandRationale: rationale,
  };
}

export function classifyContactQuality(opportunity = {}) {
  const contact = opportunity.primaryContact || (opportunity.contacts || [])[0] || null;
  if (!contact || (!contact.name && !contact.email && !contact.role)) {
    return {
      contactQuality: CONTACT_QUALITY.NO_CONTACT,
      contactQualityLabel: CONTACT_QUALITY_LABEL.NO_CONTACT,
      contactRole: null,
      relationshipToEvent: null,
      relationshipConfidence: "LOW",
      contactSource: null,
      contactSourceDate: null,
      contactabilityAdjustment: -25,
    };
  }

  const role = String(contact.role || "").toLowerCase();
  const name = String(contact.name || "");
  const email = String(contact.email || "");
  const named = hasNamedPerson(contact);
  const generic = emailLooksGeneric(email) || /associatedirector@|info@|nice@nist\.gov/i.test(email);

  let contactQuality = CONTACT_QUALITY.GENERAL_ORGANIZATION_CONTACT;
  let relationshipToEvent = "Organization-linked contact";
  let relationshipConfidence = "MEDIUM";
  let contactabilityAdjustment = 0;

  if (!named && (generic || !email)) {
    contactQuality = CONTACT_QUALITY.GENERIC_INBOX;
    relationshipToEvent = "Generic organization inbox / unnamed staff";
    relationshipConfidence = "LOW";
    contactabilityAdjustment = -20;
  } else if (
    /director of meetings|vp.{0,20}meetings|meeting planner|event director|tournament director|vp.{0,24}business development|business development|director of sales|vp.{0,20}sales/i.test(
      role
    )
  ) {
    contactQuality = named
      ? CONTACT_QUALITY.NAMED_DECISION_MAKER
      : CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT;
    relationshipToEvent = "Official planning / meetings / commercial lead";
    relationshipConfidence = "HIGH";
    contactabilityAdjustment = named ? 10 : 0;
  } else if (
    /housing|sourcing|hbc|room block|passtkey|team travel source|travel source|onpeak|official housing/i.test(
      role + name + email
    ) ||
    contact.functionalEntity === true
  ) {
    contactQuality = CONTACT_QUALITY.HOUSING_SOURCING_CONTACT;
    relationshipToEvent = "Housing / sourcing path";
    relationshipConfidence = "HIGH";
    contactabilityAdjustment = 5;
  } else if (/meetings|events|conference|program/i.test(role)) {
    contactQuality = named
      ? CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT
      : CONTACT_QUALITY.ASSOCIATION_MANAGEMENT_CONTACT;
    relationshipToEvent = "Event / meetings operations contact";
    relationshipConfidence = named ? "HIGH" : "MEDIUM";
    contactabilityAdjustment = named ? 5 : -5;
  } else if (generic) {
    contactQuality = CONTACT_QUALITY.GENERIC_INBOX;
    relationshipToEvent = "Generic inbox — may route, not a decision maker";
    relationshipConfidence = "LOW";
    contactabilityAdjustment = -15;
  } else if (!named) {
    contactQuality = CONTACT_QUALITY.ASSOCIATION_MANAGEMENT_CONTACT;
    relationshipToEvent = "Association / program desk without named individual";
    relationshipConfidence = "MEDIUM";
    contactabilityAdjustment = -10;
  }

  return {
    contactQuality,
    contactQualityLabel: CONTACT_QUALITY_LABEL[contactQuality],
    contactRole: contact.role || null,
    relationshipToEvent,
    relationshipConfidence,
    contactSource: contact.sourceUrl || null,
    contactSourceDate: contact.accessDate || contact.sourceDate || null,
    contactabilityAdjustment,
    primaryContactEnriched: {
      ...contact,
      contactRole: contact.role || null,
      relationshipToEvent,
      relationshipConfidence,
      contactQuality,
      contactQualityLabel: CONTACT_QUALITY_LABEL[contactQuality],
      source: contact.sourceUrl || null,
      sourceDate: contact.accessDate || contact.sourceDate || null,
    },
  };
}

export function classifyReactivationSignal(opportunity = {}) {
  if (opportunity.reactivationSignal && Object.values(REACTIVATION_SIGNAL).includes(opportunity.reactivationSignal)) {
    return {
      reactivationSignal: opportunity.reactivationSignal,
      reactivationSignalLabel: REACTIVATION_SIGNAL_LABEL[opportunity.reactivationSignal],
      reactivationThesis: opportunity.reactivationThesis || null,
    };
  }
  const t = blob(opportunity);
  const hist = opportunity.meetingHistory || [];
  if (/prior marriott|marriott previously|ci\/ty|historical marriott|lapsed/i.test(t)) {
    return {
      reactivationSignal: REACTIVATION_SIGNAL.PRIOR_MARRIOTT_OPPORTUNITY,
      reactivationSignalLabel: REACTIVATION_SIGNAL_LABEL.PRIOR_MARRIOTT_OPPORTUNITY,
      reactivationThesis:
        opportunity.reactivationThesis ||
        "Evidence of prior Marriott / brand interaction — evaluate as reactivation, not only net-new discovery.",
    };
  }
  if (/prior hotel opportunity|previously pursued|already pursued/i.test(t)) {
    return {
      reactivationSignal: REACTIVATION_SIGNAL.PRIOR_HOTEL_OPPORTUNITY,
      reactivationSignalLabel: REACTIVATION_SIGNAL_LABEL.PRIOR_HOTEL_OPPORTUNITY,
      reactivationThesis:
        opportunity.reactivationThesis ||
        "Prior hotel pursuit history may justify re-engagement for the current or next cycle.",
    };
  }
  if (hist.length >= 2) {
    return {
      reactivationSignal: REACTIVATION_SIGNAL.PRIOR_MARKET_PRESENCE,
      reactivationSignalLabel: REACTIVATION_SIGNAL_LABEL.PRIOR_MARKET_PRESENCE,
      reactivationThesis:
        "Recurring meeting history in-market — relationship reactivation may matter even when the lead is familiar.",
    };
  }
  if (/competitor hosted|gaylord|hilton hosted|hyatt hosted/i.test(t)) {
    return {
      reactivationSignal: REACTIVATION_SIGNAL.PRIOR_COMPETITOR_USAGE,
      reactivationSignalLabel: REACTIVATION_SIGNAL_LABEL.PRIOR_COMPETITOR_USAGE,
      reactivationThesis: "Prior competitor hosting may open a future-cycle or overflow conversation.",
    };
  }
  return {
    reactivationSignal: REACTIVATION_SIGNAL.UNKNOWN,
    reactivationSignalLabel: REACTIVATION_SIGNAL_LABEL.UNKNOWN,
    reactivationThesis: null,
  };
}

export function deriveOpportunityType({
  venueSourcingStatus,
  roomDemandStatus,
  reactivationSignal,
  bookingWindowStatus,
  opportunity = {},
} = {}) {
  const futureCycleGate = shouldPreservePlacedVenueAsFutureCycleWatch({
    opportunity: {
      ...opportunity,
      venueSourcingStatus:
        venueSourcingStatus || opportunity.venueSourcingStatus || null,
    },
    reactivationSignal,
    bookingWindowStatus,
  });

  if (
    venueSourcingStatus === VENUE_SOURCING_STATUS.FULLY_PLACED ||
    venueSourcingStatus === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE
  ) {
    // Overflow room-demand thesis wins over sticky no-overflow venue labels
    // when research claims housing/overflow path (provider-agnostic).
    if (
      roomDemandStatus === ROOM_DEMAND_STATUS.OVERFLOW_ONLY ||
      roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM ||
      /overflow|housing|stay-to-play/i.test(String(opportunity.demandType || "")) ||
      (opportunity.labels || []).some((l) => /overflow/i.test(String(l)))
    ) {
      // Still future-cycle watch when next cycle is the thesis, not current open overflow
      if (futureCycleGate.preserve) {
        return {
          opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
          opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.FUTURE_CYCLE,
          futureCycleEvidenceState: futureCycleGate.evidence.state,
        };
      }
      return {
        opportunityType: OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
        opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.OVERFLOW_HOUSING,
      };
    }

    if (futureCycleGate.preserve) {
      return {
        opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
        opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.FUTURE_CYCLE,
        futureCycleEvidenceState: futureCycleGate.evidence.state,
      };
    }
    return {
      opportunityType: OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED,
      opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.CLOSED_DISQUALIFIED,
    };
  }

  if (
    venueSourcingStatus === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE ||
    roomDemandStatus === ROOM_DEMAND_STATUS.OVERFLOW_ONLY ||
    roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM ||
    /overflow|housing|stay-to-play/i.test(String(opportunity.demandType || "")) ||
    (opportunity.labels || []).some((l) => /overflow/i.test(String(l)))
  ) {
    return {
      opportunityType: OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
      opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.OVERFLOW_HOUSING,
    };
  }

  if (
    reactivationSignal === REACTIVATION_SIGNAL.PRIOR_HOTEL_OPPORTUNITY ||
    reactivationSignal === REACTIVATION_SIGNAL.PRIOR_MARRIOTT_OPPORTUNITY ||
    reactivationSignal === REACTIVATION_SIGNAL.LAPSED_RELATIONSHIP
  ) {
    if (
      venueSourcingStatus === VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD ||
      venueSourcingStatus === VENUE_SOURCING_STATUS.OPEN_UNRESOLVED ||
      venueSourcingStatus === VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING
    ) {
      return {
        opportunityType: OPPORTUNITY_TYPE.REACTIVATION,
        opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.REACTIVATION,
      };
    }
  }

  if (
    bookingWindowStatus === BOOKING_WINDOW.TOO_EARLY ||
    venueSourcingStatus === VENUE_SOURCING_STATUS.CURRENT_CYCLE_CLOSED
  ) {
    return {
      opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
      opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.FUTURE_CYCLE,
      futureCycleEvidenceState: futureCycleGate.evidence?.state || null,
    };
  }

  if (
    venueSourcingStatus === VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD ||
    venueSourcingStatus === VENUE_SOURCING_STATUS.OPEN_UNRESOLVED ||
    venueSourcingStatus === VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING ||
    venueSourcingStatus === VENUE_SOURCING_STATUS.PARTIALLY_PLACED
  ) {
    return {
      opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.PRIMARY_PURSUIT,
    };
  }

  return {
    opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
    opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.FUTURE_CYCLE,
  };
}

export function buildHotelOpportunityThesis(opportunity = {}, ctx = {}) {
  if (opportunity.hotelOpportunityThesis && String(opportunity.hotelOpportunityThesis).trim().length > 40) {
    return String(opportunity.hotelOpportunityThesis).trim();
  }
  if (opportunity.bethesdaWinThesis && String(opportunity.bethesdaWinThesis).trim().length > 40) {
    return String(opportunity.bethesdaWinThesis).trim();
  }

  const type = ctx.opportunityType || opportunity.opportunityType;
  const venue = ctx.venueSourcingStatus || opportunity.venueSourcingStatus;
  const rooms = ctx.roomDemandStatus || opportunity.roomDemandStatus;
  const parts = [];

  if (type === OPPORTUNITY_TYPE.OVERFLOW_HOUSING) {
    parts.push(
      "Hotel path is overflow / housing / stay-to-play rather than winning the primary host venue"
    );
  } else if (type === OPPORTUNITY_TYPE.PRIMARY_PURSUIT) {
    parts.push("Primary hotel/venue still appears unresolved or actively sourced");
  } else if (type === OPPORTUNITY_TYPE.REACTIVATION) {
    parts.push("Credible reactivation of a prior hotel / Marriott / market relationship");
  } else if (type === OPPORTUNITY_TYPE.FUTURE_CYCLE) {
    parts.push("Not a current primary sell — monitor for a future booking cycle");
  }

  if (rooms === ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM || rooms === ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK) {
    parts.push("room demand is evidenced by a housing program or room block");
  } else if (rooms === ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE || rooms === ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND) {
    parts.push("peak-room need is estimated from event structure, not attendance alone");
  } else if (rooms === ROOM_DEMAND_STATUS.UNKNOWN) {
    parts.push("guestrooms demand is not yet evidenced — attendance must not be treated as room nights");
  }

  if (
    venue === VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD ||
    venue === VENUE_SOURCING_STATUS.OPEN_UNRESOLVED ||
    venue === VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING
  ) {
    parts.push("why-now trigger is unresolved venue / sourcing status");
  }

  const sizeBit =
    !isUnknown(opportunity.estimatedPeakRooms)
      ? `${opportunity.estimatedPeakRooms} peak rooms`
      : !isUnknown(opportunity.estimatedAttendance)
        ? `${opportunity.estimatedAttendance} attendees (rooms not verified)`
        : "size still uncertain";

  const base =
    opportunity.summaryWhyHotel ||
    opportunity.fitExplanation ||
    "Bethesda Marriott capacity and DMV positioning may be relevant.";

  return `${parts.join("; ")}. Size signal: ${sizeBit}. ${base}`.slice(0, 600);
}

function recommendedActionForType(opportunity, type) {
  const contact = opportunity.primaryContact?.name || "the identified planner";
  switch (type) {
    case OPPORTUNITY_TYPE.PRIMARY_PURSUIT:
      return `Verify sourcing status is still open, then contact ${contact} to introduce Bethesda Marriott, request RFP/site-visit inclusion, and position NIH / Metro / parking advantages.`;
    case OPPORTUNITY_TYPE.OVERFLOW_HOUSING:
      return `Contact the housing provider / tournament housing lead (not only the event brand), request housing-list inclusion, and confirm overflow peak-room needs and stay dates.`;
    case OPPORTUNITY_TYPE.REACTIVATION:
      return `Reopen the historical relationship, confirm the current decision maker, and reference prior hotel/Marriott interaction where evidence supports it — do not treat as cold discovery only.`;
    case OPPORTUNITY_TYPE.FUTURE_CYCLE:
      return `Do not aggressively sell the current cycle. Monitor venue/housing announcements, capture a future trigger date, and keep a light relationship touch.`;
    case OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED:
      return `No current pursuit. Retain as historical intelligence only if useful for a future cycle.`;
    default:
      return opportunity.recommendedAction || "Validate venue status, then decide pursue vs watch.";
  }
}

/**
 * Opportunity Qualification — separate from Hotel Fit.
 */
export function computeOpportunityQualification(ctx = {}) {
  const {
    venueSourcingStatus,
    opportunityType,
    roomDemandStatus,
    eventLocationStatus,
    contactQuality,
    hotelOpportunityThesis,
    evidenceConfidence = 0,
    demandTerritoryFit,
  } = ctx;

  const failures = [];

  if (venueSourcingStatus === VENUE_SOURCING_STATUS.FULLY_PLACED) {
    // Fully placed still may be future-cycle watch when recurring next cycle is the thesis
    if (
      opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE ||
      opportunityType === OPPORTUNITY_TYPE.REACTIVATION
    ) {
      return {
        opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
        opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.MODERATE,
        qualificationFailureReason: QUALIFICATION_FAILURE_REASON.TOO_EARLY,
        qualificationGatePassed: true,
        qualificationNotes:
          "Current cycle placed — retain as future-cycle / reactivation watch only (not open sourcing).",
      };
    }
    failures.push(QUALIFICATION_FAILURE_REASON.VENUE_ALREADY_SELECTED);
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.CLOSED,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.CLOSED,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.VENUE_ALREADY_SELECTED,
      qualificationGatePassed: false,
      qualificationNotes: "Host hotel/venue appears fully placed — Hotel Fit cannot rescue this.",
    };
  }

  if (venueSourcingStatus === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE) {
    if (
      opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE ||
      opportunityType === OPPORTUNITY_TYPE.REACTIVATION
    ) {
      return {
        opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
        opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.MODERATE,
        qualificationFailureReason: QUALIFICATION_FAILURE_REASON.TOO_EARLY,
        qualificationGatePassed: true,
        qualificationNotes:
          "Primary venue selected / no current overflow — future-cycle watch preservation (not Medium/High).",
      };
    }
    failures.push(QUALIFICATION_FAILURE_REASON.NO_OVERFLOW_OPPORTUNITY);
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.CLOSED,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.CLOSED,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.NO_OVERFLOW_OPPORTUNITY,
      qualificationGatePassed: false,
      qualificationNotes: "Primary venue selected with no overflow evidence.",
    };
  }

  if (demandTerritoryFit === "OUTSIDE_REALISTIC_TERRITORY") {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.CLOSED,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.CLOSED,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.EVENT_LOCATION_POOR_FIT,
      qualificationGatePassed: false,
      qualificationNotes: "Event geography outside realistic territory.",
    };
  }

  if (roomDemandStatus === ROOM_DEMAND_STATUS.LOCAL_LIMITED_ROOM_DEMAND) {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.WEAK,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.WEAK,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.MOST_ATTENDEES_LOCAL,
      qualificationGatePassed: false,
      qualificationNotes: "Attendance may be high but overnight room demand appears limited.",
    };
  }

  if (opportunityType === OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED) {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.CLOSED,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.CLOSED,
      qualificationFailureReason: failures[0] || QUALIFICATION_FAILURE_REASON.OTHER,
      qualificationGatePassed: false,
      qualificationNotes: "Opportunity type closed / disqualified.",
    };
  }

  const openVenue = [
    VENUE_SOURCING_STATUS.OPEN_UNRESOLVED,
    VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING,
    VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
    VENUE_SOURCING_STATUS.PARTIALLY_PLACED,
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
  ].includes(venueSourcingStatus);

  const roomOk = [
    ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK,
    ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM,
    ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE,
    ROOM_DEMAND_STATUS.OVERFLOW_ONLY,
    ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND,
  ].includes(roomDemandStatus);

  const thesisOk = hotelOpportunityThesis && String(hotelOpportunityThesis).length > 40;
  const geoOk = [
    EVENT_LOCATION_STATUS.VERIFIED_VENUE,
    EVENT_LOCATION_STATUS.VERIFIED_CITY,
    EVENT_LOCATION_STATUS.REGION_ONLY,
    EVENT_LOCATION_STATUS.ESTIMATED,
  ].includes(eventLocationStatus);

  const contactOk = contactQuality !== CONTACT_QUALITY.NO_CONTACT;

  if (
    openVenue &&
    roomOk &&
    thesisOk &&
    geoOk &&
    evidenceConfidence >= 55 &&
    (opportunityType === OPPORTUNITY_TYPE.PRIMARY_PURSUIT ||
      opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING)
  ) {
    const verified =
      (venueSourcingStatus === VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD ||
        venueSourcingStatus === VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING ||
        venueSourcingStatus === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE) &&
      (roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM ||
        roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK ||
        roomDemandStatus === ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE);

    return {
      opportunityQualification: verified
        ? OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN
        : OPPORTUNITY_QUALIFICATION.STRONG,
      opportunityQualificationLabel: verified
        ? OPPORTUNITY_QUALIFICATION_LABEL.VERIFIED_OPEN
        : OPPORTUNITY_QUALIFICATION_LABEL.STRONG,
      qualificationFailureReason: null,
      qualificationGatePassed: true,
      qualificationNotes: verified
        ? "Venue/housing path open with strong room-demand evidence."
        : "Credible open path with adequate thesis and evidence.",
    };
  }

  if (opportunityType === OPPORTUNITY_TYPE.REACTIVATION && openVenue) {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.MODERATE,
      qualificationFailureReason: null,
      qualificationGatePassed: true,
      qualificationNotes: "Reactivation path — useful but not automatic High.",
    };
  }

  if (opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE) {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.MODERATE,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.TOO_EARLY,
      qualificationGatePassed: true,
      qualificationNotes: "Future-cycle monitoring — not current High pursuit.",
    };
  }

  if (!roomOk || roomDemandStatus === ROOM_DEMAND_STATUS.UNKNOWN) {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.WEAK,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.WEAK,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.ROOM_DEMAND_TOO_SMALL,
      qualificationGatePassed: false,
      qualificationNotes: "Insufficient room-demand evidence to treat as a hotel sales opportunity.",
    };
  }

  if (!contactOk) {
    return {
      opportunityQualification: OPPORTUNITY_QUALIFICATION.WEAK,
      opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.WEAK,
      qualificationFailureReason: QUALIFICATION_FAILURE_REASON.CONTACT_NOT_RELEVANT,
      qualificationGatePassed: false,
      qualificationNotes: "No usable contact path.",
    };
  }

  return {
    opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
    opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.MODERATE,
    qualificationFailureReason: null,
    qualificationGatePassed: true,
    qualificationNotes: "Partial qualification — pursue with caution.",
  };
}

/**
 * Final High-priority quality checklist.
 * @returns {{ pass: boolean, failures: string[], checklist: object }}
 */
export function runHighPriorityQualityGate(opportunity = {}, ctx = {}) {
  const captureState =
    ctx.captureCapacityState ||
    opportunity.captureCapacityState ||
    null;
  const captureOk =
    !captureState ||
    captureState === CAPTURE_CAPACITY_STATE.UNKNOWN ||
    captureState === CAPTURE_CAPACITY_STATE.IDEAL_CAPTURE_RANGE ||
    captureState === CAPTURE_CAPACITY_STATE.PLAUSIBLE_CAPTURE ||
    captureState === CAPTURE_CAPACITY_STATE.STRETCH ||
    (captureState === CAPTURE_CAPACITY_STATE.OVERFLOW_ONLY &&
      (ctx.opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING ||
        ctx.opportunityType === OPPORTUNITY_TYPE.PRIMARY_PURSUIT));

  const checklist = {
    eventReal: Boolean(opportunity.title && opportunity.organizationName),
    dateCurrentOrFuture: !opportunity.eventStartDate || opportunity.eventStartDate >= "2026-01-01",
    venueOpenOrOverflow:
      [
        VENUE_SOURCING_STATUS.OPEN_UNRESOLVED,
        VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING,
        VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
        VENUE_SOURCING_STATUS.PARTIALLY_PLACED,
        VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
      ].includes(ctx.venueSourcingStatus) &&
      ctx.opportunityType !== OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED,
    locationCredible:
      ctx.eventLocationStatus !== EVENT_LOCATION_STATUS.UNKNOWN ||
      isCoreTerritory(ctx.demandTerritoryFit),
    meaningfulDemand: [
      ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK,
      ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM,
      ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE,
      ROOM_DEMAND_STATUS.OVERFLOW_ONLY,
      ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND,
    ].includes(ctx.roomDemandStatus),
    physicalFitOk:
      (Number(opportunity.hotelFitScore) || Number(ctx.hotelFitScore) || 0) >= 75 ||
      ((Number(opportunity.hotelFitScore) || Number(ctx.hotelFitScore) || 0) >= 72 &&
        [
          CAPTURE_CAPACITY_STATE.IDEAL_CAPTURE_RANGE,
          CAPTURE_CAPACITY_STATE.PLAUSIBLE_CAPTURE,
        ].includes(captureState) &&
        [
          OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN,
          OPPORTUNITY_QUALIFICATION.STRONG,
        ].includes(ctx.opportunityQualification)),
    captureCapacityOk: captureOk,
    bookingActionable: [
      BOOKING_WINDOW.CONTACT_NOW,
      BOOKING_WINDOW.QUALIFY_NOW,
      BOOKING_WINDOW.RESEARCH_FURTHER,
    ].includes(opportunity.bookingWindowStatus || ctx.bookingWindowStatus),
    contactPath:
      ctx.contactQuality &&
      ctx.contactQuality !== CONTACT_QUALITY.NO_CONTACT &&
      ctx.contactQuality !== CONTACT_QUALITY.GENERIC_INBOX,
    sourcesRecent: (Number(opportunity.evidenceConfidence) || Number(ctx.evidenceConfidence) || 0) >= 55,
    whyNowSpecific:
      opportunity.whyNow &&
      String(opportunity.whyNow).length > 40 &&
      !/^event is in 20\d\d so sales should contact now/i.test(String(opportunity.whyNow)),
    thesisOk: ctx.hotelOpportunityThesis && String(ctx.hotelOpportunityThesis).length > 40,
    qualificationOk: [
      OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN,
      OPPORTUNITY_QUALIFICATION.STRONG,
    ].includes(ctx.opportunityQualification),
    typeOk: [
      OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
    ].includes(ctx.opportunityType),
  };

  // High Priority contact path: named decision maker, named meetings contact, or housing/sourcing lead.
  // Association desks / generic inboxes may support Medium but not High.
  // Contactability helps actionability but does not alone create High (no email/phone required here).
  if (
    ctx.contactQuality === CONTACT_QUALITY.NAMED_DECISION_MAKER ||
    ctx.contactQuality === CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT ||
    ctx.contactQuality === CONTACT_QUALITY.HOUSING_SOURCING_CONTACT
  ) {
    checklist.contactPath = true;
  } else {
    checklist.contactPath = false;
  }

  const failures = Object.entries(checklist)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  return { pass: failures.length === 0, failures, checklist };
}

/**
 * Full enrichment package for one opportunity (does not call buildOpportunity).
 */
export function enrichQualificationPrecision(opportunity = {}) {
  const eventLocation = classifyEventLocation(opportunity);
  const venue = classifyVenueSourcingStatus(opportunity);
  const rooms = classifyRoomDemand(opportunity);
  const contact = classifyContactQuality(opportunity);
  const reactivation = classifyReactivationSignal(opportunity);
  const type = deriveOpportunityType({
    venueSourcingStatus: venue.venueSourcingStatus,
    roomDemandStatus: rooms.roomDemandStatus,
    reactivationSignal: reactivation.reactivationSignal,
    bookingWindowStatus: opportunity.bookingWindowStatus,
    opportunity,
  });
  const futureCycleEvidence = detectFutureCycleEvidence({
    ...opportunity,
    opportunityType: type.opportunityType,
    reactivationSignal: reactivation.reactivationSignal,
    venueSourcingStatus: venue.venueSourcingStatus,
  });

  const hotelOpportunityThesis = buildHotelOpportunityThesis(opportunity, {
    opportunityType: type.opportunityType,
    venueSourcingStatus: venue.venueSourcingStatus,
    roomDemandStatus: rooms.roomDemandStatus,
  });
  const whyMonitor =
    type.opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE
      ? futureCycleEvidence.whyMonitor ||
        "Recurring / future-cycle pattern — monitor for next published cycle; not open current sourcing."
      : null;

  const hotelContext = opportunity.hotelContext || null;
  const rawPeak =
    opportunity.estimatedPeakRooms ??
    opportunity.publishedPeakRooms ??
    rooms.publishedPeakRooms ??
    null;
  const peakRooms =
    typeof rawPeak === "number" && Number.isFinite(rawPeak) && rawPeak > 0
      ? rawPeak
      : Number.isFinite(Number(rawPeak)) && Number(rawPeak) > 0
        ? Number(rawPeak)
        : null;
  const hasHousingAccess =
    rooms.roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM ||
    rooms.roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK ||
    venue.venueSourcingStatus ===
      VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE ||
    /housing|hotel list|overflow|travel source|passtkey|onpeak/i.test(
      `${opportunity.hotelOpportunityThesis || ""} ${opportunity.summaryWhyHotel || ""} ${opportunity.venueSourcingRationale || ""}`
    );
  const captureCapacity = calculateHotelGroupCaptureCapacity({
    peakRoomDemand: peakRooms,
    hotelContext: hotelContext || {},
    opportunityType: type.opportunityType,
    hasHousingAccess,
    hasOverflowThesis: type.opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
    independentBookingPattern: Boolean(opportunity.independentBookingPattern),
    roomDemandStatus: rooms.roomDemandStatus,
  });

  const qualification = computeOpportunityQualification({
    venueSourcingStatus: venue.venueSourcingStatus,
    opportunityType: type.opportunityType,
    roomDemandStatus: rooms.roomDemandStatus,
    eventLocationStatus: eventLocation.eventLocationStatus,
    contactQuality: contact.contactQuality,
    hotelOpportunityThesis,
    evidenceConfidence: opportunity.evidenceConfidence,
    demandTerritoryFit: opportunity.demandTerritoryFit,
  });

  const highGate = runHighPriorityQualityGate(opportunity, {
    venueSourcingStatus: venue.venueSourcingStatus,
    opportunityType: type.opportunityType,
    roomDemandStatus: rooms.roomDemandStatus,
    eventLocationStatus: eventLocation.eventLocationStatus,
    contactQuality: contact.contactQuality,
    hotelOpportunityThesis,
    opportunityQualification: qualification.opportunityQualification,
    demandTerritoryFit: opportunity.demandTerritoryFit,
    hotelFitScore: opportunity.hotelFitScore,
    evidenceConfidence: opportunity.evidenceConfidence,
    bookingWindowStatus: opportunity.bookingWindowStatus,
    captureCapacityState: captureCapacity.captureCapacityState,
  });

  const recommendedAction = recommendedActionForType(
    { ...opportunity, recommendedAction: opportunity.recommendedAction },
    type.opportunityType
  );

  // Adjust contactability component for re-score
  const baseContactability =
    opportunity.fitComponents?.contactability ??
    opportunity.contactabilityScore ??
    50;
  const contactability = Math.max(
    0,
    Math.min(100, Math.round(Number(baseContactability) + (contact.contactabilityAdjustment || 0)))
  );

  // Soft-penalize geography when only org-name signal
  let geographyFit =
    opportunity.fitComponents?.geographyFit ?? opportunity.geographyFitScore ?? 50;
  if (eventLocation.eventLocationSource === "organization_name_weak_only") {
    geographyFit = Math.min(geographyFit, 35);
  }
  // Core/nearby territory with evidenced destination/title geography should score strongly
  if (
    isCoreTerritory(opportunity.demandTerritoryFit) &&
    (eventLocation.eventLocationStatus === EVENT_LOCATION_STATUS.VERIFIED_CITY ||
      eventLocation.eventLocationStatus === EVENT_LOCATION_STATUS.VERIFIED_VENUE ||
      eventLocation.eventLocationSource === "title_or_thesis_geography" ||
      eventLocation.eventLocationSource === "destination_with_core_territory")
  ) {
    geographyFit = Math.max(geographyFit, 82);
  }

  // A3: capacity-relative physical fit when hotel keys are known
  let physicalFit =
    opportunity.fitComponents?.physicalFit ?? opportunity.physicalFitScore ?? 50;
  if (captureCapacity.totalGuestrooms && peakRooms != null) {
    physicalFit = captureCapacity.physicalFitScore;
  } else if (
    captureCapacity.totalGuestrooms &&
    captureCapacity.captureCapacityState === CAPTURE_CAPACITY_STATE.UNKNOWN
  ) {
    // Keys known, peak unknown — keep research physicalFit but do not invent demand
    physicalFit = Math.min(100, Math.max(40, Number(physicalFit) || 55));
  }

  // Soft-boost historical / competitive accessibility when hotel is evidenced on an
  // official housing list / block (reusable — not hotel-specific).
  let historicalFit =
    opportunity.fitComponents?.historicalFit ?? opportunity.historicalFitScore ?? 50;
  let competitiveAccessibility =
    opportunity.fitComponents?.competitiveAccessibility ??
    opportunity.competitiveAccessibilityScore ??
    50;
  const accessBlob = `${opportunity.hotelOpportunityThesis || ""} ${opportunity.bethesdaWinThesis || ""} ${opportunity.demandTerritoryRationale || ""} ${opportunity.venueSourcingRationale || ""} ${opportunity.summaryWhyHotel || ""}`;
  if (
    /official (hotel|block|housing|partner)|already (an |the )?official|named in (the )?official|stay-to-play housing|verified housing program/i.test(
      accessBlob
    ) ||
    rooms.roomDemandStatus === ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM ||
    venue.venueSourcingStatus ===
      VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE
  ) {
    historicalFit = Math.max(historicalFit, 68);
    competitiveAccessibility = Math.max(competitiveAccessibility, 72);
  }

  return {
    ...eventLocation,
    ...venue,
    ...rooms,
    ...contact,
    ...reactivation,
    ...type,
    ...qualification,
    hotelOpportunityThesis,
    bethesdaWinThesis: opportunity.bethesdaWinThesis || hotelOpportunityThesis,
    futureCycleEvidenceState: futureCycleEvidence.state,
    futureCycleEvidenceStateLabel: futureCycleEvidence.stateLabel,
    futureCycleSignals: futureCycleEvidence.signals,
    whyMonitor: whyMonitor || opportunity.whyMonitor || null,
    highPriorityGate: highGate,
    recommendedAction,
    captureCapacity,
    captureCapacityState: captureCapacity.captureCapacityState,
    captureCapacityStateLabel: captureCapacity.captureCapacityStateLabel,
    estimatedCaptureRooms: captureCapacity.estimatedCaptureRooms,
    captureRatio: captureCapacity.captureRatio,
    captureCapacityRationale: captureCapacity.rationale,
    researchMethodsAttempted: Array.from(
      new Set([
        ...(opportunity.researchMethodsAttempted || []),
        "GDI-VENUE-HOUSING-01",
        "GDI-CONTACT-01",
        "GDI-CAPTURE-CAPACITY-01",
      ])
    ),
    fitComponents: {
      physicalFit,
      geographyFit,
      timing: opportunity.fitComponents?.timing ?? opportunity.timingScore,
      commercialValue:
        opportunity.fitComponents?.commercialValue ?? opportunity.commercialValueScore,
      historicalFit,
      competitiveAccessibility,
      contactability,
    },
  };
}
