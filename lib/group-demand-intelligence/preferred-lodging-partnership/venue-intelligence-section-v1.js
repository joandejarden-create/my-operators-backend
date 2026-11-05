/**
 * Venue Intelligence — subtype extension section (after Opportunity overview).
 * Internal section id: venue_intelligence (does not renumber canonical 1–10).
 */
import {
  CATCHMENT_CLASS_LABEL,
  PARTNERSHIP_RELATIONSHIP_STATUS,
  PARTNERSHIP_RELATIONSHIP_STATUS_LABEL,
  VENUE_ONSITE_LODGING_STATUS_LABEL,
  normalizeVenueOnsiteLodgingStatus,
  text,
} from "./preferred-lodging-partnership-contract-v1.js";

export const VENUE_INTELLIGENCE_SECTION_V1 =
  "gdi-venue-intelligence-section-v1";

export const VENUE_INTELLIGENCE_SECTION_ID = "venue_intelligence";

export function buildVenueIntelligenceSection({
  venue = {},
  proximity = {},
  lodgingStatus = null,
  partnershipStatus = PARTNERSHIP_RELATIONSHIP_STATUS.PARTNERSHIP_DEVELOPMENT,
  eventActivity = {},
  partnershipAssessment = {},
} = {}) {
  const lodging = normalizeVenueOnsiteLodgingStatus(
    lodgingStatus || venue.onSiteLodgingStatus,
  );
  const catchment =
    proximity.catchmentClassLabel ||
    CATCHMENT_CLASS_LABEL[proximity.catchmentClass] ||
    "Catchment not yet classified";

  return {
    version: VENUE_INTELLIGENCE_SECTION_V1,
    sectionId: VENUE_INTELLIGENCE_SECTION_ID,
    title: "Venue Intelligence",
    insertAfterSectionId: "opportunity_overview",
    subtypeOnly: true,
    venue: {
      name: text(venue.name || venue.venueName) || null,
      venueType: text(venue.venueType || venue.type) || null,
      address: text(venue.address || venue.venueAddress) || null,
      website: text(venue.website || venue.url || venue.contactUrl) || null,
    },
    proximity: {
      distance: proximity.customerFacingDistance || proximity.distanceLabel || null,
      travelTime:
        proximity.customerFacingTravelTime ||
        proximity.travelTimeNote ||
        "Travel time not yet verified.",
      travelMode: proximity.travelMode || null,
      catchmentClassification: catchment,
      distanceMethodLabel: proximity.distanceMethodLabel || null,
    },
    accommodation: {
      onSiteLodgingStatus: lodging,
      onSiteLodgingStatusLabel:
        VENUE_ONSITE_LODGING_STATUS_LABEL[lodging] || lodging,
      overflowConsiderations:
        text(partnershipAssessment.overflowConsiderations) ||
        (lodging === "LIMITED_ONSITE_LODGING"
          ? "Limited on-site rooms may create overflow demand — qualify with venue."
          : null),
      existingPreferredHotelRelationship:
        text(partnershipAssessment.existingPreferredHotel) ||
        text(venue.partnerStatusLabel) ||
        "No verified preferred-hotel relationship established from public research.",
    },
    eventActivity: {
      mainEventTypes: text(eventActivity.types || venue.eventTypes) || null,
      capacity:
        eventActivity.capacity ??
        venue.maxCapacity ??
        venue.venueCapacity ??
        null,
      activityLevel:
        text(eventActivity.level || venue.eventActivityEvidenceStatus) || null,
      seasonality: text(eventActivity.seasonality) || null,
    },
    partnershipAssessment: {
      hotelFitThesis:
        text(partnershipAssessment.hotelFitThesis) ||
        text(partnershipAssessment.whyHotel) ||
        null,
      competitiveConsiderations:
        text(partnershipAssessment.competitiveConsiderations) || null,
      openQualificationItems: Array.isArray(
        partnershipAssessment.openQualificationItems,
      )
        ? partnershipAssessment.openQualificationItems
        : [],
      relationshipStatus: partnershipStatus,
      relationshipStatusLabel:
        PARTNERSHIP_RELATIONSHIP_STATUS_LABEL[partnershipStatus] ||
        partnershipStatus,
    },
  };
}

/** Compact HTML-ready field rows for the drawer (no raw enums). */
export function venueIntelligenceFieldRows(section = {}) {
  const v = section.venue || {};
  const p = section.proximity || {};
  const a = section.accommodation || {};
  const e = section.eventActivity || {};
  const pa = section.partnershipAssessment || {};
  const rows = [
    ["Venue", [v.name, v.venueType].filter(Boolean).join(" · ") || null],
    ["Address", v.address],
    ["Website", v.website],
    ["Distance from hotel", p.distance],
    ["Travel time", p.travelTime],
    ["Catchment", p.catchmentClassification],
    ["On-site lodging", a.onSiteLodgingStatusLabel],
    ["Preferred-hotel relationship", a.existingPreferredHotelRelationship],
    ["Event types", e.mainEventTypes],
    [
      "Capacity",
      e.capacity != null && e.capacity !== "" ? String(e.capacity) : null,
    ],
    ["Activity level", e.activityLevel],
    ["Partnership fit", pa.hotelFitThesis],
    ["Competitive notes", pa.competitiveConsiderations],
    ["Relationship stage", pa.relationshipStatusLabel],
  ];
  return rows.filter((r) => r[1]);
}
