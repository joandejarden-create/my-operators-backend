/**
 * Attach Preferred Lodging presentation: identity, Venue Intelligence,
 * Segment 8 routing into shared ten-section drawer (subtype extension only).
 */
import {
  PREFERRED_LODGING_CLASSIFICATION_LINE,
  PREFERRED_LODGING_SEGMENT_ID,
  PREFERRED_LODGING_SUBTYPE,
  PREFERRED_LODGING_PARTNERSHIP_V1,
  PARTNERSHIP_RELATIONSHIP_STATUS,
  isPreferredLodgingPartnershipOpportunity,
  normalizeVenueOnsiteLodgingStatus,
  text,
  VENUE_ONSITE_LODGING_STATUS_LABEL,
} from "./preferred-lodging-partnership-contract-v1.js";
import { buildPreferredLodgingIdentity } from "./preferred-lodging-identity-v1.js";
import { buildVenueProximityEvidence } from "./venue-geo-proximity-v1.js";
import { evaluatePreferredLodgingEligibility } from "./preferred-lodging-eligibility-v1.js";
import {
  buildVenueIntelligenceSection,
  venueIntelligenceFieldRows,
} from "./venue-intelligence-section-v1.js";

export const PREFERRED_LODGING_PRESENTATION_V1 =
  "gdi-preferred-lodging-presentation-v1";

const DEFAULT_QUAL_QUESTIONS = Object.freeze([
  "How often does the venue host weddings or events with visiting overnight guests?",
  "Do guests commonly ask the venue for nearby hotel recommendations, and who manages those referrals?",
  "Does the venue already have preferred hotel partners, and is any arrangement exclusive?",
  "Would a preferred rate, booking link or courtesy block be useful for peak dates?",
  "What transportation or accessibility expectations do out-of-town guests typically have?",
]);

export function applyPreferredLodgingPresentation(opportunity = {}, hotel = {}) {
  if (!isPreferredLodgingPartnershipOpportunity(opportunity)) {
    return opportunity;
  }

  const next = { ...opportunity };
  const venue = {
    name: next.organizationName || next.venueName,
    venueName: next.organizationName || next.venueName,
    address: next.venueAddress,
    venueAddress: next.venueAddress,
    city: next.venueCity,
    venueCity: next.venueCity,
    venueType: next.venueType,
    website: next.contactUrl || next.contactOfficialUrl,
    url: next.contactUrl,
    maxCapacity: next.maxCapacity,
    onSiteLodgingStatus: next.onSiteLodgingStatus,
    eventActivityEvidenceStatus: next.eventActivityEvidenceStatus,
    partnerStatusLabel: next.partnerStatusLabel,
    peVenueId: next.peVenueId,
    lat: next.venueLat || next.latitude,
    lng: next.venueLng || next.longitude,
  };

  const identity = buildPreferredLodgingIdentity({
    venue,
    hotel: { hotelId: next.hotelId || hotel.hotelId, ...hotel },
    opportunity: next,
  });

  const proximity = buildVenueProximityEvidence({
    hotel: {
      lat: hotel.lat || next.hotelLat,
      lng: hotel.lng || next.hotelLng,
      latitude: hotel.latitude,
      longitude: hotel.longitude,
    },
    venue,
    historical: {
      distanceMiles: next.distanceMiles,
      driveTimeMinutes: next.driveTimeMinutes,
      distanceMethod: next.distanceMethod,
    },
  });

  const lodging = normalizeVenueOnsiteLodgingStatus(next.onSiteLodgingStatus);
  const contacts = [
    next.primaryContact,
    next.secondaryContact,
    ...(Array.isArray(next.namedContacts) ? next.namedContacts : []),
  ].filter(Boolean);

  const eligibility = evaluatePreferredLodgingEligibility({
    venue,
    proximity,
    lodgingStatus: lodging,
    contacts,
    eventActivityVerified: Boolean(
      next.eventActivityEvidenceStatus ||
        (Array.isArray(next.activityEvidence) && next.activityEvidence.length),
    ),
  });

  const venueTypeRaw = text(next.venueType);
  const venueTypeLabel =
    !venueTypeRaw || /^OTHER$/i.test(venueTypeRaw)
      ? "Private event / celebration venue"
      : venueTypeRaw.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
  const activityRaw = text(next.eventActivityEvidenceStatus);
  const activityLabel = activityRaw
    .replace(/_/g, " ")
    .replace(/\bMODERATE ACTIVITY\b/i, "Moderate verified activity")
    .replace(/\bHIGH ACTIVITY\b/i, "High verified activity")
    .replace(/\bLOW ACTIVITY\b/i, "Low verified activity")
    .replace(/\bUNKNOWN\b/i, "Activity level not yet verified");

  const venueIntelligence = buildVenueIntelligenceSection({
    venue: {
      ...venue,
      name: identity.displayTitle,
      website: venue.website,
      venueType: venueTypeLabel,
    },
    proximity,
    lodgingStatus: lodging,
    partnershipStatus: PARTNERSHIP_RELATIONSHIP_STATUS.PARTNERSHIP_DEVELOPMENT,
    eventActivity: {
      types: venueTypeLabel,
      capacity: next.maxCapacity,
      level: activityLabel || null,
    },
    partnershipAssessment: {
      hotelFitThesis:
        text(next.summaryWhyHotel || next.hotelOpportunityThesis)?.slice(0, 320) ||
        null,
      competitiveConsiderations: text(next.partnerResearchScope)?.slice(0, 280) || null,
      existingPreferredHotel: next.partnerStatusLabel,
      overflowConsiderations:
        lodging === "LIMITED_ONSITE_LODGING"
          ? "Qualify overflow demand with the venue."
          : null,
      openQualificationItems: eligibility.gaps.slice(0, 5),
    },
  });

  next.preferredLodgingPartnershipV1 = true;
  next.demandOpportunitySubtype = PREFERRED_LODGING_SUBTYPE;
  next.demandSegmentId = PREFERRED_LODGING_SEGMENT_ID;
  next.researchSegment = next.researchSegment || PREFERRED_LODGING_SEGMENT_ID;
  next.segmentDisplayLabel = "Social Groups & Special Events";
  next.opportunityType = PREFERRED_LODGING_SUBTYPE;
  next.opportunityTypeLabel = "Preferred Lodging Partnership";
  next.demandSignalType = PREFERRED_LODGING_SUBTYPE;
  // Force shared ten-section path — not legacy VENUE_PARTNERSHIP layout.
  next.detailLayout = "PREFERRED_LODGING_PARTNERSHIP_SHARED_DRAWER";
  next.displayTitle = identity.displayTitle;
  next.drawerHeaderTitle = identity.displayTitle;
  next.title = identity.displayTitle;
  next.organizationName = identity.displayTitle;
  next.opportunityIdentity = identity;
  next.venueProximity = proximity;
  next.venueIntelligence = venueIntelligence;
  next.venueIntelligenceFieldRows = venueIntelligenceFieldRows(venueIntelligence);
  next.preferredLodgingEligibility = eligibility;
  next.onSiteLodgingStatus = lodging;
  next.onSiteLodgingStatusLabel =
    VENUE_ONSITE_LODGING_STATUS_LABEL[lodging] || lodging;
  next.classificationLine = PREFERRED_LODGING_CLASSIFICATION_LINE;
  next.tileCustomerSummary =
    text(next.summaryWhat) ||
    "Independent event venue — investigate a preferred hotel accommodation arrangement for visiting guests.";
  next.eventDateDisplay =
    next.standingOpportunity || /ongoing/i.test(String(next.eventDateDisplay || ""))
      ? "Ongoing Partnership"
      : next.eventDateDisplay || "Ongoing Partnership";
  next.qualificationQuestions =
    Array.isArray(next.qualificationQuestions) && next.qualificationQuestions.length
      ? next.qualificationQuestions
      : [...DEFAULT_QUAL_QUESTIONS];
  next.preferredLodgingPresentation = {
    version: PREFERRED_LODGING_PRESENTATION_V1,
    contract: PREFERRED_LODGING_PARTNERSHIP_V1,
    at: new Date().toISOString(),
  };

  // Overview grid for Segment 8 shared Section 1
  next.segmentOverview = {
    ...(next.segmentOverview || {}),
    identityLine: identity.displayTitle,
    eventTiming: next.eventDateDisplay || "Ongoing Partnership",
    destination: [next.venueCity, next.venueRegion].filter(Boolean).join(", ") || null,
    venueHotelSourcing: "Preferred lodging partnership development",
    groupAccommodationDemand: "Guestroom referrals / group accommodation (hypothesis)",
    eventCycle: "Standing venue account — not a single dated event",
    eventType: venueTypeLabel,
    functionCateringDemand: "Venue hosts functions; hotel thesis is overnight lodging",
  };

  return next;
}
