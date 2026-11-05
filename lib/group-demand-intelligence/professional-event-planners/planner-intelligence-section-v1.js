/**
 * Planner & Intermediary Intelligence — subtype extension section
 * (after Opportunity overview). Internal id: planner_intelligence
 * (does not renumber canonical 1–10).
 */
import {
  PLANNER_PARTNERSHIP_POTENTIAL_LABEL,
  PLANNER_PARTNERSHIP_STATUS,
  PLANNER_PARTNERSHIP_STATUS_LABEL,
  PLANNER_HOTEL_REVENUE_MECHANISM_LABEL,
  normalizePlannerPartnershipStatus,
  text,
} from "./professional-event-planner-contract-v1.js";

export const PLANNER_INTELLIGENCE_SECTION_V1 =
  "gdi-planner-intelligence-section-v1";

export const PLANNER_INTELLIGENCE_SECTION_ID = "planner_intelligence";

function mechanismLabels(list = []) {
  return (list || [])
    .map((m) => PLANNER_HOTEL_REVENUE_MECHANISM_LABEL[m] || text(m))
    .filter(Boolean);
}

export function buildPlannerIntelligenceSection({
  planner = {},
  serviceArea = {},
  eventProduction = {},
  hotelBusinessPotential = {},
  commercialInfluence = {},
  partnershipAssessment = {},
  partnershipStatus = PLANNER_PARTNERSHIP_STATUS.NOT_CONTACTED,
  partnershipPotential = null,
} = {}) {
  const status = normalizePlannerPartnershipStatus(partnershipStatus);
  const revenueList = mechanismLabels(
    hotelBusinessPotential.mechanisms ||
      hotelBusinessPotential.revenueMechanisms ||
      [],
  );

  return {
    version: PLANNER_INTELLIGENCE_SECTION_V1,
    sectionId: PLANNER_INTELLIGENCE_SECTION_ID,
    title: "Planner & Intermediary Intelligence",
    insertAfterSectionId: "opportunity_overview",
    subtypeOnly: true,
    plannerProfile: {
      companyName:
        text(planner.name || planner.companyName || planner.organizationName) ||
        null,
      specialization: text(planner.specialization || planner.plannerType) || null,
      officeLocation:
        text(planner.officeLocation || serviceArea.officeLocation) || null,
      verifiedOperatingMarkets:
        (serviceArea.operatingMarkets || []).join("; ") ||
        text(planner.verifiedServiceArea) ||
        null,
      website: text(planner.website || planner.url || planner.contactUrl) || null,
    },
    eventProduction: {
      eventTypes:
        text(eventProduction.types || planner.eventTypes) || null,
      portfolioExamples: Array.isArray(eventProduction.portfolioExamples)
        ? eventProduction.portfolioExamples.map(text).filter(Boolean)
        : [],
      historicVenues: Array.isArray(eventProduction.historicVenues)
        ? eventProduction.historicVenues.map(text).filter(Boolean)
        : [],
      relevantDestinations:
        text(eventProduction.destinations) ||
        (serviceArea.destinationEvidence || [])
          .map((e) => e.destination)
          .filter(Boolean)
          .join("; ") ||
        null,
      recurringOrMultipleEvidence:
        text(eventProduction.recurringEvidence) ||
        (eventProduction.multipleEventEvidence
          ? "Multiple verified events evidenced"
          : null),
      volumeWhenVerified:
        text(eventProduction.volumeWhenVerified) ||
        "Volume not independently verified — do not invent counts.",
    },
    hotelBusinessPotential: {
      mechanisms: revenueList,
      groupAccommodation: text(hotelBusinessPotential.groupAccommodation) || null,
      weddingRoomBlocks: text(hotelBusinessPotential.weddingRoomBlocks) || null,
      banquetsCatering: text(hotelBusinessPotential.banquetsCatering) || null,
      meetingEventSpace: text(hotelBusinessPotential.meetingEventSpace) || null,
      resortProgrammes: text(hotelBusinessPotential.resortProgrammes) || null,
      otherServices: text(hotelBusinessPotential.otherServices) || null,
    },
    commercialInfluence: {
      venueSelectionRole: text(commercialInfluence.venueSelection) || null,
      guestroomCoordinationRole:
        text(commercialInfluence.guestroomCoordination) || null,
      plannerReferralInfluence:
        text(commercialInfluence.referralInfluence) || null,
      existingHotelPartnerships:
        text(commercialInfluence.existingHotelPartnerships) ||
        "No verified preferred-hotel partnership established from public research.",
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
      relationshipStatus: status,
      relationshipStatusLabel:
        PLANNER_PARTNERSHIP_STATUS_LABEL[status] || status,
      partnershipPotential,
      partnershipPotentialLabel:
        PLANNER_PARTNERSHIP_POTENTIAL_LABEL[partnershipPotential] ||
        partnershipPotential ||
        null,
    },
  };
}

/** Compact HTML-ready field rows for the drawer (no raw enums). */
export function plannerIntelligenceFieldRows(section = {}) {
  const p = section.plannerProfile || {};
  const e = section.eventProduction || {};
  const h = section.hotelBusinessPotential || {};
  const c = section.commercialInfluence || {};
  const pa = section.partnershipAssessment || {};
  const rows = [
    ["Company", [p.companyName, p.specialization].filter(Boolean).join(" · ") || null],
    ["Office location", p.officeLocation],
    ["Operating markets", p.verifiedOperatingMarkets],
    ["Website", p.website],
    ["Event types", e.eventTypes],
    [
      "Portfolio examples",
      (e.portfolioExamples || []).slice(0, 3).join("; ") || null,
    ],
    [
      "Historic venues",
      (e.historicVenues || []).slice(0, 3).join("; ") || null,
    ],
    ["Relevant destinations", e.relevantDestinations],
    ["Recurring / multi-event evidence", e.recurringOrMultipleEvidence],
    ["Volume", e.volumeWhenVerified],
    [
      "Hotel business potential",
      (h.mechanisms || []).join("; ") ||
        [h.groupAccommodation, h.banquetsCatering, h.weddingRoomBlocks]
          .filter(Boolean)
          .join("; ") ||
        null,
    ],
    ["Venue selection role", c.venueSelectionRole],
    ["Guestroom coordination", c.guestroomCoordinationRole],
    ["Referral influence", c.plannerReferralInfluence],
    ["Existing hotel partnerships", c.existingHotelPartnerships],
    ["Partnership fit", pa.hotelFitThesis],
    ["Competitive notes", pa.competitiveConsiderations],
    ["Partnership potential", pa.partnershipPotentialLabel],
    ["Relationship stage", pa.relationshipStatusLabel],
  ];
  return rows.filter((r) => r[1]);
}
