/**
 * Attach Professional Event Planner presentation: identity, Planner Intelligence,
 * Segment 8 routing into shared ten-section drawer (subtype extension only).
 */
import {
  PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE,
  PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
  PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
  PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_V1,
  PLANNER_PARTNERSHIP_STATUS,
  isProfessionalEventPlannerOpportunity,
  text,
} from "./professional-event-planner-contract-v1.js";
import { buildProfessionalEventPlannerIdentity } from "./professional-event-planner-identity-v1.js";
import { buildPlannerServiceAreaEvidence } from "./planner-service-area-v1.js";
import { evaluateProfessionalEventPlannerEligibility } from "./professional-event-planner-eligibility-v1.js";
import {
  buildPlannerIntelligenceSection,
  plannerIntelligenceFieldRows,
} from "./planner-intelligence-section-v1.js";

export const PROFESSIONAL_EVENT_PLANNER_PRESENTATION_V1 =
  "gdi-professional-event-planner-presentation-v1";

const DEFAULT_QUAL_QUESTIONS = Object.freeze([
  "How many events do you organize in this destination?",
  "What types of events typically require accommodation?",
  "Do you select hotel venues or recommend options to clients?",
  "Do you manage guestroom blocks?",
  "Do you already have preferred hotel partners?",
  "Do you typically contract banquet space directly?",
  "What are your clients' main hotel selection requirements?",
  "When do you generally begin hotel sourcing?",
  "Would a preferred planner programme or dedicated hotel contact be useful?",
  "How could we measure referrals and converted business?",
]);

function inferRevenueMechanisms(opp = {}) {
  if (Array.isArray(opp.revenueMechanisms) && opp.revenueMechanisms.length) {
    return opp.revenueMechanisms.map(text).filter(Boolean);
  }
  const blob = [
    opp.summaryWhat,
    opp.summaryWhyHotel,
    opp.hotelOpportunityThesis,
    opp.potentialBusiness,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();
  const out = [];
  if (/guestroom|room block|overnight|accommodation/i.test(blob)) {
    out.push("GROUP_GUESTROOMS");
  }
  if (/wedding.*(block|room)|bridal party/i.test(blob)) {
    out.push("WEDDING_ROOM_BLOCKS");
  }
  if (/banquet|catering|ballroom|reception catering/i.test(blob)) {
    out.push("BANQUET_CATERING");
  }
  if (/meeting space|event space|conference/i.test(blob)) {
    out.push("MEETING_EVENT_SPACE");
  }
  if (/private dining/i.test(blob)) out.push("PRIVATE_DINING");
  if (/resort celebration|destination wedding/i.test(blob)) {
    out.push("RESORT_CELEBRATIONS");
  }
  if (/incentive/i.test(blob)) out.push("INCENTIVE_GROUPS");
  return out;
}

export function applyProfessionalEventPlannerPresentation(
  opportunity = {},
  hotel = {},
) {
  if (!isProfessionalEventPlannerOpportunity(opportunity)) {
    return opportunity;
  }

  const next = { ...opportunity };
  const planner = {
    name: next.organizationName || next.plannerName || next.companyName,
    companyName: next.organizationName || next.plannerName,
    organizationName: next.organizationName || next.plannerName,
    specialization: next.plannerSpecialization || next.specialization,
    plannerType: next.plannerType,
    officeLocation: next.officeLocation || next.venueCity || next.city,
    verifiedServiceArea: next.verifiedServiceArea,
    website: next.contactUrl || next.contactOfficialUrl,
    url: next.contactUrl,
    vendorClass: next.vendorClass,
    companyType: next.companyType,
    eventTypes: next.eventTypes,
    operatingMarkets: next.operatingMarkets,
    multipleEventEvidence: next.multipleEventEvidence,
    recurringProgrammeEvidence: next.recurringProgrammeEvidence,
  };

  const identity = buildProfessionalEventPlannerIdentity({
    planner,
    hotel: { hotelId: next.hotelId || hotel.hotelId, ...hotel },
    opportunity: next,
  });

  const serviceArea = buildPlannerServiceAreaEvidence({
    planner,
    hotel: {
      market: hotel.market || next.hotelMarket || hotel.city,
      city: hotel.city || next.hotelCity,
      name: hotel.name || next.hotelDisplayName,
      destination: hotel.destination,
    },
    destinationEvidence: Array.isArray(next.destinationEvidence)
      ? next.destinationEvidence
      : [],
  });

  const contacts = [
    next.primaryContact,
    next.secondaryContact,
    ...(Array.isArray(next.namedContacts) ? next.namedContacts : []),
  ].filter(Boolean);

  const revenueMechanisms = inferRevenueMechanisms(next);
  const eventActivityVerified = Boolean(
    next.eventActivityEvidenceStatus ||
      next.plannerActivityVerified ||
      (Array.isArray(next.activityEvidence) && next.activityEvidence.length) ||
      (Array.isArray(next.destinationEvidence) &&
        next.destinationEvidence.some((e) => e.verified !== false)),
  );

  const hotelInfluenceEvidence = Boolean(
    next.hotelInfluenceEvidence ||
      next.commercialInfluenceVerified ||
      /venue selection|hotel recommend|guestroom block|accommodation|banquet/i.test(
        text(next.summaryWhat || next.hotelOpportunityThesis),
      ),
  );

  const eligibility = evaluateProfessionalEventPlannerEligibility({
    planner,
    serviceArea,
    contacts,
    eventActivityVerified,
    revenueMechanisms,
    hotelInfluenceEvidence,
    alreadyClassifiedElsewhere: Boolean(next.alreadyClassifiedElsewhere),
  });

  const specializationLabel =
    text(next.plannerSpecialization || next.specialization) ||
    "Professional event planner / intermediary";

  const plannerIntelligence = buildPlannerIntelligenceSection({
    planner: {
      ...planner,
      name: identity.displayTitle,
      website: planner.website,
      specialization: specializationLabel,
    },
    serviceArea,
    eventProduction: {
      types: text(next.eventTypes) || specializationLabel,
      portfolioExamples: next.portfolioExamples || [],
      historicVenues: next.historicVenues || [],
      destinations: serviceArea.destinationEvidence
        ?.map((e) => e.destination)
        .filter(Boolean)
        .join("; "),
      recurringEvidence: next.recurringProgrammeEvidence
        ? "Recurring programme evidence present"
        : next.multipleEventEvidence
          ? "Multiple-event evidence present"
          : null,
      multipleEventEvidence: next.multipleEventEvidence,
      volumeWhenVerified: next.verifiedVolumeNote || null,
    },
    hotelBusinessPotential: {
      mechanisms: revenueMechanisms,
      groupAccommodation: revenueMechanisms.includes("GROUP_GUESTROOMS")
        ? "Group accommodation / overnight referrals under qualification"
        : null,
      weddingRoomBlocks: revenueMechanisms.includes("WEDDING_ROOM_BLOCKS")
        ? "Wedding guestroom blocks under qualification"
        : null,
      banquetsCatering: revenueMechanisms.includes("BANQUET_CATERING")
        ? "Banquet / catering opportunity under qualification"
        : null,
      meetingEventSpace: revenueMechanisms.includes("MEETING_EVENT_SPACE")
        ? "Meeting / event space under qualification"
        : null,
      resortProgrammes: revenueMechanisms.includes("RESORT_CELEBRATIONS")
        ? "Resort celebration programmes under qualification"
        : null,
    },
    commercialInfluence: {
      venueSelection: text(next.venueSelectionRole) || null,
      guestroomCoordination: text(next.guestroomCoordinationRole) || null,
      referralInfluence: text(next.referralInfluenceRole) || null,
      existingHotelPartnerships: next.existingHotelPartnershipsNote,
    },
    partnershipStatus:
      next.plannerPartnershipStatus ||
      PLANNER_PARTNERSHIP_STATUS.NOT_CONTACTED,
    partnershipPotential: eligibility.partnershipPotential,
    partnershipAssessment: {
      hotelFitThesis:
        text(next.summaryWhyHotel || next.hotelOpportunityThesis)?.slice(
          0,
          320,
        ) || null,
      competitiveConsiderations:
        text(next.competitiveConsiderations)?.slice(0, 280) || null,
      openQualificationItems: eligibility.gaps.slice(0, 5),
    },
  });

  next.professionalEventPlannerPartnershipV1 = true;
  next.demandOpportunitySubtype = PROFESSIONAL_EVENT_PLANNER_SUBTYPE;
  next.demandSegmentId = PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID;
  next.researchSegment = next.researchSegment || PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID;
  next.segmentDisplayLabel = "Social Groups & Special Events";
  next.opportunityType = PROFESSIONAL_EVENT_PLANNER_SUBTYPE;
  next.opportunityTypeLabel = "Professional Event Planner Partnership";
  next.demandSignalType = PROFESSIONAL_EVENT_PLANNER_SUBTYPE;
  next.detailLayout = "PROFESSIONAL_EVENT_PLANNER_SHARED_DRAWER";
  next.displayTitle = identity.displayTitle;
  next.drawerHeaderTitle = identity.displayTitle;
  next.title = identity.displayTitle;
  next.organizationName = identity.displayTitle;
  next.opportunityIdentity = identity;
  next.plannerServiceArea = serviceArea;
  next.plannerIntelligence = plannerIntelligence;
  next.plannerIntelligenceFieldRows =
    plannerIntelligenceFieldRows(plannerIntelligence);
  next.professionalEventPlannerEligibility = eligibility;
  next.classificationLine = PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE;
  next.tileCustomerSummary =
    text(next.summaryWhat) ||
    "Professional event planner / intermediary — investigate a recurring hotel partnership for events serving this destination.";
  next.eventDateDisplay =
    next.standingOpportunity ||
    /ongoing/i.test(String(next.eventDateDisplay || ""))
      ? "Ongoing Partnership"
      : next.eventDateDisplay || "Ongoing Partnership";
  next.qualificationQuestions =
    Array.isArray(next.qualificationQuestions) &&
    next.qualificationQuestions.length
      ? next.qualificationQuestions
      : [...DEFAULT_QUAL_QUESTIONS];
  next.professionalEventPlannerPresentation = {
    version: PROFESSIONAL_EVENT_PLANNER_PRESENTATION_V1,
    contract: PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP_V1,
    at: new Date().toISOString(),
  };

  next.segmentOverview = {
    ...(next.segmentOverview || {}),
    identityLine: identity.displayTitle,
    eventTiming: next.eventDateDisplay || "Ongoing Partnership",
    destination:
      serviceArea.hotelMarket ||
      [next.officeLocation, next.venueCity].filter(Boolean).join(", ") ||
      null,
    venueHotelSourcing: "Professional event planner partnership development",
    groupAccommodationDemand:
      "Planner-influenced guestrooms / banquet / event demand (hypothesis)",
    eventCycle: "Standing planner account — not a single dated event",
    eventType: specializationLabel,
    functionCateringDemand:
      "Planner may source banquet, catering, or overnight accommodation",
  };

  // Default next-step prose when absent — commercially specific, hotel-capability aware.
  if (!text(next.recommendedNextStep) && !text(next.nextBestAction)) {
    next.recommendedNextStep =
      "Approach the verified lead planner to introduce the hotel's wedding accommodation and banquet offering, establish whether they regularly source guestrooms or events in the destination, and explore preferred hotel arrangements for future clients.";
  }

  return next;
}
