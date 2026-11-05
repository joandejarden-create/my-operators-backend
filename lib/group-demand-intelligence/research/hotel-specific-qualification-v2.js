/**
 * Hotel-specific commercial qualification V2.
 * Venue listings / amenity bedroom counts ≠ guestroom purchasing control.
 * London office + email ≠ overnight demand.
 * Does not change production publication eligibility.
 */

import {
  RESEARCH_PATHWAY,
  COMMERCIAL_STAGE,
  SALES_READINESS,
  evaluateCommercialQualification,
  namedCompetitorHost,
} from "./commercial-pathway-v1.js";

export const HOTEL_QUALIFICATION_V2 = "gdi-hotel-specific-qualification-v2";

const VENUE_LISTING = /\bfree\s+venue\s+find|venue\s+(?:listing|hire|finding)|book\s+(?:the\s+)?(?:ballroom|meeting\s*room)|event\s+space|maximum\s+guests|layout\s*&\s*spaces|menus?\s*&\s*brochures/i;
const BEDROOM_AS_AMENITY = /\bbedrooms?\s*:\s*\d+|\b\d+\s+bedrooms?\b/i;
/** Hyphenated forms (hotel-sourcing) must count — live corpus used them and previously failed. */
const GUESTROOM_CONTROL = /\b(?:hotel[\s-]*sourc(?:e|ing)|accommodation[\s-]*sourc(?:e|ing)|accommodation\s+services|group\s*(?:hotel|accommodation)|room\s*block|delegate\s*(?:hotel|housing)|hotel\s+and\s+restaurant\s+bookings|contract(?:ing|ed)\s+(?:hotel|rooms)|places?\s+(?:groups?|delegates?)\s+(?:in|at)\s+hotels?|official\s+(?:conference\s+)?housing|conference\s+housing|exhibitor\s+housing|attendee\s+housing|hotel\s+(?:and|&)\s+travel|hotel\s+headquarters|show\s+hotel|housing\s+(?:information|page|block|bureau)|reserved\s+(?:hotel|room)\s+block)\b/i;
const GENERIC_OFFICE = /\b(?:london\s+office|based\s+in\s+london|head\s*office)\b/i;
/** Demand-side overnight language — not target-hotel inventory ("600 guestrooms"). */
const EVENT_OVERNIGHT = /\b(?:room\s*block|delegate\s*housing|peak\s*rooms?|housing\s*block|rooms?\s+for\s+(?:attendees|delegates|participants)|overnight\s+(?:stay|rooms?|accommodation|guests?)|attendee\s+(?:rooms?|housing)|group\s+rooms?\s+(?:needed|required|blocked))\b/i;
const BANQUET_ONLY = /\b(?:awards?\s+(?:banquet|dinner)|gala\b|reception|ballroom|catered\s+special\s+events?|banquet\s+venue)\b/i;
const TARGET_HOTEL_INVENTORY_PROSE = /\b(?:\d+\s+guestrooms?|\d+\s+meeting\s+rooms?|supplied\s+(?:hotel|property)\s+facts?|physical\s+capacity)\b/i;

/**
 * Classify whether accepted evidence supports overnight accommodation control
 * vs venue/day-meeting promotion.
 */
export function classifyAccommodationEvidenceRole({
  acceptedClaims = [],
  pageTexts = [],
  organizationName = "",
} = {}) {
  const blob = [
    ...acceptedClaims.map((c) => `${c.topic || ""} ${c.statement || ""} ${c.excerpt || ""}`),
    ...pageTexts,
    organizationName,
  ].join("\n");

  const venueListingSignals = VENUE_LISTING.test(blob);
  const bedroomAmenity = BEDROOM_AS_AMENITY.test(blob);
  const guestroomControl = GUESTROOM_CONTROL.test(blob);
  const officeOnly = GENERIC_OFFICE.test(blob) && !guestroomControl;
  // Hotel-fit prose that mentions the target hotel's bedroom inventory is NOT a venue listing
  // for the organizer (common false positive in corpus summaryWhyHotel text).
  const organizerVenueListing = venueListingSignals
    || (bedroomAmenity && /\bfree\s+venue\s+find|venue\s+(?:listing|hire)|book\s+(?:the\s+)?ballroom|layout\s*&\s*spaces/i.test(blob));

  let role = "INSUFFICIENT";
  let reason = "No evidence-backed overnight accommodation sourcing or placement mechanism.";

  if (guestroomControl) {
    role = "GUESTROOM_OR_HOTEL_SOURCING_CONTROL";
    reason = "Evidence supports hotel/accommodation sourcing or booking responsibility.";
  } else if (organizerVenueListing) {
    role = "VENUE_OR_EVENT_LISTING_ONLY";
    reason = "Evidence is primarily venue/event promotion (including bedroom counts as venue amenities). Does not establish guestroom purchasing control.";
  } else if (officeOnly) {
    role = "OFFICE_LOCATION_ONLY";
    reason = "London office / geography alone does not establish overnight hotel demand.";
  }

  return {
    version: HOTEL_QUALIFICATION_V2,
    role,
    reason,
    guestroomOrHotelSourcingControl: role === "GUESTROOM_OR_HOTEL_SOURCING_CONTROL",
    venueListingOnly: role === "VENUE_OR_EVENT_LISTING_ONLY",
    officeLocationOnly: role === "OFFICE_LOCATION_ONLY",
    bedroomAmenityMentioned: bedroomAmenity,
    venueListingSignals,
  };
}

/**
 * Qualify a candidate for a target hotel under V2 commercial standards.
 * Separates pathway, stage, evidence confidence, suitability, and publication.
 */
export function qualifyHotelSpecificProspectV2({
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  targetHotel = "",
  usableContactRoute = null,
} = {}) {
  const base = evaluateCommercialQualification(
    {
      ...opportunity,
      primaryContactEmail: usableContactRoute || opportunity.primaryContactEmail || opportunity.contactEmail,
      establishedFacts: [
        ...(opportunity.establishedFacts || []),
        ...acceptedClaims.map((c) => c.statement || c.excerpt || ""),
      ],
    },
    { targetHotel: targetHotel || opportunity.targetHotel || opportunity.hotelName },
  );

  const accommodation = classifyAccommodationEvidenceRole({
    acceptedClaims,
    pageTexts,
    organizationName: opportunity.organizationName || "",
  });

  const preservedContact = usableContactRoute
    || opportunity.contactEmail
    || opportunity.primaryContactEmail
    || opportunity.email
    || opportunity.primaryContact?.email
    || opportunity.contact?.businessEmail
    || null;
  const contactOk = Boolean(preservedContact && /@/.test(String(preservedContact)));

  const evidenceBlob = [
    ...acceptedClaims.map((c) => `${c.statement || ""} ${c.excerpt || ""}`),
    ...pageTexts,
    opportunity.evidenceQuote,
    opportunity.summaryWhyHotel,
    opportunity.eventLocationSummary,
    opportunity.namedHostHotel,
    opportunity.hostHotel,
  ].join("\n");

  const competitor = namedCompetitorHost(evidenceBlob, targetHotel || opportunity.targetHotel || "")
    || opportunity.namedHostHotel
    || null;
  // Ignore target-hotel inventory prose when judging event overnight demand.
  const demandSideBlob = evidenceBlob
    .split("\n")
    .filter((line) => !TARGET_HOTEL_INVENTORY_PROSE.test(line) || EVENT_OVERNIGHT.test(line))
    .join("\n");
  const eventOvernightDemandSupported = EVENT_OVERNIGHT.test(demandSideBlob);
  const banquetOnlyWithoutOvernight = BANQUET_ONLY.test(evidenceBlob) && !eventOvernightDemandSupported
    && !accommodation.guestroomOrHotelSourcingControl;
  const futureCycleSupported = Boolean(
    competitor
    && (/overflow|future\s*cycle|next\s+(?:year|cycle|meeting)|202[8-9]|satellite|alternate\s+hotel/i.test(evidenceBlob)
      || opportunity.futureCycleSupported === true),
  );

  let internalAccountDevelopment = false;
  let eventLedInternalProspect = false;
  let competitiveFutureCycleProspect = false;
  let commerciallyUsefulInternal = false;
  let usefulKind = null;
  let pathway = base.pathway;
  let salesReadiness = base.salesReadiness;
  let commercialStage = base.commercialStage;
  let downgradeReason = null;
  let nextSalesAction = base.nextSalesAction;
  let sourcingStatus = base.sourcingStatus;

  if (competitor) {
    pathway = RESEARCH_PATHWAY.COMPETITIVE_FUTURE_CYCLE;
    sourcingStatus = "HOST_SELECTED";
    commercialStage = COMMERCIAL_STAGE.OTHER_HOST_PLACED;
    if (futureCycleSupported && contactOk) {
      competitiveFutureCycleProspect = true;
      commerciallyUsefulInternal = true;
      usefulKind = "COMPETITIVE_FUTURE_CYCLE";
      salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
      nextSalesAction = `${competitor} is the committed/announced host. Pursue future-cycle, satellite, or evidence-backed overflow only — do not present as open host business for ${targetHotel || "the target hotel"}.`;
    } else {
      salesReadiness = SALES_READINESS.EXCLUDE;
      downgradeReason = futureCycleSupported
        ? "Competitor host with future-cycle hint but no usable contact route."
        : `Committed/announced host ${competitor}; no overflow/future-cycle evidence. Unknown sourcing remains unknown — not open demand.`;
      nextSalesAction = `Do not describe this as open host business. ${competitor} is already named. Revisit only if a future cycle or overflow mechanism is evidenced.`;
    }
  } else if (accommodation.venueListingOnly) {
    internalAccountDevelopment = false;
    salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
    commercialStage = COMMERCIAL_STAGE.EXCLUDED;
    pathway = RESEARCH_PATHWAY.ACCOUNT_LED;
    downgradeReason = accommodation.reason;
    nextSalesAction = "Do not treat venue listing / bedroom amenity copy as guestroom purchasing control. Seek evidence the organization sources overnight rooms or controls residential programmes.";
  } else if (accommodation.officeLocationOnly) {
    internalAccountDevelopment = false;
    salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
    downgradeReason = accommodation.reason;
    nextSalesAction = "Office location and email are insufficient. Require overnight accommodation mechanism evidence.";
  } else if (
    accommodation.guestroomOrHotelSourcingControl
    && contactOk
    && base.dimensions?.geography?.status !== "REJECTED"
  ) {
    internalAccountDevelopment = true;
    commerciallyUsefulInternal = true;
    usefulKind = "ACCOUNT_LED";
    pathway = RESEARCH_PATHWAY.ACCOUNT_LED;
    commercialStage = COMMERCIAL_STAGE.ACCOUNT_DEVELOPMENT;
    salesReadiness = SALES_READINESS.READY_FOR_SALES;
    nextSalesAction = `Introduce ${targetHotel || "the target hotel"} for luxury / residential group hotel sourcing via ${preservedContact}. Do not claim an open RFP or existing award.`;
  } else if (banquetOnlyWithoutOvernight && contactOk) {
    // Banquet-only must be evaluated before EVENT_LED useful — hotel inventory prose is not demand.
    downgradeReason = "Banquet/reception fit without evidenced overnight accommodation demand.";
    salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
    // Preserve ACCOUNT_LED when the org was being developed as a sourcing account (DMCs), not re-label as EVENT_LED.
    if (base.pathway !== RESEARCH_PATHWAY.ACCOUNT_LED) {
      pathway = RESEARCH_PATHWAY.EVENT_LED;
    }
    nextSalesAction = "Contact is usable, but banquet-only announcements are not residential demand. Seek room-block / overnight language before pitching guestrooms.";
  } else if (
    !accommodation.venueListingOnly
    && eventOvernightDemandSupported
    && contactOk
    && base.dimensions?.geography?.status !== "REJECTED"
  ) {
    // EVENT_LED useful internal — demand-side overnight evidence + contact + fit; not fabricated open RFP
    eventLedInternalProspect = true;
    commerciallyUsefulInternal = true;
    usefulKind = "EVENT_LED";
    pathway = RESEARCH_PATHWAY.EVENT_LED;
    commercialStage = COMMERCIAL_STAGE.OPEN_HOST_PROSPECT;
    salesReadiness = SALES_READINESS.READY_FOR_SALES;
    nextSalesAction = `Ask ${preservedContact} about overnight housing needs for the evidenced meeting cycle relevant to ${targetHotel || "the target hotel"}. Do not invent a room block or open RFP.`;
  }

  // Do not leave READY_FOR_SALES on non-useful rows (base classifier can over-promote).
  if (
    !commerciallyUsefulInternal
    && salesReadiness === SALES_READINESS.READY_FOR_SALES
  ) {
    salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
    if (!nextSalesAction || /still needs rooms/i.test(String(nextSalesAction))) {
      nextSalesAction = "Keep researching the missing commercial fact before a salesperson uses this card.";
    }
  }

  return {
    version: HOTEL_QUALIFICATION_V2,
    internalOnly: true,
    productionPublicationEligibilityChanged: false,
    customerFacingClassificationChanged: false,
    pathway,
    commercialStage,
    sourcingStatus,
    evidenceConfidence: base.evidenceConfidence,
    hotelSuitability: base.dimensions?.hotelSuitability?.status || null,
    publicationEligibility: base.productionEligibility,
    salesReadiness,
    internalAccountDevelopment,
    eventLedInternalProspect,
    competitiveFutureCycleProspect,
    commerciallyUsefulInternal,
    usefulKind,
    futureCycleSupported,
    eventOvernightDemandSupported,
    banquetOnlyWithoutOvernight,
    namedCompetitorHost: competitor || null,
    accommodationEvidenceRole: accommodation,
    usableContactRoute: contactOk,
    preservedContactRoute: preservedContact,
    nextSalesAction,
    downgradeReason,
    basePathwayEvaluation: {
      pathway: base.pathway,
      salesReadiness: base.salesReadiness,
      missingForRoomDemand: base.missingForRoomDemand,
    },
    separationsPreserved: {
      commercialStageSeparateFromEvidenceConfidence: true,
      sourcingStatusSeparateFromStage: true,
      publicationEligibilitySeparateFromInternalResearch: true,
      unknownSourcingRemainsUnknown: true,
      competitorHostIsNotOpenDemand: true,
    },
  };
}
