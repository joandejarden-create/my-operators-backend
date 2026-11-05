/**
 * Structured unresolved commercial questions for Association / Event investigation.
 */

export const ASSOCIATION_EVENTS_UNRESOLVED_QUESTIONS_V1 =
  "gdi-association-events-unresolved-questions-v1";

export const ASSOC_EVENT_QUESTION = Object.freeze({
  WHO_CONTROLS_HOTEL_SOURCING: "WHO_CONTROLS_HOTEL_SOURCING?",
  IS_ACCOMMODATION_CONTRACTED: "IS_ACCOMMODATION_CONTRACTED?",
  ARE_EXTERNAL_ROOM_BLOCKS_AVAILABLE: "ARE_EXTERNAL_ROOM_BLOCKS_AVAILABLE?",
  WHO_MANAGES_SPEAKER_TRAVEL: "WHO_MANAGES_SPEAKER_TRAVEL?",
  IS_THERE_A_CONFIRMED_NEXT_EDITION: "IS_THERE_A_CONFIRMED_NEXT_EDITION?",
  COULD_THE_HOTEL_HOST_ANCILLARY_EVENTS: "COULD_THE_HOTEL_HOST_ANCILLARY_EVENTS?",
  IS_THIS_HOTEL_PRACTICALLY_COMPETITIVE: "IS_THIS_HOTEL_PRACTICALLY_COMPETITIVE?",
  IS_THE_EVENT_ENTITY_CORRECT: "IS_THE_EVENT_ENTITY_CORRECT?",
  IS_THE_CONTACT_ROUTE_USABLE: "IS_THE_CONTACT_ROUTE_USABLE?",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Derive which commercial questions remain open for an opportunity.
 */
export function deriveAssociationEventsUnresolvedQuestions(opportunity = {}, hotel = {}) {
  const q = [];
  const chrono = opportunity.associationEventsChronology || {};
  const demand = text(
    opportunity.hotelDemandThesis ||
      opportunity.proposedHotelUse ||
      opportunity.demandMechanism ||
      "",
  );
  const venue = text(
    opportunity.venueName ||
      opportunity.hostVenue ||
      opportunity.mainVenue ||
      opportunity.buyingAndSourcingStatus ||
      "",
  );
  const buyerOk = Boolean(
    opportunity.contactName &&
      opportunity.associationEventsBuyerIntegrity?.ok !== false,
  );
  const routeOk = Boolean(
    opportunity.contactEmail || opportunity.contactPhone || opportunity.contactUrl,
  );

  if (opportunity.eventEntityIncorrect || chrono.status === "INCORRECT_LISTING") {
    q.push(ASSOC_EVENT_QUESTION.IS_THE_EVENT_ENTITY_CORRECT);
  }
  if (!buyerOk) {
    q.push(ASSOC_EVENT_QUESTION.WHO_CONTROLS_HOTEL_SOURCING);
  }
  if (!routeOk) {
    q.push(ASSOC_EVENT_QUESTION.IS_THE_CONTACT_ROUTE_USABLE);
  }
  if (/host|contracted|four seasons|dreams natura|club de industriales|sede oficial/i.test(venue + demand) ||
      opportunity.hostHotelContracted === true) {
    q.push(ASSOC_EVENT_QUESTION.IS_ACCOMMODATION_CONTRACTED);
    q.push(ASSOC_EVENT_QUESTION.ARE_EXTERNAL_ROOM_BLOCKS_AVAILABLE);
    q.push(ASSOC_EVENT_QUESTION.COULD_THE_HOTEL_HOST_ANCILLARY_EVENTS);
  } else if (/canc[uú]n center|expo reforma|centro banamex|convention center|centro de convenciones/i.test(venue + demand + text(opportunity.eventName))) {
    q.push(ASSOC_EVENT_QUESTION.ARE_EXTERNAL_ROOM_BLOCKS_AVAILABLE);
    q.push(ASSOC_EVENT_QUESTION.WHO_CONTROLS_HOTEL_SOURCING);
  }
  if (
    chrono.status === "PAST_COMPLETED" ||
    chrono.status === "HISTORICALLY_RECURRING"
  ) {
    q.push(ASSOC_EVENT_QUESTION.IS_THERE_A_CONFIRMED_NEXT_EDITION);
  }
  if (!text(opportunity.whyThisHotel) && hotel?.displayName) {
    q.push(ASSOC_EVENT_QUESTION.IS_THIS_HOTEL_PRACTICALLY_COMPETITIVE);
  }
  if (/speaker|ponente|vip/i.test(demand) && !buyerOk) {
    q.push(ASSOC_EVENT_QUESTION.WHO_MANAGES_SPEAKER_TRAVEL);
  }

  // Dedupe preserve order
  const seen = new Set();
  return {
    version: ASSOCIATION_EVENTS_UNRESOLVED_QUESTIONS_V1,
    questions: q.filter((x) => {
      if (seen.has(x)) return false;
      seen.add(x);
      return true;
    }),
  };
}

/**
 * Map a question to the next investigation strategy id (association-specific).
 */
export function selectStrategyForUnresolvedQuestion(question, attempts = []) {
  const tried = new Set(
    (attempts || []).map((a) => a.strategyId).filter(Boolean),
  );
  const map = {
    [ASSOC_EVENT_QUESTION.IS_THE_EVENT_ENTITY_CORRECT]: [
      "OFFICIAL_EVENT_SITE",
      "ORGANIZER_IDENTITY",
      "CHRONOLOGY_SOURCES",
    ],
    [ASSOC_EVENT_QUESTION.IS_THERE_A_CONFIRMED_NEXT_EDITION]: [
      "FUTURE_EDITION_ANNOUNCEMENT",
      "OFFICIAL_EVENT_SITE",
      "CHRONOLOGY_SOURCES",
    ],
    [ASSOC_EVENT_QUESTION.WHO_CONTROLS_HOTEL_SOURCING]: [
      "LOC_PCO_ORGANIZER_PAGES",
      "OFFICIAL_CONTACT_PAGES",
      "HOUSING_ACCOMMODATION_PAGES",
    ],
    [ASSOC_EVENT_QUESTION.IS_ACCOMMODATION_CONTRACTED]: [
      "HOUSING_ACCOMMODATION_PAGES",
      "HOST_VENUE_EVIDENCE",
      "OFFICIAL_EVENT_SITE",
    ],
    [ASSOC_EVENT_QUESTION.ARE_EXTERNAL_ROOM_BLOCKS_AVAILABLE]: [
      "HOUSING_ACCOMMODATION_PAGES",
      "HOST_VENUE_EVIDENCE",
      "PRIOR_EDITION_HOTEL_ARRANGEMENTS",
    ],
    [ASSOC_EVENT_QUESTION.WHO_MANAGES_SPEAKER_TRAVEL]: [
      "LOC_PCO_ORGANIZER_PAGES",
      "OFFICIAL_CONTACT_PAGES",
    ],
    [ASSOC_EVENT_QUESTION.COULD_THE_HOTEL_HOST_ANCILLARY_EVENTS]: [
      "ANCILLARY_DEMAND_HYPOTHESIS",
      "HOST_VENUE_EVIDENCE",
    ],
    [ASSOC_EVENT_QUESTION.IS_THIS_HOTEL_PRACTICALLY_COMPETITIVE]: [
      "HOTEL_FIT_REASSESSMENT",
      "GEOGRAPHY_PROXIMITY",
    ],
    [ASSOC_EVENT_QUESTION.IS_THE_CONTACT_ROUTE_USABLE]: [
      "OFFICIAL_CONTACT_PAGES",
      "LOC_PCO_ORGANIZER_PAGES",
    ],
  };
  const sequence = map[question] || ["OFFICIAL_EVENT_SITE", "OFFICIAL_CONTACT_PAGES"];
  for (const id of sequence) {
    if (!tried.has(id)) return id;
  }
  return null;
}
