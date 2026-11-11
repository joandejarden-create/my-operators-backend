/**
 * Association / Event publication qualification V1.
 *
 * Firm integrity (required):
 *  - Verified event or organizing entity
 *  - Credible event-related hotel-demand thesis
 *  - ≥1 named relevant buyer
 *  - Usable contact route
 *  - Hotel-specific fit appropriate to capacity/location
 *
 * Explicitly NOT required: RFP, room-night volume, room block, second buyer, win certainty.
 */

import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { hasUsableContactRoute } from "../prospect-publication-eligibility-v1.js";
import { hotelAssociationEventsFitProfile } from "./association-events-discovery-strategies-v1.js";
import {
  isAssociationEventsValidPersonName,
  scrubAssociationEventsPhone,
} from "./association-events-buyer-integrity-v1.js";
import {
  EVENT_CHRONOLOGY_STATUS,
  classifyAssociationEventsChronology,
} from "./association-events-chronology-v1.js";
import { evaluateAssociationEventsCustomerPublicationGate } from "./association-events-customer-publication-gate-v1.js";
import { classifyCommercialDemandThesis } from "../commercial-demand-thesis-standard-v1.js";

export const ASSOCIATION_EVENTS_QUALIFICATION_V1 =
  "gdi-association-events-qualification-v1";

export const ASSOCIATION_EVENTS_PUBLICATION_STATUS = Object.freeze({
  QUALIFIED: "QUALIFIED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  HOLD_INSUFFICIENT: "HOLD_INSUFFICIENT",
  EXCLUDE_WRONG_FIT: "EXCLUDE_WRONG_FIT",
  EXCLUDE_DUPLICATE: "EXCLUDE_DUPLICATE",
});

const RELEVANT_BUYER_TITLE =
  /\b(meeting|meetings|conference|congress|event|events|convention|asamblea|congreso|director|executive director|secretario|secretaria|organizer|organiser|organizing|organizador|organizadora|LOC|local organiz|PCO|procurement|sourcing|hotel|venue|planner|planning|comercial|sales operations|gerente|presidente|chair|co-?chair)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function buyerTitleScore(title) {
  const t = text(title);
  if (!t) return 0;
  let score = 1;
  if (/\b(director|manager|coordinator|planner|officer|head|lead|vp|vice president|gerente|directora|responsable|president|ceo|coo|chief)\b/i.test(t)) {
    score += 3;
  }
  if (/\b(meeting|meetings|event|events|housing|hotel|lodging|conference|procurement|travel|venue|planner)\b/i.test(t)) {
    score += 2;
  }
  // Prefer full job titles over short remit labels such as "Meetings / housing".
  if (t.length >= 24) score += 1;
  // Penalize scraped residue that embeds a person name or trailing "at".
  if (/,\s*[A-Z][a-z]+/.test(t) || /\bat\s*$/i.test(t) || /@/.test(t)) score -= 4;
  return score;
}

function preferBuyerTitle(...candidates) {
  let best = "";
  let bestScore = -1;
  for (const candidate of candidates) {
    const t = text(candidate);
    const score = buyerTitleScore(t);
    if (score > bestScore) {
      best = t;
      bestScore = score;
    }
  }
  return best || null;
}

function namedBuyers(opp = {}) {
  const out = [];
  const push = (name, title, source) => {
    const n = text(name);
    // Integrity layer: regions / org bodies / awards cannot qualify as buyers
    if (!isAssociationEventsValidPersonName(n)) return;
    out.push({ name: n, title: text(title) || null, role: text(title) || null, source: source || null });
  };
  push(
    opp.primaryContact?.name,
    preferBuyerTitle(opp.primaryContact?.role, opp.primaryContact?.title, opp.contactRole),
    "primaryContact",
  );
  push(
    opp.contactName,
    preferBuyerTitle(opp.contactRole, opp.primaryContact?.role, opp.primaryContact?.title),
    "contactName",
  );
  for (const s of opp.namedStakeholders || []) {
    push(s?.name, preferBuyerTitle(s?.role, s?.title), "namedStakeholders");
  }
  for (const s of opp.secondaryContacts || []) {
    push(s?.name, preferBuyerTitle(s?.role, s?.title), "secondaryContacts");
  }
  // Dedupe by name, keeping the richer commercial title when both paths exist.
  const byName = new Map();
  for (const person of out) {
    const key = person.name.toLowerCase();
    const prior = byName.get(key);
    if (!prior || buyerTitleScore(person.title) > buyerTitleScore(prior.title)) {
      byName.set(key, person);
    }
  }
  return [...byName.values()];
}

function commerciallyRelevantBuyer(person = {}) {
  if (!isAssociationEventsValidPersonName(person.name)) return false;
  const title = text(person.title);
  // Registration / sponsorship / pure finance staff are not lodging/sourcing buyers
  if (
    /\b(inscripci[oó]n|inscripciones|registration|registrations|patrocinio|patrocinios|sponsorship|sponsor sales|tickets?|taquilla|administraci[oó]n y finanzas|finanzas|tesorer[ií]a)\b/i.test(
      title,
    ) &&
    !/\b(hotel|hospedaje|lodging|venue|sede|room.?block|accommodation|meetings|events|congreso|sourcing|reservacion|reservaciones)\b/i.test(
      title,
    )
  ) {
    return false;
  }
  // Speaker-only titles are not buyers
  if (/\b(speaker|ponente|keynote|panelista|moderador)\b/i.test(title) &&
      !RELEVANT_BUYER_TITLE.test(title) &&
      !/director|manager|gerente|presidente|ceo|coo|organizer/i.test(title)) {
    return false;
  }
  // Named person with valid integrity-checked name may qualify while title is still being recovered
  if (!title) return true;
  return RELEVANT_BUYER_TITLE.test(title) || /director|manager|gerente|presidente|ceo|coo/i.test(title);
}

function hasOrganizingEntity(opp = {}) {
  const org = text(opp.organizationName || opp.organizer || opp.eventOwner);
  const eventName = text(opp.eventName || opp.title);
  if (org.length >= 3) return true;
  if (eventName.length >= 8 && /congress|conference|congreso|convención|meeting|symposium|asamblea/i.test(eventName)) {
    return true;
  }
  return false;
}

function hotelDemandThesis(opp = {}, hotel = {}) {
  // Independent evidence classifier is authoritative. Generated hotelOpportunityThesis
  // wording alone must not satisfy the association demand gate.
  const classified = classifyCommercialDemandThesis(opp);
  const blob = [
    opp.proposedHotelUse,
    opp.hotelDemandThesis,
    opp.hotelOpportunityThesis,
    opp.summaryWhyHotel,
    opp.summaryWhyMatters,
    opp.roomDemandRationale,
    opp.bethesdaWinThesis,
    opp.fitExplanation,
    opp.inferredHotelDemand,
    opp.localConnection,
    opp.whatIsHappening,
    opp.whyThisHotel,
    ...(Array.isArray(opp.inferredHotelDemand) ? opp.inferredHotelDemand : []),
    classified.mechanism?.excerpt || "",
  ]
    .map(text)
    .join(" ");
  const hasRoomsOrMeetings = classified.mayQualifySalesReadyDemandGate === true;
  const fit = hotelAssociationEventsFitProfile(hotel);
  const capacityOk =
    !/citywide host|10,?000.?attendee|stadium/i.test(blob) ||
    fit.positioning !== "urban_executive_meetings";
  return {
    ok: hasRoomsOrMeetings && capacityOk,
    hasRoomsOrMeetings,
    capacityOk,
    thesis: classified.mechanism?.excerpt || blob.slice(0, 400) || null,
    thesisStandard: classified.thesisStandard,
    demandReason: classified.reason,
  };
}

/**
 * Evaluate association/event opportunity for staged publication.
 */
export function evaluateAssociationEventsPublicationQualification(opportunity = {}, hotel = {}) {
  const buyers = namedBuyers(opportunity);
  const relevantBuyers = buyers.filter(commerciallyRelevantBuyer);
  const oneNamedRelevantBuyer = relevantBuyers.length >= 1;
  // Polluted phones do not count as a usable route
  const phoneOk = scrubAssociationEventsPhone(opportunity.contactPhone);
  const routeOpp = phoneOk
    ? opportunity
    : { ...opportunity, contactPhone: null };
  const route = hasUsableContactRoute(routeOpp);
  const entityOk = hasOrganizingEntity(opportunity);
  const demand = hotelDemandThesis(opportunity, hotel);
  const fitProfile = hotelAssociationEventsFitProfile(hotel);
  const chronology =
    opportunity.associationEventsChronology ||
    classifyAssociationEventsChronology(opportunity);

  // Hard customer-delivery gate: reject hotel/venue pages, placeholders, sparse CIBs.
  const customerPublicationGate =
    evaluateAssociationEventsCustomerPublicationGate(opportunity);

  const reasons = [];
  if (!entityOk) reasons.push("missing_verified_event_or_organizer");
  if (!demand.ok) reasons.push("missing_credible_hotel_demand_thesis");
  if (!oneNamedRelevantBuyer) reasons.push("missing_one_named_relevant_buyer");
  if (!route) reasons.push("missing_usable_contact_route");
  if (customerPublicationGate.ok === false) {
    for (const d of customerPublicationGate.defects || []) reasons.push(d);
  }

  // Same commercial gates — chronology / host-closed are demand-thesis integrity,
  // not new publication requirements (RFP, volume, etc. remain not required).
  if (
    opportunity.hostHotelContracted === true &&
    opportunity.externalRoomBlocksAvailable === false
  ) {
    reasons.push("missing_credible_hotel_demand_thesis");
  }
  // Host contracted but overflow/external lodging unproven → cannot QUALIFY yet
  if (
    opportunity.hostHotelContracted === true &&
    opportunity.externalRoomBlocksAvailable !== true &&
    opportunity.externalRoomBlocksAvailable !== false
  ) {
    reasons.push("missing_credible_hotel_demand_thesis");
    reasons.push("host_contracted_overflow_unproven");
  }
  if (
    (chronology.status === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED ||
      chronology.status === EVENT_CHRONOLOGY_STATUS.HISTORICALLY_RECURRING) &&
    !chronology.futureEditionVerified
  ) {
    // Past / recurring without verified next edition → not actionable demand yet
    reasons.push("missing_credible_hotel_demand_thesis");
  }
  if (opportunity.eventEntityIncorrect === true) {
    reasons.push("missing_verified_event_or_organizer");
  }
  // Rejected / contaminated contact markers
  if (opportunity.associationEventsEvidenceMerge?.blocked?.length) {
    const blockedBuyer = opportunity.associationEventsEvidenceMerge.blocked.some(
      (b) => b.field === "contactName" || b.field === "contactEmail",
    );
    if (blockedBuyer && !oneNamedRelevantBuyer) {
      reasons.push("missing_one_named_relevant_buyer");
    }
  }

  const hardIdentityDefect = (customerPublicationGate.defects || []).some((d) =>
    [
      "venue_or_hotel_as_event_identity",
      "placeholder_event_title",
      "directory_or_bureau_page",
      "hotel_list_page_not_room_block",
      "generic_hotel_marketing_snippet",
      "founder_rejected_campaign_quality",
    ].includes(d),
  );

  let status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.QUALIFIED;
  if (opportunity.crossSegmentDuplicate === true || opportunity.duplicateOf) {
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.EXCLUDE_DUPLICATE;
  } else if (
    opportunity.wrongHotelFit === true ||
    opportunity.excludeWrongFit === true ||
    hardIdentityDefect
  ) {
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.EXCLUDE_WRONG_FIT;
  } else if (
    opportunity.hostHotelContracted === true &&
    opportunity.externalRoomBlocksAvailable === false
  ) {
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.EXCLUDE_WRONG_FIT;
  } else if (
    opportunity.hostHotelContracted === true &&
    opportunity.externalRoomBlocksAvailable !== true
  ) {
    // Overflow path hypothesized but not evidenced — continue research, do not qualify
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.RESEARCH_CONTINUATION;
  } else if (reasons.length === 0) {
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.QUALIFIED;
  } else if (entityOk && (demand.hasRoomsOrMeetings || opportunity.researchContinuation !== false)) {
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.RESEARCH_CONTINUATION;
  } else {
    status = ASSOCIATION_EVENTS_PUBLICATION_STATUS.HOLD_INSUFFICIENT;
  }

  return {
    version: ASSOCIATION_EVENTS_QUALIFICATION_V1,
    status,
    qualified: status === ASSOCIATION_EVENTS_PUBLICATION_STATUS.QUALIFIED,
    researchContinuation:
      status === ASSOCIATION_EVENTS_PUBLICATION_STATUS.RESEARCH_CONTINUATION,
    customerPublicationGate,
    gates: {
      verifiedEventOrOrganizer: entityOk && chronology.status !== EVENT_CHRONOLOGY_STATUS.INCORRECT_LISTING,
      hotelDemandThesis:
        demand.ok &&
        !(
          opportunity.hostHotelContracted === true &&
          opportunity.externalRoomBlocksAvailable !== true
        ),
      oneNamedRelevantBuyer,
      usableContactRoute: Boolean(route),
      hotelSpecificFit: demand.capacityOk,
      customerPublicationIdentity: customerPublicationGate.ok === true,
    },
    namedBuyers: relevantBuyers,
    demandThesis: demand.thesis,
    chronologyStatus: chronology.status,
    fitProfile: {
      positioning: fitProfile.positioning,
      strengths: fitProfile.strengths,
      limitations: fitProfile.limitations,
    },
    reasons: [...new Set(reasons)],
    notRequired: [
      "rfp",
      "confirmed_room_night_volume",
      "guaranteed_room_block",
      "second_named_buyer",
      "proof_hotel_will_win",
    ],
  };
}

export function applyAssociationEventsGateToEligibility(baseElig = {}, opportunity = {}, hotel = {}) {
  const q = evaluateAssociationEventsPublicationQualification(opportunity, hotel);
  return {
    ...baseElig,
    associationEventsQualification: q,
    hotelFacing: q.qualified === true && baseElig.hotelFacing !== false,
    publicationStatus: q.status,
  };
}
