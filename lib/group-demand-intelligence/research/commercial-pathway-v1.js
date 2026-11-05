/**
 * Internal commercial pathway qualification.
 * Does not change production publication eligibility or customer classifications.
 * Pathway, commercial stage, sourcing status, and evidence confidence stay separate.
 */

import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { readRecurringFromOpportunity } from "./recurring-meeting-intelligence-v1.js";

export const RESEARCH_PATHWAY = Object.freeze({
  EVENT_LED: "EVENT_LED",
  ACCOUNT_LED: "ACCOUNT_LED",
  COMPETITIVE_FUTURE_CYCLE: "COMPETITIVE_FUTURE_CYCLE",
});

export const COMMERCIAL_STAGE = Object.freeze({
  OPEN_HOST_PROSPECT: "OPEN_HOST_PROSPECT",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  OTHER_HOST_PLACED: "OTHER_HOST_PLACED",
  EXCLUDED: "EXCLUDED",
});

export const SALES_READINESS = Object.freeze({
  READY_FOR_SALES: "READY_FOR_SALES",
  NEEDS_RESEARCH: "NEEDS_RESEARCH",
  EXCLUDE: "EXCLUDE",
});

const GENERIC_FIT = /fit reflects physical capacity, demand territory/i;
const ROOM_DEMAND = /room block|hotel rooms|room nights|overnight|accommodation|delegate housing|group rooms/i;
const OVERFLOW = /overflow (?:rooms|block|hotel|available)|rooms? still (?:needed|available)|alternate hotel (?:not named|unselected)/i;
const ACCOUNT_SIGNAL = /\bdmc\b|destination management|incentive travel|professional planner/i;
const EVENT_SIGNAL = /conference|summit|annual meeting|user conference|institute/i;
const NAMED_HOSTS = [
  [/crystal gateway marriott/i, "Crystal Gateway Marriott"],
  [/hilton san diego bayfront/i, "Hilton San Diego Bayfront"],
  [/catamaran resort/i, "The Catamaran Resort"],
  [/bethesda north marriott|\bbethesda north\b/i, "Bethesda North Marriott"],
  [/philadelphia marriott downtown/i, "Philadelphia Marriott Downtown"],
  [/philadelphia marriott\b/i, "Philadelphia Marriott Downtown"],
  [/hyatt regency washington/i, "Hyatt Regency Washington"],
];

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function evidenceText(opp = {}) {
  return [
    opp.evidenceQuote,
    opp.hotelOpportunityThesis,
    opp.eventLocationSummary,
    opp.organizationName,
    opp.title,
    ...(Array.isArray(opp.establishedFacts) ? opp.establishedFacts : []),
    ...(Array.isArray(opp.evidence) ? opp.evidence.map((item) => item?.quote || item?.evidenceQuote || "") : []),
  ]
    .map(text)
    .filter(Boolean)
    .join("\n");
}

function fitProse(opp = {}) {
  return text(opp.hotelFitRationale || opp.fitExplanation || opp.hotelFit?.rationale || "");
}

export function namedCompetitorHost(quote, targetHotel = "") {
  const blob = text(quote);
  const target = text(targetHotel).toLowerCase();
  for (const [pattern, name] of NAMED_HOSTS) {
    if (!pattern.test(blob)) continue;
    const host = name.toLowerCase();
    const same =
      target &&
      (target.includes(host) || host.includes(target)) &&
      !(host.includes("bethesda north") && target.includes("bethesda") && !target.includes("north"));
    if (!same) return name;
  }
  return null;
}

function dimension(status, evidence) {
  return { status, evidence: evidence || null };
}

function confidenceFor(dimensions, roomDemand) {
  const supported = Object.values(dimensions).filter((item) => item.status === "SUPPORTED").length;
  if (roomDemand === "SUPPORTED" && supported >= 5) return "HIGH";
  if (supported >= 3) return "MEDIUM";
  if (supported >= 1) return "LOW";
  return "NONE";
}

/**
 * @param {object} opp Evidence-bearing opportunity. Not modified.
 * @param {{ targetHotel?: string }} [options]
 */
export function evaluateCommercialQualification(opp = {}, options = {}) {
  const quote = evidenceText(opp);
  const targetHotel = options.targetHotel || opp.targetHotel || opp.hotelName || "";
  const competitor = namedCompetitorHost(quote, targetHotel);
  const overflow = OVERFLOW.test(quote);
  const account = ACCOUNT_SIGNAL.test(`${opp.organizationName || ""} ${quote}`);
  const dated = Boolean(opp.eventStartDate || opp.eventEndDate) || /\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}/i.test(quote);
  const recurring = /every (?:two|2) years|annual|recurring|ongoing programmes|incentive travel/i.test(quote);
  const roomLanguage = ROOM_DEMAND.test(quote) && !competitor;
  const genericOnly = GENERIC_FIT.test(fitProse(opp)) && !ROOM_DEMAND.test(fitProse(opp)) && !competitor;
  const email = text(opp.primaryContactEmail || opp.primaryContact?.email || opp.contact?.businessEmail);
  const identitySupported = Boolean(text(opp.organizationName) && (opp.officialSource || opp.evidenceQuote || (opp.evidence || []).length || opp.sources?.length));
  const mechanismSupported = account
    ? /incentive travel|venue sourcing|destination management|group programme|plan a programme/i.test(quote)
    : EVENT_SIGNAL.test(quote);
  const geographySupported = targetHotel
    ? new RegExp(text(targetHotel).split(/\s+/).slice(-1)[0] || "hotel", "i").test(quote) || /london|san diego|bethesda|arlington|washington/i.test(quote)
    : /london|san diego|bethesda|arlington/i.test(quote);
  const hotelSuitability = competitor
    ? "REJECTED"
    : genericOnly
      ? "MISSING"
      : roomLanguage
        ? "SUPPORTED"
        : "MISSING";
  const timingSupported = account ? recurring || mechanismSupported : dated;
  const contactSupported = /@/.test(email);

  let pathway = RESEARCH_PATHWAY.EVENT_LED;
  if (competitor) pathway = RESEARCH_PATHWAY.COMPETITIVE_FUTURE_CYCLE;
  else if (account && !dated) pathway = RESEARCH_PATHWAY.ACCOUNT_LED;
  else if (account && dated) pathway = RESEARCH_PATHWAY.EVENT_LED;

  const sourcingStatus = competitor ? "HOST_SELECTED" : text(opp.venueSourcingStatus || "UNKNOWN") || "UNKNOWN";
  const openHostDemand = !competitor && hotelSuitability === "SUPPORTED" && sourcingStatus !== "HOST_SELECTED";
  const futureCycle = Boolean(competitor && overflow);

  const dimensions = {
    identity: dimension(identitySupported ? "SUPPORTED" : "MISSING", identitySupported ? text(opp.organizationName) : null),
    demandMechanism: dimension(mechanismSupported ? "SUPPORTED" : "MISSING", mechanismSupported ? "Quoted activity or service line" : null),
    geography: dimension(competitor ? "REJECTED" : geographySupported ? "SUPPORTED" : "MISSING", competitor || null),
    hotelSuitability: dimension(hotelSuitability, competitor ? `Named host: ${competitor}` : genericOnly ? "Generic fit prose" : null),
    timing: dimension(timingSupported ? "SUPPORTED" : "MISSING", account && !dated ? "Recurring programme potential, no dated occasion required" : null),
    sourcing: dimension(competitor ? "SUPPORTED" : sourcingStatus === "UNKNOWN" ? "MISSING" : "SUPPORTED", competitor ? `${competitor} is the host` : sourcingStatus),
    contact: dimension(contactSupported ? "SUPPORTED" : "MISSING", email || null),
    hotelRoomDemand: dimension(roomLanguage ? "SUPPORTED" : "MISSING", roomLanguage ? "Quote states room demand not tied to a competitor host" : null),
  };

  const missingForRoomDemand = [];
  if (!roomLanguage && !competitor) {
    missingForRoomDemand.push("A source sentence that this organization places overnight hotel rooms, not only an office address or an email.");
  }
  if (account && !/portman|marylebone|central london hotel/i.test(quote) && /nobu|portman/i.test(targetHotel)) {
    missingForRoomDemand.push("Evidence the groups use central London hotels that Nobu Portman Square can serve.");
  }
  if (competitor && !overflow) {
    missingForRoomDemand.push(`No quote supports overflow or an uncontracted future cycle beyond ${competitor}.`);
  }
  if (!contactSupported) missingForRoomDemand.push("A contact route that belongs to this organization.");

  let salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
  let commercialStage = pathway === RESEARCH_PATHWAY.ACCOUNT_LED ? COMMERCIAL_STAGE.ACCOUNT_DEVELOPMENT : COMMERCIAL_STAGE.OPEN_HOST_PROSPECT;
  if (competitor && !futureCycle) {
    salesReadiness = SALES_READINESS.EXCLUDE;
    commercialStage = COMMERCIAL_STAGE.OTHER_HOST_PLACED;
  } else if (competitor && futureCycle) {
    salesReadiness = SALES_READINESS.NEEDS_RESEARCH;
    commercialStage = COMMERCIAL_STAGE.OTHER_HOST_PLACED;
  } else if (
    identitySupported &&
    mechanismSupported &&
    contactSupported &&
    hotelSuitability === "SUPPORTED" &&
    timingSupported &&
    (pathway !== RESEARCH_PATHWAY.ACCOUNT_LED || roomLanguage)
  ) {
    salesReadiness = SALES_READINESS.READY_FOR_SALES;
  }

  const production = evaluateProspectPublicationEligibility(opp);
  const nextSalesAction = salesReadiness === SALES_READINESS.EXCLUDE
    ? `${competitor} is already the host. Do not present this as open host-hotel business.`
    : salesReadiness === SALES_READINESS.READY_FOR_SALES
      ? `Ask ${email || "the organization contact"} about the next group that still needs rooms. Do not claim a booking.`
      : pathway === RESEARCH_PATHWAY.ACCOUNT_LED
        ? "Account-development research is still open. Office location and a published email are not room demand."
        : "Keep researching the missing commercial fact before a salesperson uses this card.";

  return {
    internalOnly: true,
    productionPublication: false,
    customerVisible: false,
    pathway,
    commercialStage,
    sourcingStatus: competitor ? "HOST_SELECTED" : sourcingStatus,
    evidenceConfidence: confidenceFor(dimensions, dimensions.hotelRoomDemand.status),
    confidenceDoesNotSetStage: true,
    openHostDemand,
    futureCycleSupported: futureCycle,
    namedHost: competitor,
    salesReadiness,
    dimensions,
    missingForRoomDemand,
    nextSalesAction,
    productionHotelFacing: production.hotelFacing === true,
    productionEligibility: production.eligibility || null,
    productionReasons: production.reasons || [],
    recurring: readRecurringFromOpportunity(opp, { ...options, targetHotel }),
  };
}
