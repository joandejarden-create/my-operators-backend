/**
 * Demand-segment mechanism router V1
 *
 * Mechanism-first routing between Scaling Companies and Association /
 * Conference / Events (research axis: associations_government).
 *
 * Does NOT invent a new RESEARCH_SEGMENT. Dual opportunities require two
 * distinct demand mechanisms and distinct opportunity IDs, linked by entity.
 *
 * Zero-paid / offline safe. Callers must not treat this as publication.
 */

import { RESEARCH_SEGMENT } from "../workflow/hotel-segment-research-plan-v1.js";
import { OPPORTUNITY_TYPE_FILTER } from "../opportunity-type-filter-v1.js";

export const DEMAND_SEGMENT_MECHANISM_ROUTER_V1 =
  "gdi-demand-segment-mechanism-router-v1";

export const DEMAND_MECHANISM_CLASS = Object.freeze({
  ONGOING_CORPORATE_ACCOUNT: "ongoing_corporate_account",
  ORGANIZED_EVENT_GATHERING: "organized_event_gathering",
  INCENTIVE_RECOGNITION_TRAVEL: "incentive_recognition_travel",
  CORPORATE_MEETING_RETREAT: "corporate_meeting_retreat",
  ASSOCIATION_CONGRESS: "association_congress",
  TRADE_SHOW_EXHIBITOR: "trade_show_exhibitor",
  HISTORICAL_EVENT_RELATIONSHIP: "historical_event_relationship",
  WEAK_OR_UNCLEAR: "weak_or_unclear",
});

export const EVENT_SUBTYPE = Object.freeze({
  ASSOCIATION_LED: "association_led",
  CORPORATE_EVENT: "corporate_event",
  INCENTIVE_RECOGNITION: "incentive_recognition",
  TRADE_SHOW: "trade_show",
  TRAINING_PROGRAMME: "training_programme",
  RECURRING_SERIES_EDITION: "recurring_series_edition",
  OTHER_EVENT_LED: "other_event_led",
});

const EVENT_KEYWORDS =
  /\b(passport to paradise|gold trip|president'?s club|incentive|recognition trip|congress|conference|summit|symposium|retreat|kickoff|kick-off|annual meeting|trade show|expo(?:sition)?|convention|room block|delegate|attendee|venue|resort stay)\b/i;

const SCALING_KEYWORDS =
  /\b(new office|hq|headquarters|operating hub|expansion|mobilization|implementation|cross-market team|visiting team|corporate account|travel-producing|transformation|hiring surge|funding round)\b/i;

const VENUE_COMMITTED =
  /\b(nizuc|ava resort|majestic elegance|committed venue|existing venue|already booked|host hotel)\b/i;

function text(v) {
  return String(v || "").trim();
}

function blobOf(input = {}) {
  const parts = [
    input.organizationName,
    input.companyName,
    input.title,
    input.displayTitle,
    input.eventName,
    input.canonicalEventName,
    input.summaryWhat,
    input.whatIsHappening,
    input.demandMechanism,
    input.demand?.mechanism,
    input.commercialDisposition,
    input.disposition,
    typeof input.commercialIntelligenceBrief?.whatIsHappening === "string"
      ? input.commercialIntelligenceBrief.whatIsHappening
      : "",
    typeof input.commercialIntelligenceBrief?.opportunityOverview === "string"
      ? input.commercialIntelligenceBrief.opportunityOverview
      : "",
  ];
  return parts.map(text).filter(Boolean).join(" \n ").toLowerCase();
}

function classifyMechanism(blob, input = {}) {
  const explicit = text(input.demandMechanismClass || input.mechanismClass);
  if (explicit && DEMAND_MECHANISM_CLASS[explicit]) return explicit;

  const incentive =
    /\b(incentive|gold trip|passport to paradise|president'?s club|recognition)\b/i.test(
      blob,
    );
  const congress = /\b(congress|association|annual meeting|symposium)\b/i.test(blob);
  const corporateEvent =
    /\b(retreat|kickoff|kick-off|leadership meeting|sales meeting|internal conference|training gathering)\b/i.test(
      blob,
    );
  const trade = /\b(trade show|expo(?:sition)?|exhibitor|sponsor)\b/i.test(blob);
  const historical =
    /\b(historical|took place|past edition|21st annual|previous host)\b/i.test(blob) &&
    EVENT_KEYWORDS.test(blob);
  const eventLed = EVENT_KEYWORDS.test(blob) || Boolean(text(input.eventName));
  const scalingLed =
    SCALING_KEYWORDS.test(blob) &&
    !incentive &&
    !congress &&
    !(eventLed && !/\b(office|hub|expansion|mobilization|account)\b/i.test(blob));

  if (incentive) return DEMAND_MECHANISM_CLASS.INCENTIVE_RECOGNITION_TRAVEL;
  if (trade) return DEMAND_MECHANISM_CLASS.TRADE_SHOW_EXHIBITOR;
  if (congress) return DEMAND_MECHANISM_CLASS.ASSOCIATION_CONGRESS;
  if (corporateEvent) return DEMAND_MECHANISM_CLASS.CORPORATE_MEETING_RETREAT;
  if (historical) return DEMAND_MECHANISM_CLASS.HISTORICAL_EVENT_RELATIONSHIP;
  if (eventLed) return DEMAND_MECHANISM_CLASS.ORGANIZED_EVENT_GATHERING;
  if (scalingLed) return DEMAND_MECHANISM_CLASS.ONGOING_CORPORATE_ACCOUNT;
  return DEMAND_MECHANISM_CLASS.WEAK_OR_UNCLEAR;
}

function eventSubtypeFor(mechanism) {
  switch (mechanism) {
    case DEMAND_MECHANISM_CLASS.INCENTIVE_RECOGNITION_TRAVEL:
      return EVENT_SUBTYPE.INCENTIVE_RECOGNITION;
    case DEMAND_MECHANISM_CLASS.ASSOCIATION_CONGRESS:
      return EVENT_SUBTYPE.ASSOCIATION_LED;
    case DEMAND_MECHANISM_CLASS.CORPORATE_MEETING_RETREAT:
    case DEMAND_MECHANISM_CLASS.ORGANIZED_EVENT_GATHERING:
      return EVENT_SUBTYPE.CORPORATE_EVENT;
    case DEMAND_MECHANISM_CLASS.TRADE_SHOW_EXHIBITOR:
      return EVENT_SUBTYPE.TRADE_SHOW;
    case DEMAND_MECHANISM_CLASS.HISTORICAL_EVENT_RELATIONSHIP:
      return EVENT_SUBTYPE.INCENTIVE_RECOGNITION;
    default:
      return EVENT_SUBTYPE.OTHER_EVENT_LED;
  }
}

function isEventsPrimary(mechanism) {
  return (
    mechanism === DEMAND_MECHANISM_CLASS.ORGANIZED_EVENT_GATHERING ||
    mechanism === DEMAND_MECHANISM_CLASS.INCENTIVE_RECOGNITION_TRAVEL ||
    mechanism === DEMAND_MECHANISM_CLASS.CORPORATE_MEETING_RETREAT ||
    mechanism === DEMAND_MECHANISM_CLASS.ASSOCIATION_CONGRESS ||
    mechanism === DEMAND_MECHANISM_CLASS.TRADE_SHOW_EXHIBITOR ||
    mechanism === DEMAND_MECHANISM_CLASS.HISTORICAL_EVENT_RELATIONSHIP
  );
}

/**
 * Route one researched opportunity / candidate by demand mechanism.
 *
 * @param {object} input
 * @returns {object} routing decision (never mutates input)
 */
export function routeDemandSegmentByMechanism(input = {}) {
  const blob = blobOf(input);
  const mechanism = classifyMechanism(blob, input);
  const venueCommitted = VENUE_COMMITTED.test(blob);
  const historicalOnly =
    mechanism === DEMAND_MECHANISM_CLASS.HISTORICAL_EVENT_RELATIONSHIP;
  const orgKey = text(
    input.organizationKey ||
      input.companyKey ||
      input.organizationName ||
      input.companyName ||
      input.id ||
      "",
  )
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");

  const eventsPrimary = isEventsPrimary(mechanism);
  const scalingPrimary =
    mechanism === DEMAND_MECHANISM_CLASS.ONGOING_CORPORATE_ACCOUNT;

  // Dual only when caller supplies independent secondary mechanism evidence.
  const secondaryMechanism = text(input.secondaryDemandMechanismClass);
  const dualEligible =
    Boolean(secondaryMechanism) &&
    secondaryMechanism !== mechanism &&
    ((eventsPrimary &&
      secondaryMechanism === DEMAND_MECHANISM_CLASS.ONGOING_CORPORATE_ACCOUNT) ||
      (scalingPrimary && isEventsPrimary(secondaryMechanism)));

  let primaryResearchSegment = null;
  let customerFilterKey = OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED;
  let confidence = "MEDIUM";
  let commercialNote = "";

  if (eventsPrimary) {
    primaryResearchSegment = RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
    customerFilterKey = OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP;
    confidence = historicalOnly || venueCommitted ? "MEDIUM" : "HIGH";
    commercialNote = historicalOnly
      ? "Past event evidences buying behavior; not automatically an upcoming room-block or host-displacement opportunity."
      : venueCommitted
        ? "Event demand evidenced with competing/committed venue — overflow or relationship only when separately evidenced."
        : "Primary thesis is an organized gathering / incentive / conference — route to Association / Conference / Events.";
  } else if (scalingPrimary) {
    primaryResearchSegment = RESEARCH_SEGMENT.SCALING_COMPANIES;
    customerFilterKey = OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY;
    confidence = "HIGH";
    commercialNote =
      "Primary thesis is ongoing/emerging corporate account development from operating growth.";
  } else {
    primaryResearchSegment = null;
    confidence = "MANUAL_REVIEW";
    commercialNote =
      "Insufficient mechanism evidence to auto-route; do not guess from organizer type alone.";
  }

  return {
    version: DEMAND_SEGMENT_MECHANISM_ROUTER_V1,
    opportunityId: input.id || input.opportunityId || null,
    organizationKey: orgKey || null,
    demandMechanismClass: mechanism,
    eventSubtype: eventsPrimary ? eventSubtypeFor(mechanism) : null,
    primaryResearchSegment,
    secondaryResearchSegments: dualEligible
      ? [
          secondaryMechanism === DEMAND_MECHANISM_CLASS.ONGOING_CORPORATE_ACCOUNT
            ? RESEARCH_SEGMENT.SCALING_COMPANIES
            : RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
        ]
      : [],
    customerFilterKey,
    customerSegmentLabel:
      primaryResearchSegment === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT
        ? "Association / Conference / Events"
        : primaryResearchSegment === RESEARCH_SEGMENT.SCALING_COMPANIES
          ? "Scaling Companies"
          : null,
    dualOpportunityEligible: dualEligible,
    linkEntityKey: orgKey || null,
    venueCommitted,
    historicalEventOnly: historicalOnly,
    // Past event ≠ confirmed future edition / available block / displace prior host
    historicalEventRule:
      "A past event is evidence of meeting or incentive buying behavior — not automatically an upcoming event, available room block, or opportunity to displace the previous host hotel.",
    confidence,
    commercialNote,
    applyToLiveRecords: false,
    method: "demand_mechanism_first",
  };
}

/**
 * Apply a safe local-preview reclassification stamp without deleting history.
 * Returns a shallow-cloned opportunity; never mutates the original object.
 */
export function applyLocalPreviewSegmentStamp(opportunity = {}, routing = null) {
  const route = routing || routeDemandSegmentByMechanism(opportunity);
  if (!route.primaryResearchSegment) {
    return {
      ...opportunity,
      demandSegmentRouting: route,
      segmentReclassificationPreview: true,
      segmentReclassificationApplied: false,
    };
  }

  const prior = {
    researchSegment: opportunity.researchSegment || opportunity.segment || null,
    segment: opportunity.segment || null,
    opportunityType: opportunity.opportunityType || null,
  };

  const isEvents =
    route.primaryResearchSegment === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;

  return {
    ...opportunity,
    // Preserve immutable originals for audit / supersession
    priorResearchSegment: prior.researchSegment,
    priorSegmentLabel: prior.segment,
    priorOpportunityType: prior.opportunityType,
    researchSegment: route.primaryResearchSegment,
    segment: isEvents
      ? "Association / Conference"
      : opportunity.segment || "scaling_companies",
    segmentDisplayLabel: route.customerSegmentLabel,
    eventSubtype: route.eventSubtype || opportunity.eventSubtype || null,
    demandMechanismClass: route.demandMechanismClass,
    demandSegmentRouting: route,
    segmentReclassificationPreview: true,
    segmentReclassificationApplied: true,
    // Keep Scaling CIB / IDs; Events presentation still uses shared drawer when CIB present
    doesNotAffectProduction: true,
  };
}

/**
 * Condesa live-campaign curated hypotheses (evidence-backed, zero-paid).
 * Used only for local preview overlays — not production mutation.
 */
export const CONDESA_LIVE_RECLASS_HYPOTHESES_V1 = Object.freeze({
  gdi_sc_live_healthyhome_llc_37ebcf2e56: {
    proposedPrimary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    eventSubtype: EVENT_SUBTYPE.INCENTIVE_RECOGNITION,
    rationale:
      "Passport to Paradise Cancún 2026 is a named incentive programme — event/incentive thesis, not ongoing corporate account.",
    dispositionPreserved: "COMMERCIALLY_REVIEWABLE",
    dosReview:
      "Worth sales action subject to venue/hotel eligibility confirmation; do not claim booked at Fiesta Americana.",
  },
  gdi_sc_live_continental_aa3558d3b1: {
    proposedPrimary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    eventSubtype: EVENT_SUBTYPE.INCENTIVE_RECOGNITION,
    rationale:
      "Historical GOLD Trip at Costa Mujeres evidences incentive buying behavior — relationship/event path, not confirmed future Scaling account.",
    dispositionPreserved: "COMMERCIALLY_REVIEWABLE_RELATIONSHIP",
    dosReview:
      "Worth relationship development; past trip ≠ upcoming Condesa booking.",
  },
  gdi_sc_live_livepure_aeca81f6e3: {
    proposedPrimary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    eventSubtype: EVENT_SUBTYPE.INCENTIVE_RECOGNITION,
    rationale:
      "U.S. Brand Partner incentive at AVA Resort Cancún — event/incentive with committed competing venue.",
    dispositionPreserved: "WATCH_VENUE_COMMITTED",
    dosReview: "Watch / overflow-only unless evidence of alternative lodging demand.",
  },
  gdi_sc_live_palo_alto_networks_4d44e55d8e: {
    proposedPrimary: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    eventSubtype: EVENT_SUBTYPE.CORPORATE_EVENT,
    rationale:
      "President’s Club Cancun 2026 at NIZUC — corporate recognition event with committed venue.",
    dispositionPreserved: "WATCH_VENUE_COMMITTED",
    dosReview: "Watch; do not present as open Scaling corporate account.",
  },
  gdi_sc_live_grupo_aeroportuario_del_sure_9956a503f6: {
    proposedPrimary: RESEARCH_SEGMENT.SCALING_COMPANIES,
    eventSubtype: null,
    rationale:
      "Airport operator investment / operating activity thesis — Scaling Companies subject to beachfront AI hotel-fit eligibility (currently weak).",
    dispositionPreserved: "WATCH_WEAK_HOTEL_FIT",
    dosReview:
      "Correctly remains Watch on hotel-fit; do not promote solely to fill Scaling inventory.",
  },
});

export function routeCondesaLiveOpportunity(opportunity = {}) {
  const id = String(opportunity.id || opportunity.opportunityId || "");
  const hypo = CONDESA_LIVE_RECLASS_HYPOTHESES_V1[id];
  const auto = routeDemandSegmentByMechanism(opportunity);
  if (!hypo) {
    return { ...auto, curatedHypothesis: null };
  }
  return {
    ...auto,
    primaryResearchSegment: hypo.proposedPrimary,
    eventSubtype: hypo.eventSubtype,
    customerFilterKey:
      hypo.proposedPrimary === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT
        ? OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
        : OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY,
    customerSegmentLabel:
      hypo.proposedPrimary === RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT
        ? "Association / Conference / Events"
        : "Scaling Companies",
    confidence: "HIGH",
    commercialNote: hypo.rationale,
    dispositionPreserved: hypo.dispositionPreserved,
    dosReview: hypo.dosReview,
    curatedHypothesis: hypo,
    method: "condesa_live_curated_hypothesis_v1",
  };
}
