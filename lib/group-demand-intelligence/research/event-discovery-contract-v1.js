/**
 * Association / Conference / Events — discovery contract V1 (gates, tracks, ranking).
 * Authoritative Agent prompts/schemas live under exa-agent/contracts + schemas.
 * This module provides offline evaluation, dispositions, and CIB mapping helpers.
 */

import { RESEARCH_SEGMENT } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  EXA_EVENTS_DISCOVERY_CONTRACT_V1,
  EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
} from "./exa-agent/exa-agent-events-contracts-v1.js";
import {
  buildEventsDiscoveryAgentPayloadPreview,
  buildEventsDeepResearchAgentPayloadPreview,
} from "./exa-agent/exa-agent-events-ors-strategy-v1.js";

export const EVENT_DISCOVERY_CONTRACT_V1 = EXA_EVENTS_DISCOVERY_CONTRACT_V1;
export const EVENT_DEEP_RESEARCH_CONTRACT_V1 =
  EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1;

export {
  buildEventsDiscoveryAgentPayloadPreview,
  buildEventsDeepResearchAgentPayloadPreview,
};

export const EVENT_DISCOVERY_REQUIRED_FIELDS = Object.freeze([
  "eventOrProgrammeName",
  "organizerOrSponsoringOrganization",
  "eventType",
  "dateOrPlanningWindow",
  "hostMarketOrDestination",
  "verifiedVenueIfKnown",
  "recurringActivityEvidence",
  "roomNightOrMeetingDemandMechanism",
  "participantPopulationEvidence",
  "hotelSpecificCommercialFit",
  "competingVenueCommitments",
  "namedSourcingOrOrganizingContacts",
  "evidenceAndUncertainties",
  "recommendedSalesAction",
]);

export const EVENT_DISCOVERY_PROHIBITIONS = Object.freeze([
  "Invent attendance figures",
  "Invent room blocks or availability",
  "Invent dates or contracting authority",
  "Assume future recurrence without evidence",
  "Treat a past event as an upcoming booking",
  "Treat a generic registration email as a verified named buyer",
  "Collapse organizer, PCO, venue team, and buyer into one contact",
  "Auto-route to Scaling Companies solely because the organizer is a corporation",
  "Display historical event date as the upcoming verified date",
  "Treat competitor-hosted current edition as open inventory",
]);

export const EVENT_HOSPITALITY_ELIGIBILITY_LAYERS = Object.freeze({
  verifiedEventFacts: [
    "Published conference date",
    "Confirmed destination",
    "Named organizer",
    "Official event website / programme",
    "Past venue history (as history, not future commitment)",
  ],
  hospitalityDemandInference: [
    "Visiting delegates may need accommodation",
    "Multi-day teams may require meetings",
    "Sponsors may need hotel rooms",
    "Participants may require F&B",
  ],
  unverifiedCommercialAssumptions: [
    "A room block remains available",
    "The organizer is considering other hotels",
    "A future event will return to this destination",
    "A particular room-night volume will materialize",
  ],
  actionableContactRule:
    "For commercially eligible actionable opportunities, require at least one verified named commercially relevant person, or an explicitly documented founder-approved segment-specific exception. Prefer two relevant contacts.",
});

export const EVENT_COMMERCIAL_TRACK = Object.freeze({
  TRACK_A_UPCOMING_VENUE: "TRACK_A_UPCOMING_VENUE",
  TRACK_B_RECURRING_COMPETITIVE_CAPTURE: "TRACK_B_RECURRING_COMPETITIVE_CAPTURE",
  TRACK_C_FUTURE_EDITION_VENUE_CONSTRAINT:
    "TRACK_C_FUTURE_EDITION_VENUE_CONSTRAINT",
  TRACK_D_HISTORICAL_RELATIONSHIP: "TRACK_D_HISTORICAL_RELATIONSHIP",
});

export const EVENT_DISCOVERY_DISPOSITION = Object.freeze({
  ACCEPT_FOR_DEEP_RESEARCH: "ACCEPT_FOR_DEEP_RESEARCH",
  TARGETED_VERIFICATION_REQUIRED: "TARGETED_VERIFICATION_REQUIRED",
  MONITOR_FUTURE_EDITION: "MONITOR_FUTURE_EDITION",
  REJECT: "REJECT",
  ROUTE_TO_SCALING: "ROUTE_TO_SCALING",
});

export const EVENT_DISCOVERY_GATES = Object.freeze({
  GATE1_VERIFIED_EVENT_OR_PROGRAMME: "GATE1_VERIFIED_EVENT_OR_PROGRAMME",
  GATE2_HOSPITALITY_DEMAND_MECHANISM: "GATE2_HOSPITALITY_DEMAND_MECHANISM",
  GATE3_HOTEL_SPECIFIC_RELEVANCE: "GATE3_HOTEL_SPECIFIC_RELEVANCE",
  GATE4_TIMING_AND_COMMERCIAL_ACCESSIBILITY:
    "GATE4_TIMING_AND_COMMERCIAL_ACCESSIBILITY",
  GATE5_EXISTING_OPPORTUNITY_IDENTITY: "GATE5_EXISTING_OPPORTUNITY_IDENTITY",
});

function text(v) {
  return String(v || "").trim();
}

function blobOf(input = {}) {
  return [
    input.eventIdentity,
    input.eventName,
    input.title,
    input.organizationName,
    input.companyName,
    input.summaryWhat,
    input.whatIsHappening,
    input.commercialDisposition,
    typeof input.commercialIntelligenceBrief?.whatIsHappening === "string"
      ? input.commercialIntelligenceBrief.whatIsHappening
      : "",
  ]
    .map(text)
    .filter(Boolean)
    .join("\n")
    .toLowerCase();
}

/**
 * Offline evaluate a candidate (fixtures / stored evidence) against Events gates.
 * Does not call Exa. Does not mutate inventory.
 */
export function evaluateEventDiscoveryCandidate(input = {}, opts = {}) {
  const blob = blobOf(input);
  const gates = {};
  const rationale = [];

  const hasEventIdentity = Boolean(
    text(input.eventIdentity || input.eventName || input.title) ||
      /\b(passport to paradise|gold trip|president'?s club|congress|conference|incentive|retreat|kickoff)\b/i.test(
        blob,
      ),
  );
  gates[EVENT_DISCOVERY_GATES.GATE1_VERIFIED_EVENT_OR_PROGRAMME] = hasEventIdentity
    ? "PASS"
    : "FAIL";

  const hospitality =
    /\b(incentive|resort|accommodation|room|meeting|delegate|attendee|overnight|catering|guestrooms?)\b/i.test(
      blob,
    ) || Boolean(input.evidenceOfHospitalityDemand);
  gates[EVENT_DISCOVERY_GATES.GATE2_HOSPITALITY_DEMAND_MECHANISM] = hospitality
    ? "PASS"
    : "FAIL";

  const marketHit =
    opts.hotelMarketRegex instanceof RegExp
      ? opts.hotelMarketRegex.test(blob)
      : /\b(canc[uú]n|riviera maya|mexico city|cdmx|chapultepec|destination)\b/i.test(
          blob,
        ) || Boolean(input.hotelMarketRelevance);
  gates[EVENT_DISCOVERY_GATES.GATE3_HOTEL_SPECIFIC_RELEVANCE] = marketHit
    ? "PASS"
    : "WEAK";

  const venueCommitted =
    /\b(nizuc|ava resort|majestic elegance|committed venue|host hotel|already booked)\b/i.test(
      blob,
    ) ||
    String(input.venueCommitmentStatus || "").includes("confirmed_competing");
  const historical =
    /\b(historical|took place|past edition|21st annual|previous)\b/i.test(blob) ||
    String(input.eventStatus || "") === "historical_completed";
  const upcoming =
    /\b(2026|2027|upcoming|announced|registration)\b/i.test(blob) && !historical;
  const scalingOnly =
    /\b(airport investment|operating hub|corporate expansion|new office)\b/i.test(
      blob,
    ) &&
    !/\b(incentive|congress|conference|retreat|passport|gold trip|president'?s club)\b/i.test(
      blob,
    );

  let track = EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP;
  let disposition = EVENT_DISCOVERY_DISPOSITION.TARGETED_VERIFICATION_REQUIRED;

  if (scalingOnly) {
    disposition = EVENT_DISCOVERY_DISPOSITION.ROUTE_TO_SCALING;
    track = EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP;
    rationale.push("Primary thesis is operating/corporate activity — route to Scaling.");
  } else if (upcoming && venueCommitted) {
    track = EVENT_COMMERCIAL_TRACK.TRACK_C_FUTURE_EDITION_VENUE_CONSTRAINT;
    disposition = EVENT_DISCOVERY_DISPOSITION.MONITOR_FUTURE_EDITION;
    rationale.push(
      "Future/current edition evidenced with competing venue — not open inventory.",
    );
  } else if (upcoming && !venueCommitted) {
    track = EVENT_COMMERCIAL_TRACK.TRACK_A_UPCOMING_VENUE;
    disposition = EVENT_DISCOVERY_DISPOSITION.ACCEPT_FOR_DEEP_RESEARCH;
    rationale.push("Upcoming programme with unverified/open venue posture.");
  } else if (
    historical &&
    /\b(annual|recurring|edition|21st|incentive)\b/i.test(blob)
  ) {
    track = EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE;
    disposition = EVENT_DISCOVERY_DISPOSITION.ACCEPT_FOR_DEEP_RESEARCH;
    rationale.push(
      "Historical recurring/incentive buying — competitive capture / relationship; not confirmed future booking.",
    );
  } else if (historical) {
    track = EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP;
    disposition = EVENT_DISCOVERY_DISPOSITION.MONITOR_FUTURE_EDITION;
    rationale.push("Historical event without strong recurrence evidence.");
  } else if (!hasEventIdentity || !hospitality) {
    disposition = EVENT_DISCOVERY_DISPOSITION.REJECT;
    track = EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP;
    rationale.push("Missing verified event identity or hospitality mechanism.");
  }

  gates[EVENT_DISCOVERY_GATES.GATE4_TIMING_AND_COMMERCIAL_ACCESSIBILITY] =
    disposition === EVENT_DISCOVERY_DISPOSITION.REJECT
      ? "FAIL"
      : venueCommitted
        ? "CONSTRAINED"
        : "PASS";

  const existingId = text(input.id || input.opportunityId || "");
  gates[EVENT_DISCOVERY_GATES.GATE5_EXISTING_OPPORTUNITY_IDENTITY] = existingId
    ? "KNOWN_ID"
    : "NET_NEW_CANDIDATE";

  const currentAccessibility = venueCommitted
    ? "none"
    : upcoming
      ? "medium"
      : historical
        ? "low"
        : "unknown";
  const futureCapture =
    track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE
      ? "medium"
      : track === EVENT_COMMERCIAL_TRACK.TRACK_C_FUTURE_EDITION_VENUE_CONSTRAINT
        ? "medium"
        : track === EVENT_COMMERCIAL_TRACK.TRACK_A_UPCOMING_VENUE
          ? "low"
          : "low";

  // False-future guard: never promote historical date as verifiedDates upcoming
  const verifiedDates =
    upcoming && text(input.verifiedDates || input.eventStartDate)
      ? text(input.verifiedDates || input.eventStartDate)
      : null;
  const historicalDates = historical
    ? [text(input.verifiedDates || input.eventStartDate || "historical")].filter(
        Boolean,
      )
    : [];

  return {
    version: EVENT_DISCOVERY_CONTRACT_V1,
    opportunityId: existingId || null,
    gates,
    commercialOpportunityTrack: track,
    discoveryDisposition: disposition,
    segmentRoutingRecommendation:
      disposition === EVENT_DISCOVERY_DISPOSITION.ROUTE_TO_SCALING
        ? RESEARCH_SEGMENT.SCALING_COMPANIES
        : RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    currentEditionAccessibility: currentAccessibility,
    futureCompetitiveCapturePotential: futureCapture,
    venueCommitted,
    verifiedDates,
    historicalDates,
    datePresentationRule:
      "If future date unknown, use Dates TBD — never show historical date as upcoming.",
    rationale,
    applyToLiveRecords: false,
  };
}

/**
 * Rank candidates by accessibility-aware score (offline).
 */
export function rankEventDiscoveryCandidates(candidates = []) {
  const scored = candidates.map((c, i) => {
    const ev = c._evaluation || evaluateEventDiscoveryCandidate(c);
    let score = 0;
    if (ev.discoveryDisposition === EVENT_DISCOVERY_DISPOSITION.ACCEPT_FOR_DEEP_RESEARCH)
      score += 40;
    if (ev.discoveryDisposition === EVENT_DISCOVERY_DISPOSITION.MONITOR_FUTURE_EDITION)
      score += 20;
    if (ev.discoveryDisposition === EVENT_DISCOVERY_DISPOSITION.ROUTE_TO_SCALING)
      score -= 100;
    if (ev.discoveryDisposition === EVENT_DISCOVERY_DISPOSITION.REJECT) score -= 50;
    if (ev.futureCompetitiveCapturePotential === "high") score += 15;
    if (ev.futureCompetitiveCapturePotential === "medium") score += 10;
    if (ev.currentEditionAccessibility === "high") score += 15;
    if (ev.currentEditionAccessibility === "medium") score += 10;
    if (ev.venueCommitted) score -= 5; // not discard — still valuable for capture
    if (ev.gates[EVENT_DISCOVERY_GATES.GATE2_HOSPITALITY_DEMAND_MECHANISM] === "PASS")
      score += 10;
    return { index: i, score, evaluation: ev, candidate: c };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored;
}

/** Build Agent discovery system prompt (compat wrapper). */
export function buildEventDiscoverySystemPrompt(hotelContext = {}) {
  const hotelName = String(
    hotelContext.displayName || hotelContext.hotelName || "the subject hotel",
  );
  const market = String(
    hotelContext.market ||
      hotelContext.primaryMarket ||
      hotelContext.city ||
      "the hotel's commercial market",
  );
  const product = String(
    hotelContext.productPositioning ||
      hotelContext.positioning ||
      "the hotel's documented product",
  );
  return [
    `You are researching Association / Conference / Events demand for ${hotelName} in ${market}.`,
    `Hotel product context: ${product}.`,
    "Identify organized gatherings, congresses, corporate meetings, incentives, trade shows, and recurring event series that create credible guestroom, meeting, or catering demand.",
    "Classify by demand mechanism (event-led), not by whether the organizer is a corporation.",
    "If the thesis is ongoing corporate account development without a specific event, do NOT force it into this segment — use ROUTE_TO_SCALING.",
    "Distinguish TRACK_A upcoming venue, TRACK_B recurring competitive capture, TRACK_C venue-constrained future edition, TRACK_D historical relationship.",
    "A past event evidences buying behavior only; it is not proof of an upcoming edition, open room block, or ability to displace the prior host.",
    "Do not invent attendance, room blocks, dates, contracting authority, or future recurrence.",
    "Prefer named organizers, PCOs, and event owners; keep venue contacts distinct from buyers.",
    "When a competing venue is committed, only assert overflow/alternative demand when evidence supports it.",
  ].join("\n");
}

export function buildEventDiscoveryOutputSchema() {
  return {
    version: EVENT_DISCOVERY_CONTRACT_V1,
    type: "object",
    required: ["candidates"],
    properties: {
      candidates: {
        type: "array",
        items: {
          type: "object",
          required: EVENT_DISCOVERY_REQUIRED_FIELDS.slice(0, 8),
          properties: Object.fromEntries(
            EVENT_DISCOVERY_REQUIRED_FIELDS.map((k) => [
              k,
              { type: ["string", "null"] },
            ]),
          ),
        },
      },
      prohibitionsHonored: { type: "boolean" },
      zeroInventionCertification: { type: "boolean" },
    },
  };
}

/** Map discovery evaluation → CIB / tile presentation hints (no invented copy). */
export function mapEventDiscoveryToCibHints(evaluation = {}, input = {}) {
  const track = evaluation.commercialOpportunityTrack;
  let activityLead = text(input.summaryWhat || input.whatIsHappening || "");
  if (!activityLead) {
    if (track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE) {
      activityLead =
        "Verified historical hospitality programme with possible future competitive-capture potential. Next venue/date not confirmed.";
    } else if (track === EVENT_COMMERCIAL_TRACK.TRACK_C_FUTURE_EDITION_VENUE_CONSTRAINT) {
      activityLead =
        "Event evidenced with a competing or committed venue. Investigate future editions rather than current displacement.";
    } else if (track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP) {
      activityLead =
        "Historical event verified. Additional editions have not been established as recurring.";
    }
  }
  return {
    tileActivitySummary: activityLead,
    datePill:
      evaluation.verifiedDates &&
      evaluation.discoveryDisposition !==
        EVENT_DISCOVERY_DISPOSITION.MONITOR_FUTURE_EDITION
        ? evaluation.verifiedDates
        : "Dates TBD",
    neverUseHistoricalAsUpcoming: true,
    accessibilityStatement:
      evaluation.venueCommitted
        ? "Current edition not treated as open inventory due to competing/committed venue."
        : evaluation.currentEditionAccessibility === "medium"
          ? "Current-edition sourcing may still be accessible — verify before pursuit."
          : "Prioritize relationship / future-edition monitoring over claiming open inventory.",
    segmentLabel: "Association / Conference / Events",
  };
}
