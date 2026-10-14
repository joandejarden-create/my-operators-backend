/**
 * GDI commercial-state model V1 (restored for historical regression diagnostic).
 * Separates packet completeness from commercial openness.
 * Not wired into live Ready/Watch. Simulation and tests only.
 * Phase-3: mechanism (HBC/Traveling Teams/mandatory stay-to-play) ≠ confirmed overflow availability.
 */

import { whoResearchAttempted } from "./opportunity-who-resolution-v1.js";
import { meetsReadyContactRequirement } from "./buyer-contact-path-taxonomy-v1.js";
import { meetsReadyAccountRequirement } from "./account-quality-taxonomy-v1.js";

export const COMMERCIAL_STATE = Object.freeze({
  OPEN_PURSUIT: "OPEN_PURSUIT",
  LIKELY_OPEN_PURSUIT: "LIKELY_OPEN_PURSUIT",
  NOT_YET_OPEN: "NOT_YET_OPEN",
  OVERFLOW_SECONDARY: "OVERFLOW_SECONDARY",
  EXISTING_PLACED: "EXISTING_PLACED",
  LOCKED_ELSEWHERE: "LOCKED_ELSEWHERE",
  CLOSED: "CLOSED",
  UNKNOWN: "UNKNOWN",
});

export const PACKET_STATUS = Object.freeze({
  INCOMPLETE: "INCOMPLETE",
  COMPLETE_PLAUSIBLE: "COMPLETE_PLAUSIBLE",
  COMPLETE_STRONG: "COMPLETE_STRONG",
});

export const PROPOSED_CUSTOMER_STATUS = Object.freeze({
  READY: "READY",
  WATCH: "WATCH",
  ACCOUNT_INTELLIGENCE: "ACCOUNT_INTELLIGENCE",
  DROPPED: "DROPPED",
});

export const DEMAND_MOTION = Object.freeze({
  GROUP_LODGING: "GROUP_LODGING",
  LOCAL_CATERING: "LOCAL_CATERING",
  ROOMS_AND_CATERING: "ROOMS_AND_CATERING",
});

const READY_COMMERCIAL = new Set([
  COMMERCIAL_STATE.OPEN_PURSUIT,
  COMMERCIAL_STATE.LIKELY_OPEN_PURSUIT,
  COMMERCIAL_STATE.OVERFLOW_SECONDARY,
]);

const SUMMARY_KEYS = [
  "summaryWhat",
  "summaryWhyHotel",
  "summaryWhyNow",
  "summaryWhyMatters",
  "whyNow",
  "cardWhyNowLine",
  "fitExplanation",
];

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function evidenceBlob(opp = {}) {
  return [
    opp.title,
    opp.opportunityName,
    opp.organizationName,
    opp.venueSourcingStatus,
    opp.venueStatus,
    opp.venueSourcingRationale,
    opp.roomDemandStatus,
    opp.roomDemandRationale,
    opp.opportunityType,
    opp.hotelOpportunityThesis,
    opp.hotelDemandThesis,
    opp.eventLocationSummary,
    opp.eventLocation,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.incrementalValueStatus,
  ]
    .map(text)
    .filter(Boolean)
    .join(" \n ");
}

function hasSource(opp = {}) {
  return Boolean(
    opp.officialSource ||
      opp.discoverySource ||
      (Array.isArray(opp.sources) && opp.sources.some((s) => s?.url || typeof s === "string"))
  );
}

function structurallyActive(opp = {}, opts = {}) {
  if (opp.isTestData === true) return false;
  if (text(opp.priority).toUpperCase() === "DISQUALIFIED") return false;
  if (!text(opp.title || opp.opportunityName) || !text(opp.organizationName || opp.company)) return false;
  if (opp.hotelFitScore == null && opp.hotelFit == null) return false;
  const start = text(opp.eventStartDate).slice(0, 10);
  const now = opts.nowDate || new Date().toISOString().slice(0, 10);
  if (start && start < now && /NO_FUTURE/.test(text(opp.futureCycleEvidenceState))) return false;
  return true;
}

function demandMotion(opp = {}) {
  const explicit = text(opp.demandMotion || opp.motionType).toUpperCase();
  if (explicit === DEMAND_MOTION.LOCAL_CATERING) return DEMAND_MOTION.LOCAL_CATERING;
  if (explicit === DEMAND_MOTION.ROOMS_AND_CATERING) return DEMAND_MOTION.ROOMS_AND_CATERING;
  if (opp.cateringOnly === true) return DEMAND_MOTION.LOCAL_CATERING;
  return DEMAND_MOTION.GROUP_LODGING;
}

function otherHotelSelected(blob) {
  if (
    /\b(hotel\/venue not named|hotel not named|not yet named|unannounced)\b/i.test(blob) &&
    !/\b(confirmed at|booked at|named as home base)\b/i.test(blob)
  ) {
    return false;
  }
  return (
    /\b(confirmed at|booked at|named as home base|headquarters hotel)\b/i.test(blob) ||
    (/\b(named|confirmed|booked)\b/i.test(blob) &&
      /\b(marriott|hilton|hyatt|westin|sheraton|omni|edition)\b/i.test(blob) &&
      !/\bnot yet\b/i.test(blob))
  );
}

function placedAtTarget(blob) {
  if (/\bwhether .{0,80} was included is unknown\b/i.test(blob)) return false;
  if (/\bofficial hotel list controlled by\b/i.test(blob)) return false;
  return (
    /\balready an official\b/i.test(blob) ||
    /\balready (?:listed|contracted|the host hotel|on the official (?:hotel )?list)\b/i.test(blob) ||
    /\bthis hotel is already\b/i.test(blob) ||
    (/\bofficial [^.]+ hotel\b/i.test(blob) && /\balready\b/i.test(blob))
  );
}

function hasIncrementalMotion(blob) {
  return /\b(new sponsor block|additional (?:room )?block|incremental (?:block|rooms)|separate overflow (?:block|packet))\b/i.test(
    blob
  );
}

function forthcoming(blob) {
  return /\b(forthcoming|to be shared closer|check back|closer to (?:the )?event|more information to come|hotel information to come|coming soon|destination TBD|TBA|not listed publicly|hotel package not|not yet published|room block info coming|hotel link releases)\b/i.test(
    blob
  );
}

/**
 * Explicit overflow / secondary lodging evidence.
 * Mechanism alone (HBC / Traveling Teams / mandatory stay-to-play) must NOT set ok=true.
 */
export function evaluateOverflowEvidence(opp = {}) {
  const blob = evidenceBlob(opp);
  const reasons = [];
  const evidenceQuotes = [];

  if (placedAtTarget(blob) && !hasIncrementalMotion(blob)) {
    return {
      ok: false,
      reasons: ["target_already_on_official_or_contracted_path"],
      evidenceQuotes,
      independentOfPrimaryPlacement: false,
      classificationOnly: false,
      claims: {
        housingMechanism: false,
        hotelEligible: "UNKNOWN",
        incrementalAvailability: "UNKNOWN",
        bookingWindowOpen: "UNKNOWN",
        pursuitPathUsable: "UNKNOWN",
      },
    };
  }

  const classificationOnly =
    text(opp.roomDemandStatus).toUpperCase() === "OVERFLOW_ONLY" ||
    (text(opp.roomDemandStatus).toUpperCase() === "VERIFIED_HOUSING_PROGRAM" &&
      /OVERFLOW/i.test(text(opp.opportunityType)));

  const availabilityPatterns = [
    { re: /\bofficial overflow (?:hotel )?list\b/i, label: "official_overflow_list", independent: true },
    { re: /\boverflow hotel list\b/i, label: "overflow_hotel_list", independent: true },
    { re: /\bsecondary hotel list\b/i, label: "secondary_hotel_list", independent: true },
    { re: /\bcontracted overflow\b/i, label: "contracted_overflow", independent: true },
    { re: /\bsecondary block\b/i, label: "secondary_block", independent: true },
    { re: /\bhousing bureau\b/i, label: "housing_bureau", independent: true },
    {
      re: /\b(?:vip|sponsor) block\b/i,
      label: "vip_or_sponsor_block",
      independent: true,
      require: /\b(?:official|published|contracted)\b/i,
    },
    {
      re: /\bsatellite (?:meeting|event)s?\b/i,
      label: "satellite_event",
      independent: true,
      require: /\b(?:official|scheduled|contracted)\b/i,
    },
    {
      re: /\b(?:crew|staff) lodging\b/i,
      label: "crew_or_staff_lodging",
      independent: true,
      require: /\b(?:contract|agreement|block)\b/i,
    },
    { re: /\bofficial overflow\b/i, label: "official_overflow_phrase", independent: false },
  ];

  const mechanismPatterns = [
    { re: /\bmandatory stay-to-play\b/i, label: "mandatory_stay_to_play" },
    { re: /\bHBC Event Services\b/i, label: "hbc_event_services" },
    { re: /\bTraveling Teams\b/i, label: "traveling_teams" },
  ];

  const availabilityHits = [];
  for (const pattern of availabilityPatterns) {
    if (!pattern.re.test(blob)) continue;
    if (pattern.require && !pattern.require.test(blob)) continue;
    const match = blob.match(pattern.re);
    availabilityHits.push(pattern);
    if (match) evidenceQuotes.push(match[0]);
  }

  const mechanismHits = [];
  for (const pattern of mechanismPatterns) {
    if (!pattern.re.test(blob)) continue;
    const match = blob.match(pattern.re);
    mechanismHits.push(pattern);
    if (match && !evidenceQuotes.includes(match[0])) evidenceQuotes.push(match[0]);
  }

  const eligibilityUnknown =
    /\bonly if\b/i.test(blob) ||
    /\bif .{0,40}(?:includes|lists|adds) (?:the )?property\b/i.test(blob) ||
    /\bcan (?:bid|get) onto\b/i.test(blob) ||
    /\beligib(?:le|ility) (?:is )?unknown\b/i.test(blob);
  const windowClosed =
    /\bbooking (?:window|deadline) (?:has )?(?:closed|passed)\b/i.test(blob) ||
    /\bhousing (?:is )?closed\b/i.test(blob);
  const windowOpen =
    /\bbook (?:now|today)\b/i.test(blob) ||
    /\bhousing (?:is )?open\b/i.test(blob) ||
    /\bbooking deadline\b/i.test(blob);

  const claims = {
    housingMechanism: mechanismHits.length > 0,
    hotelEligible: eligibilityUnknown
      ? "UNKNOWN"
      : mechanismHits.length || availabilityHits.length
        ? "SUPPORTED_INFERENCE"
        : "UNKNOWN",
    incrementalAvailability:
      availabilityHits.length || hasIncrementalMotion(blob) ? "EVIDENCED" : "UNKNOWN",
    bookingWindowOpen: windowClosed ? "NO" : windowOpen ? "YES" : "UNKNOWN",
    pursuitPathUsable: "UNKNOWN",
  };

  if (!availabilityHits.length && !mechanismHits.length) {
    if (classificationOnly) reasons.push("classification_fields_are_not_overflow_evidence");
    else reasons.push("no_explicit_overflow_or_secondary_housing_evidence");
    return {
      ok: false,
      reasons,
      evidenceQuotes,
      independentOfPrimaryPlacement: false,
      classificationOnly,
      claims,
    };
  }

  if (!availabilityHits.length && !hasIncrementalMotion(blob)) {
    reasons.push("housing_mechanism_present_availability_unknown");
    for (const hit of mechanismHits) reasons.push(hit.label);
    if (eligibilityUnknown) reasons.push("hotel_eligibility_unknown");
    return {
      ok: false,
      reasons,
      evidenceQuotes,
      independentOfPrimaryPlacement: false,
      classificationOnly,
      claims,
      mechanismOnly: true,
    };
  }

  if (/\bonly pursue if\b/i.test(blob) && !mechanismHits.some((h) => h.label === "mandatory_stay_to_play")) {
    return {
      ok: false,
      reasons: ["conditional_overflow_language_without_mandatory_mechanism"],
      evidenceQuotes,
      independentOfPrimaryPlacement: false,
      classificationOnly,
      claims,
    };
  }

  const onlyAppointedPhrase =
    availabilityHits.every((h) => h.label === "official_overflow_phrase") &&
    !hasIncrementalMotion(blob) &&
    /\b(official overflow block|already|this hotel is)\b/i.test(blob);
  if (onlyAppointedPhrase) {
    return {
      ok: false,
      reasons: ["official_overflow_appointment_is_not_new_available_demand"],
      evidenceQuotes,
      independentOfPrimaryPlacement: false,
      classificationOnly,
      claims,
    };
  }

  if (windowClosed) {
    reasons.push("overflow_booking_window_closed");
    return {
      ok: false,
      reasons,
      evidenceQuotes,
      independentOfPrimaryPlacement: false,
      classificationOnly,
      claims,
    };
  }

  const independent = availabilityHits.some((h) => h.independent) || hasIncrementalMotion(blob);
  reasons.push("explicit_overflow_or_secondary_housing_evidence");
  for (const hit of availabilityHits) reasons.push(hit.label);
  for (const hit of mechanismHits) reasons.push(hit.label);
  claims.incrementalAvailability = "EVIDENCED";
  return {
    ok: true,
    reasons,
    evidenceQuotes,
    independentOfPrimaryPlacement: independent,
    classificationOnly,
    claims,
  };
}

export function isValidOverflowSecondary(opp = {}) {
  return evaluateOverflowEvidence(opp).ok === true;
}

export function isExistingPlaced(opp = {}) {
  const blob = evidenceBlob(opp);
  if (!placedAtTarget(blob)) return false;
  if (hasIncrementalMotion(blob)) return false;
  return true;
}

export function classifyGdiCommercialState(opp = {}, opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const blob = evidenceBlob(opp);
  const venue = text(opp.venueSourcingStatus).toUpperCase();
  const type = text(opp.opportunityType).toUpperCase();
  const start = text(opp.eventStartDate).slice(0, 10);
  const reasons = [];

  if (demandMotion(opp) === DEMAND_MOTION.LOCAL_CATERING) {
    reasons.push("catering_only_not_a_lodging_state");
    return { state: COMMERCIAL_STATE.UNKNOWN, reasons, overflowEvidence: false, existingPlaced: false };
  }

  if (start && start < nowDate && !/FUTURE_CYCLE_EXPECTED|CURRENT_FUTURE_CYCLE_CONFIRMED/.test(text(opp.futureCycleEvidenceState))) {
    reasons.push("event_start_before_today");
    return { state: COMMERCIAL_STATE.CLOSED, reasons, overflowEvidence: false, existingPlaced: false };
  }
  if (start && start <= nowDate && /NO_FUTURE_CYCLE/.test(text(opp.futureCycleEvidenceState))) {
    reasons.push("current_cycle_already_started");
    return { state: COMMERCIAL_STATE.CLOSED, reasons, overflowEvidence: false, existingPlaced: false };
  }

  if (isExistingPlaced(opp)) {
    reasons.push("target_already_on_official_or_contracted_path");
    return { state: COMMERCIAL_STATE.EXISTING_PLACED, reasons, overflowEvidence: false, existingPlaced: true };
  }

  const overflowEval = evaluateOverflowEvidence(opp);
  const lockedSignal =
    otherHotelSelected(blob) ||
    venue === "FULLY_PLACED" ||
    venue === "PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE" ||
    /\bnot for 20\d{2}\b/i.test(blob);

  if (lockedSignal) {
    if (overflowEval.ok && overflowEval.independentOfPrimaryPlacement) {
      reasons.push("named_primary_elsewhere_with_independent_overflow_evidence");
      reasons.push(...overflowEval.reasons);
      return {
        state: COMMERCIAL_STATE.OVERFLOW_SECONDARY,
        reasons,
        overflowEvidence: true,
        existingPlaced: false,
        overflowEvidenceQuotes: overflowEval.evidenceQuotes,
        overflowClaims: overflowEval.claims,
      };
    }
    if (overflowEval.mechanismOnly) {
      reasons.push("housing_mechanism_present_availability_unknown");
      reasons.push(...overflowEval.reasons.filter((r) => r !== "housing_mechanism_present_availability_unknown"));
      return {
        state: COMMERCIAL_STATE.NOT_YET_OPEN,
        reasons,
        overflowEvidence: false,
        existingPlaced: false,
        overflowClaims: overflowEval.claims,
      };
    }
    reasons.push("another_hotel_or_closed_path_without_secondary_evidence");
    if (overflowEval.classificationOnly) reasons.push("classification_fields_are_not_overflow_evidence");
    return { state: COMMERCIAL_STATE.LOCKED_ELSEWHERE, reasons, overflowEvidence: false, existingPlaced: false };
  }

  if (overflowEval.ok) {
    reasons.push(...overflowEval.reasons);
    return {
      state: COMMERCIAL_STATE.OVERFLOW_SECONDARY,
      reasons,
      overflowEvidence: true,
      existingPlaced: false,
      overflowEvidenceQuotes: overflowEval.evidenceQuotes,
      overflowClaims: overflowEval.claims,
    };
  }

  if (overflowEval.mechanismOnly) {
    reasons.push(...overflowEval.reasons);
    return {
      state: COMMERCIAL_STATE.NOT_YET_OPEN,
      reasons,
      overflowEvidence: false,
      existingPlaced: false,
      overflowClaims: overflowEval.claims,
    };
  }

  if (overflowEval.classificationOnly) reasons.push("classification_fields_are_not_overflow_evidence");

  if (venue === "CURRENT_CYCLE_CLOSED") {
    reasons.push("venue_status_closed");
    return { state: COMMERCIAL_STATE.CLOSED, reasons, overflowEvidence: false, existingPlaced: false };
  }

  const primary =
    type === "PRIMARY_PURSUIT" ||
    type === "VENUE_PARTNERSHIP" ||
    /\bprimary hotel\/venue still appears unresolved\b/i.test(blob);

  if (venue === "RFP_ACTIVE_SOURCING" || venue === "OPEN_BID" || /\b(rfp active|soliciting hotels|accepting proposals)\b/i.test(blob)) {
    reasons.push("structured_open_sourcing");
    return { state: COMMERCIAL_STATE.OPEN_PURSUIT, reasons, overflowEvidence: false, existingPlaced: false };
  }

  if (
    venue === "LIKELY_OPEN" ||
    venue === "HOTEL_VENUE_TBD" && primary ||
    /\b(likely open|hotel\/venue not named|primary hotel\/venue still appears unresolved)\b/i.test(blob)
  ) {
    if (forthcoming(blob) || /FUTURE_CYCLE/.test(text(opp.futureCycleEvidenceState))) {
      reasons.push("future_movement_housing_not_open");
      return { state: COMMERCIAL_STATE.NOT_YET_OPEN, reasons, overflowEvidence: false, existingPlaced: false };
    }
    reasons.push("structured_likely_open");
    return { state: COMMERCIAL_STATE.LIKELY_OPEN_PURSUIT, reasons, overflowEvidence: false, existingPlaced: false };
  }

  if (
    forthcoming(blob) ||
    venue === "HOUSING_PENDING" ||
    /FUTURE_CYCLE_EXPECTED|FUTURE_CYCLE_UNCONFIRMED|CURRENT_FUTURE_CYCLE_CONFIRMED/.test(text(opp.futureCycleEvidenceState))
  ) {
    reasons.push(
      venue === "UNKNOWN" || !venue
        ? "venue_unknown_future_movement_not_ready"
        : "future_movement_housing_not_open"
    );
    return { state: COMMERCIAL_STATE.NOT_YET_OPEN, reasons, overflowEvidence: false, existingPlaced: false };
  }

  if (venue === "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE" && !overflowEval.ok) {
    reasons.push("insufficient_structured_commercial_evidence");
    return { state: COMMERCIAL_STATE.UNKNOWN, reasons, overflowEvidence: false, existingPlaced: false };
  }

  reasons.push("insufficient_structured_commercial_evidence");
  return { state: COMMERCIAL_STATE.UNKNOWN, reasons, overflowEvidence: false, existingPlaced: false };
}

export function classifyGdiPacketStatus(opp = {}) {
  const missing = [];
  if (!text(opp.title || opp.opportunityName)) missing.push("title");
  if (!text(opp.organizationName || opp.company)) missing.push("organization");
  if (!hasSource(opp)) missing.push("source");
  if (!whoResearchAttempted(opp)) missing.push("whoResearch");
  if (opp.hotelFitScore == null && opp.hotelFit == null) missing.push("hotelFit");
  const contact = meetsReadyContactRequirement(opp);
  const account = meetsReadyAccountRequirement(opp);
  if (!contact.ok) missing.push("buyerPath");
  if (!account.ok) missing.push("namedEntity");
  const lodgingKnown = /VERIFIED|STRONG|OVERFLOW_ONLY|ESTIMATED/.test(text(opp.roomDemandStatus).toUpperCase());
  const uniqueMissing = [...new Set(missing)];
  if (uniqueMissing.length) return { status: PACKET_STATUS.INCOMPLETE, missing: uniqueMissing };
  if (
    lodgingKnown &&
    (contact.class === "NAMED_BUYER_PERSON" ||
      contact.class === "RELEVANT_FUNCTION_CONTACT" ||
      contact.class === "NAMED_BUYER_ROLE_PATH")
  ) {
    return { status: PACKET_STATUS.COMPLETE_STRONG, missing: [] };
  }
  return { status: PACKET_STATUS.COMPLETE_PLAUSIBLE, missing: [] };
}

function credibleUnknownPath(opp, packet) {
  if (packet.status === PACKET_STATUS.INCOMPLETE) return false;
  const fut = text(opp.futureCycleEvidenceState);
  const fit = Number(opp.hotelFitScore ?? opp.hotelFit);
  return (
    /CURRENT_FUTURE_CYCLE_CONFIRMED|FUTURE_CYCLE_EXPECTED|RECURRING_CYCLE_STRONG|FUTURE_CYCLE_UNCONFIRMED/.test(fut) &&
    Number.isFinite(fit) &&
    fit >= 60 &&
    hasSource(opp)
  );
}

export function proposeGdiCustomerStatusV2(opp = {}, opts = {}) {
  const motion = demandMotion(opp);
  const commercial = classifyGdiCommercialState(opp, opts);
  const packet = classifyGdiPacketStatus(opp);
  const surfaceOk = structurallyActive(opp, opts);
  const base = {
    commercialState: commercial.state,
    packetStatus: packet.status,
    overflowEvidence: commercial.overflowEvidence,
    existingPlaced: commercial.existingPlaced,
    openPursuit:
      commercial.state === COMMERCIAL_STATE.OPEN_PURSUIT ||
      commercial.state === COMMERCIAL_STATE.LIKELY_OPEN_PURSUIT,
    reasons: [...commercial.reasons],
    missing: packet.missing,
    demandMotion: motion,
  };

  if (motion === DEMAND_MOTION.LOCAL_CATERING) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.DROPPED, reasons: [...base.reasons, "catering_separated_from_lodging_ready"] };
  }
  if (!surfaceOk) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.DROPPED, reasons: [...base.reasons, "surface_ineligible"] };
  }
  if (commercial.state === COMMERCIAL_STATE.EXISTING_PLACED) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.ACCOUNT_INTELLIGENCE };
  }
  if (commercial.state === COMMERCIAL_STATE.LOCKED_ELSEWHERE || commercial.state === COMMERCIAL_STATE.CLOSED) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.DROPPED };
  }
  if (READY_COMMERCIAL.has(commercial.state) && packet.status !== PACKET_STATUS.INCOMPLETE) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.READY };
  }
  if (commercial.state === COMMERCIAL_STATE.NOT_YET_OPEN && packet.status !== PACKET_STATUS.INCOMPLETE) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.WATCH };
  }
  if (commercial.state === COMMERCIAL_STATE.UNKNOWN && credibleUnknownPath(opp, packet)) {
    return { ...base, status: PROPOSED_CUSTOMER_STATUS.WATCH, reasons: [...base.reasons, "credible_unresolved_path"] };
  }
  return { ...base, status: PROPOSED_CUSTOMER_STATUS.DROPPED };
}

export function summaryFieldsIgnoredByV2() {
  return [...SUMMARY_KEYS];
}
