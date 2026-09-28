/**
 * Lodging proof ladder + travel likelihood for V3.
 * Does not invent room counts or arrival/departure.
 */

import {
  LODGING_PROOF,
  TRAVEL_LIKELIHOOD,
  TEAM_SIZE_BAND,
  HOTEL_OPPORTUNITY_LODGING,
  CANDIDATE_STATE,
} from "./v3-states.js";
import { TRAVEL_CLASS } from "./v3-constants.js";
import { travelIncreasesLodging } from "./origin-travel.js";

/**
 * Classify travel likelihood from team + origin evidence.
 */
export function classifyTravelLikelihood({
  travelClass = TRAVEL_CLASS.UNKNOWN,
  teamSupported = false,
  namedPeople = 0,
  explicitTravel = false,
  accommodationsMention = false,
  deepenText = "",
} = {}) {
  const blob = String(deepenText || "");
  if (
    explicitTravel ||
    accommodationsMention ||
    /delegation|team\s+attend|hotel\s+block|accommodation|travel\s+arrang/i.test(blob)
  ) {
    return TRAVEL_LIKELIHOOD.STRONG;
  }
  if (
    teamSupported &&
    travelIncreasesLodging(travelClass) &&
    (namedPeople >= 2 || /booth\s*staff|sales\s*team|marketing\s*team/i.test(blob))
  ) {
    return TRAVEL_LIKELIHOOD.MEDIUM;
  }
  if (travelIncreasesLodging(travelClass) || teamSupported) {
    return TRAVEL_LIKELIHOOD.WEAK;
  }
  return TRAVEL_LIKELIHOOD.UNKNOWN;
}

/**
 * Infer team size band — never invents numeric rooms.
 */
export function classifyTeamSizeBand({
  namedPeople = 0,
  speakerCount = 0,
  boothSize = null,
  deepenText = "",
} = {}) {
  const blob = String(deepenText || "");
  if (/large\s+team|20\+|dozens|crew\s+of/i.test(blob) || (namedPeople || 0) >= 8) {
    return TEAM_SIZE_BAND.LARGE_TEAM;
  }
  if (
    /team\s+of\s+[4-9]|several\s+staff|booth\s*staff/i.test(blob) ||
    (namedPeople || 0) >= 3 ||
    (speakerCount || 0) >= 2 ||
    (boothSize || 0) >= 200
  ) {
    return TEAM_SIZE_BAND.MEDIUM_TEAM;
  }
  if ((namedPeople || 0) >= 1 || (speakerCount || 0) >= 1 || /booth/i.test(blob)) {
    return TEAM_SIZE_BAND.SMALL_TEAM;
  }
  if (/solo|single\s+attendee|one\s+person/i.test(blob)) return TEAM_SIZE_BAND.SOLO_OR_TINY;
  return TEAM_SIZE_BAND.UNKNOWN;
}

/**
 * Lodging CONFIRMED requires explicit housing language — not generic "hotel" in nav.
 * Team-supported alone must not invent CONFIRMED.
 */
export function classifyLodgingProof({
  deepenText = "",
  housingSignals = null,
  travelLikelihood = TRAVEL_LIKELIHOOD.UNKNOWN,
  travelClass = TRAVEL_CLASS.UNKNOWN,
  teamSupported = false,
  namedPeople = 0,
  multiDay = false,
  setupBreakdown = false,
} = {}) {
  const blob = String(deepenText || "");
  const level1 =
    housingSignals?.roomBlockMentioned ||
    housingSignals?.housingPageFound ||
    /hotel\s*block|room\s*block|book\s+your\s+hotel|staff\s+hotel|crew\s+lodging|group\s+travel\s+instruction|accommodation\s+page|housing\s+registration|official\s+housing/i.test(
      blob
    );
  // Guard: single "hotel" mention in unrelated page chrome is not CONFIRMED
  const level1Safe =
    level1 &&
    (housingSignals?.roomBlockMentioned ||
      housingSignals?.housingPageFound ||
      /hotel\s*block|room\s*block|official\s+housing|housing\s+registration|book\s+your\s+hotel/i.test(
        blob
      ));
  if (level1Safe) {
    return {
      lodgingProof: LODGING_PROOF.CONFIRMED,
      lodgingLevel: 1,
      lodgingSignals: { explicit: true, housingSignals },
    };
  }

  const level2 =
    (multiDay && teamSupported && travelIncreasesLodging(travelClass)) ||
    (setupBreakdown && teamSupported) ||
    (namedPeople >= 3 && travelIncreasesLodging(travelClass)) ||
    /international\s+delegation|company[- ]organized\s+travel|tour\s+itinerary|setup.+breakdown|multi[- ]day\s+team/i.test(
      blob
    );
  if (level2 || travelLikelihood === TRAVEL_LIKELIHOOD.STRONG) {
    return {
      lodgingProof: LODGING_PROOF.STRONG_INFERENCE,
      lodgingLevel: 2,
      lodgingSignals: { strongIndirect: true },
    };
  }

  const level3 =
    travelLikelihood === TRAVEL_LIKELIHOOD.MEDIUM ||
    (travelIncreasesLodging(travelClass) && (teamSupported || multiDay || namedPeople >= 2));
  if (level3) {
    return {
      lodgingProof: LODGING_PROOF.PLAUSIBLE,
      lodgingLevel: 3,
      lodgingSignals: { mediumIndirect: true },
    };
  }

  if (teamSupported || /exhibitor/i.test(blob)) {
    return {
      lodgingProof: LODGING_PROOF.WEAK,
      lodgingLevel: 4,
      lodgingSignals: { exhibitorOnly: true },
    };
  }

  return {
    lodgingProof: LODGING_PROOF.UNKNOWN,
    lodgingLevel: 5,
    lodgingSignals: {},
  };
}

/**
 * Map lodging proof → candidate pipeline state (before hotel match).
 */
export function candidateStateFromEvidence({
  teamSupported = false,
  lodgingProof = LODGING_PROOF.UNKNOWN,
  addressability = null,
  hotelMatched = false,
} = {}) {
  if (!teamSupported && lodgingProof === LODGING_PROOF.UNKNOWN) {
    return CANDIDATE_STATE.MARKET_ENTITY;
  }
  if (!teamSupported) {
    return CANDIDATE_STATE.MARKET_ENTITY;
  }
  if (
    lodgingProof === LODGING_PROOF.WEAK ||
    lodgingProof === LODGING_PROOF.UNKNOWN
  ) {
    return CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE;
  }
  if (lodgingProof === LODGING_PROOF.PLAUSIBLE) {
    return CANDIDATE_STATE.LODGING_PLAUSIBLE;
  }
  if (HOTEL_OPPORTUNITY_LODGING.includes(lodgingProof)) {
    if (
      hotelMatched &&
      addressability &&
      addressability !== "NO_USABLE_PATH"
    ) {
      return CANDIDATE_STATE.ACTIONABLE_NOW;
    }
    if (hotelMatched) return CANDIDATE_STATE.HOTEL_OPPORTUNITY;
    return CANDIDATE_STATE.HOTEL_MATCH_CANDIDATE;
  }
  return CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE;
}

export function mayBecomeHotelOpportunity(lodgingProof) {
  return HOTEL_OPPORTUNITY_LODGING.includes(lodgingProof);
}

export function mayCustomerWatch(lodgingProof) {
  return (
    lodgingProof === LODGING_PROOF.PLAUSIBLE ||
    HOTEL_OPPORTUNITY_LODGING.includes(lodgingProof)
  );
}
