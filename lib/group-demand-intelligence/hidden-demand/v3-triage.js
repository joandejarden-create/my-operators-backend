/**
 * Deterministic commercial plausibility triage before deep research.
 */

import {
  RESEARCH_TRIAGE,
  COMPANY_LOCALITY,
} from "./v3-states.js";
import { cleanEntityDisplayName, isV3QueueNoise } from "./v3-entity-clean.js";
import { classifyOriginTravel, travelIncreasesLodging } from "./origin-travel.js";
import { classifyParticipationDepth } from "./participation-depth.js";
import { TRAVEL_CLASS } from "./v3-constants.js";
import { classifyEventGeography } from "./v3-event-geography.js";

export function resolveCompanyLocality(travelClass) {
  if (travelClass === TRAVEL_CLASS.LOCAL) return COMPANY_LOCALITY.NYC_LOCAL;
  if (travelClass === TRAVEL_CLASS.DRIVE_MARKET) return COMPANY_LOCALITY.NY_METRO;
  if (travelClass === TRAVEL_CLASS.INTERNATIONAL) return COMPANY_LOCALITY.INTERNATIONAL;
  if (
    travelClass === TRAVEL_CLASS.SHORT_HAUL_AIR ||
    travelClass === TRAVEL_CLASS.LONG_HAUL_AIR
  ) {
    return COMPANY_LOCALITY.DOMESTIC_OUT_OF_MARKET;
  }
  return COMPANY_LOCALITY.UNKNOWN;
}

/**
 * @returns {{ triage, score, reasons, origin, participation, locality }}
 */
export function triageExhibitorResearchValue(entity = {}, deepenText = "") {
  if (isV3QueueNoise(entity)) {
    return {
      triage: RESEARCH_TRIAGE.STOP,
      score: 0,
      reasons: ["queue_noise"],
      origin: null,
      participation: null,
      locality: COMPANY_LOCALITY.UNKNOWN,
    };
  }

  const name = cleanEntityDisplayName(entity.entityName);
  const origin = classifyOriginTravel(entity, deepenText);
  const participation = classifyParticipationDepth(entity, deepenText);
  const locality = resolveCompanyLocality(origin.travelClass);
  const geo = classifyEventGeography(entity);

  let score = 10;
  const reasons = [];

  if (geo.geography === "NON_NYC") {
    score -= 40;
    reasons.push(geo.reason || "non_nyc_event");
  } else if (geo.geography === "NYC_MIDTOWN") {
    score += 20;
    reasons.push("nyc_midtown_event");
  }

  if (entity.sourceType === "EXHIBITOR_DIRECTORY") {
    score += 15;
    reasons.push("exhibitor_directory");
  } else if (entity.sourceType === "PROGRAM_PDF") {
    score -= 10;
    reasons.push("program_pdf_low_prior");
  }

  if (travelIncreasesLodging(origin.travelClass)) {
    score += 25;
    reasons.push(`origin_${origin.travelClass}`);
  } else if (origin.travelClass === TRAVEL_CLASS.LOCAL) {
    score -= 20;
    reasons.push("nyc_local");
  } else if (origin.travelClass === TRAVEL_CLASS.DRIVE_MARKET) {
    score -= 5;
    reasons.push("drive_market");
  }

  if (participation.participationDepth === "ANCHOR") {
    score += 30;
    reasons.push("participation_anchor");
  } else if (participation.participationDepth === "HEAVY") {
    score += 20;
    reasons.push("participation_heavy");
  } else if (participation.participationDepth === "STANDARD") {
    score += 10;
    reasons.push("participation_standard");
  } else {
    score += 2;
    reasons.push("participation_light");
  }

  if (participation.signals?.boothNumber) {
    score += 8;
    reasons.push("booth_number");
  }
  if (participation.signals?.hostedEvent) {
    score += 10;
    reasons.push("hosted_event");
  }
  if (participation.signals?.speaking) {
    score += 8;
    reasons.push("speaking");
  }

  if (
    entity.lodgingSignalStrength === "STRONG" ||
    entity.lodgingSignalStrength === "MEDIUM"
  ) {
    score += 15;
    reasons.push("prior_lodging_signal");
  }

  // Single-person consultancy / thin name
  if (/^(consulting|consultant|advisory)\b/i.test(name) || name.split(/\s+/).length <= 1) {
    score -= 15;
    reasons.push("thin_or_consultancy");
  }

  if (!entity.futureTiming && !entity.year) {
    score -= 20;
    reasons.push("no_future_timing");
  }

  let triage = RESEARCH_TRIAGE.LOW_RESEARCH_VALUE;
  if (score >= 55) triage = RESEARCH_TRIAGE.HIGH_RESEARCH_VALUE;
  else if (score >= 35) triage = RESEARCH_TRIAGE.MEDIUM_RESEARCH_VALUE;
  else if (score < 15) triage = RESEARCH_TRIAGE.STOP;

  return {
    triage,
    score,
    reasons,
    origin,
    participation,
    locality,
    eventGeography: geo,
  };
}
