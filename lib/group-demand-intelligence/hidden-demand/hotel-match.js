/**
 * Independent hotel fit for a shared hidden-demand entity.
 * Uses hotel config capability — no Hilton/Renaissance hardcodes in logic.
 */

import {
  HIDDEN_MOTION,
  LODGING_SIGNAL_STRENGTH,
  DEMAND_FAMILY,
} from "./constants.js";
import { computeHotelMatchId, computeHotelOpportunityId } from "./identity.js";

function roomsOf(cfg) {
  return Number(cfg?.capabilityProfile?.totalGuestrooms) || 0;
}
function meetingOf(cfg) {
  return Number(cfg?.capabilityProfile?.totalMeetingSpaceSqFt) || 0;
}

export function inferHiddenMotion(family, lodgingStrength, hotelConfig) {
  const limitedMeetings = meetingOf(hotelConfig) > 0 && meetingOf(hotelConfig) < 2500;
  switch (family) {
    case DEMAND_FAMILY.EXHIBITOR_VENDOR:
      return limitedMeetings ? HIDDEN_MOTION.EXHIBITOR_BLOCK : HIDDEN_MOTION.VENDOR_BLOCK;
    case DEMAND_FAMILY.PRODUCTION_CREW:
      return HIDDEN_MOTION.CREW_LODGING;
    case DEMAND_FAMILY.CORPORATE_PROJECT:
      return HIDDEN_MOTION.CORPORATE_PROJECT_BLOCK;
    case DEMAND_FAMILY.TRAINING:
      return HIDDEN_MOTION.TRAINING_COHORT;
    case DEMAND_FAMILY.TOUR_SERIES:
      return HIDDEN_MOTION.TOUR_SERIES;
    case DEMAND_FAMILY.DELEGATION:
      return HIDDEN_MOTION.DELEGATION_BLOCK;
    case DEMAND_FAMILY.EDUCATION:
      return HIDDEN_MOTION.EDUCATIONAL_GROUP;
    case DEMAND_FAMILY.SPORTS_ADJACENT:
      return HIDDEN_MOTION.SPORTS_SUPPORT_BLOCK;
    case DEMAND_FAMILY.AGENCY:
      return HIDDEN_MOTION.AGENCY_PROJECT_BLOCK;
    case DEMAND_FAMILY.FASHION:
      return HIDDEN_MOTION.FASHION_PROJECT_BLOCK;
    case DEMAND_FAMILY.MEDICAL_PHARMA:
      return HIDDEN_MOTION.PHARMA_PROJECT_BLOCK;
    case DEMAND_FAMILY.SOCIAL:
      return HIDDEN_MOTION.SOCIAL_ROOM_BLOCK;
    case DEMAND_FAMILY.ASSOCIATION_SUBGROUP:
      return limitedMeetings ? HIDDEN_MOTION.ASSOCIATION_SUBGROUP : HIDDEN_MOTION.LEADERSHIP_MEETING;
    default:
      return lodgingStrength === LODGING_SIGNAL_STRENGTH.STRONG
        ? HIDDEN_MOTION.ROOM_BLOCK
        : HIDDEN_MOTION.OVERFLOW;
  }
}

/**
 * Score hotel fit 0–100 with rationale — lodging-primary hotels prefer lodging motions.
 */
export function evaluateHotelHiddenDemandFit(hiddenDemand = {}, hotelConfig = null) {
  const rooms = roomsOf(hotelConfig);
  const meeting = meetingOf(hotelConfig);
  const limitedMeetings = meeting > 0 && meeting < 2500;
  const family = hiddenDemand.family;
  const lodging = hiddenDemand.lodgingSignalStrength || LODGING_SIGNAL_STRENGTH.UNKNOWN;
  const motion = inferHiddenMotion(family, lodging, hotelConfig);

  let score = 45;
  const rationale = [];

  if (rooms >= 250) {
    score += 12;
    rationale.push(`${rooms} guestrooms support group lodging`);
  } else if (rooms >= 150) {
    score += 6;
    rationale.push(`${rooms} guestrooms mid-size lodging capacity`);
  } else {
    score -= 8;
    rationale.push("limited guestroom inventory");
  }

  const lodgingPrimaryMotions = new Set([
    HIDDEN_MOTION.EXHIBITOR_BLOCK,
    HIDDEN_MOTION.CREW_LODGING,
    HIDDEN_MOTION.TOUR_SERIES,
    HIDDEN_MOTION.DELEGATION_BLOCK,
    HIDDEN_MOTION.SPORTS_SUPPORT_BLOCK,
    HIDDEN_MOTION.OVERFLOW,
    HIDDEN_MOTION.ROOM_BLOCK,
    HIDDEN_MOTION.SOCIAL_ROOM_BLOCK,
    HIDDEN_MOTION.CORPORATE_PROJECT_BLOCK,
    HIDDEN_MOTION.TRAINING_COHORT,
  ]);

  if (limitedMeetings && lodgingPrimaryMotions.has(motion)) {
    score += 14;
    rationale.push("lodging-primary motion fits limited formal meeting inventory");
  } else if (!limitedMeetings && /MEETING|LEADERSHIP|BOARD|ADVISORY/i.test(motion)) {
    score += 10;
    rationale.push("meeting inventory supports in-house subgroup");
  } else if (limitedMeetings && /LEADERSHIP_MEETING|BOARD_RETREAT/i.test(motion)) {
    score -= 12;
    rationale.push("formal meeting product thin for in-house meeting motion");
  }

  if (lodging === LODGING_SIGNAL_STRENGTH.STRONG) score += 15;
  else if (lodging === LODGING_SIGNAL_STRENGTH.MEDIUM) score += 8;
  else if (lodging === LODGING_SIGNAL_STRENGTH.WEAK) score += 2;
  else score -= 5;

  // Territory: Midtown keywords already in market discovery — boost if destination NYC/Midtown
  const dest = `${hiddenDemand.destination || ""} ${hiddenDemand.title || ""}`.toLowerCase();
  if (/times square|midtown|manhattan|new york|nyc|javits|broadway/.test(dest)) {
    score += 10;
    rationale.push("destination in hotel commercial territory");
  } else if (/brooklyn|queens|jersey|westchester/.test(dest)) {
    score -= 15;
    rationale.push("destination stretch vs Midtown lodging core");
  }

  score = Math.max(0, Math.min(100, Math.round(score)));

  let decision = "WATCH";
  if (score < 40) decision = "INSUFFICIENT";
  else if (
    score >= 70 &&
    (lodging === LODGING_SIGNAL_STRENGTH.STRONG || lodging === LODGING_SIGNAL_STRENGTH.MEDIUM)
  ) {
    decision = "MATCH_STRONG";
  } else if (score >= 55) decision = "MATCH";

  return {
    hotelId: hotelConfig?.hotelId,
    hotelName: hotelConfig?.displayName,
    hiddenDemandId: hiddenDemand.hiddenDemandId,
    hotelMatchId: computeHotelMatchId({
      hotelId: hotelConfig?.hotelId,
      hiddenDemandId: hiddenDemand.hiddenDemandId,
    }),
    hotelOpportunityId: computeHotelOpportunityId({
      hotelId: hotelConfig?.hotelId,
      hiddenDemandId: hiddenDemand.hiddenDemandId,
    }),
    fitScore: score,
    decision,
    commercialMotion: motion,
    rationale,
  };
}

export function buildHotelSpecificThesis(hiddenDemand, fit, hotelConfig) {
  const name = hotelConfig?.displayName || "This hotel";
  const rooms = roomsOf(hotelConfig);
  const motion = String(fit.commercialMotion || "").replace(/_/g, " ").toLowerCase();
  return `${name} (${rooms} rooms) is a credible ${motion} option for ${hiddenDemand.organizationName || hiddenDemand.title} given Midtown / Times Square lodging access${fit.rationale?.[0] ? ` — ${fit.rationale[0]}` : ""}.`;
}

export function buildHotelWhyNow(hiddenDemand, fit) {
  if (hiddenDemand.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG) {
    return "Lodging / travel signal is publicly evidenced — confirm Midtown partner acceptance now.";
  }
  if (hiddenDemand.timingKey || hiddenDemand.eventStartDate) {
    return `Monitor — future timing ${hiddenDemand.eventStartDate || hiddenDemand.timingKey}; re-research when housing or team travel details publish.`;
  }
  return "Monitor — hidden demand entity identified; lodging channel not yet public.";
}

export function buildHotelRecommendedAction(hiddenDemand, fit, contact) {
  const person = contact?.name;
  const role = contact?.role || "project / travel / events contact";
  const org = hiddenDemand.organizationName || hiddenDemand.title;
  if (person) {
    return `Contact ${person}, ${role} at ${org}, to confirm Midtown lodging needs for this ${String(fit.commercialMotion || "group").replace(/_/g, " ").toLowerCase()}.`;
  }
  return `Identify the ${org} travel / events / project owner and ask whether Midtown room-block partners are being considered.`;
}
