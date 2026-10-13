/**
 * Missing-pillar audit for finalization candidates.
 */

import { PILLAR_STRENGTH } from "./constants.js";
import { PACKET_PILLAR_ID } from "../international-discovery-v2/constants.js";

function strengthFromFlags({ proven, strong, plausible, contradicted, missing }) {
  if (contradicted) return PILLAR_STRENGTH.CONTRADICTED;
  if (proven) return PILLAR_STRENGTH.PROVEN;
  if (strong) return PILLAR_STRENGTH.STRONG;
  if (plausible) return PILLAR_STRENGTH.PLAUSIBLE;
  if (missing) return PILLAR_STRENGTH.MISSING;
  return PILLAR_STRENGTH.MISSING;
}

function isOrganizerShell(name = "") {
  return /organiz|secretar|comit[eé]|association|asociaci[oó]n|landeshauptstadt|vida sana|latinpress|pco|dmc/i.test(
    String(name || "")
  );
}

/** Cohort / role labels are not named traveling accounts */
function isCohortLabel(name = "") {
  return /\b(attendees?|delegates?|exhibitors?|speakers?|participants?|visitors?)\b/i.test(String(name || ""));
}

/**
 * Audit six commercial pillars. Does not invent facts.
 */
export function auditMissingPillars(candidate = {}) {
  const org = String(candidate.organizationName || candidate.accountName || "").trim();
  const cohortOnly = Boolean(org) && isCohortLabel(org);
  const namedEntity = strengthFromFlags({
    proven:
      Boolean(org) &&
      !isOrganizerShell(org) &&
      !cohortOnly &&
      candidate.travelingEntityProven === true,
    strong: Boolean(org) && !isOrganizerShell(org) && !cohortOnly && candidate.travelingEntityProven !== false,
    plausible: Boolean(org) || cohortOnly,
    missing: !org,
  });

  const groupMotion = strengthFromFlags({
    proven: Boolean(candidate.groupMotionProven),
    strong: Boolean(candidate.participationRole || candidate.demandType || candidate.travelMotion),
    plausible: Boolean(candidate.campaignName || candidate.eventName),
    missing: !(candidate.participationRole || candidate.demandType || candidate.campaignName),
  });

  const controllerAuth = String(candidate.lodgingAuthority || candidate.controllerAuthority || "").toUpperCase();
  const buyerController = strengthFromFlags({
    proven:
      controllerAuth === "CONFIRMED_LODGING_CONTROLLER" &&
      Boolean(candidate.publicContactPath || candidate.contactPath),
    strong:
      ["CONFIRMED_LODGING_CONTROLLER", "STRONG_SELECTION_INFLUENCE"].includes(controllerAuth) ||
      Boolean(candidate.buyerName && candidate.publicContactPath),
    plausible: Boolean(candidate.demandControllerId || candidate.controllerName || candidate.buyerEntity),
    missing: !(candidate.demandControllerId || candidate.controllerName || candidate.buyerEntity),
  });

  const futureDecision = strengthFromFlags({
    proven: Boolean(candidate.futureDecisionProven || candidate.selectionOpenDate || candidate.hotelListPublishDate),
    strong: Boolean(candidate.futureDecisionPoint || candidate.decisionWindow || candidate.eventStartDate),
    plausible: Boolean(candidate.validFutureWatch || candidate.timingClass === "FUTURE"),
    missing: !(
      candidate.futureDecisionPoint ||
      candidate.decisionWindow ||
      candidate.eventStartDate ||
      candidate.validFutureWatch
    ),
  });

  const lodgingClass = String(candidate.lodgingEvidenceClass || candidate.lodgingEvidence || "").toUpperCase();
  const lodgingEvidence = strengthFromFlags({
    proven: lodgingClass === "DIRECT_LODGING_EVIDENCE",
    strong: lodgingClass === "STRONG_HOTEL_MOTION",
    plausible: lodgingClass === "PLAUSIBLE_HOTEL_MOTION",
    contradicted: lodgingClass === "CLOSED" || candidate.selectionStatus === "CLOSED",
    missing: !lodgingClass || lodgingClass === "NONE" || lodgingClass === "UNCONFIRMED",
  });

  const fit = String(candidate.targetHotelFit || candidate.hotelFit || candidate.fitClass || "").toUpperCase();
  const hotelFit = strengthFromFlags({
    proven: fit === "STRONG_FIT",
    strong: fit === "STRONG_FIT",
    plausible: fit === "PLAUSIBLE_FIT",
    contradicted: fit === "NO_FIT" || fit === "WRONG_DESTINATION",
    missing: !fit || fit === "UNKNOWN" || fit === "WEAK_FIT",
  });
  // WEAK_FIT / UNKNOWN block Ready; PLAUSIBLE_FIT stays PLAUSIBLE (not STRONG)
  const hotelFitFinal = hotelFit;

  const pillars = {
    [PACKET_PILLAR_ID.NAMED_ENTITY]: namedEntity,
    [PACKET_PILLAR_ID.GROUP_MOTION]: groupMotion,
    [PACKET_PILLAR_ID.BUYER_OR_CONTROLLER]: buyerController,
    [PACKET_PILLAR_ID.FUTURE_DECISION]: futureDecision,
    [PACKET_PILLAR_ID.LODGING_EVIDENCE]: lodgingEvidence,
    [PACKET_PILLAR_ID.HOTEL_FIT]: hotelFitFinal,
  };

  const priority = [
    PACKET_PILLAR_ID.HOTEL_FIT,
    PACKET_PILLAR_ID.LODGING_EVIDENCE,
    PACKET_PILLAR_ID.NAMED_ENTITY,
    PACKET_PILLAR_ID.BUYER_OR_CONTROLLER,
    PACKET_PILLAR_ID.GROUP_MOTION,
    PACKET_PILLAR_ID.FUTURE_DECISION,
  ];
  const missingOrWeak = priority.filter((p) =>
    [PILLAR_STRENGTH.MISSING, PILLAR_STRENGTH.CONTRADICTED].includes(pillars[p])
  );
  // Soft blockers: PLAUSIBLE pillars that still prevent Ready / COMPLETE_STRONG
  const softWeak = priority.filter((p) => pillars[p] === PILLAR_STRENGTH.PLAUSIBLE);
  const primary = missingOrWeak[0] || softWeak[0] || null;
  const secondary = missingOrWeak[0]
    ? missingOrWeak[1] || softWeak[0] || null
    : softWeak[1] || null;

  return {
    pillars,
    FINALIZATION_BLOCKER_PRIMARY: primary,
    FINALIZATION_BLOCKER_SECONDARY: secondary,
    provenCount: Object.values(pillars).filter((s) => s === PILLAR_STRENGTH.PROVEN || s === PILLAR_STRENGTH.STRONG)
      .length,
    missingCount: missingOrWeak.length,
    softBlockerCount: softWeak.length,
  };
}

export function isFinalizationEligible(candidate = {}) {
  const q = String(candidate.packetQuality || candidate.packetQualityHint || "").toUpperCase();
  if (q === "SIGNAL_ONLY") {
    return { eligible: false, reason: FINALIZATION_ELIGIBILITY_INELIGIBLE() };
  }
  if (q === "COMPLETE_PLAUSIBLE" || q === "COMPLETE_STRONG") {
    return { eligible: true, reason: "COMPLETE_PLAUSIBLE" };
  }
  if (candidate.validFutureWatch && (candidate.demandControllerId || candidate.publicContactPath)) {
    return { eligible: true, reason: "VALID_FUTURE_WATCH" };
  }
  const audit = auditMissingPillars(candidate);
  if (audit.missingCount <= 2 && audit.provenCount + Object.values(audit.pillars).filter((s) => s === PILLAR_STRENGTH.PLAUSIBLE).length >= 3) {
    return { eligible: true, reason: "PARTIAL_NEAR_COMPLETE" };
  }
  return { eligible: false, reason: "INELIGIBLE_SIGNAL_ONLY" };
}

function FINALIZATION_ELIGIBILITY_INELIGIBLE() {
  return "INELIGIBLE_SIGNAL_ONLY";
}
