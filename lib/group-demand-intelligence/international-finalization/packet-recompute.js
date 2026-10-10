/**
 * Recompute packet quality after finalization evidence.
 * Does NOT call production Ready gate with lowered thresholds.
 * Ready promotion requires explicit readyGateOk from unchanged gate.
 */

import { PILLAR_STRENGTH } from "./constants.js";
import { FINAL_DISPOSITION, TARGET_HOTEL_FIT, SELECTION_STATUS } from "./constants.js";
import { auditMissingPillars } from "./missing-pillar-audit.js";
import { meetsReadyContactRequirement } from "../buyer-contact-path-taxonomy-v1.js";

/**
 * Derive packet quality from pillar strengths (honest — organizer shell ≠ named entity PROVEN).
 */
export function recomputePacketQuality(candidate = {}, pillarAudit = null) {
  const audit = pillarAudit || auditMissingPillars(candidate);
  const pillars = audit.pillars || {};
  const vals = Object.values(pillars);
  const strongish = vals.filter((s) => s === PILLAR_STRENGTH.PROVEN || s === PILLAR_STRENGTH.STRONG).length;
  const plausible = vals.filter((s) => s === PILLAR_STRENGTH.PLAUSIBLE).length;
  const missing = vals.filter((s) => s === PILLAR_STRENGTH.MISSING || s === PILLAR_STRENGTH.CONTRADICTED).length;

  // COMPLETE_STRONG: every commercial pillar PROVEN/STRONG — lodging/fit PLAUSIBLE never suffices
  const requiredKeys = Object.keys(pillars);
  const allRequiredStrong =
    requiredKeys.length >= 6 &&
    requiredKeys.every((k) => [PILLAR_STRENGTH.PROVEN, PILLAR_STRENGTH.STRONG].includes(pillars[k]));

  let packetQuality = "SIGNAL_ONLY";
  if (allRequiredStrong && missing === 0 && vals.length >= 6) packetQuality = "COMPLETE_STRONG";
  else if (strongish + plausible >= 4 && missing <= 2) packetQuality = "COMPLETE_PLAUSIBLE";
  else if (strongish + plausible >= 2) packetQuality = "PARTIAL_PACKET";

  return { packetQuality, audit, strongish, plausible, missing, allRequiredStrong };
}

/**
 * Final disposition — Ready only if unchanged gate inputs would pass.
 * This function does NOT lower thresholds; it refuses Ready when fit UNKNOWN or selection CLOSED.
 */
export function computeFinalDisposition({
  candidate = {},
  packetQuality,
  fit = {},
  selectionProcess = {},
  lodgingDecision = {},
  pillarAudit,
} = {}) {
  const fitClass = fit.targetHotelFit || candidate.targetHotelFit || TARGET_HOTEL_FIT.UNKNOWN;
  const status = selectionProcess.selectionStatus || SELECTION_STATUS.UNKNOWN;

  if (fitClass === TARGET_HOTEL_FIT.NO_FIT) {
    return {
      disposition: FINAL_DISPOSITION.NO_TARGET_HOTEL_FIT,
      ready: false,
      watch: false,
      reason: fit.reason || "NO_FIT",
    };
  }
  if (status === SELECTION_STATUS.CLOSED || status === SELECTION_STATUS.PLACED) {
    // CLOSED host-only with no secondary path
    if (!lodgingDecision.secondaryOverflowPossible) {
      return {
        disposition: FINAL_DISPOSITION.SELECTION_ALREADY_CLOSED,
        ready: false,
        watch: false,
        reason: "SELECTION_CLOSED",
      };
    }
  }
  if (fitClass === TARGET_HOTEL_FIT.WRONG_DESTINATION) {
    return {
      disposition: FINAL_DISPOSITION.WRONG_DESTINATION,
      ready: false,
      watch: false,
      reason: "WRONG_DESTINATION",
    };
  }

  const contact = meetsReadyContactRequirement({
    buyerEntity: candidate.controllerName || candidate.buyerEntity,
    buyerName: candidate.buyerName,
    buyerRole: candidate.controllerType || candidate.buyerRole,
    contactUrl: candidate.publicContactPath || candidate.contactPath,
    officialHousingUrl: candidate.officialHousingUrl,
    lodgingEvidence: candidate.lodgingEvidenceClass,
  });

  const pillars = pillarAudit?.pillars || {};
  const lodgingOk = [PILLAR_STRENGTH.PROVEN, PILLAR_STRENGTH.STRONG].includes(
    pillars.HOTEL_LODGING_EVIDENCE
  );
  // Ready-eligible fit must be explicit; PLAUSIBLE_FIT with readyEligibleFit=false (e.g. not on list) blocks Ready
  const fitOk =
    fit.readyEligibleFit === true &&
    [TARGET_HOTEL_FIT.STRONG_FIT, TARGET_HOTEL_FIT.PLAUSIBLE_FIT].includes(fitClass);
  const namedOk = [PILLAR_STRENGTH.PROVEN, PILLAR_STRENGTH.STRONG].includes(
    pillars.NAMED_ENTITY
  );
  const controllerOk = [PILLAR_STRENGTH.PROVEN, PILLAR_STRENGTH.STRONG].includes(
    pillars.BUYER_OR_DEMAND_CONTROLLER
  );
  const futureOk = [PILLAR_STRENGTH.PROVEN, PILLAR_STRENGTH.STRONG, PILLAR_STRENGTH.PLAUSIBLE].includes(
    pillars.FUTURE_DECISION_POINT
  );
  const motionOk = [PILLAR_STRENGTH.PROVEN, PILLAR_STRENGTH.STRONG, PILLAR_STRENGTH.PLAUSIBLE].includes(
    pillars.DEFINED_GROUP_MOTION
  );

  // Ready: all commercial pillars + contact + fit not UNKNOWN + selection not CLOSED
  // Named traveling entity must be STRONG/PROVEN (not organizer shell)
  const ready =
    packetQuality === "COMPLETE_STRONG" &&
    namedOk &&
    motionOk &&
    controllerOk &&
    futureOk &&
    lodgingOk &&
    fitOk &&
    contact.ok === true &&
    fitClass !== TARGET_HOTEL_FIT.UNKNOWN &&
    status !== SELECTION_STATUS.CLOSED;

  if (ready) {
    return {
      disposition: FINAL_DISPOSITION.READY,
      ready: true,
      watch: false,
      reason: "ALL_PILLARS_AND_CONTACT",
      contactClass: contact.class,
    };
  }

  // Valid Future Watch — commercially useful, future window, controller path, not closed
  const watch =
    !ready &&
    futureOk &&
    controllerOk &&
    fitClass !== TARGET_HOTEL_FIT.NO_FIT &&
    status !== SELECTION_STATUS.CLOSED &&
    (packetQuality === "COMPLETE_PLAUSIBLE" ||
      packetQuality === "COMPLETE_STRONG" ||
      packetQuality === "PARTIAL_PACKET") &&
    Boolean(candidate.publicContactPath || lodgingDecision.customerSafe?.bestRouteIn);

  if (watch) {
    return {
      disposition: FINAL_DISPOSITION.VALID_FUTURE_WATCH,
      ready: false,
      watch: true,
      reason: "COMMERCIALLY_USEFUL_WAITING_PILLAR",
      waitingOn: pillarAudit?.FINALIZATION_BLOCKER_PRIMARY || null,
    };
  }

  if (packetQuality === "COMPLETE_STRONG") {
    return { disposition: FINAL_DISPOSITION.COMPLETE_STRONG, ready: false, watch: false, reason: "STRONG_NOT_READY_GATE" };
  }
  if (packetQuality === "COMPLETE_PLAUSIBLE") {
    return {
      disposition: FINAL_DISPOSITION.WAITING_FOR_TRIGGER,
      ready: false,
      watch: Boolean(candidate.validFutureWatch),
      reason: "PLAUSIBLE_WAITING",
    };
  }
  if (!namedOk && controllerOk) {
    return {
      disposition: FINAL_DISPOSITION.NO_TRAVELING_ENTITY,
      ready: false,
      watch: false,
      reason: "ORGANIZER_OR_CONTROLLER_WITHOUT_TRAVELING_ACCOUNT",
    };
  }
  if (status === SELECTION_STATUS.UNKNOWN && !lodgingOk) {
    return {
      disposition: FINAL_DISPOSITION.WAITING_FOR_TRIGGER,
      ready: false,
      watch: Boolean(candidate.validFutureWatch),
      reason: "PUBLIC_DATA_CEILING_OR_UNKNOWN_SELECTION",
    };
  }
  return {
    disposition: FINAL_DISPOSITION.PARTIAL_PACKET,
    ready: false,
    watch: false,
    reason: "INCOMPLETE",
  };
}
