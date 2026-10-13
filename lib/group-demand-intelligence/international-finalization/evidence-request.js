/**
 * Controller-first finalization — next commercial evidence question (not broad search).
 */

import { EVIDENCE_ASK_CATEGORY, SELECTION_STATUS } from "./constants.js";
import { PACKET_PILLAR_ID } from "../international-discovery-v2/constants.js";

/**
 * Determine the single most useful evidence ask for the primary blocker.
 */
export function buildEvidenceRequest({
  candidate = {},
  primaryBlocker = null,
  selectionProcess = {},
  lodgingDecision = {},
  fit = {},
} = {}) {
  const controllerName = candidate.controllerName || lodgingDecision.lodgingControllerName || "the organizing team";
  const contact = candidate.publicContactPath || selectionProcess.submissionPath || null;
  const status = selectionProcess.selectionStatus || SELECTION_STATUS.UNKNOWN;

  let category = EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS;
  let nextEvidenceQuestion = "Are partner hotels still being selected for this event?";
  let bestAsk = nextEvidenceQuestion;
  let whyThisAskMatters = "Confirms whether a commercial lodging path still exists.";

  if (primaryBlocker === PACKET_PILLAR_ID.HOTEL_FIT) {
    category = EVIDENCE_ASK_CATEGORY.HOTEL_ELIGIBILITY;
    if (status === SELECTION_STATUS.PARTIALLY_PLACED) {
      category = EVIDENCE_ASK_CATEGORY.OVERFLOW;
      nextEvidenceQuestion = "Are secondary / overflow / partner hotels still being considered alongside the host hotel?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "Host is locked; only evidenced secondary paths can support target-hotel fit.";
    } else if (fit.targetHotelFit === "WEAK_FIT") {
      nextEvidenceQuestion = "What hotel criteria (location, size, meeting space) are required for inclusion?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "Determines whether this property can legitimately serve the demand.";
    } else if (status === SELECTION_STATUS.UNDER_REVIEW) {
      category = EVIDENCE_ASK_CATEGORY.PREFERRED_HOTEL_LIST;
      nextEvidenceQuestion = "Can a hotel still be added to the recommended list before the booking peak?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "List revision window is the commercial path when a list already exists.";
    } else {
      nextEvidenceQuestion = "Can this hotel be evaluated for the preferred / official hotel program?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "Clarifies eligibility without assuming overflow.";
    }
  } else if (primaryBlocker === PACKET_PILLAR_ID.LODGING_EVIDENCE) {
    if (status === SELECTION_STATUS.EXPECTED || status === SELECTION_STATUS.NOT_STARTED) {
      category = EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS;
      nextEvidenceQuestion = "When will the hotel list or accommodation guidance be published?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "Converts expected housing process into a dated lodging decision.";
    } else if (status === SELECTION_STATUS.OPEN) {
      category = EVIDENCE_ASK_CATEGORY.RATE_SUBMISSION;
      nextEvidenceQuestion = "Are you accepting hotel rate submissions for the recommended / official list?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "Rate-submission acceptance is strong lodging-motion evidence.";
    } else {
      category = EVIDENCE_ASK_CATEGORY.LODGING_CONTROLLER;
      nextEvidenceQuestion = "Who manages accommodation / hotel selection for this event?";
      bestAsk = nextEvidenceQuestion;
      whyThisAskMatters = "Identifies the lodging decision owner when public pages are thin.";
    }
  } else if (primaryBlocker === PACKET_PILLAR_ID.NAMED_ENTITY) {
    category = EVIDENCE_ASK_CATEGORY.HOTEL_ELIGIBILITY;
    nextEvidenceQuestion = "Which organizations / delegations are confirmed to travel for this cycle?";
    bestAsk = nextEvidenceQuestion;
    whyThisAskMatters = "Ready requires a named traveling account, not only an organizer shell.";
  } else if (status === SELECTION_STATUS.CLOSED || status === SELECTION_STATUS.PLACED) {
    category = EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS;
    nextEvidenceQuestion = "Is the hotel list finalized, or can additional properties still be considered?";
    bestAsk = nextEvidenceQuestion;
    whyThisAskMatters = "Avoids pursuing a closed selection cycle.";
  } else if (status === SELECTION_STATUS.UNDER_REVIEW) {
    category = EVIDENCE_ASK_CATEGORY.PREFERRED_HOTEL_LIST;
    nextEvidenceQuestion = "Can a hotel still be added to the recommended list before the booking peak?";
    bestAsk = nextEvidenceQuestion;
    whyThisAskMatters = "List revision window is the commercial path when a list already exists.";
  }

  return {
    nextEvidenceQuestion,
    targetController: controllerName,
    bestContactPath: contact,
    bestAsk,
    whyThisAskMatters,
    category,
    stopContinue: primaryBlocker ? "CONTINUE_BOUNDED" : "STOP",
    publicSourceFamilies: suggestPublicSources(primaryBlocker, selectionProcess),
  };
}

function suggestPublicSources(blocker, selectionProcess) {
  const base = [
    "official_accommodation_page",
    "pco_page",
    "delegate_guide",
    "exhibitor_manual",
    "registration_portal",
    "secretariat_contact",
  ];
  if (blocker === PACKET_PILLAR_ID.HOTEL_FIT) {
    return ["official_hotel_page", "venue_page", "exhibitor_manual", "historical_partner_list"];
  }
  if (selectionProcess.selectionModel === "PROCUREMENT_RFP") {
    return ["procurement_notice", "tender_document", "vergabe_portal"];
  }
  return base;
}

/**
 * Finalization next-blocker after a pass.
 */
export function computeFinalizationNextBlocker({
  pillarAudit,
  evidenceRequest,
  selectionProcess,
  fit,
} = {}) {
  return {
    missingFinalPillar: pillarAudit?.FINALIZATION_BLOCKER_PRIMARY || null,
    secondaryPillar: pillarAudit?.FINALIZATION_BLOCKER_SECONDARY || null,
    bestEvidenceRoute: evidenceRequest?.category || null,
    bestController: evidenceRequest?.targetController || null,
    bestContactPath: evidenceRequest?.bestContactPath || null,
    bestNextQuestion: evidenceRequest?.nextEvidenceQuestion || null,
    stopContinue: evidenceRequest?.stopContinue || "STOP",
    selectionStatus: selectionProcess?.selectionStatus || null,
    targetHotelFit: fit?.targetHotelFit || null,
  };
}
