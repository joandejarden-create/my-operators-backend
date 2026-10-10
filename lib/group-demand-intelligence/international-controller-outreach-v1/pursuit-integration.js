/**
 * Pursuit panel fields for controller outreach — draft only, never auto-send.
 */

import { GDI_PURSUIT_MESSAGE_STATUS, GDI_PURSUIT_STATUS, GDI_PURSUIT_RESPONSE_STATUS } from "../pursuit/pursuit-types-v1.js";
import { OUTREACH_READINESS } from "./constants.js";

/**
 * Build pursuit-safe overlay from outreach packet (no Ready mutation).
 */
export function buildControllerOutreachPursuitOverlay(packet = {}) {
  const draft = packet.draft || {};
  const contact = packet.contactAuthority || {};
  const readiness = packet.outreachReadiness || OUTREACH_READINESS.NEEDS_CONTACT_RESEARCH;

  return {
    controller: packet.controllerName || contact.organization || null,
    contact: contact.email || packet.contactEmail || null,
    contactAuthority: contact.authority || null,
    contactChannel: contact.channel || null,
    evidenceQuestion: packet.evidenceQuestion || null,
    draftOutreach: {
      subject: draft.subject || null,
      body: draft.body || null,
      language: draft.language || "es",
      messageStatus: GDI_PURSUIT_MESSAGE_STATUS.DRAFT,
      sent: false,
    },
    sentStatus: "NOT_SENT",
    responseStatus: GDI_PURSUIT_RESPONSE_STATUS.NO_OUTREACH,
    structuredResponseFacts: null,
    nextAction:
      readiness === OUTREACH_READINESS.READY_TO_SEND
        ? "Human send Spanish evidence-request draft; do not auto-email"
        : readiness === OUTREACH_READINESS.WAIT_FOR_PUBLICATION
          ? "Wait for public housing/hotel-list publication"
          : readiness === OUTREACH_READINESS.DO_NOT_PURSUE
            ? "Do not pursue"
            : "Verify contact before send",
    followUpDate: packet.suggestedFollowUpDate || null,
    outreachPriority: packet.outreachPriority || null,
    outreachReadiness: readiness,
    pursuitStatusHint:
      readiness === OUTREACH_READINESS.READY_TO_SEND
        ? GDI_PURSUIT_STATUS.OUTREACH_READY
        : GDI_PURSUIT_STATUS.PREPARE,
    emailsActuallySent: false,
    readyChangedWithoutRealResponse: false,
  };
}

/**
 * Customer-safe card lines for Watch / waiting-controller states.
 */
export function buildControllerOutreachCustomerCard(packet = {}, afterResponse = null) {
  const base = {
    hotelSelection: "Pending confirmation",
    whoControlsIt: packet.controllerName || "Organizing team",
    bestRouteIn: packet.contactEmail || packet.contactAuthority?.email || null,
    nextAction: packet.evidenceQuestion || null,
  };
  if (!afterResponse) return base;

  const st = afterResponse.controllerResponseStatus;
  const lines = { ...base };
  if (st === "HOTEL_LIST_OPEN" || st === "RATE_REQUESTED" || st === "TARGET_HOTEL_CAN_APPLY") {
    lines.hotelSelection = "Hotel selection appears open for submissions";
    lines.nextAction = "Prepare rates / proposal via the confirmed contact path";
  } else if (st === "HOTEL_LIST_FINALIZED" || st === "TARGET_HOTEL_REJECTED") {
    lines.hotelSelection = "Hotel list appears finalized for this cycle";
    lines.nextAction = "Do not pursue inclusion for this cycle";
  } else if (st === "HOTEL_LIST_NOT_YET_OPEN") {
    lines.hotelSelection = "Hotel list expected; not yet published";
    lines.nextAction = "Watch for circular / list publication";
  } else if (st === "SELF_BOOKING_ONLY") {
    lines.hotelSelection = "Delegates book individually";
    lines.nextAction = "Assess whether recommended-list inclusion is still possible";
  } else if (st === "PCO_REDIRECT" || st === "CONTROLLER_REDIRECT") {
    lines.hotelSelection = "Lodging managed by another team";
    lines.nextAction = "Contact the redirected lodging controller";
  }
  return lines;
}

/**
 * Patch fields for buildPursuitDraftFromOpportunity / startPursuit (human handoff).
 */
export function toPursuitDraftFields(packet = {}) {
  const overlay = buildControllerOutreachPursuitOverlay(packet);
  return {
    outreachReadiness: overlay.outreachReadiness === OUTREACH_READINESS.READY_TO_SEND ? "OUTREACH_NOW" : "OUTREACH_PREPARE",
    draftSubject: overlay.draftOutreach.subject,
    draftMessage: overlay.draftOutreach.body,
    draftLanguage: overlay.draftOutreach.language,
    outreachContactEmail: overlay.contact,
    publicContactPath: overlay.contact ? `mailto:${overlay.contact}` : null,
    outreachContactName: packet.controllerName || null,
    outreachContactRole: packet.contactAuthority?.channel || null,
    nextAction: overlay.nextAction,
    nextActionReason: packet.evidenceQuestion || null,
    nextFollowUpDate: overlay.followUpDate,
    controllerOutreachEvidenceQuestion: packet.evidenceQuestion || null,
    controllerOutreachPriority: packet.outreachPriority || null,
  };
}
