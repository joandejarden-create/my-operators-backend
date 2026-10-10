/**
 * GDI Pursuit service — create/update from opportunity intelligence.
 * NEVER mutates Ready / Watch / packet quality gates.
 */

import {
  GDI_PURSUIT_STATUS,
  GDI_HOTEL_INCLUSION_STATUS,
  GDI_PURSUIT_RESPONSE_STATUS,
  GDI_PURSUIT_MESSAGE_STATUS,
  GDI_PURSUIT_OUTCOME,
  GDI_PURSUIT_TRIGGER_STATUS,
  normalizePursuitStatus,
  normalizeHotelInclusionStatus,
  normalizeResponseStatus,
  suggestPursuitStatusFromEvent,
  isClosedPursuitStatus,
} from "./pursuit-types-v1.js";
import {
  createPursuitRecord,
  updatePursuitRecord,
  getPursuitByOpportunityId,
  getPursuitById,
  listPursuits,
  applyFollowUpDue,
  appendHotelSuppliedEvidence,
  toCustomerPursuitDto,
  loadPursuitsDoc,
} from "./pursuit-store-v1.js";
import { applyHotelSuppliedFeedbackLoop } from "../international-discovery-v2/hotel-supplied-evidence.js";
import { requalifyAfterHotelSuppliedResponse } from "../international-finalization/hotel-supplied-response-map.js";
import { classifyControllerOutreachResponse } from "../international-controller-outreach-v1/response-classifier.js";

function addDays(isoDate, days) {
  const d = new Date(`${String(isoDate).slice(0, 10)}T12:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function mapInclusionFromOpportunity(opp = {}) {
  const cls = String(opp.hotelInclusionEvidenceClass || "").toUpperCase();
  const process = String(opp.hotelSelectionProcess || "").toUpperCase();
  const venue = String(opp.venueStatus || "").toUpperCase();
  if (/HOST_HOTEL_NAMED_COMPETITOR|COMPETITOR/.test(venue)) {
    return GDI_HOTEL_INCLUSION_STATUS.COMPETITOR_HOST_CONFIRMED;
  }
  if (cls === "CURRENT_HOTEL_LIST_PUBLISHED") {
    return GDI_HOTEL_INCLUSION_STATUS.HOTEL_LIST_PENDING;
  }
  if (cls === "PAST_HOTEL_LIST_AVAILABLE") {
    return GDI_HOTEL_INCLUSION_STATUS.PROCESS_IDENTIFIED;
  }
  if (process === "UNKNOWN" || !process) {
    return GDI_HOTEL_INCLUSION_STATUS.NOT_YET_OPEN;
  }
  if (process) return GDI_HOTEL_INCLUSION_STATUS.PROCESS_IDENTIFIED;
  return GDI_HOTEL_INCLUSION_STATUS.UNKNOWN;
}

function initialStatusFromOutreach(outreachReadiness) {
  const r = String(outreachReadiness || "").toUpperCase();
  if (r === "OUTREACH_NOW") return GDI_PURSUIT_STATUS.OUTREACH_READY;
  if (r === "OUTREACH_PREPARE") return GDI_PURSUIT_STATUS.PREPARE;
  return GDI_PURSUIT_STATUS.NOT_STARTED;
}

function defaultFollowUp(outreachReadiness, decisionDate, nowDate = "2026-10-07") {
  if (decisionDate && String(decisionDate).slice(0, 10) >= nowDate) {
    return String(decisionDate).slice(0, 10);
  }
  const r = String(outreachReadiness || "").toUpperCase();
  if (r === "OUTREACH_NOW") return addDays(nowDate, 7);
  if (r === "OUTREACH_PREPARE") return addDays(nowDate, 21);
  return addDays(nowDate, 14);
}

/**
 * Eligible to start a pursuit: OUTREACH_NOW or OUTREACH_PREPARE only.
 */
export function canStartPursuitFromOpportunity(opp = {}) {
  const r = String(opp.outreachReadiness || "").toUpperCase();
  return r === "OUTREACH_NOW" || r === "OUTREACH_PREPARE";
}

export function buildNextActionFromOpportunity(opp = {}) {
  const title = opp.title || "this opportunity";
  const action =
    opp.watchCardRecommendedNextStep ||
    opp.recommendedAction ||
    opp.recommendedNextStep ||
    `Contact organizer about hotel inclusion for ${title}`;
  const reason =
    opp.watchCardWhenToAct ||
    opp.outreachReadinessReason ||
    opp.cardWhyNowLine ||
    "Lodging-decision window is open or approaching.";
  return {
    nextAction: action,
    nextActionReason: reason,
    nextActionDueDate:
      opp.nextActionDueDate ||
      opp.futureDecisionDate ||
      opp.nextResearchDate ||
      null,
  };
}

export function buildPursuitDraftFromOpportunity(opp = {}, opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const na = buildNextActionFromOpportunity(opp);
  const outreach = opp.outreachReadiness || null;
  return {
    opportunityId: opp.id,
    opportunityTitle: opp.title || opp.opportunityName || null,
    organizationName: opp.organizationName || opp.buyerOrganization || null,
    accountId: opp.organizationName || opp.buyerOrganization || null,
    gdiFacingState: opp.customerFacingState || opp.opportunityType || null,
    outreachReadiness: outreach,
    pursuitStatus: initialStatusFromOutreach(outreach),
    hotelInclusionStatus: mapInclusionFromOpportunity(opp),
    responseStatus: GDI_PURSUIT_RESPONSE_STATUS.NO_OUTREACH,
    assignedTo: "UNASSIGNED",
    contactName: opp.outreachContactName || opp.primaryContactName || opp.primaryContact?.name || null,
    contactRole: opp.outreachContactRole || opp.primaryContactRole || opp.primaryContact?.role || null,
    contactOrganization:
      opp.buyerOrganization || opp.organizationName || opp.lodgingControllerOrganization || null,
    contactPath: opp.outreachContactUrl || opp.publicContactPath || opp.organizationContactUrl || null,
    contactEmail:
      opp.outreachContactEmail ||
      opp.primaryContact?.email ||
      null,
    nextFollowUpDate: defaultFollowUp(outreach, opp.futureDecisionDate || opp.nextResearchDate, nowDate),
    nextAction: na.nextAction,
    nextActionReason: na.nextActionReason,
    nextActionDueDate: na.nextActionDueDate,
    selectionProcess: opp.hotelSelectionProcess || null,
    decisionWindowStart: opp.decisionWindowStart || null,
    decisionWindowEnd: opp.decisionWindowEnd || null,
    nextTrigger: opp.watchCardNextTrigger || opp.nextTriggerCondition || opp.nextObservableTrigger || null,
    triggerType: opp.monitoringTriggerType || opp.nextTriggerType || null,
    triggerStatus: GDI_PURSUIT_TRIGGER_STATUS.OPEN,
    triggerSource: opp.monitoringTriggerSource || opp.officialSource || null,
    draftSubject: opp.draftSubject || null,
    draftMessage: opp.draftMessage || null,
    draftLanguage: opts.language || opp.draftLanguage || "es",
    messageStatus: GDI_PURSUIT_MESSAGE_STATUS.DRAFT,
    notes: "",
    outcome: GDI_PURSUIT_OUTCOME.NONE,
  };
}

/**
 * Create pursuit from opportunity. Does not change opportunity Ready/Watch fields.
 */
export function startPursuitFromOpportunity(hotelId, opp, opts = {}) {
  if (!canStartPursuitFromOpportunity(opp) && opts.force !== true) {
    return {
      ok: false,
      error: "pursuit_not_eligible",
      message: "Pursuit requires OUTREACH_NOW or OUTREACH_PREPARE.",
    };
  }
  const existing = getPursuitByOpportunityId(hotelId, opp.id);
  if (existing && !isClosedPursuitStatus(existing.pursuitStatus)) {
    return { ok: true, created: false, pursuit: existing };
  }
  const draft = {
    ...buildPursuitDraftFromOpportunity(opp, opts),
    draftSubject: opts.draftSubject || null,
    draftMessage: opts.draftMessage || null,
    draftLanguage: opts.language || "es",
  };
  const { pursuit, created } = createPursuitRecord(
    hotelId,
    draft,
    opts.actor || "hotel_user"
  );
  return { ok: true, created, pursuit };
}

export function updatePursuit(hotelId, pursuitId, patch, opts = {}) {
  const safe = { ...patch };
  // Hard rule: never accept intelligence-gate fields on pursuit updates
  delete safe.customerFacingState;
  delete safe.packetQuality;
  delete safe.hotelMotionClass;
  delete safe.travelingEntityProven;
  delete safe.ready;
  delete safe.isReady;

  if (safe.pursuitStatus != null) safe.pursuitStatus = normalizePursuitStatus(safe.pursuitStatus);
  if (safe.hotelInclusionStatus != null) {
    safe.hotelInclusionStatus = normalizeHotelInclusionStatus(safe.hotelInclusionStatus);
  }
  if (safe.responseStatus != null) {
    safe.responseStatus = normalizeResponseStatus(safe.responseStatus);
  }

  // Structured event suggestions (optional)
  if (opts.eventType) {
    const cur = getPursuitById(hotelId, pursuitId);
    if (cur) {
      safe.pursuitStatus = suggestPursuitStatusFromEvent(
        opts.eventType,
        safe.pursuitStatus || cur.pursuitStatus
      );
    }
  }

  if (safe.responseStatus === GDI_PURSUIT_RESPONSE_STATUS.SENT) {
    const now = new Date().toISOString().slice(0, 10);
    safe.lastContactDate = safe.lastContactDate || now;
    if (!safe.firstContactDate) safe.firstContactDate = now;
    if (!safe.pursuitStatus || safe.pursuitStatus === GDI_PURSUIT_STATUS.OUTREACH_READY) {
      safe.pursuitStatus = GDI_PURSUIT_STATUS.CONTACTED;
    }
  }

  return updatePursuitRecord(
    hotelId,
    pursuitId,
    safe,
    opts.actor || "hotel_user",
    opts.source || "api_update"
  );
}

export function recordPursuitResponse(hotelId, pursuitId, body = {}, opts = {}) {
  const patch = {
    responseStatus: normalizeResponseStatus(body.responseStatus || body.status),
    notes: body.notes != null ? body.notes : undefined,
    customerDidPursue: body.didPursue || body.customerDidPursue || undefined,
  };
  if (body.outcome) patch.outcome = body.outcome;
  if (body.hotelInclusionStatus) {
    patch.hotelInclusionStatus = normalizeHotelInclusionStatus(body.hotelInclusionStatus);
  }
  const eventMap = {
    SENT: "OUTREACH_LOGGED",
    ACKNOWLEDGED: "RESPONSE_RECEIVED",
    INTERESTED: "RESPONSE_RECEIVED",
    REQUESTED_MORE_INFORMATION: "REQUESTED_INFORMATION",
    FOLLOW_UP_REQUESTED: "FOLLOW_UP_DUE",
  };
  const eventType = eventMap[patch.responseStatus] || opts.eventType;
  const res = updatePursuit(hotelId, pursuitId, patch, {
    ...opts,
    eventType,
    source: "record_response",
  });
  if (body.evidenceText || body.hotelSuppliedEvidence) {
    const evidenceText = body.evidenceText || body.hotelSuppliedEvidence;
    const isSyntheticTest = body.isSyntheticTest === true || body.syntheticTest === true;
    // Never persist synthetic test replies as production hotel-supplied evidence
    if (!isSyntheticTest) {
      appendHotelSuppliedEvidence(
        hotelId,
        pursuitId,
        {
          text: evidenceText,
          responseClass: patch.responseStatus,
        },
        opts.actor || "hotel_user"
      );
    }
    // IDV2 + Controller Outreach V1: classify hotel-supplied evidence (no auto-promote; no fabricated send)
    try {
      const classified = classifyControllerOutreachResponse(evidenceText, {
        selectionStatus: res?.pursuit?.selectionProcess?.selectionStatus,
      });
      const requal = requalifyAfterHotelSuppliedResponse({
        opportunity: {
          opportunityId: res?.pursuit?.opportunityId || pursuitId,
          hotelId,
          publicContactPath: res?.pursuit?.contactEmail,
          outreachContactEmail: res?.pursuit?.contactEmail,
          selectionStatus: res?.pursuit?.hotelInclusionStatus,
        },
        responseText: evidenceText,
        hotelId,
        senderEmail: body.senderEmail || body.sender,
        senderRole: body.senderRole,
        isSyntheticTest,
      });
      const feedback = isSyntheticTest
        ? null
        : applyHotelSuppliedFeedbackLoop({
            opportunity: {
              opportunityId: res?.pursuit?.opportunityId || pursuitId,
              hotelId,
            },
            evidenceInput: {
              evidenceText,
              actor: opts.actor || "hotel_user",
              campaignId: res?.pursuit?.campaignId || body.campaignId,
            },
            hotelId,
          });
      if (res && typeof res === "object") {
        res.controllerOutreachClassification = {
          controllerResponseStatus: classified.controllerResponseStatus,
          selectionStatus: classified.selectionStatus,
          readyEligibleFromResponseAlone: false,
          isSyntheticTest,
          persistedAsProductionEvidence: requal.persistedAsProductionEvidence === true,
        };
        res.idv2HotelSuppliedFeedback = feedback
          ? {
              evidenceType: feedback.evidence?.evidenceType,
              lodgingEquivalentOk: feedback.lodgingEquivalent?.ok,
              futureEquivalentOk: feedback.futureEquivalent?.ok,
              nextBlocker: feedback.blocker?.nextBlocker,
              nextBestPath: feedback.pathSelection?.nextBestDiscoveryPath,
              autoPromoted: false,
            }
          : {
              skipped: true,
              reason: isSyntheticTest ? "SYNTHETIC_TEST" : "NO_FEEDBACK",
              autoPromoted: false,
            };
      }
    } catch {
      /* feedback is additive; pursuit write already succeeded */
    }
  }
  return res;
}

export function recordPursuitOutcome(hotelId, pursuitId, body = {}, opts = {}) {
  const outcome = String(body.outcome || "").toUpperCase();
  const patch = {
    outcome,
    outcomeDate: body.outcomeDate || new Date().toISOString().slice(0, 10),
    notes: body.notes != null ? body.notes : undefined,
  };
  if (outcome === "WON" || outcome === "INCLUDED") {
    patch.pursuitStatus = GDI_PURSUIT_STATUS.CLOSED_WON;
    if (outcome === "INCLUDED") {
      patch.hotelInclusionStatus = GDI_HOTEL_INCLUSION_STATUS.INCLUDED_ON_RECOMMENDED_LIST;
    }
  } else if (outcome === "LOST" || outcome === "NOT_INCLUDED" || outcome === "ALREADY_PLACED") {
    patch.pursuitStatus = GDI_PURSUIT_STATUS.CLOSED_LOST;
    if (outcome === "NOT_INCLUDED") {
      patch.hotelInclusionStatus = GDI_HOTEL_INCLUSION_STATUS.NOT_INCLUDED;
    }
    if (outcome === "ALREADY_PLACED") {
      patch.hotelInclusionStatus = GDI_HOTEL_INCLUSION_STATUS.SELECTION_CLOSED;
    }
  } else if (outcome === "NOT_RELEVANT" || outcome === "DEFERRED") {
    patch.pursuitStatus =
      outcome === "DEFERRED"
        ? GDI_PURSUIT_STATUS.DEFERRED
        : GDI_PURSUIT_STATUS.CLOSED_NO_ACTION;
  }
  return updatePursuit(hotelId, pursuitId, patch, {
    ...opts,
    source: "record_outcome",
  });
}

export {
  listPursuits,
  getPursuitById,
  getPursuitByOpportunityId,
  applyFollowUpDue,
  toCustomerPursuitDto,
  loadPursuitsDoc,
  GDI_PURSUIT_STATUS,
  GDI_HOTEL_INCLUSION_STATUS,
  GDI_PURSUIT_RESPONSE_STATUS,
  GDI_PURSUIT_OUTCOME,
};
