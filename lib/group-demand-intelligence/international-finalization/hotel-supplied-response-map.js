/**
 * Map hotel-supplied / pursuit responses → structured selection + lodging facts.
 * Does NOT auto-promote Ready.
 */

import { SELECTION_STATUS, SELECTION_MODEL } from "./constants.js";
import {
  classifyPursuitResponseText,
  buildHotelSuppliedEvidenceRecord,
  applyHotelSuppliedFeedbackLoop,
} from "../international-discovery-v2/hotel-supplied-evidence.js";
import { HOTEL_SUPPLIED_EVIDENCE_TYPE } from "../international-discovery-v2/constants.js";
import {
  classifyControllerOutreachResponse,
  assessResponseAuthority,
} from "../international-controller-outreach-v1/response-classifier.js";

/**
 * Map free-text response to structured commercial facts.
 */
export function mapHotelSuppliedResponseToFacts(text = "", prior = {}) {
  const evidenceType = classifyPursuitResponseText(text);
  const t = String(text || "").toLowerCase();
  const facts = {
    evidenceType,
    selectionStatus: prior.selectionStatus || SELECTION_STATUS.UNKNOWN,
    selectionModel: prior.selectionModel || SELECTION_MODEL.UNKNOWN,
    lodgingEvidenceClass: prior.lodgingEvidenceClass || "UNCONFIRMED",
    buyerControllerProven: false,
    futureDecisionProven: false,
    closedForPursuit: false,
    selfBookingProven: false,
    readyEligibleFromResponseAlone: false,
    notes: [],
  };

  if (evidenceType === HOTEL_SUPPLIED_EVIDENCE_TYPE.RATE_REQUEST || /send rates|tarif|precios|preisliste/i.test(t)) {
    facts.selectionStatus = SELECTION_STATUS.OPEN;
    facts.lodgingEvidenceClass = "STRONG_HOTEL_MOTION";
    facts.buyerControllerProven = true;
    facts.futureDecisionProven = true;
    facts.notes.push("Rate request ⇒ selection OPEN + strong lodging motion");
  }
  if (/submit proposal|rfp|send.*proposal|propuesta|angebot/i.test(t)) {
    facts.selectionStatus = SELECTION_STATUS.OPEN;
    facts.lodgingEvidenceClass = "DIRECT_LODGING_EVIDENCE";
    facts.futureDecisionProven = true;
    facts.notes.push("Proposal/RFP ask ⇒ direct lodging evidence path");
  }
  if (/selecting hotels next month|lista.*enero|hotelliste|next month/i.test(t)) {
    facts.selectionStatus = SELECTION_STATUS.EXPECTED;
    facts.futureDecisionProven = true;
    facts.lodgingEvidenceClass =
      facts.lodgingEvidenceClass === "UNCONFIRMED" ? "PLAUSIBLE_HOTEL_MOTION" : facts.lodgingEvidenceClass;
    facts.notes.push("Future list timing ⇒ EXPECTED selection");
  }
  if (/our pco|pco handles|dmc handles|secretar[ií]a|agencia/i.test(t)) {
    facts.buyerControllerProven = true;
    facts.notes.push("Controller handoff named — still need lodging authority evidence");
  }
  if (/already selected|list.*final|lista.*cerrad|nicht mehr|already finalized|hotel list is closed/i.test(t)) {
    facts.selectionStatus = SELECTION_STATUS.CLOSED;
    facts.closedForPursuit = true;
    facts.notes.push("Selection closed — not Ready for new pursuit");
  }
  if (/can add|podemos incluir|können wir.*aufnehmen|we can add your property/i.test(t)) {
    facts.selectionStatus = SELECTION_STATUS.OPEN;
    facts.lodgingEvidenceClass = "STRONG_HOTEL_MOTION";
    facts.notes.push("Explicit add path ⇒ OPEN");
  }
  if (/not accepting|no aceptamos|keine hotels mehr/i.test(t)) {
    facts.selectionStatus = SELECTION_STATUS.CLOSED;
    facts.closedForPursuit = true;
  }
  if (/delegates book individually|reservar directamente|einzeln buchen|self.?book/i.test(t)) {
    facts.selfBookingProven = true;
    facts.selectionModel = SELECTION_MODEL.SELF_BOOKING_RECOMMENDED_LIST;
    facts.notes.push("Self-booking model proven — account-level room demand still needs evaluation");
  }
  if (/overflow|secondary|partner hotel|hoteles secundarios/i.test(t)) {
    facts.notes.push("Overflow/secondary mentioned — require explicit confirmation before fit upgrade");
    facts.secondaryPartnerOpen = /still|aún|noch|open|consider/i.test(t);
  }

  // Hard rule: outreach response alone never Ready
  facts.readyEligibleFromResponseAlone = false;
  facts.evidence = buildHotelSuppliedEvidenceRecord({
    evidenceText: text,
    evidenceType,
    source: "PURSUIT_RESPONSE",
  });

  return facts;
}

/**
 * Requalify candidate after hotel-supplied response (wraps V2 loop + selection facts).
 */
export function requalifyAfterHotelSuppliedResponse({
  opportunity = {},
  responseText = "",
  hotelId,
  marketProfile = {},
  senderEmail = null,
  senderRole = null,
  isSyntheticTest = false,
} = {}) {
  // Prefer controller-outreach classifier (superset) when available
  const mapped = classifyControllerOutreachResponse(responseText, {
    selectionStatus: opportunity.selectionStatus,
    selectionModel: opportunity.selectionModel,
    lodgingEvidenceClass: opportunity.lodgingEvidenceClass,
  });
  const responseAuthority = assessResponseAuthority({
    senderEmail,
    expectedEmail: opportunity.publicContactPath || opportunity.outreachContactEmail,
    senderRole,
    isSyntheticTest,
  });
  if (isSyntheticTest || responseAuthority.okForCanonicalFacts === false) {
    return {
      mapped,
      loop: null,
      responseAuthority,
      autoPromotedReady: false,
      persistedAsProductionEvidence: false,
      note: isSyntheticTest
        ? "SYNTHETIC_TEST — classified only; not persisted as production HOTEL_SUPPLIED_EVIDENCE"
        : "Response authority too weak for canonical commercial facts — do not overwrite public evidence",
    };
  }
  const loop = applyHotelSuppliedFeedbackLoop({
    opportunity: {
      ...opportunity,
      lodgingEvidenceClass: mapped.lodgingEvidenceClass,
      selectionStatus: mapped.selectionStatus,
    },
    evidenceInput: { evidenceText: responseText, whoSupplied: senderEmail || senderRole },
    hotelId,
    marketProfile,
  });
  return {
    mapped,
    loop,
    responseAuthority,
    autoPromotedReady: false,
    persistedAsProductionEvidence: true,
    note: "Hotel-supplied evidence updates lodging/selection facts only; Ready gate unchanged",
  };
}
