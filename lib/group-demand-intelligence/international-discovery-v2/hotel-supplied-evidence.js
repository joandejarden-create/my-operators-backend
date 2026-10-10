/**
 * HOTEL_SUPPLIED_EVIDENCE formalization + Pursuit → GDI feedback (no auto-promote).
 */

import { HOTEL_SUPPLIED_EVIDENCE_TYPE, EVIDENCE_ROUTE, DISCOVERY_SPINE } from "./constants.js";
import { evaluateEquivalentEvidence, CANONICAL_FACTS } from "./equivalent-evidence-policy.js";
import { runHotelHistoryFirstSpine } from "./discovery-spines.js";
import { computeNextBlocker } from "./next-blocker-engine.js";
import { selectNextDiscoveryPath } from "./path-selection-engine.js";

const TYPES = new Set(Object.values(HOTEL_SUPPLIED_EVIDENCE_TYPE));

/** Map free-text organizer responses → evidence type. */
export function classifyPursuitResponseText(text = "") {
  const t = String(text || "").toLowerCase();
  if (/send rates|rate request|tarif|precios|preisliste/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.RATE_REQUEST;
  }
  if (/selecting hotels|hotel list publishes|lista de hoteles|hotelliste/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.HOTEL_LIST_INVITATION;
  }
  if (/meeting package|send.*package|paquete/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.ORGANIZER_RESPONSE;
  }
  if (/our pco|pco handles|contact this agency|dmc handles|secretar[ií]a/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.PCO_RESPONSE;
  }
  if (/already selected|otro hotel|anderes hotel|lost/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.LOST_GROUP;
  }
  if (/room block|bloque|kontingent|allotment/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.ROOM_BLOCK_REQUEST;
  }
  if (/rfp|procurement|licitaci/i.test(t)) {
    return HOTEL_SUPPLIED_EVIDENCE_TYPE.CURRENT_RFP;
  }
  return HOTEL_SUPPLIED_EVIDENCE_TYPE.ORGANIZER_RESPONSE;
}

export function buildHotelSuppliedEvidenceRecord(input = {}) {
  const evidenceType = TYPES.has(String(input.evidenceType || "").toUpperCase())
    ? String(input.evidenceType).toUpperCase()
    : classifyPursuitResponseText(input.evidenceText || input.text);

  return {
    evidenceId: input.evidenceId || `hse_${Date.now().toString(36)}`,
    evidenceType,
    source: input.source || "PURSUIT_RESPONSE",
    timestamp: input.timestamp || new Date().toISOString(),
    whoSupplied: input.whoSupplied || input.actor || "hotel_user",
    evidenceText: input.evidenceText || input.text || null,
    structuredValue: input.structuredValue || null,
    confidence: input.confidence || "MEDIUM",
    entityLinkage: input.entityLinkage || input.organizationName || null,
    campaignLinkage: input.campaignLinkage || input.campaignId || null,
    opportunityId: input.opportunityId || null,
    hotelId: input.hotelId || null,
    provenance: "HOTEL_SUPPLIED_EVIDENCE",
    evidenceRoute: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
  };
}

/**
 * Re-run controller / future decision / lodging / packet assessment after hotel-supplied evidence.
 * Does NOT auto-promote Ready/Watch.
 */
export function applyHotelSuppliedFeedbackLoop({
  opportunity = {},
  evidenceInput = {},
  hotelId,
  marketProfile = {},
} = {}) {
  const evidence = buildHotelSuppliedEvidenceRecord({
    ...evidenceInput,
    hotelId: hotelId || opportunity.hotelId,
    opportunityId: opportunity.opportunityId || opportunity.id,
  });

  const lodgingEq = evaluateEquivalentEvidence({
    fact: CANONICAL_FACTS.LODGING_SELECTION_ACTIVE,
    route: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
    form:
      evidence.evidenceType === HOTEL_SUPPLIED_EVIDENCE_TYPE.RATE_REQUEST
        ? "organizer_rate_request"
        : evidence.evidenceType === HOTEL_SUPPLIED_EVIDENCE_TYPE.ROOM_BLOCK_REQUEST
          ? "room_block_request"
          : "hotel_list_invitation",
    evidence: { provenance: "HOTEL_SUPPLIED_EVIDENCE", sourceUrl: evidence.source },
  });

  const futureEq = evaluateEquivalentEvidence({
    fact: CANONICAL_FACTS.FUTURE_DECISION_POINT,
    route: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
    form: "selecting_hotels_next_month",
    evidence: { provenance: "HOTEL_SUPPLIED_EVIDENCE" },
  });

  const next = { ...opportunity };
  next.hotelSuppliedEvidence = [...(next.hotelSuppliedEvidence || []), evidence];
  next.discoverySpines = [
    ...new Set([...(next.discoverySpines || []), DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]),
  ];

  // Soft updates only when equivalent evidence qualifies — never maturity auto-promote
  if (lodgingEq.ok && !["DIRECT_LODGING_EVIDENCE", "STRONG_HOTEL_MOTION"].includes(next.lodgingEvidenceClass)) {
    next.lodgingEvidenceClass = next.lodgingEvidenceClass || "PLAUSIBLE_HOTEL_MOTION";
    next.lodgingEvidenceRoute = EVIDENCE_ROUTE.HOTEL_SUPPLIED;
  }
  if (futureEq.ok && !next.futureDecisionPoint) {
    next.futureDecisionPoint = {
      source: "HOTEL_SUPPLIED_EVIDENCE",
      text: evidence.evidenceText,
      requiresConfirmation: true,
    };
  }

  // Controller hint from PCO_RESPONSE text
  if (evidence.evidenceType === HOTEL_SUPPLIED_EVIDENCE_TYPE.PCO_RESPONSE) {
    const m = String(evidence.evidenceText || "").match(
      /(?:pco|dmc|agency|agencia|secretar[ií]a)\s*[:\-]?\s*([A-Z][\w &.'-]{2,50})/i
    );
    if (m && !next.demandControllerId) {
      next.processControllerHint = {
        name: m[1].trim(),
        role: "FROM_HOTEL_SUPPLIED_RESPONSE",
        requiresCurrentEvidence: true,
      };
    }
  }

  const historySpine = runHotelHistoryFirstSpine({
    hotelId: hotelId || next.hotelId,
    hotelSupplied: [evidence],
  });

  const blocker = computeNextBlocker(next, {
    hotelId: hotelId || next.hotelId,
    marketProfile,
    hotelSuppliedEvidenceCount: next.hotelSuppliedEvidence.length,
    availableEvidence: {
      hotelHistoryAvailable: true,
      controllerResolved: Boolean(next.demandControllerId),
    },
  });

  const pathSelection = selectNextDiscoveryPath({
    hotelId: hotelId || next.hotelId,
    marketProfile,
    hotelSuppliedEvidenceCount: next.hotelSuppliedEvidence.length,
    availableEvidence: {
      hotelHistoryAvailable: true,
      controllerResolved: Boolean(next.demandControllerId),
      publicDataCeiling: next.publicDataCeiling === true,
    },
  });

  return {
    opportunity: next,
    evidence,
    lodgingEquivalent: lodgingEq,
    futureEquivalent: futureEq,
    historySpine,
    blocker,
    pathSelection,
    autoPromoted: false,
    note: "Pursuit feedback updates evidence routes only; Ready/Watch gates unchanged",
  };
}
