/**
 * Next-blocker engine — for every non-final opportunity, name the missing pillars
 * and the highest-value next evidence (no broad search once blocker known).
 */

import { DISCOVERY_SPINE, PACKET_PILLAR_ID } from "./constants.js";
import { selectNextDiscoveryPath } from "./path-selection-engine.js";

function truthy(v) {
  if (v === true) return true;
  if (v === false || v == null) return false;
  const s = String(v).toUpperCase();
  return s && s !== "NONE" && s !== "FALSE" && s !== "UNCONFIRMED" && s !== "UNKNOWN";
}

/**
 * Assess which packet pillars are present on an opportunity-like object.
 */
export function assessPacketPillars(opp = {}) {
  const named =
    Boolean(opp.organizationName || opp.accountName || opp.namedEntity || opp.entityName) &&
    !/unknown|tbd|n\/a/i.test(String(opp.organizationName || opp.accountName || ""));
  const groupMotion = truthy(
    opp.groupMotion ||
      opp.travelMotion ||
      opp.participationRole ||
      opp.demandType ||
      opp.opportunityType
  );
  const buyerOrController = truthy(
    opp.demandControllerId ||
      opp.buyerName ||
      opp.buyerEntity ||
      opp.contactPath ||
      opp.buyerContactPath ||
      (opp.demandController && opp.demandController.controllerName)
  );
  const futureDecision = truthy(
    opp.futureDecisionPoint ||
      opp.decisionWindow ||
      opp.actionWindow ||
      opp.eventStartDate ||
      opp.timingClass === "FUTURE" ||
      opp.validFutureWatch
  );
  const lodging = ["DIRECT_LODGING_EVIDENCE", "STRONG_HOTEL_MOTION", "PLAUSIBLE_HOTEL_MOTION"].includes(
    String(opp.lodgingEvidenceClass || opp.lodgingEvidence || "").toUpperCase()
  );
  const hotelFit = truthy(opp.hotelFit || opp.fitClass || opp.targetHotelFit);

  return {
    [PACKET_PILLAR_ID.NAMED_ENTITY]: named,
    [PACKET_PILLAR_ID.GROUP_MOTION]: groupMotion,
    [PACKET_PILLAR_ID.BUYER_OR_CONTROLLER]: buyerOrController,
    [PACKET_PILLAR_ID.FUTURE_DECISION]: futureDecision,
    [PACKET_PILLAR_ID.LODGING_EVIDENCE]: lodging,
    [PACKET_PILLAR_ID.HOTEL_FIT]: hotelFit,
  };
}

const PILLAR_TO_EVIDENCE = Object.freeze({
  [PACKET_PILLAR_ID.NAMED_ENTITY]: {
    evidence: "named organization / traveling account on official or controller source",
    sourceFamily: "ACCOUNT_OR_PARTICIPANT_LIST",
    path: DISCOVERY_SPINE.ACCOUNT_FIRST,
  },
  [PACKET_PILLAR_ID.GROUP_MOTION]: {
    evidence: "defined group / travel motion (exhibitor, delegate, crew, project team)",
    sourceFamily: "EVENT_MANUAL_OR_TRIGGER",
    path: DISCOVERY_SPINE.PARTICIPANT_FIRST,
  },
  [PACKET_PILLAR_ID.BUYER_OR_CONTROLLER]: {
    evidence: "PCO / DMC / secretariat / housing contact or named buyer path",
    sourceFamily: "CONTROLLER_CONTACT",
    path: DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
  },
  [PACKET_PILLAR_ID.FUTURE_DECISION]: {
    evidence: "future decision window (housing open, RFP, list publish date, cycle date)",
    sourceFamily: "OFFICIAL_TIMELINE",
    path: DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
  },
  [PACKET_PILLAR_ID.LODGING_EVIDENCE]: {
    evidence: "exhibitor manual, PCO housing page, RFP, or hotel-supplied rate request",
    sourceFamily: "HOUSING_OR_RFP",
    path: DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
  },
  [PACKET_PILLAR_ID.HOTEL_FIT]: {
    evidence: "market/destination fit vs target hotel product",
    sourceFamily: "FIT_ENGINE",
    path: DISCOVERY_SPINE.PARTICIPANT_FIRST,
  },
});

/**
 * @returns {{ packetQualityHint, missingPillars, highestValueNextEvidence, bestDiscoveryPath, bestSourceFamily, stopContinue, pathSelection }}
 */
export function computeNextBlocker(opp = {}, context = {}) {
  const pillars = assessPacketPillars(opp);
  const missing = Object.entries(pillars)
    .filter(([, ok]) => !ok)
    .map(([id]) => id);

  const maturity = String(opp.maturity || opp.status || "").toUpperCase();
  const isFinal = maturity === "READY" || opp.ready === true;
  if (isFinal && missing.length === 0) {
    return {
      packetQualityHint: opp.packetQuality || "COMPLETE_STRONG",
      missingPillars: [],
      highestValueNextEvidence: null,
      bestDiscoveryPath: null,
      bestSourceFamily: null,
      stopContinue: "STOP",
      reason: "finalized_complete",
      pillars,
    };
  }

  // Priority order for international stall patterns
  const priority = [
    PACKET_PILLAR_ID.BUYER_OR_CONTROLLER,
    PACKET_PILLAR_ID.LODGING_EVIDENCE,
    PACKET_PILLAR_ID.NAMED_ENTITY,
    PACKET_PILLAR_ID.GROUP_MOTION,
    PACKET_PILLAR_ID.FUTURE_DECISION,
    PACKET_PILLAR_ID.HOTEL_FIT,
  ];
  const topMissing = priority.find((p) => missing.includes(p)) || missing[0] || null;
  const map = topMissing ? PILLAR_TO_EVIDENCE[topMissing] : null;

  const availableEvidence = {
    participantListAbsent: !opp.participantListUrl && !opp.exhibitorListUrl,
    participantListPublished: Boolean(opp.participantListUrl || opp.exhibitorListUrl),
    controllerResolved: Boolean(opp.demandControllerId || opp.demandController),
    lodgingPagePublished: ["DIRECT_LODGING_EVIDENCE", "STRONG_HOTEL_MOTION"].includes(
      String(opp.lodgingEvidenceClass || "").toUpperCase()
    ),
    accountsKnown: pillars[PACKET_PILLAR_ID.NAMED_ENTITY],
    travelingEntityKnown: pillars[PACKET_PILLAR_ID.GROUP_MOTION] && pillars[PACKET_PILLAR_ID.NAMED_ENTITY],
    buyerPathKnown: pillars[PACKET_PILLAR_ID.BUYER_OR_CONTROLLER],
    futureDecisionKnown: pillars[PACKET_PILLAR_ID.FUTURE_DECISION],
    recurringCongress: /congress|congreso|kongress|annual|recurring/i.test(
      String(opp.title || opp.campaignName || "")
    ),
    priorCycleKnown: Boolean(opp.historicalProcess || opp.priorCycle),
    publicDataCeiling: String(opp.stallReason || opp.publicDataCeiling || "").includes("PUBLIC_DATA_CEILING") ||
      opp.publicDataCeiling === true,
    ...(context.availableEvidence || {}),
  };

  const pathSelection = selectNextDiscoveryPath({
    hotelId: context.hotelId || opp.hotelId,
    market: context.market || opp.market,
    country: context.country || opp.country,
    availableEvidence,
    priorResearchState: context.priorResearchState || {},
    hotelSuppliedEvidenceCount: context.hotelSuppliedEvidenceCount || 0,
    marketProfile: context.marketProfile,
  });

  const presentCount = Object.values(pillars).filter(Boolean).length;
  let packetQualityHint = "SIGNAL_ONLY";
  if (presentCount >= 5) packetQualityHint = "COMPLETE_STRONG";
  else if (presentCount >= 4) packetQualityHint = "COMPLETE_PLAUSIBLE";
  else if (presentCount >= 2) packetQualityHint = "PARTIAL_PACKET";

  const stopContinue =
    availableEvidence.publicDataCeiling &&
    missing.includes(PACKET_PILLAR_ID.LODGING_EVIDENCE) &&
    !availableEvidence.controllerResolved
      ? "CONTINUE_CONTROLLER_THEN_STOP"
      : missing.length
        ? "CONTINUE"
        : "STOP";

  return {
    packetQualityHint: opp.packetQuality || packetQualityHint,
    missingPillars: missing,
    highestValueNextEvidence: map?.evidence || pathSelection.nextBestEvidenceTarget,
    bestDiscoveryPath: map?.path || pathSelection.nextBestDiscoveryPath,
    bestSourceFamily: map?.sourceFamily || null,
    stopContinue,
    reason: topMissing ? `blocked_on_${topMissing}` : pathSelection.reason,
    pillars,
    pathSelection,
    nextBlocker: topMissing,
  };
}
