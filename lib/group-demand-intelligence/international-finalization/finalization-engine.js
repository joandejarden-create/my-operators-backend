/**
 * International Finalization Engine V1 — orchestrates fit → selection → lodging → evidence ask → disposition.
 * No Ready threshold changes. No city-only fit. No inferred overflow.
 */

import { IFE_VERSION, TARGET_HOTEL_FIT } from "./constants.js";
import { auditMissingPillars, isFinalizationEligible } from "./missing-pillar-audit.js";
import { evaluateTargetHotelFit, assertFitNotCityOnly } from "./target-hotel-fit.js";
import {
  buildHotelSelectionProcess,
  fromLodgingDecisionIntelligenceRow,
} from "./hotel-selection-process.js";
import { buildLodgingDecisionState } from "./lodging-decision.js";
import { buildEvidenceRequest, computeFinalizationNextBlocker } from "./evidence-request.js";
import { generateEvidenceOutreachDraft } from "./outreach-drafts.js";
import { recomputePacketQuality, computeFinalDisposition } from "./packet-recompute.js";
import { buildCustomerFinalizationCard } from "./customer-card.js";

/**
 * Run finalization on a single candidate.
 */
export function finalizeCandidate(input = {}) {
  const {
    candidate = {},
    hotel = {},
    demand = {},
    controller = null,
    selectionHints = {},
    ldiRow = null,
    languages = ["en"],
  } = input;

  const eligibility = isFinalizationEligible(candidate);
  if (!eligibility.eligible) {
    return {
      ok: false,
      eligible: false,
      reason: eligibility.reason,
      opportunityId: candidate.opportunityId || candidate.campaignId,
      ifeVersion: IFE_VERSION,
    };
  }

  // 1) Selection process
  let selectionProcess = ldiRow
    ? fromLodgingDecisionIntelligenceRow({
        ...ldiRow,
        knownOfficialHotels: demand.knownOfficialHotels || selectionHints.knownOfficialHotels,
      })
    : buildHotelSelectionProcess({
        ...selectionHints,
        demandControllerId: controller?.demandControllerId || candidate.demandControllerId,
        publicContactPath: candidate.publicContactPath,
        evidenceSource: candidate.evidenceSource || controller?.evidenceSource,
      });

  // Merge explicit hints
  selectionProcess = buildHotelSelectionProcess({
    ...selectionProcess,
    ...selectionHints,
    selectionControllerId: controller?.demandControllerId || selectionProcess.selectionControllerId,
    knownOfficialHotels:
      demand.knownOfficialHotels || selectionHints.knownOfficialHotels || selectionProcess.knownOfficialHotels,
  });

  // 2) Target hotel fit
  const fitDemand = {
    ...demand,
    selectionModel: selectionProcess.selectionModel,
    knownOfficialHotels: selectionProcess.knownOfficialHotels,
    knownCompetitorHost: demand.knownCompetitorHost || demand.officialHostHotel,
    historicalSecondaryPartner: demand.historicalSecondaryPartner || selectionHints.historicalSecondaryPartner,
    secondaryPartnerOpen: demand.secondaryPartnerOpen === true,
    overflowEvidenced: demand.overflowEvidenced === true,
    onRecommendedList: demand.onRecommendedList,
    destinationCity: demand.destinationCity || hotel.city || hotel.market,
  };
  const fit = evaluateTargetHotelFit({ hotel, demand: fitDemand, fitEvidence: demand.fitEvidence || [] });
  const cityOnlyCheck = assertFitNotCityOnly(fit);
  if (!cityOnlyCheck.ok && fit.targetHotelFit === TARGET_HOTEL_FIT.STRONG_FIT) {
    fit.targetHotelFit = TARGET_HOTEL_FIT.PLAUSIBLE_FIT;
    fit.fitClass = TARGET_HOTEL_FIT.PLAUSIBLE_FIT;
    fit.note = "Downgraded from STRONG — city-only STRONG forbidden";
  }

  // 3) Enrich candidate with fit + lodging class for pillar audit
  // Subject lodging class may be weaker than event-level housing pages (host locked / not on list)
  const lodgingEvidenceClass = subjectLodgingEvidenceClass({
    candidate,
    demand,
    selectionProcess,
    fit,
  });
  const enriched = {
    ...candidate,
    targetHotelFit: fit.targetHotelFit,
    hotelFit: fit.targetHotelFit,
    lodgingAuthority: controller?.lodgingAuthority || candidate.lodgingAuthority,
    controllerAuthority: controller?.lodgingAuthority || candidate.controllerAuthority,
    demandControllerId: controller?.demandControllerId || candidate.demandControllerId,
    controllerName: controller?.controllerName || candidate.controllerName,
    publicContactPath: candidate.publicContactPath || controller?.publicContactPath,
    selectionStatus: selectionProcess.selectionStatus,
    lodgingEvidenceClass,
  };

  // 4) Pillar audit (after fit)
  const pillarAudit = auditMissingPillars(enriched);

  // 5) Lodging decision state
  const lodgingDecision = buildLodgingDecisionState({
    controller: controller || {
      demandControllerId: enriched.demandControllerId,
      controllerName: enriched.controllerName,
      lodgingAuthority: enriched.lodgingAuthority,
      publicContactPath: enriched.publicContactPath,
    },
    selectionProcess,
    lodgingEvidenceClass: enriched.lodgingEvidenceClass,
    publicFacts: {
      recommendedListPublished: demand.onRecommendedList != null || selectionHints.recommendedListPublished,
      rateSubmissionsAccepted: selectionHints.rateSubmissionsAccepted,
      selfBooking: selectionProcess.selectionModel === "SELF_BOOKING_RECOMMENDED_LIST",
      secondaryPartnerOpen: demand.secondaryPartnerOpen === true,
      overflowEvidenced: demand.overflowEvidenced === true,
      roomBlockConfirmed: demand.roomBlockConfirmed === true,
    },
  });

  // 6) Evidence request + outreach
  const evidenceRequest = buildEvidenceRequest({
    candidate: enriched,
    primaryBlocker: pillarAudit.FINALIZATION_BLOCKER_PRIMARY,
    selectionProcess,
    lodgingDecision,
    fit,
  });
  const outreach = generateEvidenceOutreachDraft({
    hotelName: hotel.name || hotel.displayName || candidate.hotelName,
    campaignName: candidate.campaignName || candidate.title,
    controllerName: enriched.controllerName,
    contactPath: evidenceRequest.bestContactPath,
    category: evidenceRequest.category,
    languages,
    ask: evidenceRequest.bestAsk,
  });

  // 7) Packet recompute + disposition (no Ready inflation)
  const packet = recomputePacketQuality(enriched, pillarAudit);
  const disposition = computeFinalDisposition({
    candidate: enriched,
    packetQuality: packet.packetQuality,
    fit,
    selectionProcess,
    lodgingDecision,
    pillarAudit,
  });

  const nextBlocker = computeFinalizationNextBlocker({
    pillarAudit,
    evidenceRequest,
    selectionProcess,
    fit,
  });

  const customerCard = buildCustomerFinalizationCard({
    lodgingDecision,
    fit,
    selectionProcess,
    evidenceRequest,
    disposition,
    hotelName: hotel.name || hotel.displayName,
  });

  return {
    ok: true,
    eligible: true,
    ifeVersion: IFE_VERSION,
    opportunityId: candidate.opportunityId || candidate.campaignId,
    hotelId: hotel.hotelId || candidate.hotelId,
    campaignId: candidate.campaignId,
    packetBefore: candidate.packetQuality || candidate.packetQualityHint || null,
    primaryBlockerBefore: candidate.nextBlocker || null,
    targetHotelFit: fit.targetHotelFit,
    fit,
    selectionProcess,
    lodgingDecision,
    controller: controller
      ? {
          demandControllerId: controller.demandControllerId,
          controllerName: controller.controllerName,
          lodgingAuthority: controller.lodgingAuthority,
          publicContactPath: controller.publicContactPath,
        }
      : null,
    pillarAudit,
    evidenceRequest,
    outreach,
    packetAfter: packet.packetQuality,
    packetDetail: packet,
    disposition: disposition.disposition,
    readyAfter: disposition.ready === true,
    watchAfter: disposition.watch === true,
    dispositionDetail: disposition,
    nextBlocker,
    customerCard,
    qualityFlags: {
      cityOnlyFit: false,
      unprovenOverflow: demand.overflowEvidenced !== true && demand.secondaryPartnerOpen !== true,
      historicalAsCurrent: selectionProcess.historicalVsCurrent === "HISTORICAL",
      readyFromOutreachAlone: false,
      speculativeLodging: false,
    },
  };
}

function deriveLodgingClassFromSelection(selectionProcess, demand = {}) {
  if (demand.lodgingEvidenceClass) return demand.lodgingEvidenceClass;
  if (selectionProcess.selectionStatus === "OPEN") return "STRONG_HOTEL_MOTION";
  if (selectionProcess.selectionStatus === "UNDER_REVIEW") {
    return demand.onRecommendedList === true ? "STRONG_HOTEL_MOTION" : "PLAUSIBLE_HOTEL_MOTION";
  }
  if (selectionProcess.selectionStatus === "EXPECTED") return "PLAUSIBLE_HOTEL_MOTION";
  if ((selectionProcess.knownOfficialHotels || []).length) {
    return demand.onRecommendedList === true || demand.secondaryPartnerOpen === true
      ? "DIRECT_LODGING_EVIDENCE"
      : "PLAUSIBLE_HOTEL_MOTION";
  }
  if (selectionProcess.evidenceSource) return "PLAUSIBLE_HOTEL_MOTION";
  return "NONE";
}

/** Downgrade event-level lodging proof when subject is not included / host-locked without secondary */
function subjectLodgingEvidenceClass({ candidate, demand, selectionProcess, fit }) {
  let cls =
    candidate.lodgingEvidenceClass ||
    deriveLodgingClassFromSelection(selectionProcess, demand);
  const hostLocked =
    Boolean(demand.knownCompetitorHost || demand.officialHostHotel) &&
    demand.secondaryPartnerOpen !== true &&
    demand.overflowEvidenced !== true;
  const notOnList =
    demand.selectionModel === "SELF_BOOKING_RECOMMENDED_LIST" ||
    selectionProcess.selectionModel === "SELF_BOOKING_RECOMMENDED_LIST"
      ? demand.onRecommendedList === false
      : false;
  if (hostLocked || notOnList || fit.readyEligibleFit === false) {
    if (cls === "DIRECT_LODGING_EVIDENCE" || cls === "STRONG_HOTEL_MOTION") {
      cls = "PLAUSIBLE_HOTEL_MOTION";
    }
  }
  return cls;
}

export function getFinalizationArchitectureStatus() {
  return {
    FINALIZATION_ENGINE_IMPLEMENTED: true,
    TARGET_HOTEL_FIT_ENGINE_IMPLEMENTED: true,
    HOTEL_SELECTION_PROCESS_MODEL_IMPLEMENTED: true,
    LODGING_DECISION_MODEL_IMPLEMENTED: true,
    CONTROLLER_EVIDENCE_REQUEST_ENGINE_IMPLEMENTED: true,
    OUTREACH_EVIDENCE_GENERATOR_IMPLEMENTED: true,
    HOTEL_SUPPLIED_RESPONSE_REQUALIFICATION_IMPLEMENTED: true,
    FINALIZATION_NEXT_BLOCKER_ENGINE_IMPLEMENTED: true,
    READY_THRESHOLD_CHANGED: false,
    WATCH_THRESHOLD_CHANGED: false,
    APIFY_USED: false,
  };
}
