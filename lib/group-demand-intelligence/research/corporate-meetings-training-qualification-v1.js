/**
 * GDI 2.0 Phase 3A — Corporate Meetings & Training qualification.
 *
 * Composes shared commercial eligibility, demand-mechanism A–D, and buyer-evidence gate.
 * Does not mutate hotelFacingEligible or customer visibility.
 */

import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateBuyerEvidenceGate } from "../buyer-evidence-gate-v1.js";
import {
  classifyDemandMechanismEvidence,
  DEMAND_EVIDENCE_CLASS,
} from "../demand-mechanism-research-v1.js";
import {
  classifyCommercialDemandThesis,
  DEMAND_THESIS_STANDARD,
  thesisStandardMayQualifySalesReady,
} from "../commercial-demand-thesis-standard-v1.js";
import { classifyResearchContinuationDisposition } from "../research-continuation-disposition-v1.js";
import { classifyCmtDiscoverySignal } from "./corporate-meetings-training-discovery-v1.js";
import {
  CMT_RESEARCH_SEGMENT,
  CORPORATE_MEETINGS_TRAINING_V1,
} from "./corporate-meetings-training-constants-v1.js";

export const CMT_QUALIFICATION_V1 = "gdi-corporate-meetings-training-qualification-v1";

export const CMT_QUALIFICATION_STATUS = Object.freeze({
  SHADOW_SALES_READY: "SHADOW_SALES_READY",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  REJECTED_FALSE_POSITIVE: "REJECTED_FALSE_POSITIVE",
  SYNTHETIC_FIXTURE_ONLY: "SYNTHETIC_FIXTURE_ONLY",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function extractPerson(opportunity = {}) {
  return {
    name:
      text(opportunity.primaryContact?.name) ||
      text(opportunity.contactName) ||
      "",
    role:
      text(opportunity.primaryContact?.role) ||
      text(opportunity.primaryContact?.title) ||
      text(opportunity.contactRole) ||
      "",
    email: opportunity.primaryContact?.email || opportunity.contactEmail || null,
    organization: opportunity.organizationName || null,
  };
}

/**
 * Evaluate a CMT opportunity candidate (shadow — no visibility writes).
 */
export function evaluateCorporateMeetingsTrainingQualification(opportunity = {}, hotel = {}, opts = {}) {
  const discovery = classifyCmtDiscoverySignal({
    headline: opportunity.activityHeadline || opportunity.eventName || opportunity.title,
    description: opportunity.whatIsHappening || opportunity.hotelDemandThesis,
    evidenceQuote: opportunity.evidenceQuote,
    category: opportunity.cmtDemandCategory || opportunity.demandCategory,
    indicators: opportunity.cmtIndicators || [],
    synthetic: opportunity.syntheticFixture === true || opts.synthetic === true,
  });

  if (discovery.status === "REJECT_INSUFFICIENT" || discovery.status === "REJECT_CONTRADICTORY") {
    return {
      version: CMT_QUALIFICATION_V1,
      parentVersion: CORPORATE_MEETINGS_TRAINING_V1,
      status: CMT_QUALIFICATION_STATUS.REJECTED_FALSE_POSITIVE,
      qualified: false,
      researchContinuation: false,
      discovery,
      demand: null,
      buyerGate: null,
      shared: null,
      hotelFacingEligibleUnchanged: true,
      note: "Rejected before shared eligibility — insufficient or contradictory demand signal.",
    };
  }

  if (opportunity.syntheticFixture === true || opts.synthetic === true) {
    // Still evaluate quality, but flag so pilot cannot count as real discovery
  }

  const demand = classifyDemandMechanismEvidence(opportunity, opts.extraction || {});
  const demandThesis = classifyCommercialDemandThesis(opportunity, opts.extraction || {});
  const person = extractPerson(opportunity);
  const buyerGate = evaluateBuyerEvidenceGate(person, opportunity, {
    contactDataOrigin: opportunity.contactDataOrigin || "PUBLIC_SOURCE",
    affiliationVerified: Boolean(opportunity.organizationName),
  });

  const shared = evaluateSharedCommercialEligibility(
    {
      ...opportunity,
      researchSegment: opportunity.researchSegment || CMT_RESEARCH_SEGMENT,
    },
    {
      hotel,
      segmentFamily: opts.segmentFamily || "generic",
      hotelCommercialAlignment: true,
      extraction: opts.extraction || {},
    },
  );

  // CONFIRMED (A) or RELIABLE_INFERRED (B) may qualify — not SPECULATIVE (C).
  // Does not require confirmed room nights, group blocks, RFP, or booking announcement.
  const demandOk =
    thesisStandardMayQualifySalesReady(demandThesis.thesisStandard) ||
    demand.evidenceClass === DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT ||
    demand.evidenceClass === DEMAND_EVIDENCE_CLASS.B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND;

  const shadowSalesReady =
    shared.salesReady === true &&
    buyerGate.buyerEligible === true &&
    demandOk &&
    discovery.status !== "REJECT_INSUFFICIENT" &&
    demandThesis.thesisStandard !== DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND &&
    demandThesis.thesisStandard !== DEMAND_THESIS_STANDARD.CONTRADICTORY_OR_NEGATIVE;

  let status = CMT_QUALIFICATION_STATUS.RESEARCH_CONTINUATION;
  if (opportunity.syntheticFixture === true || opts.synthetic === true) {
    status = shadowSalesReady
      ? CMT_QUALIFICATION_STATUS.SYNTHETIC_FIXTURE_ONLY
      : CMT_QUALIFICATION_STATUS.SYNTHETIC_FIXTURE_ONLY;
  } else if (shadowSalesReady) {
    status = CMT_QUALIFICATION_STATUS.SHADOW_SALES_READY;
  }

  const continuation = classifyResearchContinuationDisposition({
    opportunity,
    lodgingEvidenceIncomplete: !demandOk,
    contactNotYetVerified: buyerGate.buyerEligible !== true,
  });

  return {
    version: CMT_QUALIFICATION_V1,
    parentVersion: CORPORATE_MEETINGS_TRAINING_V1,
    status,
    qualified: shadowSalesReady && status === CMT_QUALIFICATION_STATUS.SHADOW_SALES_READY,
    researchContinuation: status === CMT_QUALIFICATION_STATUS.RESEARCH_CONTINUATION,
    syntheticFixture: opportunity.syntheticFixture === true || opts.synthetic === true,
    discovery,
    demand: {
      evidenceClass: demand.evidenceClass,
      thesisStandard: demandThesis.thesisStandard,
      reason: demand.reason,
      mechanismsDetected: demand.mechanismsDetected,
      strengthensEligibility: demand.strengthensEligibility,
      mayQualifySalesReadyDemandGate: demandThesis.mayQualifySalesReadyDemandGate,
      notRequired: demandThesis.notRequired,
    },
    buyerGate: {
      buyerEligible: buyerGate.buyerEligible,
      completedStages: buyerGate.completedStages,
      notes: buyerGate.notes,
      person: buyerGate.person,
      contactEligibilityClass: buyerGate.contactEligibilityClass || null,
      contactTypeForBrief: buyerGate.contactTypeForBrief || null,
      isDirectBuyer: buyerGate.isDirectBuyer === true,
      isRelevantCommercialEntryPoint:
        buyerGate.isRelevantCommercialEntryPoint === true,
      continueResearchForDirectBuyer:
        buyerGate.continueResearchForDirectBuyer === true,
    },
    shared: {
      status: shared.status,
      salesReady: shared.salesReady,
      gaps: shared.gaps || [],
    },
    continuationDisposition: {
      disposition: continuation.disposition,
      commerciallyExhausted: continuation.commerciallyExhausted === true,
    },
    hotelFacingEligibleUnchanged: true,
    customerVisibilityUnchanged: true,
    doesNotAuthorizePublication: true,
  };
}
