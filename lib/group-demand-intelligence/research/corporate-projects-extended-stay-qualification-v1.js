/**
 * GDI 2.0 Phase 3B — Corporate Projects & Extended Stay qualification.
 * Composes shared eligibility + demand thesis standard + buyer evidence gate.
 */

import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateBuyerEvidenceGate } from "../buyer-evidence-gate-v1.js";
import { classifyDemandMechanismEvidence } from "../demand-mechanism-research-v1.js";
import {
  classifyCommercialDemandThesis,
  DEMAND_THESIS_STANDARD,
  thesisStandardMayQualifySalesReady,
} from "../commercial-demand-thesis-standard-v1.js";
import { classifyResearchContinuationDisposition } from "../research-continuation-disposition-v1.js";
import { classifyCpeDiscoverySignal } from "./corporate-projects-extended-stay-discovery-v1.js";
import {
  CPE_RESEARCH_SEGMENT,
  CORPORATE_PROJECTS_EXTENDED_STAY_V1,
} from "./corporate-projects-extended-stay-constants-v1.js";

export const CPE_QUALIFICATION_V1 = "gdi-corporate-projects-extended-stay-qualification-v1";

export const CPE_QUALIFICATION_STATUS = Object.freeze({
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
    name: text(opportunity.primaryContact?.name || opportunity.contactName || ""),
    role: text(
      opportunity.primaryContact?.role ||
        opportunity.primaryContact?.title ||
        opportunity.contactRole ||
        "",
    ),
    email: opportunity.primaryContact?.email || opportunity.contactEmail || null,
    organization: opportunity.organizationName || null,
  };
}

export function evaluateCorporateProjectsExtendedStayQualification(
  opportunity = {},
  hotel = {},
  opts = {},
) {
  const discovery = classifyCpeDiscoverySignal({
    headline: opportunity.activityHeadline || opportunity.eventName || opportunity.title,
    description: opportunity.whatIsHappening || opportunity.hotelDemandThesis,
    evidenceQuote: opportunity.evidenceQuote,
    category: opportunity.cpeDemandCategory || opportunity.demandCategory,
    indicators: opportunity.cpeIndicators || [],
    synthetic: opportunity.syntheticFixture === true || opts.synthetic === true,
  });

  if (discovery.status === "REJECT_INSUFFICIENT" || discovery.status === "REJECT_CONTRADICTORY") {
    return {
      version: CPE_QUALIFICATION_V1,
      parentVersion: CORPORATE_PROJECTS_EXTENDED_STAY_V1,
      status: CPE_QUALIFICATION_STATUS.REJECTED_FALSE_POSITIVE,
      qualified: false,
      researchContinuation: false,
      discovery,
      demand: null,
      buyerGate: null,
      shared: null,
      hotelFacingEligibleUnchanged: true,
      note: "Rejected before shared eligibility — insufficient or contradictory project-demand signal.",
    };
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
      researchSegment: opportunity.researchSegment || CPE_RESEARCH_SEGMENT,
    },
    {
      hotel,
      segmentFamily: opts.segmentFamily || "generic",
      hotelCommercialAlignment: true,
      extraction: opts.extraction || {},
    },
  );

  const demandOk =
    thesisStandardMayQualifySalesReady(demandThesis.thesisStandard) ||
    demand.strengthensEligibility === true;

  const shadowSalesReady =
    shared.salesReady === true &&
    buyerGate.buyerEligible === true &&
    demandOk &&
    discovery.status !== "REJECT_INSUFFICIENT" &&
    demandThesis.thesisStandard !== DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND &&
    demandThesis.thesisStandard !== DEMAND_THESIS_STANDARD.CONTRADICTORY_OR_NEGATIVE;

  let status = CPE_QUALIFICATION_STATUS.RESEARCH_CONTINUATION;
  if (opportunity.syntheticFixture === true || opts.synthetic === true) {
    status = CPE_QUALIFICATION_STATUS.SYNTHETIC_FIXTURE_ONLY;
  } else if (shadowSalesReady) {
    status = CPE_QUALIFICATION_STATUS.SHADOW_SALES_READY;
  }

  const continuation = classifyResearchContinuationDisposition({
    opportunity,
    lodgingEvidenceIncomplete: !demandOk,
    contactNotYetVerified: buyerGate.buyerEligible !== true,
  });

  return {
    version: CPE_QUALIFICATION_V1,
    parentVersion: CORPORATE_PROJECTS_EXTENDED_STAY_V1,
    status,
    qualified: shadowSalesReady && status === CPE_QUALIFICATION_STATUS.SHADOW_SALES_READY,
    researchContinuation: status === CPE_QUALIFICATION_STATUS.RESEARCH_CONTINUATION,
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
