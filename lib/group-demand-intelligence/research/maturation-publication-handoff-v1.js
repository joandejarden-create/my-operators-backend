/**
 * Advisory supervisor PUBLISHABLE → existing eligibility / publication path.
 *
 * Does NOT invent hotel-facing status. Uses evaluateProspectPublicationEligibility
 * and authorizeProspectPublicationPromotion with explicit authorizePublication.
 */

import {
  evaluateProspectPublicationEligibility,
  authorizeProspectPublicationPromotion,
} from "../prospect-publication-eligibility-v1.js";
import { RESEARCH_TERMINAL } from "./research-constants-v1.js";

/**
 * Map supervisor matured state onto opportunity fields eligibility expects.
 */
export function mapMaturedToPublicationCandidate(matured = {}, prior = {}) {
  const contact = {
    ...(prior.contact || {}),
    ...(matured.contact || {}),
  };
  const businessEmail =
    contact.businessEmail ||
    matured.contactEmail ||
    prior.contactEmail ||
    null;
  const timing =
    matured.timing ||
    prior.timing ||
    matured.futureDemand?.dateWindow ||
    prior.futureDemand?.dateWindow ||
    matured.eventStartDate ||
    prior.eventStartDate ||
    null;
  return {
    ...prior,
    ...matured,
    organizationName:
      matured.organizationName ||
      prior.organizationName ||
      matured.company ||
      prior.company,
    title: matured.title || prior.title || matured.organizationName,
    timing,
    eventStartDate:
      matured.eventStartDate || prior.eventStartDate || timing || null,
    contactEmail: businessEmail,
    contactPhone:
      contact.businessPhone || matured.contactPhone || prior.contactPhone || null,
    contactUrl:
      contact.contactFormUrl || matured.contactUrl || prior.contactUrl || null,
    contact,
    hotelFitRationale:
      matured.hotelFitRationale ||
      prior.hotelFitRationale ||
      matured.summaryWhyHotel ||
      prior.summaryWhyHotel ||
      null,
    proposedHotelUse:
      matured.proposedHotelUse || prior.proposedHotelUse || null,
    localConnection: matured.localConnection || prior.localConnection || null,
    sources: matured.sources || prior.sources || [],
    evidence: matured.evidence || prior.evidence || [],
    futureDemand: matured.futureDemand || prior.futureDemand,
    demandScale: matured.demandScale || prior.demandScale,
    hotelFit: matured.hotelFit || prior.hotelFit,
    // Preserve already-published facing; never invent customerVisible here
    customerVisible: prior.customerVisible === true ? true : matured.customerVisible,
    prospectHotelFacing:
      prior.prospectHotelFacing === true ? true : matured.prospectHotelFacing,
    prospectPublicationBasis: prior.prospectPublicationBasis || null,
  };
}

/**
 * Assess + optionally promote. Never sets customerVisible without authorizePublication.
 *
 * @returns {{
 *   advisoryPublishable: boolean,
 *   eligibility: object,
 *   handoffStatus: string,
 *   blockingGate: string|null,
 *   patched: object,
 *   event: object|null,
 *   wouldPublish: boolean
 * }}
 */
export function handoffMaturedOpportunityToPublication({
  matured,
  prior = {},
  authorizePublication = false,
  authorizedBy = "gdi_hotel_research_orchestrator_v1",
  reason = "supervisor_advisory_publishable_handoff",
} = {}) {
  const mapped = mapMaturedToPublicationCandidate(matured, prior);
  const advisoryPublishable =
    matured.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT;

  const eligibility = evaluateProspectPublicationEligibility(mapped);

  if (!advisoryPublishable) {
    return {
      advisoryPublishable: false,
      eligibility,
      handoffStatus: "NOT_ADVISORY_PUBLISHABLE",
      blockingGate: "supervisor_not_publishable",
      patched: mapped,
      event: null,
      wouldPublish: false,
    };
  }

  if (!eligibility.hotelFacing) {
    return {
      advisoryPublishable: true,
      eligibility,
      handoffStatus: "ELIGIBILITY_BLOCKED",
      blockingGate: `eligibility:${(eligibility.reasons || []).join(",") || "not_hotel_facing"}`,
      patched: mapped,
      event: null,
      wouldPublish: false,
      nextTask: eligibility.nextTask || null,
    };
  }

  if (!authorizePublication) {
    return {
      advisoryPublishable: true,
      eligibility,
      handoffStatus: "ELIGIBLE_AWAITING_AUTHORIZE",
      blockingGate: "authorizePublication_required",
      patched: {
        ...mapped,
        prospectPublicationEligibility: eligibility.eligibility,
        _handoffNote:
          "Passes hotel-facing eligibility; awaiting explicit authorizePublication",
      },
      event: null,
      wouldPublish: true,
    };
  }

  const promo = authorizeProspectPublicationPromotion(mapped, {
    authorizePublication: true,
    authorizedBy,
    reason,
  });

  if (!promo.ok) {
    return {
      advisoryPublishable: true,
      eligibility: promo.eligibility || eligibility,
      handoffStatus: "PROMOTION_REFUSED",
      blockingGate: promo.reason || "promotion_refused",
      patched: mapped,
      event: null,
      wouldPublish: false,
    };
  }

  return {
    advisoryPublishable: true,
    eligibility: promo.eligibility || eligibility,
    handoffStatus: promo.idempotent
      ? "ALREADY_PUBLISHED"
      : "PROMOTED_HOTEL_FACING",
    blockingGate: null,
    patched: promo.patched,
    event: promo.event,
    wouldPublish: true,
  };
}
