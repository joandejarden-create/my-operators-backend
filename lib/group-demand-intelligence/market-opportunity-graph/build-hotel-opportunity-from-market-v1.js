/**
 * Build a hotel-specific opportunity from a shared market packet (governed apply).
 * Does not duplicate market identity; stamps marketOpportunityId + source links.
 */

import { createHash } from "node:crypto";
import { enrichGdiOpportunityForCustomer } from "../enrich-gdi-opportunity-for-customer.js";
import { applyGdiSummaryEnrichment } from "../opportunity-summary-v1.js";
import { applyGdiWhoHowResolution, WHO_PATH_CLASS } from "../opportunity-who-resolution-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { classifyDemandTerritoryFitFromConfig } from "../demand-territory.js";
import { evaluateHotelGeographicApplicability } from "./geographic-applicability-v1.js";
import { evaluateCrossHotelFit } from "./cross-hotel-fit-v1.js";
import { decideSupportingDataNextAction } from "./jev-supporting-data-router-v1.js";
import { applicabilityIsCandidate } from "./geographic-applicability-v1.js";
import { GEO_APPLICABILITY } from "./market-geography-v1.js";

export function computeMarketLinkedHotelOpportunityId(hotelId, marketOpportunityId) {
  const seed = `mktlink|${String(hotelId || "").trim()}|${String(marketOpportunityId || "").trim()}`;
  return `gdi_opp_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * @param {object} opts
 * @param {object} opts.marketPacket - hotel-neutral packet
 * @param {object} opts.seedOpp - source hotel opportunity (e.g. Renaissance)
 * @param {object} opts.targetHotelProfile - from buildHotelGeographyProfile
 * @param {string} opts.nowDate
 * @param {boolean} [opts.requireStrictReady=true]
 */
export function buildHotelOpportunityFromMarketPacket(opts = {}) {
  const {
    marketPacket,
    seedOpp,
    targetHotelProfile,
    nowDate = new Date().toISOString().slice(0, 10),
    requireStrictReady = true,
  } = opts;

  const hotelId = targetHotelProfile?.hotelId;
  const marketOpportunityId = marketPacket?.marketOpportunityId;
  if (!hotelId || !marketOpportunityId) {
    return {
      ok: false,
      reason: "missing_hotel_or_market_id",
      opportunity: null,
    };
  }

  const geoApp = evaluateHotelGeographicApplicability(seedOpp || marketPacket, targetHotelProfile);
  if (!applicabilityIsCandidate(geoApp.applicability)) {
    return {
      ok: false,
      reason: "geographic_not_applicable",
      geographicApplicability: geoApp,
      opportunity: null,
      finalState: "NOT_APPLICABLE",
    };
  }

  // Commercial openness — shared market fact (not hotel-specific).
  // FULLY_PLACED + explicit no-third-party housing = not winnable for any hotel.
  const commercialBlob = [
    seedOpp?.venueStatus,
    seedOpp?.hotelOpportunityThesis,
    seedOpp?.summaryWhyHotel,
    seedOpp?.summaryWhyMatters,
    seedOpp?.whyNow,
    marketPacket?.venueStatus,
  ]
    .map((x) => String(x || ""))
    .join(" ");
  const lodgingCommerciallyClosed =
    /^FULLY_PLACED$/i.test(String(seedOpp?.venueStatus || marketPacket?.venueStatus || "")) &&
    /does not use third-party|single-hotel arrangement|keeps booking within|eliminate superficially/i.test(
      commercialBlob
    );
  if (lodgingCommerciallyClosed) {
    return {
      ok: false,
      reason: "commercial_lodging_closed",
      geographicApplicability: geoApp,
      opportunity: null,
      finalState: "NOT_FIT",
      commercialStatus: "FULLY_PLACED_NO_THIRD_PARTY",
    };
  }

  // Minimum threshold for live apply: DIRECT or STRONG preferred; PLAUSIBLE needs strong fit
  const fit = evaluateCrossHotelFit({
    marketPacket,
    hotelProfile: targetHotelProfile,
    geographicApplicability: geoApp,
    seedOpp,
  });

  if (
    geoApp.applicability === GEO_APPLICABILITY.PLAUSIBLE &&
    (fit.hotelFitScore == null || fit.hotelFitScore < 58)
  ) {
    return {
      ok: false,
      reason: "plausible_geo_weak_fit",
      geographicApplicability: geoApp,
      fit,
      opportunity: null,
      finalState: fit.finalState || "NOT_FIT",
    };
  }

  const hotelOppId = computeMarketLinkedHotelOpportunityId(hotelId, marketOpportunityId);

  // Shared evidence layer (copied once from market packet / seed — not Ren-specific thesis)
  const shared = {
    title: marketPacket.title || seedOpp?.title || null,
    organizationName: marketPacket.organizationName || seedOpp?.organizationName || null,
    eventName: marketPacket.eventName || seedOpp?.eventName || null,
    eventStartDate: marketPacket.eventStartDate || seedOpp?.eventStartDate || null,
    eventEndDate: marketPacket.eventEndDate || seedOpp?.eventEndDate || null,
    eventYear: marketPacket.eventYear || seedOpp?.eventYear || null,
    venueStatus: marketPacket.venueStatus || seedOpp?.venueStatus || null,
    destinationStatus: marketPacket.destinationStatus || seedOpp?.destinationStatus || null,
    officialSource: marketPacket.officialSource || seedOpp?.officialSource || null,
    discoverySource: marketPacket.discoverySource || seedOpp?.discoverySource || null,
    sources: marketPacket.sources?.length ? marketPacket.sources : seedOpp?.sources || [],
    lodgingEvidence:
      marketPacket.lodgingEvidence ||
      seedOpp?.lodgingEvidence ||
      seedOpp?.lodging ||
      null,
    eventSeriesId: marketPacket.eventSeriesId || seedOpp?.eventSeriesId || null,
    eventCycleId: marketPacket.eventCycleId || seedOpp?.eventCycleId || null,
    opportunityType: seedOpp?.opportunityType || null,
    segment: seedOpp?.segment || null,
    estimatedPeakRooms:
      marketPacket.roomDemand?.estimatedPeakRooms ??
      seedOpp?.estimatedPeakRooms ??
      seedOpp?.peakRooms ??
      null,
    estimatedAttendance:
      marketPacket.roomDemand?.estimatedAttendance ??
      seedOpp?.estimatedAttendance ??
      null,
    roomDemandStatus:
      marketPacket.roomDemand?.roomDemandStatus || seedOpp?.roomDemandStatus || null,
    primaryContact: marketPacket.who?.primaryContact || seedOpp?.primaryContact || null,
    contactPathClass: marketPacket.who?.contactPathClass || seedOpp?.contactPathClass || null,
    contactResearchState:
      marketPacket.who?.contactResearchState || seedOpp?.contactResearchState || null,
    evidenceConfidence: seedOpp?.evidenceConfidence ?? null,
  };

  // Strip Ren-locked territory so target hotel reclassifies
  let draft = {
    ...shared,
    id: hotelOppId,
    opportunityId: hotelOppId,
    hotelId,
    marketOpportunityId,
    sourceMarketHotelId: marketPacket.sourceHotelId || seedOpp?.hotelId || null,
    sourceHotelOpportunityId:
      marketPacket.sourceHotelOpportunityId || seedOpp?.id || seedOpp?.opportunityId || null,
    sharedEvidenceRef: {
      marketOpportunityId,
      sourceHotelOpportunityId:
        marketPacket.sourceHotelOpportunityId || seedOpp?.id || null,
    },
    geographicApplicability: geoApp.applicability,
    geographicApplicabilityReasons: geoApp.reasons,
    demandTerritoryFitLocked: false,
    demandTerritoryFit: undefined,
    demandTerritoryRationale: undefined,
    // Do not inherit Ren priority / why-hotel / action
    priority: undefined,
    summaryWhyHotel: undefined,
    fitExplanation: undefined,
    whyNow: undefined,
    recommendedAction: undefined,
    recommendedNextStep: undefined,
    hotelFitScore: undefined,
    summaryWhat: undefined,
    customerFacingState: seedOpp?.customerFacingState || "ACTIVE",
    governedApplyV1: {
      appliedAt: new Date().toISOString(),
      cohort: "gdi_market_first_governed_apply_v1",
      fromMarketOpportunityId: marketOpportunityId,
    },
  };

  // Territory for target hotel
  const territory = classifyDemandTerritoryFitFromConfig(draft, targetHotelProfile.config);
  if (territory) {
    draft.demandTerritoryFit = territory.demandTerritoryFit;
    draft.demandTerritoryRationale = territory.demandTerritoryRationale;
  }

  // Enrich for target hotel (hotel-specific why / whyNow / action / summary)
  draft = enrichGdiOpportunityForCustomer(draft, {
    hotelId,
    hotelConfig: targetHotelProfile.config,
    nowDate,
    skipReadinessHold: true,
  });

  // Preserve shared market commercial type / lodging / lifecycle from seed.
  // enrichGdiOpportunityForCustomer can rewrite type to FUTURE_WATCH and drop lodging.
  if (shared.opportunityType) draft.opportunityType = shared.opportunityType;
  if (shared.lodgingEvidence) {
    draft.lodgingEvidence = shared.lodgingEvidence;
    draft.lodging = shared.lodgingEvidence;
  } else if (
    /OVERFLOW|HOUSING|ROOM_BLOCK/i.test(String(shared.opportunityType || ""))
  ) {
    draft.lodgingEvidence = {
      status: "EVENT_TYPE_OVERFLOW_HOUSING",
      overflowMentioned: /OVERFLOW/i.test(String(shared.opportunityType || "")),
      roomBlockMentioned: true,
    };
  }
  if (seedOpp?.customerFacingState) {
    draft.customerFacingState = seedOpp.customerFacingState;
  }
  if (seedOpp?.teamSupported != null) draft.teamSupported = seedOpp.teamSupported;
  if (seedOpp?.teamEvidence) draft.teamEvidence = seedOpp.teamEvidence;
  // Re-stamp market-first identity (enrich/factory may drop unknown fields)
  draft.id = hotelOppId;
  draft.opportunityId = hotelOppId;
  draft.hotelId = hotelId;
  draft.marketOpportunityId = marketOpportunityId;
  draft.sourceMarketHotelId =
    marketPacket.sourceHotelId || seedOpp?.hotelId || null;
  draft.sourceHotelOpportunityId =
    marketPacket.sourceHotelOpportunityId || seedOpp?.id || seedOpp?.opportunityId || null;
  draft.sharedEvidenceRef = {
    marketOpportunityId,
    sourceHotelOpportunityId:
      marketPacket.sourceHotelOpportunityId || seedOpp?.id || null,
  };
  draft.geographicApplicability = geoApp.applicability;
  draft.geographicApplicabilityReasons = geoApp.reasons;
  draft.governedApplyV1 = {
    appliedAt: new Date().toISOString(),
    cohort: "gdi_market_first_governed_apply_v1",
    fromMarketOpportunityId: marketOpportunityId,
  };
  // Exhibitor-style surface DQ: stamp team proof when seed was already surface-active
  if (
    /symposium|conference|meeting|forum/i.test(String(draft.title || "")) &&
    draft.teamSupported == null &&
    !draft.teamEvidence
  ) {
    draft.teamSupported = true;
    draft.teamEvidence = {
      teamSupported: true,
      source: "market_seed_surface_active",
      note: "Inherited from market-ready seed; multi-person event travel assumed for lodging motion",
    };
  }

  // Force hotel-specific summary rebuild (do not keep Ren wording)
  const sumPkt = applyGdiSummaryEnrichment(draft, { force: true });
  draft = sumPkt.opportunity;

  // WHO stamp if needed
  let whoPkt = applyGdiWhoHowResolution(draft);
  draft = whoPkt.opportunity;
  if (whoPkt.classified.pathClass === WHO_PATH_CLASS.NOT_RESEARCHED) {
    whoPkt = applyGdiWhoHowResolution(draft, {
      markAttempted: true,
      ceilingReason: "PUBLIC_DATA_CEILING",
    });
    draft = whoPkt.opportunity;
  }

  // Prefer cross-hotel fit score when enrich left weak
  if (fit.hotelFitScore != null) {
    draft.hotelFitScore = fit.hotelFitScore;
    draft.fitComponents = fit.components || draft.fitComponents;
  }

  const ready = isGdiCustomerOpportunityReady(draft, { nowDate });
  draft.customerReadiness = ready;

  const jev = decideSupportingDataNextAction({
    marketPacket,
    geographicApplicability: geoApp,
    hotelFit: { ...fit, missingData: ready.ok ? [] : ready.failed },
  });

  if (requireStrictReady && !ready.ok) {
    return {
      ok: false,
      reason: "strict_readiness_failed",
      failed: ready.failed,
      state: ready.state,
      geographicApplicability: geoApp,
      fit,
      jev,
      opportunity: draft,
      finalState: "NEEDS_MORE_DATA",
    };
  }

  return {
    ok: true,
    reason: "passed",
    geographicApplicability: geoApp,
    fit,
    jev,
    opportunity: draft,
    finalState: "CUSTOMER_READY",
    readiness: ready,
  };
}
