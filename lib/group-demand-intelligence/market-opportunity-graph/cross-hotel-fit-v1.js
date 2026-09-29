/**
 * Cross-hotel fit evaluation for a shared market opportunity (shadow).
 * Reuses computeHotelFitScore weights; does not invent pricing.
 */

import { computeHotelFitScore } from "../scoring.js";
import { GEO_APPLICABILITY } from "./market-geography-v1.js";
import { applicabilityIsCandidate } from "./geographic-applicability-v1.js";
import { HOTEL_OPP_FINAL_STATE } from "./market-opportunity-packet-v1.js";
import { TERRITORY_ROLE } from "../demand-territory.js";

/**
 * Score hotel fit for market packet against hotel profile.
 */
export function evaluateCrossHotelFit({
  marketPacket,
  hotelProfile,
  geographicApplicability,
  seedOpp = null,
} = {}) {
  const reasons = [];
  const missingData = [];
  const app = geographicApplicability?.applicability;

  if (!applicabilityIsCandidate(app) && app !== GEO_APPLICABILITY.WEAK) {
    return {
      hotelFitScore: null,
      components: null,
      reasons: geographicApplicability?.reasons || ["Not geographically applicable"],
      missingData: [],
      finalState:
        app === GEO_APPLICABILITY.NONE
          ? HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE
          : HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE,
      productConstraint: null,
    };
  }

  // Geography component from applicability
  const geographyFit =
    app === GEO_APPLICABILITY.DIRECT
      ? 90
      : app === GEO_APPLICABILITY.STRONG
        ? 75
        : app === GEO_APPLICABILITY.PLAUSIBLE
          ? 55
          : app === GEO_APPLICABILITY.WEAK
            ? 35
            : 20;

  // Physical / room-scale
  const peak =
    marketPacket?.roomDemand?.estimatedPeakRooms ??
    seedOpp?.estimatedPeakRooms ??
    seedOpp?.peakRooms ??
    null;
  const rooms = hotelProfile?.rooms;
  const peakMin = hotelProfile?.peakRoomsMin ?? 0;
  const peakMax = hotelProfile?.peakRoomsMax ?? rooms ?? 999;
  let physicalFit = 60;
  let productConstraint = null;

  if (peak != null && rooms != null) {
    if (peak > rooms * 0.9) {
      physicalFit = 25;
      reasons.push(`Peak rooms ${peak} near/over hotel inventory ${rooms}`);
      productConstraint = "ROOM_SCALE_OVER";
    } else if (peak >= peakMin && peak <= peakMax) {
      physicalFit = 85;
      reasons.push(`Peak ${peak} within hotel band ${peakMin}-${peakMax}`);
    } else if (peak < peakMin) {
      physicalFit = hotelProfile?.config?.commercialPriorities
        ?.allowSmallerIfCommerciallyMeaningful
        ? 65
        : 40;
      reasons.push(`Peak ${peak} below core band (min ${peakMin})`);
    } else {
      physicalFit = 45;
      reasons.push(`Peak ${peak} above core band (max ${peakMax})`);
    }
  } else {
    missingData.push("room_demand_peak");
    physicalFit = 50;
    reasons.push("Peak rooms unknown — physical fit provisional");
  }

  // Meeting/event fit — Hilton has very limited formal meeting space
  const meetingSqFt = hotelProfile?.meetingSqFt ?? 0;
  const needsMeeting =
    /\b(meeting|conference|symposium|convention|ballroom|plenary)\b/i.test(
      `${marketPacket?.title || ""} ${marketPacket?.summaryWhat || ""}`
    ) && !/\b(overflow|housing|room block|exhibitor)\b/i.test(
      `${marketPacket?.title || ""} ${seedOpp?.opportunityType || ""}`
    );
  if (needsMeeting && meetingSqFt > 0 && meetingSqFt < 1000) {
    physicalFit = Math.min(physicalFit, 40);
    productConstraint = productConstraint || "LIMITED_MEETING_SPACE";
    reasons.push(
      `Limited formal meeting space (${meetingSqFt} sq ft) vs meeting-led demand`
    );
  }

  // Overflow / housing motion favors room-scale hotels even with limited meetings
  const isOverflow =
    /overflow|housing|room block|vip housing/i.test(
      `${marketPacket?.title || ""} ${seedOpp?.opportunityType || ""}`
    ) ||
    Boolean(
      marketPacket?.lodgingEvidence?.overflowMentioned ||
        marketPacket?.lodgingEvidence?.roomBlockMentioned
    );
  if (isOverflow && rooms >= 200) {
    physicalFit = Math.max(physicalFit, 70);
    reasons.push("Overflow/housing motion aligns with room-block hotel");
  }

  const timing = seedOpp?.bookingWindowStatus
    ? seedOpp.bookingWindowStatus === "CLOSED"
      ? 20
      : 75
    : 60;
  if (seedOpp?.customerFacingState === "CLOSED" || seedOpp?.priority === "DISQUALIFIED") {
    return {
      hotelFitScore: 0,
      components: null,
      reasons: ["Seed commercial status closed/DQ"],
      missingData,
      finalState: HOTEL_OPP_FINAL_STATE.CLOSED,
      productConstraint,
    };
  }

  const commercialValue =
    seedOpp?.evidenceConfidence != null
      ? Math.min(90, 40 + Number(seedOpp.evidenceConfidence || 0) * 0.5)
      : 55;

  const contactability = marketPacket?.who?.primaryContact?.name ? 70 : 40;
  if (!marketPacket?.who?.primaryContact?.name) missingData.push("who_contact");

  const scored = computeHotelFitScore({
    physicalFit,
    geographyFit,
    timing,
    commercialValue,
    historicalFit: 50,
    competitiveAccessibility:
      geographicApplicability?.territoryRole === TERRITORY_ROLE.CORE ? 75 : 55,
    contactability,
  });

  // Lodging evidence — accept seed lodging fields or lodging-led commercial motion
  const lodging =
    marketPacket?.lodgingEvidence ||
    seedOpp?.lodgingEvidence ||
    seedOpp?.lodging ||
    null;
  const lodgingLed =
    /overflow|housing|room block|vip housing|exhibitor/i.test(
      `${marketPacket?.title || ""} ${seedOpp?.opportunityType || ""} ${marketPacket?.summaryWhat || ""}`
    ) || Boolean(lodging);
  if (!lodgingLed) missingData.push("lodging_evidence");
  if (!marketPacket?.officialSource && !(marketPacket?.sources || []).length && !seedOpp?.officialSource) {
    missingData.push("source_url");
  }

  let finalState = HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA;
  if (app === GEO_APPLICABILITY.WEAK && scored.hotelFitScore < 50) {
    finalState = HOTEL_OPP_FINAL_STATE.NOT_FIT;
    reasons.push("Weak geography + low fit");
  } else if (productConstraint === "ROOM_SCALE_OVER" && scored.hotelFitScore < 45) {
    finalState = HOTEL_OPP_FINAL_STATE.NOT_FIT;
  } else if (
    scored.hotelFitScore >= 60 &&
    missingData.length === 0 &&
    applicabilityIsCandidate(app)
  ) {
    // Shadow "would be ready" — does NOT write customer row
    finalState = HOTEL_OPP_FINAL_STATE.CUSTOMER_READY;
    reasons.push("Shadow: would meet fit+geo+evidence bar (not promoted)");
  } else if (scored.hotelFitScore >= 50 && applicabilityIsCandidate(app)) {
    finalState = HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA;
  } else if (!applicabilityIsCandidate(app)) {
    finalState = HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE;
  } else {
    finalState = HOTEL_OPP_FINAL_STATE.NOT_FIT;
  }

  return {
    hotelFitScore: scored.hotelFitScore,
    components: scored.components,
    reasons,
    missingData: [...new Set(missingData)],
    finalState,
    productConstraint,
  };
}
