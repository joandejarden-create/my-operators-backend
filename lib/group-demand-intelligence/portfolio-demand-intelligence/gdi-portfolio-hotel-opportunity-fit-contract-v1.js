/**
 * GDI Portfolio Demand Intelligence — hotel opportunity fit contract V1 (PDI-1).
 *
 * Maps existing evaluateCrossHotelFit / market packets to founder fit classes.
 * Does not introduce new production scoring weights.
 * UNKNOWN attributes must not become positive suitability claims.
 *
 * Layers (must remain distinct):
 *   A. Shared public-market intelligence
 *   B. Operator portfolio membership
 *   C. Hotel-specific qualification
 *   D. Confidential operator/hotel information (never in shared market packet)
 */

import {
  extractMarketOpportunityPacket,
  HOTEL_OPP_FINAL_STATE,
} from "../market-opportunity-graph/market-opportunity-packet-v1.js";
import { evaluateCrossHotelFit } from "../market-opportunity-graph/cross-hotel-fit-v1.js";
import { evaluateHotelGeographicApplicability } from "../market-opportunity-graph/geographic-applicability-v1.js";
import { buildHotelGeographyProfile } from "../market-opportunity-graph/hotel-geography-profile-v1.js";
import {
  HOTEL_PRIVATE_PAYLOAD_KEYS,
  stripHotelPrivateKeys,
} from "../workflow/gdi-commercial-payload-durability-v1.js";
import { SALES_OWNED_PAYLOAD_KEYS } from "../workflow/gdi-airtable-field-ownership-v1.js";
import { computeMarketLinkedHotelOpportunityId } from "../market-opportunity-graph/build-hotel-opportunity-from-market-v1.js";

export const GDI_PORTFOLIO_HOTEL_OPPORTUNITY_FIT_CONTRACT_V1 =
  "gdi-portfolio-hotel-opportunity-fit-contract-v1";

export const PORTFOLIO_HOTEL_FIT_CLASS = Object.freeze({
  POTENTIAL_PRIMARY_HOST: "POTENTIAL_PRIMARY_HOST",
  OVERFLOW_ACCOMMODATION: "OVERFLOW_ACCOMMODATION",
  PREFERRED_LODGING_OPPORTUNITY: "PREFERRED_LODGING_OPPORTUNITY",
  MEETING_CATERING_OPPORTUNITY: "MEETING_CATERING_OPPORTUNITY",
  OTHER_PARTNERSHIP: "OTHER_PARTNERSHIP",
  UNSUITABLE: "UNSUITABLE",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

export const FIT_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  UNKNOWN: "UNKNOWN",
});

const CONFIDENTIAL_STRIP_KEYS = Object.freeze([
  ...SALES_OWNED_PAYLOAD_KEYS,
  ...HOTEL_PRIVATE_PAYLOAD_KEYS,
  "hotelSalesFeedback",
  "salesOwner",
  "assignedSalesperson",
  "operatorPrivateNotes",
  "pursuitHistory",
  "accountActivity",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function toPattern(p) {
  if (p instanceof RegExp) return p;
  const s = text(p);
  if (!s) return null;
  try {
    return new RegExp(s, "i");
  } catch {
    return new RegExp(
      s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
      "i",
    );
  }
}

/**
 * JSON fixtures store geography patterns as strings; MOG expects RegExp.
 */
export function hydrateMarketHints(hints = {}) {
  if (!hints || typeof hints !== "object") return {};
  const mapEntries = (arr) =>
    (Array.isArray(arr) ? arr : [])
      .map((e) => {
        if (!e || typeof e !== "object") return null;
        const patterns = (e.patterns || []).map(toPattern).filter(Boolean);
        if (!patterns.length) return null;
        return { ...e, patterns };
      })
      .filter(Boolean);
  return {
    ...hints,
    metroPatterns: (hints.metroPatterns || []).map(toPattern).filter(Boolean),
    boroughs: mapEntries(hints.boroughs),
    submarkets: mapEntries(hints.submarkets),
    microAreas: mapEntries(hints.microAreas),
  };
}

function blobOf(packet, seed) {
  return [
    packet?.title,
    packet?.summaryWhat,
    packet?.eventName,
    seed?.opportunityType,
    seed?.segment,
    seed?.researchSegment,
    seed?.demandMechanism,
  ]
    .map(text)
    .join(" ");
}

/**
 * Strip confidential operator/hotel fields from a shared market packet source.
 */
export function buildSharedPublicMarketPacket(sourceOpp = {}, sourceHotelId = null) {
  const cleaned = stripHotelPrivateKeys({ ...sourceOpp });
  for (const k of CONFIDENTIAL_STRIP_KEYS) {
    if (k in cleaned) delete cleaned[k];
  }
  const packet = extractMarketOpportunityPacket(cleaned, sourceHotelId);
  // Defense in depth — ensure confidential keys never hitchhike on who/contacts.
  if (packet?.who?.primaryContact) {
    const c = { ...packet.who.primaryContact };
    delete c.operatorPrivateNotes;
    delete c.hotelSalesFeedback;
    packet.who.primaryContact = c;
  }
  packet.layer = "SHARED_PUBLIC_MARKET_INTELLIGENCE";
  packet.confidentialKeysStripped = CONFIDENTIAL_STRIP_KEYS.filter(
    (k) => sourceOpp[k] != null,
  );
  return packet;
}

function isOverflowMotion(packet, seed) {
  return /overflow|housing|room block|vip housing|exhibitor lodging/i.test(
    blobOf(packet, seed),
  );
}

function isPreferredLodgingMotion(packet, seed) {
  return /preferred.?lodging|venue partnership|housing agency partner/i.test(
    blobOf(packet, seed),
  );
}

function isMeetingCateringMotion(packet, seed) {
  return (
    /\b(meeting|conference|symposium|convention|ballroom|plenary|catering|banquet)\b/i.test(
      blobOf(packet, seed),
    ) && !isOverflowMotion(packet, seed)
  );
}

function materialMissingForDecision(missingData = [], hotelProfile = {}) {
  const material = [];
  const roomsUnknown =
    hotelProfile?.rooms == null && hotelProfile?.totalGuestrooms == null;
  if (roomsUnknown) material.push("rooms_unknown");
  for (const m of missingData) {
    if (
      m === "room_demand_peak" ||
      m === "event_spaces_not_researched" ||
      m === "source_url"
    ) {
      material.push(m);
    }
  }
  if (
    hotelProfile?.eventSpaceDomainStatus === "UNKNOWN_UNRESEARCHED" ||
    hotelProfile?.eventSpaceCapability?.meetingCapabilityState ===
      "UNKNOWN_UNRESEARCHED"
  ) {
    material.push("meeting_capability_unknown");
  }
  return [...new Set(material)];
}

/**
 * Map cross-hotel fit engine output → founder portfolio fit class.
 * No new production weights — uses existing finalState + demand mechanism cues.
 */
export function mapCrossHotelFitToPortfolioClass({
  crossHotelFit = {},
  marketPacket = {},
  seedOpp = {},
  hotelProfile = {},
} = {}) {
  const finalState = crossHotelFit.finalState;
  const missing = Array.isArray(crossHotelFit.missingData)
    ? crossHotelFit.missingData
    : [];
  const materialMissing = materialMissingForDecision(missing, hotelProfile);

  const meetingCapabilityUnknown =
    hotelProfile?.meetingSqFt == null ||
    hotelProfile?.eventSpaceCapability?.meetingCapabilityState ===
      "UNKNOWN_UNRESEARCHED" ||
    hotelProfile?.eventSpaceDomainStatus === "UNKNOWN_UNRESEARCHED" ||
    missing.includes("event_spaces_not_researched");

  // UNKNOWN must never become a positive claim — and must not be mislabeled
  // UNSUITABLE when the material attribute simply has not been researched.
  if (isMeetingCateringMotion(marketPacket, seedOpp) && meetingCapabilityUnknown) {
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.INSUFFICIENT_EVIDENCE,
      confidence: FIT_CONFIDENCE.UNKNOWN,
      materialMissingAttributes: [
        ...new Set([...materialMissing, "meeting_capability_unknown"]),
      ],
    };
  }

  if (
    finalState === HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE ||
    finalState === HOTEL_OPP_FINAL_STATE.NOT_FIT ||
    finalState === HOTEL_OPP_FINAL_STATE.CLOSED
  ) {
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.UNSUITABLE,
      confidence: FIT_CONFIDENCE.HIGH,
      materialMissingAttributes: materialMissing,
    };
  }

  if (
    finalState === HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA &&
    materialMissing.length > 0
  ) {
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.INSUFFICIENT_EVIDENCE,
      confidence: FIT_CONFIDENCE.LOW,
      materialMissingAttributes: materialMissing,
    };
  }

  if (isPreferredLodgingMotion(marketPacket, seedOpp)) {
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.PREFERRED_LODGING_OPPORTUNITY,
      confidence:
        finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY
          ? FIT_CONFIDENCE.HIGH
          : FIT_CONFIDENCE.MEDIUM,
      materialMissingAttributes: materialMissing,
    };
  }

  if (isOverflowMotion(marketPacket, seedOpp)) {
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.OVERFLOW_ACCOMMODATION,
      confidence:
        Number(crossHotelFit.hotelFitScore) >= 60
          ? FIT_CONFIDENCE.MEDIUM
          : FIT_CONFIDENCE.LOW,
      materialMissingAttributes: materialMissing,
    };
  }

  if (
    isMeetingCateringMotion(marketPacket, seedOpp) &&
    Number(hotelProfile?.meetingSqFt) > 0
  ) {
    const peak =
      marketPacket?.roomDemand?.estimatedPeakRooms ??
      seedOpp?.estimatedPeakRooms ??
      null;
    const rooms = hotelProfile?.rooms ?? hotelProfile?.totalGuestrooms ?? null;
    if (peak != null && rooms != null && peak > rooms * 0.9) {
      return {
        fitClass: PORTFOLIO_HOTEL_FIT_CLASS.MEETING_CATERING_OPPORTUNITY,
        confidence: FIT_CONFIDENCE.MEDIUM,
        materialMissingAttributes: materialMissing,
      };
    }
    if (
      finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY ||
      Number(crossHotelFit.hotelFitScore) >= 60
    ) {
      return {
        fitClass: PORTFOLIO_HOTEL_FIT_CLASS.POTENTIAL_PRIMARY_HOST,
        confidence:
          finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY
            ? FIT_CONFIDENCE.HIGH
            : FIT_CONFIDENCE.MEDIUM,
        materialMissingAttributes: materialMissing,
      };
    }
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.MEETING_CATERING_OPPORTUNITY,
      confidence: FIT_CONFIDENCE.MEDIUM,
      materialMissingAttributes: materialMissing,
    };
  }

  if (
    finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY ||
    Number(crossHotelFit.hotelFitScore) >= 60
  ) {
    return {
      fitClass: PORTFOLIO_HOTEL_FIT_CLASS.OTHER_PARTNERSHIP,
      confidence: FIT_CONFIDENCE.MEDIUM,
      materialMissingAttributes: materialMissing,
    };
  }

  return {
    fitClass: PORTFOLIO_HOTEL_FIT_CLASS.INSUFFICIENT_EVIDENCE,
    confidence: FIT_CONFIDENCE.LOW,
    materialMissingAttributes: materialMissing.length
      ? materialMissing
      : ["fit_evidence_incomplete"],
  };
}

function recommendationFor(fitClass, hotelDisplayName) {
  const name = hotelDisplayName || "this hotel";
  switch (fitClass) {
    case PORTFOLIO_HOTEL_FIT_CLASS.POTENTIAL_PRIMARY_HOST:
      return `Pursue ${name} as a potential primary host with verified evidence in hand.`;
    case PORTFOLIO_HOTEL_FIT_CLASS.OVERFLOW_ACCOMMODATION:
      return `Position ${name} for overflow / room-block demand; confirm housing pathway.`;
    case PORTFOLIO_HOTEL_FIT_CLASS.PREFERRED_LODGING_OPPORTUNITY:
      return `Evaluate preferred-lodging partnership route for ${name}.`;
    case PORTFOLIO_HOTEL_FIT_CLASS.MEETING_CATERING_OPPORTUNITY:
      return `Pursue meeting/catering capture at ${name}; overnight role secondary.`;
    case PORTFOLIO_HOTEL_FIT_CLASS.OTHER_PARTNERSHIP:
      return `Explore a non-host commercial partnership path for ${name}.`;
    case PORTFOLIO_HOTEL_FIT_CLASS.UNSUITABLE:
      return `Do not pursue ${name} for this market opportunity on current evidence.`;
    case PORTFOLIO_HOTEL_FIT_CLASS.INSUFFICIENT_EVIDENCE:
    default:
      return `Do not claim suitability for ${name} until material hotel attributes are verified.`;
  }
}

/**
 * Evaluate one shared market opportunity against one portfolio hotel.
 */
export function evaluatePortfolioHotelOpportunityFit({
  sourceOpportunity = {},
  hotelConfig = {},
  hotelProfileOverride = null,
  marketHints = {},
  membership = null,
} = {}) {
  if (membership && membership.authorized !== true) {
    return {
      ok: false,
      code: "HOTEL_NOT_AUTHORIZED_FOR_OPERATOR_MARKET",
      blockers: membership.blockers || ["UNAUTHORIZED_MEMBERSHIP"],
      airtableWrites: 0,
      publicationWrites: 0,
    };
  }

  const hotelId = text(
    hotelConfig.hotelId || hotelProfileOverride?.hotelId || membership?.hotelId,
  );
  const marketPacket = buildSharedPublicMarketPacket(
    sourceOpportunity,
    text(sourceOpportunity.hotelId) || null,
  );

  const configCapability = {
    hotelId,
    displayName:
      hotelConfig.displayName ||
      hotelConfig.hotelName ||
      membership?.hotelDisplayName ||
      hotelId,
    rooms:
      hotelConfig.capabilityProfile?.totalGuestrooms ??
      hotelConfig.rooms ??
      null,
    meetingSqFt:
      hotelConfig.capabilityProfile?.totalMeetingSpaceSqFt ??
      hotelConfig.meetingSqFt ??
      null,
    peakRoomsMin: hotelConfig.commercialPriorities?.coreTargetPeakRoomsMin ?? null,
    peakRoomsMax: hotelConfig.commercialPriorities?.coreTargetPeakRoomsMax ?? null,
    eventSpaceDomainStatus: hotelConfig.eventSpaceDomainStatus || null,
    eventSpaceCapability: hotelConfig.eventSpaceCapability || null,
    config: hotelConfig,
    metro: hotelConfig.metro || null,
    submarket:
      hotelConfig.submarket || hotelConfig.demandTerritory?.label || null,
    microArea: hotelConfig.microArea || null,
    territoryLabel: hotelConfig.demandTerritory?.label || null,
    marketHintsKey: hotelConfig.marketHintsKey || null,
  };

  let geoProfile = hotelProfileOverride || null;
  if (!geoProfile && hotelId && !String(hotelId).startsWith("fixture_")) {
    try {
      const built = buildHotelGeographyProfile(hotelId);
      if (built && !built.error) {
        // Prefer explicit verified capability from steward hotelConfigs when present.
        geoProfile = {
          ...built,
          displayName: configCapability.displayName || built.displayName,
          rooms: configCapability.rooms ?? built.rooms ?? built.totalGuestrooms ?? null,
          meetingSqFt:
            configCapability.meetingSqFt ?? built.meetingSqFt ?? null,
          peakRoomsMin: configCapability.peakRoomsMin ?? built.peakRoomsMin ?? null,
          peakRoomsMax: configCapability.peakRoomsMax ?? built.peakRoomsMax ?? null,
          eventSpaceDomainStatus:
            configCapability.eventSpaceDomainStatus ||
            built.eventSpaceDomainStatus ||
            null,
          eventSpaceCapability:
            configCapability.eventSpaceCapability ||
            built.eventSpaceCapability ||
            null,
          config: hotelConfig?.hotelId ? hotelConfig : built.config,
          metro: configCapability.metro || built.metro || null,
          submarket: configCapability.submarket || built.submarket || null,
          microArea: configCapability.microArea || built.microArea || null,
          territoryLabel:
            configCapability.territoryLabel || built.territoryLabel || null,
        };
      }
    } catch {
      geoProfile = null;
    }
  }
  if (!geoProfile) {
    geoProfile = configCapability;
  }

  const geographicApplicability = evaluateHotelGeographicApplicability(
    sourceOpportunity,
    geoProfile,
    { marketHints: hydrateMarketHints(marketHints) },
  );

  const crossHotelFit = evaluateCrossHotelFit({
    marketPacket,
    hotelProfile: geoProfile,
    geographicApplicability,
    seedOpp: sourceOpportunity,
  });

  const mapped = mapCrossHotelFitToPortfolioClass({
    crossHotelFit,
    marketPacket,
    seedOpp: sourceOpportunity,
    hotelProfile: geoProfile,
  });

  const hotelOpportunityId = computeMarketLinkedHotelOpportunityId(
    hotelId,
    marketPacket.marketOpportunityId,
  );

  const evidence = [];
  for (const u of marketPacket.sources || []) {
    if (typeof u === "string" && /^https?:\/\//i.test(u)) evidence.push({ url: u });
    else if (u?.url && /^https?:\/\//i.test(u.url)) evidence.push({ url: u.url });
  }
  if (marketPacket.officialSource && /^https?:\/\//i.test(marketPacket.officialSource)) {
    evidence.push({ url: marketPacket.officialSource });
  }

  const limitations = [
    ...(crossHotelFit.reasons || []),
    ...(mapped.materialMissingAttributes || []).map(
      (a) => `Missing/unknown attribute: ${a}`,
    ),
  ];

  return {
    ok: true,
    version: GDI_PORTFOLIO_HOTEL_OPPORTUNITY_FIT_CONTRACT_V1,
    layers: {
      market: "SHARED_PUBLIC_MARKET_INTELLIGENCE",
      membership: "OPERATOR_PORTFOLIO_MEMBERSHIP",
      qualification: "HOTEL_SPECIFIC_QUALIFICATION",
      confidential: "EXCLUDED_FROM_SHARED_PACKET",
    },
    marketOpportunityId: marketPacket.marketOpportunityId,
    hotelOpportunityId,
    hotelId,
    operatorId: membership?.operatorId || null,
    marketId: membership?.marketId || null,
    fitClass: mapped.fitClass,
    confidence: mapped.confidence,
    hotelFitScore: crossHotelFit.hotelFitScore,
    crossHotelFinalState: crossHotelFit.finalState,
    geographicApplicability: geographicApplicability?.applicability || null,
    evidence,
    limitations,
    missingAttributes: mapped.materialMissingAttributes || [],
    recommendedPursuit: recommendationFor(
      mapped.fitClass,
      geoProfile.displayName || hotelId,
    ),
    sharedMarketPacket: marketPacket,
    confidentialKeysStripped: marketPacket.confidentialKeysStripped || [],
    airtableWrites: 0,
    publicationWrites: 0,
    productionWeightsIntroduced: false,
  };
}

/**
 * Evaluate one market opportunity across many authorized portfolio hotels.
 * Skips unauthorized hotels; does not duplicate the market research object.
 */
export function evaluatePortfolioMarketOpportunityAcrossHotels({
  sourceOpportunity = {},
  memberships = [],
  hotelConfigsById = {},
  marketHints = {},
} = {}) {
  const shared = buildSharedPublicMarketPacket(sourceOpportunity);
  const hotelAssessments = [];
  for (const membership of memberships) {
    const norm =
      membership.authorized != null
        ? membership
        : null;
    if (!norm || norm.authorized !== true) {
      hotelAssessments.push({
        ok: false,
        hotelId: membership?.hotelId || null,
        code: "HOTEL_NOT_AUTHORIZED_FOR_OPERATOR_MARKET",
        blockers: membership?.blockers || ["UNAUTHORIZED"],
      });
      continue;
    }
    const hotelConfig = hotelConfigsById[norm.hotelId] || {
      hotelId: norm.hotelId,
      displayName: norm.hotelDisplayName,
    };
    hotelAssessments.push(
      evaluatePortfolioHotelOpportunityFit({
        sourceOpportunity,
        hotelConfig,
        membership: norm,
        marketHints,
      }),
    );
  }

  const marketOpportunityIds = new Set(
    hotelAssessments
      .filter((a) => a.ok)
      .map((a) => a.marketOpportunityId)
      .filter(Boolean),
  );

  return {
    version: GDI_PORTFOLIO_HOTEL_OPPORTUNITY_FIT_CONTRACT_V1,
    sharedMarketPacket: shared,
    marketOpportunityId: shared.marketOpportunityId,
    marketOpportunityIdentityStable: marketOpportunityIds.size <= 1,
    duplicateMarketResearchObjects: false,
    hotelAssessments,
    airtableWrites: 0,
    publicationWrites: 0,
  };
}

export default {
  GDI_PORTFOLIO_HOTEL_OPPORTUNITY_FIT_CONTRACT_V1,
  PORTFOLIO_HOTEL_FIT_CLASS,
  FIT_CONFIDENCE,
  buildSharedPublicMarketPacket,
  hydrateMarketHints,
  mapCrossHotelFitToPortfolioClass,
  evaluatePortfolioHotelOpportunityFit,
  evaluatePortfolioMarketOpportunityAcrossHotels,
};
