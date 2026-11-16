/**
 * Canonical Competitive Market Intelligence (CMI) eligibility contract V1.
 *
 * Recovers Bethesda Marriott's approved commercial meaning and makes it a
 * mandatory, testable part of GDI_FULL_E2E_HOTEL_TEMPLATE_V1.
 *
 * CMI = verified future group/event business committed to a competing hotel,
 * with a credible opportunity to develop the account for a future booking cycle.
 *
 * Evidence status is separate from commercial classification / Market Scope.
 */

import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
} from "./commercial-status-dimensions-v1.js";
import {
  classifyCompetitiveMarketIntelligence,
  isConfirmedCompetitorBookedBusiness,
  isHotelMarketScopeEligible,
  isCompetitiveMarketScopeEligible,
  CMI_CLASS,
  COMPETITIVE_STATUS,
  INVENTORY_POPULATION,
} from "../bethesda-67-sales-baseline-cmi-v1.js";
import { CMI_CLAIM_TYPE } from "./cmi-evidence-ledger-v1.js";

export const GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1 =
  "gdi-cmi-canonical-eligibility-contract-v1";

export const GDI_CMI_CONTRACT_VERSION = "1.0.0";

/** Founder commercial definition (binding). */
export const CMI_COMMERCIAL_DEFINITION = Object.freeze({
  meaning:
    "A future group, meeting, conference, or event booking has already been committed to another hotel. The target hotel cannot realistically pursue the principal booking for that cycle. GDI identifies the organization, competing hotel, booking evidence, and relevant contact so sales can develop the relationship for future business.",
  is: Object.freeze([
    "FUTURE_ACCOUNT_DEVELOPMENT_INFORMED_BY_COMPETITOR_WON_BUSINESS",
  ]),
  isNot: Object.freeze([
    "NEARBY_COMPETITOR_HOTEL_LIST",
    "PREFERRED_HOTEL_RECOMMENDATION_LIST",
    "HOTEL_PROGRAM_LISTING_ALONE",
    "CONVENTION_CENTER_HOSTING_WITHOUT_HOTEL_BOOKING_EVIDENCE",
    "HISTORICAL_EVENT_WITHOUT_CREDIBLE_FUTURE_RELATIONSHIP",
    "UNCONFIRMED_ASSUMPTION_OF_COMPETITOR_GROUP_WIN",
  ]),
  committedBusinessMustBeIdentified: Object.freeze([
    "EVENT_VENUE",
    "GUESTROOMS",
    "GROUP_BLOCK",
    "OTHER_HOSPITALITY_BUSINESS",
  ]),
  roomBlockRequiresGuestroomEvidence: true,
});

/**
 * Evidence / relationship statuses (orthogonal to Market Scope / sales maturity).
 */
export const CMI_EVIDENCE_STATUS = Object.freeze({
  CONFIRMED_COMPETITOR_COMMITMENT: "CONFIRMED_COMPETITOR_COMMITMENT",
  HOTEL_PROGRAM_LISTING_ONLY: "HOTEL_PROGRAM_LISTING_ONLY",
  HISTORICAL_COMPETITOR_RELATIONSHIP: "HISTORICAL_COMPETITOR_RELATIONSHIP",
  UNVERIFIED_COMPETITOR_ASSOCIATION: "UNVERIFIED_COMPETITOR_ASSOCIATION",
  FUTURE_CYCLE_ACCOUNT_DEVELOPMENT: "FUTURE_CYCLE_ACCOUNT_DEVELOPMENT",
});

export const CMI_EVIDENCE_STATUS_LABEL = Object.freeze({
  [CMI_EVIDENCE_STATUS.CONFIRMED_COMPETITOR_COMMITMENT]:
    "Confirmed competitor commitment",
  [CMI_EVIDENCE_STATUS.HOTEL_PROGRAM_LISTING_ONLY]:
    "Hotel-program listing only",
  [CMI_EVIDENCE_STATUS.HISTORICAL_COMPETITOR_RELATIONSHIP]:
    "Historical competitor relationship",
  [CMI_EVIDENCE_STATUS.UNVERIFIED_COMPETITOR_ASSOCIATION]:
    "Unverified competitor association",
  [CMI_EVIDENCE_STATUS.FUTURE_CYCLE_ACCOUNT_DEVELOPMENT]:
    "Future-cycle account development",
});

/** Bethesda-certified committed-booking venue statuses that may satisfy CMI. */
export const CMI_COMMITTED_VENUE_STATUSES = Object.freeze([
  COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK,
  COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
]);

/**
 * Eight mandatory gates for customer-facing CMI qualification.
 */
export const CMI_QUALIFICATION_GATES = Object.freeze([
  {
    id: "IDENTIFIABLE_FUTURE_EVENT_OR_GROUP",
    required: true,
  },
  {
    id: "NAMED_COMPETING_HOTEL",
    required: true,
  },
  {
    id: "EVIDENCE_OF_COMMITTED_HOTEL_BUSINESS",
    required: true,
  },
  {
    id: "COMMITTED_BUSINESS_TYPE_IDENTIFIED",
    required: true,
  },
  {
    id: "TARGET_CANNOT_REALISTICALLY_WIN_PRINCIPAL_NOW",
    required: true,
  },
  {
    id: "CREDIBLE_FUTURE_CYCLE_ACCOUNT_DEVELOPMENT",
    required: true,
  },
  {
    id: "VERIFIED_NAMED_CONTACT_OR_DEFENSIBLE_INTERMEDIARY",
    required: true,
  },
  {
    id: "EVIDENCE_GROUNDED_COMMERCIAL_NEXT_STEP",
    required: true,
  },
]);

/**
 * Mandatory Full E2E competitor-booking discovery coverage checklist.
 * Zero CMI rows is acceptable only when this research was completed and documented.
 */
export const CMI_DISCOVERY_COVERAGE_CHECKLIST = Object.freeze([
  "ESTABLISH_RELEVANT_COMPETITIVE_HOTEL_SET",
  "SEARCH_FUTURE_EVENTS_BOOKED_AT_COMPETING_HOTELS",
  "INVESTIGATE_COMPETITOR_PROPERTY_MEETING_CALENDARS",
  "IDENTIFY_ORGANIZATIONS_HOLDING_EVENTS_THERE",
  "VERIFY_ACTUAL_BOOKING_RELATIONSHIPS",
  "IDENTIFY_RECURRING_EVENT_CYCLES",
  "LOCATE_RELEVANT_DECISION_MAKERS_OR_ENTRY_POINTS",
  "DETERMINE_REALISTIC_NEXT_CYCLE_PURSUIT",
  "DEDUPLICATE_AGAINST_EXISTING_GDI_OPPORTUNITIES",
  "PERSIST_CONFIRMED_AND_REJECTED_EVIDENCE",
]);

export const CMI_DISCOVERY_STAGE = Object.freeze({
  id: "competitor_booked_business_discovery",
  name: "Competitor-booked business discovery",
  mandatoryInFullE2e: true,
  zeroCmiRequiresCoverageProof: true,
  reuse: Object.freeze([
    "specialist hunters",
    "Exa infrastructure",
    "full-e2e supervisor / research-to-page workflow",
    "classifyCompetitiveMarketIntelligence",
    "competitive-future-cycle-presentation-v1",
  ]),
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function hasNamedCompetitor(opp = {}, cmi = null) {
  return Boolean(
    text(
      (cmi && cmi.competitorHotel) ||
        opp.cmiCompetitorHotel ||
        opp.competitorHotel ||
        opp.likelyCompetitor ||
        ""
    )
  );
}

function inferCommittedBusinessType(opp = {}) {
  const explicit = upper(
    opp.committedBusinessType ||
      opp.cmiEvidenceRegisterV1?.commitmentScope ||
      opp.cmiEvidenceRegisterV1?.bookingType ||
      ""
  );
  if (/VENUE_AND_GUESTROOMS|GROUP_BLOCK|ROOM_BLOCK/.test(explicit)) {
    return "VENUE_AND_GUESTROOMS";
  }
  if (/GUESTROOM|ROOM/.test(explicit) && !/VENUE_ONLY|VENUE_PRIMARY/.test(explicit)) {
    return "GUESTROOMS";
  }
  if (/VENUE/.test(explicit)) return "EVENT_VENUE";
  if (/OTHER/.test(explicit)) return "OTHER_HOSPITALITY_BUSINESS";

  const venue = upper(opp.competitorVenueStatus || "");
  if (venue === COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK) {
    return "GUESTROOMS";
  }
  if (venue === COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE) {
    return "EVENT_VENUE";
  }
  const blob = [
    opp.competitorBookingClaim,
    opp.lodgingEvidence,
    opp.summaryWhat,
    ...(opp.claims || []).map((c) => c?.statement || ""),
  ]
    .map(text)
    .join(" ");
  if (/room block|published room rates|group (?:clave|code)|contracted block/i.test(blob)) {
    return "GUESTROOMS";
  }
  if (/host hotel|conference hotel|hotel sede|event venue/i.test(blob)) {
    return "EVENT_VENUE";
  }
  return null;
}

function hasFutureEventOrGroup(opp = {}) {
  const start =
    opp.tileEventStartDate || opp.eventStartDate || opp.eventDates || "";
  if (/\b20(2[6-9]|[3-9]\d)\b/.test(String(start))) return true;
  if (/\b20(2[6-9]|[3-9]\d)\b/.test(`${opp.title || ""} ${opp.displayTitle || ""}`)) {
    return true;
  }
  // Recurring / future-cycle monitor without locked next date still qualifies
  // as identifiable activity when historical venue is evidenced + monitor thesis.
  return (
    upper(opp.targetHotelSourcingStatus) ===
      TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR ||
    upper(opp.competitorVenueStatus) ===
      COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE
  );
}

function hasContactOrIntermediary(opp = {}) {
  const c = opp.primaryContact || {};
  if (text(c.name) && text(c.name).length >= 2) return true;
  if (text(opp.primaryContactName)) return true;
  if (text(c.organization) || text(opp.organizationName)) {
    // Defensible intermediary / organizer desk allowed when accurately labeled.
    const cls = upper(c.contactClass || "");
    if (
      cls.includes("ENTRY_POINT") ||
      cls.includes("INTERMEDIARY") ||
      cls.includes("ORGANIZER")
    ) {
      return true;
    }
  }
  return false;
}

function hasEvidenceGroundedNextStep(opp = {}) {
  const action = text(
    opp.recommendedAction ||
      opp.recommendedNextStep ||
      opp.competitiveFutureCyclePresentation?.potentialFutureOpportunity
        ?.recommendedSalesAction ||
      ""
  );
  if (!action) return false;
  // Reject open-host RFP spam for confirmed competitor commitments.
  // Allow explicit negatives: "do not request an RFP as host".
  if (
    /(?:^|[^.])\b(?:please\s+)?(?:request an rfp as host|bid as host)\b/i.test(
      action
    ) &&
    !/\bdo not (?:request an rfp as host|bid as host)\b/i.test(action)
  ) {
    return false;
  }
  if (/\bopen host rfp\b/i.test(action) && !/\bnot an open host rfp\b/i.test(action)) {
    return false;
  }
  return action.length >= 24;
}

/**
 * Map Bethesda classifier + opportunity fields → founder evidence status.
 */
export function classifyCmiEvidenceStatus(opp = {}, cmi = null) {
  const classification = cmi || classifyCompetitiveMarketIntelligence(opp);
  const venue = upper(
    classification.competitorVenueStatus || opp.competitorVenueStatus || ""
  );

  if (
    classification.verified === true ||
    classification.competitiveStatus === COMPETITIVE_STATUS.VERIFIED ||
    CMI_COMMITTED_VENUE_STATUSES.includes(venue)
  ) {
    // Confirmed commitment may still carry future-cycle thesis; evidence status
    // stays CONFIRMED; commercial actionability remains separate.
    return CMI_EVIDENCE_STATUS.CONFIRMED_COMPETITOR_COMMITMENT;
  }

  if (
    classification.competitiveStatus === COMPETITIVE_STATUS.HISTORICAL ||
    venue === COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE ||
    classification.class === CMI_CLASS.HISTORICAL_COMPETITIVE
  ) {
    return CMI_EVIDENCE_STATUS.HISTORICAL_COMPETITOR_RELATIONSHIP;
  }

  const blob = [
    opp.hotelSourcingStatusLabel,
    opp.lodgingEvidence,
    opp.competitorBookingClaim,
    opp.demandEvidenceClass,
    opp.cmiEvidenceRegisterV1?.bookingType,
  ]
    .map(text)
    .join(" ");
  if (
    /official hotel|allied hotel|hotel.?program|preferred hotel|hoteles aliados|convenio especial|recommendation list/i.test(
      blob
    ) &&
    !/contracted block|published room rates|host hotel confirmed/i.test(blob)
  ) {
    return CMI_EVIDENCE_STATUS.HOTEL_PROGRAM_LISTING_ONLY;
  }

  if (
    upper(opp.targetHotelSourcingStatus) ===
      TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR &&
    hasNamedCompetitor(opp, classification)
  ) {
    return CMI_EVIDENCE_STATUS.FUTURE_CYCLE_ACCOUNT_DEVELOPMENT;
  }

  if (hasNamedCompetitor(opp, classification)) {
    return CMI_EVIDENCE_STATUS.UNVERIFIED_COMPETITOR_ASSOCIATION;
  }

  return CMI_EVIDENCE_STATUS.UNVERIFIED_COMPETITOR_ASSOCIATION;
}

/**
 * Evaluate the eight CMI qualification gates for one opportunity.
 */
export function evaluateCmiQualificationGates(opp = {}) {
  const cmi = classifyCompetitiveMarketIntelligence(opp);
  const evidenceStatus = classifyCmiEvidenceStatus(opp, cmi);
  const committedType = inferCommittedBusinessType(opp);
  const namedCompetitor = hasNamedCompetitor(opp, cmi);
  const confirmed =
    evidenceStatus === CMI_EVIDENCE_STATUS.CONFIRMED_COMPETITOR_COMMITMENT &&
    (cmi.verified === true ||
      isConfirmedCompetitorBookedBusiness(opp, cmi) ||
      CMI_COMMITTED_VENUE_STATUSES.includes(
        upper(opp.competitorVenueStatus || cmi.competitorVenueStatus || "")
      ));

  const gates = {
    IDENTIFIABLE_FUTURE_EVENT_OR_GROUP: hasFutureEventOrGroup(opp),
    NAMED_COMPETING_HOTEL: namedCompetitor,
    EVIDENCE_OF_COMMITTED_HOTEL_BUSINESS: confirmed,
    COMMITTED_BUSINESS_TYPE_IDENTIFIED: Boolean(committedType),
    TARGET_CANNOT_REALISTICALLY_WIN_PRINCIPAL_NOW:
      confirmed &&
      upper(opp.targetHotelSourcingStatus || cmi.targetHotelSourcingStatus) !==
        TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED,
    CREDIBLE_FUTURE_CYCLE_ACCOUNT_DEVELOPMENT:
      confirmed &&
      (Boolean(text(opp.whyThisHotel)) ||
        Boolean(opp.cmiEvidenceRegisterV1?.futureCycleThesis) ||
        upper(opp.commercialActionability || cmi.commercialActionability) ===
          COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE ||
        upper(opp.targetHotelSourcingStatus) ===
          TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR ||
        upper(opp.targetHotelSourcingStatus) ===
          TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
        upper(opp.targetHotelSourcingStatus) ===
          TARGET_HOTEL_SOURCING_STATUS.UNKNOWN),
    VERIFIED_NAMED_CONTACT_OR_DEFENSIBLE_INTERMEDIARY:
      hasContactOrIntermediary(opp),
    EVIDENCE_GROUNDED_COMMERCIAL_NEXT_STEP: hasEvidenceGroundedNextStep(opp),
  };

  const failed = Object.entries(gates)
    .filter(([, ok]) => !ok)
    .map(([id]) => id);

  const qualifiesAsCustomerCmi =
    evidenceStatus === CMI_EVIDENCE_STATUS.CONFIRMED_COMPETITOR_COMMITMENT &&
    failed.length === 0;

  // Hotel-program listings must never promote to verified CMI.
  const blockedAsListingOnly =
    evidenceStatus === CMI_EVIDENCE_STATUS.HOTEL_PROGRAM_LISTING_ONLY;

  return {
    contractVersion: GDI_CMI_CONTRACT_VERSION,
    contractId: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
    evidenceStatus,
    evidenceStatusLabel: CMI_EVIDENCE_STATUS_LABEL[evidenceStatus] || evidenceStatus,
    committedBusinessType: committedType,
    competitorHotel:
      cmi.competitorHotel ||
      opp.cmiCompetitorHotel ||
      opp.competitorHotel ||
      null,
    cmiClass: cmi.class,
    cmiVerified: cmi.verified === true,
    competitiveStatus: cmi.competitiveStatus,
    gates,
    failedGates: failed,
    qualifiesAsCustomerCmi: qualifiesAsCustomerCmi && !blockedAsListingOnly,
    blockedAsListingOnly,
    hotelMarketScopeEligible: isHotelMarketScopeEligible(opp, cmi),
    competitiveMarketScopeEligible: isCompetitiveMarketScopeEligible(opp, cmi),
    inventoryPopulationPrimaryHint: qualifiesAsCustomerCmi
      ? INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
      : null,
  };
}

/**
 * Negative eligibility helpers for regression fixtures.
 */
export function assertNotCmiPromotionReasons(opp = {}) {
  const reasons = [];
  const blob = JSON.stringify(opp);
  if (/nearby|proximity|adjacent competitor list/i.test(blob) && !opp.competitorVenueStatus) {
    reasons.push("GEOGRAPHIC_PROXIMITY_ONLY");
  }
  if (
    /preferred hotel|hotel recommendation|official hotel list|allied hotel/i.test(
      blob
    ) &&
    !CMI_COMMITTED_VENUE_STATUSES.includes(upper(opp.competitorVenueStatus || ""))
  ) {
    reasons.push("HOTEL_PROGRAM_OR_RECOMMENDATION_ONLY");
  }
  if (
    /convention center|expo guadalajara|walter e\. washington/i.test(blob) &&
    !hasNamedCompetitor(opp) &&
    !opp.competitorVenueStatus
  ) {
    reasons.push("VENUE_WITHOUT_HOTEL_BOOKING");
  }
  const evalResult = evaluateCmiQualificationGates(opp);
  if (!evalResult.qualifiesAsCustomerCmi) {
    reasons.push("GATES_NOT_SATISFIED");
  }
  return reasons;
}

/**
 * Research coverage proof: zero confirmed CMI is OK only with completed checklist.
 */
export function evaluateCmiDiscoveryCoverage(coverage = {}) {
  const completed = new Set(
    (coverage.completedChecklistItems || []).map((x) => String(x).toUpperCase())
  );
  const missing = CMI_DISCOVERY_COVERAGE_CHECKLIST.filter(
    (item) => !completed.has(item)
  );
  const confirmedCount = Number(coverage.confirmedCompetitorBookingCount || 0);
  const rejectedCount = Number(coverage.rejectedEvidenceCount || 0);
  const attempted = coverage.researchAttempted === true;
  const documented = coverage.evidenceFindingsDocumented === true;

  const zeroCmiAcceptable =
    confirmedCount === 0 &&
    attempted &&
    documented &&
    missing.length === 0;

  const coverageComplete =
    attempted && documented && missing.length === 0;

  return {
    contractVersion: GDI_CMI_CONTRACT_VERSION,
    stage: CMI_DISCOVERY_STAGE,
    checklist: CMI_DISCOVERY_COVERAGE_CHECKLIST,
    missingChecklistItems: missing,
    confirmedCompetitorBookingCount: confirmedCount,
    rejectedEvidenceCount: rejectedCount,
    researchAttempted: attempted,
    evidenceFindingsDocumented: documented,
    coverageComplete,
    zeroConfirmedCmiAcceptable: zeroCmiAcceptable,
    fullE2eCmiCoverageClaimAllowed: coverageComplete,
    failReason:
      coverageComplete
        ? null
        : missing.length
          ? "CMI_DISCOVERY_CHECKLIST_INCOMPLETE"
          : !attempted
            ? "CMI_DISCOVERY_NOT_ATTEMPTED"
            : "CMI_EVIDENCE_FINDINGS_NOT_DOCUMENTED",
  };
}

/**
 * Annotate an opportunity with canonical CMI contract fields (non-mutating copy).
 */
export function annotateCanonicalCmiContract(opp = {}) {
  const evaluation = evaluateCmiQualificationGates(opp);
  return {
    ...opp,
    cmiContractVersion: GDI_CMI_CONTRACT_VERSION,
    cmiEvidenceStatus: evaluation.evidenceStatus,
    cmiEvidenceStatusLabel: evaluation.evidenceStatusLabel,
    committedBusinessType: evaluation.committedBusinessType,
    cmiQualificationGatesV1: {
      qualifiesAsCustomerCmi: evaluation.qualifiesAsCustomerCmi,
      failedGates: evaluation.failedGates,
      gates: evaluation.gates,
    },
  };
}

export {
  CMI_CLAIM_TYPE,
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
  CMI_CLASS,
  COMPETITIVE_STATUS,
  INVENTORY_POPULATION,
  classifyCompetitiveMarketIntelligence,
};
