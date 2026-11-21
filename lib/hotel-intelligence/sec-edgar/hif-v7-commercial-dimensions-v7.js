/**
 * HIF V7 — Independent commercial dimensions + no-RFP qualification policy.
 *
 * Permanent founder policy: a public RFP is NOT required for commercial qualification.
 * Commercial relevance, timing, and accessibility are evaluated independently.
 * CUSTOMER_BLOCKED.
 */

import { DECISION_WINDOW_KIND } from "./hif-v6-commercial-timing-model-v6.js";
import { COMMERCIAL_PRIORITY } from "./hif-v6-commercial-actionability-v6.js";
import { GEO_CLASS } from "./hif-v6-geographic-confidence-v6.js";
import {
  COMMERCIAL_CATEGORY,
  isPrincipalBctOrMct,
} from "./hif-v7-procurement-engagement-v7.js";
import {
  OPPORTUNITY_STATUS,
  isOpenOpportunityStatus,
  classifyOpportunityStatus,
} from "./hif-v7-opportunity-status-v7.js";

export const HIF_V7_COMMERCIAL_DIMENSIONS = "hif-v7-commercial-dimensions-v7";

/** Evidence pathways — none is superior merely because an RFP exists. */
export const EVIDENCE_PATHWAY = Object.freeze({
  PUBLIC_COMPETITIVE_PROCESS: "PUBLIC_COMPETITIVE_PROCESS",
  OWNER_LED_PRIVATE_DECISION: "OWNER_LED_PRIVATE_DECISION",
  CONTRACTUAL_DECISION_WINDOW: "CONTRACTUAL_DECISION_WINDOW",
  COMMERCIAL_EVENT_TRIGGER: "COMMERCIAL_EVENT_TRIGGER",
  RELATIONSHIP_LED_OPPORTUNITY: "RELATIONSHIP_LED_OPPORTUNITY",
  OTHER_VERIFIED_PATHWAY: "OTHER_VERIFIED_PATHWAY",
});

export const COMMERCIAL_RELEVANCE = Object.freeze({
  VERIFIED_RELEVANT: "VERIFIED_RELEVANT",
  EVIDENCE_SUPPORTED_POTENTIAL: "EVIDENCE_SUPPORTED_POTENTIAL",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  NOT_RELEVANT: "NOT_RELEVANT",
});

export const COMMERCIAL_TIMING = Object.freeze({
  ACTIVE_DECISION: "ACTIVE_DECISION",
  M0_6: "0_6_MONTHS",
  M6_12: "6_12_MONTHS",
  M12_24: "12_24_MONTHS",
  M24_36: "24_36_MONTHS",
  LONGER_TERM: "LONGER_TERM",
  UNKNOWN: "UNKNOWN",
});

export const COMMERCIAL_ACCESSIBILITY = Object.freeze({
  ACCESSIBLE_VERIFIED: "ACCESSIBLE_VERIFIED",
  ACCESSIBLE_PROBABLE: "ACCESSIBLE_PROBABLE",
  ACCESS_ROUTE_RESEARCH_REQUIRED: "ACCESS_ROUTE_RESEARCH_REQUIRED",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  ACCESS_CLOSED: "ACCESS_CLOSED",
  UNKNOWN: "UNKNOWN",
});

export const ENGAGEMENT_ROUTE_V7 = Object.freeze({
  DIRECT_OWNER_ENGAGEMENT: "DIRECT_OWNER_ENGAGEMENT",
  ASSET_MANAGER: "ASSET_MANAGER",
  OWNERSHIP_REPRESENTATIVE: "OWNERSHIP_REPRESENTATIVE",
  DEVELOPER: "DEVELOPER",
  HOTEL_INVESTMENT_ADVISOR: "HOTEL_INVESTMENT_ADVISOR",
  EXISTING_COMMERCIAL_RELATIONSHIP: "EXISTING_COMMERCIAL_RELATIONSHIP",
  MANAGEMENT_OR_BRAND_DEVELOPMENT_CONTACT: "MANAGEMENT_OR_BRAND_DEVELOPMENT_CONTACT",
  PRIVATE_NEGOTIATIONS: "PRIVATE_NEGOTIATIONS",
  PUBLIC_PROCUREMENT: "PUBLIC_PROCUREMENT",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
});

/** Pathways that do NOT require a public RFP. */
export const NO_RFP_REQUIRED_PATHWAYS = Object.freeze([
  EVIDENCE_PATHWAY.OWNER_LED_PRIVATE_DECISION,
  EVIDENCE_PATHWAY.CONTRACTUAL_DECISION_WINDOW,
  EVIDENCE_PATHWAY.COMMERCIAL_EVENT_TRIGGER,
  EVIDENCE_PATHWAY.RELATIONSHIP_LED_OPPORTUNITY,
  EVIDENCE_PATHWAY.OTHER_VERIFIED_PATHWAY,
  EVIDENCE_PATHWAY.PUBLIC_COMPETITIVE_PROCESS,
]);

export function requiresPublicRfp(pathway) {
  return false; // Permanent policy: never require RFP for qualification
}

export function isCommerciallyRelevant(relevance) {
  return (
    relevance === COMMERCIAL_RELEVANCE.VERIFIED_RELEVANT ||
    relevance === COMMERCIAL_RELEVANCE.EVIDENCE_SUPPORTED_POTENTIAL
  );
}

export function isCommerciallyAccessible(accessibility) {
  return (
    accessibility === COMMERCIAL_ACCESSIBILITY.ACCESSIBLE_VERIFIED ||
    accessibility === COMMERCIAL_ACCESSIBILITY.ACCESSIBLE_PROBABLE
  );
}

export function isNearTermTiming(timing) {
  return [
    COMMERCIAL_TIMING.ACTIVE_DECISION,
    COMMERCIAL_TIMING.M0_6,
    COMMERCIAL_TIMING.M6_12,
    COMMERCIAL_TIMING.M12_24,
  ].includes(timing);
}

/**
 * Classify commercial relevance from hotel-specific evidence — never invent.
 */
export function classifyCommercialRelevance(row = {}) {
  if (row.commercialRelevance && COMMERCIAL_RELEVANCE[row.commercialRelevance]) {
    return { commercialRelevance: row.commercialRelevance, basis: "EXPLICIT_FIELD" };
  }

  const category = row.commercialCategory || row.opportunityType;
  const adjacentOnly =
    category === COMMERCIAL_CATEGORY.HOTEL_DEVELOPMENT ||
    category === COMMERCIAL_CATEGORY.HOTEL_CONCESSION_OPERATING ||
    category === COMMERCIAL_CATEGORY.HOTEL_REDEVELOPMENT;

  // Acquisition / foreclosure / renovation alone ≠ BCT/MCT
  if (
    row.signalCategory === "COMMERCIAL_CATALYST" &&
    !row.linkedBrandOrOperatorDecision &&
    !row.brandOrManagementDecisionEvidenced
  ) {
    return {
      commercialRelevance: COMMERCIAL_RELEVANCE.NOT_RELEVANT,
      basis: "CATALYST_WITHOUT_BRAND_OR_OPERATOR_DECISION",
    };
  }

  if (row.hotelAcquisitionOnly === true && !row.linkedBrandOrOperatorDecision) {
    return {
      commercialRelevance: COMMERCIAL_RELEVANCE.NOT_RELEVANT,
      basis: "ACQUISITION_ALONE_INSUFFICIENT",
    };
  }

  if (adjacentOnly && !row.forcePrincipalBctMct) {
    // Adjacent intelligence may be commercially useful but is not principal BCT/MCT relevance
    if (row.brandOrManagementDecisionEvidenced) {
      return {
        commercialRelevance: COMMERCIAL_RELEVANCE.EVIDENCE_SUPPORTED_POTENTIAL,
        basis: "ADJACENT_WITH_OPERATOR_OR_BRAND_SIGNAL",
        principalBctMct: false,
      };
    }
    return {
      commercialRelevance: COMMERCIAL_RELEVANCE.EVIDENCE_SUPPORTED_POTENTIAL,
      basis: "ADJACENT_CATEGORY",
      principalBctMct: false,
    };
  }

  if (row.decisionCompleted === true || row.decisionAwarded === true) {
    return {
      commercialRelevance: COMMERCIAL_RELEVANCE.NOT_RELEVANT,
      basis: "DECISION_ALREADY_COMPLETED_OR_AWARDED",
    };
  }

  if (
    row.brandOrManagementDecisionEvidenced === true ||
    row.ownerLedBrandReview === true ||
    row.ownerLedOperatorReview === true ||
    row.activeSelectionProcess === true ||
    row.openProcessVerified === true ||
    row.confirmedNonrenewal === true ||
    row.confirmedTerminationForthcoming === true
  ) {
    return {
      commercialRelevance: COMMERCIAL_RELEVANCE.VERIFIED_RELEVANT,
      basis: "HOTEL_SPECIFIC_DECISION_EVIDENCE",
      principalBctMct: isPrincipalBctOrMct(category) || Boolean(row.bct || row.mct),
    };
  }

  if (row.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY) {
    return {
      commercialRelevance: COMMERCIAL_RELEVANCE.INSUFFICIENT_EVIDENCE,
      basis: "EXPIRATION_ONLY_PROXY",
    };
  }

  return {
    commercialRelevance: COMMERCIAL_RELEVANCE.INSUFFICIENT_EVIDENCE,
    basis: "NO_DEFENSIBLE_DECISION_EVIDENCE",
  };
}

/**
 * Accessibility is independent of relevance.
 * Missing RFP / unknown owner must NOT auto-map to ACCESS_CLOSED.
 */
export function classifyCommercialAccessibility(row = {}) {
  if (row.commercialAccessibility && COMMERCIAL_ACCESSIBILITY[row.commercialAccessibility]) {
    return { commercialAccessibility: row.commercialAccessibility, basis: "EXPLICIT_FIELD" };
  }

  if (row.accessClosed === true || row.procurementClosedToNewEntrants === true) {
    return {
      commercialAccessibility: COMMERCIAL_ACCESSIBILITY.ACCESS_CLOSED,
      basis: "CLOSED_TO_NEW_PARTICIPANTS",
    };
  }
  if (row.accessRestricted === true) {
    return {
      commercialAccessibility: COMMERCIAL_ACCESSIBILITY.ACCESS_RESTRICTED,
      basis: "RESTRICTED_PROCESS",
    };
  }
  if (row.engagementRouteVerified === true && row.decisionPartyIdentified === true) {
    return {
      commercialAccessibility: COMMERCIAL_ACCESSIBILITY.ACCESSIBLE_VERIFIED,
      basis: "VERIFIED_ENGAGEMENT_ROUTE",
    };
  }
  if (row.engagementRouteProbable === true || row.decisionPartyIdentified === true) {
    return {
      commercialAccessibility: COMMERCIAL_ACCESSIBILITY.ACCESSIBLE_PROBABLE,
      basis: "PROBABLE_ENGAGEMENT_ROUTE",
    };
  }
  // Unknown owner/contact → research required — NOT closed
  if (!row.ownerOrContractingParty && !row.decisionPartyIdentified) {
    return {
      commercialAccessibility: COMMERCIAL_ACCESSIBILITY.ACCESS_ROUTE_RESEARCH_REQUIRED,
      basis: "OWNER_OR_CONTACT_UNKNOWN_NOT_CLOSED",
    };
  }
  if (!row.engagementRoute && !row.engagementRouteVerified) {
    return {
      commercialAccessibility: COMMERCIAL_ACCESSIBILITY.ACCESS_ROUTE_RESEARCH_REQUIRED,
      basis: "ENGAGEMENT_ROUTE_UNKNOWN_NOT_CLOSED",
    };
  }
  return {
    commercialAccessibility: COMMERCIAL_ACCESSIBILITY.UNKNOWN,
    basis: "INSUFFICIENT_ACCESS_EVIDENCE",
  };
}

export function mapMonthsToCommercialTiming(months, { active = false } = {}) {
  if (active) return COMMERCIAL_TIMING.ACTIVE_DECISION;
  if (months === null || months === undefined || Number.isNaN(months)) {
    return COMMERCIAL_TIMING.UNKNOWN;
  }
  if (months < 0) return COMMERCIAL_TIMING.UNKNOWN;
  if (months < 6) return COMMERCIAL_TIMING.M0_6;
  if (months < 12) return COMMERCIAL_TIMING.M6_12;
  if (months < 24) return COMMERCIAL_TIMING.M12_24;
  if (months < 36) return COMMERCIAL_TIMING.M24_36;
  return COMMERCIAL_TIMING.LONGER_TERM;
}

/**
 * First-100 commercially relevant gate (principal BCT/MCT).
 * Does NOT require public RFP or verified accessibility.
 * Does NOT count adjacent-only or expiration-only proxies.
 */
export function qualifiesCommerciallyRelevantFirst100(row = {}) {
  const reasons = [];
  const fail = (code) => ({ qualifies: false, reasons: [...reasons, code] });

  const relevance = classifyCommercialRelevance(row);
  if (!isCommerciallyRelevant(relevance.commercialRelevance)) {
    return fail(relevance.basis || "NOT_COMMERCIALLY_RELEVANT");
  }
  if (relevance.principalBctMct === false) {
    return fail("ADJACENT_NOT_PRINCIPAL_BCT_MCT");
  }

  const category = row.commercialCategory;
  const bct =
    category === COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET ||
    row.bct === true ||
    row.opportunityType === "BCT" ||
    row.commercialList === "BRAND_CONVERSION_TARGETS";
  const mct =
    category === COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET ||
    row.mct === true ||
    row.opportunityType === "MCT" ||
    row.commercialList === "MANAGEMENT_CONTRACT_TARGETS";
  if (!bct && !mct) return fail("NO_PRINCIPAL_BCT_OR_MCT");

  if (!row.hotelName || String(row.hotelName).trim().length < 5) {
    return fail("HOTEL_IDENTITY_MISSING");
  }
  const geoClass = row.geographicClass;
  if (geoClass === GEO_CLASS.UNRESOLVED) return fail("PHYSICAL_COUNTRY_UNRESOLVED");
  if (!row.physicalCountry && !row.hotelCountry) return fail("PHYSICAL_COUNTRY_MISSING");

  if (row.decisionCompleted === true || row.decisionAwarded === true) {
    return fail("DECISION_COMPLETED_OR_AWARDED");
  }
  if (row.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY) {
    return fail("EXPIRATION_ONLY_PROXY_EXCLUDED");
  }

  const timing =
    row.commercialTiming ||
    mapMonthsToCommercialTiming(row.monthsToDecision, {
      active: row.activeSelectionProcess === true || row.openProcessVerified === true,
    });
  if (!isNearTermTiming(timing) && row.commercialTiming !== COMMERCIAL_TIMING.ACTIVE_DECISION) {
    // Allow explicit ACTIVE_DECISION or near-term; reject unknown long-term without active flag
    if (timing === COMMERCIAL_TIMING.UNKNOWN && !row.nearTermDecisionEvidenced) {
      return fail("TIMING_NOT_NEAR_TERM");
    }
    if (
      timing === COMMERCIAL_TIMING.M24_36 ||
      timing === COMMERCIAL_TIMING.LONGER_TERM
    ) {
      return fail("OUTSIDE_24M_WINDOW");
    }
  }

  if (!row.brandOrManagementDecisionEvidenced && !row.ownerLedBrandReview && !row.ownerLedOperatorReview && !row.activeSelectionProcess && !row.openProcessVerified && !row.confirmedNonrenewal) {
    return fail("DECISION_EVIDENCE_MISSING");
  }

  // Public RFP is optional — never fail for missing RFP
  if (row.requiresPublicRfp === true) {
    // Ignore any legacy flag that tried to require RFP
  }

  reasons.push("COMMERCIALLY_RELEVANT_FIRST_100");
  return {
    qualifies: true,
    reasons,
    commercialRelevance: relevance.commercialRelevance,
    commercialTiming: timing,
    requiresPublicRfp: false,
  };
}

/**
 * Commercially accessible subset — independent of first-100 relevance count.
 */
export function qualifiesCommerciallyAccessible(row = {}) {
  const relevanceGate = qualifiesCommerciallyRelevantFirst100(row);
  if (!relevanceGate.qualifies) {
    return { qualifies: false, reasons: ["NOT_COMMERCIALLY_RELEVANT", ...relevanceGate.reasons] };
  }
  const access = classifyCommercialAccessibility(row);
  if (!isCommerciallyAccessible(access.commercialAccessibility)) {
    return {
      qualifies: false,
      reasons: [access.basis || "NOT_ACCESSIBLE"],
      commercialAccessibility: access.commercialAccessibility,
    };
  }
  return {
    qualifies: true,
    reasons: ["COMMERCIALLY_ACCESSIBLE"],
    commercialAccessibility: access.commercialAccessibility,
  };
}

/**
 * Outreach-ready: accessible + identified decision party + defensible engagement reason.
 * Not required for commercially relevant inventory.
 */
export function qualifiesOutreachReady(row = {}) {
  const accessible = qualifiesCommerciallyAccessible(row);
  if (!accessible.qualifies) {
    return { qualifies: false, reasons: accessible.reasons };
  }
  if (!row.decisionPartyIdentified && !row.ownerOrContractingParty) {
    return { qualifies: false, reasons: ["DECISION_PARTY_NOT_IDENTIFIED"] };
  }
  if (!row.contactInformationValidated) {
    return { qualifies: false, reasons: ["CONTACT_NOT_VALIDATED"] };
  }
  if (!row.engagementReason || String(row.engagementReason).trim().length < 15) {
    return { qualifies: false, reasons: ["ENGAGEMENT_REASON_MISSING"] };
  }
  return { qualifies: true, reasons: ["OUTREACH_READY"] };
}

/**
 * Build dimensional scorecard for a research candidate.
 */
export function scoreCommercialDimensions(row = {}) {
  const status = classifyOpportunityStatus(row);
  const relevance = classifyCommercialRelevance(row);
  const accessibility = classifyCommercialAccessibility(row);
  const timing =
    row.commercialTiming ||
    mapMonthsToCommercialTiming(row.monthsToDecision, {
      active:
        status.opportunityStatus === OPPORTUNITY_STATUS.ACTIVE_SELECTION ||
        status.opportunityStatus === OPPORTUNITY_STATUS.OPEN_PROCESS_VERIFIED,
    });

  const pathway = row.evidencePathway || null;
  const relevantFirst100 = qualifiesCommerciallyRelevantFirst100({
    ...row,
    commercialTiming: timing,
  });
  const accessible = qualifiesCommerciallyAccessible({ ...row, commercialTiming: timing });
  const outreach = qualifiesOutreachReady({ ...row, commercialTiming: timing });

  return {
    hotelName: row.hotelName || null,
    physicalCountry: row.physicalCountry || row.hotelCountry || null,
    evidencePathway: pathway,
    requiresPublicRfp: false,
    commercialRelevance: relevance.commercialRelevance,
    relevanceBasis: relevance.basis,
    commercialTiming: timing,
    commercialAccessibility: accessibility.commercialAccessibility,
    accessibilityBasis: accessibility.basis,
    opportunityStatus: status.opportunityStatus,
    commerciallyRelevantFirst100: relevantFirst100.qualifies,
    commerciallyAccessible: accessible.qualifies,
    outreachReady: outreach.qualifies,
    commercialPriority: relevantFirst100.qualifies
      ? COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW
      : isCommerciallyRelevant(relevance.commercialRelevance)
        ? COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH
        : COMMERCIAL_PRIORITY.HOLD,
  };
}

export function isOpenOpportunityStatusExport(status) {
  return isOpenOpportunityStatus(status);
}
