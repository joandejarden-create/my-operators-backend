/**
 * HIF Track B — Historical foundation helpers:
 * current-state reconciliation classes + Track A coordination keys.
 * CUSTOMER_BLOCKED.
 */

export const HIF_TRACK_B_HISTORICAL_FOUNDATION = "hif-track-b-historical-foundation-v1";

export const CURRENT_STATUS_CLASS = Object.freeze({
  CURRENTLY_CORROBORATED: "CURRENTLY_CORROBORATED",
  HISTORICALLY_VERIFIED_CURRENT_UNKNOWN: "HISTORICALLY_VERIFIED_CURRENT_UNKNOWN",
  BRAND_CHANGED: "BRAND_CHANGED",
  OPERATOR_CHANGED: "OPERATOR_CHANGED",
  AGREEMENT_EXTENDED_OR_AMENDED: "AGREEMENT_EXTENDED_OR_AMENDED",
  SUPERSEDED: "SUPERSEDED",
  HOTEL_CLOSED_OR_GONE: "HOTEL_CLOSED_OR_GONE",
  CONFLICTED: "CONFLICTED",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
});

export const HISTORICAL_EVENT_TYPE = Object.freeze({
  BRAND_CONVERSION: "BRAND_CONVERSION",
  FRANCHISE_CHANGE: "FRANCHISE_CHANGE",
  MANAGEMENT_REPLACEMENT: "MANAGEMENT_REPLACEMENT",
  HOTEL_TRANSACTION: "HOTEL_TRANSACTION",
  OPERATOR_TERMINATION: "OPERATOR_TERMINATION",
  OWNERSHIP_CHANGE: "OWNERSHIP_CHANGE",
  REPOSITIONING: "REPOSITIONING",
  REDEVELOPMENT: "REDEVELOPMENT",
  CONTRACT_AMENDMENT: "CONTRACT_AMENDMENT",
  PORTFOLIO_MASTER_FRANCHISE: "PORTFOLIO_MASTER_FRANCHISE",
});

/** Track A owns these discovery IDs — Track B must not recreate. */
export const TRACK_A_RESERVED_DISCOVERY_IDS = Object.freeze([
  "disc_cb1_lakeville_inn_mn_2026",
  "find_cb1_lakeville_bct_brand_search_2026",
]);

export function isTrackAReservedId(id) {
  return TRACK_A_RESERVED_DISCOVERY_IDS.includes(String(id || ""));
}

/**
 * Classify reconciliation outcome from evidence flags — never invent.
 */
export function classifyCurrentStatus(row = {}) {
  if (row.conflicted === true) {
    return { currentStatusClass: CURRENT_STATUS_CLASS.CONFLICTED, basis: "CONFLICT_FLAG" };
  }
  if (row.hotelClosed === true) {
    return { currentStatusClass: CURRENT_STATUS_CLASS.HOTEL_CLOSED_OR_GONE, basis: "CLOSURE_EVIDENCE" };
  }
  if (row.superseded === true) {
    return { currentStatusClass: CURRENT_STATUS_CLASS.SUPERSEDED, basis: "SUPERSEDING_EVENT" };
  }
  if (row.agreementExtendedOrAmended === true) {
    return {
      currentStatusClass: CURRENT_STATUS_CLASS.AGREEMENT_EXTENDED_OR_AMENDED,
      basis: "AMENDMENT_OR_EXTENSION_EVIDENCE",
    };
  }
  if (row.brandChanged === true) {
    return { currentStatusClass: CURRENT_STATUS_CLASS.BRAND_CHANGED, basis: "BRAND_CHANGE_EVIDENCE" };
  }
  if (row.operatorChanged === true) {
    return { currentStatusClass: CURRENT_STATUS_CLASS.OPERATOR_CHANGED, basis: "OPERATOR_CHANGE_EVIDENCE" };
  }
  if (row.currentStatusCorroborated === true) {
    return {
      currentStatusClass: CURRENT_STATUS_CLASS.CURRENTLY_CORROBORATED,
      basis: "CORROBORATING_SOURCE",
    };
  }
  if (row.historicallyVerified === true) {
    return {
      currentStatusClass: CURRENT_STATUS_CLASS.HISTORICALLY_VERIFIED_CURRENT_UNKNOWN,
      basis: "NO_CURRENT_CORROBORATION",
    };
  }
  return { currentStatusClass: CURRENT_STATUS_CLASS.RESEARCH_REQUIRED, basis: "INSUFFICIENT_EVIDENCE" };
}

/**
 * Lack of a newer filing must NOT prove old agreement remains effective.
 */
export function lackOfNewerFilingImpliesCurrent(agreementStillEffective) {
  return false;
}

export function buildCountryTimingMatrixRow({
  country,
  hpcHotels = 0,
  contractualEvents = 0,
  franchiseResolved = 0,
  managementResolved = 0,
  unresolvedAgreementType = 0,
  currentStatusCorroborated = 0,
  researchCoverageLabel = "UNDER_RESEARCHED",
  brandedDenominatorStatus = "UNKNOWN",
} = {}) {
  return {
    country,
    hotelUniverseCoverage: {
      hpcHotels,
      brandedDenominatorStatus,
      note: "Hotel-universe coverage ≠ contractual event count",
    },
    contractualIntelligenceCoverage: {
      events: contractualEvents,
      franchiseResolved,
      managementResolved,
      unresolvedAgreementType,
    },
    currentStatusVerification: { corroborated: currentStatusCorroborated },
    researchCoverageLabel,
    worldwideEligible: true,
  };
}
