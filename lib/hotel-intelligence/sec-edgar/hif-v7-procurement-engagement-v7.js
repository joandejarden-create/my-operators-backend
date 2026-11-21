/**
 * HIF V7 — Procurement status + commercial engagement route.
 * Separates commercial relevance from accessibility to new participants.
 */

export const HIF_V7_PROCUREMENT_ENGAGEMENT = "hif-v7-procurement-engagement-v7";

export const COMMERCIAL_CATEGORY = Object.freeze({
  BRAND_CONVERSION_TARGET: "BRAND_CONVERSION_TARGET",
  MANAGEMENT_CONTRACT_TARGET: "MANAGEMENT_CONTRACT_TARGET",
  HOTEL_DEVELOPMENT: "HOTEL_DEVELOPMENT",
  HOTEL_CONCESSION_OPERATING: "HOTEL_CONCESSION_OPERATING",
  HOTEL_REDEVELOPMENT: "HOTEL_REDEVELOPMENT",
});

export const PROCUREMENT_STATUS = Object.freeze({
  OPEN_FOR_SUBMISSIONS: "OPEN_FOR_SUBMISSIONS",
  ACTIVE_SELECTION: "ACTIVE_SELECTION",
  SUBMISSIONS_CLOSED: "SUBMISSIONS_CLOSED",
  AWARD_PENDING: "AWARD_PENDING",
  AWARDED: "AWARDED",
  COMPLETED: "COMPLETED",
  CANCELLED: "CANCELLED",
  STATUS_UNKNOWN: "STATUS_UNKNOWN",
});

export const ENGAGEMENT_ROUTE = Object.freeze({
  DIRECT_OWNER_APPROACH: "DIRECT_OWNER_APPROACH",
  PUBLIC_TENDER_SUBMISSION: "PUBLIC_TENDER_SUBMISSION",
  DEVELOPER_PARTNERSHIP: "DEVELOPER_PARTNERSHIP",
  OPERATOR_OR_BRAND_SELECTION: "OPERATOR_OR_BRAND_SELECTION",
  INCUMBENT_NEGOTIATION: "EXISTING_RELATIONSHIP_INCUMBENT_NEGOTIATION",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  NO_LONGER_COMMERCIALLY_ACCESSIBLE: "NO_LONGER_COMMERCIALLY_ACCESSIBLE",
});

/** Principal BCT/MCT categories only — adjacent categories tracked separately. */
export function isPrincipalBctOrMct(category) {
  return (
    category === COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET ||
    category === COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET
  );
}

export function isAccessibleToNewParticipants(procurementStatus) {
  return (
    procurementStatus === PROCUREMENT_STATUS.OPEN_FOR_SUBMISSIONS ||
    procurementStatus === PROCUREMENT_STATUS.ACTIVE_SELECTION
  );
}

/**
 * @deprecated Prefer qualifiesCommerciallyRelevantFirst100 from
 * hif-v7-commercial-dimensions-v7.js — public RFP is NOT required.
 *
 * Legacy helper retained for closed-tender accessibility checks only.
 * Commercially relevant first-100 counting must use commercial-dimensions.
 */
export function qualifiesVerifiedPriorityAForMilestone(row = {}) {
  // Permanent no-RFP policy: do not require OPEN_FOR_SUBMISSIONS.
  // Closed/awarded public tenders still fail this accessibility-oriented helper.
  if (!isPrincipalBctOrMct(row.commercialCategory)) return false;
  if (row.verifiedPriorityA !== true) return false;
  if (
    row.procurementStatus === PROCUREMENT_STATUS.AWARDED ||
    row.procurementStatus === PROCUREMENT_STATUS.COMPLETED ||
    row.procurementStatus === PROCUREMENT_STATUS.CANCELLED
  ) {
    return false;
  }
  // Private owner-led / contractual pathways may omit procurementStatus entirely
  if (!row.procurementStatus) {
    return row.brandOrManagementDecisionEvidenced === true || row.ownerLedBrandReview === true || row.ownerLedOperatorReview === true;
  }
  if (
    row.procurementStatus === PROCUREMENT_STATUS.SUBMISSIONS_CLOSED ||
    row.procurementStatus === PROCUREMENT_STATUS.AWARD_PENDING
  ) {
    // Still commercially trackable elsewhere; not "open accessible tender"
    return false;
  }
  return (
    row.procurementStatus === PROCUREMENT_STATUS.OPEN_FOR_SUBMISSIONS ||
    row.procurementStatus === PROCUREMENT_STATUS.ACTIVE_SELECTION ||
    row.procurementStatus === PROCUREMENT_STATUS.STATUS_UNKNOWN
  );
}
