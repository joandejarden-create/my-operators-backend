/**
 * Recommendation qualification — customer-safe claim classes.
 * Never convert unsupported hypotheses into customer recommendations.
 */

export const AVA_RECOMMENDATION_CLASSES = Object.freeze({
  VERIFIED_ISSUE: "VERIFIED_ISSUE",
  EVIDENCE_SUPPORTED_INVESTIGATION: "EVIDENCE_SUPPORTED_INVESTIGATION",
  OPTIONAL_COMMERCIAL_OPPORTUNITY: "OPTIONAL_COMMERCIAL_OPPORTUNITY",
  RESEARCH_ONLY: "RESEARCH_ONLY",
  REJECTED: "REJECTED",
});

/** Map qualification class → PDF claimType used by the report renderer. */
export function qualificationToPdfClaimType(cls) {
  switch (String(cls || "")) {
    case AVA_RECOMMENDATION_CLASSES.VERIFIED_ISSUE:
      return "VERIFIED_ISSUE";
    case AVA_RECOMMENDATION_CLASSES.EVIDENCE_SUPPORTED_INVESTIGATION:
      return "RECOMMENDED_INVESTIGATION";
    case AVA_RECOMMENDATION_CLASSES.OPTIONAL_COMMERCIAL_OPPORTUNITY:
      return "OPTIONAL_COMMERCIAL_OPPORTUNITY";
    case AVA_RECOMMENDATION_CLASSES.RESEARCH_ONLY:
    case AVA_RECOMMENDATION_CLASSES.REJECTED:
      return null;
    default:
      return "REQUIRES_HOTEL_CONFIRMATION";
  }
}

export function isCustomerFacingRecommendation(cls) {
  return (
    qualificationToPdfClaimType(cls) != null &&
    cls !== AVA_RECOMMENDATION_CLASSES.RESEARCH_ONLY &&
    cls !== AVA_RECOMMENDATION_CLASSES.REJECTED
  );
}

export const AVA_ACTION_CUSTOMER_FIELDS = Object.freeze([
  "actionId",
  "headline",
  "priority",
  "whatWeFound",
  "whyItMatters",
  "supportingEvidence",
  "recommendedSteps",
  "accountableOwner",
  "supportingParties",
  "suggestedTimeframe",
  "expectedOutcome",
  "completionCriteria",
  "technicalDetails",
  "requiresHotelApproval",
  "implementationStatus",
  "claimType",
]);
