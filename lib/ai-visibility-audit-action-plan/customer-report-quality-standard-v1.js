/**
 * AVA Customer Report Quality Standard V1 — machine-readable contract.
 * Human-readable companion: reports/.../AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1.md
 */
export const AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1 =
  "AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1";

export const AVA_CUSTOMER_REPORT_QUALITY_RULES_V1 = Object.freeze({
  contract: AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1,
  version: 1,
  appliesTo: "EVERY_AVA_CUSTOMER_REPORT",
  independentOf: [
    "brand",
    "location",
    "propertySize",
    "independentVsBranded",
    "findingCount",
    "researchProvider",
    "sourceAvailability",
    "pdfLength",
  ],
  requirements: {
    researchToFinding: "Evidence-backed only; no invented corrections",
    findingClassification: [
      "VERIFIED_CORRECTION",
      "VERIFIED_CONTENT_IMPROVEMENT",
      "DISTRIBUTION_OPPORTUNITY",
      "VERIFIED_AND_CONSISTENT",
      "PARTIALLY_ASSESSED",
      "RESEARCH_FOLLOW_UP",
    ],
    canonicalActionRegister: true,
    duplicateDetection: "SEMANTIC_WORKSTREAM",
    sharedActionRecordsAcrossSections: true,
    noObjectSerializationArtifacts: true,
    noTruncatedNarratives: true,
    evidenceRequiredForMaterialActions: true,
    googleTravelNotGbp: true,
    listingVsContentSeparated: true,
    noSpeculativeRevenueClaims: true,
    digitalAuthorityRequiresSourceUrl: true,
  },
  regressionFixtures: [
    "TRUNCATED_EXECUTIVE_TAKEAWAY",
    "OBJECT_OBJECT_PAGE_LABEL",
    "DUPLICATE_MEETING_SPACE_ACTIONS",
    "GENERIC_REPEATED_PLATFORM_ACTIONS",
    "MISSING_DIGITAL_AUTHORITY_EVIDENCE",
    "INCORRECT_HOTEL_SOURCE_URL",
    "ACCESS_RESTRICTED_LABELED_VERIFIED_CONTENT",
    "HIGH_PRIORITY_FINDING_OMITTED",
  ],
});
