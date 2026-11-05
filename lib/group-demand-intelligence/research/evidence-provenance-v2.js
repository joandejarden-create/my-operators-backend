/**
 * Evidence provenance contract V2 — track source ownership separately from URL/domain.
 * Multiple pages from the same organization ≠ independent publisher corroboration.
 */

import {
  resolvePublishingOrganization,
  isIndependentPublisherUrl,
  RELATIONSHIP_VERIFICATION,
  assessRelationshipVerification,
} from "./commercial-source-selection-v1.js";

export const EVIDENCE_PROVENANCE_VERSION = "gdi-evidence-provenance-v2";

export const RETRIEVAL_OUTCOME_V2 = Object.freeze({
  SUCCESS_WITH_CONTENT: "SUCCESS_WITH_CONTENT",
  SUCCESS_WITHOUT_RELEVANT_CONTENT: "SUCCESS_WITHOUT_RELEVANT_CONTENT",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  THIN_OR_UNREADABLE: "THIN_OR_UNREADABLE",
  SKIPPED_COOLED_OR_BLOCKED: "SKIPPED_COOLED_OR_BLOCKED",
});

/**
 * Build a provenance record for one retrieval / claim.
 * Technical failures must not invent negative commercial findings.
 */
export function buildEvidenceProvenanceRecord({
  url = "",
  organizationHost = "",
  organizationName = "",
  sourceFamily = null,
  originalClaimant = null,
  corroboratingOrganization = null,
  claimSupported = null,
  evidenceQuotation = null,
  retrievalOutcome = RETRIEVAL_OUTCOME_V2.SUCCESS_WITH_CONTENT,
  evidenceConfidence = null,
  relationshipMentionedOnOrganizer = false,
  confirmingUrl = null,
  independentPublisherConfirms = false,
} = {}) {
  const pub = resolvePublishingOrganization({ url, organizationHost, organizationName });
  const independent = isIndependentPublisherUrl(url, organizationHost);
  const verification = assessRelationshipVerification({
    provenanceUrl: url,
    organizationHost,
    relationshipMentionedOnOrganizer: relationshipMentionedOnOrganizer || pub.samePublisherAsOrganizer,
    confirmingUrl,
    independentPublisherConfirms: independentPublisherConfirms === true,
  });

  const technicalFailure = [
    RETRIEVAL_OUTCOME_V2.PROVIDER_ERROR,
    RETRIEVAL_OUTCOME_V2.ACCESS_RESTRICTED,
    RETRIEVAL_OUTCOME_V2.THIN_OR_UNREADABLE,
    RETRIEVAL_OUTCOME_V2.SKIPPED_COOLED_OR_BLOCKED,
  ].includes(retrievalOutcome);

  return {
    version: EVIDENCE_PROVENANCE_VERSION,
    url: url || null,
    sourceDomain: pub.sourceOwnerHost || null,
    publishingOrganization: pub.publishingOrganization || null,
    sourceOwner: pub.publishingOrganization || pub.sourceOwnerHost || null,
    sourceFamily: sourceFamily || null,
    originalClaimant: originalClaimant || (pub.samePublisherAsOrganizer ? organizationName : pub.publishingOrganization),
    corroboratingOrganization: corroboratingOrganization || null,
    claimSupported: technicalFailure ? null : claimSupported,
    evidenceQuotation: technicalFailure ? null : evidenceQuotation,
    retrievalOutcome,
    evidenceConfidence: technicalFailure ? null : evidenceConfidence,
    independentOfOrganizer: independent,
    samePublisherAsOrganizer: pub.samePublisherAsOrganizer === true,
    verificationStatus: verification.verificationStatus,
    independentlyVerified: verification.independentlyVerified === true,
    technicalFailureIsNotNegativeFinding: technicalFailure,
    countsAsIndependentPublisherCorroboration:
      independent
      && !technicalFailure
      && verification.verificationStatus === RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED,
  };
}

/**
 * Summarize whether a set of reads constitutes independent corroboration.
 */
export function summarizeIndependenceCoverage(records = []) {
  const list = Array.isArray(records) ? records : [];
  const organizerSelf = list.filter((r) => r.samePublisherAsOrganizer === true && !r.technicalFailureIsNotNegativeFinding);
  const independentReads = list.filter((r) => r.independentOfOrganizer === true && !r.technicalFailureIsNotNegativeFinding);
  const independentlyVerified = list.filter((r) => r.countsAsIndependentPublisherCorroboration === true);
  return {
    totalRecords: list.length,
    organizerSelfPageReads: organizerSelf.length,
    independentPublisherReads: independentReads.length,
    independentlyVerifiedRelationships: independentlyVerified.length,
    sameOrganizationPagesDoNotEqualIndependence: true,
    hotelMarketingDoesNotProveDmcPlacement: true,
  };
}
