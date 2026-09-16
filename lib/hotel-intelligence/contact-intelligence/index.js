/**
 * Contact Intelligence V1 — public exports.
 */

export {
  CONTACT_INTELLIGENCE_VERSION,
  CONTACT_WRITE_GUARANTEES,
  ATTRIBUTION,
  DELIVERABILITY,
  ROLE_CURRENCY,
  PROPERTY_RELEVANCE,
  FRESHNESS,
  USAGE_RIGHTS,
  CHANNEL_KIND,
  UNRESOLVED_REASON,
  REFRESH_STATUS,
} from "./vocabulary.js";

export {
  classifyEmailAttribution,
  classifyPhoneAttribution,
  classifyDeliverability,
  isGenericMailboxEmail,
  isRoleMailboxEmail,
  isNamedPersonEmail,
} from "./dimensions.js";

export {
  classifyEmailType,
  classifyEmailVerificationStatus,
  classifyPhoneType,
  preferStrongerEmail,
  preferStrongerPhone,
  personDedupeKey,
  REACHABILITY_EMAIL_TYPE,
  REACHABILITY_EMAIL_VERIFICATION,
  REACHABILITY_PHONE_TYPE,
} from "./contact-reachability.js";

export { applyPublicationRules, publishContactPackage } from "./publication-rules.js";
export {
  isPaidEnrichmentEnabled,
  isNativeResearchEnabled,
  assertPaidEnrichmentAllowed,
} from "./policy.js";
export { createChannel, createPersonContact, createHotelContactPackage } from "./contact-record.js";
export { createContactStore } from "./store.js";
export { buildOrganizationContactRoute, applyOwnerRouteReuse } from "./owner-reuse.js";
export { runNativeContactResearch } from "./native-research.js";
export {
  CONTACT_SLICE_TEN_VERSION,
  CONTACT_SLICE_TEN_HOTELS,
  buildSliceTenSeedPackage,
  listSliceTenPackages,
} from "./slice-ten.js";
export {
  createContactIntelligenceService,
  getDefaultContactIntelligenceService,
} from "./service.js";
export {
  CONTACT_BENCHMARK_100_VERSION,
  CONTACT_BENCHMARK_100_HOTELS,
  BENCHMARK_OWNER_GROUPS,
  getBenchmark100Cohort,
} from "./benchmark/cohort-100.js";
export {
  CONTACT_BENCHMARK_GATES_V1,
  runContactBenchmark100,
  evaluateBenchmarkGates,
  createDefaultBenchmarkResolver,
} from "./benchmark/metrics.js";
export { buildBenchmarkLabeledPackage } from "./benchmark/labeled-package.js";
export {
  discoverHotelContactsLive,
  LIVE_NATIVE_DISCOVERY_VERSION,
  classifyOfficialUrl,
  extractContactsFromHtml,
  serpGoogle,
} from "./live-native-discovery.js";
export { LIVE_SLICE_TEN_HOTELS, LIVE_SLICE_TEN_VERSION, LIVE_SLICE_LIMITATIONS } from "./live-slice-ten.js";
export { computeCommercialMetrics, COMMERCIAL_METRICS_VERSION } from "./commercial-metrics.js";
export { CI_FAILURE_CODE, createEmptyTrace, addCode } from "./failure-codes.js";
export { joinCensusContactByRecordId } from "./census-contact-join.js";
export { createOwnerResearchCache } from "./owner-research-cache.js";
export {
  CONTACT_BENCHMARK_REAL_VERSION,
  FIXED_COMPARISON_TEN,
  buildRealBenchmarkManifest,
  mapCensusRecordToManifestEntry,
} from "./benchmark/cohort-real-v1.2.js";
export {
  EVIDENCED_OWNER_HOTELS,
  OWNER_PERSON_SUBSET_VERSION,
  groupHotelsByOwner,
} from "./evidenced-owner-subset-v1.3.js";
export {
  verifyOwnershipPath,
  discoverOwnerPersonPath,
  buildOwnershipConfidenceLayers,
  OWNER_PERSON_DISCOVERY_VERSION,
} from "./owner-person-discovery.js";
export {
  extractLeadershipPeople,
  classifyContactPageMechanism,
  classifyLeadershipCategory,
  LEADERSHIP_CATEGORY,
  LEADERSHIP_EXTRACTION_VERSION,
} from "./leadership-extraction.js";
export {
  GENERALIZATION_TEN_HOTELS,
  GENERALIZATION_TEN_VERSION,
  summarizeSelection,
} from "./generalization-ten-v1.4.js";
export {
  PERSON_EMAIL_DISCOVERY_VERSION,
  getEmailVerificationStatus,
  generateInferredEmailCandidates,
} from "./person-email-discovery.js";
export {
  INVESTOR_DOC_DISCOVERY_VERSION,
  buildInvestorSearchQueries,
  rankInvestorDocumentUrls,
  extractAttributedPersonEmailsFromText,
  classifyAffiliationVsTarget,
} from "./investor-document-discovery.js";
export {
  classifyFetchFailure,
  FETCH_FAILURE_CLASS,
  isMarriottPropertyUrl,
} from "./fetch-failure-taxonomy.js";
export { fetchContactResearchPage } from "./research-fetch-v1.3.js";
export { migrateContactStorePackages, STORE_VERSION } from "./store.js";
export { isPlausibleOwnerPage } from "./live-native-discovery.js";
export {
  PROVIDER_SUBMISSION_GATE_VERSION,
  SUBMISSION_DECISION,
  SUBMISSION_REJECT_REASON,
  validateProviderSubmissionInput,
  validateLinkedInIdentifier,
  linkedInNameTokenAgreement,
  classifyInvalidInputEvaluation,
} from "./provider-submission-gate.js";
export {
  OWNER_PERSON_ENRICHMENT_GATE_VERSION,
  OWNER_PERSON_ENRICHMENT_WORKFLOW,
  OWNER_PERSON_SEARCH_WORKFLOW,
  OWNER_PERSON_REJECT,
  AFFILIATION_SOURCE_CLASS,
  validateOwnerPersonEnrichmentSubmission,
  evaluateOwnerPersonReturnedContact,
  isAffiliationCorroborated,
  isRoleRelevantForOwnerContact,
  sponsorQualifiesWithoutDeed,
} from "./owner-person-enrichment-gate.js";
export {
  gateProviderCandidates,
  submitFullEnrichWorkEmailsGated,
} from "./fullenrich-gated-submit.js";
export {
  researchHotelOwnershipContactPath,
  buildEnrichmentSubjectsFromResearch,
  assessOrgDomainMatch,
  OWNERSHIP_CONTACT_HANDOFF_VERSION,
} from "./ownership-contact-research-handoff.js";

/** Contact Resolution V2 — owner-level resolver (staging). */
export {
  resolve_owner_contacts,
  CONTACT_RESOLUTION_V2,
  EMAIL_STATUS,
  PHONE_TYPE,
  CONTACT_STRENGTH,
  RESOLUTION_STATUS,
  SOURCE_CLASS,
  resolveOwnerDomain,
  planWebhoundEscalation,
  needsWebhoundEscalation,
  COUNTRY_CONTACT_METHODS,
} from "./owner-contact-resolution-v2/index.js";

/** Canonical contact merge policy (dry-run default; no auto production writes). */
export {
  CANONICAL_MERGE_POLICY_VERSION,
  CANONICAL_FIELD_KIND,
  FIELD_SOURCE_CLASS,
  FIELD_SOURCE_PRECEDENCE,
  FIELD_MERGE_ACTION,
  MERGE_WRITE_MODE,
  MERGE_SAFETY_TIER,
  REUSE_OUTCOME,
  HOTEL_FEEDBACK_STATE,
  evaluateContactFieldMerge,
  createCanonicalField,
  createCanonicalPerson,
  createOpportunityRelationship,
  createCanonicalPersonRegistry,
  simulateReachabilityRowMerge,
  summarizeMergeDecisions,
  extendCoverageWithMergeMetrics,
  applyHotelFeedback,
  sourceClassPrecedence,
  valuesEquivalent,
} from "./canonical-merge-policy.js";

export {
  REVIEW_APPLY_VERSION,
  PROPOSAL_STATUS,
  APPROVAL_PATH,
  APPLY_RESULT,
  createReviewApplyStore,
  generateMergeProposalsFromReachability,
  approveCanonicalContactMergeProposal,
  rejectCanonicalContactMergeProposal,
  applyApprovedCanonicalContactMerge,
  rollbackCanonicalContactMerge,
  validateProposalForApply,
  evaluatePhoneAutoApplyEligibility,
  resolveCanonicalReuseAfterApply,
  computePhonePilotRates,
  hydrateRegistryFromReachability,
  buildProposalSummary,
  fieldStateFingerprint,
  createEmptyPhonePilotMetrics,
} from "./canonical-merge-review-apply.js";
