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
  CONTACT_SUBJECT_KIND,
  UNRESOLVED_REASON,
  REFRESH_STATUS,
  VERIFICATION_STATUS,
  CONTACT_TYPE,
  ROLE_CATEGORY,
  RELEVANCE_RANK,
  NEGATIVE_SCREEN,
  WATERFALL_LEVEL,
  CONFIDENCE_BAND,
  PROVIDER_ID,
} from "./vocabulary.js";

export {
  createContactTarget,
  rankContactTargetRelevance,
  inferRoleCategoryFromTitle,
  CONTACT_TARGET_VERSION,
} from "./contact-target.js";
export {
  planContactWaterfall,
  shouldStopWaterfall,
  nextWaterfallLevel,
  WATERFALL_ORDER,
  CONTACT_WATERFALL_VERSION,
} from "./contact-waterfall.js";
export {
  inferEmailPattern,
  generatePatternCandidates,
  EMAIL_PATTERN_VERSION,
} from "./email-pattern.js";
export {
  verifyEmailCandidate,
  checkEmailSyntax,
  mapMailboxResultToVerificationStatus,
  MAILBOX_CHECK_RESULT,
  SMTP_SAFETY_POLICY,
  EMAIL_VERIFICATION_INTERFACE_VERSION,
  checkDomainMx,
  createNativeMxVerificationAdapter,
  resolveVerificationAdapter,
} from "./email-verification.js";
export {
  normalizeBusinessPhone,
  phonePathPriority,
  contactTypeForHotelPhone,
  PHONE_NORMALIZE_VERSION,
} from "./phone-normalize.js";
export {
  createProviderLicenseFlags,
  createProviderResult,
  createNativeWebProvider,
  createParallelResearchProvider,
  createFuturePaidProviderStub,
  listContactProviderRegistry,
  CONTACT_PROVIDER_INTERFACE_VERSION,
} from "./provider-interface.js";
export {
  evaluateNegativeScreens,
  passesNegativeScreens,
  NEGATIVE_SCREENS_VERSION,
} from "./negative-screens.js";
export {
  buildFreshnessModel,
  ttlDaysForContactType,
  DEFAULT_TTL_DAYS,
  FRESHNESS_CHANGE_TRIGGERS,
  FRESHNESS_MODEL_VERSION,
} from "./freshness-model.js";
export {
  computeActionableContactMetrics,
  ACTIONABLE_METRICS_VERSION,
} from "./actionable-metrics.js";
export {
  buildContactIntelligenceViewModel,
  CONTACT_VIEW_MODEL_VERSION,
} from "./view-model.js";
export {
  CONTACT_V1_12_CASE_PLAN,
  CONTACT_V1_12_VERSION,
  summarize12CasePlan,
  getCi12Case,
  allianceReuseHotelIds,
} from "./cohort-12-v1.js";
export {
  resolveContactOwnerTarget,
  createContactOrganizationTarget,
  buildPersonTargetsFromHypotheses,
  OWNER_TARGET_RESOLVER_VERSION,
  OWNER_TARGET_STATUS,
  OWNER_RESOLVER_CONSOLIDATION,
} from "./owner-target-resolver.js";
export {
  createCi12Staging,
  resolveCi12StagingRoot,
  STAGING_STORE_CONSOLIDATION,
  CI12_STAGING_VERSION,
} from "./ci12-staging.js";
export {
  runCi12DryRun,
  runCi12LiveCase,
  runCi12LiveAll,
  scoreCi12Results,
  CI12_RUNNER_VERSION,
} from "./ci12-runner.js";

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
  acceptProviderContactAsOwnerContact,
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
export {
  validateOwnershipHandoffInput,
  normalizeOwnershipHandoffBudgets,
  detectHotelNameEncodingIssues,
  OWNERSHIP_HANDOFF_CONTRACT_VERSION,
  OWNERSHIP_HANDOFF_VS_HOTEL_EXPLORER,
  OWNERSHIP_HANDOFF_ALLOWED_BUDGET_KEYS,
} from "./ownership-handoff-contract.js";
export {
  isOwnershipNativeMethodRouterEnabled,
  runOwnershipNativeMethodRouter,
  OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION,
} from "./ownership-native-method-router.js";
export {
  loadCadasturDatasetForHotel,
  loadDenueDatasetForHotel,
  createDefaultRegistryDatasetDeps,
  resolveDenueEntidadCode,
  REGISTRY_DATASET_STATUS,
  OWNERSHIP_REGISTRY_DATASET_LOADER_VERSION,
} from "./ownership-registry-dataset-loaders.js";

/** Iterative ownership research loop (adapted from dzhng/deep-research; rollout OFF by default). */
export {
  runOwnershipIterativeResearchLoop,
  shouldUseIterativeOwnershipLoop,
  createSharedBudgetController,
  createModelUsdLedger,
  createOwnershipResearchState,
  generateInitialOwnershipQueryGoals,
  generateFollowUpQueryGoals,
  generateMissRecoveryQueryGoals,
  resolveNextQueryGoals,
  proposeModelAssistedFollowUpSearches,
  ingestDocumentClaims,
  shouldInvokeStructuredReader,
  deterministicFollowUpsInadequate,
  sanitizeRetrievedTextForControls,
  mapClaimToPartyRole,
  detectConflictingClaims,
  isOwnershipObjectiveSupported,
  OWNERSHIP_ITERATIVE_LOOP_VERSION,
  DEEP_RESEARCH_PROVENANCE,
  OWNERSHIP_PARTY_ROLE,
} from "./ownership-iterative-research-loop.js";

/** Country-aware ownership research strategy (Brazil V1 ladder). */
export {
  resolveOwnershipResearchStrategy,
  getOwnershipResearchStrategyToolkit,
  createBrazilOwnershipStrategyV1,
  extractLegalEntityBundle,
  extractCnpjCandidates,
  extractPrincipalCandidates,
  classifyEntityCandidateRole,
  isNonAuthoritativeCnpj,
  classifySourceTier,
  shouldScrapeForOwnership,
  assessPropertyLineage,
  computeOwnershipResearchProgress,
  concludeFromEntityEvidence,
  planScrapeFailureRecovery,
  selectUrlsForPaidScrape,
  evaluateScrapeCandidate,
  classifySourceClass,
  inferResearchGoalKind,
  buildSerpLeadFollowUpGoals,
  classifyPropertyEntityMatch,
  buildExactAddressContinuationQueries,
  stageLegalEntityFromEvidence,
  applyStagedConclusionToOwnership,
  PROPERTY_LINEAGE_STATE,
  ENTITY_CANDIDATE_ROLE,
  OWNERSHIP_CONCLUSION_STATE,
  SOURCE_TIER,
  NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE,
  RESEARCH_PROGRESS_KEYS,
  STRATEGY_VERSION,
} from "./ownership-research-strategy/index.js";
// SOURCE_CLASS / PROPERTY_ENTITY_MATCH / RESEARCH_GOAL_KIND: import from
// ownership-research-strategy/index.js directly — barrel SOURCE_CLASS is reserved for contact V2.

/** Stage-level ownership research tracing + earliest failure classification. */
export {
  OWNERSHIP_RESEARCH_FAILURE_STAGE,
  OWNERSHIP_PARTY_KIND,
  createOwnershipStageTrace,
  pushStageEvent,
  normalizeOwnershipClaimContract,
  mapRelationshipToPartyKind,
  historicalClaimFollowUpQuestions,
  inferEarliestOwnershipFailureStage,
  sanitizeOwnershipStageTraceForFixture,
} from "./ownership-research-stage-trace.js";

/** Bounded ownership follow-up (merge + question-dependent search plans; staging only). */
export {
  mergeOwnershipClaimCandidates,
  combineSavedDocumentClaims,
  buildQuestionDependentFollowUpPlan,
  prepareDevelopmentContinuationCase,
  resolveAcquisitionDirection,
  resolveOwnsDirection,
  repairOwnershipCurrentness,
  normalizeUnnamedFamilyClaim,
  resolveContextualHotelLink,
  DEVELOPMENT_CONTINUATION_CAPS,
  OWNERSHIP_FOLLOW_UP_VERSION,
} from "./ownership-follow-up-research.js";

/** Provider-neutral ownership discovery + Parallel fallback (off by default). */
export {
  discoverOwnershipSources,
  createLiveParallelDiscoverFn,
  OWNERSHIP_DISCOVERY_PROVIDERS,
  OWNERSHIP_DISCOVERY_VERSION,
} from "./ownership-source-discovery.js";
export {
  adjudicateOwnershipDiscovery,
  isSupportedCurrentOwnerClaim,
  scoreAdjudicatedClaims,
  OWNERSHIP_RETRIEVAL_ADJUDICATION_VERSION,
} from "./ownership-retrieval-adjudication.js";
export {
  shouldInvokeOwnershipDiscoveryFallback,
  mergeDiscoveryAdjudications,
  isOwnershipDiscoveryFallbackEnabled,
  getOwnershipDiscoveryFallbackProvider,
  FALLBACK_TRIGGER,
  OWNERSHIP_DISCOVERY_FALLBACK_VERSION,
} from "./ownership-discovery-fallback.js";
export { createMockDiscoveryDeps } from "./ownership-retrieval-provider-mocks.js";

/** PDL vs Surfe contact enrichment (post owner-person gate; default provider remains surfe). */
export {
  resolveContactProvider,
  DEFAULT_CONTACT_PROVIDER,
  CONTACT_PROVIDER,
  scorePdlEnrichmentResult,
  comparePdlToSavedSurfe,
  PDL_OUTCOME_BUCKET,
  PDL_CONTACT_EVAL_VERSION,
} from "./pdl-contact-enrichment-eval.js";
export {
  PDL_VS_SURFE_DQ_VERSION,
  FIELD_STATUS,
  EMAIL_AGREEMENT,
  PHONE_AGREEMENT,
  REVIEW_LABEL,
  normalizeEmail,
  normalizePhoneToE164,
  classifyEmailAgreement,
  classifyPhoneAgreement,
  correctedCostDenominators,
  hardCapReservation,
  provisionalIndependentReview,
  assertNoCanonicalWrites,
  enforceRequestCap,
} from "./pdl-vs-surfe-data-quality.js";
export {
  enrichOwnerPersonContactAfterGate,
  buildPostGatePdlInput,
  POST_GATE_CONTACT_ENRICHMENT_VERSION,
} from "./post-gate-contact-enrichment.js";
export {
  runHotelOwnershipContactWorkflow,
  HOTEL_OWNERSHIP_CONTACT_WORKFLOW_VERSION,
} from "./hotel-ownership-contact-workflow.js";
export {
  runFullResearchWorkflow,
  loadResearchCase,
  FULL_RESEARCH_WORKFLOW_VERSION,
  RESEARCH_STAGE,
  RESEARCH_OBJECTIVE,
} from "./full-research-workflow.js";
export {
  createResearchCaseStore,
  newCaseId,
  assertValidCaseId,
  RESEARCH_CASE_STORE_VERSION,
} from "./research-case-store.js";
export {
  assessHotelPhysicalIdentity,
  loadHpcHotelByRecordId,
  hotelSeedFromHpcRecord,
  IDENTITY_STATUS,
  HOTEL_PHYSICAL_IDENTITY_VERSION,
  AIRTABLE_ID_RE,
} from "./hotel-physical-identity.js";
export {
  createJournaledCaller,
  createJournaledContextProviders,
  pendingUnknownFromJournal,
  resultsFromJournal,
  completedKeysFromJournal,
  buildOperationWorkKey,
  normalizeWorkKey,
  sanitizeProviderResultForStorage,
  estimateOperationReservationCredits,
  replayStoredResult,
  isUsageRightsBlocked,
  restoreBudgetUsageFromJournal,
  mergeCumulativeBudgetUsage,
  outstandingReservationsFromJournal,
  summarizeContextDevJournalAccounting,
  reconcileContextDevCaseSpend,
  OP_STATUS,
  RESEARCH_OP_JOURNAL_VERSION,
} from "./research-operation-journal.js";
export {
  qualifyPersonOwnerAffiliation,
  parseAffiliationAssertions,
  parseEmployerPhrase,
  employerMatches,
} from "./person-owner-affiliation-gate.js";
export {
  evaluatePilotCompleteChain,
  findCompletePersonContactChain,
  nameKey as pilotScorecardNameKey,
} from "./pilot-scorecard-chain.js";
export {
  buildTrustedExecutionPolicy,
  validateResearchObjective,
  remainingAllowance,
  RESEARCH_EXECUTION_POLICY_VERSION,
  SUPPORTED_RESEARCH_OBJECTIVES,
} from "./research-execution-policy.js";
export {
  adaptHandoffResearchResult,
  deriveEvidenceContactStatuses,
  buildSupportedRelationships,
  deriveObjectiveCompletion,
  isSupportedOwnerClassification,
  hasHotelSpecificOwnershipEvidence,
  isCurrentOwnership,
  contactValueSupportedBySourceBody,
  collectPermittedSourceBodies,
  filterBlockedSources,
  isCapturedDocumentSource,
  HANDOFF_RESULT_ADAPTER_VERSION,
} from "./handoff-result-adapter.js";
export {
  currentOwnershipPassageSupport,
  ownershipPassageSupport,
  operatorOnlyBinding,
  formerOwnerAfterSaleBinding,
  ownershipNegationForParty,
  isOwnershipReporterFrame,
  contactValuePersonBound,
  analyzeContactAttribution,
  CONTACT_ROUTE,
  protectContactTokens,
  mentionsHotel,
} from "./research-evidence.js";
export {
  buildAdminResearchWorkbenchView,
  buildHumanReviewUpdate,
  resolveTrustedReviewer,
  isSupportedOwnerSponsorRelationship,
  isOperatorRelationship,
  formatContactRouteDisplay,
  channelAttributionFromEvidence,
  ADMIN_RESEARCH_WORKBENCH_VERSION,
  REVIEW_DECISIONS,
} from "./admin-research-workbench.js";

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
