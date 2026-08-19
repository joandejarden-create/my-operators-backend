/**
 * Dealality AI Visibility — public API.
 */

export * from "./config.js";
export { normalizeProviderResponse } from "./normalize-response.js";
export {
  normalizeMatchKey,
  decoratedNameKeys,
  isBlockedBareParentMention,
  buildEntityAliasIndex,
  resolveEntityMention,
  findEntitySpans,
  stripMarkdownNoiseForEntityMatch,
  CONTEXTUAL_ALIAS_RULES,
  contextualAliasAccepted,
  applyContextualAliasSpans,
  RESOLVER_VERSION,
} from "./normalize-entities.js";
export { extractMentions, unresolvedMention } from "./extract-mentions.js";
export { extractCitations, parseDomain, matchFirstPartyDomain } from "./extract-citations.js";
export { associateCitationsToEntities } from "./citation-association.js";
export {
  filterUnresolvedCandidates,
  classifyUnresolvedNoise,
} from "./unresolved-candidate-filter.js";
export {
  loadRuntimeAliasOverlay,
  applyRuntimeAliasOverlay,
  RUNTIME_ALIAS_OVERLAY_VERSION,
} from "./runtime-alias-overlay.js";
export {
  classifyMentionRoleV3,
  assignFirstRecommendationAcrossMentionsV3,
  detectRankMarker,
  detectResponseSections,
  detectBulletLine,
  detectOrderedListContext,
  RECOMMENDATION_CLASSIFIER_VERSION as RECOMMENDATION_CLASSIFIER_VERSION_V3,
} from "./recommendation-classifier-v3.js";
export {
  classifyMentionRoleV4,
  assignFirstRecommendationAcrossMentionsV4,
  decideRecommendationRoleFromEvidence as decideRecommendationRoleFromEvidenceV4,
  classifyEntityFromMentionSpans as classifyEntityFromMentionSpansV4,
  questionStatusFromRecommendationRole as questionStatusFromRecommendationRoleV4,
  RECOMMENDATION_CLASSIFIER_VERSION as RECOMMENDATION_CLASSIFIER_VERSION_V4,
} from "./recommendation-classifier-v4.js";
export {
  extractEntityLocalEvidence as extractEntityLocalEvidenceV4,
  aggregateEntityEvidence as aggregateEntityEvidenceV4,
  classifyHeadingSemanticType as classifyHeadingSemanticTypeV4,
  RECOMMENDATION_EVIDENCE_VERSION as RECOMMENDATION_EVIDENCE_VERSION_V4,
} from "./recommendation-evidence-v4.js";
export {
  classifyMentionRoleV4_1,
  assignFirstRecommendationAcrossMentionsV4_1,
  decideRecommendationRoleFromEvidence,
  classifyEntityFromMentionSpans,
  questionStatusFromRecommendationRole,
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "./recommendation-classifier-v4_1.js";
export {
  extractEntityLocalEvidence,
  aggregateEntityEvidence,
  buildTypedSections,
  classifySectionType,
  classifyCatalogSemantics,
  evaluateSectionPropagation,
  classifyHeadingSemanticType,
  RECOMMENDATION_EVIDENCE_VERSION,
} from "./recommendation-evidence-v4_1.js";
// Phase 2B compat re-exports
export {
  classifyMentionRoleV2,
  assignFirstRecommendationAcrossMentions,
} from "./recommendation-classifier-v3.js";
export { assessMetricReadiness } from "./metric-readiness.js";
export * from "./signal-architecture/index.js";
export {
  normalizePromptGeography,
  filterObservationsByGeography,
  calculateVisibilityMetrics,
  resolveCountryGeography,
  buildPeerSetDescriptor,
  auditCanonicalGeographySources,
  GEOGRAPHY_MODEL_VERSION,
  CALA_COMMERCIAL_REGION,
  HEADLINE_REGION_METRIC_COHORT_RULE,
} from "./geography.js";
export {
  resolveCommercialRegionForCountry,
  validateCountryRegionPair,
  auditCommercialGeography,
  COMMERCIAL_GEOGRAPHY_VERSION,
  COMMERCIAL_REGIONS,
  EUROPE_COUNTRIES,
  NORTH_AMERICA_COUNTRIES,
} from "./commercial-geography.js";
export {
  validatePromptRow,
  validatePromptSeedSet,
  PROMPT_VALIDATION_VERSION,
} from "./prompt-validation.js";
export {
  resolvePromptUpsertAction,
  suggestNextPromptVersion,
  PROMPT_VERSIONING_VERSION,
} from "./prompt-versioning.js";
export {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  diffBrandPeerSetVersions,
  PEER_SET_CONFIG_VERSION,
  PEER_SET_ID_V1,
  PEER_SET_ID_V2,
  PEER_SET_ID_V3,
} from "./peer-sets.js";
export {
  loadGovernedAiVisibilityPrompts,
  loadGovernedAiVisibilityPromptsFromFixture,
  PROMPT_LOADER_VERSION,
} from "./load-prompts.js";
export { buildPromptCohort, PROMPT_COHORT_VERSION } from "./prompt-cohort.js";
export {
  AI_VISIBILITY_PROMPTS_TABLE,
  AI_VISIBILITY_OPPORTUNITIES_TABLE,
} from "./airtable-schema-proposal.js";
export {
  createExecutionBatch,
  createBatchId,
  deriveBatchStatus,
  deriveBatchHealth,
  isRetryableProviderError,
  isAuthProviderError,
  findDuplicateRecentBatch,
  buildDuplicateKey,
  hashPromptText,
  BATCH_STATUSES,
  BATCH_HEALTH,
  EXECUTION_BATCH_VERSION,
} from "./execution-batch.js";
export {
  planAiVisibilityCohort,
  executeAiVisibilityCohort,
  validatePeerSetAgainstIndex,
  EXECUTE_ENGINE_VERSION,
  resolveEstimatedUsdPerCall,
  resolveMaxBatchCostUsd,
} from "./execute-cohort.js";
export {
  computeAiPresenceRate,
  computeRecommendationShare,
  computeFirstRecommendationRate,
  computeQuestionsWon,
  computeQuestionsMissing,
  computeCompetitivePosition,
  computeCitationRate,
  buildObservationFromExtractions,
} from "./metrics.js";
export { assembleEvidenceRecord, metricEvidenceTrace } from "./evidence.js";
export { createAiVisibilityStore, createFileStore } from "./storage/index.js";
export { runVisibilityPrompt, ProviderError } from "./providers/index.js";

export {
  buildAiVisibilityEntityIndex,
  buildLiveAiVisibilityEntityIndex,
  buildFixtureAiVisibilityEntityIndex,
  ENTITY_INDEX_VERSION,
} from "./entity-index.js";
export { loadLiveBrandEntities, selectBrandsByCanonicalNames } from "./load-brands-live.js";
export {
  loadLiveOperatorEntities,
  selectOperatorsByCanonicalNames,
  parseOperatorAliases,
} from "./load-operators-live.js";
export {
  classifyMentionRole,
  harvestUnresolvedProperPhrases,
  harvestUnresolvedWithFilterStats,
} from "./mention-classification.js";

/* Phase 2F — company-scoped authorization (read layer; monitoring stays central) */
export {
  normalizeAiVisibilityViewerContext,
  buildFixtureViewerContext,
  VIEWER_CONTEXT_VERSION,
} from "./viewer-context.js";
export {
  normalizeAiVisibilitySubject,
  SUBJECT_TYPES,
  SUBJECT_CONTEXT_VERSION,
} from "./subject-context.js";
export {
  ACCESS_DEPTH,
  COMPARATIVE_SAFE_METRIC_FIELDS,
  COMPARATIVE_BLOCKED_FIELDS,
  ACCESS_DEPTH_VERSION,
} from "./access-depth.js";
export {
  ACCESS_REASON,
  toClientAccessError,
  ACCESS_REASON_VERSION,
} from "./access-reason-codes.js";
export {
  resolveEntitledBrands,
  resolveEntitledOperators,
  resolveEntitledDeals,
  resolveBrandPortfolio,
  buildFixtureEntitlementGraph,
  emptyEntitlementGraph,
  isPeerComparativeEntity,
  MAP_AI_VISIBILITY_ENTITLEMENT,
  ENTITLEMENT_VERSION,
} from "./entitlements.js";
export {
  resolveAiIntelligenceAccess,
  buildAiIntelligenceQueryContext,
  AUTHORIZATION_VERSION,
} from "./authorization.js";
export {
  getAuthorizedVisibilityOverview,
  getAuthorizedEvidence,
  toBenchmarkSafeEntityView,
  applyAccessDepthToMetrics,
  AUTHORIZED_READ_VERSION,
} from "./authorized-reads.js";
export {
  filterEvidenceByAccessDepth,
  COMPARATIVE_EVIDENCE_LIMIT,
  EVIDENCE_ACCESS_VERSION,
} from "./evidence-access.js";
export {
  auditOwnerAiRecommendationContext,
  OWNER_AI_RECOMMENDATION_CONTEXT_FIELDS,
  OWNER_CONTEXT_AUDIT_VERSION,
} from "./owner-context-audit.js";
export {
  AVAILABILITY,
  classifyMetricAvailability,
  normalizeMetricKey,
  AVAILABILITY_VERSION,
} from "./availability-states.js";
export {
  mapRecommendationRoleToBrandStatus,
  buildEvidenceDescriptors,
  ROLE_COPY_VERSION,
} from "./role-copy.js";
export {
  parseGeographyQuery,
  parseLanguageQuery,
  listAvailableAiVisibilityLanguages,
  resolveMonitoringLanguageForRead,
  getBrandPortfolioPayload,
  getBrandOverviewPayload,
  getBrandTrendPayload,
  getBrandQuestionsPayload,
  getBrandCompetitorsPayload,
  getBrandSourcesPayload,
  getBrandEvidencePayload,
  resolveBrandGeographyMonitoringState,
  entityInMonitoredUniverse,
  MONITORING_STATE,
  HEADLINE_GEOGRAPHIES,
  HEADLINE_GEOGRAPHY_ORDER,
  headlineGeographyByKey,

  BRAND_READ_SERVICE_VERSION,
} from "./brand-read-service.js";
export {
  getBrandExecutiveSummaryPayload,
  BRAND_EXECUTIVE_SUMMARY_VERSION,
} from "./brand-executive-summary.js";
export { loadBrandViewerEntitlements } from "./load-brand-entitlements.js";
export { resolveAiVisibilityStoreRoot } from "./storage/index.js";

/* Phase 3A.6 — language first-class dimension */
export {
  AI_VISIBILITY_LANGUAGE_VERSION,
  AI_VISIBILITY_LANGUAGES,
  AI_VISIBILITY_LANGUAGE_DISPLAY,
  NON_COMPARABLE_LANGUAGE,
  normalizeLanguage,
  isSupportedAiVisibilityLanguage,
  getLanguageDisplayLabel,
  requireSupportedLanguage,
  resolveRecordLanguage,
  recordMatchesLanguage,
  resolveReadLanguage,
  listLanguagesFromMonitoringRecords,
  buildLanguageFilterContract,
} from "./language-dimension.js";
export {
  SEMANTIC_PAIR_VERSION,
  validateSemanticPairMembers,
  suggestSemanticPairId,
  promptExecutionIdentity,
} from "./semantic-pair.js";
export {
  TREND_COMPARABILITY_VERSION,
  buildTrendComparabilityKey,
  compareTrendObservations,
} from "./trend-comparability.js";

/* Phase 3A.7 — showcase portfolio / peer v2 / decision eligibility */
export {
  normalizeParentCompany,
  parentsMatchCanonical,
  CANONICAL_PARENT_COMPANIES,
  PARENT_COMPANY_NORMALIZE_VERSION,
} from "./parent-company-normalize.js";
export {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
  getShowcasePortfolioBrandIds,
  assertShowcasePortfolioParentPurity,
  listShowcaseCompanyKeys,
  SHOWCASE_COMPANIES_CONFIG_ID,
  SHOWCASE_COMPANIES_VERSION,
} from "./brand-ai-showcase-companies.js";
export {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  listEligibilityForBrand,
  summarizeIntentCompetitiveDensity,
  eligibilityIsLanguageNeutral,
  ELIGIBILITY,
  SHOWCASE_DECISION_TERRITORIES,
  ACTIVE_SHOWCASE_DECISION_TERRITORIES,
  listEligibilityByTerritory,
  DECISION_ELIGIBILITY_CONFIG_ID,
  DECISION_ELIGIBILITY_VERSION,
} from "./brand-decision-eligibility.js";
export {
  ACTIVE_SHOWCASE_INTENTS,
  DEFERRED_SHOWCASE_INTENTS,
  SHOWCASE_INTENT_DEFINITIONS,
  isActiveShowcaseIntent,
  getShowcaseIntentDefinition,
  resolveEligibilityTerritoryKey,
  SHOWCASE_INTENT_GOVERNANCE_VERSION,
} from "./showcase-intents.js";
export {
  buildWave1ShowcaseDryRunPlan,
  buildWave1ExecutionFingerprint,
  WAVE1_SHOWCASE_PLAN_VERSION,
  WAVE1_BASELINE_SERIES_ID,
  WAVE1_PEER_SET_ID,
  WAVE1_COST_EVIDENCE,
  WAVE1_RETRY_POLICY,
} from "./wave1-showcase-plan.js";

export {
  executeWave1Showcase,
  preflightWave1LiveEnv,
  WAVE1_ORCHESTRATOR_VERSION,
  WAVE1_PROMPT_LIBRARY_VERSION,
} from "./wave1-showcase-orchestrator.js";

export {
  preflightProviderCredentials,
  resolveGeminiCredential,
  resolvePerplexityCredential,
  resolveClaudeCredential,
} from "./provider-credentials.js";
export {
  MONITORING_RUN_PURPOSE,
  isBaselineMonitoringRun,
  isValidationMonitoringRun,
  monitoringRunTypeSupported,
} from "./monitoring-run-purpose.js";
export {
  executeProviderValidation,
  executeMultiProviderValidation,
  preflightValidationLiveEnv,
  PROVIDER_VALIDATION_ORCHESTRATOR_VERSION,
} from "./provider-validation-orchestrator.js";
export { buildPhase3b2AuditReport, summarizeProviderValidationStats, assessGoNoGo } from "./provider-validation-audit.js";

export { evaluateGlobalEnActivationGate } from "./wave1-activation-gate.js";
export { WAVE1_HARD_CAP_USD, estimateWave1CallCostUsd } from "./wave1-cost.js";
export { buildWave1PostWaveAudit } from "./wave1-post-wave-audit.js";
export {
  METHODOLOGICAL_TERM,
  replaceEligibilityTerminology,
  hasMethodologicalSuitability,
  classifySuitabilityOccurrence,
  ELIGIBILITY_TERMINOLOGY_VERSION,
} from "./eligibility-terminology.js";
export {
  deriveBrandArchetypes,
  ARCHETYPE,
  BRAND_ARCHETYPE_VERSION,
} from "./brand-archetypes.js";
export {
  loadGeographyEligibilityConfig,
  getBrandGeographyEligibility,
  isSafeForMexicoShowcase,
  GEOGRAPHY_ELIGIBILITY_CONFIG_ID,
  GEOGRAPHY_SCOPES,
} from "./brand-geography-eligibility.js";
