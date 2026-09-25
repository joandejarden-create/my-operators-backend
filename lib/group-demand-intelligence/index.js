/**
 * Group Demand Intelligence — public lib surface.
 */

export {
  GDI_FEATURE_FLAG,
  GDI_PRODUCT_VERSION,
  isGroupDemandIntelligenceEnabled,
  isGroupDemandIntelligenceLocalDev,
  isGroupDemandIntelligencePilotReadAllowed,
  getGroupDemandIntelligenceFlagState,
} from "./feature-flag.js";

export {
  NEW_OPPORTUNITIES_V1,
  DEMAND_SIGNAL_TYPE,
  DEMAND_SIGNAL_TYPE_LABEL,
  DEMAND_FAMILY,
  DEMAND_FAMILY_LABEL,
  DEMAND_LANE,
  NEWNESS_STATUS,
  RECURRENCE_CLASS,
  classifyDemandSignalType,
  demandSignalTypeLabel,
  demandFamilyForSignalType,
  enrichOpportunityDemandSignal,
  buildDemandLaneSearchTasks,
  classifyPublicTrigger,
  PUBLIC_TRIGGER_CLASS,
  createOrgResearchReuseCache,
  buildDemandGeneratorStub,
  preferCommercialFit,
  assertNoCrmInference,
} from "./demand-signal-types.js";

export {
  CLAIM_KIND,
  CLAIM_KIND_LABEL,
  DEMAND_STATUS,
  BOOKING_WINDOW,
  BOOKING_WINDOW_LABEL,
  PRIORITY,
  DEMAND_TERRITORY_FIT,
  DEMAND_TERRITORY_FIT_LABEL,
  SOURCING_STATUS,
  SOURCING_STATUS_LABEL,
  INCREMENTAL_VALUE_STATUS,
  INCREMENTAL_VALUE_STATUS_LABEL,
  COMPETITOR_CLASS,
  COMPETITOR_CLASS_LABEL,
  HOTEL_FAMILIARITY,
  HOTEL_COMMERCIAL_STATUS,
  HOTEL_VALUE_FEEDBACK,
  SEGMENTS,
  FEEDBACK_QUALITY,
  SALES_OUTCOME,
  GDI_WEBHOUND_HARD_CAP_USD,
  OPPORTUNITY_TYPE,
  OPPORTUNITY_TYPE_LABEL,
  VENUE_SOURCING_STATUS,
  VENUE_SOURCING_STATUS_LABEL,
  EVENT_LOCATION_STATUS,
  EVENT_LOCATION_STATUS_LABEL,
  ROOM_DEMAND_STATUS,
  ROOM_DEMAND_STATUS_LABEL,
  FUTURE_CYCLE_STATE,
  FUTURE_CYCLE_STATE_LABEL,
  CONTACT_QUALITY,
  CONTACT_QUALITY_LABEL,
  REACTIVATION_SIGNAL,
  REACTIVATION_SIGNAL_LABEL,
  OPPORTUNITY_QUALIFICATION,
  OPPORTUNITY_QUALIFICATION_LABEL,
  QUALIFICATION_FAILURE_REASON,
  QUALIFICATION_FAILURE_REASON_LABEL,
} from "./claim-types.js";

export {
  computeHotelFitScore,
  computeEvidenceConfidence,
  classifyPriority,
  reconstructScoreAudit,
  buildHotelFitExplanation,
  buildEvidenceConfidenceExplanation,
  DEFAULT_HOTEL_FIT_WEIGHTS,
  HOTEL_FIT_COMPONENT_LABELS,
  EVIDENCE_CONFIDENCE_TOOLTIP,
} from "./scoring.js";

export {
  PILOT_HOTEL_ID,
  buildBethesdaMarriottProfileFromExistingKnowledge,
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
  isHotelOnboardedForGdi,
} from "./hotel-profile.js";

export {
  runGroupDemandResearch,
  getLatestOpportunities,
} from "./research-orchestrator.js";

export {
  buildWeeklyBrief,
  filterSalespersonView,
  buildOpportunity,
  buildPilotMetrics,
} from "./opportunity-factory.js";

export {
  isGdiTestOrFixtureOpportunity,
  filterCustomerFacingOpportunities,
  markAsTestOpportunity,
} from "./customer-visibility.js";

export {
  applyRadFeedbackEnrichmentPass,
  enrichOpportunityForRadFeedback,
  classifyDemandTerritoryFit,
  classifyCompetitors,
} from "./rad-feedback-enrichment.js";

export {
  applyDmvExpansionPass,
  buildDmvExpansionOpportunities,
  listRejectedDmvExpansionCandidates,
} from "./dmv-expansion-pass.js";

export {
  applyQualificationPrecisionPass,
} from "./qualification-precision-pass.js";

export {
  applyCommercialQaOverride,
  finalizeCommercialQaOpportunity,
  clampPriorityAfterCommercialQa,
  COMMERCIAL_QA_PASS_ID,
  MEDIUM_CONTACT_VS_QUALIFICATION_NOTES,
} from "./commercial-qa-overrides-v1.js";

export {
  enrichQualificationPrecision,
  classifyVenueSourcingStatus,
  classifyEventLocation,
  classifyRoomDemand,
  classifyContactQuality,
  classifyReactivationSignal,
  deriveOpportunityType,
  computeOpportunityQualification,
  runHighPriorityQualityGate,
  buildHotelOpportunityThesis,
} from "./qualification-precision.js";

export {
  FUTURE_CYCLE_EVIDENCE_STATE,
  FUTURE_CYCLE_EVIDENCE_STATE_LABEL,
  detectFutureCycleEvidence,
  shouldPreservePlacedVenueAsFutureCycleWatch,
} from "./future-cycle-evidence.js";

export {
  RESEARCH_PROVIDER,
  CANONICAL_VENUE_SOURCING,
  normalizeResearchProviderResult,
  parseNativeResearchResult,
  parseParallelResearchResult,
  parseWebhoundResearchResult,
} from "./provider-normalization.js";

export {
  PARALLEL_ESCALATION_TRIGGER,
  PARALLEL_ESCALATION_POLICY,
  shouldEscalateToParallel,
  resolveResearchProviderPath,
} from "./parallel-fallback-gate.js";

export {
  CAPTURE_CAPACITY_STATE,
  CAPTURE_CAPACITY_STATE_LABEL,
  calculateHotelGroupCaptureCapacity,
  resolveRealisticGroupCaptureCapacity,
  estimateCaptureShareRooms,
  hotelContextFromProfile,
} from "./hotel-capture-capacity.js";

export {
  RESEARCH_IDENTITY_KIND,
  normalizeOpportunityContactEvidence,
  classifyResearchIdentityKind,
  bridgeResearchContactsToWhoCandidates,
  collectOpportunityResearchContacts,
} from "./contact-candidate/opportunity-contact-evidence.js";

export {
  gradeContact,
  enrichContactRecord,
  shouldEnrichContact,
  classifyTargetRoleMatch,
  scoreContactConfidence,
  buildWhyThisContact,
  hasNamedPerson,
  hasActionableIdentity,
} from "./contact-resolution.js";

export { applyContactResolutionPass } from "./contact-resolution-pass.js";
export { OFFICIAL_CONTACT_ENRICHMENTS_V1 } from "./contact-official-enrichments-v1.js";

export {
  calculateGdiContactCoverage,
  buildGdiContactFunnel,
  classifyReachabilityNeed,
  buildReachabilityCohortFromQueue,
  mapOutcomeToFieldMerge,
  simulateGradeAfterAcceptedFields,
  inferDomainFromOfficialSourceUrl,
  REACHABILITY_NEED,
  FIELD_MERGE_DECISION,
} from "./contact-coverage.js";

export {
  GDI_CONTACT_ROLE,
  ROLE_RELEVANCE,
  EVENT_FAMILY,
  CANDIDATE_STATUS,
  CANDIDATE_CONFIDENCE,
  discoverContactCandidates,
  discoverContactCandidatesForOpportunities,
  scoreContactCandidate,
  annotateCandidate,
  classifyEventFamily,
  classifyGdiContactRole,
  buildCandidateSearchQueries,
  CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
  NO_PROBABLE,
  PRIMARY_KIND,
} from "./contact-candidate/index.js";

export {
  listRegisteredHotels,
  listGdiSelectableHotels,
  loadHotelProfile,
  loadOpportunities,
  loadFeedback,
  saveFeedbackItem,
  listResearchRuns,
  loadResearchRun,
  loadLatestSummary,
  getGdiDataRoot,
  clearGdiRepositoryReadCache,
} from "./repository.js";

export {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
  upsertSingleOpportunity,
  getGdiOpportunityPersistenceMode,
  getGdiOpportunitiesAirtableBaseId,
  isGdiOpportunityAirtableConfigured,
} from "./opportunity-persistence.js";

export {
  toOpportunityListDto,
  mapOpportunitiesToListDto,
  GDI_OPPORTUNITY_LIST_SCHEMA,
} from "./opportunity-list-dto.js";

export {
  LIVE_COMMERCIAL_QUALITY_V1,
  DATE_GRANULARITY,
  NEXT_CYCLE_STATUS,
  ROOM_DEMAND_LIVE,
  HOTEL_VALIDATION_REASON,
  HOTEL_VALIDATION_REASON_LABEL,
  applyLiveCommercialQuality,
  buildRelatedOpportunityGroups,
  classifyBethesdaStyleCorrections,
  reconcileSuggestedAction,
  normalizeDateModel,
  normalizeFutureCycle,
  formatCustomerDate,
  unknownDisplay,
  toExportRow,
  buildExportCsv,
  rowsToCsv,
  customerCsvContentDisposition,
  buildCustomerCsvFilename,
  GDI_CUSTOMER_CSV_HEADERS,
  toCustomerExportRow,
  buildCustomerExportCsv,
} from "./live-commercial-quality-v1.js";

export { listGdiResearchMethods } from "./research-methods.js";

export {
  DISCOVERY_MODE,
  DISCOVERY_CADENCE,
  OPEN_UNIVERSE_PROMOTION_FLOW,
  shouldRunDiscoveryMode,
  listDiscoveryModes,
} from "./discovery-modes.js";

export {
  OPEN_UNIVERSE_QUERY_GENERATOR_VERSION,
  SECTOR_THEME_CATALOG,
  inferOpenUniverseThemes,
  generateOpenUniverseQueries,
  generateEventSeriesExpansionQueries,
} from "./open-universe-query-generator.js";

export {
  scoreOfficialSource,
  rankOrganicByOfficialSource,
  isDeprioritizedSource,
  SOURCE_RANK_VERSION,
} from "./official-source-ranking.js";

export {
  extractEventSeriesFromCalendarPage,
  cyclesToDiscoveryCandidates,
  SERIES_RECURRENCE,
  DISCOVERY_LANE,
  CALENDAR_SERIES_EXTRACTOR_VERSION,
} from "./calendar-series-extractor.js";

export {
  loadEventSeriesGraph,
  upsertEventSeriesGraph,
  EVENT_SERIES_GRAPH_VERSION,
} from "./event-series-graph.js";
export { getWebhoundHardCapUsd, resolveAmpfyAdapterStatus } from "./cost-ledger.js";
export {
  issueGdiShareCapability,
  revokeGdiShareCapability,
  verifyGdiShareCapability,
  restoreGdiShareCapabilityFromToken,
  upsertGdiShareRegistryFromClaims,
  extractGdiShareCapabilityFromRequest,
  sanitizeOpportunityForShare,
  readGdiShareRegistry,
  gdiShareSignatureSelfHealEnabled,
  GDI_SHARE_TOKEN_PREFIX,
  DEFAULT_GDI_SHARE_SURFACES,
  assertGdiShareProductionConfig,
  customerMessageForShareCode,
  resolveGdiShareCapabilities,
  grantGdiShareCapabilitiesByTokenId,
  shareHasCapability,
  GDI_SHARE_CAPABILITY,
  GDI_SHARE_CUSTOMER_MESSAGES,
  getGdiShareVerificationSecrets,
  loadDurableRevokedTokenIds,
  markTokenIdDurablyRevoked,
  isTokenIdDurablyRevoked,
} from "./share/gdi-signed-share-capability-v1.js";

export {
  projectCanonicalContactForExternal,
  overlayCanonicalContactOnOpportunity,
  mapExternalPhoneType,
  assertNoProviderLeak,
  EXTERNAL_PHONE_TYPE,
  EXTERNAL_PHONE_TYPE_LABEL,
} from "./canonical-contact-external-projection.js";

export {
  loadCanonicalContacts,
  saveCanonicalContacts,
  buildCanonicalPersonEntry,
  resolveCanonicalOverlayForOpportunity,
  applyCanonicalOverlaysToOpportunities,
} from "./canonical-contacts-store.js";

export {
  loadShareValidation,
  saveShareValidationItem,
  getShareValidationForOpportunity,
  validateShareValidationPayload,
  mapValidationToFeedbackEvents,
  SHARE_FAMILIARITY_STATUS,
  SHARE_FAMILIARITY_LABEL,
  SHARE_COMMERCIAL_VALUE,
  SHARE_COMMERCIAL_VALUE_LABEL,
  SHARE_CONTACT_PERSON_ASSESSMENT,
  SHARE_CONTACT_PERSON_LABEL,
  SHARE_EMAIL_ASSESSMENT,
  SHARE_EMAIL_ASSESSMENT_LABEL,
  SHARE_PHONE_ASSESSMENT,
  SHARE_PHONE_ASSESSMENT_LABEL,
  CONTACT_FEEDBACK_EVENT_TYPE,
} from "./share-validation.js";

export {
  computePhonePilotValidationMetrics,
  computeHotelValidationMetrics,
  buildShareValidationSummary,
} from "./hotel-validation-metrics.js";

export {
  mapWebhoundOpportunityRow,
  mapWebhoundOpportunityUniverse,
  extractOpportunityRowsFromWebhoundText,
} from "./webhound-opportunity-import.js";

export {
  toTerritoryRole,
  isCoreTerritory,
  isCompetitiveOrStretchTerritory,
  isOutsideTerritory,
  classifyDemandTerritoryFitFromConfig,
  resolveTerritoryFitLabel,
  TERRITORY_CLASS,
  TERRITORY_ROLE,
} from "./demand-territory.js";

export {
  PRIVATE_EVENTS_V1,
  VENUE_TYPE,
  ON_SITE_LODGING_STATUS,
  VENUE_PRIORITY,
  BUYER_PATH,
  ROOM_DEMAND_CLAIM,
  HOTEL_ARCHETYPE_PE,
  buildVenueEntity,
  createVenueGraph,
  buildHotelContext,
  buildHotelVenueRelationship,
  modelRoomDemand,
  assessLodgingCapture,
  classifyVenuePriority,
  qualifySpecificEvent,
  qualifyVenuePartnership,
  runPrivateEventsForHotel,
} from "./private-events/index.js";
