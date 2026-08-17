/**
 * Research Engine V2 — public exports.
 */

export {
  checkHotelFreshness,
  findDirectoryGaps,
  loadIhgDirectoryRows,
  loadMarriottTributeDirectoryRows,
  loadMarriottSoftBrandDirectoryRows,
  computeDirectoryGaps,
  computeChoiceIndividualsGaps,
  computeMarriottSoftBrandGaps,
  MATCH_GATE_CONFIG_V1_1,
  CORROBORATION_CONFIG_V1_1,
  ENGINE_VERSION,
} from "./check-hotel-freshness.js";
export { createClaim, createProposedCorrection, CLAIM_TYPES, CLAIM_STATUSES, RECOMMENDED_ACTIONS } from "./claim-model.js";
export { generateResearchQueries } from "./query-generator.js";
export { SOURCE_HIERARCHY_BY_CLAIM, resolveTemporalConflict } from "./source-hierarchy.js";
export { resolveBrandFamily } from "./brand-family.js";
export { runCrossTableChecks } from "./cross-table-checks.js";
export { assessEntityMatch } from "./match-confidence.js";
export { assessStatusCorroboration, assessReflagCorroboration } from "./corroboration.js";
export { RESEARCH_MODES, ACTIVATION_STATUSES, IMAGE_CLASSIFICATIONS, IMAGE_ACTIONS } from "./research-modes.js";
export { runShadowCohort, formatShadowDigestMarkdown } from "./shadow-monitor.js";
export { runBrandActivationResearch, ACTIVATION_HARD_GATES } from "./brand-activation.js";
export { auditImagesForEntity } from "./image-integrity.js";
export { assessOpeningCorroborationFromOfficialPage } from "./opening-corroboration.js";
export { batchIdentityEnrichmentProposals } from "./identity-enrichment.js";
export { claimFingerprint, applyAlertDedup, loadShadowState, saveShadowState } from "./shadow-state.js";
export {
  createQueueItem,
  buildReviewPack,
  mergeIntoStewardQueue,
  loadStewardQueue,
  saveStewardQueue,
  assignPriority,
  governanceHandoff,
  STEWARD_STATUSES,
  ENGINE_OPS_VERSION,
} from "./steward-queue.js";
export { classifySourceState, isSourceUnsafeForProposals, SOURCE_STATES } from "./source-state.js";
export { recommendEscalation, ESCALATION_ACTIONS } from "./escalation.js";
export { FALLBACK_LADDER, summarizeFallbackAttempts } from "./source-fallback.js";
export { SHADOW_OPS_CONFIG, selectCohortHotels } from "./ops-config.js";
export { emptyMetrics, finalizeMetrics } from "./ops-metrics.js";
export { createResearchFirewall, ResearchFirewallError } from "./clean-census/research-firewall.js";
export { discoverIhgIndigoKimptonMexico } from "./clean-census/independent-discovery.js";
export { buildIndependentCohortRecords, buildIndependentRecord } from "./clean-census/independent-record.js";
export { reconcileAfterFreeze, runLegacyOnlyChallenges, fingerprintFreeze } from "./clean-census/legacy-reconcile.js";
export {
  PROVENANCE_CLASSES,
  CLEAN_CENSUS_RECORD_STATUSES,
  MATERIAL_CENSUS_FIELDS,
  CORE_MATERIAL_FIELDS,
  createFieldClaim,
  scoreMaterialCompleteness,
} from "./clean-census/provenance.js";
export { runReconstructionWave, DEFAULT_WAVE_CONFIGS } from "./clean-census/wave-engine.js";
export { discoverIhgMexicoAll, getGroupAdapterInventory, mapIhgDirectoryBrand } from "./clean-census/group-discovery.js";
export {
  discoverHiltonMexicoAll,
  buildHiltonIndependentRecord,
  buildHiltonIndependentCohortRecords,
} from "./clean-census/hilton-mexico-discovery.js";
export {
  discoverChoiceMexicoAll,
  buildChoiceIndependentRecord,
  buildChoiceIndependentCohortRecords,
  researchRadissonIndividualsChoiceRelationship,
} from "./clean-census/choice-mexico-discovery.js";
export {
  discoverMarriottMexicoAll,
  buildMarriottIndependentRecord,
  buildMarriottIndependentCohortRecords,
  mapMarriottMexicoBrand,
} from "./clean-census/marriott-mexico-discovery.js";
export {
  createPropertyIdentity,
  propertyIdentityFromVerifiedRecord,
  assessSamePhysicalProperty,
  attachPropertyIdentities,
  classifyReflagOrAffiliation,
} from "./clean-census/property-identity.js";
export {
  createAffiliationPeriod,
  seedCurrentAffiliationFromRecord,
  applyTemporalAffiliationSeed,
  appendHistoricalAffiliation,
} from "./clean-census/temporal-affiliation.js";
export { findCrossFamilyIdentities, classifyCrossFamilyPair } from "./clean-census/cross-family-identity.js";
export { assessProductionEligibility, batchAssessProductionEligibility } from "./clean-census/production-eligibility.js";
export {
  runStrictIndependentRediscovery,
  runTargetedVerificationChallenges,
  CHALLENGE_CLASS_RECOMMENDATION,
} from "./clean-census/legacy-challenges.js";
export { createVerifiedIndependentRecord, VIC_ENGINE_VERSION } from "./clean-census/verified-record.js";
export { extractDeepOfficialPageSignals, FIELD_RESEARCH_PLANS } from "./clean-census/field-research.js";
