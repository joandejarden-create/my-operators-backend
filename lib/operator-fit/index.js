/**
 * Public exports for Operator Fit Engine v2 domain layer.
 */

export {
  isOperatorFitEngineV2Enabled,
  isOperatorFitEngineV2ShadowEnabled,
  isOperatorFitDifferentiationV21Enabled,
  getOperatorFitEngineFlagState,
  resolveOperatorFitMethodology,
  OPERATOR_FIT_ENGINE_VERSION,
  OPERATOR_FIT_ENGINE_VERSION_V21,
  OPERATOR_FIT_ENGINE_V2_FLAG,
  OPERATOR_FIT_METHODOLOGY_V2,
  OPERATOR_FIT_METHODOLOGY_V21,
} from "./feature-flag.js";
export {
  evaluateCandidateV21,
  buildOwnerTierPresentation,
  assignOwnerCandidateTier,
  calculateComparableRelevanceIndex,
  calculateGeographyRelevanceScore,
  V21_TIE_MATERIALITY_POINTS,
  V21_OWNER_TIERS,
} from "./v21/index.js";

export * from "./config.js";
export { adaptProjectFromDealContext } from "./adapters/project-from-deal.js";
export {
  adaptOperatorFromPrefill,
  adaptBrandManagedCandidate,
  classifyServices,
  isTableStakesToken,
} from "./adapters/operator-from-prefill.js";
export {
  mapOperatingStructureValue,
  mapOperatingStructureList,
  preservedStructureCatalog,
} from "./structure-mapping.js";
export { evaluateEligibility, isOwnerFacingEligible } from "./eligibility.js";
export {
  scoreAllOperatorProjectFactors,
  aggregateOperatorProjectAlignment,
} from "./alignment-factors.js";
export { evaluateCandidate } from "./evaluate-candidate.js";
export { selectTop5OperatorAlignment } from "./top5-selector.js";
export { evaluateOperatorFitForDeal } from "./evaluate-deal.js";
export {
  classifyOperatorReadiness,
  assessFieldPresence,
  calculateProjectApplicableCoverage,
  missingCriticalRankingFields,
  filterProductionTop5Candidates,
  classifyBrandManagedAvailability,
  buildEnrichmentQueueRow,
  validateOperatorTaxonomy,
  validateOperatorEvidence,
  ENRICHMENT_FIELD_CATALOG,
  READINESS_STATUS,
  RESEARCH_PRIORITY,
  PRODUCTION_COVERAGE_THRESHOLD_PCT,
} from "./readiness.js";

export {
  evaluateOperatorFitInternalPilotAccess,
  isOperatorFitInternalPilotEnabled,
  getOperatorFitPilotDealAllowlist,
  getOperatorFitInternalPilotFlagState,
} from "./internal-pilot-access.js";
export { explainRankingDifference } from "./ranking-difference.js";
export { listRankingChangeValidations } from "./ranking-change-validations.js";
export {
  OPERATOR_SHORTLIST_TABLE,
  SHORTLIST_STATUS,
  map_operatorShortlistFields,
  buildShortlistDecisionSnapshot,
  fieldsFromShortlistCreate,
} from "./shortlist.js";
export { buildShortlistComparison } from "./shortlist-compare.js";
export {
  mapAlignmentBand,
  mapEvidenceStrength,
  prioritizeUnknowns,
  buildOwnerCandidatePresentation,
  buildAdvisorCandidatePresentation,
  buildOwnerStyleComparison,
  buildZeroUniverseOwnerMessage,
  OWNER_TERMS,
} from "./owner-presentation.js";
