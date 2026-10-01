/**
 * Ownership Intelligence domain — public exports.
 */

export {
  OWNERSHIP_ONTOLOGY_VERSION,
  ENTITY_TYPES,
  RELATIONSHIP_TYPES,
  RELATIONSHIP_ROLES,
  RELATIONSHIP_TYPE_TO_ROLE,
  P0_RELATIONSHIP_TYPES,
  VERIFICATION_STATUSES,
  ENTITY_STATUSES,
  SOURCE_TYPES,
  FORBIDDEN_EVIDENCE_SOURCES,
  RESEARCH_STAGES,
  RESEARCH_RUN_STATUSES,
  EXTRACTION_METHODS,
  IDENTIFIER_KINDS,
  roleForRelationshipType,
  isAllowedEnum,
} from "./ontology.js";

export {
  OWNERSHIP_CONFIDENCE_MAP_VERSION,
  PRODUCT_LABEL_TO_VERIFICATION,
  OWNERSHIP_SOURCE_AUTHORITY,
  CENSUS_FLAT_FIELD_CONFIDENCE_CAP,
  verificationFromConfidence,
  scoreOwnershipEvidence,
  rollupVerificationStatus,
  isVerificationStatus,
} from "./confidence-map.js";

export {
  OWNERSHIP_IDS_VERSION,
  generateEntityId,
  isEntityId,
  generateRelationshipId,
  isRelationshipId,
  generateEvidenceId,
  isEvidenceId,
  generateAliasId,
  isAliasId,
  generateResearchRunId,
  isResearchRunId,
  generateObservationId,
  isObservationId,
  generateDossierId,
  isDossierId,
  generateClaimId,
  isClaimId,
  generateRelationshipCandidateId,
  isRelationshipCandidateId,
  normalizeEntityName,
} from "./ids.js";

export {
  OWNERSHIP_SCHEMAS_VERSION,
  createOwnershipEntity,
  createEntityAlias,
  createOwnershipRelationship,
  createRelationshipEvidence,
  createResearchRun,
} from "./schemas.js";

export {
  OWNERSHIP_VALIDATE_VERSION,
  FORBIDDEN_ENTITY_TYPE_ROLES,
  validateOwnershipEntity,
  validateEntityAlias,
  validateOwnershipRelationship,
  validateRelationshipEvidence,
  validateResearchRun,
  validateIntelligenceObservation,
  validateResearchDossier,
  assertValid,
} from "./validate.js";

export {
  OWNERSHIP_REPOSITORY_VERSION,
  OWNERSHIP_REPOSITORY_METHODS,
  isOwnershipRepository,
} from "./repository.js";

export { createMemoryOwnershipRepository } from "./memory-repository.js";
export {
  createLocalOwnershipRepository,
  LOCAL_OWNERSHIP_REPOSITORY_VERSION,
} from "./local-repository.js";

export { queryOwnerGet, queryHotelOwnership } from "./queries.js";
export { researchHotelOwnership } from "./research/run.js";
export { createCompanySeed, seedProceedsToOwnershipResearch } from "./company-seed.js";
export { reconstructOwnerPortfolio } from "./portfolio/reconstruct.js";
export { deriveOrganizationAggregates } from "./portfolio/organization-aggregates.js";
export {
  PORTFOLIO_MATCH_STATUS,
  matchPortfolioAssetToCensus,
} from "./portfolio/census-match.js";
export { classifyPortfolioRelationship } from "./portfolio/classify-claim.js";
export {
  createIntelligenceObservation,
  classifyResearchClaim,
  INTELLIGENCE_CLASSES,
  OBSERVATION_TYPES,
  ACTIVITY_EVENT_TYPES,
  PROMOTION_STATUSES,
} from "../intelligence-observation.js";
export {
  createResearchDossier,
  createProvenanceStep,
  provenanceStepsFromBrazilNotes,
} from "./research-dossier.js";
export {
  evaluateObservationPromotionEligibility,
  observationToCandidateRelationship,
} from "./observation-promotion.js";
export { buildOwnershipQueries } from "./research/stage-c-search.js";
export {
  buildOwnershipEvalCohort,
  OWNERSHIP_EVAL_REGIONS,
  ownershipEvalRegion,
} from "./evaluation/cohort.js";
export { computeOwnershipEvalMetrics } from "./evaluation/metrics.js";
export { buildTruthSubsetScaffold } from "./evaluation/truth-subset.js";

/** Packet 2.5 — atomic claim → entity → relationship pipeline */
export * from "./claims/index.js";
export { PACKET25_ENTITY_REGISTRY } from "./claims/packet25-entity-registry.js";
