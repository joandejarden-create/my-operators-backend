/**
 * Packet 2.5 — Claim engine public exports.
 */

export {
  CLAIM_ENGINE_VERSION,
  CLAIM_TYPES,
  CRITICAL_CLAIM_TYPES,
  CLAIM_IMPORTANCE,
  TEMPORAL_STATUSES,
  CLAIM_VERIFICATION_STATUSES,
  CONTRADICTION_STATUSES,
  ENTITY_SCOPES,
  ALIAS_TYPES,
  PROMOTION_STATUSES,
  INTELLIGENCE_EVENT_TYPES,
  EXTRACTION_ERROR_CODES,
  ENTITY_RESOLUTION_ERROR_CODES,
  RELATIONSHIP_RECONCILE_ERROR_CODES,
} from "./claim-types.js";

export {
  createIntelligenceClaim,
  createEvidenceAlias,
  createRelationshipCandidate,
  deriveOverallClaimConfidence,
} from "./schemas.js";

export {
  SOURCE_LANGUAGE_MAP_VERSION,
  SOURCE_LANGUAGE_TO_CANDIDATE,
  claimTypeToRelationshipCandidate,
  mapSourceLanguageToCandidate,
} from "./source-language.js";

export {
  CLAIM_AUTHORITY_VERSION,
  authorityForClaimType,
  normalizeAuthorityKey,
} from "./authority.js";

export {
  CLAIM_EXTRACTOR_VERSION,
  extractClaimsFromDocument,
  extractClaimsFromCorpus,
} from "./extractor.js";

export {
  CLAIM_ENTITY_RESOLVE_VERSION,
  ANTI_MERGE_PAIRS,
  createEntityResolver,
  resolveClaimsEntities,
  aliasesFromRegistry,
  wouldFalseMerge,
} from "./entity-resolve.js";

export {
  CLAIM_RECONCILE_VERSION,
  claimsToRelationshipCandidates,
  reconcileRelationshipCandidates,
} from "./reconcile.js";

export {
  CLAIM_PROMOTE_VERSION,
  evaluatePromotion,
  promoteCandidates,
} from "./promote.js";

export {
  CLAIM_EVENTS_VERSION,
  createIntelligenceEvent,
} from "./events.js";

export {
  CLAIM_PIPELINE_VERSION,
  runClaimPipeline,
} from "./pipeline.js";

export {
  CLAIM_VALIDATE_VERSION,
  validateClaimCandidate,
  acceptClaimsFromCandidates,
} from "./validate-claim.js";

export {
  FP_TAXONOMY_VERSION,
  FALSE_POSITIVE_CATEGORIES,
  CLAIM_ACCEPTANCE_STATUSES,
  CLAIM_ASSERTION_KINDS,
  CLAIM_FAMILIES,
  familyForClaimType,
} from "./fp-taxonomy.js";

export {
  ROLE_LEXICON_VERSION,
  ROLE_LEXICON,
  CLAIM_TYPE_PRECONDITIONS,
} from "./role-lexicon.js";

export {
  SOURCE_PROFILE_VERSION,
  profileForDocument,
  getSourceProfile,
} from "./source-profiles.js";

export {
  CLAIM_METRICS_VERSION,
  claimMatchKey,
  scoreClaimExtraction,
  scoreRelationships,
  scoreEntityResolution,
} from "./metrics.js";
