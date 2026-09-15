/**
 * GDI Contact Candidate Discovery — public surface.
 */

export {
  GDI_CONTACT_ROLE,
  GDI_CONTACT_ROLE_LABEL,
  ROLE_RELEVANCE,
  ROLE_RELEVANCE_LABEL,
  EVENT_FAMILY,
  ROLE_PRIORITY_BY_FAMILY,
  CANDIDATE_STATUS,
  CANDIDATE_CONFIDENCE,
  IDENTITY_STATUS,
  classifyEventFamily,
  classifyGdiContactRole,
  classifyRoleRelevance,
  rolePriorityScore,
  mapTargetRoleMatchToGdiRole,
  buildCandidateSearchQueries,
} from "./ontology.js";

export {
  CANDIDATE_SCORE_WEIGHTS,
  scoreContactCandidate,
  annotateCandidate,
  classifyCandidateConfidence,
  classifyCandidateStatus,
  classifyIdentityStatus,
  buildWhyThisPerson,
} from "./scoring.js";

export {
  discoverContactCandidates,
  discoverContactCandidatesForOpportunities,
  selectRankedCandidates,
  summarizeDiscoveryResults,
  buildBeforeSnapshot,
  NO_PROBABLE,
  CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
} from "./discovery.js";

export {
  CONTACT_CANDIDATE_SEEDS_V1,
} from "./seeds-v1.js";

export {
  OFFICIAL_PERSON_DISCOVERIES_V1,
  OFFICIAL_PERSON_DISCOVERY_PASS_ID,
} from "./official-person-discoveries-v1.js";

export {
  EMPLOYMENT_STATUS,
  EVENT_RELATIONSHIP,
  PRIMARY_KIND,
  ENRICHMENT_GAP,
  SOURCE_TYPE,
  applyPersonDiscoveryGates,
  classifyEnrichmentGap,
  isSurfeEligiblePerson,
  isLinkedInOnlyEvidence,
} from "./person-discovery-states.js";
