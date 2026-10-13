/**
 * Country-aware ownership research strategy resolver.
 */
import {
  STRATEGY_VERSION,
  PROPERTY_LINEAGE_STATE,
  ENTITY_CANDIDATE_ROLE,
  OWNERSHIP_CONCLUSION_STATE,
  SOURCE_TIER,
  SOURCE_CLASS,
  PROPERTY_ENTITY_MATCH,
  RESEARCH_GOAL_KIND,
  NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE,
  RESEARCH_PROGRESS_KEYS,
} from "./constants.js";
import {
  createBrazilOwnershipStrategyV1,
  buildAddressLegalEntityContinuationGoals,
  hasUnattemptedAddressLegalEntityLead,
  buildBrazilLegalEntityQueries,
  generateBrazilEntryGoals,
} from "./brazil/brazil-v1.js";
import {
  extractLegalEntityBundle,
  extractCnpjCandidates,
  extractPrincipalCandidates,
  classifyEntityCandidateRole,
  isNonAuthoritativeCnpj,
  normalizeCnpj,
  formatCnpj,
} from "./legal-entity-extract.js";
import {
  classifySourceTier,
  shouldScrapeForOwnership,
  sourceTierRankDelta,
} from "./source-tier.js";
import { assessPropertyLineage, appendHistoricalName } from "./property-lineage.js";
import {
  computeOwnershipResearchProgress,
  concludeFromEntityEvidence,
} from "./progress-metrics.js";
import { planScrapeFailureRecovery } from "./scrape-failure-recovery.js";
import {
  selectUrlsForPaidScrape,
  evaluateScrapeCandidate,
  classifySourceClass,
  inferResearchGoalKind,
  buildSerpLeadFollowUpGoals,
} from "./scrape-selection-gate.js";
import {
  isSyntheticOrTestResult,
  normalizeSyntheticDetectionBlob,
} from "./synthetic-result.js";
import {
  classifyPropertyEntityMatch,
  buildExactAddressContinuationQueries,
  extractEntityLocationHints,
} from "./property-entity-identity-match.js";
import {
  stageLegalEntityFromEvidence,
  applyStagedConclusionToOwnership,
} from "./legal-entity-staging.js";

const BRAZIL = createBrazilOwnershipStrategyV1();

function normalizeCountry(country) {
  return String(country || "")
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .trim()
    .toUpperCase();
}

/**
 * @param {string|object} countryOrHotel — country string or hotel object with .country
 * @returns {object|null} strategy or null when no country strategy applies
 */
export function resolveOwnershipResearchStrategy(countryOrHotel) {
  const country =
    typeof countryOrHotel === "string"
      ? countryOrHotel
      : countryOrHotel?.country || countryOrHotel?.country_code || "";
  const c = normalizeCountry(country);
  if (!c) return null;
  if (BRAZIL.country_codes.includes(c) || c === "BRAZIL" || c === "BRASIL") {
    return BRAZIL;
  }
  return null;
}

/**
 * Default / shared helpers always available (even without country strategy).
 */
export function getOwnershipResearchStrategyToolkit() {
  return {
    version: STRATEGY_VERSION,
    resolveOwnershipResearchStrategy,
    extractLegalEntityBundle,
    extractCnpjCandidates,
    extractPrincipalCandidates,
    classifyEntityCandidateRole,
    isNonAuthoritativeCnpj,
    normalizeCnpj,
    formatCnpj,
    classifySourceTier,
    shouldScrapeForOwnership,
    sourceTierRankDelta,
    assessPropertyLineage,
    appendHistoricalName,
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
    extractEntityLocationHints,
    stageLegalEntityFromEvidence,
    applyStagedConclusionToOwnership,
    isSyntheticOrTestResult,
    normalizeSyntheticDetectionBlob,
    buildAddressLegalEntityContinuationGoals,
    hasUnattemptedAddressLegalEntityLead,
    buildBrazilLegalEntityQueries,
    generateBrazilEntryGoals,
    PROPERTY_LINEAGE_STATE,
    ENTITY_CANDIDATE_ROLE,
    OWNERSHIP_CONCLUSION_STATE,
    SOURCE_TIER,
    SOURCE_CLASS,
    PROPERTY_ENTITY_MATCH,
    RESEARCH_GOAL_KIND,
    NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE,
    RESEARCH_PROGRESS_KEYS,
  };
}

export {
  STRATEGY_VERSION,
  PROPERTY_LINEAGE_STATE,
  ENTITY_CANDIDATE_ROLE,
  OWNERSHIP_CONCLUSION_STATE,
  SOURCE_TIER,
  SOURCE_CLASS,
  PROPERTY_ENTITY_MATCH,
  RESEARCH_GOAL_KIND,
  NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE,
  RESEARCH_PROGRESS_KEYS,
  extractLegalEntityBundle,
  extractCnpjCandidates,
  extractPrincipalCandidates,
  classifyEntityCandidateRole,
  isNonAuthoritativeCnpj,
  normalizeCnpj,
  formatCnpj,
  classifySourceTier,
  shouldScrapeForOwnership,
  sourceTierRankDelta,
  assessPropertyLineage,
  appendHistoricalName,
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
  extractEntityLocationHints,
  stageLegalEntityFromEvidence,
  applyStagedConclusionToOwnership,
  isSyntheticOrTestResult,
  normalizeSyntheticDetectionBlob,
  buildAddressLegalEntityContinuationGoals,
  hasUnattemptedAddressLegalEntityLead,
  buildBrazilLegalEntityQueries,
  generateBrazilEntryGoals,
  createBrazilOwnershipStrategyV1,
};
