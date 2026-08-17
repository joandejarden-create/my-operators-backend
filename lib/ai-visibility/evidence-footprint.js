/**
 * Evidence Footprint — deterministic, non-composite intelligence (Phase 3B.4).
 * Not a Visibility Score. Distinct from AI Presence / Recommendation Rate.
 */

import { parseDomain } from "./extract-citations.js";

export const EVIDENCE_FOOTPRINT_VERSION = "ai_visibility_evidence_footprint_v1";

export const EVIDENCE_ASSOCIATION_LEVEL = Object.freeze({
  DIRECT_CITATION_ASSOCIATION: "DIRECT_CITATION_ASSOCIATION",
  RESPONSE_LEVEL_ASSOCIATION: "RESPONSE_LEVEL_ASSOCIATION",
  SEARCH_RESULT_ONLY: "SEARCH_RESULT_ONLY",
  UNAVAILABLE: "UNAVAILABLE",
});

/**
 * Provider capability → evidence association level (from live baseline observation methodology).
 */
export const PROVIDER_EVIDENCE_ASSOCIATION = Object.freeze({
  openai: EVIDENCE_ASSOCIATION_LEVEL.DIRECT_CITATION_ASSOCIATION,
  gemini: EVIDENCE_ASSOCIATION_LEVEL.RESPONSE_LEVEL_ASSOCIATION,
  perplexity: EVIDENCE_ASSOCIATION_LEVEL.RESPONSE_LEVEL_ASSOCIATION,
  claude: EVIDENCE_ASSOCIATION_LEVEL.DIRECT_CITATION_ASSOCIATION,
});

/**
 * Brand Mentions: recognized Brand appearance events within successful responses.
 * COUNTING_UNIT: mention event (entity match in response)
 * Distinct from AI Presence (share of responses where brand appears).
 */
export function countBrandMentions(mentions = [], opts = {}) {
  const entityId = opts.entityId || null;
  let count = 0;
  for (const m of mentions) {
    if (entityId && m.entityId !== entityId && m.brandId !== entityId) continue;
    if (m.isParentCompanyLabel) continue;
    count += 1;
  }
  return count;
}

/**
 * Recommendation Mentions: mention events classified as recommendations.
 */
export function countRecommendationMentions(mentions = [], opts = {}) {
  const entityId = opts.entityId || null;
  let count = 0;
  for (const m of mentions) {
    if (entityId && m.entityId !== entityId && m.brandId !== entityId) continue;
    const recommended =
      m.role === "recommended" ||
      m.explicitRecommendation === true ||
      m.recommendationRole === "recommended";
    if (recommended) count += 1;
  }
  return count;
}

/**
 * Evidence-bearing: response has valid normalized source material.
 */
export function isEvidenceBearingResponse({ citations = [], searchResults = [] } = {}) {
  const hasValidCitation = (citations || []).some((c) => {
    const url = c.url || c.sourceUrl;
    const domain = c.domain || parseDomain(url);
    return Boolean(url || domain);
  });
  const hasSearch = (searchResults || []).some((s) => s.url || s.domain);
  return hasValidCitation || hasSearch;
}

/**
 * Aggregate Evidence Footprint for a set of successful observation rows.
 * @param {Array<{responseId, mentions, citations, searchResults, provider, language, geographyKey, intent}>} responses
 */
export function buildEvidenceFootprint(responses = [], opts = {}) {
  const entityId = opts.entityId || null;
  let brandMentions = 0;
  let recommendationMentions = 0;
  let evidenceBearing = 0;
  const domainResponseMap = new Map(); // domain -> Set(responseId)
  const urlSet = new Set();

  for (const row of responses) {
    const mentions = row.mentions || [];
    brandMentions += countBrandMentions(mentions, { entityId });
    recommendationMentions += countRecommendationMentions(mentions, { entityId });

    const citations = row.citations || [];
    const searchResults = row.searchResults || [];
    if (isEvidenceBearingResponse({ citations, searchResults })) {
      evidenceBearing += 1;
    }

    const responseId = row.responseId || row.runId || null;
    for (const c of citations) {
      const url = c.url || c.sourceUrl || null;
      const domain = (c.domain || parseDomain(url) || "").toLowerCase();
      if (url) urlSet.add(String(url));
      if (!domain || !responseId) continue;
      if (!domainResponseMap.has(domain)) domainResponseMap.set(domain, new Set());
      domainResponseMap.get(domain).add(responseId);
    }
  }

  const uniqueDomains = domainResponseMap.size;
  const repeatedAcrossResponses = [...domainResponseMap.entries()]
    .filter(([, set]) => set.size >= 2)
    .map(([domain, set]) => ({
      domain,
      distinctResponses: set.size,
      label: "Repeated Across Monitored Responses",
    }))
    .sort((a, b) => b.distinctResponses - a.distinctResponses);

  return {
    version: EVIDENCE_FOOTPRINT_VERSION,
    BRAND_MENTIONS: {
      count: brandMentions,
      COUNTING_UNIT: "recognized_brand_mention_event",
      DEDUPLICATION_RULE: "each_entity_match_in_response_counts_once_per_match_span",
      FILTER_DIMENSIONS: ["provider", "brand", "geography", "language", "intent", "period"],
      RELATIONSHIP_TO_AI_PRESENCE:
        "Presence = share of responses with brand; Mentions = count of mention events",
    },
    RECOMMENDATION_MENTIONS: {
      count: recommendationMentions,
      COUNTING_UNIT: "recommendation_classified_mention_event",
      RELATIONSHIP_TO_RECOMMENDATION_RATE:
        "Rate = share of responses with recommendation; Mentions = count of recommendation events",
    },
    EVIDENCE_BEARING_RESPONSES: {
      count: evidenceBearing,
      RESPONSE_LEVEL_EVIDENCE_READY: true,
    },
    UNIQUE_CITED_SOURCES: {
      uniqueDomains,
      uniqueUrls: urlSet.size,
      clientFacingLabel: "Unique Cited Sources",
    },
    REPEATED_ACROSS_MONITORED_RESPONSES: repeatedAcrossResponses,
    COMPOSITE_SCORE: null,
    READY: true,
  };
}

export function resolveEvidenceAssociationLevel(provider) {
  const id = String(provider || "").toLowerCase();
  return PROVIDER_EVIDENCE_ASSOCIATION[id] || EVIDENCE_ASSOCIATION_LEVEL.UNAVAILABLE;
}

/**
 * Filter responses by monitoring period (Phase 3B.6).
 */
export function filterResponsesByPeriod(responses = [], periodId) {
  if (!periodId) return responses;
  return responses.filter(
    (r) =>
      r.periodId === periodId ||
      r.monitoringPeriodId === periodId ||
      r.batchId === periodId
  );
}

/**
 * Build Evidence Footprint scoped to a monitoring period.
 */
export function buildEvidenceFootprintForPeriod(responses = [], opts = {}) {
  const scoped = filterResponsesByPeriod(responses, opts.periodId);
  const footprint = buildEvidenceFootprint(scoped, opts);
  return {
    ...footprint,
    periodId: opts.periodId || null,
    PERIOD_FILTER_SUPPORTED: "YES",
    scopedResponseCount: scoped.length,
  };
}
