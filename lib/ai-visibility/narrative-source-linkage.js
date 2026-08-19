/**
 * Narrative ↔ source linkage (V1).
 * Same-response citation grain preferred. No causal claims.
 */

import { parseDomain } from "./extract-citations.js";
import { classifySourceOwnership } from "./cited-source-intelligence.js";
import { resolveOwnedDomainsForBrand } from "./brand-website-wiring.js";
import {
  ALLOWED_SOURCE_RELATIONSHIP,
  classifySourceCategory,
  containsForbiddenSourceLanguage,
} from "./narrative-taxonomy.js";

export const NARRATIVE_SOURCE_LINKAGE_VERSION = "ai_visibility_narrative_source_linkage_v1";

function citationInSpan(citation, spanStart, spanEnd) {
  const pos = citation.startIndex ?? citation.start ?? citation.citationPosition ?? null;
  if (pos == null || spanStart == null || spanEnd == null) return false;
  return pos >= spanStart && pos <= spanEnd + 120;
}

function citationNearSpan(citation, spanStart, spanEnd) {
  const pos = citation.startIndex ?? citation.start ?? citation.citationPosition ?? null;
  if (pos == null || spanStart == null || spanEnd == null) return false;
  return Math.abs(pos - spanEnd) < 400 || Math.abs(pos - spanStart) < 400;
}

/**
 * Link narrative observations to cited domains at response grain.
 * @param {object[]} observations
 * @param {object[]} evidence
 * @param {object} [options]
 */
export function buildNarrativeSourceRelationships(observations = [], evidence = [], options = {}) {
  const evidenceById = new Map(evidence.map((e) => [e.evidenceId, e]));
  const relationships = [];
  const seen = new Set();

  for (const obs of observations) {
    const ev = evidenceById.get(obs.evidenceId);
    if (!ev) continue;
    const citations = ev.payload?.citations || [];
    const owned = resolveOwnedDomainsForBrand(obs.brandId, options.entityIndex || {});
    const ownedList = owned?.owned?.ownedDomainList || [];

    for (const cite of citations) {
      const domain = cite.domain || parseDomain(cite.url);
      if (!domain) continue;
      const inSpan = citationInSpan(cite, obs.evidenceSpanStart, obs.evidenceSpanEnd);
      const nearSpan =
        !inSpan && citationNearSpan(cite, obs.evidenceSpanStart, obs.evidenceSpanEnd);
      const ownership = classifySourceOwnership(domain, ownedList);
      const ownedExternal =
        ownership.type === "OWNED"
          ? "OWNED"
          : ownership.type === "THIRD_PARTY"
            ? "EXTERNAL"
            : "UNKNOWN";
      const relationship = inSpan
        ? "DIRECTLY_CITED_WITH_NARRATIVE"
        : nearSpan
          ? "CITED_IN_SAME_RESPONSE"
          : "ASSOCIATED_NOT_CAUSAL";
      const key = `${obs.brandId}|${obs.narrativeFamily}|${domain}|${obs.responseId}|${relationship}`;
      if (seen.has(key)) continue;
      seen.add(key);

      relationships.push({
        brandId: obs.brandId,
        brandName: obs.brandName,
        narrativeFamily: obs.narrativeFamily,
        narrativeLabel: obs.narrativeLabel,
        domain,
        ownedExternal,
        sourceCategory: classifySourceCategory(domain, { ownedExternal }),
        relationship,
        responseId: obs.responseId,
        promptId: obs.promptId,
        provider: obs.provider,
        productionState:
          relationship === "DIRECTLY_CITED_WITH_NARRATIVE" ? "DETAIL_ONLY" : "RESEARCH_ONLY",
      });
    }
  }

  return aggregateSourceRelationships(relationships);
}

function aggregateSourceRelationships(rows = []) {
  const byKey = new Map();
  for (const row of rows) {
    const key = `${row.brandId}|${row.narrativeFamily}|${row.domain}|${row.relationship}`;
    if (!byKey.has(key)) {
      byKey.set(key, {
        brandId: row.brandId,
        brandName: row.brandName,
        narrativeFamily: row.narrativeFamily,
        narrativeLabel: row.narrativeLabel,
        domain: row.domain,
        ownedExternal: row.ownedExternal,
        sourceCategory: row.sourceCategory,
        relationship: row.relationship,
        responseIds: [],
        providers: [],
        productionState: row.productionState,
      });
    }
    const agg = byKey.get(key);
    if (row.responseId && !agg.responseIds.includes(row.responseId)) {
      agg.responseIds.push(row.responseId);
    }
    if (row.provider && !agg.providers.includes(row.provider)) {
      agg.providers.push(row.provider);
    }
  }

  return [...byKey.values()]
    .map((r) => ({
      ...r,
      responses: r.responseIds.length,
      providers: r.providers.sort(),
      recurrence: {
        label:
          r.relationship === "DIRECTLY_CITED_WITH_NARRATIVE"
            ? `Directly cited in ${r.responseIds.length} response(s)`
            : `Associated in ${r.responseIds.length} response(s)`,
      },
    }))
    .sort((a, b) => b.responses - a.responses);
}

export function validateSourceLinkageLanguage(text) {
  return !containsForbiddenSourceLanguage(text);
}

export { ALLOWED_SOURCE_RELATIONSHIP };
