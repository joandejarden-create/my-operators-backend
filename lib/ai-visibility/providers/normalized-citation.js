/**
 * Provider-neutral normalized citation contract (Phase 3B.1).
 * Missing fields remain null — never fabricated.
 */

import { parseDomain } from "../extract-citations.js";

export const NORMALIZED_CITATION_CONTRACT_VERSION = "ai_visibility_normalized_citation_v1";

/**
 * @typedef {object} NormalizedProviderCitation
 * @property {string} contractVersion
 * @property {string} provider
 * @property {string|null} providerCitationId
 * @property {number|null} providerCitationIndex
 * @property {string|null} url
 * @property {string|null} normalizedUrl
 * @property {string|null} domain
 * @property {string|null} title
 * @property {string|null} citedText
 * @property {number|null} startIndex
 * @property {number|null} endIndex
 * @property {number|null} searchResultRank
 * @property {string|null} citationType
 * @property {object|null} rawProviderCitation
 * @property {string|null} runFingerprint
 */

function normalizeUrl(url) {
  if (!url || typeof url !== "string") return null;
  const trimmed = url.trim();
  if (!trimmed) return null;
  try {
    const u = new URL(trimmed);
    if (!/^https?:$/i.test(u.protocol)) return null;
    u.hash = "";
    return u.toString();
  } catch {
    return null;
  }
}

/**
 * @param {object} citation
 * @param {object} [ctx]
 * @returns {NormalizedProviderCitation}
 */
export function normalizeProviderCitation(citation = {}, ctx = {}) {
  const url = citation.url ? String(citation.url).trim() : null;
  const normalizedUrl = citation.normalizedUrl || normalizeUrl(url);
  const domain = citation.domain || parseDomain(url);

  return {
    contractVersion: NORMALIZED_CITATION_CONTRACT_VERSION,
    provider: ctx.provider || citation.provider || null,
    providerCitationId: citation.providerCitationId ?? citation.id ?? null,
    providerCitationIndex:
      citation.providerCitationIndex ?? citation.citationPosition ?? citation.index ?? null,
    url: url || null,
    normalizedUrl,
    domain: domain || null,
    title: citation.title ? String(citation.title) : null,
    citedText: citation.citedText ?? citation.cited_text ?? citation.snippet ?? null,
    startIndex: citation.startIndex ?? citation.start_index ?? null,
    endIndex: citation.endIndex ?? citation.end_index ?? null,
    searchResultRank: citation.searchResultRank ?? citation.rank ?? null,
    citationType: citation.citationType ?? citation.type ?? null,
    rawProviderCitation: citation.rawProviderCitation ?? citation.raw ?? citation,
    runFingerprint: ctx.runFingerprint || ctx.fingerprint || null,
  };
}

/**
 * @param {object[]} citations
 * @param {object} [ctx]
 */
export function normalizeProviderCitations(citations = [], ctx = {}) {
  return (Array.isArray(citations) ? citations : []).map((c, i) =>
    normalizeProviderCitation(
      {
        ...c,
        providerCitationIndex: c.providerCitationIndex ?? c.citationPosition ?? i + 1,
      },
      ctx
    )
  );
}

export function listNormalizedCitationContractFields() {
  return [
    "provider",
    "providerCitationId",
    "providerCitationIndex",
    "url",
    "normalizedUrl",
    "domain",
    "title",
    "citedText",
    "startIndex",
    "endIndex",
    "searchResultRank",
    "citationType",
    "rawProviderCitation",
    "runFingerprint",
  ];
}
