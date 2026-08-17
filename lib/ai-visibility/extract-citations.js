/**
 * Citation extraction — provider-supplied only.
 * No aggressive crawling. Do not invent citations.
 * Phase 2B: optional deterministic entity association via citation-association.js
 */

import { randomUUID } from "crypto";
import { associateCitationsToEntities } from "./citation-association.js";
import { CITATION_ASSOC_VERSION } from "./config.js";

function newCitationId() {
  return `cit_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

export function parseDomain(url) {
  if (!url || typeof url !== "string") return null;
  try {
    const u = new URL(url);
    if (!/^https?:$/i.test(u.protocol)) return null;
    return u.hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

/**
 * @param {string|null} domain
 * @param {import('./types.js').CanonicalEntity[]} entities
 */
export function matchFirstPartyDomain(domain, entities) {
  if (!domain) return { firstParty: null, entityAssociation: null };
  const d = domain.toLowerCase();
  for (const entity of entities || []) {
    for (const fp of entity.firstPartyDomains || []) {
      const needle = String(fp).toLowerCase().replace(/^www\./, "");
      if (d === needle || d.endsWith(`.${needle}`)) {
        return { firstParty: true, entityAssociation: entity.id };
      }
    }
  }
  return { firstParty: false, entityAssociation: null };
}

/**
 * @param {{
 *   responseId: string,
 *   providerCitations?: object[],
 *   entities?: import('./types.js').CanonicalEntity[],
 *   mentions?: object[],
 *   responseText?: string,
 * }} args
 */
export function extractCitations(args) {
  const {
    responseId,
    providerCitations = [],
    entities = [],
    mentions = [],
    responseText = "",
  } = args;
  const draft = [];

  providerCitations.forEach((c, i) => {
    const url = c?.url ? String(c.url).trim() : null;
    const domain = c?.domain || parseDomain(url);
    draft.push({
      citationId: newCitationId(),
      responseId,
      url,
      domain: url && !domain ? null : domain,
      title: c?.title ? String(c.title) : null,
      citationPosition: c?.citationPosition ?? i + 1,
      providerSupplied: c?.providerSupplied !== false,
      firstParty: null,
      entityAssociation: null,
      sourceType: "other",
      startIndex: c?.startIndex ?? c?.start_index ?? null,
      endIndex: c?.endIndex ?? c?.end_index ?? null,
      associationVersion: CITATION_ASSOC_VERSION,
    });
  });

  return associateCitationsToEntities({
    citations: draft,
    mentions,
    entities,
    responseText,
  });
}
