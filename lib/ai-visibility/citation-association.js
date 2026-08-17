/**
 * Citation → entity association v1 (deterministic).
 * No crawling. No AI. No inferred causality.
 *
 * Rules (in order):
 * 1. First-party domain match against entity.firstPartyDomains
 * 2. Provider citation span (startIndex/endIndex) overlaps or closely surrounds a mention
 * 3. Otherwise unresolved
 */

export const CITATION_ASSOC_VERSION = "ai_visibility_citation_assoc_v1";

function parseDomain(url) {
  if (!url || typeof url !== "string") return null;
  try {
    const u = new URL(url);
    if (!/^https?:$/i.test(u.protocol)) return null;
    return u.hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

function matchFirstPartyDomain(domain, entities) {
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
 *   citations: object[],
 *   mentions: object[],
 *   entities: object[],
 *   responseText?: string,
 * }} args
 */
export function associateCitationsToEntities(args) {
  const citations = Array.isArray(args.citations) ? args.citations : [];
  const mentions = Array.isArray(args.mentions) ? args.mentions : [];
  const entities = Array.isArray(args.entities) ? args.entities : [];
  const text = String(args.responseText || "");

  return citations.map((c) => {
    const url = c?.url ? String(c.url).trim() : null;
    const domain = c?.domain || parseDomain(url);
    const base = {
      ...c,
      url,
      domain,
      associationVersion: CITATION_ASSOC_VERSION,
      associationMethod: null,
    };

    const fp = matchFirstPartyDomain(domain, entities);
    if (fp.entityAssociation) {
      return {
        ...base,
        firstParty: true,
        entityAssociation: fp.entityAssociation,
        associationMethod: "first_party_domain",
        sourceType: "first_party",
      };
    }

    const start = c?.startIndex ?? c?.start_index ?? null;
    const end = c?.endIndex ?? c?.end_index ?? null;
    if (typeof start === "number" && typeof end === "number" && end > start && text) {
      const spanMentions = mentions.filter(
        (m) =>
          m.canonicalEntityId &&
          typeof m.mentionPosition === "number" &&
          m.mentionPosition < end &&
          m.mentionPosition + String(m.rawMention || "").length > start
      );
      if (spanMentions.length === 1) {
        return {
          ...base,
          firstParty: false,
          entityAssociation: spanMentions[0].canonicalEntityId,
          associationMethod: "citation_span_overlap",
          sourceType: "associated_source",
        };
      }
      const near = mentions.filter((m) => {
        if (!m.canonicalEntityId || typeof m.mentionPosition !== "number") return false;
        const mStart = m.mentionPosition;
        const mEnd = mStart + String(m.rawMention || "").length;
        return mEnd >= start - 40 && mStart <= end + 40;
      });
      const uniqueIds = [...new Set(near.map((m) => m.canonicalEntityId))];
      if (uniqueIds.length === 1) {
        return {
          ...base,
          firstParty: false,
          entityAssociation: uniqueIds[0],
          associationMethod: "citation_span_near_mention",
          sourceType: "associated_source",
        };
      }
    }

    return {
      ...base,
      firstParty: domain == null ? null : fp.firstParty === true ? true : false,
      entityAssociation: null,
      associationMethod: "unresolved",
      sourceType: domain ? "third_party" : "other",
    };
  });
}
