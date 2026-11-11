/**
 * Rehydrate opportunity.evidence / acceptedClaims from persisted Exa packs
 * when the canonical arrays were left empty after Aimbridge settle.
 * Does not invent claims; only maps existing sources/packs already on disk.
 */

export const GDI_PERSISTED_EXA_EVIDENCE_REHYDRATE_V1 =
  "gdi-persisted-exa-evidence-rehydrate-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * @returns {{ opportunity: object, recovered: boolean, evidenceAdded: number, claimsAdded: number, source: string|null, gap: string|null }}
 */
export function rehydratePersistedExaEvidence(opportunity = {}) {
  const next = { ...opportunity };
  const hadEvidence = (next.evidence || []).length > 0;
  const hadClaims = (next.acceptedClaims || []).length > 0;
  if (hadEvidence && hadClaims) {
    return {
      opportunity: next,
      recovered: false,
      evidenceAdded: 0,
      claimsAdded: 0,
      source: null,
      gap: null,
    };
  }

  const pack = next.exaAgentEvidencePack || null;
  const packClaims = Array.isArray(pack?.claims) ? pack.claims : [];
  const packSources = Array.isArray(pack?.sources) ? pack.sources : [];
  const topSources = Array.isArray(next.sources) ? next.sources : [];
  const claimMap = Array.isArray(next.exaAgentClaimToSourceMap)
    ? next.exaAgentClaimToSourceMap
    : [];
  const drawerSources = Array.isArray(next.supportingEvidenceDrawer?.sources)
    ? next.supportingEvidenceDrawer.sources
    : [];

  const sources = [
    ...topSources,
    ...packSources,
    ...drawerSources,
  ].filter((s) => s && (s.url || s.sourceUrl));

  const claims = [
    ...packClaims,
    ...claimMap,
  ].filter((c) => c && text(c.claimText || c.statement || c.text));

  let evidenceAdded = 0;
  let claimsAdded = 0;

  if (!hadEvidence && sources.length) {
    const seen = new Set();
    next.evidence = [];
    for (const s of sources) {
      const url = text(s.url || s.sourceUrl);
      if (!url || seen.has(url)) continue;
      seen.add(url);
      next.evidence.push({
        url,
        statement: text(
          s.establishes || s.title || s.excerpt || s.statement || ""
        ),
        quote: text(s.establishes || s.excerpt || ""),
        topic: s.evidenceGroup || s.evidenceGroupLabel || "source",
        sourceId: s.sourceId || null,
        provider: s.provider || "exa_agent",
        rehydratedFrom: "persisted_exa_sources",
      });
      evidenceAdded += 1;
    }
  }

  if (Array.isArray(next.evidence)) {
    next.evidence = next.evidence.map((item) => {
      if (!item || typeof item !== "object") return item;
      if (text(item.quote) || text(item.excerpt)) return item;
      const quote = text(typeof item.extractedText === "string" ? item.extractedText : item.value || item.establishes || "");
      if (!quote) return item;
      evidenceAdded += 1;
      return { ...item, quote, rehydratedFrom: "persisted_evidence_title_or_extract" };
    });
  }

  if (!hadClaims && claims.length) {
    next.acceptedClaims = claims.map((c, i) => ({
      claimId: c.claimId || `rehydrated_claim_${i}`,
      statement: text(c.claimText || c.statement || c.text),
      url:
        (c.supportingSourceUrls || [])[0] ||
        c.url ||
        sources[0]?.url ||
        null,
      accepted: true,
      topic: c.claimCategory || "exa_claim",
      factClass: c.factClass || null,
      rehydratedFrom: "persisted_exa_claim_pack",
    }));
    claimsAdded = next.acceptedClaims.length;
  }

  // Seed unresolved questions when empty so ORS eligibility can continue.
  if (!(next.unresolvedQuestions || []).length) {
    next.unresolvedQuestions = [
      "Who is the commercially relevant lodging buyer or entry point?",
      "What hotel-specific room or catering demand is evidenced?",
      "What independent confirmation exists for programme timing or partnership?",
    ];
  }

  const recovered = evidenceAdded > 0 || claimsAdded > 0;
  next.exaEvidenceRehydrate = {
    version: GDI_PERSISTED_EXA_EVIDENCE_REHYDRATE_V1,
    recovered,
    evidenceAdded,
    claimsAdded,
    at: new Date().toISOString(),
  };

  return {
    opportunity: next,
    recovered,
    evidenceAdded,
    claimsAdded,
    source: recovered ? "persisted_exa_pack_or_sources" : null,
    gap: recovered
      ? null
      : "NO_RECOVERABLE_PERSISTED_EXA_EVIDENCE_OR_SOURCES",
  };
}

export default {
  GDI_PERSISTED_EXA_EVIDENCE_REHYDRATE_V1,
  rehydratePersistedExaEvidence,
};
