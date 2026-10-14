/**
 * Claim-level evidence persistence for Parallel / GDI research.
 * Separates original source text from model summaries.
 * A URL alone is not a verified source excerpt.
 */

import { createHash } from "node:crypto";

export const CLAIM_EVIDENCE_VERSION = "gdi-claim-evidence-v1";

export const CLAIM_KIND = Object.freeze({
  FACT: "fact",
  INFERENCE: "inference",
  MODELED_ESTIMATE: "modeled_estimate",
  UNKNOWN: "unknown",
});

export const SOURCE_TYPE = Object.freeze({
  OFFICIAL: "official",
  TRADE_PRESS: "trade_press",
  PROVIDER_RESPONSE: "provider_response",
  LOCAL_PUBLIC: "local_public",
  HOUSING_AGENCY: "housing_agency",
  OTHER: "other",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function buildClaimEvidence({
  claim,
  claimKind = CLAIM_KIND.UNKNOWN,
  sourceUrl = null,
  retrievedAt = null,
  sourceExcerpt = null,
  sourceType = SOURCE_TYPE.OTHER,
  provenance = null,
  providerResponseId = null,
  modelSummary = null,
} = {}) {
  const excerpt = text(sourceExcerpt) || null;
  const url = text(sourceUrl) || null;
  const verifiedExcerpt = Boolean(excerpt && excerpt.length >= 12);
  return {
    version: CLAIM_EVIDENCE_VERSION,
    claimId: createHash("sha256")
      .update(`${text(claim)}|${url || ""}|${claimKind}`)
      .digest("hex")
      .slice(0, 16),
    claim: text(claim) || null,
    claimKind,
    sourceUrl: url,
    retrievedAt: retrievedAt || new Date().toISOString(),
    sourceExcerpt: excerpt,
    excerptVerified: verifiedExcerpt,
    sourceType,
    provenance: provenance || null,
    providerResponseId: providerResponseId || null,
    /** Model-generated summary — never substitute for sourceExcerpt. */
    modelSummary: text(modelSummary) || null,
    evidenceStatus: verifiedExcerpt
      ? "EXCERPT_PRESENT"
      : url
        ? "URL_ONLY_UNVERIFIED"
        : "MISSING",
  };
}

/**
 * Map lab claimBuckets → claim evidence rows (fact vs inference explicit).
 */
export function claimBucketsToEvidence(buckets = {}, opts = {}) {
  const out = [];
  const retrievedAt = opts.retrievedAt || new Date().toISOString();
  const links = Array.isArray(opts.evidenceLinks) ? opts.evidenceLinks : [];
  const primaryUrl = links[0] || null;

  for (const claim of buckets.confirmed_facts || []) {
    out.push(
      buildClaimEvidence({
        claim,
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: primaryUrl,
        retrievedAt,
        sourceExcerpt: opts.excerptForFact || null,
        sourceType: SOURCE_TYPE.OFFICIAL,
        modelSummary: null,
      })
    );
  }
  for (const claim of buckets.inferences || []) {
    out.push(
      buildClaimEvidence({
        claim,
        claimKind: CLAIM_KIND.INFERENCE,
        sourceUrl: primaryUrl,
        retrievedAt,
        sourceExcerpt: null,
        sourceType: SOURCE_TYPE.OTHER,
        modelSummary: claim,
      })
    );
  }
  for (const claim of buckets.modeled_estimates || []) {
    out.push(
      buildClaimEvidence({
        claim,
        claimKind: CLAIM_KIND.MODELED_ESTIMATE,
        sourceUrl: primaryUrl,
        retrievedAt,
        sourceType: SOURCE_TYPE.OTHER,
        modelSummary: claim,
      })
    );
  }
  for (const claim of buckets.unknowns || []) {
    out.push(
      buildClaimEvidence({
        claim,
        claimKind: CLAIM_KIND.UNKNOWN,
        sourceUrl: null,
        retrievedAt,
        sourceExcerpt: null,
        evidenceStatus: "MISSING",
      })
    );
  }
  return out;
}

/**
 * Persist claim evidence onto an opportunity/candidate without inventing excerpts.
 */
export function attachClaimEvidence(target = {}, buckets = {}, opts = {}) {
  const evidence = claimBucketsToEvidence(buckets, {
    evidenceLinks: opts.evidenceLinks || target.evidenceLinks || [],
    retrievedAt: opts.retrievedAt,
  });
  return {
    ...target,
    claimEvidence: evidence,
    claimEvidenceVersion: CLAIM_EVIDENCE_VERSION,
    evidenceStats: {
      total: evidence.length,
      facts: evidence.filter((e) => e.claimKind === CLAIM_KIND.FACT).length,
      inferences: evidence.filter((e) => e.claimKind === CLAIM_KIND.INFERENCE).length,
      excerptVerified: evidence.filter((e) => e.excerptVerified).length,
      urlOnly: evidence.filter((e) => e.evidenceStatus === "URL_ONLY_UNVERIFIED").length,
      missing: evidence.filter((e) => e.evidenceStatus === "MISSING").length,
    },
  };
}
