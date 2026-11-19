/**
 * Treat Exa/Parallel search hits as discovery candidates — never verified
 * ownership facts. Independent acceptance requires separate evidence checks
 * + HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1 classifier.
 */

import { isSafeHttpUrl } from "./evidence.js";

export const DISCOVERY_CANDIDATE_KIND = "DISCOVERY_CANDIDATE";

/**
 * Normalize a retrieved hit into a discovery candidate (verified: false).
 */
export function toDiscoveryCandidate(hit = {}, opts = {}) {
  const url = hit.url || hit.sourceUrl || null;
  const urlOk = isSafeHttpUrl(url);
  return {
    kind: DISCOVERY_CANDIDATE_KIND,
    claimType: hit.claimType || "OWNERSHIP_STRUCTURE_EDGE",
    value: hit.title || hit.value || hit.excerpt || "discovery_candidate",
    url: urlOk ? url : null,
    excerpt: hit.excerpt ? String(hit.excerpt).slice(0, 400) : null,
    hotelHint: opts.hotelName || hit.hotelHint || null,
    isSeedFact: false,
    verified: false,
    linkedToHpc: false,
    inferred: false,
    unsupported: true,
    independentAcceptance: {
      ok: false,
      reason: "DISCOVERY_ONLY_REQUIRES_INDEPENDENT_ACCEPTANCE",
      urlPresent: urlOk,
      urlAloneNotVerification: true,
    },
    note: "DISCOVERY_CANDIDATE_NOT_VERIFIED_OWNERSHIP",
  };
}

/**
 * Independent acceptance gate — URL presence is insufficient.
 */
export function independentlyAcceptOwnershipClaim(claim = {}, checks = {}) {
  const urlOk = isSafeHttpUrl(claim.url || claim.evidenceUrl);
  if (!urlOk) {
    return { ok: false, reason: "MISSING_OR_INVALID_EVIDENCE_URL" };
  }
  if (claim.kind === DISCOVERY_CANDIDATE_KIND && checks.promoted !== true) {
    return { ok: false, reason: "STILL_DISCOVERY_CANDIDATE" };
  }
  if (checks.linksToHotel !== true) {
    return { ok: false, reason: "NOT_LINKED_TO_HOTEL" };
  }
  if (checks.ownershipRelationshipSupported !== true) {
    return { ok: false, reason: "OWNERSHIP_RELATIONSHIP_UNSUPPORTED" };
  }
  if (checks.secondSourceOrPrimaryDoc !== true && checks.primaryDocument !== true) {
    return { ok: false, reason: "NO_INDEPENDENT_CORROBORATION" };
  }
  if (claim.inferred === true) {
    return { ok: false, reason: "INFERRED_FORBIDDEN" };
  }
  return {
    ok: true,
    verified: true,
    evidenceUrl: claim.url || claim.evidenceUrl,
    linkedToHpc: true,
  };
}

/**
 * Map Parallel Task output text/citations into discovery candidates +
 * provisional structured fields (still unverified until acceptance).
 */
export function extractParallelTaskDiscovery(output = {}, opts = {}) {
  const citations = Array.isArray(output.citations)
    ? output.citations
    : Array.isArray(output.basis)
      ? output.basis
      : [];
  const candidates = citations
    .map((c) =>
      toDiscoveryCandidate(
        {
          url: c.url || c.source_url,
          title: c.title || c.name,
          excerpt: c.excerpt || c.snippet || c.text,
        },
        opts
      )
    )
    .filter((c) => c.url);

  const text = String(output.content || output.output || output.result || "");
  return {
    candidates,
    rawTextExcerpt: text.slice(0, 2000),
    task_completed: true,
    ownership_claims_verified: false,
    note: "Task output retained as discovery; classifier requires evidence acceptance",
  };
}
