/**
 * Canonical owner-domain resolver (Contact Resolution V2).
 * Never overwrites higher-confidence domain with weaker result.
 */

import { CACHE_TTL } from "./vocabulary.js";
import { SOURCE_CLASS } from "./vocabulary.js";

export const OWNER_DOMAIN_RESOLVER_V2 = "owner-domain-resolver-v2";

const REJECT_HOST_RE =
  /(?:^|\.)(?:tripadvisor|booking\.com|expedia|hotels\.com|hotel\.com|facebook|instagram|linkedin|twitter|x\.com|youtube|wikipedia|yelp|google|maps\.app|marriott\.com|hilton\.com|hyatt\.com|ihg\.com|wyndhamhotels|choicehotels|accor\.com|radissonhotels|bestwestern|kayak|atrapalo|despegar)\b/i;

export function isRejectedOwnerDomain(hostOrUrl) {
  try {
    const host = String(hostOrUrl).includes("://")
      ? new URL(hostOrUrl).hostname
      : String(hostOrUrl || "");
    return REJECT_HOST_RE.test(host.replace(/^www\./, ""));
  } catch {
    return true;
  }
}

export function normalizeDomain(hostOrUrl) {
  try {
    const host = String(hostOrUrl).includes("://")
      ? new URL(hostOrUrl).hostname
      : String(hostOrUrl || "");
    return host.replace(/^www\./i, "").toLowerCase().trim() || null;
  } catch {
    return null;
  }
}

/**
 * Confidence ranks — higher wins; never overwrite with weaker.
 */
export const DOMAIN_CONFIDENCE_RANK = Object.freeze({
  VERIFIED_STORED: 100,
  OFFICIAL_EVIDENCE: 80,
  RESEARCH_DISCOVERED: 50,
  CANDIDATE: 20,
});

export function resolveOwnerDomain({
  stored_domain = null,
  stored_confidence = null,
  evidence_domains = [],
  discovered_domain = null,
  brand_is_owner = false,
} = {}) {
  const candidates = [];

  if (stored_domain && !isRejectedOwnerDomain(stored_domain)) {
    candidates.push({
      domain: normalizeDomain(stored_domain),
      source: "stored_verified",
      confidence: stored_confidence || "VERIFIED_STORED",
      rank: DOMAIN_CONFIDENCE_RANK.VERIFIED_STORED,
      source_class: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
    });
  }

  for (const e of evidence_domains || []) {
    const d = normalizeDomain(e.domain || e.url || e);
    if (!d || isRejectedOwnerDomain(d)) continue;
    // Brand central only if owner IS the brand
    if (/marriott|hilton|hyatt|ihg|wyndham|choice|accor/i.test(d) && !brand_is_owner) continue;
    candidates.push({
      domain: d,
      source: e.source || "evidence",
      confidence: e.confidence || "OFFICIAL_EVIDENCE",
      rank: DOMAIN_CONFIDENCE_RANK.OFFICIAL_EVIDENCE,
      source_class: e.source_class || SOURCE_CLASS.OFFICIAL_OWNER_SITE,
    });
  }

  if (discovered_domain && !isRejectedOwnerDomain(discovered_domain)) {
    const d = normalizeDomain(discovered_domain);
    if (d) {
      candidates.push({
        domain: d,
        source: "research_discovery",
        confidence: "RESEARCH_DISCOVERED",
        rank: DOMAIN_CONFIDENCE_RANK.RESEARCH_DISCOVERED,
        source_class: SOURCE_CLASS.OTHER_PUBLIC_SOURCE,
      });
    }
  }

  candidates.sort((a, b) => b.rank - a.rank);
  const best = candidates[0] || null;
  return {
    ok: Boolean(best?.domain),
    domain: best?.domain || null,
    source: best?.source || null,
    confidence: best?.confidence || null,
    source_class: best?.source_class || null,
    verified_at: best ? new Date().toISOString() : null,
    candidates: candidates.slice(0, 8),
    rejected: (evidence_domains || [])
      .map((e) => normalizeDomain(e.domain || e.url || e))
      .filter((d) => d && isRejectedOwnerDomain(d)),
    cache_ttl_ms: CACHE_TTL.domain_ms,
  };
}
