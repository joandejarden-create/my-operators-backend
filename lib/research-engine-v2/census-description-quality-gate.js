/**
 * Hotel description quality gate — public vs internal copy.
 */

export const DESCRIPTION_QUALITY_GATE_VERSION =
  "census-description-quality-gate-v1";

/** Forbidden in public / owner-facing descriptions. */
export const PUBLIC_INTERNAL_TERMS_RE =
  /\b(census only|human review|radar hold|public hold|brand governance|source-supported|official inventory|ai-assisted|ai assisted|source evidence|dealality census|under source review|pending brand|not owner-facing|enrichment status|gap ledger)\b/i;

/** Forbidden marketing / unsupported adjectives. */
export const FORBIDDEN_HYPE_RE =
  /\b(world-class|iconic|premier|perfectly located|offers guests|luxury|luxurious|stunning|breathtaking|unparalleled|best-in-class|5-star|five-star)\b/i;

/** Location claims that require supporting evidence in fields. */
export const LOCATION_CLAIM_RE =
  /\b(beachfront|oceanfront|waterfront|airport|downtown|historic district|city center|centro historico|centro histórico)\b/i;

/**
 * @param {string} text
 * @param {{ allowInternalTerms?: boolean, allowLocationClaims?: boolean, locationSupported?: boolean, luxurySupported?: boolean }} [opts]
 */
export function evaluateDescriptionQuality(text, opts = {}) {
  const s = String(text || "").trim();
  const failures = [];
  if (!s) failures.push("blank");
  if (s.length > 420) failures.push("too_long");
  const sentences = s.split(/(?<=[.!?])\s+/).filter(Boolean);
  if (sentences.length > 2) failures.push("more_than_two_sentences");

  if (!opts.allowInternalTerms && PUBLIC_INTERNAL_TERMS_RE.test(s)) {
    failures.push("internal_process_terms");
  }
  if (FORBIDDEN_HYPE_RE.test(s)) {
    if (/\bluxury|luxurious\b/i.test(s) && opts.luxurySupported) {
      // allow luxury only when supported
    } else {
      failures.push("unsupported_hype_adjective");
    }
  }
  if (LOCATION_CLAIM_RE.test(s) && !opts.locationSupported && !opts.allowLocationClaims) {
    failures.push("unsupported_location_claim");
  }
  if (/\b(ADR|RevPAR|fee|contract|owner|operator|developer)\b/i.test(s)) {
    failures.push("forbidden_commercial_claim");
  }

  return {
    ok: failures.length === 0,
    version: DESCRIPTION_QUALITY_GATE_VERSION,
    failures,
    sentence_count: sentences.length,
  };
}

/**
 * Strip internal terms for a public rewrite attempt (conservative).
 */
export function stripInternalTerms(text) {
  return String(text || "")
    .replace(PUBLIC_INTERNAL_TERMS_RE, "")
    .replace(/\s{2,}/g, " ")
    .replace(/\s+([.,])/g, "$1")
    .trim();
}
