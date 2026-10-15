/**
 * Customer-facing prose helpers for GDI drawers.
 * Evidence claim kinds (FACT / INFERENCE / MIXED) stay in structured metadata —
 * never as leaked prefixes in sales-readable paragraphs.
 */

const CLAIM_PREFIX_RE =
  /^(?:FACT(?:\/FIT-GATE)?|INFERENCE|MIXED|HYPOTHESIS|UNKNOWN)\s*:\s*/i;

const CLAIM_INLINE_RE =
  /\b(?:FACT(?:\/FIT-GATE)?|INFERENCE|MIXED|HYPOTHESIS)\s*:\s*/gi;

export function stripEvidenceClaimPrefixes(raw = "") {
  let t = String(raw || "").replace(/\s+/g, " ").trim();
  if (!t) return "";
  // Strip repeated leading prefixes
  while (CLAIM_PREFIX_RE.test(t)) {
    t = t.replace(CLAIM_PREFIX_RE, "").trim();
  }
  // Strip mid-sentence leaked labels (rare but seen after concatenation)
  t = t.replace(CLAIM_INLINE_RE, "").replace(/\s{2,}/g, " ").trim();
  return t;
}

/**
 * Normalize prose for near-duplicate detection across drawer sections.
 */
export function proseDedupeKey(raw = "", len = 96) {
  return stripEvidenceClaimPrefixes(raw)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, len);
}

/**
 * Collapse near-duplicate paragraphs while preserving distinct facts.
 * Later args are dropped when they substantially repeat an earlier one.
 */
export function distinctCustomerProse(...parts) {
  const seen = [];
  const out = [];
  for (const part of parts) {
    const cleaned = stripEvidenceClaimPrefixes(part);
    if (!cleaned || cleaned === "—") continue;
    if (
      /reviewed package did not run canonical|canonical (?:fit )?engine|canonical scoring|implementation note/i.test(
        cleaned
      )
    ) {
      continue;
    }
    const key = proseDedupeKey(cleaned);
    if (!key) continue;
    const isDup = seen.some(
      (prev) =>
        prev === key ||
        (key.length >= 48 && prev.includes(key.slice(0, 48))) ||
        (prev.length >= 48 && key.includes(prev.slice(0, 48)))
    );
    if (isDup) continue;
    seen.push(key);
    out.push(cleaned);
  }
  return out;
}

export function detectClaimKindFromProse(raw = "") {
  const t = String(raw || "").trim();
  if (/^INFERENCE\s*:/i.test(t)) return "INFERENCE";
  if (/^FACT(?:\/FIT-GATE)?\s*:/i.test(t)) return "FACT";
  if (/^MIXED\s*:/i.test(t)) return "MIXED";
  if (/^HYPOTHESIS\s*:/i.test(t)) return "HYPOTHESIS";
  return null;
}
