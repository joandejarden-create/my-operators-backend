/**
 * Source evidence URL dedupe V1 (run-049).
 *
 * Identical / near-identical URLs must not inflate independent corroboration counts.
 * Distinct passages from the same URL may be preserved as claims.
 */

import { canonicalSourceKey } from "./commercial-source-selection-v1.js";

export const SOURCE_EVIDENCE_DEDUPE_V1 = "gdi-source-evidence-dedupe-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Normalize a URL for independent-source counting.
 */
export function normalizeEvidenceUrl(url = "") {
  const raw = text(url);
  if (!raw) return "";
  try {
    return canonicalSourceKey(raw) || raw.toLowerCase().replace(/\/+$/, "");
  } catch {
    return raw.toLowerCase().replace(/\/+$/, "");
  }
}

/**
 * Deduplicate evidence / source arrays by canonical URL while preserving the
 * strongest claim text per URL (longest non-empty statement/quote).
 */
export function dedupeEvidenceByCanonicalUrl(items = []) {
  const byKey = new Map();
  const order = [];
  for (const item of items || []) {
    if (!item) continue;
    const url = item.url || item.sourceUrl || item.href || "";
    const key = normalizeEvidenceUrl(url) || `no_url:${text(item.statement || item.quote || item.id).slice(0, 40)}`;
    const statement = text(item.statement || item.quote || item.excerpt || "");
    const prior = byKey.get(key);
    if (!prior) {
      byKey.set(key, { ...item, url: url || item.url, _canonicalSourceKey: key });
      order.push(key);
      continue;
    }
    const priorStatement = text(prior.statement || prior.quote || prior.excerpt || "");
    if (statement.length > priorStatement.length) {
      byKey.set(key, {
        ...prior,
        ...item,
        url: url || prior.url,
        statement: statement || priorStatement,
        _canonicalSourceKey: key,
        _dedupedPassageCount: (prior._dedupedPassageCount || 1) + 1,
      });
    } else {
      byKey.set(key, {
        ...prior,
        _dedupedPassageCount: (prior._dedupedPassageCount || 1) + 1,
      });
    }
  }
  return {
    version: SOURCE_EVIDENCE_DEDUPE_V1,
    items: order.map((k) => byKey.get(k)),
    uniqueSourceCount: order.filter((k) => !k.startsWith("no_url:")).length,
    rawCount: (items || []).length,
  };
}

/**
 * Independent source count for confidence — never raw URL list length.
 */
export function countIndependentSources(items = []) {
  return dedupeEvidenceByCanonicalUrl(items).uniqueSourceCount;
}
