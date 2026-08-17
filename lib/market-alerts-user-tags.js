/**
 * User-facing tag sanitizer — suppress internal ingestion/debug tags.
 */

const INTERNAL_TAG_PATTERNS = [
  /^RSS$/i,
  /^GOOGLE_NEWS$/i,
  /^EARLY_SIGNAL$/i,
  /^EARLY_SIGNAL_/i,
  /^QUERY_FAMILY_/i,
  /^INGEST_/i,
  /^DEBUG_/i,
];

/**
 * @param {string} tag
 * @returns {boolean}
 */
export function isInternalMarketAlertTag(tag) {
  const t = String(tag || "").trim();
  if (!t) return true;
  return INTERNAL_TAG_PATTERNS.some((re) => re.test(t));
}

/**
 * @param {string[]|null|undefined} tags
 * @returns {string[]}
 */
export function sanitizeUserFacingTags(tags) {
  if (!Array.isArray(tags)) return [];
  return tags.filter((t) => !isInternalMarketAlertTag(t));
}

/**
 * Optional human label when a query family must be shown (prefer hiding in V1.3).
 * @param {string} tag
 * @returns {string|null}
 */
export function translateQueryFamilyTag(tag) {
  const t = String(tag || "").trim();
  if (/^EARLY_SIGNAL_DEVELOPMENT$/i.test(t)) return "Early Development";
  if (/^EARLY_SIGNAL_MIXED_USE$/i.test(t)) return "Mixed Use";
  if (/^EARLY_SIGNAL_PLANNING$/i.test(t)) return "Planning";
  if (/^EARLY_SIGNAL_ADAPTIVE_REUSE$/i.test(t)) return "Adaptive Reuse";
  return null;
}
