/**
 * Presentation boundary for Market Alerts.
 * Internal provenance (Tags, family IDs) stays in Airtable.
 * User-facing source labels and metadata must never leak technical suffixes.
 */

const INTERNAL_PAREN_SUFFIX =
  /\s*\((?:EARLY_SIGNAL(?:_[A-Z0-9]+)*|RSS|GOOGLE_NEWS(?:_[A-Z0-9]+)*|QUERY_FAMILY_[A-Z0-9_]+|INGEST_[A-Z0-9_]+|DEBUG_[A-Z0-9_]+)\)\s*/gi;

const INTERNAL_TOKEN =
  /\b(?:EARLY_SIGNAL(?:_[A-Z0-9]+)*|GOOGLE_NEWS(?:_[A-Z0-9]+)*|QUERY_FAMILY_[A-Z0-9_]+)\b/g;

const INTERNAL_STANDALONE_TAG = /^(?:RSS|GOOGLE_NEWS|EARLY_SIGNAL|EARLY_SIGNAL_[A-Z0-9_]+|QUERY_FAMILY_[A-Z0-9_]+|INGEST_[A-Z0-9_]+|DEBUG_[A-Z0-9_]+)$/i;

/**
 * @param {string|null|undefined} raw
 * @returns {string}
 */
export function getUserFacingSourceName(raw) {
  let s = String(raw || "").trim();
  if (!s) return "";
  s = s.replace(INTERNAL_PAREN_SUFFIX, " ");
  s = s.replace(INTERNAL_TOKEN, " ");
  s = s.replace(/\s{2,}/g, " ").replace(/\s+([,·•|/])/g, "$1").trim();
  s = s.replace(/[(\[]\s*[)\]]/g, "").trim();
  return s;
}

/**
 * True when a display string still contains technical family/system tokens.
 * Does not flag ordinary publisher names that happen to contain "Early" or "Signal".
 * @param {string|null|undefined} text
 */
export function userFacingTextHasInternalMetadata(text) {
  const s = String(text || "");
  if (!s) return false;
  if (/\bEARLY_SIGNAL(?:_[A-Z0-9]+)*\b/.test(s)) return true;
  if (/\bGOOGLE_NEWS(?:_[A-Z0-9]+)*\b/.test(s)) return true;
  if (/\bQUERY_FAMILY_[A-Z0-9_]+\b/.test(s)) return true;
  if (/\(\s*RSS\s*\)/i.test(s)) return true;
  if (INTERNAL_STANDALONE_TAG.test(s.trim())) return true;
  return false;
}

/**
 * Sanitize any user-visible metadata string (source, card meta, drawer meta).
 * @param {string|null|undefined} text
 */
export function sanitizeUserFacingMetadata(text) {
  return getUserFacingSourceName(text);
}
