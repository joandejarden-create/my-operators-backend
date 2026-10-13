/**
 * Synthetic / provider-index test-result detection for ownership SERP triage.
 * Separators (hyphen, underscore, %, etc.) normalize to spaces so URL-path
 * artifacts match the same labels as titled snippets.
 * Bare "CNPJ" or "Brazil" alone is NOT synthetic.
 */

const SYNTHETIC_LABEL_RE =
  /\bCNPJ\s+Test(?:\s+II)?(?:\s+Numeric)?\b|\bBrazil\s+CNPJ\s+Test(?:\s+II)?(?:\s+Numeric)?\b|\bplaceholder\s+cnpj\b|\bdemo\s+cnpj\b/i;

export function normalizeSyntheticDetectionBlob(text = "") {
  let s = String(text || "");
  try {
    s = decodeURIComponent(s);
  } catch {
    /* keep raw if malformed encoding */
  }
  return s
    .replace(/[-_+/\\|.]+/g, " ")
    .replace(/%20/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {{ title?: string, snippet?: string, url?: string }} row
 */
export function isSyntheticOrTestResult(row = {}) {
  const raw = `${row.title || ""} ${row.snippet || ""} ${row.url || ""}`;
  const normalized = normalizeSyntheticDetectionBlob(raw);
  return SYNTHETIC_LABEL_RE.test(raw) || SYNTHETIC_LABEL_RE.test(normalized);
}

export { SYNTHETIC_LABEL_RE };
