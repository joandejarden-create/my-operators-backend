/**
 * Strip demo/meta disclaimers from user-facing Deal Setup field values.
 * Internal metadata (fieldSources, sourceType) is not passed through here.
 */

const TRAILING_DISCLAIMER_RE =
  /\s*[\(\—\-–]\s*(?:fictional|sample|demo|needs validation|public class|inferred|public listing class|public listing)[^)\]]*[\)\]]/gi;

const INLINE_DISCLAIMER_RE =
  /\b(?:fictional|demo)\s+(?:sample|operator|GC|design firm|—)?\s*/gi;

const SUFFIX_SAMPLE_RE = /\s*[\(\—\-–,]\s*sample\s*[\)\]]?/gi;

const FULL_LINE_BOILERPLATE = [
  /^Fictional demo opportunity for Dealality product workflows.*$/i,
  /^Sample deal for product demonstration only.*$/i,
];

/**
 * @param {string} text
 * @returns {string}
 */
export function sanitizeDemoIntakeCopy(text) {
  if (typeof text !== "string") return text;
  let s = text.trim();
  if (!s) return s;

  for (const re of FULL_LINE_BOILERPLATE) {
    if (re.test(s)) return "";
  }

  // Repeated passes for nested parentheses e.g. "(fictional operator — sample)"
  for (let i = 0; i < 4; i++) {
    const next = s
      .replace(TRAILING_DISCLAIMER_RE, "")
      .replace(SUFFIX_SAMPLE_RE, "")
      .replace(INLINE_DISCLAIMER_RE, "")
      .replace(/\s*\(sample\)/gi, "")
      .replace(/\s*—\s*sample\b/gi, "")
      .replace(/\s*-\s*sample\b/gi, "")
      .replace(/\s*\bdemo\b\s*$/gi, "")
      .replace(/\s*\(demo\)/gi, "")
      .replace(/\s*\(fictional\)/gi, "");
    if (next === s) break;
    s = next;
  }

  s = s
    .replace(/\s{2,}/g, " ")
    .replace(/\s+,/g, ",")
    .replace(/\s+;/g, ";")
    .replace(/\s+\./g, ".")
    .trim();

  // Clean dangling em-dash fragments
  s = s.replace(/\s*[\—\-–]\s*$/g, "").trim();

  // Street / address placeholders
  s = s.replace(/\bDemo\b/gi, "").replace(/\s{2,}/g, " ").trim();
  s = s.replace(/\bLote\s+(\d)/i, "Lote $1");

  return s;
}

/**
 * @param {unknown} value
 * @returns {unknown}
 */
export function sanitizeDemoIntakeValue(value) {
  if (typeof value === "string") return sanitizeDemoIntakeCopy(value);
  if (Array.isArray(value)) {
    return value.map((v) => sanitizeDemoIntakeCopy(String(v))).filter(Boolean);
  }
  return value;
}

/**
 * @param {Record<string, unknown>} fields
 * @returns {Record<string, unknown>}
 */
export function sanitizeDemoIntakeFields(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    const cleaned = sanitizeDemoIntakeValue(v);
    if (cleaned === "" || cleaned === undefined) continue;
    if (Array.isArray(cleaned) && cleaned.length === 0) continue;
    out[k] = cleaned;
  }
  return out;
}

const SANITIZE_SKIP_KEYS = new Set([
  "sampleId",
  "sourceType",
  "layer",
  "reviewSetSource",
  "sampleTier",
]);

function sanitizeEmailString(s) {
  return String(s || "").replace(/@dealality\.sample$/i, "@hospitalitygroup.com");
}

/**
 * Deep-sanitize user-facing strings in nested fixture/config objects.
 * @param {unknown} value
 * @param {string} [key]
 * @returns {unknown}
 */
export function sanitizeDemoIntakeDeep(value, key = "") {
  if (typeof value === "string") {
    let s = sanitizeDemoIntakeCopy(value);
    if (/email/i.test(key) || /@dealality\.sample/i.test(s)) {
      s = sanitizeEmailString(s);
    }
    return s;
  }
  if (Array.isArray(value)) {
    return value
      .map((v, i) => sanitizeDemoIntakeDeep(v, key))
      .filter((v) => v !== "" && v != null);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (SANITIZE_SKIP_KEYS.has(k)) {
        out[k] = v;
        continue;
      }
      const cleaned = sanitizeDemoIntakeDeep(v, k);
      if (cleaned === "" || cleaned === undefined) continue;
      if (Array.isArray(cleaned) && cleaned.length === 0) continue;
      out[k] = cleaned;
    }
    return out;
  }
  return value;
}

/** @param {object} cfg — build-cala-sample-deals config */
export function sanitizeCalaDealConfig(cfg) {
  return /** @type {typeof cfg} */ (sanitizeDemoIntakeDeep(cfg));
}
