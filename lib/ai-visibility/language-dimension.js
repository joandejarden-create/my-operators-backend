/**
 * AI Visibility — first-class language dimension (Phase 3A.6).
 * Canonical internal values: en | es. Geography stays separate (never es-MX).
 */

export const AI_VISIBILITY_LANGUAGE_VERSION = "ai_visibility_language_v1";

export const AI_VISIBILITY_LANGUAGES = Object.freeze(["en", "es"]);

export const AI_VISIBILITY_LANGUAGE_DISPLAY = Object.freeze({
  en: "English",
  es: "Spanish",
});

export const NON_COMPARABLE_LANGUAGE = "NON_COMPARABLE_LANGUAGE";

const ALIASES = Object.freeze({
  en: "en",
  english: "en",
  eng: "en",
  es: "es",
  spanish: "es",
  español: "es",
  espanol: "es",
  spa: "es",
});

/**
 * @param {unknown} raw
 * @returns {"en"|"es"|null}
 */
export function normalizeLanguage(raw) {
  if (raw == null || raw === "") return null;
  const key = String(raw).trim().toLowerCase();
  if (!key) return null;
  // Reject locale tags as language values (es-MX, en-US, …)
  if (key.includes("-") || key.includes("_")) return null;
  return ALIASES[key] || null;
}

/**
 * @param {unknown} raw
 * @returns {boolean}
 */
export function isSupportedAiVisibilityLanguage(raw) {
  return normalizeLanguage(raw) != null;
}

/**
 * @param {unknown} raw
 * @returns {string}
 */
export function getLanguageDisplayLabel(raw) {
  const n = normalizeLanguage(raw);
  if (!n) return "—";
  return AI_VISIBILITY_LANGUAGE_DISPLAY[n] || n;
}

/**
 * Reject unsupported language strings (including locales).
 * @param {unknown} raw
 * @returns {{ ok: true, language: "en"|"es" } | { ok: false, reasonCode: string, message: string }}
 */
export function requireSupportedLanguage(raw) {
  const n = normalizeLanguage(raw);
  if (n) return { ok: true, language: n };
  return {
    ok: false,
    reasonCode: "UNSUPPORTED_LANGUAGE",
    message:
      "Language must be a supported AI Visibility value (en or es). Locales like es-MX are not language values.",
  };
}

/**
 * Effective language on a stored record.
 * Legacy rows with missing language are treated as English for read matching only
 * when `treatMissingAsEn` is true (default for English-era history).
 *
 * @param {{ language?: string|null }|null|undefined} record
 * @param {{ treatMissingAsEn?: boolean }} [opts]
 * @returns {"en"|"es"|null}
 */
export function resolveRecordLanguage(record, opts = {}) {
  const treatMissingAsEn = opts.treatMissingAsEn !== false;
  const n = normalizeLanguage(record?.language);
  if (n) return n;
  if (treatMissingAsEn && (record?.language == null || record?.language === "")) {
    return "en";
  }
  return null;
}

/**
 * Does a stored record match the requested language filter?
 * @param {{ language?: string|null }|null|undefined} record
 * @param {"en"|"es"|null|undefined} want
 * @param {{ treatMissingAsEn?: boolean }} [opts]
 */
export function recordMatchesLanguage(record, want, opts = {}) {
  if (!want) return true;
  const got = resolveRecordLanguage(record, opts);
  return got === want;
}

/**
 * Resolve request language against available monitored languages.
 *
 * - Explicit request that is unmonitored → not_monitored (never silent fallback)
 * - Omitted + exactly one available → use it (backward compatible)
 * - Omitted + multiple available → language_required (UI should send explicit; product default en only via UI selection)
 * - Omitted + none available → not_monitored
 *
 * @param {{ requested?: unknown, availableLanguages?: string[] }} args
 */
export function resolveReadLanguage(args = {}) {
  const available = [
    ...new Set(
      (args.availableLanguages || [])
        .map((l) => normalizeLanguage(l))
        .filter(Boolean)
    ),
  ].sort();

  const hasExplicit =
    args.requested != null && String(args.requested).trim() !== "";

  if (hasExplicit) {
    const req = requireSupportedLanguage(args.requested);
    if (!req.ok) return { ...req, availableLanguages: available };
    if (!available.includes(req.language)) {
      return {
        ok: true,
        language: req.language,
        status: "not_monitored",
        availableLanguages: available,
        SILENT_LANGUAGE_FALLBACK: false,
      };
    }
    return {
      ok: true,
      language: req.language,
      status: "ok",
      availableLanguages: available,
      SILENT_LANGUAGE_FALLBACK: false,
    };
  }

  if (available.length === 1) {
    return {
      ok: true,
      language: available[0],
      status: "ok",
      availableLanguages: available,
      SILENT_LANGUAGE_FALLBACK: false,
      resolvedFrom: "sole_available",
    };
  }
  if (available.length === 0) {
    return {
      ok: true,
      language: null,
      status: "not_monitored",
      availableLanguages: available,
      SILENT_LANGUAGE_FALLBACK: false,
      resolvedFrom: "none_available",
    };
  }
  return {
    ok: false,
    reasonCode: "LANGUAGE_REQUIRED",
    message:
      "Multiple monitored languages are available. Pass language=en or language=es explicitly.",
    availableLanguages: available,
    SILENT_LANGUAGE_FALLBACK: false,
  };
}

/**
 * List completed languages for a provider × geography from summaries/batches.
 * @param {object[]} records — summaries or batches with language (+ legacy missing→en)
 */
export function listLanguagesFromMonitoringRecords(records = []) {
  const set = new Set();
  for (const r of records || []) {
    const status = r.status || r.batchStatus;
    if (status && !["completed", "partial"].includes(status)) continue;
    const lang = resolveRecordLanguage(r, { treatMissingAsEn: true });
    if (lang) set.add(lang);
  }
  return [...set].sort();
}

/**
 * UI filter contract: show selector only when >1 language available.
 */
export function buildLanguageFilterContract(availableLanguages = []) {
  const available = [
    ...new Set(
      (availableLanguages || []).map((l) => normalizeLanguage(l)).filter(Boolean)
    ),
  ].sort();
  return {
    availableLanguages: available,
    options: available.map((code) => ({
      value: code,
      label: getLanguageDisplayLabel(code),
    })),
    visible: available.length > 1,
    ALL_LANGUAGES_OPTION: false,
    /** Explicit product default for initial UI selection when multiple exist. */
    defaultSelection: available.includes("en")
      ? "en"
      : available[0] || null,
  };
}
