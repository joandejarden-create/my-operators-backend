/**
 * Wave-1 / provider-baseline multi-slot batches store one parent summary
 * (slots: GLOBAL_EN, CALA_EN, …) instead of Phase 2E per-geography cohorts.
 * Brand reads must project those slots onto geography + language filters.
 */

import { WAVE1_EXECUTION_ORDER } from "./wave1-showcase-plan.js";
import { normalizeLanguage } from "./language-dimension.js";

export const MULTI_SLOT_GEOGRAPHY_VERSION = "ai_visibility_multi_slot_geography_v1";

/**
 * @param {object|null|undefined} summary
 */
export function isMultiSlotBatchSummary(summary) {
  return Boolean(summary && summary.slots && typeof summary.slots === "object");
}

/**
 * @param {object|null|undefined} snap
 */
export function isWave1MultiMetricSnapshot(snap) {
  const scope = String(snap?.geographyScope || "").toLowerCase();
  const lang = String(snap?.language || "").toLowerCase();
  return scope === "wave1_multi" || lang === "multi";
}

function slotIsUsable(slotState) {
  if (!slotState || typeof slotState !== "object") return false;
  if (slotState.status === "completed") return true;
  return Number(slotState.succeeded || 0) > 0;
}

/**
 * @param {object} summary
 * @param {{ geographyScope?: string|null, commercialRegion?: string|null, region?: string|null, country?: string|null }} [geoFilter]
 * @param {{ language?: string|null, includeAllLanguages?: boolean }} [languageOpts]
 */
export function listMatchingSlots(summary, geoFilter = {}, languageOpts = {}) {
  if (!isMultiSlotBatchSummary(summary)) return [];

  const wantLang = languageOpts.includeAllLanguages
    ? null
    : normalizeLanguage(languageOpts.language) ||
      (languageOpts.language != null && String(languageOpts.language).trim()
        ? String(languageOpts.language).trim().toLowerCase()
        : null);

  const wantScope = geoFilter?.geographyScope
    ? String(geoFilter.geographyScope).trim()
    : "";
  const wantRegion = String(geoFilter?.commercialRegion || geoFilter?.region || "").trim();
  const wantCountry = geoFilter?.country ? String(geoFilter.country).trim() : "";

  return WAVE1_EXECUTION_ORDER.filter((slotDef) => {
    if (!slotIsUsable(summary.slots[slotDef.key])) return false;

    if (wantScope) {
      if (String(slotDef.geographyScope || "") !== wantScope) return false;
      if (wantRegion && String(slotDef.commercialRegion || "") !== wantRegion) return false;
      if (wantCountry && String(slotDef.country || "") !== wantCountry) return false;
    }

    if (wantLang && String(slotDef.language || "").toLowerCase() !== wantLang) return false;
    return true;
  });
}

/**
 * Languages present on usable slots matching a geography filter (multi-slot baselines).
 * @param {object} summary
 * @param {object} [geoFilter]
 * @returns {string[]}
 */
export function listLanguagesFromMultiSlotSummary(summary, geoFilter = {}) {
  const slots = listMatchingSlots(summary, geoFilter, { includeAllLanguages: true });
  return [
    ...new Set(
      slots.map((s) => normalizeLanguage(s.language)).filter(Boolean)
    ),
  ].sort();
}

/**
 * Whether a multi-slot summary should survive store-level list filters.
 * @param {object} summary
 * @param {object} [filter]
 */
export function multiSlotSummaryMatchesStoreFilter(summary, filter = {}) {
  if (!isMultiSlotBatchSummary(summary)) return false;
  const hasGeo =
    Boolean(filter.geographyScope) ||
    Boolean(filter.commercialRegion) ||
    Boolean(filter.region) ||
    Boolean(filter.country);
  const hasLang = Boolean(filter.language);
  if (!hasGeo && !hasLang) return true;
  return (
    listMatchingSlots(
      summary,
      {
        geographyScope: filter.geographyScope || null,
        commercialRegion: filter.commercialRegion || filter.region || null,
        country: filter.country || null,
      },
      {
        language: filter.language || null,
        includeAllLanguages: !filter.language,
      }
    ).length > 0
  );
}

/**
 * Attach projected cohort + matched slot keys for brand-read consumers.
 * @param {object} summary
 * @param {ReturnType<typeof listMatchingSlots>} matchedSlots
 * @param {string|null} [language]
 */
export function projectMultiSlotSummaryForRead(summary, matchedSlots, language = null) {
  if (!matchedSlots?.length) return summary;
  const primary = matchedSlots[0];
  const promptCount = matchedSlots.reduce((n, slotDef) => {
    const succeeded = Number(summary.slots?.[slotDef.key]?.succeeded);
    return n + (Number.isFinite(succeeded) && succeeded > 0 ? succeeded : 12);
  }, 0);
  const peerIds =
    summary.peerSet?.entityIds ||
    (summary.metrics?.competitivePosition?.peers || []).map((p) => p.entityId).filter(Boolean) ||
    [];

  return {
    ...summary,
    cohort: {
      geographyScope: primary.geographyScope,
      commercialRegion: primary.commercialRegion,
      country: primary.country,
      language: language || primary.language,
      promptCount,
      entityIds: peerIds,
    },
    language: language || primary.language,
    _matchedSlotKeys: matchedSlots.map((s) => s.key),
    _metricScope: "wave1_parent_aggregate",
    METRIC_SCOPE_NOTE:
      "Headline KPIs are from the full multi-slot baseline (all slots). Question and evidence lists are filtered to the selected geography/language slot(s).",
  };
}

/**
 * @param {string|null|undefined} runSlot
 * @param {string[]|null|undefined} matchedSlotKeys
 */
export function runMatchesMatchedSlots(runSlot, matchedSlotKeys) {
  if (!matchedSlotKeys || !matchedSlotKeys.length) return true;
  if (runSlot == null || runSlot === "") return false;
  return matchedSlotKeys.includes(String(runSlot));
}

/**
 * Derive WAVE1-style slot key from run geographyKey + language when run.slot is absent
 * (provider baselines persist geographyKey/language instead of slot).
 * @param {object|null|undefined} run
 * @returns {string|null}
 */
export function deriveRunSlotKey(run) {
  if (!run || typeof run !== "object") return null;
  if (run.slot != null && String(run.slot).trim()) return String(run.slot);

  const lang =
    normalizeLanguage(run.language) ||
    normalizeLanguage(run.payload?.language) ||
    "en";
  const geoRaw = String(run.geographyKey || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (!geoRaw) return null;

  const suffix = lang === "es" ? "ES" : "EN";
  const candidate = `${geoRaw}_${suffix}`;
  if (WAVE1_EXECUTION_ORDER.some((s) => s.key === candidate)) return candidate;

  // Map commercialRegion-style labels (e.g. "Europe" → EUROPE_EN).
  const byRegion = WAVE1_EXECUTION_ORDER.find((s) => {
    if (normalizeLanguage(s.language) !== lang) return false;
    if (s.country) {
      return (
        String(s.country).toUpperCase().replace(/\s+/g, "_") === geoRaw ||
        String(s.key).replace(/_(EN|ES)$/i, "").toUpperCase() === geoRaw
      );
    }
    const region = String(s.commercialRegion || "")
      .trim()
      .toUpperCase()
      .replace(/\s+/g, "_");
    const scope = String(s.geographyScope || "")
      .trim()
      .toUpperCase()
      .replace(/\s+/g, "_");
    return region === geoRaw || scope === geoRaw;
  });
  return byRegion?.key || null;
}

/**
 * Whether a run belongs to any of the matched multi-slot keys.
 * Supports OpenAI runs with run.slot and baseline runs with geographyKey+language.
 * @param {object|null|undefined} run
 * @param {string[]|null|undefined} matchedSlotKeys
 */
export function runMatchesSlotFilter(run, matchedSlotKeys) {
  if (!matchedSlotKeys || !matchedSlotKeys.length) return true;
  const slotKey = deriveRunSlotKey(run);
  return runMatchesMatchedSlots(slotKey, matchedSlotKeys);
}

/**
 * Resolve monitoring language from a batch run slot key (e.g. CALA_ES → es).
 * @param {string|null|undefined} runSlot
 * @returns {"en"|"es"|null}
 */
export function languageFromRunSlot(runSlot) {
  if (runSlot == null || runSlot === "") return null;
  const key = String(runSlot);
  const def = WAVE1_EXECUTION_ORDER.find((s) => s.key === key);
  if (def?.language) return normalizeLanguage(def.language);
  if (/_ES(?:_|$)/i.test(key)) return "es";
  if (/_EN(?:_|$)/i.test(key)) return "en";
  return null;
}
