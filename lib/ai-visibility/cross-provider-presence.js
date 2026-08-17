/**
 * Cross-provider Presence intelligence — DERIVED All Providers view.
 * Provider-specific observations remain primary source of truth.
 * Never creates an All Providers run / provider record / arbitrary score.
 */

import {
  ALL_PROVIDERS_SELECTOR_ID,
  formatProviderLabel,
  normalizeProviderId,
} from "./provider-dimension.js";

export const CROSS_PROVIDER_PRESENCE_VERSION =
  "ai_visibility_cross_provider_presence_v1";

export { ALL_PROVIDERS_SELECTOR_ID };

export const CROSS_PROVIDER_COMPARABILITY_FIELDS = Object.freeze([
  "prompt",
  "promptFamily",
  "geography",
  "language",
  "monitoringWindow",
]);

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isAllProvidersSelector(value) {
  const id = normalizeProviderId(value);
  return id === ALL_PROVIDERS_SELECTOR_ID || id === "all_providers";
}

/**
 * Comparability check for aggregating provider observations.
 * Geography + language + prompt cohort must align.
 * Monitoring window may differ across providers (expected for All Providers derived view).
 * @param {Array<object>} observations — per-provider presence observations
 */
export function assertCrossProviderComparability(observations = []) {
  const rows = (observations || []).filter(Boolean);
  if (rows.length < 2) {
    return {
      comparable: rows.length === 1,
      status: rows.length === 1 ? "SINGLE_PROVIDER" : "INSUFFICIENT_PROVIDERS",
      NOT_COMPARABLE: rows.length !== 1,
      reason:
        rows.length === 0
          ? "No provider observations"
          : "Only one provider observation — cross-provider aggregate not applicable",
      windowDrift: false,
    };
  }

  const keys = rows.map((r) => {
    const geography = String(r.geography || r.geographyKey || "").trim().toLowerCase();
    const language = String(r.language || "").trim().toLowerCase();
    const window = String(r.monitoringWindow || r.periodId || r.batchId || "").trim();
    const cohort = String(
      r.promptCohortKey ||
        r.comparabilityKey ||
        (r.promptFamily && r.promptFamily !== "portfolio_presence" ? r.promptFamily : "") ||
        r.promptId ||
        ""
    ).trim();
    return {
      geography,
      language,
      window,
      cohort,
      // Hard gate excludes monitoring window — providers complete on different days.
      hard: [cohort, geography, language].join("|"),
      raw: [cohort, geography, language, window].join("|"),
    };
  });

  const missingGeo = keys.some((k) => !k.geography);
  const missingLang = keys.some((k) => !k.language);
  if (missingGeo || missingLang) {
    return {
      comparable: false,
      status: "NOT_COMPARABLE",
      NOT_COMPARABLE: true,
      reason: "Missing required comparability fields (geography and/or language)",
      fields: CROSS_PROVIDER_COMPARABILITY_FIELDS,
      windowDrift: false,
    };
  }

  // Reject synthetic portfolio_presence as false comparability glue.
  const synthetic = rows.some(
    (r) => String(r.promptFamily || "") === "portfolio_presence" && !r.promptCohortKey && !r.comparabilityKey
  );
  if (synthetic) {
    return {
      comparable: false,
      status: "NOT_COMPARABLE",
      NOT_COMPARABLE: true,
      reason: "Synthetic promptFamily=portfolio_presence is not a valid comparability cohort",
      fields: CROSS_PROVIDER_COMPARABILITY_FIELDS,
      windowDrift: false,
    };
  }

  const uniqueHard = new Set(keys.map((k) => k.hard));
  if (uniqueHard.size > 1) {
    return {
      comparable: false,
      status: "NOT_COMPARABLE",
      NOT_COMPARABLE: true,
      reason:
        "Provider observations differ on prompt cohort, geography, and/or language",
      fields: CROSS_PROVIDER_COMPARABILITY_FIELDS,
      windowDrift: false,
    };
  }

  const uniqueWindows = new Set(keys.map((k) => k.window).filter(Boolean));
  const windowDrift = uniqueWindows.size > 1;
  return {
    comparable: true,
    status: windowDrift ? "COMPARABLE_WINDOW_DRIFT" : "COMPARABLE",
    NOT_COMPARABLE: false,
    cohortKey: keys[0].hard,
    windowDrift,
    reason: windowDrift
      ? "Providers share geography/language/cohort; monitoring windows differ (allowed for All Providers derived average)."
      : null,
  };
}

/**
 * Presence rate from a provider observation. Missing ≠ 0.
 * @param {object} obs
 * @returns {number|null}
 */
function presenceRateOrNull(obs) {
  if (!obs || obs.monitored === false || obs.availability === "not_monitored") {
    return null;
  }
  if (obs.presence === true) return 1;
  if (obs.presence === false) return 0;
  const v = obs.presenceRate ?? obs.aiPresenceRate ?? obs.value;
  if (typeof v !== "number" || !Number.isFinite(v)) return null;
  return v;
}

/**
 * Build factual cross-provider Presence measures (no arbitrary scores).
 *
 * @param {{
 *   entityId?: string,
 *   geography?: string,
 *   language?: string,
 *   monitoringWindow?: string,
 *   promptFamily?: string,
 *   providers: Array<{
 *     provider: string,
 *     monitored?: boolean,
 *     availability?: string,
 *     presenceRate?: number|null,
 *     presence?: boolean|null,
 *     questionsWithPresence?: number,
 *     questionsTotal?: number,
 *     promptFamily?: string,
 *     geography?: string,
 *     language?: string,
 *     monitoringWindow?: string,
 *     domains?: string[],
 *   }>,
 * }} input
 */
export function buildCrossProviderPresenceIntelligence(input = {}) {
  const providersIn = Array.isArray(input.providers) ? input.providers : [];
  const monitored = providersIn.filter(
    (p) =>
      p &&
      normalizeProviderId(p.provider) &&
      p.monitored !== false &&
      p.availability !== "not_monitored"
  );

  const breakdown = providersIn.map((p) => {
    const id = normalizeProviderId(p.provider);
    const rate = presenceRateOrNull(p);
    return {
      provider: id,
      label: formatProviderLabel(id),
      monitored: p.monitored !== false && p.availability !== "not_monitored",
      availability:
        p.availability ||
        (p.monitored === false ? "not_monitored" : rate == null ? "partial" : "observed"),
      presenceRate: rate,
      questionsWithPresence: p.questionsWithPresence ?? null,
      questionsTotal: p.questionsTotal ?? null,
      MISSING_IS_NOT_ZERO: true,
    };
  });

  const rates = monitored
    .map((p) => ({ provider: normalizeProviderId(p.provider), rate: presenceRateOrNull(p) }))
    .filter((r) => r.rate != null);

  const whereAppears = rates.filter((r) => r.rate > 0).map((r) => r.provider);
  const comparableCheck = assertCrossProviderComparability(
    monitored.map((p) => ({
      ...p,
      geography: p.geography || input.geography,
      language: p.language || input.language,
      monitoringWindow: p.monitoringWindow || input.monitoringWindow,
      promptFamily: p.promptFamily || input.promptFamily,
    }))
  );

  let crossProviderAverage = null;
  let presenceRange = null;
  let strongest = null;
  let weakest = null;
  let agreement = null;
  let disagreement = null;

  if (!comparableCheck.comparable && monitored.length > 1) {
    return {
      version: CROSS_PROVIDER_PRESENCE_VERSION,
      mode: "DERIVED",
      ALL_PROVIDERS_RUN: false,
      ALL_PROVIDERS_PROVIDER_RECORD: false,
      ARBITRARY_SCORE: false,
      entityId: input.entityId || null,
      geography: input.geography || null,
      language: input.language || null,
      PROVIDERS_MONITORED: monitored.map((p) => normalizeProviderId(p.provider)),
      PROVIDERS_WHERE_BRAND_APPEARS: whereAppears,
      PROVIDER_PRESENCE_BREAKDOWN: breakdown,
      CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE: null,
      PRESENCE_RANGE: null,
      STRONGEST_PROVIDER_BY_PRESENCE: null,
      WEAKEST_PROVIDER_BY_PRESENCE: null,
      PROVIDER_AGREEMENT: null,
      PROVIDER_DISAGREEMENT: null,
      QUESTIONS_WITH_ALL_PROVIDER_PRESENCE: null,
      QUESTIONS_WITH_PARTIAL_PROVIDER_PRESENCE: null,
      QUESTIONS_WITH_NO_PROVIDER_PRESENCE: null,
      comparability: comparableCheck,
      status: "NOT_COMPARABLE",
      NOT_COMPARABLE: true,
      message: comparableCheck.reason,
    };
  }

  if (rates.length) {
    const vals = rates.map((r) => r.rate);
    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
    crossProviderAverage = avg;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    presenceRange = { min, max, spread: max - min };
    const byRateDesc = [...rates].sort((a, b) => b.rate - a.rate);
    strongest = byRateDesc[0];
    weakest = byRateDesc[byRateDesc.length - 1];

    const allPresent = rates.every((r) => r.rate > 0);
    const allAbsent = rates.every((r) => r.rate === 0);
    const mixedPresenceAbsence = !allPresent && !allAbsent;
    const materialSpread = presenceRange.spread >= 0.15;
    agreement = {
      status: allPresent || allAbsent ? (materialSpread ? "MIXED" : "AGREE") : "MIXED",
      allProvidersShowPresence: allPresent,
      allProvidersShowAbsence: allAbsent,
    };
    disagreement = {
      status: mixedPresenceAbsence || materialSpread ? "DISAGREE" : "NONE",
      materialSpread,
      spread: presenceRange.spread,
    };
  }

  // Question-level agreement when provided
  let qAll = null;
  let qPartial = null;
  let qNone = null;
  const questionMaps = monitored
    .map((p) => p.questionPresenceByPromptId)
    .filter((m) => m && typeof m === "object");
  if (questionMaps.length >= 2) {
    const promptIds = new Set();
    for (const m of questionMaps) {
      for (const k of Object.keys(m)) promptIds.add(k);
    }
    qAll = 0;
    qPartial = 0;
    qNone = 0;
    for (const pid of promptIds) {
      const flags = questionMaps.map((m) => m[pid] === true);
      const known = questionMaps.map((m) => pid in m).filter(Boolean).length;
      if (known < 2) continue;
      const presentCount = flags.filter(Boolean).length;
      if (presentCount === known && presentCount > 0) qAll += 1;
      else if (presentCount === 0) qNone += 1;
      else qPartial += 1;
    }
  }

  return {
    version: CROSS_PROVIDER_PRESENCE_VERSION,
    mode: "DERIVED",
    ALL_PROVIDERS_RUN: false,
    ALL_PROVIDERS_PROVIDER_RECORD: false,
    ARBITRARY_SCORE: false,
    AI_VISIBILITY_SCORE: null,
    GEO_SCORE: null,
    PROVIDER_CONFIDENCE_SCORE: null,
    CONSENSUS_SCORE: null,
    entityId: input.entityId || null,
    geography: input.geography || null,
    language: input.language || null,
    PROVIDERS_MONITORED: monitored.map((p) => normalizeProviderId(p.provider)),
    PROVIDERS_WHERE_BRAND_APPEARS: whereAppears,
    PROVIDER_PRESENCE_BREAKDOWN: breakdown,
    CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE: crossProviderAverage,
    PRESENCE_RANGE: presenceRange,
    STRONGEST_PROVIDER_BY_PRESENCE: strongest,
    WEAKEST_PROVIDER_BY_PRESENCE: weakest,
    PROVIDER_AGREEMENT: agreement,
    PROVIDER_DISAGREEMENT: disagreement,
    QUESTIONS_WITH_ALL_PROVIDER_PRESENCE: qAll,
    QUESTIONS_WITH_PARTIAL_PROVIDER_PRESENCE: qPartial,
    QUESTIONS_WITH_NO_PROVIDER_PRESENCE: qNone,
    comparability: comparableCheck,
    status: rates.length ? "OBSERVED" : "NOT_MONITORED",
    NOT_COMPARABLE: false,
    MISSING_PROVIDER_DATA_IS_NOT_ZERO: true,
  };
}

/**
 * Domain overlap across providers (factual — not "influencing sources").
 * @param {Record<string, string[]>} domainsByProvider
 */
export function buildSourceOverlapBetweenProviders(domainsByProvider = {}) {
  const entries = Object.entries(domainsByProvider || {}).map(([provider, domains]) => [
    normalizeProviderId(provider),
    new Set((domains || []).map((d) => String(d).toLowerCase()).filter(Boolean)),
  ]);
  if (entries.length < 2) {
    return {
      SOURCE_OVERLAP_BETWEEN_PROVIDERS: null,
      status: "INSUFFICIENT_PROVIDERS",
      CAUSAL_LANGUAGE_USED: false,
      INFLUENCING_SOURCES_LABEL: false,
    };
  }
  let intersection = null;
  const union = new Set();
  for (const [, set] of entries) {
    for (const d of set) union.add(d);
    intersection = intersection == null ? new Set(set) : new Set([...intersection].filter((d) => set.has(d)));
  }
  return {
    SOURCE_OVERLAP_BETWEEN_PROVIDERS: {
      sharedDomains: [...(intersection || [])].sort(),
      sharedCount: intersection?.size || 0,
      unionCount: union.size,
      providersCompared: entries.map(([p]) => p),
    },
    status: "OBSERVED",
    CAUSAL_LANGUAGE_USED: false,
    INFLUENCING_SOURCES_LABEL: false,
  };
}
