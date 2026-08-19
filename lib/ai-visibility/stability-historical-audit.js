/**
 * Read-only audit of stored AI Visibility observations.
 * No provider calls. No DataForSEO. Does not overwrite evidence.
 */

import { createBrandAiVisibilityReadStore } from "./storage/index.js";
import { aggregateStabilitySeries, classifyCrossProviderAlignment } from "./stability-aggregation.js";
import { resolvePromptProvenance } from "./prompt-provenance.js";
import {
  loadScenarioRegistry,
  buildScenarioRegistryIndex,
  resolvePromptScenario,
} from "./scenario-registry.js";
import { buildPromptMetadataById } from "./associations/prompt-metadata-lookup.js";
import { VALIDATION_COHORT } from "./stability-policy.js";

function providerOf(row) {
  const p = row.provider?.name || row.provider || "";
  return String(p || "openai").toLowerCase();
}

function languageOf(row) {
  const lang = row.language || row.payload?.language || "en";
  return String(lang).toLowerCase();
}

function geographyOf(row) {
  return (
    row.commercialRegion ||
    row.regionName ||
    row.geographyKey ||
    row.payload?.commercialRegion ||
    (row.geographyScope === "Global" ? "Global" : null) ||
    "unspecified"
  );
}

function timestampOf(row) {
  return row.timestamp || row.completedAt || row.savedAt || row.payload?.timestamp || null;
}

export function coarsePairKey(row) {
  return `${row.promptId || ""}|${providerOf(row)}`;
}

export function preferredGrainKey(row) {
  return `${row.promptId || ""}|${providerOf(row)}|${languageOf(row)}|${geographyOf(row)}`;
}

function presentEntityIds(row) {
  const mentions = row.payload?.mentions || row.mentions || [];
  return [
    ...new Set(
      mentions
        .map((m) => m.canonicalEntityId || m.resolvedEntityId || m.entityId)
        .filter(Boolean)
    ),
  ];
}

export function evidenceToObservation(row) {
  const citations = row.payload?.citations || row.citations || [];
  return {
    observationId: row.evidenceId || row.observationId,
    promptId: row.promptId,
    provider: providerOf(row),
    language: languageOf(row),
    geography: geographyOf(row),
    timestamp: timestampOf(row),
    presentEntityIds: presentEntityIds(row),
    mentions: row.payload?.mentions || [],
    citations,
    success: row.success !== false,
    batchId: row.batchId || null,
  };
}

export async function auditStoredStabilityHistory(options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore();
  const evidence = (await store.listEvidence({})) || [];
  const promptsById = buildPromptMetadataById();
  const scenarioIndex = buildScenarioRegistryIndex(loadScenarioRegistry());

  const pairMap = new Map();
  const grainMap = new Map();
  const dates = [];

  for (const row of evidence) {
    if (!row?.promptId) continue;
    const obs = evidenceToObservation(row);
    const ts = obs.timestamp;
    if (ts) dates.push(ts);

    const pk = coarsePairKey(row);
    if (!pairMap.has(pk)) pairMap.set(pk, []);
    pairMap.get(pk).push(obs);

    const gk = preferredGrainKey(row);
    if (!grainMap.has(gk)) grainMap.set(gk, []);
    grainMap.get(gk).push(obs);
  }

  const pairCounts = { 1: 0, 2: 0, "3plus": 0 };
  for (const rows of pairMap.values()) {
    const n = rows.length;
    if (n === 1) pairCounts[1] += 1;
    else if (n === 2) pairCounts[2] += 1;
    else if (n >= 3) pairCounts["3plus"] += 1;
  }

  const grainCounts = { 1: 0, 2: 0, "3plus": 0 };
  const grainSeries = [];
  for (const [key, rows] of grainMap.entries()) {
    const n = rows.length;
    if (n === 1) grainCounts[1] += 1;
    else if (n === 2) grainCounts[2] += 1;
    else if (n >= 3) grainCounts["3plus"] += 1;
    const [promptId, provider, language, geographyKey] = key.split("|");
    const prompt = promptsById.get(promptId) || { promptId };
    const provenance = resolvePromptProvenance(prompt, { scenarioIndex });
    const scenario = resolvePromptScenario(prompt, scenarioIndex);
    const series = aggregateStabilitySeries(rows, {
      promptId,
      provider,
      language,
      geographyKey,
      repeatType: "EXACT_REPEAT",
    });
    grainSeries.push({
      grain: key,
      promptId,
      provider,
      language,
      geographyKey,
      observationCount: n,
      distinctRunDates: series.runDates,
      presenceCount: series.presenceCount,
      citationResultCount: rows.filter((r) => (r.citations || []).length).length,
      brandMentions: [...new Set(rows.flatMap((r) => r.presentEntityIds))].length,
      scenarioId: provenance.scenarioId || scenario.scenarioId || null,
      promptOrigin: provenance.promptOrigin,
      commercialPriority: scenario.commercialPriority || null,
      recurrenceState: series.recurrenceState,
      stabilityState: series.stabilityState,
      firstObservedAt: series.firstObservedAt,
      lastObservedAt: series.lastObservedAt,
    });
  }

  const byPromptProvider = new Map();
  for (const g of grainSeries) {
    const k = `${g.promptId}|${g.provider}`;
    if (!byPromptProvider.has(k)) byPromptProvider.set(k, []);
    byPromptProvider.get(k).push(g);
  }
  const crossProviderExamples = [];
  const byPromptGeoLang = new Map();
  for (const g of grainSeries) {
    const k = `${g.promptId}|${g.language}|${g.geographyKey}`;
    if (!byPromptGeoLang.has(k)) byPromptGeoLang.set(k, []);
    byPromptGeoLang.get(k).push(g);
  }
  for (const series of byPromptGeoLang.values()) {
    if (series.length < 2) continue;
    const align = classifyCrossProviderAlignment(series);
    if (align.crossProviderAlignment === "HIGH_VARIABILITY" || align.crossProviderAlignment === "PARTIAL_ALIGNMENT") {
      crossProviderExamples.push({
        promptId: series[0].promptId,
        geographyKey: series[0].geographyKey,
        language: series[0].language,
        ...align,
      });
    }
  }

  const parsed = dates.map((d) => Date.parse(d)).filter((n) => Number.isFinite(n));
  parsed.sort((a, b) => a - b);

  return {
    evidenceRows: evidence.length,
    PROMPT_PROVIDER_PAIRS: pairMap.size,
    PAIRS_WITH_1_OBSERVATION: pairCounts[1],
    PAIRS_WITH_2_OBSERVATIONS: pairCounts[2],
    PAIRS_WITH_3_PLUS_OBSERVATIONS: pairCounts["3plus"],
    STRICT_GRAIN_KEYS: grainMap.size,
    GRAINS_WITH_1: grainCounts[1],
    GRAINS_WITH_2: grainCounts[2],
    GRAINS_WITH_3_PLUS: grainCounts["3plus"],
    DATE_SPAN:
      parsed.length >= 2
        ? {
            first: new Date(parsed[0]).toISOString().slice(0, 10),
            last: new Date(parsed[parsed.length - 1]).toISOString().slice(0, 10),
          }
        : parsed.length === 1
          ? {
              first: new Date(parsed[0]).toISOString().slice(0, 10),
              last: new Date(parsed[0]).toISOString().slice(0, 10),
            }
          : { first: null, last: null },
    crossProviderExamples: crossProviderExamples.slice(0, 12),
    grainSeries,
    PROVIDER_CALLS: 0,
  };
}

const EMPTY_PROVIDER_COUNTS = Object.freeze({
  openai: 0,
  gemini: 0,
  perplexity: 0,
  claude: 0,
});

/**
 * Resolve the validation cohort against the complete historical store.
 * Do not sample grainSeries. ZERO means no evidence rows, not "missing from first 40".
 */
export async function lookupValidationCohortHistory(options = {}) {
  const cohort = options.cohort || VALIDATION_COHORT;
  const audit = await auditStoredStabilityHistory(options);
  const grainsByPrompt = new Map();
  for (const g of audit.grainSeries || []) {
    if (!g?.promptId) continue;
    if (!grainsByPrompt.has(g.promptId)) grainsByPrompt.set(g.promptId, []);
    grainsByPrompt.get(g.promptId).push(g);
  }

  const rows = cohort.map((c) => {
    const grains = grainsByPrompt.get(c.promptId) || [];
    const byProvider = { ...EMPTY_PROVIDER_COUNTS };
    let exactRepeatCount = 0;
    let first = null;
    let last = null;
    for (const g of grains) {
      const p = String(g.provider || "").toLowerCase();
      if (byProvider[p] != null) byProvider[p] += Number(g.observationCount) || 0;
      else byProvider[p] = Number(g.observationCount) || 0;
      exactRepeatCount += Number(g.observationCount) || 0;
      if (g.firstObservedAt && (!first || g.firstObservedAt < first)) first = g.firstObservedAt;
      if (g.lastObservedAt && (!last || g.lastObservedAt > last)) last = g.lastObservedAt;
    }
    const primary = [...grains].sort(
      (a, b) => (b.observationCount || 0) - (a.observationCount || 0)
    )[0];
    const zero = exactRepeatCount === 0;
    return {
      PROMPT_ID: c.promptId,
      origin: c.origin,
      HISTORICAL_OBSERVATIONS_BY_PROVIDER: zero ? "ZERO" : byProvider,
      EXACT_REPEAT_COUNT: exactRepeatCount,
      FIRST_OBSERVED_AT: zero ? "ZERO" : first,
      LAST_OBSERVED_AT: zero ? "ZERO" : last,
      CURRENT_RECURRENCE_STATE: zero
        ? "ZERO"
        : primary?.recurrenceState || "INSUFFICIENT_OBSERVATIONS",
      CURRENT_STABILITY_STATE: zero
        ? "ZERO"
        : primary?.stabilityState || "INSUFFICIENT_OBSERVATIONS",
      grains,
      SAMPLE_SLICE_ONLY: false,
    };
  });

  const scenarioRows = rows.filter((r) => r.origin === "SCENARIO");
  const scenarioAllZero =
    scenarioRows.length > 0 && scenarioRows.every((r) => r.EXACT_REPEAT_COUNT === 0);
  const emptyStore = (audit.evidenceRows || 0) === 0;
  const failReasons = [];
  if (cohort.length !== 16) failReasons.push(`cohort_length_${cohort.length}_expected_16`);
  if (emptyStore) failReasons.push("historical_store_empty");
  if (scenarioAllZero && !emptyStore) {
    failReasons.push("scenario_prompts_zero_despite_nonempty_store");
  }

  return {
    FULL_COHORT_LOOKUP: failReasons.length ? "FAIL" : "PASS",
    failReasons,
    PROMPTS_RESOLVED: `${rows.length} / ${cohort.length}`,
    evidenceRows: audit.evidenceRows,
    STRICT_GRAIN_KEYS: audit.STRICT_GRAIN_KEYS,
    DATE_SPAN: audit.DATE_SPAN,
    grainSeriesCount: (audit.grainSeries || []).length,
    rows,
    PROVIDER_CALLS: 0,
  };
}
