/**
 * Monitoring Coverage + Operations aggregator for Validation Scorecard.
 * Reads stored summaries/runs/evidence only. LIVE_PROVIDER_CALLS: 0.
 */

import { createBrandAiVisibilityReadStore } from "../storage/index.js";
import { isPositiveRecommendationRole } from "../metrics.js";
import { loadBatchValidationManifest } from "./publication-gate.js";
import { OPS_METRIC_CONTRACT_VERSION } from "./ops-metric-contracts.js";
import { PROVIDER_PRICING_CONFIG_V1 } from "../providers/provider-cost.js";

export const MONITORING_OPS_VERSION = "ai_intelligence_monitoring_ops_v1";

function providerOf(summary, run) {
  const p = run?.provider || summary?.provider?.name || summary?.provider;
  return p ? String(p).toLowerCase() : "unknown";
}

function langOf(run) {
  if (run?.language == null || run.language === "") return null;
  return String(run.language).toLowerCase();
}

function geoOf(run) {
  if (run?.geographyKey) return String(run.geographyKey).toUpperCase();
  if (run?.slot) return String(run.slot).replace(/_(EN|ES)$/i, "").toUpperCase();
  return null;
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function safeRate(n, d) {
  if (!d) return null;
  if (n == null) return null;
  return n / d;
}

function emptyBucket(key) {
  return {
    key,
    batchIds: new Set(),
    runCount: 0,
    attempted: 0,
    successful: 0,
    failed: 0,
    retries: 0,
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    tokenRuns: 0,
    estimatedCost: 0,
    costRuns: 0,
    citations: 0,
    citationResponses: 0,
    evidence: 0,
    entities: new Set(),
    brands: new Set(),
    providers: new Set(),
    languages: new Set(),
    geographies: new Set(),
    models: new Set(),
    firstAt: null,
    latestAt: null,
    recBearing: 0,
    mentionBearing: 0,
  };
}

function touch(row, at) {
  if (!at) return;
  const s = String(at);
  if (!row.firstAt || s < row.firstAt) row.firstAt = s;
  if (!row.latestAt || s > row.latestAt) row.latestAt = s;
}

function finalize(row) {
  return {
    key: row.key,
    BATCH_COUNT: row.batchIds.size,
    RUN_COUNT: row.runCount,
    PROMPTS_ATTEMPTED: row.attempted,
    PROMPTS_SUCCESSFUL: row.successful,
    PROMPTS_FAILED: row.failed,
    SUCCESS_RATE: safeRate(row.successful, row.attempted),
    RETRIES: row.retries,
    TOKENS_INPUT: row.tokenRuns ? row.inputTokens : null,
    TOKENS_OUTPUT: row.tokenRuns ? row.outputTokens : null,
    TOKENS_TOTAL: row.tokenRuns ? row.totalTokens : null,
    ESTIMATED_COST: row.costRuns ? row.estimatedCost : null,
    CITATIONS: row.citations,
    EVIDENCE_RECORDS: row.evidence,
    ENTITIES_COVERED: row.entities.size,
    BRANDS_COVERED: row.brands.size,
    PROVIDERS: [...row.providers],
    LANGUAGES: [...row.languages],
    GEOGRAPHIES: [...row.geographies],
    MODELS: [...row.models],
    FIRST_RUN_AT: row.firstAt,
    LATEST_RUN_AT: row.latestAt,
    CITATION_YIELD: safeRate(row.citationResponses, row.successful),
    RECOMMENDATION_BEARING_RESPONSE_RATE: safeRate(row.recBearing, row.successful),
    ENTITY_MENTION_YIELD: safeRate(row.mentionBearing, row.successful),
    EVIDENCE_YIELD: safeRate(row.evidence, row.successful),
  };
}

/**
 * @param {{ store?: object }} [options]
 */
export async function buildMonitoringOperationsReport(options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore({});
  const summaries =
    typeof store.listBatchSummaries === "function" ? await store.listBatchSummaries({}) : [];

  /** @type {Record<string, ReturnType<typeof emptyBucket>>} */
  const byProvider = {};
  /** @type {Record<string, ReturnType<typeof emptyBucket>>} */
  const byLanguage = {};
  /** @type {Record<string, ReturnType<typeof emptyBucket>>} */
  const byGeography = {};
  /** @type {Record<string, { en: ReturnType<typeof emptyBucket>, es: ReturnType<typeof emptyBucket> }>} */
  const byProviderLang = {};

  const inventory = [];
  const allEntities = new Set();
  const allBrands = new Set();
  const validatedBatchIds = new Set();
  const publishableBatchIds = new Set();

  let totalRuns = 0;
  let totalAttempted = 0;
  let totalSuccess = 0;
  let totalFailed = 0;
  let totalRetries = 0;
  let totalCitations = 0;
  let totalEvidence = 0;
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  let totalTokens = 0;
  let tokenRuns = 0;
  let totalEstimatedCost = 0;
  let costRuns = 0;
  let batchesWithCost = 0;
  let batchesWithoutCost = 0;
  let batchesWithTokens = 0;
  let batchesWithoutTokens = 0;
  let completedBatches = 0;
  let partialBatches = 0;
  let failedBatches = 0;
  let recBearing = 0;
  let mentionBearing = 0;
  let citationResponses = 0;
  let firstMonitoring = null;
  let latestMonitoring = null;
  const lastByProvider = {};
  const lastByLanguage = {};

  // Evidence index by runId / evidenceId (one listEvidence per federated root via provider filter)
  const evidenceByRunId = new Map();
  const evidenceById = new Map();
  if (typeof store.listEvidence === "function") {
    for (const provider of ["openai", "gemini", "perplexity", "claude"]) {
      const rows = await store.listEvidence({ provider });
      for (const ev of rows || []) {
        if (ev.evidenceId) evidenceById.set(ev.evidenceId, ev);
        if (ev.runId) evidenceByRunId.set(ev.runId, ev);
      }
    }
  }

  function ensure(map, key) {
    if (!map[key]) map[key] = emptyBucket(key);
    return map[key];
  }

  for (const summary of summaries) {
    if (!summary?.batchId) continue;
    const batchId = summary.batchId;
    const runs =
      typeof store.listBatchRuns === "function" ? (await store.listBatchRuns(batchId)) || [] : [];
    const manifest = loadBatchValidationManifest(batchId);
    if (manifest?.overallValidationStatus === "PASS") validatedBatchIds.add(batchId);
    if (manifest?.publishable === true) publishableBatchIds.add(batchId);

    const stSum = String(summary.status || "").toLowerCase();
    if (stSum === "completed") completedBatches += 1;
    else if (stSum === "partial") partialBatches += 1;
    else if (stSum === "failed") failedBatches += 1;

    let batchCost = 0;
    let batchCostRuns = 0;
    let batchTokenRuns = 0;
    let batchSuccess = 0;
    let batchFail = 0;
    let batchAttempted = 0;
    let batchCitations = 0;
    const batchEntities = new Set();
    const languages = new Set();
    const geographies = new Set();
    const models = new Set();
    let batchLatest = null;

    for (const run of runs) {
      totalRuns += 1;
      const st = String(run.status || "").toLowerCase();
      const isAttempt = ["completed", "failed", "error", "partial"].includes(st);
      const isOk = st === "completed";
      const isFail = st === "failed" || st === "error";
      if (isAttempt) {
        totalAttempted += 1;
        batchAttempted += 1;
      }
      if (isOk) {
        totalSuccess += 1;
        batchSuccess += 1;
      }
      if (isFail) {
        totalFailed += 1;
        batchFail += 1;
      }
      const retries = Number(run.retries) || 0;
      totalRetries += retries;

      const provider = providerOf(summary, run);
      const language = langOf(run);
      const geography = geoOf(run);
      const model = run.model || summary.provider?.model || null;
      const at = run.completedAt || run.savedAt || run.startedAt || null;

      if (at) {
        if (!firstMonitoring || String(at) < firstMonitoring) firstMonitoring = String(at);
        if (!latestMonitoring || String(at) > latestMonitoring) latestMonitoring = String(at);
        if (!batchLatest || String(at) > batchLatest) batchLatest = String(at);
        if (!lastByProvider[provider] || String(at) > lastByProvider[provider]) {
          lastByProvider[provider] = String(at);
        }
        if (language && (!lastByLanguage[language] || String(at) > lastByLanguage[language])) {
          lastByLanguage[language] = String(at);
        }
      }

      if (language) languages.add(language);
      if (geography) geographies.add(geography);
      if (model) models.add(model);

      const buckets = [ensure(byProvider, provider)];
      if (language) buckets.push(ensure(byLanguage, language));
      if (geography) buckets.push(ensure(byGeography, geography));
      if (language === "en" || language === "es") {
        if (!byProviderLang[provider]) {
          byProviderLang[provider] = { en: emptyBucket("en"), es: emptyBucket("es") };
        }
        buckets.push(byProviderLang[provider][language]);
      }

      for (const b of buckets) {
        b.runCount += 1;
        b.batchIds.add(batchId);
        if (isAttempt) b.attempted += 1;
        if (isOk) b.successful += 1;
        if (isFail) b.failed += 1;
        b.retries += retries;
        b.providers.add(provider);
        if (language) b.languages.add(language);
        if (geography) b.geographies.add(geography);
        if (model) b.models.add(model);
        touch(b, at);
      }

      const usage = run.usage || {};
      const inTok = num(usage.inputTokens ?? usage.promptTokens);
      const outTok = num(usage.outputTokens ?? usage.completionTokens);
      const totTok =
        num(usage.totalTokens) ?? (inTok != null && outTok != null ? inTok + outTok : null);
      if (inTok != null || outTok != null || totTok != null) {
        tokenRuns += 1;
        batchTokenRuns += 1;
        totalInputTokens += inTok || 0;
        totalOutputTokens += outTok || 0;
        totalTokens += totTok || (inTok || 0) + (outTok || 0);
        for (const b of buckets) {
          b.inputTokens += inTok || 0;
          b.outputTokens += outTok || 0;
          b.totalTokens += totTok || (inTok || 0) + (outTok || 0);
          b.tokenRuns += 1;
        }
      }

      const cost = num(run.estimatedCost);
      if (cost != null) {
        costRuns += 1;
        batchCostRuns += 1;
        totalEstimatedCost += cost;
        batchCost += cost;
        for (const b of buckets) {
          b.estimatedCost += cost;
          b.costRuns += 1;
        }
      }

      if (isOk) {
        const ev =
          (run.evidenceId && evidenceById.get(run.evidenceId)) ||
          (run.runId && evidenceByRunId.get(run.runId)) ||
          null;
        if (ev) {
          totalEvidence += 1;
          for (const b of buckets) b.evidence += 1;
          const mentions = ev.payload?.mentions || [];
          const citations = ev.payload?.citations || [];
          if (citations.length) {
            citationResponses += 1;
            batchCitations += citations.length;
            totalCitations += citations.length;
            for (const b of buckets) {
              b.citations += citations.length;
              b.citationResponses += 1;
            }
          }
          let hasRec = false;
          let hasEntity = false;
          for (const m of mentions) {
            const id = m.canonicalEntityId || m.entityId;
            if (id) {
              hasEntity = true;
              allEntities.add(id);
              allBrands.add(id);
              batchEntities.add(id);
              for (const b of buckets) {
                b.entities.add(id);
                b.brands.add(id);
              }
            }
            if (isPositiveRecommendationRole(m.role)) hasRec = true;
          }
          if (hasRec) {
            recBearing += 1;
            for (const b of buckets) b.recBearing += 1;
          }
          if (hasEntity) {
            mentionBearing += 1;
            for (const b of buckets) b.mentionBearing += 1;
          }
        }
      }
    }

    if (batchCostRuns > 0) batchesWithCost += 1;
    else batchesWithoutCost += 1;
    if (batchTokenRuns > 0) batchesWithTokens += 1;
    else batchesWithoutTokens += 1;

    inventory.push({
      BATCH_ID: batchId,
      PROVIDER: providerOf(summary, null),
      MODEL: summary.provider?.model || [...models][0] || null,
      GEOGRAPHY: summary.slots ? "multi_slot" : summary.cohort?.commercialRegion || null,
      LANGUAGE: summary.slots ? "multi_slot" : summary.language || null,
      PROMPT_SET_VERSION: summary.versions?.promptSetId || summary.logical?.promptSetId || null,
      PROMPT_COUNT: batchAttempted,
      SUCCESS_COUNT: batchSuccess,
      FAIL_COUNT: batchFail,
      ENTITY_UNIVERSE_COUNT:
        Object.keys(summary.metrics?.byEntity || {}).length || batchEntities.size,
      BRANDS_COVERED: batchEntities.size,
      CITATIONS: batchCitations,
      TOKENS: null,
      ESTIMATED_COST: batchCostRuns ? batchCost : null,
      EXECUTED_AT: batchLatest || summary.completedAt || summary.savedAt || null,
      VALIDATED_AT: manifest?.validatedAt || null,
      VALIDATION_STATUS: manifest?.overallValidationStatus || "NOT_VALIDATED",
      PUBLISHABLE: manifest?.publishable === true,
      LANGUAGES: [...languages],
      GEOGRAPHIES: [...geographies],
      MODELS: [...models],
    });
  }

  const mapFinish = (map) =>
    Object.fromEntries(Object.entries(map).map(([k, v]) => [k, finalize(v)]));

  const providerOps = mapFinish(byProvider);
  const languageOps = mapFinish(byLanguage);
  const geographyOps = mapFinish(byGeography);

  const providerLanguageMatrix = Object.entries(byProviderLang).map(([provider, langs]) => ({
    PROVIDER: provider,
    ENGLISH_PROMPTS: langs.en.attempted,
    SPANISH_PROMPTS: langs.es.attempted,
    ENGLISH_SUCCESSFUL: langs.en.successful,
    SPANISH_SUCCESSFUL: langs.es.successful,
    ENGLISH_BATCHES: langs.en.batchIds.size,
    SPANISH_BATCHES: langs.es.batchIds.size,
    LATEST_ENGLISH_RUN: langs.en.latestAt,
    LATEST_SPANISH_RUN: langs.es.latestAt,
    ESTIMATED_ENGLISH_COST: langs.en.costRuns ? langs.en.estimatedCost : null,
    ESTIMATED_SPANISH_COST: langs.es.costRuns ? langs.es.estimatedCost : null,
  }));

  const validatedN = validatedBatchIds.size;
  const publishableN = publishableBatchIds.size;

  const coverage = {
    TOTAL_MONITORING_BATCHES: summaries.length,
    TOTAL_MONITORING_RUNS: totalRuns,
    TOTAL_PROMPTS_ATTEMPTED: totalAttempted,
    TOTAL_PROMPTS_SUCCESSFUL: totalSuccess,
    TOTAL_PROMPTS_FAILED: totalFailed,
    TOTAL_RESPONSES_STORED: totalSuccess,
    TOTAL_CANONICAL_ENTITIES_COVERED: allEntities.size,
    TOTAL_BRANDS_COVERED: allBrands.size,
    TOTAL_CITATIONS_CAPTURED: totalCitations,
    TOTAL_EVIDENCE_RECORDS: totalEvidence,
    TOTAL_VALIDATED_BATCHES: validatedN,
    TOTAL_PUBLISHABLE_BATCHES: publishableN,
  };

  const cost = {
    TOTAL_ESTIMATED_MONITORING_COST: costRuns ? totalEstimatedCost : null,
    COST_BY_PROVIDER: Object.fromEntries(
      Object.entries(providerOps).map(([k, v]) => [k, v.ESTIMATED_COST])
    ),
    COST_BY_LANGUAGE: Object.fromEntries(
      Object.entries(languageOps).map(([k, v]) => [k, v.ESTIMATED_COST])
    ),
    COST_BY_GEOGRAPHY: Object.fromEntries(
      Object.entries(geographyOps).map(([k, v]) => [k, v.ESTIMATED_COST])
    ),
    COST_PER_SUCCESSFUL_PROMPT: costRuns && totalSuccess ? totalEstimatedCost / totalSuccess : null,
    COST_PER_VALIDATED_BATCH: costRuns && validatedN ? totalEstimatedCost / validatedN : null,
    COST_PER_PUBLISHABLE_BATCH: costRuns && publishableN ? totalEstimatedCost / publishableN : null,
    BATCHES_WITH_COST: batchesWithCost,
    BATCHES_WITHOUT_COST: batchesWithoutCost,
    BATCHES_WITH_TOKEN_USAGE: batchesWithTokens,
    BATCHES_WITHOUT_TOKEN_USAGE: batchesWithoutTokens,
    COST_SOURCE: "runs.estimatedCost",
    ESTIMATED_OR_ACTUAL: "Estimated",
    PRICING_VERSION: PROVIDER_PRICING_CONFIG_V1?.version || null,
    LABEL: "Estimated Cost",
  };

  const freshness = {
    FIRST_MONITORING_DATE: firstMonitoring,
    LATEST_MONITORING_DATE: latestMonitoring,
    LATEST_VALIDATION_DATE: null,
    LATEST_PUBLISHABLE_BATCH_DATE: null,
    LAST_OPENAI_RUN: lastByProvider.openai || null,
    LAST_GEMINI_RUN: lastByProvider.gemini || null,
    LAST_PERPLEXITY_RUN: lastByProvider.perplexity || null,
    LAST_CLAUDE_RUN: lastByProvider.claude || null,
    LAST_ENGLISH_RUN: lastByLanguage.en || null,
    LAST_SPANISH_RUN: lastByLanguage.es || null,
    BATCHES_AWAITING_VALIDATION: 0,
    FAILED_VALIDATION_BATCHES: 0,
  };
  for (const row of inventory) {
    if (!row.VALIDATED_AT || row.VALIDATION_STATUS === "NOT_VALIDATED") {
      freshness.BATCHES_AWAITING_VALIDATION += 1;
    }
    if (row.VALIDATION_STATUS === "FAIL") freshness.FAILED_VALIDATION_BATCHES += 1;
    if (row.VALIDATED_AT) {
      if (!freshness.LATEST_VALIDATION_DATE || row.VALIDATED_AT > freshness.LATEST_VALIDATION_DATE) {
        freshness.LATEST_VALIDATION_DATE = row.VALIDATED_AT;
      }
    }
    if (row.PUBLISHABLE && row.EXECUTED_AT) {
      if (
        !freshness.LATEST_PUBLISHABLE_BATCH_DATE ||
        row.EXECUTED_AT > freshness.LATEST_PUBLISHABLE_BATCH_DATE
      ) {
        freshness.LATEST_PUBLISHABLE_BATCH_DATE = row.EXECUTED_AT;
      }
    }
  }

  return {
    version: MONITORING_OPS_VERSION,
    opsMetricContractVersion: OPS_METRIC_CONTRACT_VERSION,
    generatedAt: new Date().toISOString(),
    coverage,
    providerOps,
    languageOps,
    geographyOps,
    providerLanguageMatrix,
    inventory,
    cost,
    reliability: {
      CALLS_ATTEMPTED: totalAttempted,
      CALLS_SUCCESSFUL: totalSuccess,
      CALLS_FAILED: totalFailed,
      RETRIES: totalRetries,
      RETRY_RATE: safeRate(totalRetries, totalAttempted),
      SUCCESS_RATE: safeRate(totalSuccess, totalAttempted),
      COMPLETED_BATCHES: completedBatches,
      PARTIAL_BATCHES: partialBatches,
      FAILED_BATCHES: failedBatches,
    },
    yield: {
      CITATION_YIELD: safeRate(citationResponses, totalSuccess),
      RECOMMENDATION_BEARING_RESPONSE_RATE: safeRate(recBearing, totalSuccess),
      ENTITY_MENTION_YIELD: safeRate(mentionBearing, totalSuccess),
      EVIDENCE_YIELD: safeRate(totalEvidence, totalSuccess),
      VALIDATED_OBSERVATIONS_PER_DOLLAR:
        costRuns && totalEstimatedCost > 0 ? totalEvidence / totalEstimatedCost : null,
    },
    freshness,
    tokens: {
      INPUT: tokenRuns ? totalInputTokens : null,
      OUTPUT: tokenRuns ? totalOutputTokens : null,
      TOTAL: tokenRuns ? totalTokens : null,
      RUNS_WITH_TOKENS: tokenRuns,
    },
    costIntegrity: {
      BATCHES_WITH_COST: batchesWithCost,
      BATCHES_WITHOUT_COST: batchesWithoutCost,
      BATCHES_WITH_TOKENS: batchesWithTokens,
      BATCHES_WITHOUT_TOKENS: batchesWithoutTokens,
      PROVIDERS_WITH_RELIABLE_COST: Object.entries(providerOps)
        .filter(([, v]) => v.ESTIMATED_COST != null)
        .map(([k]) => k),
      PROVIDERS_WITH_LIMITED_COST_ESTIMATION: Object.entries(providerOps)
        .filter(([, v]) => v.ESTIMATED_COST == null)
        .map(([k]) => k),
      LIMITATIONS:
        "Claude/Gemini/Perplexity baseline runs often lack estimatedCost/usage. OpenAI wave1 has estimatedCost+tokens. Costs are Estimated, not Actual Billed.",
    },
  };
}
