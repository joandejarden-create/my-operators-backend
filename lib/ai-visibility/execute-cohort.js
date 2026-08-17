/**
 * Governed AI Visibility cohort execution engine (Phase 2E).
 * Manual CLI trigger only — no scheduler, no Airtable execution writes.
 */

import { createHash } from "crypto";
import {
  isAiVisibilityEnabled,
  isAiVisibilityLiveTestAllowed,
  resolveDefaultModel,
  resolveDefaultProvider,
  resolveMaxTestRuns,
  METRIC_VERSION,
  RECOMMENDATION_CLASSIFIER_VERSION,
  CITATION_ASSOC_VERSION,
  GEOGRAPHY_MODEL_VERSION,
  RESOLVER_VERSION,
} from "./config.js";
import { loadGovernedAiVisibilityPrompts } from "./load-prompts.js";
import { buildPromptCohort } from "./prompt-cohort.js";
import { requireSupportedLanguage } from "./language-dimension.js";
import {
  createExecutionBatch,
  deriveBatchStatus,
  deriveBatchHealth,
  isRetryableProviderError,
  isAuthProviderError,
  findDuplicateRecentBatch,
  buildDuplicateKey,
  hashPromptText,
  BATCH_HEALTH,
} from "./execution-batch.js";
import { resolvePeerSetMembership, loadPeerSetConfig } from "./peer-sets.js";
import { buildLiveAiVisibilityEntityIndex } from "./entity-index.js";
import { createAiVisibilityStore } from "./storage/index.js";
import { assessMetricReadiness } from "./metric-readiness.js";
import { runVisibilityPrompt } from "./providers/index.js";
import { extractMentions } from "./extract-mentions.js";
import { extractCitations } from "./extract-citations.js";
import { harvestUnresolvedWithFilterStats } from "./mention-classification.js";
import { normalizeMatchKey } from "./normalize-entities.js";
import {
  buildObservationFromExtractions,
  computeAiPresenceRate,
  computeRecommendationShare,
  computeRecommendationRate,
  computeTop3RecommendationRate,
  computeFirstRecommendationRate,
  computeQuestionsWon,
  computeQuestionsMissing,
  computeCompetitivePosition,
  computeCitationRate,
} from "./metrics.js";
import { assembleEvidenceRecord, metricEvidenceTrace } from "./evidence.js";

export const EXECUTE_ENGINE_VERSION = "ai_visibility_execute_engine_v1";

/** Conservative per-call estimate for gpt-5.6 + web_search (USD). Override via env. */
export function resolveEstimatedUsdPerCall() {
  const n = Number(process.env.AI_VISIBILITY_EST_USD_PER_CALL || "0.25");
  return Number.isFinite(n) && n >= 0 ? n : 0.25;
}

export function resolveMaxBatchCostUsd() {
  const raw = process.env.AI_VISIBILITY_MAX_BATCH_COST_USD;
  if (raw == null || String(raw).trim() === "") return 5;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : 5;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function titleScope(scope) {
  if (!scope) return null;
  const s = String(scope).trim().toLowerCase();
  if (s === "global") return "Global";
  if (s === "region") return "Region";
  if (s === "subregion") return "Subregion";
  if (s === "country") return "Country";
  if (s === "market") return "Market";
  return scope;
}

function titleEntityScope(scope) {
  if (!scope) return null;
  const s = String(scope).trim().toLowerCase();
  if (s === "brand") return "Brand";
  if (s === "operator") return "Operator";
  if (s === "both") return "Both";
  return scope;
}

function knownNameKeys(entities) {
  const keys = new Set();
  for (const e of entities || []) {
    keys.add(normalizeMatchKey(e.name));
    for (const a of e.aliases || []) keys.add(normalizeMatchKey(a));
  }
  return keys;
}

/**
 * Validate peer-set entity IDs against live canonical index.
 */
export function validatePeerSetAgainstIndex(peerResolution, entityIndex) {
  if (!peerResolution?.ok) {
    return {
      ...peerResolution,
      canonicalValid: false,
      missingEntityIds: [],
      error: peerResolution?.error || "peer_set_not_found",
    };
  }
  const ids = new Set((entityIndex?.entities || []).map((e) => e.id));
  const missing = (peerResolution.entityIds || []).filter((id) => !ids.has(id));
  return {
    ...peerResolution,
    canonicalValid: missing.length === 0,
    missingEntityIds: missing,
    error: missing.length
      ? `peer_set_non_canonical_ids:${missing.slice(0, 8).join(",")}`
      : null,
  };
}

function pickDefaultPeerSetId(entityScope, commercialRegion) {
  const scope = String(entityScope || "").toLowerCase();
  if (scope === "operator") return "peers_cala_operators_global_v1";
  return "peers_upper_upscale_brands_global_v1";
}

/**
 * Plan a cohort execution (dry-run or pre-execute).
 */
export async function planAiVisibilityCohort(args = {}) {
  const stakeholder = args.stakeholder || "brand";
  const entityScope = titleEntityScope(args.entityScope || stakeholder);
  const geographyScope = titleScope(args.geographyScope || args.scope || "region");
  const commercialRegion = args.region || args.commercialRegion || null;
  const country = args.country || null;
  const intentTerritories = args.intentTerritories || args.intent || null;
  const provider = args.provider || resolveDefaultProvider();
  const model = args.model || resolveDefaultModel() || "gpt-5.6";
  const promptMode = args.promptMode || "auto";
  const languageRaw = args.language != null ? args.language : "en";
  const langReq = requireSupportedLanguage(languageRaw);
  if (!langReq.ok) {
    return {
      engineVersion: EXECUTE_ENGINE_VERSION,
      dryRun: true,
      ok: false,
      prerequisitesOk: false,
      prerequisites: [langReq.reasonCode],
      message: langReq.message,
      language: null,
      plannedRuns: 0,
      DRY_RUN_PROVIDER_CALLS: 0,
    };
  }
  const language = langReq.language;

  const loaded = await loadGovernedAiVisibilityPrompts(
    {
      activeOnly: true,
      monitoringEligible: true,
    },
    { mode: promptMode, fixturePath: args.fixturePath }
  );

  const cohort = buildPromptCohort({
    prompts: loaded.prompts,
    geographyScope,
    region: geographyScope === "Region" ? commercialRegion : null,
    country: geographyScope === "Country" ? country : null,
    stakeholder,
    entityScope,
    intentTerritories,
    monitoringEligible: true,
    activeOnly: true,
    includeCountryRollup: false,
    language,
    requireLanguage: true,
  });

  if (cohort.ok === false) {
    return {
      engineVersion: EXECUTE_ENGINE_VERSION,
      dryRun: true,
      ok: false,
      prerequisitesOk: false,
      prerequisites: [cohort.error || "cohort_error"],
      message: cohort.message,
      language,
      plannedRuns: 0,
      DRY_RUN_PROVIDER_CALLS: 0,
    };
  }

  // Attach promptText onto members for hashing/manifest from loaded prompts
  const byId = new Map(loaded.prompts.map((p) => [p.promptId, p]));
  cohort.members = cohort.members.map((m) => {
    const full = byId.get(m.promptId);
    return {
      ...m,
      promptText: full?.promptText || null,
      version: full?.version || m.version,
    };
  });

  const maxRuns = resolveMaxTestRuns();
  const estPerCall = resolveEstimatedUsdPerCall();
  const maxBatchCost = resolveMaxBatchCostUsd();
  const plannedRuns = cohort.count;
  const estimatedMaxCost = plannedRuns * estPerCall;

  const prerequisites = [];
  if (plannedRuns === 0) prerequisites.push("empty_cohort");
  if (plannedRuns > maxRuns) {
    prerequisites.push(`planned_runs_exceed_max_test_runs:${plannedRuns}>${maxRuns}`);
  }
  // Estimated cost over cap is a warning — hard stop happens mid-batch on accumulated spend
  const warnings = [];
  if (estimatedMaxCost > maxBatchCost) {
    warnings.push(
      `estimated_cost_exceeds_batch_cap:${estimatedMaxCost.toFixed(2)}>${maxBatchCost}`
    );
  }
  if (geographyScope === "Region" && !commercialRegion) {
    prerequisites.push("region_scope_requires_commercial_region");
  }
  if (geographyScope === "Country" && !country) {
    prerequisites.push("country_scope_requires_country");
  }
  if (geographyScope === "Global" && (commercialRegion || country)) {
    prerequisites.push("global_must_not_set_region_or_country");
  }

  // Region purity check
  const impure = cohort.members.filter((m) => {
    if (geographyScope === "Region") return m.geographyScope !== "Region";
    if (geographyScope === "Global") return m.geographyScope !== "Global";
    if (geographyScope === "Country") return m.geographyScope !== "Country";
    return false;
  });
  if (impure.length) prerequisites.push("cohort_geography_impurity");

  return {
    engineVersion: EXECUTE_ENGINE_VERSION,
    dryRun: true,
    DRY_RUN_PROVIDER_CALLS: 0,
    AIRTABLE_EXECUTION_WRITES: 0,
    stakeholder,
    entityScope,
    geographyScope,
    commercialRegion,
    country,
    language,
    provider,
    model,
    cohort: {
      count: cohort.count,
      fingerprint: cohort.fingerprint,
      language: cohort.language || language,
      promptIds: cohort.promptIds,
      members: cohort.members.map((m) => ({
        promptId: m.promptId,
        version: m.version,
        language: m.language || language,
        semanticPairId: m.semanticPairId || null,
        intentTerritory: m.intentTerritory,
        geographyScope: m.geographyScope,
        commercialRegion: m.commercialRegion,
        country: m.country,
        promptTextHash: hashPromptText(m.promptText),
        promptText: m.promptText,
      })),
    },
    monitoringEligibleOnly: true,
    peerSetId: pickDefaultPeerSetId(entityScope, commercialRegion),
    plannedRuns,
    maxTestRuns: maxRuns,
    estimatedUsdPerCall: estPerCall,
    estimatedMaxCostUsd: estimatedMaxCost,
    maxBatchCostUsd: maxBatchCost,
    prerequisitesOk: prerequisites.length === 0,
    prerequisites,
    warnings,
    promptsLoaded: loaded.prompts.length,
    malformedPromptRows: loaded.malformed?.length || 0,
  };
}

async function runWithRetry({ runFn, maxRetries = 1, backoffMs = 1500 }) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= maxRetries) {
    try {
      const result = await runFn();
      return { result, retries: attempt, error: null };
    } catch (err) {
      lastErr = err;
      if (attempt >= maxRetries || !isRetryableProviderError(err) || isAuthProviderError(err)) {
        return { result: null, retries: attempt, error: err };
      }
      await sleep(backoffMs * (attempt + 1));
      attempt += 1;
    }
  }
  return { result: null, retries: attempt, error: lastErr };
}

/**
 * Execute a governed cohort (live) or return dry-run plan.
 */
export async function executeAiVisibilityCohort(args = {}) {
  const dryRun = args.dryRun !== false && !args.execute;
  const forceNewBatch = Boolean(args.forceNewBatch);
  const store = args.store || createAiVisibilityStore({ rootDir: args.storeRoot });

  const plan = await planAiVisibilityCohort(args);
  if (dryRun) return { mode: "dry-run", ...plan };

  // Live gates
  if (!isAiVisibilityEnabled()) {
    throw Object.assign(new Error("AI_VISIBILITY_ENABLED must be true"), {
      code: "gate",
      health: BATCH_HEALTH.PROMPT_CONFIG_ERROR,
    });
  }
  if (!isAiVisibilityLiveTestAllowed()) {
    throw Object.assign(new Error("AI_VISIBILITY_LIVE_TEST must be true"), {
      code: "gate",
      health: BATCH_HEALTH.PROMPT_CONFIG_ERROR,
    });
  }
  if (!process.env.OPENAI_API_KEY) {
    throw Object.assign(new Error("OPENAI_API_KEY missing"), {
      code: "gate",
      health: BATCH_HEALTH.PROVIDER_AUTH_ERROR,
    });
  }
  if (!args.execute) {
    throw Object.assign(new Error("Refusing live execution without --execute"), { code: "gate" });
  }
  if (String(plan.model) !== "gpt-5.6" && !args.allowModelOverride) {
    throw Object.assign(
      new Error(`Model must be gpt-5.6 (got ${plan.model}). Pass allowModelOverride only if founder-approved.`),
      { code: "gate" }
    );
  }
  if (!plan.prerequisitesOk) {
    throw Object.assign(new Error(`Prerequisites failed: ${plan.prerequisites.join("; ")}`), {
      code: "gate",
      health: BATCH_HEALTH.PROMPT_CONFIG_ERROR,
      prerequisites: plan.prerequisites,
    });
  }

  // Duplicate protection
  if (!forceNewBatch) {
    const existing = await store.listBatches();
    const dup = findDuplicateRecentBatch(existing, {
      cohortFingerprint: plan.cohort.fingerprint,
      provider: plan.provider,
      model: plan.model,
      geographyScope: plan.geographyScope,
      commercialRegion: plan.commercialRegion,
      country: plan.country,
      language: plan.language || "en",
    });
    if (dup) {
      throw Object.assign(
        new Error(
          `Duplicate recent batch ${dup.batchId} for same cohort/model within safety window. Use --force-new-batch to override.`
        ),
        { code: "duplicate", batchId: dup.batchId }
      );
    }
  }

  // Live entity index
  let entityIndex;
  try {
    if (args.entityIndex) {
      entityIndex = args.entityIndex;
    } else {
      const live = await buildLiveAiVisibilityEntityIndex({ applyOverlay: true });
      entityIndex = live.index;
    }
  } catch (err) {
    throw Object.assign(new Error(`Entity index load failed: ${err.message}`), {
      code: "entity_index",
      health: BATCH_HEALTH.ENTITY_INDEX_ERROR,
    });
  }

  const peerCfg = args.peerSetConfig || loadPeerSetConfig();
  const peerRaw = resolvePeerSetMembership(
    {
      peerSetId: args.peerSetId || plan.peerSetId,
      commercialRegion: plan.commercialRegion,
    },
    peerCfg
  );
  const peerSet = validatePeerSetAgainstIndex(peerRaw, entityIndex);

  const { batch, manifest } = createExecutionBatch({
    cohort: {
      ...plan.cohort,
      language: plan.language,
      members: plan.cohort.members.map((m) => {
        const full = (args._promptLookup || new Map()).get?.(m.promptId);
        return { ...m, promptText: m.promptText || full?.promptText };
      }),
    },
    stakeholder: plan.stakeholder,
    entityScope: plan.entityScope,
    geographyScope: plan.geographyScope,
    commercialRegion: plan.commercialRegion,
    country: plan.country,
    language: plan.language,
    provider: plan.provider,
    model: plan.model,
    peerSet,
    entityIndexFingerprint: entityIndex.fingerprint,
  });

  // Re-attach full prompt texts from plan members
  const promptById = new Map();
  // Reload prompts for text (already in plan via members if we attached)
  const loadedAgain = await loadGovernedAiVisibilityPrompts(
    { activeOnly: true, monitoringEligible: true },
    { mode: args.promptMode || "auto", fixturePath: args.fixturePath }
  );
  for (const p of loadedAgain.prompts) promptById.set(p.promptId, p);
  manifest.prompts = manifest.prompts.map((m) => {
    const full = promptById.get(m.promptId);
    return {
      ...m,
      promptText: full?.promptText || null,
      promptTextHash: hashPromptText(full?.promptText || ""),
      intentTerritory: full?.intentTerritory || m.intentTerritory,
    };
  });

  await store.saveBatchManifest(manifest);
  batch.startedAt = new Date().toISOString();
  batch.status = "running";
  await store.saveBatch(batch);

  const maxBatchCost = resolveMaxBatchCostUsd();
  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  const observations = [];
  const evidenceTraces = [];
  let successfulRuns = 0;
  let failedRuns = 0;
  let retriesTotal = 0;
  let costLimitReached = false;
  let providerAuthError = false;
  const errorSummary = [];
  const usage = { inputTokens: 0, outputTokens: 0, totalTokens: 0, estimatedCost: 0 };
  let mentionsExtracted = 0;
  let citationsExtracted = 0;
  let evidenceCount = 0;
  let unresolvedMeaningful = 0;
  const nameKeys = knownNameKeys(entityIndex.entities);

  for (const entry of manifest.prompts) {
    if (usage.estimatedCost >= maxBatchCost) {
      costLimitReached = true;
      break;
    }

    const prompt = promptById.get(entry.promptId);
    if (!prompt?.promptText) {
      failedRuns += 1;
      errorSummary.push({
        promptId: entry.promptId,
        version: entry.version,
        errorClass: "prompt_missing_text",
        message: "Prompt text missing from governed loader",
        retries: 0,
        timestamp: new Date().toISOString(),
      });
      continue;
    }

    const runId = store.generateId("run");
    const runBase = {
      runId,
      batchId: batch.batchId,
      promptId: entry.promptId,
      promptVersion: entry.version,
      provider: plan.provider,
      model: plan.model,
      status: "running",
      startedAt: new Date().toISOString(),
    };
    await store.saveRun(runBase);

    const { result, retries, error } = await runWithRetry({
      runFn: () =>
        runFn({
          prompt: { text: prompt.promptText, promptId: prompt.promptId },
          model: plan.model,
          apiKey: process.env.OPENAI_API_KEY,
          enableWebSearch: true,
          fetchImpl: args.fetchImpl,
        }),
      maxRetries: 1,
      backoffMs: 1500,
    });
    retriesTotal += retries;

    if (error || !result) {
      failedRuns += 1;
      if (isAuthProviderError(error)) providerAuthError = true;
      const failRec = {
        ...runBase,
        status: "failed",
        completedAt: new Date().toISOString(),
        retries,
        error: {
          type: error?.type || "provider_error",
          status: error?.status ?? null,
          message: String(error?.message || "unknown").slice(0, 500),
          retryable: isRetryableProviderError(error),
        },
      };
      await store.saveRun(failRec);
      errorSummary.push({
        runId,
        promptId: entry.promptId,
        version: entry.version,
        errorClass: failRec.error.type,
        status: failRec.error.status,
        retries,
        message: failRec.error.message,
        timestamp: failRec.completedAt,
      });
      if (providerAuthError) break;
      continue;
    }

    // Cost ledger — prefer token-based if available, else per-call estimate
    const inTok = result.usage?.inputTokens || 0;
    const outTok = result.usage?.outputTokens || 0;
    const totTok = result.usage?.totalTokens || inTok + outTok;
    usage.inputTokens += inTok;
    usage.outputTokens += outTok;
    usage.totalTokens += totTok;
    const callCost =
      totTok > 0
        ? Math.max(resolveEstimatedUsdPerCall() * 0.5, (totTok / 1_000_000) * 15)
        : resolveEstimatedUsdPerCall();
    usage.estimatedCost += callCost;

    const responseId = store.generateId("resp");
    const response = {
      responseId,
      runId,
      batchId: batch.batchId,
      promptId: entry.promptId,
      promptVersion: entry.version,
      provider: result.provider,
      model: result.model,
      text: result.text,
      citations: result.citations,
      usage: result.usage,
      latencyMs: result.latencyMs,
      citationCapability: result.citationCapability,
      parserVersion: result.parserVersion,
      providerMeta: result.providerMeta,
      raw: result.raw,
      createdAt: new Date().toISOString(),
    };
    await store.saveResponse(response);

    const mentions = extractMentions({
      responseId,
      text: response.text,
      entityIndex: entityIndex.aliasIndex,
      promptIntentTerritory: prompt.intentTerritory,
    });
    const citations = extractCitations({
      responseId,
      providerCitations: (response.citations || []).map((c) => ({
        ...c,
        startIndex: c.startIndex ?? c.start_index ?? null,
        endIndex: c.endIndex ?? c.end_index ?? null,
      })),
      entities: entityIndex.entities,
      mentions,
      responseText: response.text,
    });
    await store.saveMentions(responseId, mentions);
    await store.saveCitations(responseId, citations);
    mentionsExtracted += mentions.length;
    citationsExtracted += citations.length;

    const unresolved = harvestUnresolvedWithFilterStats(response.text, nameKeys);
    unresolvedMeaningful += unresolved.filteredUnresolvedCount || 0;

    const observation = buildObservationFromExtractions({
      observationId: `obs_${responseId}`,
      promptId: entry.promptId,
      provider: plan.provider,
      periodKey: batch.batchId,
      success: true,
      mentions,
      citations,
      geography: prompt.geography || {
        geographyScope: String(plan.geographyScope || "").toLowerCase(),
        regionName: plan.commercialRegion,
        countryName: plan.country,
      },
      intentTerritory: prompt.intentTerritory,
    });
    observations.push(observation);

    const evidence = assembleEvidenceRecord({
      prompt: {
        promptId: entry.promptId,
        version: entry.version,
        text: prompt.promptText,
        intentTerritory: prompt.intentTerritory,
        geographyScope: plan.geographyScope,
        region: plan.commercialRegion,
        country: plan.country,
        language: plan.language,
        semanticPairId: entry.semanticPairId || prompt.semanticPairId || null,
      },
      run: {
        ...runBase,
        status: "completed",
        completedAt: new Date().toISOString(),
        retries,
        language: plan.language,
      },
      response,
      mentions,
      citations,
      metrics: { observation },
      geography: observation.geography,
      language: plan.language,
    });
    await store.saveEvidence(evidence);
    evidenceCount += 1;

    await store.saveRun({
      ...runBase,
      status: "completed",
      completedAt: new Date().toISOString(),
      retries,
      responseId,
      evidenceId: evidence.evidenceId,
      usage: result.usage,
      estimatedCost: callCost,
    });

    successfulRuns += 1;
    if (evidenceTraces.length < 5 && observation.presentEntityIds[0]) {
      const presence = computeAiPresenceRate([observation], observation.presentEntityIds[0]);
      evidenceTraces.push({
        ...metricEvidenceTrace({
          metricResult: presence,
          evidenceId: evidence.evidenceId,
          observationIds: [observation.observationId],
        }),
        promptId: entry.promptId,
        responseId,
        batchId: batch.batchId,
      });
    }
  }

  const status = deriveBatchStatus({ successfulRuns, failedRuns, costLimitReached });
  const health = deriveBatchHealth({
    status,
    costLimitReached,
    entityIndexError: false,
    peerSetError: !peerSet.canonicalValid,
    providerAuthError,
    promptConfigError: false,
  });

  // Metrics over successful observations
  const promptIds = [...new Set(observations.map((o) => o.promptId))];
  const entityIdsForMetrics = peerSet.canonicalValid
    ? peerSet.entityIds
    : [
        ...new Set(
          observations.flatMap((o) => [
            ...(o.presentEntityIds || []),
            ...(o.recommendedEntityIds || []),
          ])
        ),
      ];

  const byEntity = {};
  const idToName = new Map(entityIndex.entities.map((e) => [e.id, e.name]));
  for (const id of entityIdsForMetrics) {
    const presence = computeAiPresenceRate(observations, id);
    const share = computeRecommendationShare(observations, id);
    const recRate = computeRecommendationRate(observations, id);
    const top3 = computeTop3RecommendationRate(observations, id);
    const first = computeFirstRecommendationRate(observations, id);
    const won = computeQuestionsWon(observations, id, promptIds);
    const missing = computeQuestionsMissing(observations, id, promptIds);
    const citation = computeCitationRate(observations, id);
    byEntity[idToName.get(id) || id] = {
      id,
      presence: presence.value,
      presenceDetail: presence,
      recommendationShare: share.value,
      recommendationRate: recRate.value,
      top3RecommendationRate: top3.value,
      firstRecommendationRate: first.value,
      questionsWon: won.value ?? won.count,
      questionsMissing: missing.value ?? missing.count,
      citationRate: citation.value,
      citationReadiness: "PARTIAL",
    };

    // Historical snapshots (additive metrics do not invalidate ai_visibility_metrics_v1)
    for (const [metric, detail] of [
      ["ai_presence_rate", presence],
      ["recommendation_share", share],
      ["recommendation_rate", recRate],
      ["top3_recommendation_rate", top3],
      ["first_recommendation_rate", first],
      ["citation_rate", citation],
    ]) {
      await store.saveMetricSnapshot({
        batchId: batch.batchId,
        batchDate: batch.startedAt,
        entityId: id,
        entityName: idToName.get(id) || null,
        metric,
        value: detail.value,
        numerator: detail.numerator,
        denominator: detail.denominator,
        geographyScope: plan.geographyScope,
        commercialRegion: plan.commercialRegion,
        country: plan.country,
        language: plan.language || batch.language || "en",
        provider: plan.provider,
        model: plan.model,
        metricVersion: METRIC_VERSION,
        peerSetId: peerSet.peerSetId,
        citationReadiness: metric === "citation_rate" ? "PARTIAL" : undefined,
      });
    }
  }

  let competitivePosition = null;
  if (peerSet.canonicalValid && peerSet.entityIds?.length) {
    competitivePosition = computeCompetitivePosition(observations, peerSet.entityIds);
    competitivePosition = {
      ...competitivePosition,
      peers: (competitivePosition.peers || []).map((p) => ({
        ...p,
        name: idToName.get(p.entityId) || null,
      })),
    };
  }

  const metricReadiness = assessMetricReadiness({
    classificationIntegrity: true,
    citationAssociationCompleteness: "partial",
    testCoverage: true,
    parentBrandCollisions: 0,
    manualClassificationAccuracy: 0.92,
  });

  const completedAt = new Date().toISOString();
  const finalBatch = await store.updateBatch(batch.batchId, {
    status,
    health,
    completedAt,
    successfulRuns,
    failedRuns,
    skippedRuns: costLimitReached
      ? Math.max(0, plan.plannedRuns - successfulRuns - failedRuns)
      : 0,
    retries: retriesTotal,
    usage,
    errorSummary,
    peerSetValid: peerSet.canonicalValid,
    peerSetError: peerSet.error,
  });

  const summary = {
    label: "CONTROLLED GOVERNED COHORT — NOT PRODUCTION BENCHMARK",
    batchId: batch.batchId,
    status,
    health,
    cohort: {
      stakeholder: plan.stakeholder,
      entityScope: plan.entityScope,
      geographyScope: plan.geographyScope,
      commercialRegion: plan.commercialRegion,
      country: plan.country,
      fingerprint: plan.cohort.fingerprint,
      promptCount: plan.plannedRuns,
    },
    provider: { name: plan.provider, model: plan.model },
    execution: {
      planned: plan.plannedRuns,
      successful: successfulRuns,
      failed: failedRuns,
      retries: retriesTotal,
      costLimitReached,
    },
    usage,
    peerSet: {
      peerSetId: peerSet.peerSetId,
      peerSetVersion: peerSet.peerSetVersion,
      canonicalValid: peerSet.canonicalValid,
      missingEntityIds: peerSet.missingEntityIds || [],
      error: peerSet.error,
    },
    metrics: {
      label: "CONTROLLED GOVERNED COHORT — NOT PRODUCTION BENCHMARK",
      metricVersion: METRIC_VERSION,
      byEntity,
      competitivePosition,
      citationRateReadiness: "PARTIAL",
      metricReadiness,
    },
    evidenceCount,
    citationCount: citationsExtracted,
    mentionCount: mentionsExtracted,
    unresolvedEntityCount: unresolvedMeaningful,
    evidenceTraces,
    AIRTABLE_EXECUTION_WRITES: 0,
    AI_VISIBILITY_OPPORTUNITY_WRITES: 0,
    versions: {
      resolver: RESOLVER_VERSION,
      classifier: RECOMMENDATION_CLASSIFIER_VERSION,
      citationAssoc: CITATION_ASSOC_VERSION,
      metric: METRIC_VERSION,
      geography: GEOGRAPHY_MODEL_VERSION,
      engine: EXECUTE_ENGINE_VERSION,
    },
    startedAt: batch.startedAt,
    completedAt,
    elapsedMs: Date.parse(completedAt) - Date.parse(batch.startedAt),
  };

  await store.saveBatchSummary(summary);

  return {
    mode: "execute",
    batch: finalBatch,
    manifest,
    summary,
    AIRTABLE_EXECUTION_WRITES: 0,
  };
}

export { buildDuplicateKey };
