/**
 * Phase 3B.3 — Full 84-call provider baseline orchestrator.
 * Provider-pure · baseline purpose · isolated storage · checkpoint/resume.
 */

import fs from "fs";
import path from "path";
import { randomBytes } from "crypto";
import {
  isAiVisibilityEnabled,
  isAiVisibilityLiveTestAllowed,
  METRIC_VERSION,
  RECOMMENDATION_CLASSIFIER_VERSION,
  CITATION_ASSOC_VERSION,
  RESOLVER_VERSION,
  GEOGRAPHY_MODEL_VERSION,
} from "./config.js";
import { MONITORING_RUN_PURPOSE } from "./monitoring-run-purpose.js";
import { buildProviderBaselineExecutionPlan } from "./provider-baseline-plan.js";
import {
  PROVIDER_BASELINE_SERIES,
  PROVIDER_BASELINE_HARD_CAPS,
} from "./provider-baseline-state.js";
import {
  createValidationCostLedger,
  applyValidationCallCost,
} from "./provider-validation-cost.js";
import {
  isRetryableProviderError,
  isAuthProviderError,
  BATCH_HEALTH,
} from "./execution-batch.js";
import { createAiVisibilityStore } from "./storage/index.js";
import { resolveProviderBaselineStoreRoot } from "./storage/resolve-store-root.js";
import { buildLiveAiVisibilityEntityIndex } from "./entity-index.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "./peer-sets.js";
import { validatePeerSetAgainstIndex } from "./execute-cohort.js";
import { runVisibilityPrompt } from "./providers/index.js";
import { normalizeVisibilityProviderResponse } from "./providers/normalized-response.js";
import { extractMentions } from "./extract-mentions.js";
import { extractCitations, parseDomain } from "./extract-citations.js";
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
  computeCitationRate,
  computeCompetitivePosition,
} from "./metrics.js";
import { assembleEvidenceRecord } from "./evidence.js";
import {
  GEMINI_RETRY_POLICY,
  PERPLEXITY_RETRY_POLICY,
  CLAUDE_RETRY_POLICY,
} from "./providers/provider-retry-policy.js";
import { classifyProviderError } from "./providers/provider-errors.js";
import { WAVE1_EXECUTION_ORDER, WAVE1_SHOWCASE_PLAN_VERSION } from "./wave1-showcase-plan.js";
import {
  assertFingerprintExecutable,
  CompletedFingerprintProtectionError,
} from "./baseline-fingerprint-protection.js";

export const PROVIDER_BASELINE_ORCHESTRATOR_VERSION =
  "ai_visibility_provider_baseline_orchestrator_v1";

const RETRY_BY_PROVIDER = {
  gemini: GEMINI_RETRY_POLICY,
  perplexity: PERPLEXITY_RETRY_POLICY,
  claude: CLAUDE_RETRY_POLICY,
};

const DEFAULT_MODELS = {
  gemini: "gemini-3.6-flash",
  perplexity: "sonar",
  claude: "claude-sonnet-4-6",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function checkpointPath(storeRoot, waveId) {
  return path.join(storeRoot, "checkpoints", `${waveId}.json`);
}

function createBaselineWaveId(provider, now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const min = String(now.getUTCMinutes()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_baseline_${provider}_${y}${m}${d}_${h}${min}_${rand}`;
}

function resolveApiKey(provider) {
  if (provider === "gemini") return process.env.GEMINI_API_KEY;
  if (provider === "perplexity") return process.env.PERPLEXITY_API_KEY;
  if (provider === "claude") return process.env.ANTHROPIC_API_KEY;
  return null;
}

function resolveModel(provider, requested) {
  return (
    requested ||
    process.env[`AI_VISIBILITY_${provider.toUpperCase()}_MODEL`] ||
    DEFAULT_MODELS[provider]
  );
}

function knownNameKeys(entities) {
  const keys = new Set();
  for (const e of entities || []) {
    keys.add(normalizeMatchKey(e.name));
    for (const a of e.aliases || []) keys.add(normalizeMatchKey(a));
  }
  return keys;
}

async function runWithRetry({ runFn, maxRetries = 1, backoffMs = 1500 }) {
  let attempt = 0;
  let lastErr = null;
  let providerAttempts = 0;
  while (attempt <= maxRetries) {
    providerAttempts += 1;
    try {
      const result = await runFn();
      return { result, retries: attempt, error: null, providerAttempts };
    } catch (err) {
      lastErr = err;
      if (attempt >= maxRetries || !isRetryableProviderError(err) || isAuthProviderError(err)) {
        return { result: null, retries: attempt, error: err, providerAttempts };
      }
      await sleep(backoffMs * (attempt + 1));
      attempt += 1;
    }
  }
  return { result: null, retries: attempt, error: lastErr, providerAttempts };
}

function initCheckpoint({ waveId, provider, model, hardCapUsd, storeRoot, baselineSeriesId }) {
  const now = new Date().toISOString();
  const slots = Object.fromEntries(
    WAVE1_EXECUTION_ORDER.map((s) => [
      s.key,
      {
        key: s.key,
        planned: 12,
        succeeded: 0,
        failed: 0,
        attempts: 0,
        retries: 0,
        cost: 0,
        status: "pending",
      },
    ])
  );
  return {
    waveId,
    provider,
    model,
    baselineSeriesId,
    storeRoot,
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
    status: "running",
    createdAt: now,
    updatedAt: now,
    completedFingerprints: {},
    failedFingerprints: {},
    slots,
    logical: { succeeded: 0, failedFinal: 0, notExecuted: 84, retried: 0, totalAttempts: 0 },
    costLedger: createValidationCostLedger(provider, hardCapUsd),
    modelReturned: [],
    startedAt: now,
    completedAt: null,
    stopReason: null,
  };
}

function persistCheckpoint(cp) {
  cp.updatedAt = new Date().toISOString();
  writeJson(checkpointPath(cp.storeRoot, cp.waveId), cp);
}

function recomputeLogicalTotals(cp) {
  const completed = Object.keys(cp.completedFingerprints).length;
  const failed = Object.keys(cp.failedFingerprints).length;
  cp.logical.succeeded = completed;
  cp.logical.failedFinal = failed;
  cp.logical.notExecuted = Math.max(0, 84 - completed - failed);
  cp.logical.totalAttempts = cp.costLedger.providerAttempts || 0;
}

async function persistSuccessfulBaselineCall({
  store,
  waveId,
  storeRoot,
  exec,
  result,
  entityIndex,
  nameKeys,
}) {
  const runId = store.generateId("run");
  const responseId = store.generateId("resp");

  const rawArtifactUri = path.join(storeRoot, "waves", waveId, "raw", `${exec.fingerprint}.json`);
  writeJson(rawArtifactUri, {
    fingerprint: exec.fingerprint,
    waveId,
    provider: result.provider,
    model: result.model,
    raw: result.raw,
    savedAt: new Date().toISOString(),
  });

  const normalized = normalizeVisibilityProviderResponse(result, {
    promptId: exec.promptId,
    promptVersion: exec.version,
    promptFamily: exec.promptFamily,
    semanticPairId: exec.semanticPairId,
    geography: exec.geographyKey,
    geographyKey: exec.geographyKey,
    language: exec.language,
    intent: exec.intent,
    peerSetId: PEER_SET_ID_V2,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
    rawArtifactUri,
    useV1_1: true,
  });
  writeJson(
    path.join(storeRoot, "waves", waveId, "normalized", `${exec.fingerprint}.json`),
    normalized
  );

  const response = {
    responseId,
    runId,
    batchId: waveId,
    waveId,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    promptVersion: exec.version,
    promptFamily: exec.promptFamily,
    semanticPairId: exec.semanticPairId,
    provider: result.provider,
    model: result.model,
    text: result.text,
    rawText: result.text,
    citations: result.citations,
    searchResults: result.searchResults || null,
    usage: result.usage,
    latencyMs: result.latencyMs,
    citationCapability: result.citationCapability,
    providerMeta: result.providerMeta,
    raw: result.raw,
    normalized,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    peerSetId: PEER_SET_ID_V2,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
    baselineSeriesId: exec.baselineSeriesId,
    createdAt: new Date().toISOString(),
  };
  await store.saveResponse(response);

  const mentions = extractMentions({
    responseId,
    text: response.text,
    entityIndex: entityIndex.aliasIndex,
    promptIntentTerritory: exec.intent,
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
  harvestUnresolvedWithFilterStats(response.text, nameKeys);

  const run = {
    runId,
    responseId,
    batchId: waveId,
    waveId,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    provider: result.provider,
    model: result.model,
    status: "completed",
    rawText: result.text,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
    baselineSeriesId: exec.baselineSeriesId,
    latencyMs: result.latencyMs,
    completedAt: new Date().toISOString(),
  };
  await store.saveRun(run);

  const evidence = assembleEvidenceRecord({
    prompt: { promptId: exec.promptId, version: exec.version, text: exec.promptText },
    run,
    response,
    mentions,
    citations,
    language: exec.language,
  });
  await store.saveEvidence(evidence);

  const observation = buildObservationFromExtractions({
    observationId: `obs_${responseId}`,
    promptId: exec.promptId,
    provider: result.provider,
    periodKey: waveId,
    success: true,
    mentions,
    citations,
    geography: {
      geographyScope:
        exec.geographyKey === "GLOBAL"
          ? "global"
          : exec.geographyKey === "MEXICO"
            ? "country"
            : "region",
      regionName:
        exec.geographyKey === "CALA" || exec.geographyKey === "MEXICO"
          ? "CALA"
          : exec.geographyKey === "EUROPE"
            ? "Europe"
            : exec.geographyKey === "NORTH_AMERICA"
              ? "North America"
              : null,
      countryName: exec.geographyKey === "MEXICO" ? "Mexico" : null,
    },
    intentTerritory: exec.intent,
  });

  return { run, response, mentions, citations, observation };
}

async function generateBaselineMetrics({ store, cp, observations, peerSet, entityIndex, provider }) {
  const promptIds = [...new Set(observations.map((o) => o.promptId))];
  const entityIds = peerSet.entityIds || [];
  const idToName = new Map(entityIndex.entities.map((e) => [e.id, e.name]));
  const byEntity = {};

  for (const id of entityIds) {
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
      recommendationShare: share.value,
      recommendationRate: recRate.value,
      top3RecommendationRate: top3.value,
      firstRecommendationRate: first.value,
      questionsWon: won.value ?? won.count,
      questionsMissing: missing.value ?? missing.count,
      citationRate: citation.value,
    };
    for (const [metric, detail] of [
      ["ai_presence_rate", presence],
      ["recommendation_share", share],
      ["recommendation_rate", recRate],
      ["top3_recommendation_rate", top3],
      ["first_recommendation_rate", first],
      ["citation_rate", citation],
    ]) {
      await store.saveMetricSnapshot({
        batchId: cp.waveId,
        waveId: cp.waveId,
        batchDate: cp.createdAt,
        entityId: id,
        entityName: idToName.get(id) || null,
        metric,
        value: detail.value,
        numerator: detail.numerator,
        denominator: detail.denominator,
        geographyScope: "wave1_multi",
        commercialRegion: null,
        country: null,
        language: "multi",
        provider,
        model: cp.model,
        metricVersion: METRIC_VERSION,
        peerSetId: PEER_SET_ID_V2,
        monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
        baselineSeriesId: cp.baselineSeriesId,
        COMPARABLE_PRIOR_PERIOD: "NONE",
        TREND_AVAILABLE: false,
      });
    }
  }

  let competitivePosition = null;
  if (entityIds.length) {
    competitivePosition = computeCompetitivePosition(observations, entityIds);
  }
  return { byEntity, competitivePosition };
}

/**
 * Execute full 84-call provider baseline.
 */
export async function executeProviderBaseline(args = {}) {
  const provider = String(args.provider || "").toLowerCase();
  if (!["gemini", "perplexity", "claude"].includes(provider)) {
    throw new Error(`Invalid baseline provider: ${provider}`);
  }

  if (!args.force && !isAiVisibilityEnabled()) {
    throw new Error("AI_VISIBILITY_ENABLED must be true for live baseline");
  }
  if (!args.force && !isAiVisibilityLiveTestAllowed()) {
    throw new Error("AI_VISIBILITY_LIVE_TEST must be true for live baseline");
  }

  const plan = buildProviderBaselineExecutionPlan(provider);
  if (!plan.ok) throw new Error(`Baseline plan invalid: ${(plan.errors || []).join("; ")}`);

  const baselineSeriesId = args.baselineSeriesId || PROVIDER_BASELINE_SERIES[provider];
  const model = resolveModel(provider, args.model);
  const hardCapUsd = args.hardCapUsd ?? PROVIDER_BASELINE_HARD_CAPS[provider] ?? 50;
  const storeRoot = args.storeRoot || resolveProviderBaselineStoreRoot(provider);
  const store = createAiVisibilityStore({ rootDir: storeRoot });
  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  const retryPolicy = RETRY_BY_PROVIDER[provider];
  const waveId = args.waveId || createBaselineWaveId(provider);
  const now = new Date();

  let cp = args.resume ? readJson(checkpointPath(storeRoot, args.waveId || waveId)) : null;
  if (cp && args.completionHardCapUsd != null) {
    cp.costLedger.hardCapUsd = args.completionHardCapUsd;
    cp.costLedger.capBreached = false;
    cp.stopReason = null;
    cp.status = "running";
    cp.completedAt = null;
    persistCheckpoint(cp);
  }
  if (cp && args.retryFailedFingerprints) {
    const retryable = Object.entries(cp.failedFingerprints || {}).filter(([, v]) => {
      if (args.retryFailedCategories?.length) {
        return args.retryFailedCategories.includes(v?.error?.category);
      }
      return v?.error?.retryable === true || v?.error?.category === "SERVER";
    });
    const slotsToReopen = new Set();
    for (const [fp, v] of retryable) {
      delete cp.failedFingerprints[fp];
      if (v?.slot) slotsToReopen.add(v.slot);
    }
    for (const slotKey of slotsToReopen) {
      const slot = cp.slots[slotKey];
      if (!slot) continue;
      slot.failed = Object.values(cp.failedFingerprints).filter((f) => f.slot === slotKey).length;
      slot.status = slot.succeeded === slot.planned ? "completed" : "running";
    }
    cp.status = "running";
    cp.completedAt = null;
    cp.stopReason = null;
    recomputeLogicalTotals(cp);
    persistCheckpoint(cp);
  }
  if (!cp) {
    cp = initCheckpoint({ waveId, provider, model, hardCapUsd, storeRoot, baselineSeriesId });
    await store.saveBatch({
      batchId: waveId,
      waveId,
      kind: "provider_baseline",
      baselineSeriesId,
      peerSetId: PEER_SET_ID_V2,
      provider: { name: provider, model },
      model,
      status: "running",
      plannedRuns: 84,
      monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
      startedAt: cp.createdAt,
      metricVersion: METRIC_VERSION,
    });
    persistCheckpoint(cp);
  }

  const live = args.entityIndex
    ? { index: args.entityIndex }
    : await buildLiveAiVisibilityEntityIndex({ applyOverlay: true });
  const peerCfg = loadPeerSetConfig();
  const peerRaw = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, peerCfg);
  const peerSet = validatePeerSetAgainstIndex(peerRaw, live.index);
  const nameKeys = knownNameKeys(live.index.entities);

  const executionsBySlot = new Map();
  for (const slot of WAVE1_EXECUTION_ORDER) {
    executionsBySlot.set(
      slot.key,
      plan.EXECUTIONS.filter((e) => e.slot === slot.key).map((e) => ({
        ...e,
        baselineSeriesId,
      }))
    );
  }

  const protectCompleted =
    args.protectCompletedFingerprints !== false &&
    (args.onlyMissingFingerprints || args.onlyFingerprints?.length || args.completionMode);

  let onlyFingerprintSet = null;
  if (args.onlyFingerprints?.length) {
    onlyFingerprintSet = new Set(args.onlyFingerprints.map(String));
  } else if (args.onlyMissingFingerprints) {
    onlyFingerprintSet = new Set(
      plan.EXECUTIONS.filter((e) => !cp?.completedFingerprints?.[e.fingerprint]).map(
        (e) => e.fingerprint
      )
    );
  }

  const maxRetriesThisRun =
    args.maxRetriesPerCall != null ? args.maxRetriesPerCall : retryPolicy.maxRetriesPerCall;

  const phaseAttemptsStart = cp?.costLedger?.providerAttempts || 0;
  const observations = [];
  if (args.resume || Object.keys(cp.completedFingerprints).length) {
    const existingRuns = (await store.listBatchRuns(cp.waveId)) || [];
    for (const run of existingRuns.filter((r) => r.status === "completed")) {
      const mentions = (await store.getMentions(run.responseId)) || [];
      const citations = (await store.getCitations(run.responseId)) || [];
      observations.push(
        buildObservationFromExtractions({
          observationId: `obs_${run.responseId}`,
          promptId: run.promptId,
          provider,
          periodKey: cp.waveId,
          success: true,
          mentions,
          citations,
          geography: {
            geographyScope:
              run.geographyKey === "GLOBAL"
                ? "global"
                : run.geographyKey === "MEXICO"
                  ? "country"
                  : "region",
            regionName:
              run.geographyKey === "CALA" || run.geographyKey === "MEXICO"
                ? "CALA"
                : run.geographyKey === "EUROPE"
                  ? "Europe"
                  : run.geographyKey === "NORTH_AMERICA"
                    ? "North America"
                    : null,
            countryName: run.geographyKey === "MEXICO" ? "Mexico" : null,
          },
          intentTerritory: run.intent,
        })
      );
    }
  }

  const stats = {
    provider,
    waveId,
    baselineSeriesId,
    PLANNED: 84,
    SUCCEEDED: cp.logical.succeeded,
    FAILED: cp.logical.failedFinal,
    ATTEMPTS: 0,
    RETRIES: 0,
    TOTAL_ATTEMPTS: 0,
    startedAt: cp.startedAt,
    completedAt: null,
    storeRoot,
    modelRequested: model,
    modelReturned: cp.modelReturned || [],
    slotResults: {},
    stopReason: null,
    OPENAI_BASELINE_UNTOUCHED: true,
  };

  ensureDir(path.join(storeRoot, "waves", waveId, "raw"));
  ensureDir(path.join(storeRoot, "waves", waveId, "normalized"));

  for (const slotDef of WAVE1_EXECUTION_ORDER) {
    const slotKey = slotDef.key;
    const slot = cp.slots[slotKey];
    if (slot.status === "completed") {
      stats.slotResults[slotKey] = { ...slot };
      continue;
    }
    if (cp.stopReason) break;

    slot.status = "running";
    persistCheckpoint(cp);
    const slotExecs = executionsBySlot.get(slotKey) || [];

    for (const exec of slotExecs) {
      if (onlyFingerprintSet && !onlyFingerprintSet.has(exec.fingerprint)) continue;
      if (cp.completedFingerprints[exec.fingerprint]) continue;
      if (cp.failedFingerprints[exec.fingerprint]?.exhausted && !args.retryFailedFingerprints) {
        continue;
      }

      if (protectCompleted) {
        assertFingerprintExecutable(cp, exec.fingerprint, { protectCompleted: true });
      }

      if (cp.costLedger.capBreached) {
        cp.stopReason = "hard_cost_cap";
        slot.status = "stopped_cost_cap";
        break;
      }

      stats.ATTEMPTS += 1;
      slot.attempts += 1;

      const { result, retries, error, providerAttempts } = await runWithRetry({
        runFn: () =>
          runFn({
            provider,
            prompt: { text: exec.promptText, promptId: exec.promptId },
            model,
            apiKey: resolveApiKey(provider),
            enableWebSearch: true,
            timeoutMs: Number(
              process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || retryPolicy.timeoutMsDefault
            ),
            fetchImpl: args.fetchImpl,
          }),
        maxRetries: maxRetriesThisRun,
        backoffMs: retryPolicy.backoffMs,
      });

      stats.TOTAL_ATTEMPTS += providerAttempts;
      stats.RETRIES += retries;
      slot.retries += retries;
      cp.costLedger.providerAttempts = (cp.costLedger.providerAttempts || 0) + providerAttempts;

      if (error || !result) {
        stats.FAILED += 1;
        slot.failed += 1;
        const classified = classifyProviderError(error);
        cp.failedFingerprints[exec.fingerprint] = {
          exhausted: true,
          error: classified,
          slot: slotKey,
        };
        if (classified.category === "AUTH") {
          cp.stopReason = "provider_auth_error";
          slot.status = "failed_auth";
          break;
        }
      } else {
        applyValidationCallCost(cp.costLedger, result.usage, provider);
        slot.cost = cp.costLedger.actualUsd;
        if (result.model) cp.modelReturned.push(result.model);

        try {
          const persisted = await persistSuccessfulBaselineCall({
            store,
            waveId: cp.waveId,
            storeRoot,
            exec,
            result,
            entityIndex: live.index,
            nameKeys,
          });
          cp.completedFingerprints[exec.fingerprint] = {
            runId: persisted.run.runId,
            slot: slotKey,
          };
          delete cp.failedFingerprints[exec.fingerprint];
          observations.push(persisted.observation);
          stats.SUCCEEDED += 1;
          slot.succeeded += 1;
        } catch (err) {
          stats.FAILED += 1;
          slot.failed += 1;
          cp.failedFingerprints[exec.fingerprint] = {
            exhausted: true,
            error: { category: "PIPELINE", message: err?.message || String(err) },
            slot: slotKey,
          };
        }
      }
      recomputeLogicalTotals(cp);
      persistCheckpoint(cp);
    }

    slot.status =
      slot.succeeded === slot.planned
        ? "completed"
        : slot.succeeded > 0
          ? "partial"
          : cp.stopReason
            ? cp.stopReason
            : "failed";
    stats.slotResults[slotKey] = { ...slot };
    persistCheckpoint(cp);
    if (cp.stopReason) break;
  }

  recomputeLogicalTotals(cp);
  cp.completedAt = new Date().toISOString();
  cp.status =
    cp.logical.succeeded === 84 && !cp.stopReason
      ? "completed"
      : cp.logical.succeeded > 0
        ? "partial"
        : "failed";

  const metrics =
    observations.length > 0
      ? await generateBaselineMetrics({
          store,
          cp,
          observations,
          peerSet,
          entityIndex: live.index,
          provider,
        })
      : { byEntity: {}, competitivePosition: null };

  const summary = {
    batchId: cp.waveId,
    waveId: cp.waveId,
    baselineSeriesId,
    provider: { name: provider, vendor: provider, model },
    requestedModel: model,
    returnedModel: [...new Set(cp.modelReturned)][0] || null,
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
    status: cp.status,
    stopReason: cp.stopReason,
    logical: cp.logical,
    slots: cp.slots,
    succeeded: cp.logical.succeeded,
    failed: cp.logical.failedFinal,
    plannedRuns: 84,
    costUsd: cp.costLedger.actualUsd,
    hardCapUsd: cp.costLedger.hardCapUsd ?? hardCapUsd,
    startedAt: cp.startedAt,
    completedAt: cp.completedAt,
    metrics,
    orchestratorVersion: PROVIDER_BASELINE_ORCHESTRATOR_VERSION,
    planVersion: WAVE1_SHOWCASE_PLAN_VERSION,
    metricVersion: METRIC_VERSION,
    peerSetId: PEER_SET_ID_V2,
    OPENAI_BASELINE_UNTOUCHED: true,
    versions: {
      resolver: RESOLVER_VERSION,
      classifier: RECOMMENDATION_CLASSIFIER_VERSION,
      citationAssoc: CITATION_ASSOC_VERSION,
      geography: GEOGRAPHY_MODEL_VERSION,
    },
  };

  await store.saveBatchSummary(summary);
  await store.updateBatch(cp.waveId, {
    status: cp.status,
    health:
      cp.stopReason === "hard_cost_cap"
        ? BATCH_HEALTH.COST_LIMIT_REACHED
        : cp.logical.failedFinal > 0
          ? BATCH_HEALTH.PARTIAL_PROVIDER_FAILURE
          : BATCH_HEALTH.HEALTHY,
    completedAt: cp.completedAt,
    successfulRuns: cp.logical.succeeded,
    failedRuns: cp.logical.failedFinal,
    usage: {
      estimatedCost: cp.costLedger.actualUsd,
      inputTokens: cp.costLedger.inputTokens,
      outputTokens: cp.costLedger.outputTokens,
    },
  });

  writeJson(path.join(storeRoot, "waves", cp.waveId, "baseline-summary.json"), summary);
  persistCheckpoint(cp);

  stats.completedAt = cp.completedAt;
  stats.costLedger = cp.costLedger;
  stats.status = cp.status;
  stats.metrics = metrics;
  stats.ACTUAL_MODEL_RETURNED = summary.returnedModel;
  stats.ATTEMPTS_THIS_PHASE = Math.max(
    0,
    (cp.costLedger.providerAttempts || 0) - phaseAttemptsStart
  );
  stats.COMPLETION_MODE = Boolean(args.completionMode || args.onlyMissingFingerprints);
  stats.PROTECTED_FINGERPRINTS = Object.keys(cp.completedFingerprints).length;

  return stats;
}

export function deriveProviderHardCapFromValidation(provider, validationStats) {
  const providerKey = String(provider || "").toLowerCase();
  const fixed = PROVIDER_BASELINE_HARD_CAPS[providerKey];
  if (providerKey === "perplexity") return fixed ?? 15;

  const cost = validationStats?.costLedger?.actualUsd;
  const succeeded = validationStats?.SUCCEEDED || 0;
  if (!(cost > 0) || !(succeeded > 0)) {
    return fixed ?? 50;
  }
  const avg = cost / succeeded;
  const projectedHigh = avg * 84 * 1.35;
  const buffered = Number((projectedHigh * 1.25).toFixed(2));
  return Math.min(Math.max(buffered, 10), fixed ?? 50);
}
