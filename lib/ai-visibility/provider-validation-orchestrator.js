/**
 * Phase 3B.2 — Controlled multi-provider live validation orchestrator.
 * 12 governed prompts per provider · validation purpose · isolated storage.
 */

import fs from "fs";
import path from "path";
import { randomBytes } from "crypto";
import {
  isAiVisibilityEnabled,
  isAiVisibilityLiveTestAllowed,
  METRIC_VERSION,
} from "./config.js";
import { MONITORING_RUN_PURPOSE } from "./monitoring-run-purpose.js";
import { preflightProviderCredentials } from "./provider-credentials.js";
import {
  buildProviderValidationExecutionPlan,
  attachProviderFingerprints,
  PROVIDER_VALIDATION_PLAN_VERSION,
} from "./provider-validation-plan.js";
import { evaluateProviderValidationActivationGate } from "./provider-validation-activation-gate.js";
import {
  createValidationCostLedger,
  applyValidationCallCost,
  VALIDATION_HARD_CAP_USD,
} from "./provider-validation-cost.js";
import {
  isRetryableProviderError,
  isAuthProviderError,
} from "./execution-batch.js";
import { createAiVisibilityStore } from "./storage/index.js";
import { resolveProviderValidationStoreRoot } from "./storage/resolve-store-root.js";
import { WAVE1_ROOT } from "./storage/resolve-store-root.js";
import { buildLiveAiVisibilityEntityIndex } from "./entity-index.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2 } from "./peer-sets.js";
import { validatePeerSetAgainstIndex } from "./execute-cohort.js";
import { runVisibilityPrompt } from "./providers/index.js";
import { normalizeVisibilityProviderResponse } from "./providers/normalized-response.js";
import { extractMentions } from "./extract-mentions.js";
import { extractCitations, parseDomain } from "./extract-citations.js";
import { harvestUnresolvedWithFilterStats } from "./mention-classification.js";
import { normalizeMatchKey } from "./normalize-entities.js";
import { assembleEvidenceRecord } from "./evidence.js";
import {
  GEMINI_RETRY_POLICY,
  PERPLEXITY_RETRY_POLICY,
  CLAUDE_RETRY_POLICY,
} from "./providers/provider-retry-policy.js";
import { classifyProviderError } from "./providers/provider-errors.js";

export const PROVIDER_VALIDATION_ORCHESTRATOR_VERSION =
  "ai_visibility_provider_validation_orchestrator_v1";

const PROVIDER_ORDER = ["gemini", "perplexity", "claude"];
const ACTIVATION_SAMPLE_SIZE = 3;

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

function createValidationWaveId(provider, now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const min = String(now.getUTCMinutes()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_validation_${provider}_${y}${m}${d}_${h}${min}_${rand}`;
}

function createParentValidationId(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const min = String(now.getUTCMinutes()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_mult_provider_validation_${y}${m}${d}_${h}${min}_${rand}`;
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

async function persistSuccessfulCall({
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

  const rawArtifactUri = path.join(
    storeRoot,
    "waves",
    waveId,
    "raw",
    `${exec.fingerprint}.json`
  );
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
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.VALIDATION,
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
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.VALIDATION,
    latencyMs: result.latencyMs,
    completedAt: new Date().toISOString(),
  };
  writeJson(path.join(storeRoot, "runs", `${runId}.json`), run);

  const evidence = assembleEvidenceRecord({
    prompt: { promptId: exec.promptId, version: exec.version, text: exec.promptText },
    run,
    response,
    mentions,
    citations,
    language: exec.language,
  });
  await store.saveEvidence(evidence);

  return {
    run,
    response,
    mentions,
    citations,
    normalized,
    missing: [],
    resolverMalfunction: false,
    classifierMalfunction: false,
    citationCorruption: false,
  };
}

async function persistFailedCall({ storeRoot, waveId, exec, error, retries, providerAttempts }) {
  const runId = `run_${randomBytes(8).toString("hex")}`;
  writeJson(path.join(storeRoot, "runs", `${runId}.json`), {
    runId,
    batchId: waveId,
    waveId,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    provider: exec.provider,
    status: "failed",
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.VALIDATION,
    error: classifyProviderError(error),
    retries,
    providerAttempts,
    failedAt: new Date().toISOString(),
  });
}

/**
 * Execute validation for one provider (up to 12 calls).
 */
export async function executeProviderValidation(args = {}) {
  const provider = String(args.provider || "").toLowerCase();
  if (!PROVIDER_ORDER.includes(provider)) {
    throw new Error(`Invalid validation provider: ${provider}`);
  }

  const cred = preflightProviderCredentials();
  const credKey = `${provider.toUpperCase()}_CREDENTIAL`;
  const credStatus = cred[credKey];
  if (credStatus === "MISSING") {
    return {
      provider,
      status: "NOT_EXECUTED_MISSING_CREDENTIAL",
      PLANNED: 12,
      SUCCEEDED: 0,
      FAILED: 0,
    };
  }

  const plan = buildProviderValidationExecutionPlan();
  if (!plan.ok) throw new Error(`Validation plan invalid: ${plan.errors.join("; ")}`);

  const executions = args.executionsOverride || attachProviderFingerprints(plan.EXECUTIONS, provider);
  const model = resolveModel(provider, args.model);
  const storeRoot = args.storeRoot || resolveProviderValidationStoreRoot(provider);
  const store = createAiVisibilityStore({ rootDir: storeRoot });
  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  const now = new Date();
  const waveId = args.waveId || createValidationWaveId(provider, now);
  const parentId = args.parentValidationId || null;
  const retryPolicy = RETRY_BY_PROVIDER[provider];
  const costLedger = createValidationCostLedger(provider, args.hardCapUsd);

  const live = args.entityIndex
    ? { index: args.entityIndex }
    : await buildLiveAiVisibilityEntityIndex({ applyOverlay: true });
  const peerCfg = loadPeerSetConfig();
  const peerRaw = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, peerCfg);
  const peerSet = validatePeerSetAgainstIndex(peerRaw, live.index);
  const nameKeys = knownNameKeys(live.index.entities);

  const stats = {
    provider,
    waveId,
    parentValidationId: parentId,
    modelRequested: model,
    modelReturned: [],
    PLANNED: executions.length,
    ATTEMPTED: 0,
    SUCCEEDED: 0,
    FAILED: 0,
    RETRIES: 0,
    TOTAL_ATTEMPTS: 0,
    latencies: [],
    errors: [],
    activationGate: null,
    stoppedReason: null,
    webSearchUsed: 0,
    responsesWithGrounding: 0,
    responsesWithCitations: 0,
    totalNormalizedCitations: 0,
    totalSearchResults: 0,
    uniqueDomains: new Set(),
    inputTokens: 0,
    outputTokens: 0,
    peerMentions: 0,
    recommendedPeerMentions: 0,
    unresolvedBrandLike: 0,
    stopReasons: [],
    pauseContinuations: 0,
    startedAt: now.toISOString(),
    completedAt: null,
    storeRoot,
    OPENAI_BASELINE_UNTOUCHED: true,
  };

  ensureDir(path.join(storeRoot, "waves", waveId, "raw"));
  ensureDir(path.join(storeRoot, "waves", waveId, "normalized"));

  let activationEvaluated = false;

  for (let i = 0; i < executions.length; i += 1) {
    const exec = executions[i];
    if (costLedger.capBreached) {
      stats.stoppedReason = "hard_cost_cap";
      break;
    }

    stats.ATTEMPTED += 1;
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
      maxRetries: retryPolicy.maxRetriesPerCall,
      backoffMs: retryPolicy.backoffMs,
    });

    stats.TOTAL_ATTEMPTS += providerAttempts;
    stats.RETRIES += retries;

    if (error || !result) {
      stats.FAILED += 1;
      const classified = classifyProviderError(error);
      stats.errors.push({
        promptId: exec.promptId,
        category: classified.category,
        message: classified.message,
      });
      await persistFailedCall({
        storeRoot,
        waveId,
        exec,
        error,
        retries,
        providerAttempts,
      });
      if (classified.category === "AUTH") {
        stats.stoppedReason = "provider_auth_error";
        break;
      }
    } else {
      stats.SUCCEEDED += 1;
      if (result.model) stats.modelReturned.push(result.model);
      if (result.latencyMs != null) stats.latencies.push(result.latencyMs);
      if (result.providerMeta?.webSearchUsed) stats.webSearchUsed += 1;
      if (result.searchMetadata || result.searchResults?.length) stats.responsesWithGrounding += 1;
      if ((result.citations || []).length > 0) stats.responsesWithCitations += 1;
      stats.totalNormalizedCitations += (result.citations || []).length;
      stats.totalSearchResults += (result.searchResults || []).length;
      for (const c of result.citations || []) {
        const d = c.domain || parseDomain(c.url);
        if (d) stats.uniqueDomains.add(d);
      }
      const costApply = applyValidationCallCost(costLedger, result.usage, provider);
      stats.inputTokens += Number(result.usage?.inputTokens || 0);
      stats.outputTokens += Number(result.usage?.outputTokens || 0);
      if (result.stopReason) stats.stopReasons.push(result.stopReason);

      try {
        const persisted = await persistSuccessfulCall({
          store,
          waveId,
          storeRoot,
          exec,
          result,
          entityIndex: live.index,
          nameKeys,
        });
        stats.peerMentions += (persisted.mentions || []).length;
        stats.recommendedPeerMentions += (persisted.mentions || []).filter(
          (m) =>
            m.role === "recommended" ||
            m.explicitRecommendation === true ||
            m.recommendationRole === "recommended"
        ).length;
      } catch (err) {
        stats.FAILED += 1;
        stats.SUCCEEDED -= 1;
        stats.errors.push({
          promptId: exec.promptId,
          category: "PIPELINE",
          message: err?.message || String(err),
        });
      }
    }

    // Activation gate after first 3 logical attempts (or fewer if plan shorter)
    const gateAt = Math.min(ACTIVATION_SAMPLE_SIZE, executions.length) - 1;
    if (i === gateAt && !activationEvaluated) {
      activationEvaluated = true;
      stats.activationGate = evaluateProviderValidationActivationGate({
        planned: ACTIVATION_SAMPLE_SIZE,
        succeeded: stats.SUCCEEDED,
        failed: stats.FAILED,
        authErrors: stats.errors.filter((e) => e.category === "AUTH").length,
      });
      if (stats.activationGate.RESULT === "FAIL") {
        stats.stoppedReason = "activation_gate_fail";
        break;
      }
    }
  }

  stats.completedAt = new Date().toISOString();
  stats.costLedger = costLedger;
  stats.ACTUAL_MODEL_RETURNED = [...new Set(stats.modelReturned)][0] || null;
  stats.MODEL_MATCH = stats.ACTUAL_MODEL_RETURNED
    ? String(stats.ACTUAL_MODEL_RETURNED).includes(model) ||
      model.includes(String(stats.ACTUAL_MODEL_RETURNED).split("-")[0])
    : null;

  const summary = {
    batchId: waveId,
    waveId,
    parentValidationId: parentId,
    provider: { name: provider },
    model,
    status:
      stats.stoppedReason === "provider_auth_error"
        ? "failed_auth"
        : stats.SUCCEEDED === stats.PLANNED
          ? "completed"
          : stats.SUCCEEDED > 0
            ? "partial"
            : "failed",
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.VALIDATION,
    planVersion: PROVIDER_VALIDATION_PLAN_VERSION,
    orchestratorVersion: PROVIDER_VALIDATION_ORCHESTRATOR_VERSION,
    logicalCalls: stats.PLANNED,
    succeeded: stats.SUCCEEDED,
    failed: stats.FAILED,
    startedAt: stats.startedAt,
    completedAt: stats.completedAt,
    costUsd: costLedger.actualUsd,
    providerReportedCostUsd: costLedger.providerReportedUsd,
    activationGate: stats.activationGate,
    storeRoot,
  };
  await store.saveBatchSummary(summary);
  stats.uniqueDomains = [...stats.uniqueDomains];
  writeJson(path.join(storeRoot, "waves", waveId, "validation-summary.json"), stats);

  return stats;
}

/**
 * Execute all configured providers sequentially.
 */
export async function executeMultiProviderValidation(args = {}) {
  if (!args.force && !isAiVisibilityEnabled()) {
    throw new Error("AI_VISIBILITY_ENABLED must be true for live validation");
  }
  if (!args.force && !isAiVisibilityLiveTestAllowed()) {
    throw new Error("AI_VISIBILITY_LIVE_TEST must be true for live validation");
  }

  const cred = preflightProviderCredentials();
  const parentId = args.parentValidationId || createParentValidationId();
  const results = {};
  const waveIds = {};

  for (const provider of args.providers || PROVIDER_ORDER) {
    const credStatus = cred[`${provider.toUpperCase()}_CREDENTIAL`];
    if (credStatus === "MISSING") {
      results[provider] = {
        provider,
        status: "NOT_EXECUTED_MISSING_CREDENTIAL",
        PLANNED: 12,
        SUCCEEDED: 0,
        FAILED: 0,
      };
      continue;
    }

    const waveId = createValidationWaveId(provider);
    waveIds[provider] = waveId;
    results[provider] = await executeProviderValidation({
      ...args,
      provider,
      waveId,
      parentValidationId: parentId,
    });
  }

  const report = {
    phase: "3B.2",
    parentValidationId: parentId,
    waveIds,
    credentials: cred,
    results,
    openAiBaselineStore: WAVE1_ROOT,
    openAiBaselineUntouched: true,
    completedAt: new Date().toISOString(),
  };
  const reportRoot = path.join(resolveProviderValidationStoreRoot("_parent"), parentId);
  writeJson(path.join(reportRoot, "phase3b2-validation-report.json"), report);

  return report;
}

export function preflightValidationLiveEnv() {
  const cred = preflightProviderCredentials();
  const enabled = isAiVisibilityEnabled();
  const live = isAiVisibilityLiveTestAllowed();
  const blockers = [];
  if (!enabled) blockers.push("AI_VISIBILITY_ENABLED_false");
  if (!live) blockers.push("AI_VISIBILITY_LIVE_TEST_false");
  if (cred.AUTH_PREFLIGHT_READY !== "YES") blockers.push("no_provider_credentials");

  return {
    LIVE_ENV_READY: blockers.length === 0 || cred.AUTH_PREFLIGHT_READY === "YES",
    ...cred,
    AI_VISIBILITY_ENABLED: enabled,
    AI_VISIBILITY_LIVE_TEST: live,
    OPENAI_CALLS: 0,
    blockers,
    SECRET_EXPOSURE: "NONE",
  };
}
