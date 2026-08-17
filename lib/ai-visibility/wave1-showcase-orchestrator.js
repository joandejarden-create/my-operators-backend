/**
 * Phase 3A.11 — Wave-1 multi-slot live orchestrator (OpenAI showcase).
 * One parent Wave-1 identity · 7 ordered slots · activation gate · hard cap · resume.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
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
import {
  buildWave1ShowcaseDryRunPlan,
  WAVE1_BASELINE_SERIES_ID,
  WAVE1_PEER_SET_ID,
  WAVE1_PROVIDER,
  WAVE1_RETRY_POLICY,
  WAVE1_EXECUTION_ORDER,
  WAVE1_COST_EVIDENCE,
  WAVE1_SHOWCASE_PLAN_VERSION,
} from "./wave1-showcase-plan.js";
import {
  WAVE1_HARD_CAP_USD,
  createWave1CostLedger,
  applyWave1CallCost,
  wouldBreachHardCap,
  projectWaveCostFromSample,
  estimateWave1CallCostUsd,
} from "./wave1-cost.js";
import { evaluateGlobalEnActivationGate } from "./wave1-activation-gate.js";
import {
  isRetryableProviderError,
  isAuthProviderError,
  hashPromptText,
  BATCH_HEALTH,
} from "./execution-batch.js";
import { createAiVisibilityStore } from "./storage/index.js";
import { WAVE1_ROOT, PHASE2E_ROOT } from "./storage/resolve-store-root.js";
import { buildLiveAiVisibilityEntityIndex, buildAiVisibilityEntityIndex } from "./entity-index.js";
import { loadPeerSetConfig, resolvePeerSetMembership } from "./peer-sets.js";
import { validatePeerSetAgainstIndex } from "./execute-cohort.js";
import { runVisibilityPrompt } from "./providers/index.js";
import { normalizeVisibilityProviderResponse } from "./providers/normalized-response.js";
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
import { assembleEvidenceRecord } from "./evidence.js";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
void __dirname;

export const WAVE1_ORCHESTRATOR_VERSION = "ai_visibility_wave1_orchestrator_v1";
export const WAVE1_PROMPT_LIBRARY_VERSION = "showcase_prompts_v1";

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

function createWave1Id(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const min = String(now.getUTCMinutes()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_wave1_openai_showcase_${y}${m}${d}_${h}${min}_${rand}`;
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

/**
 * Live environment preflight — never prints secrets.
 */
export function preflightWave1LiveEnv(options = {}) {
  const model =
    String(options.model || process.env.AI_VISIBILITY_MODEL || "").trim() || "gpt-5.6";
  const hardCap = Number(
    options.hardCapUsd ?? process.env.AI_VISIBILITY_MAX_BATCH_COST_USD ?? WAVE1_HARD_CAP_USD
  );
  const storeRoot = String(options.storeRoot || process.env.AI_VISIBILITY_STORE_ROOT || WAVE1_ROOT);
  const keyPresent = Boolean(String(process.env.OPENAI_API_KEY || "").trim());
  const enabled = isAiVisibilityEnabled();
  const live = isAiVisibilityLiveTestAllowed();
  const blockers = [];
  if (!enabled) blockers.push("AI_VISIBILITY_ENABLED_false");
  if (!live) blockers.push("AI_VISIBILITY_LIVE_TEST_false");
  if (!keyPresent) blockers.push("OPENAI_API_KEY_missing");
  if (model !== "gpt-5.6" && !options.allowModelOverride) {
    blockers.push(`model_${model}_expected_gpt-5.6`);
  }
  if (!(hardCap >= WAVE1_HARD_CAP_USD)) {
    blockers.push(`hard_cap_${hardCap}_below_founder_approved_${WAVE1_HARD_CAP_USD}`);
  }
  if (!/wave1-showcase/i.test(String(storeRoot))) {
    blockers.push("store_root_not_wave1_namespace");
  }
  const checkpointRoot = path.join(storeRoot, "checkpoints");
  return {
    LIVE_ENV_READY: blockers.length === 0,
    OPENAI_KEY_PRESENT: keyPresent ? "YES" : "NO",
    MODEL: model,
    STORE_ROOT: storeRoot,
    HARD_COST_CAP: hardCap,
    PLANNED_LOGICAL_CALLS: WAVE1_RETRY_POLICY.plannedCalls,
    MAX_ATTEMPTS: WAVE1_RETRY_POLICY.maxTotalAttempts,
    CHECKPOINT_ROOT: checkpointRoot,
    PHASE2E_ROOT,
    LEGACY_ISOLATED: path.resolve(storeRoot) !== path.resolve(PHASE2E_ROOT),
    blockers,
  };
}

function emptySlotState(key) {
  return {
    key,
    planned: 12,
    succeeded: 0,
    failed: 0,
    retried: 0,
    attempts: 0,
    cost: 0,
    status: "pending",
    timeouts: 0,
    providerErrors: 0,
    authErrors: 0,
  };
}

function createInitialCheckpoint({ wave1Id, plan, model, hardCapUsd, storeRoot }) {
  const slots = {};
  for (const s of WAVE1_EXECUTION_ORDER) {
    slots[s.key] = emptySlotState(s.key);
  }
  return {
    version: WAVE1_ORCHESTRATOR_VERSION,
    wave1Id,
    batchId: wave1Id,
    baselineSeriesId: WAVE1_BASELINE_SERIES_ID,
    peerSetId: WAVE1_PEER_SET_ID,
    peerSetVersion: "2",
    provider: WAVE1_PROVIDER,
    model,
    metricVersion: METRIC_VERSION,
    promptLibraryVersion: WAVE1_PROMPT_LIBRARY_VERSION,
    planVersion: WAVE1_SHOWCASE_PLAN_VERSION,
    hardCapUsd,
    storeRoot,
    status: "running",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    activationGate: null,
    costLedger: createWave1CostLedger(hardCapUsd),
    completedFingerprints: {},
    failedFingerprints: {},
    slots,
    logical: {
      planned: 84,
      succeeded: 0,
      failedFinal: 0,
      notExecuted: 84,
      retried: 0,
      totalAttempts: 0,
      retrySuccess: 0,
      retryFailure: 0,
    },
    observations: [],
    audit: {
      parseFailures: 0,
      storageFailures: 0,
      identityMissing: 0,
      resolverMalfunctions: 0,
      classifierMalfunctions: 0,
      citationCorruptions: 0,
      governanceViolations: [],
      AIRTABLE_WRITES: 0,
      ENTITLEMENT_WRITES: 0,
    },
    planFingerprintUnique: plan.FINGERPRINTS.UNIQUE,
    peerFingerprint: plan.PEER.FINGERPRINT,
  };
}

function checkpointPath(storeRoot, wave1Id) {
  return path.join(storeRoot, "checkpoints", `${wave1Id}.json`);
}

function persistCheckpoint(cp) {
  cp.updatedAt = new Date().toISOString();
  writeJson(checkpointPath(cp.storeRoot, cp.wave1Id), cp);
  // Mirror under wave parent for inspectability
  writeJson(path.join(cp.storeRoot, "waves", cp.wave1Id, "checkpoint.json"), cp);
  return cp;
}

function requiredIdentityFields(run, normalized) {
  const missing = [];
  const checks = {
    provider: run.provider || normalized?.provider,
    providerModel: run.model || normalized?.providerModel,
    promptId: run.promptId,
    promptVersion: run.promptVersion,
    promptFamily: run.promptFamily,
    language: run.language,
    geography: run.geographyKey,
    intent: run.intent,
    peerSetVersion: run.peerSetVersion,
    metricVersion: run.metricVersion,
    status: run.status,
  };
  for (const [k, v] of Object.entries(checks)) {
    if (v == null || v === "") missing.push(k);
  }
  if (run.status === "completed") {
    if (!normalized?.rawText && !run.rawText) missing.push("rawText");
    if (!run.responseId) missing.push("responseId");
  }
  return missing;
}

/**
 * Process one successful provider result into store artifacts + observation.
 */
async function persistSuccessfulLogicalCall({
  store,
  cp,
  exec,
  result,
  retries,
  providerAttempts,
  entityIndex,
  nameKeys,
  peerSet,
}) {
  const runId = store.generateId("run");
  const responseId = store.generateId("resp");
  const callCost = estimateWave1CallCostUsd(result.usage);
  applyWave1CallCost(cp.costLedger, result.usage, providerAttempts);

  const rawArtifactUri = path.join(
    cp.storeRoot,
    "waves",
    cp.wave1Id,
    "raw",
    `${exec.fingerprint}.json`
  );
  writeJson(rawArtifactUri, {
    fingerprint: exec.fingerprint,
    wave1Id: cp.wave1Id,
    slot: exec.slot,
    provider: result.provider,
    model: result.model,
    raw: result.raw,
    savedAt: new Date().toISOString(),
  });

  const normalized = normalizeVisibilityProviderResponse(result, {
    promptId: exec.promptId,
    promptVersion: exec.version,
    geography: exec.geographyKey,
    geographyKey: exec.geographyKey,
    language: exec.language,
    intent: exec.intent,
    peerSetVersion: "2",
    rawArtifactUri,
  });
  writeJson(
    path.join(cp.storeRoot, "waves", cp.wave1Id, "normalized", `${exec.fingerprint}.json`),
    normalized
  );

  const response = {
    responseId,
    runId,
    batchId: cp.wave1Id,
    wave1Id: cp.wave1Id,
    slot: exec.slot,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    promptVersion: exec.version,
    promptFamily: exec.promptFamily,
    semanticPairId: exec.semanticPairId,
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
    normalized,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    peerSetId: WAVE1_PEER_SET_ID,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
    createdAt: new Date().toISOString(),
  };
  await store.saveResponse(response);

  let mentions = [];
  let citations = [];
  let observation = null;
  let resolverMalfunction = false;
  let classifierMalfunction = false;
  let citationCorruption = false;

  try {
    mentions = extractMentions({
      responseId,
      text: response.text,
      entityIndex: entityIndex.aliasIndex,
      promptIntentTerritory: exec.intent,
    });
  } catch (err) {
    resolverMalfunction = true;
    throw Object.assign(err, { pipeline: "resolver" });
  }

  try {
    citations = extractCitations({
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
    for (const c of citations) {
      if (c.url && typeof c.url === "string") {
        try {
          // eslint-disable-next-line no-new
          new URL(c.url);
        } catch {
          citationCorruption = true;
        }
      }
    }
  } catch (err) {
    citationCorruption = true;
    throw Object.assign(err, { pipeline: "citations" });
  }

  await store.saveMentions(responseId, mentions);
  await store.saveCitations(responseId, citations);
  harvestUnresolvedWithFilterStats(response.text, nameKeys);

  try {
    observation = buildObservationFromExtractions({
      observationId: `obs_${responseId}`,
      promptId: exec.promptId,
      provider: WAVE1_PROVIDER,
      periodKey: cp.wave1Id,
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
      language: exec.language,
    });
  } catch (err) {
    classifierMalfunction = true;
    throw Object.assign(err, { pipeline: "classifier" });
  }

  const runRec = {
    runId,
    batchId: cp.wave1Id,
    wave1Id: cp.wave1Id,
    slot: exec.slot,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    promptVersion: exec.version,
    promptFamily: exec.promptFamily,
    semanticPairId: exec.semanticPairId,
    provider: WAVE1_PROVIDER,
    model: cp.model,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    peerSetId: WAVE1_PEER_SET_ID,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
    status: "completed",
    startedAt: new Date().toISOString(),
    completedAt: new Date().toISOString(),
    retries,
    providerAttempts,
    responseId,
    usage: result.usage,
    estimatedCost: callCost,
    latencyMs: result.latencyMs,
    rawText: result.text,
    promptTextHash: hashPromptText(exec.promptText),
  };

  const missing = requiredIdentityFields(runRec, normalized);
  if (missing.length) {
    cp.audit.identityMissing += 1;
  }

  const evidence = assembleEvidenceRecord({
    prompt: {
      promptId: exec.promptId,
      version: exec.version,
      text: exec.promptText,
      intentTerritory: exec.intent,
      geographyScope: observation.geography.geographyScope,
      region: observation.geography.regionName,
      country: observation.geography.countryName,
      language: exec.language,
      semanticPairId: exec.semanticPairId,
      promptFamily: exec.promptFamily,
    },
    run: runRec,
    response,
    mentions,
    citations,
    metrics: { observation },
    geography: observation.geography,
    language: exec.language,
  });
  await store.saveEvidence(evidence);
  runRec.evidenceId = evidence.evidenceId;
  await store.saveRun(runRec);

  cp.completedFingerprints[exec.fingerprint] = {
    status: "completed",
    runId,
    responseId,
    evidenceId: evidence.evidenceId,
    slot: exec.slot,
    cost: callCost,
    retries,
    providerAttempts,
    completedAt: runRec.completedAt,
  };
  delete cp.failedFingerprints[exec.fingerprint];

  return {
    runRec,
    response,
    mentions,
    citations,
    observation,
    callCost,
    missing,
    resolverMalfunction,
    classifierMalfunction,
    citationCorruption,
  };
}

async function persistFailedLogicalCall({
  store,
  cp,
  exec,
  error,
  retries,
  providerAttempts,
}) {
  const runId = store.generateId("run");
  const failRec = {
    runId,
    batchId: cp.wave1Id,
    wave1Id: cp.wave1Id,
    slot: exec.slot,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    promptVersion: exec.version,
    promptFamily: exec.promptFamily,
    semanticPairId: exec.semanticPairId,
    provider: WAVE1_PROVIDER,
    model: cp.model,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    peerSetId: WAVE1_PEER_SET_ID,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
    status: "failed",
    startedAt: new Date().toISOString(),
    completedAt: new Date().toISOString(),
    retries,
    providerAttempts,
    error: {
      type: error?.type || "provider_error",
      status: error?.status ?? null,
      message: String(error?.message || "unknown").slice(0, 500),
      retryable: isRetryableProviderError(error),
    },
  };
  await store.saveRun(failRec);
  // Failed attempts still may have incurred cost on retries that returned usage — usually not.
  // Count attempt budget only.
  cp.costLedger.providerAttempts += providerAttempts;
  cp.failedFingerprints[exec.fingerprint] = {
    status: "failed",
    runId,
    slot: exec.slot,
    retries,
    providerAttempts,
    error: failRec.error,
    completedAt: failRec.completedAt,
  };
  return failRec;
}

function recomputeLogicalTotals(cp) {
  const completed = Object.keys(cp.completedFingerprints).length;
  const failed = Object.keys(cp.failedFingerprints).length;
  cp.logical.succeeded = completed;
  cp.logical.failedFinal = failed;
  cp.logical.notExecuted = Math.max(0, 84 - completed - failed);
  cp.logical.totalAttempts = cp.costLedger.providerAttempts;
}

/**
 * Generate peer metric snapshots from successful observations only.
 */
async function generateWave1Metrics({ store, cp, observations, peerSet, entityIndex }) {
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
      citationReadiness: "PARTIAL",
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
        batchId: cp.wave1Id,
        wave1Id: cp.wave1Id,
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
        provider: WAVE1_PROVIDER,
        model: cp.model,
        metricVersion: METRIC_VERSION,
        peerSetId: WAVE1_PEER_SET_ID,
        citationReadiness: metric === "citation_rate" ? "PARTIAL" : undefined,
        COMPARABLE_PRIOR_PERIOD: "NONE",
        TREND_AVAILABLE: false,
      });
    }
  }

  let competitivePosition = null;
  if (entityIds.length) {
    competitivePosition = computeCompetitivePosition(observations, entityIds);
    competitivePosition = {
      ...competitivePosition,
      peers: (competitivePosition.peers || []).map((p) => ({
        ...p,
        name: idToName.get(p.entityId) || null,
      })),
      PEER_DENOMINATOR_COUNT: entityIds.length,
      PORTFOLIO_ONLY_EXCLUDED_FROM_PEER_RANK: true,
    };
  }

  return { byEntity, competitivePosition, citationRateReadiness: "PARTIAL" };
}

/**
 * Execute Wave-1 live showcase (or resume).
 *
 * @param {{
 *   execute?: boolean,
 *   resume?: boolean,
 *   wave1Id?: string,
 *   storeRoot?: string,
 *   hardCapUsd?: number,
 *   model?: string,
 *   runVisibilityPrompt?: Function,
 *   entityIndex?: object,
 *   fetchImpl?: typeof fetch,
 *   stopAfterFirstSlot?: boolean,
 *   skipActivationContinue?: boolean,
 * }} args
 */
export async function executeWave1Showcase(args = {}) {
  if (!args.execute) {
    throw Object.assign(new Error("Refusing Wave-1 live without execute:true"), {
      code: "gate",
    });
  }

  const preflight = preflightWave1LiveEnv({
    model: args.model,
    hardCapUsd: args.hardCapUsd,
    storeRoot: args.storeRoot,
    allowModelOverride: args.allowModelOverride,
  });
  if (!preflight.LIVE_ENV_READY && !args.allowUnsafePreflight) {
    return {
      mode: "blocked",
      BUILD_STATUS: "BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_BLOCKED",
      reason: "preflight_failed",
      preflight,
      LIVE_PROVIDER_LOGICAL_CALLS: 0,
      LIVE_PROVIDER_ATTEMPTS: 0,
    };
  }

  const storeRoot = preflight.STORE_ROOT;
  const store = createAiVisibilityStore({ rootDir: storeRoot });
  const plan = buildWave1ShowcaseDryRunPlan();
  if (!plan.ok) {
    throw Object.assign(new Error(`Wave-1 plan invalid: ${plan.errors.join("; ")}`), {
      code: "plan",
    });
  }

  let cp;
  if (args.resume && args.wave1Id) {
    cp = readJson(checkpointPath(storeRoot, args.wave1Id));
    if (!cp) {
      throw Object.assign(new Error(`Checkpoint not found for ${args.wave1Id}`), {
        code: "resume",
      });
    }
  } else if (args.resume) {
    // Resume latest running/partial checkpoint
    const dir = path.join(storeRoot, "checkpoints");
    if (fs.existsSync(dir)) {
      const files = fs
        .readdirSync(dir)
        .filter((f) => f.endsWith(".json"))
        .map((f) => readJson(path.join(dir, f)))
        .filter((c) => c && ["running", "partial", "activation_gate_passed"].includes(c.status))
        .sort((a, b) => String(b.updatedAt || "").localeCompare(String(a.updatedAt || "")));
      cp = files[0] || null;
    }
    if (!cp) {
      throw Object.assign(new Error("No resumable Wave-1 checkpoint found"), { code: "resume" });
    }
  } else {
    const wave1Id = args.wave1Id || createWave1Id();
    cp = createInitialCheckpoint({
      wave1Id,
      plan,
      model: preflight.MODEL,
      hardCapUsd: preflight.HARD_COST_CAP,
      storeRoot,
    });
    await store.saveBatch({
      batchId: wave1Id,
      wave1Id,
      kind: "wave1_showcase",
      baselineSeriesId: WAVE1_BASELINE_SERIES_ID,
      peerSetId: WAVE1_PEER_SET_ID,
      provider: WAVE1_PROVIDER,
      model: preflight.MODEL,
      status: "running",
      plannedRuns: 84,
      startedAt: cp.createdAt,
      metricVersion: METRIC_VERSION,
      promptLibraryVersion: WAVE1_PROMPT_LIBRARY_VERSION,
    });
    persistCheckpoint(cp);
  }

  // Entity index
  let entityIndex;
  if (args.entityIndex) {
    entityIndex = args.entityIndex;
  } else {
    const live = await buildLiveAiVisibilityEntityIndex({ applyOverlay: true });
    entityIndex = live.index;
  }
  const peerCfg = loadPeerSetConfig();
  const peerRaw = resolvePeerSetMembership({ peerSetId: WAVE1_PEER_SET_ID }, peerCfg);
  const peerSet = validatePeerSetAgainstIndex(peerRaw, entityIndex);
  if (!peerSet.canonicalValid) {
    throw Object.assign(new Error(`Peer set invalid against index: ${peerSet.error}`), {
      code: "peer",
      missing: peerSet.missingEntityIds,
    });
  }
  const nameKeys = knownNameKeys(entityIndex.entities);
  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  const executionsBySlot = new Map();
  for (const slot of WAVE1_EXECUTION_ORDER) {
    executionsBySlot.set(
      slot.key,
      plan.EXECUTIONS.filter((e) => e.slot === slot.key)
    );
  }

  // Rebuild full observations from store on resume (checkpoint keeps thin copies only)
  let observations = [];
  if (args.resume || Object.keys(cp.completedFingerprints || {}).length) {
    const existingRuns = (await store.listBatchRuns(cp.wave1Id)) || [];
    for (const run of existingRuns.filter((r) => r.status === "completed")) {
      const mentions = (await store.getMentions(run.responseId)) || [];
      const citations = (await store.getCitations(run.responseId)) || [];
      observations.push(
        buildObservationFromExtractions({
          observationId: `obs_${run.responseId}`,
          promptId: run.promptId,
          provider: WAVE1_PROVIDER,
          periodKey: cp.wave1Id,
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
  let stopReason = null;

  async function executeSlot(slotKey, { evaluateGate = false } = {}) {
    const slotExecs = executionsBySlot.get(slotKey) || [];
    const slot = cp.slots[slotKey];
    slot.status = "running";
    persistCheckpoint(cp);

    let slotParseFailures = 0;
    let slotStorageFailures = 0;
    let slotIdentityMissing = 0;
    let slotResolverMalfunctions = 0;
    let slotClassifierMalfunctions = 0;
    let slotCitationCorruptions = 0;

    for (const exec of slotExecs) {
      if (cp.completedFingerprints[exec.fingerprint]) {
        continue; // resume-safe skip
      }
      // If previously failed: on resume allow one targeted re-attempt for timeout/network
      // before activation gate closes (Part 34 technical retry — not answer reroll).
      const priorFail = cp.failedFingerprints[exec.fingerprint];
      if (priorFail && priorFail.exhausted) {
        const errType = priorFail.error?.type;
        const allowTargeted =
          args.resume &&
          (!cp.activationGate || cp.activationGate.RESULT !== "PASS") &&
          (errType === "timeout" || errType === "network_error" || errType === "rate_limit");
        if (!allowTargeted) continue;
        delete cp.failedFingerprints[exec.fingerprint];
        if (slot.failed > 0) slot.failed -= 1;
      }

      if (wouldBreachHardCap(cp.costLedger)) {
        cp.costLedger.capBreached = true;
        cp.costLedger.stoppedReason = "hard_cost_cap";
        cp.status = "partial_cost_cap";
        stopReason = "hard_cost_cap";
        slot.status = "stopped_cost_cap";
        persistCheckpoint(cp);
        break;
      }

      const { result, retries, error, providerAttempts } = await runWithRetry({
        runFn: () =>
          runFn({
            prompt: { text: exec.promptText, promptId: exec.promptId },
            model: cp.model,
            apiKey: process.env.OPENAI_API_KEY,
            enableWebSearch: true,
            timeoutMs: Number(
              process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || WAVE1_RETRY_POLICY.timeoutMsDefault
            ),
            fetchImpl: args.fetchImpl,
            provider: WAVE1_PROVIDER,
          }),
        maxRetries: WAVE1_RETRY_POLICY.maxRetriesPerCall,
        backoffMs: WAVE1_RETRY_POLICY.backoffMs,
      });

      slot.attempts += providerAttempts;
      cp.logical.totalAttempts += providerAttempts;
      if (retries > 0) {
        slot.retried += 1;
        cp.logical.retried += 1;
        if (result) cp.logical.retrySuccess += 1;
        else cp.logical.retryFailure += 1;
      }

      if (error || !result) {
        if (error?.type === "timeout") slot.timeouts += 1;
        if (isAuthProviderError(error)) {
          slot.authErrors += 1;
          await persistFailedLogicalCall({
            store,
            cp,
            exec,
            error,
            retries,
            providerAttempts,
          });
          slot.failed += 1;
          cp.failedFingerprints[exec.fingerprint].exhausted = true;
          recomputeLogicalTotals(cp);
          persistCheckpoint(cp);
          stopReason = "provider_auth_error";
          slot.status = "failed_auth";
          break;
        }
        if (error) slot.providerErrors += 1;
        await persistFailedLogicalCall({
          store,
          cp,
          exec,
          error,
          retries,
          providerAttempts,
        });
        slot.failed += 1;
        cp.failedFingerprints[exec.fingerprint].exhausted = true;
        recomputeLogicalTotals(cp);
        persistCheckpoint(cp);
        continue;
      }

      try {
        const persisted = await persistSuccessfulLogicalCall({
          store,
          cp,
          exec,
          result,
          retries,
          providerAttempts,
          entityIndex,
          nameKeys,
          peerSet,
        });
        if (persisted.missing.length) slotIdentityMissing += 1;
        if (persisted.resolverMalfunction) slotResolverMalfunctions += 1;
        if (persisted.classifierMalfunction) slotClassifierMalfunctions += 1;
        if (persisted.citationCorruption) slotCitationCorruptions += 1;
        observations.push(persisted.observation);
        slot.succeeded += 1;
        slot.cost = Number((slot.cost + persisted.callCost).toFixed(6));
      } catch (err) {
        if (err.pipeline === "resolver") slotResolverMalfunctions += 1;
        else if (err.pipeline === "classifier") slotClassifierMalfunctions += 1;
        else if (err.pipeline === "citations") slotCitationCorruptions += 1;
        else slotParseFailures += 1;
        slotStorageFailures += 1;
        cp.audit.parseFailures += 1;
        await persistFailedLogicalCall({
          store,
          cp,
          exec,
          error: err,
          retries,
          providerAttempts,
        });
        slot.failed += 1;
        cp.failedFingerprints[exec.fingerprint].exhausted = true;
      }

      recomputeLogicalTotals(cp);
      persistCheckpoint(cp);

      if (cp.costLedger.capBreached) {
        stopReason = "hard_cost_cap";
        slot.status = "stopped_cost_cap";
        cp.status = "partial_cost_cap";
        break;
      }
    }

    if (!stopReason || stopReason === "activation_gate_fail") {
      slot.status =
        slot.failed === 0 && slot.succeeded === slot.planned
          ? "completed"
          : slot.succeeded > 0
            ? "partial"
            : "failed";
    }

    cp.audit.identityMissing += slotIdentityMissing;
    cp.audit.resolverMalfunctions += slotResolverMalfunctions;
    cp.audit.classifierMalfunctions += slotClassifierMalfunctions;
    cp.audit.citationCorruptions += slotCitationCorruptions;
    cp.audit.parseFailures += slotParseFailures;
    cp.audit.storageFailures += slotStorageFailures;
    cp.observations = observations.map((o) => ({
      observationId: o.observationId,
      promptId: o.promptId,
      presentEntityIds: o.presentEntityIds,
      recommendedEntityIds: o.recommendedEntityIds,
    }));
    persistCheckpoint(cp);

    if (evaluateGate) {
      const gate = evaluateGlobalEnActivationGate({
        planned: slot.planned,
        succeeded: slot.succeeded,
        failed: slot.failed,
        retries: slot.retried,
        timeouts: slot.timeouts,
        providerErrors: slot.providerErrors,
        authErrors: slot.authErrors,
        parseFailures: slotParseFailures,
        storageFailures: slotStorageFailures,
        identityMissing: slotIdentityMissing,
        resolverMalfunctions: slotResolverMalfunctions,
        classifierMalfunctions: slotClassifierMalfunctions,
        citationCorruptions: slotCitationCorruptions,
        slotCostUsd: slot.cost,
        governanceViolations: cp.audit.governanceViolations,
      });
      cp.activationGate = gate;
      persistCheckpoint(cp);
      return gate;
    }
    return null;
  }

  // Slot 1: GLOBAL_EN + gate
  if (!cp.activationGate || cp.activationGate.RESULT !== "PASS") {
    if (cp.slots.GLOBAL_EN.status === "pending" || cp.slots.GLOBAL_EN.status === "running") {
      const gate = await executeSlot("GLOBAL_EN", { evaluateGate: true });
      if (stopReason === "hard_cost_cap" || stopReason === "provider_auth_error") {
        persistCheckpoint(cp);
        return finalizeWave1({
          store,
          cp,
          plan,
          peerSet,
          entityIndex,
          observations,
          stopReason,
          preflight,
        });
      }
      if (gate?.RESULT === "FAIL") {
        cp.status = "activation_gate_failed";
        stopReason = "activation_gate_fail";
        persistCheckpoint(cp);
        return finalizeWave1({
          store,
          cp,
          plan,
          peerSet,
          entityIndex,
          observations,
          stopReason,
          preflight,
        });
      }
      cp.status = "activation_gate_passed";
      persistCheckpoint(cp);
    }
  }

  if (args.stopAfterFirstSlot) {
    return finalizeWave1({
      store,
      cp,
      plan,
      peerSet,
      entityIndex,
      observations,
      stopReason: stopReason || "stop_after_first_slot",
      preflight,
    });
  }

  // Remaining slots
  const remaining = WAVE1_EXECUTION_ORDER.map((s) => s.key).filter((k) => k !== "GLOBAL_EN");
  for (const slotKey of remaining) {
    if (stopReason) break;
    const st = cp.slots[slotKey];
    if (st.status === "completed") continue;
    await executeSlot(slotKey, { evaluateGate: false });
  }

  if (!stopReason) {
    cp.status =
      cp.logical.failedFinal === 0 && cp.logical.succeeded === 84
        ? "completed"
        : cp.logical.succeeded > 0
          ? "partial"
          : "failed";
  }
  persistCheckpoint(cp);

  return finalizeWave1({
    store,
    cp,
    plan,
    peerSet,
    entityIndex,
    observations,
    stopReason,
    preflight,
  });
}

async function finalizeWave1({
  store,
  cp,
  plan,
  peerSet,
  entityIndex,
  observations,
  stopReason,
  preflight,
}) {
  recomputeLogicalTotals(cp);
  const metrics =
    observations.length > 0
      ? await generateWave1Metrics({ store, cp, observations, peerSet, entityIndex })
      : { byEntity: {}, competitivePosition: null, citationRateReadiness: "PARTIAL" };

  const summary = {
    label: "WAVE-1 OPENAI SHOWCASE BASELINE — FIRST COMPARABLE PERIOD",
    batchId: cp.wave1Id,
    wave1Id: cp.wave1Id,
    baselineSeriesId: WAVE1_BASELINE_SERIES_ID,
    status: cp.status,
    stopReason,
    provider: { name: WAVE1_PROVIDER, model: cp.model },
    peerSet: {
      peerSetId: WAVE1_PEER_SET_ID,
      peerSetVersion: "2",
      count: (peerSet.entityIds || []).length,
      canonicalValid: peerSet.canonicalValid,
    },
    activationGate: cp.activationGate,
    logical: cp.logical,
    slots: cp.slots,
    cost: {
      actualUsd: cp.costLedger.actualUsd,
      hardCapUsd: cp.costLedger.hardCapUsd,
      capBreached: cp.costLedger.capBreached,
      inputTokens: cp.costLedger.inputTokens,
      outputTokens: cp.costLedger.outputTokens,
      totalTokens: cp.costLedger.totalTokens,
      providerAttempts: cp.costLedger.providerAttempts,
      averagePerSuccessfulLogicalCall:
        cp.logical.succeeded > 0
          ? Number((cp.costLedger.actualUsd / cp.logical.succeeded).toFixed(6))
          : null,
      projectionFromGlobalEn: cp.activationGate?.COST?.PROJECTION || null,
    },
    metrics: {
      metricVersion: METRIC_VERSION,
      byEntity: metrics.byEntity,
      competitivePosition: metrics.competitivePosition,
      citationRateReadiness: "PARTIAL",
      COMPARABLE_PRIOR_PERIOD: "NONE",
      TREND_AVAILABLE: false,
    },
    versions: {
      orchestrator: WAVE1_ORCHESTRATOR_VERSION,
      resolver: RESOLVER_VERSION,
      classifier: RECOMMENDATION_CLASSIFIER_VERSION,
      citationAssoc: CITATION_ASSOC_VERSION,
      metric: METRIC_VERSION,
      geography: GEOGRAPHY_MODEL_VERSION,
      promptLibrary: WAVE1_PROMPT_LIBRARY_VERSION,
      plan: WAVE1_SHOWCASE_PLAN_VERSION,
    },
    AIRTABLE_EXECUTION_WRITES: 0,
    AI_VISIBILITY_OPPORTUNITY_WRITES: 0,
    ENTITLEMENT_WRITES: 0,
    COMPARABLE_PRIOR_PERIOD: "NONE",
    TREND_AVAILABLE: false,
    preflight,
    completedAt: new Date().toISOString(),
  };

  await store.saveBatchSummary(summary);
  await store.updateBatch(cp.wave1Id, {
    status: cp.status,
    health:
      stopReason === "hard_cost_cap"
        ? BATCH_HEALTH.COST_LIMIT_REACHED
        : stopReason === "provider_auth_error"
          ? BATCH_HEALTH.PROVIDER_AUTH_ERROR
          : cp.logical.failedFinal > 0
            ? BATCH_HEALTH.PARTIAL_PROVIDER_FAILURE
            : BATCH_HEALTH.HEALTHY,
    completedAt: summary.completedAt,
    successfulRuns: cp.logical.succeeded,
    failedRuns: cp.logical.failedFinal,
    skippedRuns: cp.logical.notExecuted,
    retries: cp.logical.retried,
    usage: {
      inputTokens: cp.costLedger.inputTokens,
      outputTokens: cp.costLedger.outputTokens,
      totalTokens: cp.costLedger.totalTokens,
      estimatedCost: cp.costLedger.actualUsd,
    },
  });
  persistCheckpoint(cp);
  writeJson(path.join(cp.storeRoot, "waves", cp.wave1Id, "summary.json"), summary);

  return {
    mode: "execute",
    wave1Id: cp.wave1Id,
    summary,
    checkpoint: cp,
    planMeta: {
      baselineSeriesId: WAVE1_BASELINE_SERIES_ID,
      peerSetId: WAVE1_PEER_SET_ID,
      promptCount: plan.PROMPT_LIBRARY.LOADED,
    },
  };
}

export {
  createWave1Id,
  checkpointPath,
  persistCheckpoint,
  createInitialCheckpoint,
  WAVE1_HARD_CAP_USD,
  WAVE1_COST_EVIDENCE,
  projectWaveCostFromSample,
  buildAiVisibilityEntityIndex,
};
