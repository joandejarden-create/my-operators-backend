/**
 * Initial multi-parent Brand AI longitudinal wave.
 * Shared PORTFOLIO_PROMPT_PROVIDER execution — 86 calls, not 344.
 * Isolated store. No scheduler. No Airtable response writes.
 */

import fs from "fs";
import path from "path";
import { createHash, randomBytes } from "crypto";
import { fileURLToPath } from "url";
import { isAiVisibilityEnabled, isAiVisibilityLiveTestAllowed, METRIC_VERSION } from "../config.js";
import { MONITORING_RUN_PURPOSE } from "../monitoring-run-purpose.js";
import { preflightAllProviderCredentials } from "../provider-credentials.js";
import { estimateProviderCost, COST_UNKNOWN } from "../providers/provider-cost.js";
import { isAuthProviderError, hashPromptText } from "../execution-batch.js";
import { createAiVisibilityStore, createBrandAiVisibilityReadStore } from "../storage/index.js";
import { HISTORIC_PROVIDER_COST } from "../stability-policy.js";
import { buildLiveAiVisibilityEntityIndex } from "../entity-index.js";
import { loadPeerSetConfig, resolvePeerSetMembership, PEER_SET_ID_V2, PEER_SET_ID_V3 } from "../peer-sets.js";
import { validatePeerSetAgainstIndex } from "../execute-cohort.js";
import { runVisibilityPrompt } from "../providers/index.js";
import { normalizeVisibilityProviderResponse } from "../providers/normalized-response.js";
import { extractMentions } from "../extract-mentions.js";
import { extractCitations } from "../extract-citations.js";
import { harvestUnresolvedWithFilterStats } from "../mention-classification.js";
import { normalizeMatchKey } from "../normalize-entities.js";
import {
  buildObservationFromExtractions,
  computeAiPresenceRate,
  computeQuestionsMissing,
  computeCitationRate,
} from "../metrics.js";
import { assembleEvidenceRecord } from "../evidence.js";
import { classifyProviderError } from "../providers/provider-errors.js";
import { getProviderRetryPolicy } from "../providers/provider-retry-policy.js";
import { loadScenarioRegistry, buildScenarioRegistryIndex, resolvePromptScenario } from "../scenario-registry.js";
import { resolvePromptProvenance } from "../prompt-provenance.js";
import {
  buildMonthlyExecutionMatrix,
} from "./cohort-v1.js";
import { buildLongitudinalCostModel } from "./cost-model.js";
import {
  BRAND_LONGITUDINAL_STORE_ROOT,
  DATASET_NAMESPACE,
  PERIOD_QUALITY_STATE,
  VALID_PERIOD_SUCCESS_THRESHOLD,
  createBrandMeasurementPeriodId,
  buildMeasurementPeriodManifest,
  writeMeasurementPeriodManifest,
  qualifyMeasurementPeriod,
} from "./measurement-period.js";
import {
  acquireMeasurementLock,
  completeMeasurementLock,
  isDuplicateMeasurementCycle,
  buildMeasurementIdempotencyKey,
} from "./idempotency.js";
import { normalizeMeasurementDate } from "./grain.js";
import { PRIMARY_BASELINE_DATE } from "./baseline-audit.js";
import { evidenceToObservation } from "../stability-historical-audit.js";
import { auditRadissonMeasurementEligibility } from "./radisson-gate.js";
import {
  loadSelectedBrandUniverse,
  RADISSON_BRAND_ID,
  SELECTED_BRANDS_EXPECTED,
  PARENT_COMPANIES_EXPECTED,
} from "./selected-universe.js";

export const MULTI_PARENT_WAVE_VERSION = "brand_longitudinal_multi_parent_wave_v1";
export const MULTI_PARENT_HARD_CAP_USD = 60;
export const PLANNED_PROVIDER_CALLS_EXPECTED = 86;
export const PROVIDER_CALLS_EXPECTED = Object.freeze({
  openai: 27,
  gemini: 16,
  perplexity: 27,
  claude: 16,
});
export const PROVIDER_MODELS = Object.freeze({
  openai: "gpt-5.6",
  gemini: "gemini-3.6-flash",
  perplexity: "sonar",
  claude: "claude-sonnet-4-6",
});
export const PROVIDER_ORDER = Object.freeze(["perplexity", "gemini", "openai", "claude"]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");
const SEED_PATHS = [
  path.join(REPO_ROOT, "fixtures", "ai-visibility", "phase3a9-showcase-prompt-seed.json"),
  path.join(REPO_ROOT, "fixtures", "ai-visibility", "phase2d-prompt-seed.json"),
  path.join(REPO_ROOT, "fixtures", "ai-visibility", "observed-demand-prompts-v1.json"),
];

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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function knownNameKeys(entities) {
  const keys = new Set();
  for (const e of entities || []) {
    keys.add(normalizeMatchKey(e.name));
    for (const a of e.aliases || []) keys.add(normalizeMatchKey(a));
  }
  return keys;
}

function resolveApiKey(provider) {
  if (provider === "gemini") return process.env.GEMINI_API_KEY;
  if (provider === "perplexity") return process.env.PERPLEXITY_API_KEY;
  if (provider === "claude") return process.env.ANTHROPIC_API_KEY;
  if (provider === "openai") return process.env.OPENAI_API_KEY;
  return null;
}

function resolveModel(provider) {
  const envKey = `AI_VISIBILITY_${provider.toUpperCase()}_MODEL`;
  if (provider === "openai") {
    return process.env.AI_VISIBILITY_MODEL || process.env[envKey] || PROVIDER_MODELS.openai;
  }
  return process.env[envKey] || PROVIDER_MODELS[provider];
}

export function loadLongitudinalPromptLibrary() {
  const map = new Map();
  for (const seedPath of SEED_PATHS) {
    if (!fs.existsSync(seedPath)) continue;
    const raw = JSON.parse(fs.readFileSync(seedPath, "utf8"));
    for (const row of raw.prompts || []) {
      if (row.promptId && !map.has(row.promptId)) {
        map.set(row.promptId, { ...row, _seedFile: path.basename(seedPath) });
      }
    }
  }
  return map;
}

function resolveCallCost(provider, usage) {
  const historic = HISTORIC_PROVIDER_COST[provider]?.historicUsdPerCall ?? 0;
  const conservative = HISTORIC_PROVIDER_COST[provider]?.conservativeUsdPerCall ?? historic;
  const est = estimateProviderCost(provider, usage);
  if (usage?.providerCostUsd != null && Number.isFinite(Number(usage.providerCostUsd))) {
    return {
      amountUsd: Number(usage.providerCostUsd),
      basis: "PROVIDER_RETURNED",
      estimate: est,
      historic,
      conservative,
    };
  }
  if (est.amountUsd != null && est.status !== COST_UNKNOWN) {
    return {
      amountUsd: Number(est.amountUsd),
      basis: `ESTIMATED_${est.status}`,
      estimate: est,
      historic,
      conservative,
    };
  }
  return {
    amountUsd: historic,
    basis: "ESTIMATED_HISTORIC_PER_CALL",
    estimate: est,
    historic,
    conservative,
  };
}

function minSuccessfulCalls(planned = PLANNED_PROVIDER_CALLS_EXPECTED) {
  return Math.ceil(planned * VALID_PERIOD_SUCCESS_THRESHOLD);
}

export async function buildMultiParentWavePreflight(options = {}) {
  const radisson = auditRadissonMeasurementEligibility();
  const universe = radisson.universe || loadSelectedBrandUniverse();
  const monthly = buildMonthlyExecutionMatrix();
  const cost = buildLongitudinalCostModel();
  const cred = preflightAllProviderCredentials();
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  for (const row of monthly.rows) byProvider[row.provider] += 1;

  const library = loadLongitudinalPromptLibrary();
  const scenarioIndex = buildScenarioRegistryIndex(loadScenarioRegistry());
  const invalidPromptIds = [];
  const derivedMonthly = [];
  const configs = [];
  for (const row of monthly.rows) {
    if (configs.some((c) => c.promptId === row.promptId)) continue;
    const prompt = library.get(row.promptId);
    if (!prompt || !String(prompt.promptText || "").trim()) {
      invalidPromptIds.push(row.promptId);
      continue;
    }
    if (String(prompt.promptOrigin || "").toUpperCase() === "DERIVED") {
      derivedMonthly.push(row.promptId);
    }
    const provenance = resolvePromptProvenance(prompt, { scenarioIndex });
    const scenario = resolvePromptScenario(prompt, scenarioIndex);
    configs.push({
      promptId: row.promptId,
      promptText: prompt.promptText,
      promptOrigin: provenance.promptOrigin || prompt.promptOrigin || "SCENARIO",
      scenarioId: provenance.scenarioId || scenario.scenarioId || prompt.scenarioId || null,
      language: prompt.language || "en",
      geographyKey: prompt.commercialRegion || prompt.country || "CALA",
      geographyScope: prompt.geographyScope || "Region",
      country: prompt.country || null,
      version: prompt.version || "1",
      promptFamily: prompt.promptFamily || null,
      semanticPairId: prompt.semanticPairId || null,
      intentTerritory: prompt.intentTerritory || null,
      tier: row.tier,
    });
  }

  const failReasons = [];
  if (universe.TOTAL_PARENT_COMPANIES !== PARENT_COMPANIES_EXPECTED) {
    failReasons.push(`parent_count_${universe.TOTAL_PARENT_COMPANIES}`);
  }
  if (universe.TOTAL_SELECTED_BRANDS !== SELECTED_BRANDS_EXPECTED) {
    failReasons.push(`selected_brands_${universe.TOTAL_SELECTED_BRANDS}_expected_${SELECTED_BRANDS_EXPECTED}`);
  }
  if (radisson.RADISSON_MEASUREMENT_ELIGIBLE !== "YES") {
    failReasons.push("RADISSON_MEASUREMENT_ELIGIBLE_NO");
  }
  if (monthly.callCount !== PLANNED_PROVIDER_CALLS_EXPECTED) {
    failReasons.push(`planned_calls_${monthly.callCount}_expected_${PLANNED_PROVIDER_CALLS_EXPECTED}`);
  }
  for (const [p, n] of Object.entries(PROVIDER_CALLS_EXPECTED)) {
    if (byProvider[p] !== n) failReasons.push(`provider_matrix_${p}_${byProvider[p]}_expected_${n}`);
  }
  if (invalidPromptIds.length) failReasons.push("invalid_prompt_ids");
  if (derivedMonthly.length) failReasons.push("derived_monthly_nonzero");
  if (cost.conservativeExpectedCostUsd > MULTI_PARENT_HARD_CAP_USD) {
    failReasons.push("COST_GATE_FAILED");
  }
  const credMissing = ["OPENAI_CREDENTIAL", "GEMINI_CREDENTIAL", "PERPLEXITY_CREDENTIAL", "CLAUDE_CREDENTIAL"].filter(
    (k) => cred[k] === "MISSING"
  );
  if (options.requireCredentials !== false && credMissing.length) {
    failReasons.push(`missing_credentials:${credMissing.join(",")}`);
  }

  const now = options.now || new Date();
  const measurementDate = normalizeMeasurementDate(now.toISOString());
  const idempotencyKey = buildMeasurementIdempotencyKey({
    cohortId: "BRAND_LONGITUDINAL_COHORT_V1",
    cohortVersion: "1.0.0",
    measurementDate,
    datasetNamespace: DATASET_NAMESPACE,
    cadence: "monthly",
  });
  const dup = isDuplicateMeasurementCycle(idempotencyKey, options.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT);

  return {
    radisson,
    universe,
    monthly,
    cost,
    credentials: {
      OPENAI: cred.OPENAI_CREDENTIAL,
      GEMINI: cred.GEMINI_CREDENTIAL,
      PERPLEXITY: cred.PERPLEXITY_CREDENTIAL,
      CLAUDE: cred.CLAUDE_CREDENTIAL,
    },
    callsByProvider: byProvider,
    invalidPromptIds,
    derivedMonthly,
    configs,
    PROJECTED_HISTORIC_COST: cost.historicExpectedCostUsd,
    PROJECTED_CONSERVATIVE_COST: cost.conservativeExpectedCostUsd,
    COST_GATE: cost.conservativeExpectedCostUsd <= MULTI_PARENT_HARD_CAP_USD ? "PASS" : "FAIL",
    PLANNED_CALLS: monthly.callCount,
    MIN_SUCCESSFUL_CALLS: minSuccessfulCalls(monthly.callCount),
    measurementDate,
    idempotencyKey,
    duplicate: dup,
    DUPLICATE_LOCK: dup.duplicate ? "FAIL" : "PASS",
    failReasons,
    blocked: failReasons.length > 0 || dup.duplicate,
    DATAFORSEO_CALLS: 0,
    STANDARD_PROMPTS: 0,
    PER_PARENT_DUPLICATE_EXECUTION: 0,
  };
}

function classifyBrandCoverage({ brandId, peerIds, presentCount, identityIssues }) {
  if (identityIssues?.length) return "IDENTITY_AMBIGUITY";
  if (!(peerIds || []).includes(brandId)) return "NOT_IN_MEASUREMENT_PEER_SET";
  if (presentCount > 0) return "MEASURED_WITH_RESOLVED_EVIDENCE";
  return "MEASURED_NO_MENTION";
}

function buildCoverage(universe, peerIds, observations, identityByBrand = {}) {
  const parents = [];
  const perBrand = [];
  for (const parent of universe.parents) {
    const brandRows = [];
    for (const brand of universe.brands.filter((b) => b.companyKey === parent.companyKey)) {
      const presentCount = observations.filter((o) => o.success && (o.presentEntityIds || []).includes(brand.brandId)).length;
      const classLabel = classifyBrandCoverage({
        brandId: brand.brandId,
        peerIds,
        presentCount,
        identityIssues: identityByBrand[brand.brandId],
      });
      const row = {
        BRAND: brand.brandName,
        brandId: brand.brandId,
        CLASS: classLabel,
        presentCount,
        inPeerSet: (peerIds || []).includes(brand.brandId),
      };
      brandRows.push(row);
      perBrand.push({ ...row, PARENT: parent.PARENT });
    }
    const valid = brandRows.filter((b) => b.inPeerSet);
    parents.push({
      PARENT: parent.PARENT,
      SELECTED_BRANDS: parent.BRAND_COUNT,
      VALID_MEASUREMENT_BRANDS: valid.length,
      BRANDS_WITH_PRESENCE: brandRows.filter((b) => b.presentCount > 0).map((b) => b.BRAND),
      BRANDS_WITH_ZERO_PRESENCE: brandRows.filter((b) => b.presentCount === 0).map((b) => b.BRAND),
      IDENTITY_ISSUES: brandRows.filter((b) => b.CLASS === "IDENTITY_AMBIGUITY").map((b) => b.BRAND),
    });
  }
  return { parents, perBrand };
}

async function recoverBaselineObservations(promptIds, entityIndex) {
  const store = createBrandAiVisibilityReadStore();
  const evidence = (await store.listEvidence({})) || [];
  const want = new Set(promptIds);
  const out = [];
  for (const row of evidence) {
    if (!want.has(row.promptId)) continue;
    const ts = row.timestamp || row.completedAt || row.savedAt || row.payload?.timestamp;
    const date = normalizeMeasurementDate(ts);
    if (date && date !== PRIMARY_BASELINE_DATE) continue;
    const rawText = row.payload?.rawResponseText || row.rawText || row.text || null;
    let observation = evidenceToObservation(row);
    if (rawText && entityIndex?.aliasIndex) {
      const mentions = extractMentions({
        responseId: row.responseId || row.evidenceId || "baseline",
        text: rawText,
        entityIndex: entityIndex.aliasIndex,
        promptIntentTerritory: row.intentTerritory || null,
      });
      observation = {
        ...observation,
        presentEntityIds: [
          ...new Set(mentions.map((m) => m.canonicalEntityId).filter(Boolean)),
        ],
        mentions,
        recoveredExtraction: true,
      };
    }
    observation.measurementDate = date || PRIMARY_BASELINE_DATE;
    out.push(observation);
  }
  return out;
}

function ratePct(metric) {
  if (metric?.value == null) return null;
  return Number((metric.value * 100).toFixed(2));
}

function buildPerBrandCurrentVsPrior(universe, currentObs, priorObs, promptIds) {
  const rows = [];
  for (const brand of universe.brands) {
    const currentPresence = computeAiPresenceRate(currentObs, brand.brandId);
    const currentQm = computeQuestionsMissing(currentObs, brand.brandId, promptIds);
    const hasPrior = (priorObs || []).length > 0;
    const priorPresence = hasPrior ? computeAiPresenceRate(priorObs, brand.brandId) : null;
    const priorQm = hasPrior ? computeQuestionsMissing(priorObs, brand.brandId, promptIds) : null;
    const comparable =
      hasPrior &&
      priorPresence?.denominator > 0 &&
      currentPresence?.denominator > 0;
    rows.push({
      BRAND: brand.brandName,
      brandId: brand.brandId,
      PARENT: brand.parent,
      BASELINE_COMPARABLE: comparable ? "YES" : "NO",
      CURRENT_PRESENCE: ratePct(currentPresence),
      PRIOR_PRESENCE: comparable ? ratePct(priorPresence) : null,
      ABSOLUTE_CHANGE:
        comparable && currentPresence.value != null && priorPresence.value != null
          ? Number((currentPresence.value - priorPresence.value).toFixed(4))
          : null,
      CURRENT_QM: currentQm.count,
      PRIOR_QM: comparable ? priorQm.count : null,
      SERIES_STATE: comparable ? "CURRENT_VS_PRIOR" : "CURRENT_BASELINE_START",
    });
  }
  return rows;
}

async function persistSuccessfulCall({
  store,
  storeRoot,
  periodId,
  exec,
  result,
  entityIndex,
  nameKeys,
  costInfo,
  peerSetId,
  peerSetVersion,
}) {
  const runId = store.generateId ? store.generateId("run") : `run_${randomBytes(8).toString("hex")}`;
  const responseId = store.generateId
    ? store.generateId("resp")
    : `resp_${randomBytes(8).toString("hex")}`;
  const promptTextHash = hashPromptText(exec.promptText);
  const rawArtifactUri = path.join(storeRoot, periodId, "raw", `${exec.fingerprint}.json`);
  writeJson(rawArtifactUri, {
    fingerprint: exec.fingerprint,
    periodId,
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
    peerSetId,
    peerSetVersion,
    metricVersion: METRIC_VERSION,
    rawArtifactUri,
    useV1_1: true,
  });
  writeJson(path.join(storeRoot, periodId, "normalized", `${exec.fingerprint}.json`), normalized);

  const createdAt = new Date().toISOString();
  const response = {
    responseId,
    runId,
    batchId: periodId,
    periodId,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    promptVersion: exec.version,
    promptFamily: exec.promptFamily,
    semanticPairId: exec.semanticPairId,
    provider: result.provider,
    model: result.model || "UNKNOWN",
    text: result.text,
    rawText: result.text,
    citations: result.citations,
    searchResults: result.searchResults || null,
    usage: result.usage,
    latencyMs: result.latencyMs,
    citationCapability: result.citationCapability,
    providerMeta: result.providerMeta || null,
    requestId: result.requestId || result.providerMeta?.id || result.raw?.id || null,
    normalized,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    peerSetId,
    peerSetVersion,
    metricVersion: METRIC_VERSION,
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.RECURRING,
    promptTextHash,
    createdAt,
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

  const observation = buildObservationFromExtractions({
    observationId: responseId,
    promptId: exec.promptId,
    provider: result.provider,
    periodKey: periodId,
    success: true,
    mentions,
    citations,
    geography: exec.geographyKey,
    intentTerritory: exec.intent,
  });

  const run = {
    runId,
    responseId,
    batchId: periodId,
    periodId,
    fingerprint: exec.fingerprint,
    promptId: exec.promptId,
    provider: result.provider,
    model: result.model || "UNKNOWN",
    status: "completed",
    rawText: result.text,
    language: exec.language,
    geographyKey: exec.geographyKey,
    intent: exec.intent,
    monitoringRunPurpose: MONITORING_RUN_PURPOSE.RECURRING,
    latencyMs: result.latencyMs,
    completedAt: createdAt,
  };
  writeJson(path.join(storeRoot, periodId, "runs", `${runId}.json`), run);

  const evidence = assembleEvidenceRecord({
    prompt: {
      promptId: exec.promptId,
      version: exec.version,
      text: exec.promptText,
      geographyScope: exec.geographyScope,
      commercialRegion: exec.geographyKey,
      country: exec.country || null,
    },
    run,
    response,
    mentions,
    citations,
    language: exec.language,
    geography: {
      geographyScope: exec.geographyScope,
      regionName: exec.geographyKey,
      countryName: exec.country || null,
    },
  });
  evidence.commercialRegion = exec.geographyKey;
  evidence.geographyKey = exec.geographyKey;
  evidence.promptTextHash = promptTextHash;
  evidence.monitoringRunPurpose = MONITORING_RUN_PURPOSE.RECURRING;
  evidence.payload = {
    ...(evidence.payload || {}),
    promptTextHash,
    cost: costInfo,
    observation,
    runId,
  };
  const saved = await store.saveEvidence(evidence);
  return { run, response, mentions, citations, evidence: saved, observation };
}

export async function executeMultiParentLongitudinalWave(args = {}) {
  const preflight = args.preflight || (await buildMultiParentWavePreflight(args));
  if (preflight.blocked) {
    return {
      status: preflight.COST_GATE === "FAIL" ? "COST_GATE_FAILED" : "STOPPED_PREFLIGHT",
      FINAL:
        preflight.duplicate?.duplicate
          ? "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED"
          : "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED",
      preflight,
      PROVIDER_CALLS: 0,
      SPEND: 0,
    };
  }
  if (!isAiVisibilityEnabled() || !isAiVisibilityLiveTestAllowed()) {
    return {
      status: "STOPPED_WRONG_ENVIRONMENT",
      reason: "AI_VISIBILITY_ENABLED and AI_VISIBILITY_LIVE_TEST must be true",
      preflight,
      PROVIDER_CALLS: 0,
      SPEND: 0,
      FINAL: "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED",
    };
  }

  const storeRoot = args.storeRoot || BRAND_LONGITUDINAL_STORE_ROOT;
  const periodId = args.periodId || createBrandMeasurementPeriodId(args.now || new Date());
  const lock = acquireMeasurementLock(preflight.idempotencyKey, periodId, storeRoot);
  if (!lock.acquired) {
    return {
      status: "NO_SECOND_EXECUTION",
      existingPeriodId: lock.existingPeriodId,
      preflight,
      PROVIDER_CALLS: 0,
      SPEND: 0,
      FINAL: "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED",
    };
  }

  const store = args.store || createAiVisibilityStore({ rootDir: path.join(storeRoot, periodId) });
  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  const live = args.entityIndex
    ? { index: args.entityIndex }
    : await buildLiveAiVisibilityEntityIndex({ applyOverlay: true });
  if (!live?.index?.brands?.length) {
    return {
      status: "STOPPED_ENTITY_INDEX_EMPTY",
      preflight,
      PROVIDER_CALLS: 0,
      SPEND: 0,
      FINAL: "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED",
    };
  }

  const peerCfg = loadPeerSetConfig();
  const peer = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V3, commercialRegion: "CALA" }, peerCfg);
  const peerCheck = validatePeerSetAgainstIndex(peer, live.index);
  if (!peerCheck.canonicalValid) {
    return {
      status: "STOPPED_PEER_SET_NOT_IN_INDEX",
      missingEntityIds: peerCheck.missingEntityIds,
      preflight,
      PROVIDER_CALLS: 0,
      SPEND: 0,
      FINAL: "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED",
    };
  }

  const nameKeys = knownNameKeys(live.index.entities);
  const configById = new Map(preflight.configs.map((c) => [c.promptId, c]));
  const executions = preflight.monthly.rows
    .map((row) => {
      const cfg = configById.get(row.promptId);
      const canonical = [
        "brand-longitudinal-multi-parent",
        periodId,
        row.provider,
        row.promptId,
        cfg.version,
        cfg.language,
        cfg.geographyKey,
      ].join("|");
      return {
        ...row,
        ...cfg,
        fingerprint: createHash("sha256").update(canonical).digest("hex").slice(0, 24),
        canonical,
        intent: cfg.intentTerritory,
      };
    })
    .sort(
      (a, b) => PROVIDER_ORDER.indexOf(a.provider) - PROVIDER_ORDER.indexOf(b.provider)
    );

  writeJson(path.join(storeRoot, periodId, "planned-call-matrix.json"), {
    periodId,
    callCount: executions.length,
    executions: executions.map((e) => ({
      promptId: e.promptId,
      provider: e.provider,
      tier: e.tier,
      fingerprint: e.fingerprint,
      language: e.language,
      geographyKey: e.geographyKey,
    })),
  });

  const ledger = {
    hardCapUsd: MULTI_PARENT_HARD_CAP_USD,
    actualUsd: 0,
    retries: 0,
    byProvider: {
      openai: { actualUsd: 0, calls: 0, basis: [] },
      gemini: { actualUsd: 0, calls: 0, basis: [] },
      perplexity: { actualUsd: 0, calls: 0, basis: [] },
      claude: { actualUsd: 0, calls: 0, basis: [] },
    },
    capBreached: false,
    stoppedReason: null,
  };
  const stats = {
    PLANNED: executions.length,
    ATTEMPTED: 0,
    SUCCEEDED: 0,
    FAILED: 0,
    RETRIES: 0,
    byProvider: { openai: 0, gemini: 0, perplexity: 0, claude: 0 },
    completedFingerprints: {},
    failedFingerprints: {},
    observations: [],
  };
  const checkpointPath = path.join(storeRoot, periodId, "checkpoint.json");
  const persistCheckpoint = () =>
    writeJson(checkpointPath, { periodId, ledger, stats: { ...stats, observations: undefined } });

  const startedAt = new Date().toISOString();
  writeMeasurementPeriodManifest(
    buildMeasurementPeriodManifest({
      measurementPeriodId: periodId,
      datasetNamespace: DATASET_NAMESPACE,
      startedAt,
      plannedCalls: executions.length,
      brandCount: preflight.universe.TOTAL_SELECTED_BRANDS,
      promptCount: preflight.monthly.promptCount,
      qualityState: PERIOD_QUALITY_STATE.RUNNING,
    }),
    storeRoot
  );

  for (const exec of executions) {
    if (stats.completedFingerprints[exec.fingerprint]) continue;
    const nextConservative = HISTORIC_PROVIDER_COST[exec.provider]?.conservativeUsdPerCall || 0;
    if (ledger.actualUsd >= MULTI_PARENT_HARD_CAP_USD || ledger.actualUsd + nextConservative > MULTI_PARENT_HARD_CAP_USD) {
      ledger.capBreached = true;
      ledger.stoppedReason = "hard_cost_cap";
      break;
    }

    stats.ATTEMPTED += 1;
    const policy = getProviderRetryPolicy(exec.provider);
    const model = resolveModel(exec.provider);
    let result = null;
    let error = null;
    let attempts = 0;
    while (attempts <= policy.maxRetriesPerCall) {
      attempts += 1;
      try {
        result = await runFn({
          provider: exec.provider,
          prompt: { text: exec.promptText, promptId: exec.promptId },
          model,
          apiKey: resolveApiKey(exec.provider),
          enableWebSearch: true,
          timeoutMs: Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || policy.timeoutMsDefault),
          fetchImpl: args.fetchImpl,
        });
        error = null;
        break;
      } catch (err) {
        error = err;
        const classified = classifyProviderError(err);
        writeJson(
          path.join(storeRoot, periodId, "failed-attempts", `${exec.fingerprint}_attempt${attempts}.json`),
          {
            fingerprint: exec.fingerprint,
            promptId: exec.promptId,
            provider: exec.provider,
            attempt: attempts,
            error: classified,
            preserved: true,
            failedAt: new Date().toISOString(),
          }
        );
        if (attempts <= policy.maxRetriesPerCall && (policy.retryCategories || []).includes(classified.category)) {
          stats.RETRIES += 1;
          ledger.retries += 1;
          await sleep(policy.backoffMs || 1500);
          continue;
        }
        break;
      }
    }

    if (error || !result) {
      stats.FAILED += 1;
      stats.failedFingerprints[exec.fingerprint] = true;
      persistCheckpoint();
      if (isAuthProviderError(error) || classifyProviderError(error).category === "AUTH") {
        ledger.stoppedReason = "provider_auth_error";
        break;
      }
      continue;
    }

    const costInfo = resolveCallCost(exec.provider, result.usage);
    ledger.actualUsd = Number((ledger.actualUsd + costInfo.amountUsd).toFixed(6));
    ledger.byProvider[exec.provider].actualUsd = Number(
      (ledger.byProvider[exec.provider].actualUsd + costInfo.amountUsd).toFixed(6)
    );
    ledger.byProvider[exec.provider].calls += 1;
    ledger.byProvider[exec.provider].basis.push(costInfo.basis);

    const persisted = await persistSuccessfulCall({
      store,
      storeRoot,
      periodId,
      exec,
      result,
      entityIndex: live.index,
      nameKeys,
      costInfo,
      peerSetId: PEER_SET_ID_V3,
      peerSetVersion: "3",
    });
    stats.SUCCEEDED += 1;
    stats.byProvider[exec.provider] += 1;
    stats.completedFingerprints[exec.fingerprint] = {
      evidenceId: persisted.evidence.evidenceId,
      runId: persisted.run.runId,
    };
    stats.observations.push({
      ...persisted.observation,
      promptId: exec.promptId,
      provider: exec.provider,
      language: exec.language,
      geographyKey: exec.geographyKey,
      promptVersion: exec.version,
      providerModel: result.model || "UNKNOWN",
      brandIds: persisted.observation.presentEntityIds,
    });
    persistCheckpoint();
  }

  const completedAt = new Date().toISOString();
  const measurementDate = normalizeMeasurementDate(completedAt);
  const quality = qualifyMeasurementPeriod({
    plannedCalls: stats.PLANNED,
    successfulCalls: stats.SUCCEEDED,
    qualityState:
      stats.SUCCEEDED / stats.PLANNED >= VALID_PERIOD_SUCCESS_THRESHOLD
        ? PERIOD_QUALITY_STATE.VALID
        : PERIOD_QUALITY_STATE.PARTIAL_PERIOD,
  });

  const promptIds = [...new Set(preflight.monthly.rows.map((r) => r.promptId))];
  const priorObs = await recoverBaselineObservations(promptIds, live.index);
  const coverage = buildCoverage(preflight.universe, peer.entityIds, stats.observations);
  const brandCompare = buildPerBrandCurrentVsPrior(
    preflight.universe,
    stats.observations,
    priorObs,
    promptIds
  );
  const radissonRow = brandCompare.find((b) => b.brandId === RADISSON_BRAND_ID);
  const radissonPriorAvailable = Boolean(
    priorObs.some((o) => o.success) && (radissonRow?.BASELINE_COMPARABLE === "YES" || priorObs.length > 0)
  );
  const radissonSeries =
    radissonRow?.BASELINE_COMPARABLE === "YES" ? "CURRENT_VS_PRIOR" : "BASELINE_ONLY";

  const commonPrompts = promptIds.filter((id) => priorObs.some((o) => o.promptId === id));
  const currentProviderGrains = new Set(
    stats.observations.map((o) => `${o.promptId}|${o.provider}|${o.language}|${o.geographyKey}`)
  );
  const priorProviderGrains = new Set(
    priorObs.map((o) => `${o.promptId}|${o.provider}|${o.language || "en"}|${o.geography}`)
  );
  const commonProviderGrains = [...currentProviderGrains].filter((k) => priorProviderGrains.has(k));

  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" }, peerCfg);
  const cohortChanged = (peer.entityIds || []).length !== (v2.entityIds || []).length;

  const period = buildMeasurementPeriodManifest({
    measurementPeriodId: periodId,
    datasetNamespace: DATASET_NAMESPACE,
    startedAt,
    completedAt,
    promptCount: preflight.monthly.promptCount,
    brandCount: preflight.universe.TOTAL_SELECTED_BRANDS,
    plannedCalls: stats.PLANNED,
    successfulCalls: stats.SUCCEEDED,
    failedCalls: stats.FAILED,
    totalCostUsd: ledger.actualUsd,
    qualityState: quality.qualityState,
    modelMetadata: Object.fromEntries(
      Object.entries(PROVIDER_MODELS).map(([p, m]) => [p, resolveModel(p) || m || "UNKNOWN"])
    ),
    comparabilityNotes: [
      `measurement_peer_set=${PEER_SET_ID_V3}`,
      `frozen_live_peer_set=${PEER_SET_ID_V2}`,
      `selected_brands=${preflight.universe.TOTAL_SELECTED_BRANDS}`,
    ],
  });
  const manifestPath = writeMeasurementPeriodManifest(period, storeRoot);
  writeJson(path.join(storeRoot, periodId, "cost-ledger.json"), ledger);
  writeJson(path.join(storeRoot, periodId, "quality-state.json"), quality);
  writeJson(path.join(storeRoot, periodId, "coverage.json"), coverage);
  writeJson(path.join(storeRoot, periodId, "current-vs-prior.json"), {
    CURRENT_DATE: measurementDate,
    PRIOR_DATE: PRIMARY_BASELINE_DATE,
    brandCompare,
    commonPrompts: commonPrompts.length,
    commonProviderGrains: commonProviderGrains.length,
    COHORT_CHANGED: cohortChanged ? "YES" : "NO",
  });
  completeMeasurementLock(preflight.idempotencyKey, periodId, storeRoot);

  const nextDate = new Date(completedAt);
  nextDate.setUTCMonth(nextDate.getUTCMonth() + 1);
  const nextRecommended = nextDate.toISOString().slice(0, 10);

  const valid = quality.qualityState === PERIOD_QUALITY_STATE.VALID;
  const FINAL = valid
    ? "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_PASS"
    : stats.SUCCEEDED > 0
      ? "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_PARTIAL"
      : "BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_REMEDIATION_REQUIRED";

  return {
    BRAND_AI_INITIAL_MULTI_PARENT_LONGITUDINAL_WAVE_COMPLETE: true,
    FINAL,
    periodId,
    measurementDate,
    qualityState: quality.qualityState,
    STATUS: valid ? "VALID" : stats.SUCCEEDED > 0 ? "PARTIAL" : "FAILED",
    preflight,
    stats,
    ledger,
    coverage,
    brandCompare,
    radisson: {
      BASELINE_2026_08_14: radissonPriorAvailable ? "AVAILABLE" : "NOT_AVAILABLE",
      CURRENT_PERIOD: stats.SUCCEEDED > 0 ? "PASS" : "FAIL",
      SERIES_STATE: radissonSeries,
    },
    longitudinal: {
      REAL_DISTINCT_PERIODS: valid ? 2 : 1,
      CLIENT_STATE: valid ? "CURRENT_VS_PRIOR" : "BASELINE_ONLY",
      CURRENT_DATE: measurementDate,
      PRIOR_DATE: PRIMARY_BASELINE_DATE,
      COMMON_PROMPTS: commonPrompts.length,
      COMMON_PROVIDER_GRAINS: commonProviderGrains.length,
      COHORT_CHANGED: cohortChanged ? "YES" : "NO",
    },
    storage: {
      PERIOD_MANIFEST: manifestPath,
      OBSERVATION_STORE: path.join(storeRoot, periodId),
      COST_LEDGER: path.join(storeRoot, periodId, "cost-ledger.json"),
      QUALITY_STATE: path.join(storeRoot, periodId, "quality-state.json"),
    },
    NEXT_RECOMMENDED_MEASUREMENT_DATE: nextRecommended,
    SCHEDULER_ENABLE: 0,
    PROVIDER_CALLS: stats.SUCCEEDED + stats.FAILED,
    SPEND: ledger.actualUsd,
  };
}
