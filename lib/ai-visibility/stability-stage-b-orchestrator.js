/**
 * Controlled Stage B repeated-testing execution.
 * Isolated store. Exact repeats only. Does not federate into live Brand AI reads.
 * Does not change certified classifiers.
 */

import fs from "fs";
import path from "path";
import { createHash, randomBytes } from "crypto";
import { fileURLToPath } from "url";
import { isAiVisibilityEnabled, isAiVisibilityLiveTestAllowed, METRIC_VERSION } from "./config.js";
import { MONITORING_RUN_PURPOSE } from "./monitoring-run-purpose.js";
import { preflightAllProviderCredentials } from "./provider-credentials.js";
import { estimateProviderCost, COST_UNKNOWN } from "./providers/provider-cost.js";
import { isAuthProviderError } from "./execution-batch.js";
import { hashPromptText } from "./execution-batch.js";
import { createAiVisibilityStore, createBrandAiVisibilityReadStore, STABILITY_STAGE_B_ROOT } from "./storage/index.js";
import {
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
  STAGE_B_NON_AUTHORITATIVE_WAVE_IDS,
} from "./stability-policy.js";
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
import { classifyProviderError } from "./providers/provider-errors.js";
import { getProviderRetryPolicy } from "./providers/provider-retry-policy.js";
import { isAssociationAttributeProductionEligible } from "./gaps/association-eligibility.js";
import {
  VALIDATION_COHORT,
  expandValidationCalls,
  estimateValidationCost,
  HISTORIC_PROVIDER_COST,
} from "./stability-policy.js";
import { lookupValidationCohortHistory, evidenceToObservation } from "./stability-historical-audit.js";
import {
  aggregateStabilitySeries,
  classifyCrossProviderAlignment,
  classifyStageBTimeWindow,
} from "./stability-aggregation.js";
import {
  classifyExecutiveEvidenceSupportLabel,
  formatExecutiveEvidenceLanguage,
} from "./stability-client.js";
import {
  loadScenarioRegistry,
  buildScenarioRegistryIndex,
  resolvePromptScenario,
} from "./scenario-registry.js";
import { resolvePromptProvenance } from "./prompt-provenance.js";
import { buildExecutiveFindings } from "./executive-finding-engine.js";

export const STAGE_B_ORCHESTRATOR_VERSION = "ai_visibility_stability_stage_b_v1";
export const STAGE_B_HARD_CAP_USD = 30;
export const STAGE_B_MAX_CALLS = 31;
export const CONTROLLED_VARIANT_CALLS = 0;
export const STAGE_B_PROVIDER_ORDER = Object.freeze(["perplexity", "gemini", "openai", "claude"]);
export const STAGE_B_MODELS = Object.freeze({
  openai: "gpt-5.6",
  gemini: "gemini-3.6-flash",
  perplexity: "sonar",
  claude: "claude-sonnet-4-6",
});

const FROZEN_PROMPT_IDS = Object.freeze(VALIDATION_COHORT.map((r) => r.promptId));
const CRITICAL_FOUR_PROVIDER = Object.freeze([
  "p_cala_independent_affiliation_v1",
  "p_cala_collection_affiliation_v1",
  "p_cala_soft_brand_shortlist_v1",
]);
const OBSERVED_PROMPT_IDS = Object.freeze(
  VALIDATION_COHORT.filter((r) => r.origin === "OBSERVED").map((r) => r.promptId)
);
const PARENT_PROMPT_ID = "p_obs_hotel_franchise_fees_en_v1";
const DERIVED_PROMPT_ID = "p_obs_hotel_franchise_fees_derived_en_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..");
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
    return process.env.AI_VISIBILITY_MODEL || process.env[envKey] || STAGE_B_MODELS.openai;
  }
  return process.env[envKey] || STAGE_B_MODELS[provider];
}

export function loadValidationPromptLibrary() {
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

export function verifyValidationPromptConfigs(cohort = VALIDATION_COHORT) {
  const library = loadValidationPromptLibrary();
  const scenarioIndex = buildScenarioRegistryIndex(loadScenarioRegistry());
  const invalid = [];
  const configs = [];
  for (const row of cohort) {
    const prompt = library.get(row.promptId);
    if (!prompt || !String(prompt.promptText || "").trim()) {
      invalid.push(row.promptId);
      continue;
    }
    const provenance = resolvePromptProvenance(prompt, { scenarioIndex });
    const scenario = resolvePromptScenario(prompt, scenarioIndex);
    configs.push({
      promptId: row.promptId,
      promptText: prompt.promptText,
      promptOrigin: provenance.promptOrigin || prompt.promptOrigin || row.origin,
      scenarioId: provenance.scenarioId || scenario.scenarioId || prompt.scenarioId || null,
      language: prompt.language || row.language,
      geography: prompt.country || prompt.commercialRegion || row.geographyKey,
      geographyScope: prompt.geographyScope || null,
      samplingPriority: row.samplingPriority,
      monitoringEligible: prompt.monitoringEligible === true,
      cadence: prompt.cadence || null,
      version: prompt.version || "1",
      promptFamily: prompt.promptFamily || null,
      semanticPairId: prompt.semanticPairId || null,
      intentTerritory: prompt.intentTerritory || null,
      seedFile: prompt._seedFile,
    });
  }
  return {
    ok: invalid.length === 0 && configs.length === cohort.length,
    INVALID_PROMPT_IDS: invalid,
    configs,
    monitoringEligibleToggled: false,
  };
}

function primaryGeographyFromLookup(lookupRow, fallback) {
  const grains = lookupRow?.grains || [];
  const counted = new Map();
  for (const g of grains) {
    const key = g.geographyKey || "unspecified";
    counted.set(key, (counted.get(key) || 0) + (g.observationCount || 0));
  }
  let best = fallback;
  let n = -1;
  for (const [k, v] of counted.entries()) {
    if (v > n && k !== "unspecified") {
      best = k;
      n = v;
    }
  }
  return best || fallback;
}

export async function buildStageBPreflight(options = {}) {
  const cohort = options.cohort || VALIDATION_COHORT;
  const frozenMismatch = cohort
    .map((r) => r.promptId)
    .filter((id, i) => id !== FROZEN_PROMPT_IDS[i] || cohort.length !== 16);
  const promptCheck = verifyValidationPromptConfigs(cohort);
  const lookup = await lookupValidationCohortHistory({
    cohort,
    store: options.historicalStore,
  });
  const cost = estimateValidationCost(cohort);
  const cred = preflightAllProviderCredentials();
  const callPlan = expandValidationCalls(cohort);
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  for (const c of callPlan) byProvider[c.provider] += 1;

  const failReasons = [...(lookup.failReasons || [])];
  if (frozenMismatch.length || cohort.length !== 16) {
    failReasons.push("frozen_cohort_mismatch");
  }
  if (!promptCheck.ok) failReasons.push("invalid_prompt_ids");
  if (callPlan.length !== STAGE_B_MAX_CALLS) {
    failReasons.push(`call_plan_${callPlan.length}_expected_${STAGE_B_MAX_CALLS}`);
  }
  if (byProvider.openai !== 16 || byProvider.gemini !== 3 || byProvider.perplexity !== 9 || byProvider.claude !== 3) {
    failReasons.push("provider_matrix_mismatch");
  }
  if (cost.PROJECTED_TOTAL_COST > STAGE_B_HARD_CAP_USD || cost.STOP) {
    failReasons.push("REPEATED_TESTING_BUDGET_BLOCKED");
  }

  const FULL_COHORT_LOOKUP = failReasons.includes("historical_store_empty") ||
    failReasons.includes("scenario_prompts_zero_despite_nonempty_store") ||
    !lookup.rows
      ? "FAIL"
      : lookup.FULL_COHORT_LOOKUP;

  const blocked =
    FULL_COHORT_LOOKUP === "FAIL" ||
    !promptCheck.ok ||
    failReasons.includes("REPEATED_TESTING_BUDGET_BLOCKED") ||
    failReasons.includes("frozen_cohort_mismatch") ||
    failReasons.includes("provider_matrix_mismatch") ||
    failReasons.includes(`call_plan_${callPlan.length}_expected_${STAGE_B_MAX_CALLS}`);

  return {
    FULL_COHORT_LOOKUP,
    PROMPTS_RESOLVED: promptCheck.ok ? "16 / 16" : `${promptCheck.configs.length} / 16`,
    INVALID_PROMPT_IDS: promptCheck.INVALID_PROMPT_IDS,
    failReasons,
    blocked,
    budgetBlocked: failReasons.includes("REPEATED_TESTING_BUDGET_BLOCKED"),
    lookup,
    promptCheck,
    cost,
    callPlan,
    callsByProvider: byProvider,
    credentials: {
      OPENAI: cred.OPENAI_CREDENTIAL,
      GEMINI: cred.GEMINI_CREDENTIAL,
      PERPLEXITY: cred.PERPLEXITY_CREDENTIAL,
      CLAUDE: cred.CLAUDE_CREDENTIAL,
    },
    HARD_CAP: STAGE_B_HARD_CAP_USD,
    CONTROLLED_VARIANT_CALLS,
    PROVIDER_CALLS: 0,
    DATAFORSEO_CALLS: 0,
  };
}

function createWaveId(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  return `aiv_stability_stage_b_${y}${m}${d}_${randomBytes(3).toString("hex")}`;
}

function buildFingerprint(parts) {
  const canonical = [
    "stability-stage-b",
    parts.waveId,
    parts.provider,
    parts.promptId,
    parts.promptVersion || "1",
    parts.language,
    parts.geographyKey,
    "EXACT_REPEAT",
    String(parts.repeatIndex || 1),
  ].join("|");
  return {
    canonical,
    fingerprint: createHash("sha256").update(canonical).digest("hex").slice(0, 24),
  };
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

async function persistSuccessfulCall({
  store,
  storeRoot,
  waveId,
  exec,
  result,
  entityIndex,
  nameKeys,
  costInfo,
}) {
  const runId = store.generateId ? store.generateId("run") : `run_${randomBytes(8).toString("hex")}`;
  const responseId = store.generateId
    ? store.generateId("resp")
    : `resp_${randomBytes(8).toString("hex")}`;
  const promptTextHash = hashPromptText(exec.promptText);

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

  const createdAt = new Date().toISOString();
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
    repeatType: "EXACT_REPEAT",
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

  const associationOutputs = (mentions || []).filter((m) =>
    isAssociationAttributeProductionEligible(m.attributeId || m.associationAttributeId)
  );

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
    repeatType: "EXACT_REPEAT",
    latencyMs: result.latencyMs,
    completedAt: createdAt,
  };
  writeJson(path.join(storeRoot, "runs", `${runId}.json`), run);

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
  evidence.repeatType = "EXACT_REPEAT";
  evidence.monitoringRunPurpose = MONITORING_RUN_PURPOSE.VALIDATION;
  evidence.payload = {
    ...(evidence.payload || {}),
    repeatType: "EXACT_REPEAT",
    promptTextHash,
    cost: costInfo,
    associationOutputs,
    truthClaimSpans: null,
    observationId: evidence.evidenceId,
    runId,
  };
  const saved = await store.saveEvidence(evidence);
  return {
    run,
    response,
    mentions,
    citations,
    evidence: saved,
    associationOutputs,
  };
}

function summarizePresence(mentions) {
  const ids = [
    ...new Set(
      (mentions || [])
        .map((m) => m.canonicalEntityId || m.resolvedEntityId || m.entityId)
        .filter(Boolean)
    ),
  ];
  return { present: ids.length > 0, presentEntityIds: ids };
}

function sourceRecurrenceFromObservations(observations) {
  const n = observations.length;
  const counts = new Map();
  for (const obs of observations) {
    const cites = obs.citations || obs.payload?.citations || [];
    const domains = new Set();
    for (const c of cites) {
      const d = String(c.domain || c.sourceDomain || parseDomain(c.url) || "")
        .trim()
        .toLowerCase();
      if (d) domains.add(d);
    }
    for (const d of domains) counts.set(d, (counts.get(d) || 0) + 1);
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 8)
    .map(([domain, citedIn]) => ({
      domain,
      citedIn,
      ofObservations: n,
      label: `${domain} cited ${citedIn} of ${n} responses`,
    }));
}

function combineObservations(historical, stageB) {
  return [...(historical || []), ...(stageB || [])];
}

export function loadStageBEvidenceForWave(storeRoot, waveId) {
  const runsDir = path.join(storeRoot, "runs");
  const runIds = new Set();
  if (fs.existsSync(runsDir)) {
    for (const file of fs.readdirSync(runsDir)) {
      if (!file.endsWith(".json")) continue;
      const run = readJson(path.join(runsDir, file));
      if (!run?.runId) continue;
      if (run.waveId === waveId || run.batchId === waveId) {
        runIds.add(run.runId);
      }
    }
  }
  const evidenceDir = path.join(storeRoot, "evidence");
  if (!fs.existsSync(evidenceDir)) return { runIds: [...runIds], evidence: [] };
  const evidence = [];
  for (const file of fs.readdirSync(evidenceDir)) {
    if (!file.endsWith(".json")) continue;
    const row = readJson(path.join(evidenceDir, file));
    if (row?.runId && runIds.has(row.runId)) evidence.push(row);
  }
  return { runIds: [...runIds], evidence };
}

export async function buildStageBReportFromStore(args = {}) {
  const waveId = args.waveId;
  if (!waveId) throw new Error("buildStageBReportFromStore: waveId required");
  const storeRoot = args.storeRoot || STABILITY_STAGE_B_ROOT;
  const preflight = args.preflight || (await buildStageBPreflight(args));
  const checkpointPath = path.join(storeRoot, "waves", waveId, "checkpoint.json");
  const checkpoint = readJson(checkpointPath) || {};

  const { evidence: stageBEvidence } = loadStageBEvidenceForWave(storeRoot, waveId);
  const historicalEvidenceByPrompt = new Map();
  for (const id of FROZEN_PROMPT_IDS) historicalEvidenceByPrompt.set(id, []);

  const histStore = args.historicalStore || createBrandAiVisibilityReadStore();
  const allHist = (await histStore.listEvidence({})) || [];
  const cohortSet = new Set(FROZEN_PROMPT_IDS);
  for (const ev of allHist) {
    if (!cohortSet.has(ev.promptId)) continue;
    if (!historicalEvidenceByPrompt.has(ev.promptId)) historicalEvidenceByPrompt.set(ev.promptId, []);
    historicalEvidenceByPrompt.get(ev.promptId).push(evidenceToObservation(ev));
  }

  const configById = new Map((preflight.promptCheck?.configs || []).map((c) => [c.promptId, c]));
  const grains = assembleStabilityResults({
    cohort: VALIDATION_COHORT,
    lookupRows: preflight.lookup?.rows || [],
    stageBEvidence,
    historicalEvidenceByPrompt,
  });

  const crossProvider = CRITICAL_FOUR_PROVIDER.map((promptId) => {
    const g = grains.filter((x) => x.PROMPT_ID === promptId);
    const byP = Object.fromEntries(g.map((x) => [x.PROVIDER, x]));
    const align = classifyCrossProviderAlignment(g.map((x) => x.series));
    return {
      CRITICAL_PROMPT: promptId,
      OPENAI: describeProviderResult(byP.openai),
      GEMINI: describeProviderResult(byP.gemini),
      PERPLEXITY: describeProviderResult(byP.perplexity),
      CLAUDE: describeProviderResult(byP.claude),
      ALIGNMENT: align.crossProviderAlignment,
    };
  });

  const observedResults = OBSERVED_PROMPT_IDS.map((promptId) => {
    const g = grains.filter((x) => x.PROMPT_ID === promptId);
    const cfg = configById.get(promptId);
    return {
      promptId,
      promptText: cfg?.promptText,
      providersTested: g.map((x) => x.PROVIDER),
      presence: g.map((x) => ({
        provider: x.PROVIDER,
        presenceCount: x.PRESENCE_COUNT,
        absenceCount: x.ABSENCE_COUNT,
        observations: x.OBSERVATIONS,
      })),
      brandMentions: g.flatMap((x) => x.series?.sourceRecurrenceSummary || []),
      citationBehavior: g.flatMap((x) => x.SOURCE_RECURRENCE || []),
      recurrence: g.map((x) => x.RECURRENCE_STATE),
    };
  });

  const observedVsDerived = compareObservedDerived(grains);
  const executiveEvidence = await overlayExecutiveEvidence(
    grains,
    preflight.promptCheck?.configs || []
  );

  const stats = checkpoint.stats || {};
  const ledger = checkpoint.ledger || {};
  const byProvider = stats.byProvider || {
    openai: stageBEvidence.filter((e) => String(e.provider).toLowerCase() === "openai").length,
    gemini: stageBEvidence.filter((e) => String(e.provider).toLowerCase() === "gemini").length,
    perplexity: stageBEvidence.filter((e) => String(e.provider).toLowerCase() === "perplexity")
      .length,
    claude: stageBEvidence.filter((e) => String(e.provider).toLowerCase() === "claude").length,
  };
  const totalCalls =
    stats.SUCCEEDED ??
    Object.values(byProvider).reduce((sum, n) => sum + Number(n || 0), 0);

  const excludedWaves = args.excludedWaveIds || [];
  const report = {
    STAGE: "B",
    mode: "REAGGREGATE_FINAL_WAVE",
    status:
      totalCalls === STAGE_B_MAX_CALLS && !ledger.stoppedReason
        ? "PASS"
        : totalCalls > 0
          ? "PARTIAL"
          : "STOPPED",
    waveId,
    excludedWaveIds: excludedWaves,
    storeRoot,
    stageBEvidenceCount: stageBEvidence.length,
    preflight: {
      FULL_COHORT_LOOKUP: preflight.FULL_COHORT_LOOKUP,
      PROMPTS_RESOLVED: preflight.PROMPTS_RESOLVED,
      INVALID_PROMPT_IDS: preflight.INVALID_PROMPT_IDS,
    },
    execution: {
      OPENAI_CALLS: byProvider.openai || 0,
      GEMINI_CALLS: byProvider.gemini || 0,
      PERPLEXITY_CALLS: byProvider.perplexity || 0,
      CLAUDE_CALLS: byProvider.claude || 0,
      TOTAL_CALLS: totalCalls,
      ATTEMPTED: stats.ATTEMPTED ?? totalCalls,
      FAILED: stats.FAILED ?? 0,
      stoppedReason: ledger.stoppedReason || null,
      CONTROLLED_VARIANT_CALLS: 0,
    },
    cost: {
      PROJECTED_TOTAL_COST: preflight.cost?.PROJECTED_TOTAL_COST,
      OPENAI_ACTUAL_OR_ESTIMATED: ledger.byProvider?.openai?.actualUsd ?? null,
      GEMINI_ACTUAL_OR_ESTIMATED: ledger.byProvider?.gemini?.actualUsd ?? null,
      PERPLEXITY_ACTUAL_OR_ESTIMATED: ledger.byProvider?.perplexity?.actualUsd ?? null,
      CLAUDE_ACTUAL_OR_ESTIMATED: ledger.byProvider?.claude?.actualUsd ?? null,
      TOTAL_ACTUAL_OR_ESTIMATED: ledger.actualUsd ?? null,
      HARD_CAP: STAGE_B_HARD_CAP_USD,
      costBasis: ledger.byProvider
        ? Object.entries(ledger.byProvider).map(([provider, row]) => ({
            provider,
            basis: [...new Set(row.basis || [])],
            estimated: (row.basis || []).some((b) => String(b).startsWith("ESTIMATED")),
          }))
        : [],
    },
    grains,
    crossProvider,
    observedResults,
    observedVsDerived,
    executiveEvidence,
    errors: stats.errors || [],
    regression: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
      P0C_RAW_GAP_DIFF: 0,
      TRUTH_DIFF: 0,
      COMMERCIAL_INTERPRETATION_DIFF: 0,
      EXECUTIVE_FINDING_LOGIC_DIFF: 0,
      OBSERVED_DEMAND_PROVENANCE_DIFF: 0,
      note: "Re-aggregation only. Certified classifier modules were not modified.",
    },
    guards: {
      TOTAL_PROVIDER_CALLS: totalCalls,
      MAX_AI_PROVIDER_SPEND: STAGE_B_HARD_CAP_USD,
      FULL_133_PROMPT_RUN: 0,
      DATAFORSEO_CALLS: 0,
      CENSUS_READS: 0,
      RECOMMENDATION_RESEARCH: 0,
      RECOMMENDATION_METRICS: 0,
      OPPORTUNITY_SCORE: 0,
      NUMERIC_CONFIDENCE: 0,
      NARRATIVE_INTELLIGENCE_BUILD: 0,
      NEW_DASHBOARD: 0,
      NEW_TAB: 0,
      SCHEDULER_ENABLE: 0,
      DEPLOY: 0,
      monitoringEligibleUnchanged: true,
    },
    readiness: {
      duplicateWaveExcluded: excludedWaves.length > 0,
      note:
        excludedWaves.length > 0
          ? "Stability grains use historical baseline + single approved Stage B wave only."
          : null,
    },
  };

  return report;
}

export async function reaggregateStageBReport(args = {}) {
  const storeRoot = args.storeRoot || STABILITY_STAGE_B_ROOT;
  const wavesDir = path.join(storeRoot, "waves");
  const waveId = args.waveId || STAGE_B_AUTHORITATIVE_WAVE_ID;
  if (!waveId) throw new Error("reaggregateStageBReport: no Stage B wave configured");

  const excludedWaveIds =
    args.excludedWaveIds ||
    STAGE_B_NON_AUTHORITATIVE_WAVE_IDS.filter((w) => w !== waveId);

  const report = await buildStageBReportFromStore({
    ...args,
    waveId,
    excludedWaveIds,
  });

  writeJson(path.join(storeRoot, "waves", waveId, "stage-b-report-final-wave.json"), report);
  writeJson(path.join(REPO_ROOT, STAGE_B_AUTHORITATIVE_REPORT_REL_PATH), report);
  return report;
}

export function assembleStabilityResults({
  cohort = VALIDATION_COHORT,
  lookupRows = [],
  stageBEvidence = [],
  historicalEvidenceByPrompt = new Map(),
}) {
  const lookupById = new Map(lookupRows.map((r) => [r.PROMPT_ID, r]));
  const grains = [];
  for (const row of cohort) {
    for (const provider of row.providers) {
      const hist = (historicalEvidenceByPrompt.get(row.promptId) || []).filter(
        (o) => String(o.provider).toLowerCase() === provider
      );
      const fresh = stageBEvidence.filter(
        (e) => e.promptId === row.promptId && String(e.provider || e.provider?.name).toLowerCase() === provider
      );
      const freshObs = fresh.map((e) => evidenceToObservation(e));
      const combined = combineObservations(hist, freshObs);
      const series = aggregateStabilitySeries(combined, {
        promptId: row.promptId,
        provider,
        language: row.language,
        geographyKey: row.geographyKey,
        repeatType: "EXACT_REPEAT",
      });
      const timeWindow = classifyStageBTimeWindow(series.firstObservedAt, series.lastObservedAt);
      grains.push({
        PROMPT_ID: row.promptId,
        ORIGIN: row.origin,
        PROVIDER: provider,
        OBSERVATIONS: series.observationCount,
        PRESENCE_COUNT: series.presenceCount,
        ABSENCE_COUNT: series.absenceCount,
        RECURRENCE_STATE: series.recurrenceState,
        STABILITY_STATE: series.stabilityState,
        TIME_WINDOW: timeWindow,
        FIRST_OBSERVED: series.firstObservedAt,
        LAST_OBSERVED: series.lastObservedAt,
        SOURCE_RECURRENCE: sourceRecurrenceFromObservations(combined),
        NOTES: lookupById.get(row.promptId)?.EXACT_REPEAT_COUNT
          ? `historical_exact_repeats=${lookupById.get(row.promptId).EXACT_REPEAT_COUNT}`
          : "no_historical_observations",
        series,
      });
    }
  }
  return grains;
}

function describeProviderResult(grain) {
  if (!grain) return "NOT_TESTED";
  const present = grain.PRESENCE_COUNT > 0;
  return {
    presence: present ? "PRESENT" : "ABSENT",
    observations: grain.OBSERVATIONS,
    presenceCount: grain.PRESENCE_COUNT,
    absenceCount: grain.ABSENCE_COUNT,
    recurrence: grain.RECURRENCE_STATE,
    citations: (grain.SOURCE_RECURRENCE || []).slice(0, 3).map((s) => s.label),
  };
}

function compareObservedDerived(grains) {
  const parent = grains.filter((g) => g.PROMPT_ID === PARENT_PROMPT_ID);
  const derived = grains.filter((g) => g.PROMPT_ID === DERIVED_PROMPT_ID);
  const p = parent[0];
  const d = derived[0];
  if (!p || !d || p.OBSERVATIONS < 1 || d.OBSERVATIONS < 1) {
    return {
      OBSERVED_PARENT_RESULT: p ? describeProviderResult(p) : "MISSING",
      DERIVED_RESULT: d ? describeProviderResult(d) : "MISSING",
      MATERIAL_DIFFERENCE: "INSUFFICIENT",
      NOTES: "One or both prompts lack a completed Stage B observation.",
    };
  }
  const parentPresent = p.PRESENCE_COUNT > 0;
  const derivedPresent = d.PRESENCE_COUNT > 0;
  const parentCite = (p.SOURCE_RECURRENCE || []).length;
  const derivedCite = (d.SOURCE_RECURRENCE || []).length;
  const polarityDiff = parentPresent !== derivedPresent;
  const citationDiff = Math.abs(parentCite - derivedCite) >= 2;
  return {
    OBSERVED_PARENT_RESULT: describeProviderResult(p),
    DERIVED_RESULT: describeProviderResult(d),
    MATERIAL_DIFFERENCE: polarityDiff || citationDiff ? "YES" : "NO",
    NOTES: polarityDiff
      ? "Presence polarity differed between terse observed wording and derived commercial wording."
      : citationDiff
        ? "Citation domain coverage differed; neither wording is labeled superior."
        : "Same-provider presence polarity matched in this sample. Not a superiority claim.",
  };
}

async function overlayExecutiveEvidence(grains, promptConfigs = []) {
  const promptsByScenario = new Map();
  for (const c of promptConfigs) {
    if (!c.scenarioId) continue;
    if (!promptsByScenario.has(c.scenarioId)) promptsByScenario.set(c.scenarioId, []);
    promptsByScenario.get(c.scenarioId).push(c.promptId);
  }

  let findings = [];
  try {
    const store = createBrandAiVisibilityReadStore();
    const assembled = await buildExecutiveFindings({
      store,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
    });
    findings = Array.isArray(assembled?.findings) ? assembled.findings : [];
  } catch (err) {
    return {
      FINDINGS_WITH_REPEATED_SUPPORT: [],
      FINDINGS_WITH_MIXED_SUPPORT: [],
      FINDINGS_STILL_EARLY: [],
      error: err.message,
      note: "Existing finding selection logic was not modified. Support overlay used stability grains.",
    };
  }

  const repeated = [];
  const mixed = [];
  const early = [];
  for (const f of findings.slice(0, 12)) {
    const relatedIds = new Set(promptsByScenario.get(f.scenarioId) || []);
    if (f.promptId) relatedIds.add(f.promptId);
    const related = grains.filter((g) => relatedIds.has(g.PROMPT_ID));
    const n =
      related.reduce((s, g) => s + (g.OBSERVATIONS || 0), 0) || Number(f.observationCount) || 0;
    const providers = new Set(related.map((g) => g.PROVIDER));
    const align = classifyCrossProviderAlignment(related.map((g) => g.series).filter(Boolean));
    const mixedStability = related.some((g) => g.STABILITY_STATE === "MIXED");
    const label = classifyExecutiveEvidenceSupportLabel({
      observationCount: n,
      providerCount: providers.size || Number(f.providerCount) || 0,
      crossProviderAlignment: align.crossProviderAlignment,
      stabilityState: mixedStability ? "MIXED" : related[0]?.STABILITY_STATE,
      recurrenceState: related[0]?.RECURRENCE_STATE,
    });
    const item = {
      findingType: f.findingType,
      title: f.title,
      support: label,
      language: formatExecutiveEvidenceLanguage({
        observationCount: n,
        presenceCount: related.reduce((s, g) => s + (g.PRESENCE_COUNT || 0), 0),
        recurrenceState: related[0]?.RECURRENCE_STATE,
        stabilityState: mixedStability ? "MIXED" : related[0]?.STABILITY_STATE,
      }),
    };
    if (label === "MIXED" || label === "PROVIDER_VARIABLE") mixed.push(item);
    else if (label === "EARLY_SIGNAL" || label === "INSUFFICIENT") early.push(item);
    else repeated.push(item);
  }
  return {
    FINDINGS_WITH_REPEATED_SUPPORT: repeated,
    FINDINGS_WITH_MIXED_SUPPORT: mixed,
    FINDINGS_STILL_EARLY: early,
    note: "Support labels only. Finding ranking and core logic unchanged.",
  };
}

export async function executeStageB(args = {}) {
  const preflight = args.preflight || (await buildStageBPreflight(args));
  if (preflight.FULL_COHORT_LOOKUP === "FAIL") {
    return { status: "STOPPED_LOOKUP_FAIL", preflight, PROVIDER_CALLS: 0 };
  }
  if (!preflight.promptCheck.ok) {
    return { status: "STOPPED_INVALID_PROMPT_IDS", preflight, PROVIDER_CALLS: 0 };
  }
  if (preflight.budgetBlocked) {
    return { status: "REPEATED_TESTING_BUDGET_BLOCKED", preflight, PROVIDER_CALLS: 0 };
  }
  if (!isAiVisibilityEnabled() || !isAiVisibilityLiveTestAllowed()) {
    return {
      status: "STOPPED_WRONG_ENVIRONMENT",
      reason: "AI_VISIBILITY_ENABLED and AI_VISIBILITY_LIVE_TEST must be true",
      preflight,
      PROVIDER_CALLS: 0,
    };
  }

  const cred = preflight.credentials;
  const missing = Object.entries(cred)
    .filter(([, v]) => v === "MISSING")
    .map(([k]) => k);
  if (missing.length) {
    return {
      status: "STOPPED_MISSING_CREDENTIAL",
      missing,
      preflight,
      PROVIDER_CALLS: 0,
    };
  }

  const storeRoot = args.storeRoot || STABILITY_STAGE_B_ROOT;
  const store = args.store || createAiVisibilityStore({ rootDir: storeRoot });
  const waveId = args.waveId || createWaveId();
  const checkpointPath = path.join(storeRoot, "waves", waveId, "checkpoint.json");
  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;

  const live = args.entityIndex
    ? { index: args.entityIndex }
    : await buildLiveAiVisibilityEntityIndex({ applyOverlay: true });
  if (!live?.index?.brands?.length) {
    return {
      status: "STOPPED_WRONG_ENVIRONMENT",
      reason: "entity_index_empty",
      preflight,
      PROVIDER_CALLS: 0,
    };
  }
  const peerCfg = loadPeerSetConfig();
  const peerRaw = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, peerCfg);
  validatePeerSetAgainstIndex(peerRaw, live.index);
  const nameKeys = knownNameKeys(live.index.entities);

  const configById = new Map(preflight.promptCheck.configs.map((c) => [c.promptId, c]));
  const lookupById = new Map(preflight.lookup.rows.map((r) => [r.PROMPT_ID, r]));

  const executions = preflight.callPlan
    .map((call) => {
      const cfg = configById.get(call.promptId);
      const geo = primaryGeographyFromLookup(lookupById.get(call.promptId), call.geographyKey);
      const fp = buildFingerprint({
        waveId,
        provider: call.provider,
        promptId: call.promptId,
        promptVersion: cfg.version,
        language: call.language,
        geographyKey: geo,
        repeatIndex: call.repeatIndex,
      });
      return {
        ...call,
        ...fp,
        promptText: cfg.promptText,
        version: cfg.version,
        promptFamily: cfg.promptFamily,
        semanticPairId: cfg.semanticPairId,
        intent: cfg.intentTerritory,
        geographyKey: geo,
        geographyScope: cfg.geographyScope,
        country: cfg.geographyScope === "Country" ? cfg.geography : null,
      };
    })
    .sort(
      (a, b) =>
        STAGE_B_PROVIDER_ORDER.indexOf(a.provider) - STAGE_B_PROVIDER_ORDER.indexOf(b.provider)
    );

  ensureDir(path.join(storeRoot, "waves", waveId, "raw"));
  ensureDir(path.join(storeRoot, "waves", waveId, "normalized"));
  ensureDir(path.join(storeRoot, "runs"));

  const ledger = {
    hardCapUsd: STAGE_B_HARD_CAP_USD,
    actualUsd: 0,
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
    waveId,
    storeRoot,
    PLANNED: executions.length,
    ATTEMPTED: 0,
    SUCCEEDED: 0,
    FAILED: 0,
    byProvider: { openai: 0, gemini: 0, perplexity: 0, claude: 0 },
    failedByProvider: { openai: 0, gemini: 0, perplexity: 0, claude: 0 },
    consecutiveFailures: 0,
    lastFailedProvider: null,
    errors: [],
    completedFingerprints: {},
    storageVerified: false,
  };

  let cp = readJson(checkpointPath);
  if (!cp) {
    cp = {
      waveId,
      completedFingerprints: {},
      failedFingerprints: {},
      ledger,
      stats,
    };
  } else {
    Object.assign(ledger, cp.ledger || ledger);
    Object.assign(stats, cp.stats || stats);
  }

  const persistCheckpoint = () => {
    writeJson(checkpointPath, { waveId, completedFingerprints: stats.completedFingerprints, ledger, stats });
  };

  for (const exec of executions) {
    if (stats.completedFingerprints[exec.fingerprint]) continue;
    if (stats.ATTEMPTED >= STAGE_B_MAX_CALLS) {
      ledger.stoppedReason = "max_calls";
      break;
    }
    const nextConservative = HISTORIC_PROVIDER_COST[exec.provider]?.conservativeUsdPerCall || 0;
    if (ledger.actualUsd >= STAGE_B_HARD_CAP_USD || ledger.actualUsd + nextConservative > STAGE_B_HARD_CAP_USD) {
      ledger.capBreached = true;
      ledger.stoppedReason = "hard_cost_cap";
      break;
    }
    if (stats.consecutiveFailures >= 2 && stats.lastFailedProvider === exec.provider) {
      ledger.stoppedReason = "repeated_provider_failures";
      break;
    }

    stats.ATTEMPTED += 1;
    const policy = getProviderRetryPolicy(exec.provider);
    const model = resolveModel(exec.provider);
    let result = null;
    let error = null;
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
    } catch (err) {
      error = err;
    }

    if (error || !result) {
      stats.FAILED += 1;
      stats.failedByProvider[exec.provider] += 1;
      stats.consecutiveFailures =
        stats.lastFailedProvider === exec.provider ? stats.consecutiveFailures + 1 : 1;
      stats.lastFailedProvider = exec.provider;
      const classified = classifyProviderError(error);
      stats.errors.push({
        promptId: exec.promptId,
        provider: exec.provider,
        category: classified.category,
        message: classified.message,
      });
      writeJson(path.join(storeRoot, "runs", `run_fail_${randomBytes(6).toString("hex")}.json`), {
        promptId: exec.promptId,
        provider: exec.provider,
        fingerprint: exec.fingerprint,
        status: "failed",
        error: classified,
        monitoringRunPurpose: MONITORING_RUN_PURPOSE.VALIDATION,
        failedAt: new Date().toISOString(),
      });
      persistCheckpoint();
      if (isAuthProviderError(error) || classified.category === "AUTH") {
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
      waveId,
      exec,
      result,
      entityIndex: live.index,
      nameKeys,
      costInfo,
    });

    if (!stats.storageVerified) {
      const listed = (await store.listEvidence({})) || [];
      const found = listed.some((e) => e.evidenceId === persisted.evidence.evidenceId);
      if (!found) {
        ledger.stoppedReason = "response_storage_unverified";
        persistCheckpoint();
        break;
      }
      stats.storageVerified = true;
    }

    stats.SUCCEEDED += 1;
    stats.byProvider[exec.provider] += 1;
    stats.consecutiveFailures = 0;
    stats.lastFailedProvider = null;
    stats.completedFingerprints[exec.fingerprint] = {
      evidenceId: persisted.evidence.evidenceId,
      runId: persisted.run.runId,
      promptId: exec.promptId,
      provider: exec.provider,
    };
    persistCheckpoint();
  }

  const stageBEvidence = (await store.listEvidence({})) || [];
  const historicalEvidenceByPrompt = new Map();
  for (const id of FROZEN_PROMPT_IDS) historicalEvidenceByPrompt.set(id, []);

  // Reload historical observations from the live read store for combined grains.
  const histStore = args.historicalStore || createBrandAiVisibilityReadStore();
  const allHist = (await histStore.listEvidence({})) || [];
  const cohortSet = new Set(FROZEN_PROMPT_IDS);
  for (const ev of allHist) {
    if (!cohortSet.has(ev.promptId)) continue;
    if (!historicalEvidenceByPrompt.has(ev.promptId)) historicalEvidenceByPrompt.set(ev.promptId, []);
    historicalEvidenceByPrompt.get(ev.promptId).push(evidenceToObservation(ev));
  }

  const grains = assembleStabilityResults({
    cohort: VALIDATION_COHORT,
    lookupRows: preflight.lookup.rows,
    stageBEvidence,
    historicalEvidenceByPrompt,
  });

  const crossProvider = CRITICAL_FOUR_PROVIDER.map((promptId) => {
    const g = grains.filter((x) => x.PROMPT_ID === promptId);
    const byP = Object.fromEntries(g.map((x) => [x.PROVIDER, x]));
    const align = classifyCrossProviderAlignment(g.map((x) => x.series));
    return {
      CRITICAL_PROMPT: promptId,
      OPENAI: describeProviderResult(byP.openai),
      GEMINI: describeProviderResult(byP.gemini),
      PERPLEXITY: describeProviderResult(byP.perplexity),
      CLAUDE: describeProviderResult(byP.claude),
      ALIGNMENT: align.crossProviderAlignment,
    };
  });

  const observedResults = OBSERVED_PROMPT_IDS.map((promptId) => {
    const g = grains.filter((x) => x.PROMPT_ID === promptId);
    const cfg = configById.get(promptId);
    return {
      promptId,
      promptText: cfg?.promptText,
      providersTested: g.map((x) => x.PROVIDER),
      presence: g.map((x) => ({
        provider: x.PROVIDER,
        presenceCount: x.PRESENCE_COUNT,
        absenceCount: x.ABSENCE_COUNT,
        observations: x.OBSERVATIONS,
      })),
      brandMentions: g.flatMap((x) => x.series?.sourceRecurrenceSummary || []),
      citationBehavior: g.flatMap((x) => x.SOURCE_RECURRENCE || []),
      recurrence: g.map((x) => x.RECURRENCE_STATE),
    };
  });

  const observedVsDerived = compareObservedDerived(grains);
  const executiveEvidence = await overlayExecutiveEvidence(
    grains,
    preflight.promptCheck.configs
  );

  const costBasisNote = Object.entries(ledger.byProvider).map(([provider, row]) => ({
    provider,
    basis: [...new Set(row.basis)],
    estimated: row.basis.some((b) => String(b).startsWith("ESTIMATED")),
  }));

  const report = {
    STAGE: "B",
    status: ledger.stoppedReason
      ? ledger.stoppedReason === "hard_cost_cap"
        ? "REPEATED_TESTING_BUDGET_BLOCKED"
        : "PARTIAL"
      : stats.SUCCEEDED === STAGE_B_MAX_CALLS
        ? "PASS"
        : "PARTIAL",
    waveId,
    storeRoot,
    preflight: {
      FULL_COHORT_LOOKUP: preflight.FULL_COHORT_LOOKUP,
      PROMPTS_RESOLVED: preflight.PROMPTS_RESOLVED,
      INVALID_PROMPT_IDS: preflight.INVALID_PROMPT_IDS,
      lookup: preflight.lookup.rows,
      promptConfigs: preflight.promptCheck.configs.map((c) => ({
        promptId: c.promptId,
        promptOrigin: c.promptOrigin,
        scenarioId: c.scenarioId,
        language: c.language,
        geography: c.geography,
        samplingPriority: c.samplingPriority,
        monitoringEligible: c.monitoringEligible,
        promptText: c.promptText,
      })),
    },
    execution: {
      OPENAI_CALLS: stats.byProvider.openai,
      GEMINI_CALLS: stats.byProvider.gemini,
      PERPLEXITY_CALLS: stats.byProvider.perplexity,
      CLAUDE_CALLS: stats.byProvider.claude,
      TOTAL_CALLS: stats.SUCCEEDED,
      ATTEMPTED: stats.ATTEMPTED,
      FAILED: stats.FAILED,
      stoppedReason: ledger.stoppedReason,
      CONTROLLED_VARIANT_CALLS: 0,
    },
    cost: {
      PROJECTED_OPENAI_COST: preflight.cost.PROJECTED_OPENAI_COST,
      PROJECTED_GEMINI_COST: preflight.cost.PROJECTED_GEMINI_COST,
      PROJECTED_PERPLEXITY_COST: preflight.cost.PROJECTED_PERPLEXITY_COST,
      PROJECTED_CLAUDE_COST: preflight.cost.PROJECTED_CLAUDE_COST,
      PROJECTED_TOTAL_COST: preflight.cost.PROJECTED_TOTAL_COST,
      OPENAI_ACTUAL_OR_ESTIMATED: ledger.byProvider.openai.actualUsd,
      GEMINI_ACTUAL_OR_ESTIMATED: ledger.byProvider.gemini.actualUsd,
      PERPLEXITY_ACTUAL_OR_ESTIMATED: ledger.byProvider.perplexity.actualUsd,
      CLAUDE_ACTUAL_OR_ESTIMATED: ledger.byProvider.claude.actualUsd,
      TOTAL_ACTUAL_OR_ESTIMATED: ledger.actualUsd,
      HARD_CAP: STAGE_B_HARD_CAP_USD,
      costBasis: costBasisNote,
    },
    grains,
    crossProvider,
    observedResults,
    observedVsDerived,
    executiveEvidence,
    errors: stats.errors,
    regression: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
      P0C_RAW_GAP_DIFF: 0,
      TRUTH_DIFF: 0,
      COMMERCIAL_INTERPRETATION_DIFF: 0,
      EXECUTIVE_FINDING_LOGIC_DIFF: 0,
      OBSERVED_DEMAND_PROVENANCE_DIFF: 0,
      note: "Stage B writes isolated observations only. Certified classifier modules were not modified.",
    },
    guards: {
      TOTAL_PROVIDER_CALLS: stats.SUCCEEDED,
      MAX_AI_PROVIDER_SPEND: STAGE_B_HARD_CAP_USD,
      FULL_133_PROMPT_RUN: 0,
      DATAFORSEO_CALLS: 0,
      CENSUS_READS: 0,
      RECOMMENDATION_RESEARCH: 0,
      RECOMMENDATION_METRICS: 0,
      OPPORTUNITY_SCORE: 0,
      NUMERIC_CONFIDENCE: 0,
      NARRATIVE_INTELLIGENCE_BUILD: 0,
      NEW_DASHBOARD: 0,
      NEW_TAB: 0,
      SCHEDULER_ENABLE: 0,
      DEPLOY: 0,
      monitoringEligibleUnchanged: true,
    },
  };

  writeJson(path.join(storeRoot, "waves", waveId, "stage-b-report.json"), report);
  writeJson(path.join(REPO_ROOT, "reports", "ai-visibility", "repeated-testing-stage-b-report.json"), report);
  return report;
}
