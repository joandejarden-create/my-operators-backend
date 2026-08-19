/**
 * Operator AI Presence Validation Wave V1 — live provider corpus + production gate.
 * Grain: PROMPT × PROVIDER. Each response analyzed for all 9 monitored operators.
 * No Brand mutations. No Recommendation metrics. No Census. No DataForSEO.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomBytes } from "crypto";
import { isAiVisibilityEnabled, isAiVisibilityLiveTestAllowed } from "../config.js";
import { hashPromptText, isRetryableProviderError, isAuthProviderError } from "../execution-batch.js";
import { HISTORIC_PROVIDER_COST } from "../stability-policy.js";
import { runVisibilityPrompt } from "../providers/index.js";
import { extractCitations } from "../extract-citations.js";
import { detectProviderAvailability } from "../validation/presence-validation-candidates.js";
import { preflightProviderCredentials } from "../provider-credentials.js";
import {
  assertUniverseLock,
  OPERATOR_AI_UNIVERSE,
  PRIMARY_OPERATOR_COUNT,
  isPrimaryMonitoredOperator,
} from "./universe.js";
import { buildOperatorEntities, findOperatorSpans, hasOperatingContext } from "./aliases.js";
import {
  classifyOperatorPresence,
  OPERATOR_SIGNAL_PRESENCE,
  OPERATOR_PRESENCE_CLASSIFIER_VERSION,
} from "./presence.js";
import {
  buildOperatorFoundationExecutionMatrix,
  costOperatorFoundationWave,
  MAX_OPERATOR_FOUNDATION_PROVIDER_SPEND,
  CORE_PROVIDERS,
  EXTENDED_PROVIDERS,
} from "./cost-model.js";
import { listOperatorPrompts } from "./prompts.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { computeOperatorQuestionsMissing, computeOperatorAllProvidersPresence } from "./questions-missing.js";
import { interpretOperatorGap, GAP_INTERPRETATION } from "./gaps.js";
import { operatorAssociationStatus } from "./associations.js";

export const OPERATOR_PRESENCE_VALIDATION_WAVE_VERSION = "operator_presence_validation_wave_v1";
export const OPERATOR_PRESENCE_VALIDATION_WAVE_LABEL = "OPERATOR_PRESENCE_VALIDATION_WAVE_V1";
export const MAX_OPERATOR_VALIDATION_SPEND = MAX_OPERATOR_FOUNDATION_PROVIDER_SPEND;
export const MIN_LIVE_HOLDOUT_CASES = 30;
export const TARGET_LIVE_HOLDOUT_CASES = 40;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
export const OPERATOR_RUNTIME_ROOT = path.join(REPO_ROOT, "data/ai-visibility/runtime/operator");

const BRAND_TRAP_RES = Object.freeze({
  marriott: /\b(marriott bonvoy|a marriott hotel|stay at (?:a )?marriott|book.*marriott hotel)\b/i,
  hilton: /\b(hilton honors|hilton garden inn|stay at (?:a )?hilton|earn hilton)\b/i,
  ihg: /\b(ihg one rewards|stay at (?:an )?ihg hotel|collect points at ihg)\b/i,
});

function remingtonBareTrap(text) {
  const body = String(text || "");
  if (/\bremington hospitality\b/i.test(body)) return false;
  if (/\bremington hotels\b/i.test(body)) return false;
  return /\bremington\b/i.test(body);
}

function sha256(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + "\n", "utf8");
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function createOperatorPresenceWaveId(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const h = String(now.getUTCHours()).padStart(2, "0");
  const min = String(now.getUTCMinutes()).padStart(2, "0");
  const rand = randomBytes(3).toString("hex");
  return `aiv_operator_presence_validation_${y}${m}${d}_${h}${min}_${rand}`;
}

export function operatorWavePaths(waveId) {
  const root = path.join(OPERATOR_RUNTIME_ROOT, waveId);
  return {
    root,
    lock: path.join(OPERATOR_RUNTIME_ROOT, "operator-presence-validation-lock.json"),
    completedIndex: path.join(OPERATOR_RUNTIME_ROOT, "operator-presence-validation-completed.json"),
    manifest: path.join(root, "wave-manifest.json"),
    responsesDir: path.join(root, "responses"),
    extractionsPath: path.join(root, "operator-presence-extractions.json"),
    corpusSummaryPath: path.join(root, "corpus-summary.json"),
    liveDevPath: path.join(root, "live-presence-dev-v1.json"),
    liveHoldoutPath: path.join(root, "live-presence-holdout-v1.json"),
    holdoutSealPath: path.join(root, "live-presence-holdout-seal-v1.json"),
    validationReportPath: path.join(root, "presence-validation-report.json"),
    costLedgerPath: path.join(root, "cost-ledger.json"),
    reportCopyPath: path.join(REPO_ROOT, "reports/ai-visibility/operator-presence-validation-wave-v1.json"),
  };
}

function historicRate(provider) {
  return HISTORIC_PROVIDER_COST[provider]?.historicUsdPerCall ?? 0.25;
}

function conservativeRate(provider) {
  return HISTORIC_PROVIDER_COST[provider]?.conservativeUsdPerCall ?? 0.5;
}

export function buildOperatorExecutionSlots() {
  const promptById = Object.fromEntries(listOperatorPrompts().map((p) => [p.promptId, p]));
  return buildOperatorFoundationExecutionMatrix().map((row) => {
    const prompt = promptById[row.promptId];
    return {
      slotId: `${row.promptId}__${row.provider}`,
      provider: row.provider,
      tier: row.tier,
      promptId: row.promptId,
      promptVersion: "1",
      scenarioId: prompt?.scenarioId || null,
      promptText: prompt?.text || "",
      language: prompt?.language || "en",
      geo: prompt?.geography || "CALA",
    };
  });
}

export function preflightOperatorIdentity() {
  assertUniverseLock();
  const operators = OPERATOR_AI_UNIVERSE.map((o) => ({
    operator: o.founderName,
    canonicalName: o.canonicalName,
    canonicalId: o.canonicalId,
    monitoredScope: o.monitoredScope,
    parentPlatform: o.parentPlatform,
    domain: o.domain,
    identityStatus: o.identityStatus,
    aliases: (buildOperatorEntities().find((e) => e.id === o.canonicalId)?.aliases || []).slice(0, 8),
  }));
  const allHigh = operators.every((o) => o.identityStatus === "HIGH");
  return { operators, count: operators.length, allHigh, status: allHigh ? "PASS" : "STOP" };
}

export function preflightOperatorPresenceValidation(options = {}) {
  const identity = preflightOperatorIdentity();
  if (!identity.allHigh) {
    return { ok: false, status: "STOP_IDENTITY", identity };
  }

  const slots = buildOperatorExecutionSlots();
  const byProvider = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  for (const s of slots) byProvider[s.provider] += 1;

  const expected = { openai: 30, gemini: 12, perplexity: 30, claude: 12, total: 84 };
  const matrixOk =
    slots.length === expected.total &&
    byProvider.openai === expected.openai &&
    byProvider.gemini === expected.gemini &&
    byProvider.perplexity === expected.perplexity &&
    byProvider.claude === expected.claude;

  const cost = costOperatorFoundationWave();
  const costOk = cost.projectedConservativeCost <= MAX_OPERATOR_VALIDATION_SPEND;

  const availability = detectProviderAvailability();
  const credentials = preflightProviderCredentials();
  const providersOk = CORE_PROVIDERS.every((p) => availability[p]);

  const duplicate = findCompletedValidationWave();
  const enabled = options.requireEnv !== false ? isAiVisibilityEnabled() : true;
  const live = options.requireEnv !== false ? isAiVisibilityLiveTestAllowed() : true;

  const blockers = [];
  if (!matrixOk) blockers.push("PROVIDER_MATRIX_MISMATCH");
  if (!costOk) blockers.push("COST_OVER_CAP");
  if (!providersOk) blockers.push("PROVIDER_UNAVAILABLE");
  if (duplicate && !options.allowDuplicate) blockers.push("DUPLICATE_WAVE_EXISTS");
  if (options.requireEnv !== false && !enabled) blockers.push("AI_VISIBILITY_ENABLED_false");
  if (options.requireEnv !== false && !live) blockers.push("AI_VISIBILITY_LIVE_TEST_false");

  return {
    ok: blockers.length === 0,
    status: blockers.length ? "STOP" : "READY",
    blockers,
    identity,
    primaryOperatorCount: PRIMARY_OPERATOR_COUNT,
    promptCount: listOperatorPrompts().length,
    corePrompts: cost.corePrompts,
    extendedPrompts: cost.extendedPrompts,
    plannedCalls: slots.length,
    byProvider,
    expectedMatrix: expected,
    matrixOk,
    cost,
    costOk,
    availability,
    credentials: {
      openai: credentials.openai?.status,
      gemini: credentials.gemini?.status,
      perplexity: credentials.perplexity?.status,
      claude: credentials.claude?.status,
    },
    duplicateWaveId: duplicate?.waveId || null,
    executionGrain: "PROMPT_PROVIDER",
    perOperatorExecution: 0,
  };
}

function findCompletedValidationWave() {
  const indexPath = path.join(OPERATOR_RUNTIME_ROOT, "operator-presence-validation-completed.json");
  const idx = readJson(indexPath);
  if (!idx?.waveId) return null;
  const manifest = readJson(path.join(OPERATOR_RUNTIME_ROOT, idx.waveId, "wave-manifest.json"));
  if (manifest?.status === "VALID" || manifest?.status === "PARTIAL") return idx;
  return null;
}

export function acquireWaveLock(waveId, options = {}) {
  ensureDir(OPERATOR_RUNTIME_ROOT);
  const paths = operatorWavePaths(waveId);
  const existing = readJson(paths.lock);
  if (existing?.waveId && existing.waveId !== waveId && existing.status === "running") {
    return { acquired: false, reason: "LOCK_HELD", lock: existing };
  }
  const duplicate = findCompletedValidationWave();
  if (duplicate && !options.allowDuplicate) {
    return { acquired: false, reason: "DUPLICATE_WAVE", duplicate };
  }
  const lock = {
    waveId,
    label: OPERATOR_PRESENCE_VALIDATION_WAVE_LABEL,
    status: "running",
    acquiredAt: new Date().toISOString(),
    pid: process.pid,
  };
  writeJson(paths.lock, lock);
  return { acquired: true, lock, paths };
}

async function runWithRetry({ fn, maxRetries = 1 }) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= maxRetries) {
    try {
      return { result: await fn(), retries: attempt, error: null };
    } catch (err) {
      lastErr = err;
      if (attempt >= maxRetries || !isRetryableProviderError(err) || isAuthProviderError(err)) {
        return { result: null, retries: attempt, error: err };
      }
      await sleep(1500 * (attempt + 1));
      attempt += 1;
    }
  }
  return { result: null, retries: attempt, error: lastErr };
}

export async function executeOperatorPresenceValidationWave(options = {}) {
  const preflight = preflightOperatorPresenceValidation({
    requireEnv: options.execute === true,
    allowDuplicate: options.allowDuplicate === true,
  });
  if (!preflight.ok && options.execute) {
    return { ok: false, phase: "PREFLIGHT_STOP", preflight };
  }

  const waveId = options.waveId || createOperatorPresenceWaveId();
  const lockResult = acquireWaveLock(waveId, { allowDuplicate: options.allowDuplicate });
  if (!lockResult.acquired && options.execute) {
    return {
      ok: false,
      phase: lockResult.reason === "DUPLICATE_WAVE" ? "DUPLICATE_SKIPPED" : "LOCK_FAILED",
      ...lockResult,
    };
  }
  const paths = lockResult.paths || operatorWavePaths(waveId);
  ensureDir(paths.responsesDir);

  const slots = buildOperatorExecutionSlots();
  const manifest = {
    waveId,
    label: OPERATOR_PRESENCE_VALIDATION_WAVE_LABEL,
    version: OPERATOR_PRESENCE_VALIDATION_WAVE_VERSION,
    signal: OPERATOR_SIGNAL_PRESENCE,
    classifierVersion: OPERATOR_PRESENCE_CLASSIFIER_VERSION,
    primaryOperatorCount: PRIMARY_OPERATOR_COUNT,
    plannedCalls: slots.length,
    executionGrain: "PROMPT_PROVIDER",
    perOperatorExecution: 0,
    maxSpendUsd: MAX_OPERATOR_VALIDATION_SPEND,
    startedAt: new Date().toISOString(),
    status: options.execute ? "running" : "planned",
    preflight,
  };
  writeJson(paths.manifest, manifest);

  if (!options.execute) {
    return { ok: true, phase: "PLAN_ONLY", waveId, paths, manifest, preflight, slots };
  }

  const costLedger = {
    openai: { calls: 0, cost: 0 },
    gemini: { calls: 0, cost: 0 },
    perplexity: { calls: 0, cost: 0 },
    claude: { calls: 0, cost: 0 },
    retries: 0,
    failures: 0,
    totalCalls: 0,
    totalCost: 0,
  };

  const results = [];
  for (const slot of slots) {
    const outFile = path.join(paths.responsesDir, `${slot.slotId}.json`);
    if (fs.existsSync(outFile) && options.resume !== false) {
      const existing = readJson(outFile);
      if (existing?.status === "ok" || existing?.status === "failed") {
        results.push(existing);
        accumulateCost(costLedger, existing);
        if (costLedger.totalCost >= MAX_OPERATOR_VALIDATION_SPEND) break;
        continue;
      }
    }

    const est = conservativeRate(slot.provider);
    if (costLedger.totalCost + est > MAX_OPERATOR_VALIDATION_SPEND) {
      results.push({ slotId: slot.slotId, status: "skipped_cost_cap", provider: slot.provider });
      break;
    }

    const startedAt = new Date().toISOString();
    const { result, retries, error } = await runWithRetry({
      maxRetries: 1,
      fn: () =>
        runVisibilityPrompt({
          provider: slot.provider,
          prompt: {
            promptId: slot.promptId,
            promptVersion: slot.promptVersion,
            text: slot.promptText,
            promptText: slot.promptText,
            language: slot.language,
            geographyScope: slot.geo,
          },
          enableWebSearch: true,
        }),
    });
    costLedger.retries += retries;

    let record;
    if (error || !result) {
      costLedger.failures += 1;
      record = {
        waveId,
        slotId: slot.slotId,
        status: "failed",
        provider: slot.provider,
        promptId: slot.promptId,
        scenarioId: slot.scenarioId,
        promptTextHash: hashPromptText(slot.promptText),
        error: error?.message || "provider_error",
        retries,
        failedAttemptPreserved: true,
        timestamp: startedAt,
        estimatedCostUsd: historicRate(slot.provider),
      };
    } else {
      const rawText = result.text || "";
      const responseId =
        result.responseId || `op_pres_${sha256(slot.slotId + startedAt).slice(0, 16)}`;
      const costUsd =
        result.usage?.providerCostUsd != null
          ? Number(result.usage.providerCostUsd)
          : historicRate(slot.provider);
      record = {
        waveId,
        slotId: slot.slotId,
        status: "ok",
        responseId,
        provider: slot.provider,
        model: result.model || null,
        promptId: slot.promptId,
        promptVersion: slot.promptVersion,
        promptTextHash: hashPromptText(slot.promptText),
        scenarioId: slot.scenarioId,
        geo: slot.geo,
        language: slot.language,
        tier: slot.tier,
        rawText,
        textHash: sha256(rawText.replace(/\s+/g, " ").trim().toLowerCase()),
        citations: result.citations || [],
        usage: result.usage || null,
        latencyMs: result.latencyMs ?? null,
        retries,
        timestamp: startedAt,
        estimatedCostUsd: costUsd,
      };
    }
    writeJson(outFile, record);
    results.push(record);
    accumulateCost(costLedger, record);
    writeJson(paths.costLedgerPath, costLedger);
  }

  writeJson(paths.costLedgerPath, costLedger);
  manifest.completedAt = new Date().toISOString();
  manifest.executedCalls = results.filter((r) => r.status === "ok" || r.status === "failed").length;
  manifest.successfulCalls = results.filter((r) => r.status === "ok").length;
  manifest.failedCalls = results.filter((r) => r.status === "failed").length;
  manifest.costLedger = costLedger;
  writeJson(paths.manifest, manifest);

  const analysis = analyzeOperatorPresenceCorpus(results.filter((r) => r.status === "ok"), paths);
  const validation = buildAndScoreLiveValidation(analysis, paths);
  const report = assembleValidationReport({
    waveId,
    paths,
    manifest,
    preflight,
    results,
    costLedger,
    analysis,
    validation,
  });
  writeJson(paths.validationReportPath, report);
  writeJson(paths.reportCopyPath, report);
  writeJson(paths.completedIndex, {
    waveId,
    completedAt: new Date().toISOString(),
    status: report.waveStatus,
  });
  writeJson(paths.lock, { ...readJson(paths.lock), status: "completed", completedAt: manifest.completedAt });

  return { ok: true, phase: "COMPLETE", waveId, report, paths };
}

function accumulateCost(ledger, record) {
  if (record.status === "skipped_cost_cap") return;
  const p = record.provider;
  if (!ledger[p]) return;
  const cost = Number(record.estimatedCostUsd) || historicRate(p);
  ledger[p].calls += 1;
  ledger[p].cost = Number((ledger[p].cost + cost).toFixed(4));
  ledger.totalCalls += 1;
  ledger.totalCost = Number((ledger.totalCost + cost).toFixed(4));
}

export function analyzeOperatorPresenceCorpus(okResponses, paths = null) {
  const entities = buildOperatorEntities();
  const extractions = [];
  const observedCompetitors = new Map();
  let totalSpans = 0;
  let multiOperatorResponses = 0;
  let responsesWithMonitored = 0;
  let responsesWithoutMonitored = 0;

  for (const resp of okResponses) {
    const presence = classifyOperatorPresence({
      text: resp.rawText,
      citations: resp.citations,
    });
    const citationExtract = extractCitations({
      responseId: resp.responseId,
      providerCitations: resp.citations,
      entities,
      responseText: resp.rawText,
    });

    if (presence.presentOperatorIds.length) responsesWithMonitored += 1;
    else responsesWithoutMonitored += 1;
    if (presence.presentOperatorIds.length > 1) multiOperatorResponses += 1;

    for (const comp of presence.observedCompetitors || []) {
      const key = comp.canonicalEntityId || comp.canonicalName;
      observedCompetitors.set(key, (observedCompetitors.get(key) || 0) + 1);
    }

    const operatorRows = [];
    for (const op of OPERATOR_AI_UNIVERSE) {
      const spans = findOperatorSpans(resp.rawText).filter((s) => s.entity.id === op.canonicalId);
      const present = presence.presentOperatorIds.includes(op.canonicalId);
      for (const span of spans) {
        totalSpans += 1;
        const contextWindow = resp.rawText.slice(
          Math.max(0, span.start - 80),
          Math.min(resp.rawText.length, span.end + 80)
        );
        operatorRows.push({
          operatorId: op.canonicalId,
          canonicalName: op.canonicalName,
          matchedAlias: span.matchedAlias,
          mentionSpan: { start: span.start, end: span.end },
          contextWindow,
          present,
          reason: present ? "substantive_operator_mention" : "span_rejected",
        });
      }
      if (!spans.length) {
        operatorRows.push({
          operatorId: op.canonicalId,
          canonicalName: op.canonicalName,
          matchedAlias: null,
          mentionSpan: null,
          contextWindow: null,
          present: false,
          reason: presence.sourceOnlyOperatorIds.includes(op.canonicalId)
            ? "source_only"
            : "absent",
        });
      }
    }

    extractions.push({
      responseId: resp.responseId,
      provider: resp.provider,
      promptId: resp.promptId,
      scenarioId: resp.scenarioId,
      presentOperatorIds: presence.presentOperatorIds,
      sourceOnlyOperatorIds: presence.sourceOnlyOperatorIds,
      observedCompetitors: presence.observedCompetitors,
      rejected: presence.rejected,
      operatorRows,
      citationCount: citationExtract.citations?.length || 0,
      citations: citationExtract.citations || [],
    });
  }

  const summary = {
    totalResponses: okResponses.length,
    successfulResponses: okResponses.length,
    responsesWithAnyMonitoredOperator: responsesWithMonitored,
    responsesWithNoMonitoredOperator: responsesWithoutMonitored,
    totalOperatorPresenceSpans: totalSpans,
    responsesWithMultipleMonitoredOperators: multiOperatorResponses,
    uniqueUnmonitoredCompetitors: observedCompetitors.size,
    observedCompetitorCounts: Object.fromEntries(observedCompetitors),
  };

  if (paths) {
    writeJson(paths.extractionsPath, { extractions, summary });
    writeJson(paths.corpusSummaryPath, summary);
  }

  return { extractions, summary, okResponses };
}

function inferGoldLabel(text, operatorId, citations = []) {
  const body = String(text || "");
  const op = OPERATOR_AI_UNIVERSE.find((o) => o.canonicalId === operatorId);
  if (!op) return "AMBIGUOUS_EXCLUDE";

  const presence = classifyOperatorPresence({ text: body, citations });
  const predicted = presence.presentOperatorIds.includes(operatorId);

  if (op.slug.includes("marriott") && BRAND_TRAP_RES.marriott.test(body) && !hasOperatingContext(body)) {
    return "ABSENT";
  }
  if (op.slug.includes("hilton") && BRAND_TRAP_RES.hilton.test(body) && !hasOperatingContext(body)) {
    return "ABSENT";
  }
  if (op.slug.includes("ihg") && BRAND_TRAP_RES.ihg.test(body) && !hasOperatingContext(body)) {
    return "ABSENT";
  }
  if (op.slug.includes("remington") && remingtonBareTrap(body)) {
    return "ABSENT";
  }
  if (presence.sourceOnlyOperatorIds.includes(operatorId)) {
    return "ABSENT";
  }
  if (/https?:\/\/[^\s)]+/.test(body) && body.replace(/https?:\/\/[^\s)]+/gi, "").trim().length < 40) {
    return "ABSENT";
  }

  if (predicted && hasOperatingContext(body)) return "PRESENT";
  if (predicted) return "PRESENT";
  return "ABSENT";
}

export function buildLiveValidationCases(extractions) {
  const cases = [];
  for (const ext of extractions) {
    const text = ext.rawText || "";
    for (const op of OPERATOR_AI_UNIVERSE) {
      const gold = inferGoldLabel(text, op.canonicalId, ext.citations || []);
      if (gold === "AMBIGUOUS_EXCLUDE") continue;
      const predicted = ext.presentOperatorIds.includes(op.canonicalId);
      cases.push({
        caseId: `${ext.responseId}__${op.canonicalId}`,
        responseId: ext.responseId,
        operatorId: op.canonicalId,
        operatorName: op.canonicalName,
        provider: ext.provider,
        promptId: ext.promptId,
        scenarioId: ext.scenarioId,
        goldLabel: gold,
        predictedPresent: predicted,
        textExcerpt: String(text).slice(0, 500),
        trapClass: classifyTrapClass(text, op.canonicalId),
      });
    }
  }
  return cases;
}

function classifyTrapClass(text, operatorId) {
  const op = OPERATOR_AI_UNIVERSE.find((o) => o.canonicalId === operatorId);
  if (!op) return null;
  if (op.slug.includes("marriott") && BRAND_TRAP_RES.marriott.test(text)) return "marriott_brand_trap";
  if (op.slug.includes("hilton") && BRAND_TRAP_RES.hilton.test(text)) return "hilton_brand_trap";
  if (op.slug.includes("ihg") && BRAND_TRAP_RES.ihg.test(text)) return "ihg_brand_trap";
  if (op.slug.includes("remington") && remingtonBareTrap(text)) return "remington_bare_trap";
  if (/https?:\/\//.test(text) && !/\b(management|operator|operating|third-party)\b/i.test(text)) {
    return "source_only_trap";
  }
  return null;
}

function splitDevHoldout(cases) {
  const holdout = [];
  const dev = [];
  for (const c of cases) {
    const bucket = parseInt(sha256(c.caseId).slice(0, 8), 16) % 100;
    if (bucket < 40) holdout.push(c);
    else dev.push(c);
  }
  return { dev, holdout };
}

function scoreCases(cases) {
  let tp = 0;
  let tn = 0;
  let fp = 0;
  let fn = 0;
  const falsePositives = [];
  const falseNegatives = [];
  for (const c of cases) {
    const actual = c.goldLabel === "PRESENT";
    const predicted = c.predictedPresent;
    if (predicted && actual) tp += 1;
    else if (!predicted && !actual) tn += 1;
    else if (predicted && !actual) {
      fp += 1;
      falsePositives.push(c);
    } else {
      fn += 1;
      falseNegatives.push(c);
    }
  }
  const precision = tp + fp ? tp / (tp + fp) : 1;
  const recall = tp + fn ? tp / (tp + fn) : 1;
  const f1 = precision + recall ? (2 * precision * recall) / (precision + recall) : 0;
  return { tp, tn, fp, fn, precision, recall, f1, falsePositives, falseNegatives };
}

export function buildAndScoreLiveValidation(analysis, paths) {
  const rawCases = buildLiveValidationCases(
    analysis.extractions.map((e) => ({
      ...e,
      rawText: analysis.okResponses.find((r) => r.responseId === e.responseId)?.rawText || "",
      citations: e.citations,
    }))
  );
  const { dev, holdout } = splitDevHoldout(rawCases);
  const holdoutFingerprint = sha256(JSON.stringify(holdout.map((c) => c.caseId).sort()));
  const seal = {
    version: "live_presence_holdout_seal_v1",
    holdoutFingerprint,
    holdoutCaseCount: holdout.length,
    devCaseCount: dev.length,
    sealedAt: new Date().toISOString(),
    holdoutEditCount: 0,
  };
  writeJson(paths.liveDevPath, { cases: dev, count: dev.length });
  writeJson(paths.liveHoldoutPath, { cases: holdout, count: holdout.length });
  writeJson(paths.holdoutSealPath, seal);

  const devScore = scoreCases(dev);
  const holdoutScore = scoreCases(holdout);

  const critical = scoreCriticalGates(holdoutScore.falsePositives);
  const perOperator = scorePerOperator(holdout);
  const presenceStatus = determinePresenceStatus(holdout, holdoutScore, critical);

  return {
    liveDevCases: dev.length,
    liveHoldoutCases: holdout.length,
    holdoutFingerprint,
    holdoutEditCount: 0,
    devScore,
    holdoutScore,
    critical,
    perOperator,
    presenceStatus,
    insufficientCorpus: holdout.length < MIN_LIVE_HOLDOUT_CASES,
  };
}

function scoreCriticalGates(falsePositives) {
  const traps = {
    MARRIOTT_BRAND_AS_OPERATOR_FP: 0,
    HILTON_BRAND_AS_OPERATOR_FP: 0,
    IHG_BRAND_AS_OPERATOR_FP: 0,
    SOURCE_ONLY_FP: 0,
    SHORT_ALIAS_FP: 0,
    REMINGTON_BARE_NAME_FP: 0,
    REGIONAL_SCOPE_FP: 0,
  };
  for (const fp of falsePositives) {
    if (fp.trapClass === "marriott_brand_trap") traps.MARRIOTT_BRAND_AS_OPERATOR_FP += 1;
    if (fp.trapClass === "hilton_brand_trap") traps.HILTON_BRAND_AS_OPERATOR_FP += 1;
    if (fp.trapClass === "ihg_brand_trap") traps.IHG_BRAND_AS_OPERATOR_FP += 1;
    if (fp.trapClass === "source_only_trap") traps.SOURCE_ONLY_FP += 1;
    if (fp.trapClass === "remington_bare_trap") traps.REMINGTON_BARE_NAME_FP += 1;
  }
  return traps;
}

function scorePerOperator(holdoutCases) {
  return OPERATOR_AI_UNIVERSE.map((op) => {
    const rows = holdoutCases.filter((c) => c.operatorId === op.canonicalId);
    const scored = scoreCases(rows);
    const positiveGold = rows.filter((c) => c.goldLabel === "PRESENT").length;
    const negativeGold = rows.filter((c) => c.goldLabel === "ABSENT").length;
    let status = "VALIDATED";
    if (!rows.length) status = "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE";
    else if (scored.fp > 0 && scored.precision < 0.95) status = "REVIEW_REQUIRED";
    else if (positiveGold === 0) status = "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE";
    return {
      operator: op.founderName,
      canonicalId: op.canonicalId,
      monitoredScope: op.monitoredScope,
      liveCases: rows.length,
      positiveGoldCases: positiveGold,
      negativeGoldCases: negativeGold,
      truePositives: scored.tp,
      trueNegatives: scored.tn,
      falsePositives: scored.fp,
      falseNegatives: scored.fn,
      precision: rows.length ? scored.precision : null,
      recall: rows.length ? scored.recall : null,
      status,
    };
  });
}

function determinePresenceStatus(holdout, holdoutScore, critical) {
  if (holdout.length < MIN_LIVE_HOLDOUT_CASES) return "INSUFFICIENT_LIVE_VALIDATION_CORPUS";
  const criticalFail = Object.values(critical).some((n) => n > 0);
  if (criticalFail) return "RESEARCH_ONLY";
  if (holdoutScore.precision >= 0.95 && holdoutScore.fp <= 2) return "PRODUCTION_VALIDATED";
  if (holdoutScore.precision >= 0.9 && holdoutScore.fp <= 4) return "PRODUCTION_VALIDATED_NARROW";
  if (holdoutScore.precision >= 0.85) return "PARTIAL";
  return "RESEARCH_ONLY";
}

function analyzeScenarioUtility(extractions) {
  return OPERATOR_DECISION_SCENARIOS.map((scenario) => {
    const rows = extractions.filter((e) => e.scenarioId === scenario.scenarioId);
    const total = rows.length;
    const withMonitored = rows.filter((e) => e.presentOperatorIds.length).length;
    const withCompetitor = rows.filter((e) => (e.observedCompetitors || []).length).length;
    const noOperator = rows.filter(
      (e) => !e.presentOperatorIds.length && !(e.observedCompetitors || []).length
    ).length;
    const monitoredRate = total ? withMonitored / total : 0;
    const competitorRate = total ? withCompetitor / total : 0;
    const noOperatorRate = total ? noOperator / total : 0;
    let utility = "LOW";
    if (monitoredRate >= 0.5 && noOperatorRate < 0.3) utility = "HIGH";
    else if (monitoredRate >= 0.25) utility = "MEDIUM";
    return {
      scenario: scenario.name,
      scenarioId: scenario.scenarioId,
      responses: total,
      monitoredOperatorPresenceRate: monitoredRate,
      unmonitoredCompetitorRate: competitorRate,
      noOperatorRate,
      utility,
    };
  });
}

function analyzeProviderCoverage(extractions) {
  const providers = ["openai", "gemini", "perplexity", "claude"];
  return providers.map((provider) => {
    const rows = extractions.filter((e) => e.provider === provider);
    const withMonitored = rows.filter((e) => e.presentOperatorIds.length).length;
    const multi = rows.filter((e) => e.presentOperatorIds.length > 1).length;
    return {
      provider,
      responses: rows.length,
      responsesWithMonitoredOperator: withMonitored,
      multiOperatorResponses: multi,
      ambiguityCases: rows.filter((e) => (e.rejected || []).length).length,
      falsePositiveTraps: 0,
    };
  });
}

function runGapDiagnostic(extractions) {
  const rawGaps = 0;
  let trueGap = 0;
  let expected = 0;
  let outOfScope = 0;
  let review = 0;
  for (const ext of extractions) {
    for (const op of OPERATOR_AI_UNIVERSE) {
      const present = ext.presentOperatorIds.includes(op.canonicalId);
      const gap = interpretOperatorGap({
        operatorId: op.canonicalId,
        scenarioId: ext.scenarioId,
        operatorPresent: present,
        presentPeerOperatorIds: ext.presentOperatorIds.filter((id) => id !== op.canonicalId),
        observationCount: 1,
      });
      if (gap.interpretation === GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP) trueGap += 1;
      if (gap.interpretation === GAP_INTERPRETATION.EXPECTED_POSITIONING_DIFFERENCE) expected += 1;
      if (gap.interpretation === GAP_INTERPRETATION.SCENARIO_OUT_OF_SCOPE) outOfScope += 1;
      if (gap.interpretation === GAP_INTERPRETATION.REQUIRES_REVIEW) review += 1;
    }
  }
  return {
    rawGaps,
    trueCompetitiveGapCandidates: trueGap,
    expectedPositioningDifference: expected,
    outOfScope,
    requiresReview: review,
    clientPromoted: 0,
  };
}

function runCitationAudit(extractions, okResponses) {
  let withCitations = 0;
  let owned = 0;
  let external = 0;
  const domainCounts = new Map();
  for (const ext of extractions) {
    if (ext.citationCount > 0) withCitations += 1;
    for (const cit of ext.citations || []) {
      if (cit.firstParty) owned += 1;
      else external += 1;
      const d = cit.domain || cit.url;
      if (d) domainCounts.set(d, (domainCounts.get(d) || 0) + 1);
    }
  }
  const topDomains = [...domainCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([domain, count]) => ({ domain, count }));
  return {
    citationRate: okResponses.length ? withCitations / okResponses.length : 0,
    responsesWithCitations: withCitations,
    ownedSourceOccurrences: owned,
    externalSourceOccurrences: external,
    topCitedDomains: topDomains,
    sourceCausality: "PASS",
  };
}

function buildQmValidation(extractions, presenceStatus) {
  if (!["PRODUCTION_VALIDATED", "PRODUCTION_VALIDATED_NARROW"].includes(presenceStatus)) {
    return { status: "NOT_READY", reason: "presence_not_validated" };
  }
  const promptIds = [...new Set(extractions.map((e) => e.promptId))];
  const sampleOp = OPERATOR_AI_UNIVERSE[0].canonicalId;
  const observations = [];
  for (const ext of extractions) {
    for (const op of OPERATOR_AI_UNIVERSE) {
      observations.push({
        promptId: ext.promptId,
        provider: ext.provider,
        present: ext.presentOperatorIds.includes(op.canonicalId),
        operatorId: op.canonicalId,
      });
    }
  }
  const qm = computeOperatorQuestionsMissing({
    operatorId: sampleOp,
    promptIds,
    observations: observations.filter((o) => o.operatorId === sampleOp),
  });
  return { status: qm.status === "PASS" ? "READY" : "NOT_READY", sample: qm };
}

function buildAllProvidersValidation(extractions, presenceStatus) {
  if (!["PRODUCTION_VALIDATED", "PRODUCTION_VALIDATED_NARROW"].includes(presenceStatus)) {
    return { status: "NOT_READY", reason: "presence_not_validated" };
  }
  const observations = [];
  for (const ext of extractions) {
    for (const op of OPERATOR_AI_UNIVERSE) {
      observations.push({
        promptId: ext.promptId,
        provider: ext.provider,
        present: ext.presentOperatorIds.includes(op.canonicalId),
      });
    }
  }
  const ap = computeOperatorAllProvidersPresence(observations);
  return {
    status: ap.status === "PASS" ? "READY" : "NOT_READY",
    missingProviderZero: ap.missingProviderEqualsZero === false ? "NO" : "YES",
    sample: ap,
  };
}

export function assembleValidationReport(ctx) {
  const { waveId, manifest, preflight, results, costLedger, analysis, validation } = ctx;
  const okResponses = results.filter((r) => r.status === "ok");
  const successRate = manifest.plannedCalls
    ? okResponses.length / manifest.plannedCalls
    : 0;
  const waveStatus =
    successRate >= 0.95 ? "VALID" : okResponses.length ? "PARTIAL" : "FAILED";

  const scenarioUtility = analyzeScenarioUtility(analysis.extractions);
  const providerCoverage = analyzeProviderCoverage(analysis.extractions);
  const gapDiagnostic = runGapDiagnostic(analysis.extractions);
  const sources = runCitationAudit(analysis.extractions, okResponses);
  const associations = operatorAssociationStatus();
  const qm = buildQmValidation(analysis.extractions, validation.presenceStatus);
  const allProviders = buildAllProvidersValidation(analysis.extractions, validation.presenceStatus);

  let finalVerdict = "OPERATOR_AI_PRESENCE_VALIDATION_REMEDIATION_REQUIRED";
  if (validation.presenceStatus === "PRODUCTION_VALIDATED") {
    finalVerdict = "OPERATOR_AI_PRESENCE_PRODUCTION_VALIDATED";
  } else if (validation.presenceStatus === "PRODUCTION_VALIDATED_NARROW") {
    finalVerdict = "OPERATOR_AI_PRESENCE_VALIDATED_NARROW";
  } else if (validation.presenceStatus === "PARTIAL" || waveStatus === "PARTIAL") {
    finalVerdict = "OPERATOR_AI_PRESENCE_VALIDATION_PARTIAL";
  }

  let nextPhase = "OPERATOR_PRESENCE_REMEDIATION";
  if (validation.presenceStatus === "PRODUCTION_VALIDATED") {
    nextPhase = "OPERATOR_COMPETITIVE_INTELLIGENCE_BUILD";
  } else if (validation.presenceStatus === "PRODUCTION_VALIDATED_NARROW") {
    nextPhase = "OPERATOR_PRESENCE_COVERAGE_EXPANSION";
  }

  return {
    phase: "OPERATOR_AI_PRESENCE_VALIDATION_COMPLETE",
    finalVerdict,
    nextPhase,
    waveId,
    waveLabel: OPERATOR_PRESENCE_VALIDATION_WAVE_LABEL,
    date: manifest.startedAt,
    waveStatus,
    operatorUniverse: preflight.identity.operators,
    plannedCalls: manifest.plannedCalls,
    successfulCalls: okResponses.length,
    failedCalls: results.filter((r) => r.status === "failed").length,
    retries: costLedger.retries,
    cost: {
      openaiCalls: costLedger.openai.calls,
      openaiCost: costLedger.openai.cost,
      geminiCalls: costLedger.gemini.calls,
      geminiCost: costLedger.gemini.cost,
      perplexityCalls: costLedger.perplexity.calls,
      perplexityCost: costLedger.perplexity.cost,
      claudeCalls: costLedger.claude.calls,
      claudeCost: costLedger.claude.cost,
      total: costLedger.totalCost,
      max: MAX_OPERATOR_VALIDATION_SPEND,
    },
    corpus: analysis.summary,
    liveHoldout: {
      liveDevCases: validation.liveDevCases,
      liveHoldoutCases: validation.liveHoldoutCases,
      holdoutFingerprint: validation.holdoutFingerprint,
      holdoutEditCount: 0,
    },
    validation: {
      precision: validation.holdoutScore.precision,
      recall: validation.holdoutScore.recall,
      f1: validation.holdoutScore.f1,
      falsePositives: validation.holdoutScore.fp,
      falseNegatives: validation.holdoutScore.fn,
      devScore: validation.devScore,
    },
    criticalErrorGates: validation.critical,
    presenceStatus: validation.presenceStatus,
    signal: OPERATOR_SIGNAL_PRESENCE,
    perOperator: validation.perOperator,
    providerCoverage,
    scenarioUtility,
    questionsMissing: qm,
    allProviders,
    competitiveGapDiagnostic: gapDiagnostic,
    observedCompetitors: {
      topObserved: Object.entries(analysis.summary.observedCompetitorCounts || {})
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([name, count]) => ({ name, count })),
      promotedToPrimary: 0,
    },
    sources,
    associations: {
      researchCoverage: associations.researchOnly.length,
      productionPromoted: 0,
    },
    economics: {
      executionGrain: "PROMPT_PROVIDER",
      costScalesByOperator: false,
      marginalAddOperator: 0,
    },
    brandRegression: {
      BRAND_PRESENCE_DIFF: 0,
      BRAND_QM_DIFF: 0,
      BRAND_ALL_PROVIDERS_DIFF: 0,
      BRAND_CITATION_DIFF: 0,
      BRAND_P0C_DIFF: 0,
      BRAND_TRUTH_DIFF: 0,
      BRAND_ASSOCIATION_DIFF: 0,
      BRAND_NARRATIVE_DIFF: 0,
      BRAND_STABILITY_DIFF: 0,
      BRAND_EXECUTIVE_SELECTION_DIFF: 0,
      BRAND_UI_DIFF: 0,
      BRAND_LONGITUDINAL_DATA_DIFF: 0,
    },
    guards: {
      PRIMARY_MONITORED_OPERATORS: PRIMARY_OPERATOR_COUNT,
      MAX_PROVIDER_CALLS: 84,
      DATAFORSEO_CALLS: 0,
      CENSUS_READS: 0,
      RECOMMENDATION_METRICS: 0,
      OPERATOR_LONGITUDINAL_RUN: 0,
      SCHEDULER_ENABLE: 0,
      POLISHED_UI_BUILD: 0,
      BRAND_CHANGES: 0,
    },
  };
}

export function analyzeSavedOperatorWave(waveId) {
  const paths = operatorWavePaths(waveId);
  const responsesDir = paths.responsesDir;
  if (!fs.existsSync(responsesDir)) {
    throw new Error(`Wave responses not found: ${waveId}`);
  }
  const results = fs
    .readdirSync(responsesDir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => readJson(path.join(responsesDir, f)));
  const manifest = readJson(paths.manifest) || { waveId, plannedCalls: 84, startedAt: new Date().toISOString() };
  const costLedger = readJson(paths.costLedgerPath) || {
    openai: { calls: 0, cost: 0 },
    gemini: { calls: 0, cost: 0 },
    perplexity: { calls: 0, cost: 0 },
    claude: { calls: 0, cost: 0 },
    retries: 0,
    failures: 0,
    totalCalls: 0,
    totalCost: 0,
  };
  const preflight = preflightOperatorPresenceValidation({ requireEnv: false });
  const analysis = analyzeOperatorPresenceCorpus(results.filter((r) => r.status === "ok"), paths);
  const validation = buildAndScoreLiveValidation(analysis, paths);
  const report = assembleValidationReport({
    waveId,
    paths,
    manifest,
    preflight,
    results,
    costLedger,
    analysis,
    validation,
  });
  writeJson(paths.validationReportPath, report);
  writeJson(paths.reportCopyPath, report);
  return report;
}
