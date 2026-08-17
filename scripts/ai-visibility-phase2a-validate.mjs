#!/usr/bin/env node
/**
 * AI Visibility Phase 2A — live entity integration + controlled OpenAI validation.
 *
 * Modes:
 *   --entities-only     Load live entities + select cohort (no provider calls)
 *   --single-prompt     One live OpenAI call (requires flags + key)
 *   --cohort            Run full Phase 2A prompt cohort after single-prompt gate file exists
 *   --report-only       Rebuild metrics/report from saved runtime evidence
 *
 * Safety:
 *   AI_VISIBILITY_ENABLED=true
 *   AI_VISIBILITY_LIVE_TEST=true
 *   AI_VISIBILITY_MAX_TEST_RUNS (default 20 for cohort)
 *
 * Zero Airtable writes. No scheduler. Does not commit live payloads.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  createAiVisibilityStore,
  normalizeProviderResponse,
  extractMentions,
  extractCitations,
  buildObservationFromExtractions,
  computeAiPresenceRate,
  computeRecommendationShare,
  computeFirstRecommendationRate,
  computeQuestionsWon,
  computeQuestionsMissing,
  computeCompetitivePosition,
  computeCitationRate,
  assembleEvidenceRecord,
  metricEvidenceTrace,
  runVisibilityPrompt,
  isAiVisibilityEnabled,
  isAiVisibilityLiveTestAllowed,
  resolveDefaultModel,
  resolveMaxTestRuns,
  resolveMaxDailyCostUsd,
  METRIC_VERSION,
} from "../lib/ai-visibility/index.js";
import {
  loadLiveBrandEntities,
  selectBrandsByCanonicalNames,
} from "../lib/ai-visibility/load-brands-live.js";
import {
  loadLiveOperatorEntities,
  selectOperatorsByCanonicalNames,
} from "../lib/ai-visibility/load-operators-live.js";
import { buildAiVisibilityEntityIndex } from "../lib/ai-visibility/entity-index.js";
import {
  classifyMentionRole,
  harvestUnresolvedProperPhrases,
} from "../lib/ai-visibility/mention-classification.js";
import { normalizeMatchKey } from "../lib/ai-visibility/normalize-entities.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const COHORT_PATH = path.join(ROOT, "fixtures", "ai-visibility", "phase2a-cohort.json");
const RUNTIME = path.join(ROOT, "data", "ai-visibility", "runtime", "phase2a");
const GATE_PATH = path.join(RUNTIME, "single-prompt-gate.json");
const REPORT_PATH = path.join(RUNTIME, "phase2a-validation-report.json");

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, value) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(value, null, 2), "utf8");
}

function assertLiveGates({ requireKey = true } = {}) {
  if (!isAiVisibilityEnabled()) {
    throw Object.assign(new Error("Set AI_VISIBILITY_ENABLED=true"), { code: "gate" });
  }
  if (!isAiVisibilityLiveTestAllowed()) {
    throw Object.assign(new Error("Set AI_VISIBILITY_LIVE_TEST=true"), { code: "gate" });
  }
  if (requireKey && !String(process.env.OPENAI_API_KEY || "").trim()) {
    throw Object.assign(new Error("OPENAI_API_KEY is not configured"), { code: "missing_openai_key" });
  }
}

async function loadCohortUniverse() {
  const cohort = readJson(COHORT_PATH);
  const brandPack = await loadLiveBrandEntities();
  const operatorPack = await loadLiveOperatorEntities();
  const brandSel = selectBrandsByCanonicalNames(brandPack.entities, cohort.brands);
  const opSel = selectOperatorsByCanonicalNames(operatorPack.entities, cohort.operators);
  if (brandSel.missing.length || opSel.missing.length) {
    const err = new Error(
      `Cohort entity mismatch. Missing brands: ${brandSel.missing.join(", ") || "none"}; missing operators: ${opSel.missing.join(", ") || "none"}`
    );
    err.code = "cohort_entity_mismatch";
    err.missingBrands = brandSel.missing;
    err.missingOperators = opSel.missing;
    throw err;
  }
  const index = buildAiVisibilityEntityIndex({
    brands: brandSel.selected,
    operators: opSel.selected,
  });
  return {
    cohort,
    brandPack,
    operatorPack,
    selectedBrands: brandSel.selected,
    selectedOperators: opSel.selected,
    index,
  };
}

function knownNameKeys(entities) {
  const keys = new Set();
  for (const e of entities) {
    keys.add(normalizeMatchKey(e.name));
    for (const a of e.aliases || []) keys.add(normalizeMatchKey(a));
  }
  return keys;
}

async function processProviderResult({
  store,
  prompt,
  providerResult,
  index,
  runExtras = {},
}) {
  const run = await store.saveRun({
    runId: store.generateId("run"),
    promptId: prompt.promptId,
    promptVersion: prompt.version,
    provider: providerResult.provider || "openai",
    model: providerResult.model || resolveDefaultModel(),
    startedAt: runExtras.startedAt || new Date().toISOString(),
    completedAt: new Date().toISOString(),
    status: "completed",
    latencyMs: providerResult.latencyMs ?? null,
    usage: providerResult.usage ?? null,
    estimatedCost: runExtras.estimatedCost ?? null,
    error: null,
    providerMeta: providerResult.providerMeta || {},
  });

  const normalized = normalizeProviderResponse({
    runId: run.runId,
    promptId: prompt.promptId,
    providerResult,
  });
  if (!normalized.ok) {
    throw new Error(`Normalize failed: ${normalized.error?.message || "unknown"}`);
  }
  const response = await store.saveResponse(normalized.response);
  const mentions = extractMentions({
    responseId: response.responseId,
    text: response.text,
    entityIndex: index.aliasIndex,
  });
  const citations = extractCitations({
    responseId: response.responseId,
    providerCitations: response.citations,
    entities: index.entities,
    mentions,
    responseText: response.text,
  });
  await store.saveMentions(response.responseId, mentions);
  await store.saveCitations(response.responseId, citations);

  const observation = buildObservationFromExtractions({
    observationId: `obs_${response.responseId}`,
    promptId: prompt.promptId,
    provider: response.provider,
    periodKey: "phase2a",
    success: true,
    mentions,
    citations,
  });

  const roles = mentions.map((m) => ({
    ...m,
    role: classifyMentionRole(m),
  }));

  const evidence = assembleEvidenceRecord({
    prompt,
    run,
    response,
    mentions,
    citations,
    metrics: { observation },
  });
  await store.saveEvidence(evidence);

  const unresolvedCandidates = harvestUnresolvedProperPhrases(
    response.text,
    knownNameKeys(index.entities)
  );

  return {
    run,
    response,
    mentions: roles,
    citations,
    observation,
    evidence,
    unresolvedCandidates,
  };
}

function summarizeObservation(row) {
  const recOrder = (row.mentions || [])
    .filter((m) => m.explicitRecommendation && m.canonicalEntityName)
    .sort((a, b) => (a.recommendationPosition || 99) - (b.recommendationPosition || 99))
    .map((m) => m.canonicalEntityName);
  return {
    promptId: row.prompt.promptId,
    promptText: row.prompt.text,
    intentTerritory: row.prompt.intentTerritory,
    provider: row.response.provider,
    model: row.response.model,
    excerpt: String(row.response.text || "").slice(0, 400),
    brandsDetected: [
      ...new Set(
        row.mentions
          .filter((m) => m.entityType === "brand")
          .map((m) => m.canonicalEntityName)
      ),
    ],
    operatorsDetected: [
      ...new Set(
        row.mentions
          .filter((m) => m.entityType === "operator")
          .map((m) => m.canonicalEntityName)
      ),
    ],
    recommendationOrder: [...new Set(recOrder)],
    citations: row.citations.map((c) => ({
      url: c.url,
      domain: c.domain,
      firstParty: c.firstParty,
      entityAssociation: c.entityAssociation,
      providerSupplied: c.providerSupplied,
    })),
    firstPartyCitations: row.citations.filter((c) => c.firstParty),
    unresolvedCandidates: row.unresolvedCandidates.slice(0, 15),
    mentionRoles: row.mentions.map((m) => ({
      name: m.canonicalEntityName,
      role: m.role,
      snippet: m.contextSnippet,
    })),
    evidenceId: row.evidence.evidenceId,
    runId: row.run.runId,
    responseId: row.response.responseId,
  };
}

function computeCohortMetrics(rows, peerEntityIds) {
  const observations = rows.map((r) => r.observation);
  const promptIds = [...new Set(rows.map((r) => r.prompt.promptId))];
  const byEntity = {};
  for (const id of peerEntityIds) {
    byEntity[id] = {
      presence: computeAiPresenceRate(observations, id),
      recommendationShare: computeRecommendationShare(observations, id),
      firstRecommendationRate: computeFirstRecommendationRate(observations, id),
      questionsWon: computeQuestionsWon(observations, id, promptIds),
      questionsMissing: computeQuestionsMissing(observations, id, promptIds),
      citationRate: computeCitationRate(observations, id),
    };
  }
  return {
    label: "CONTROLLED VALIDATION SAMPLE — NOT PRODUCTION BENCHMARK",
    metricVersion: METRIC_VERSION,
    competitivePosition: computeCompetitivePosition(observations, peerEntityIds),
    byEntity,
  };
}

function buildEvidenceTraces(rows, limit = 5) {
  const traces = [];
  for (const row of rows) {
    if (traces.length >= limit) break;
    const entityId = row.observation.presentEntityIds[0];
    if (!entityId) continue;
    const presence = computeAiPresenceRate([row.observation], entityId);
    traces.push(
      metricEvidenceTrace({
        metricResult: presence,
        evidenceId: row.evidence.evidenceId,
        observationIds: [row.observation.observationId],
      })
    );
    traces[traces.length - 1].chain = {
      metric: presence.metric,
      observationId: row.observation.observationId,
      evidenceId: row.evidence.evidenceId,
      promptId: row.prompt.promptId,
      promptVersion: row.prompt.version,
      runId: row.run.runId,
      responseId: row.response.responseId,
      rawExcerpt: String(row.response.text || "").slice(0, 180),
      mentionIds: row.mentions.map((m) => m.mentionId),
      citationIds: row.citations.map((c) => c.citationId),
    };
  }
  return traces;
}

async function runSinglePrompt(universe) {
  assertLiveGates({ requireKey: true });
  const maxRuns = resolveMaxTestRuns();
  if (maxRuns < 1) throw new Error("AI_VISIBILITY_MAX_TEST_RUNS must be >= 1");

  const prompt = universe.cohort.prompts[0];
  console.log("=== SINGLE PROMPT PLAN ===");
  console.log(
    JSON.stringify(
      {
        provider: "openai",
        model: resolveDefaultModel(),
        promptId: prompt.promptId,
        plannedRuns: 1,
        maxConfiguredRuns: maxRuns,
        maxDailyCostUsd: resolveMaxDailyCostUsd(),
      },
      null,
      2
    )
  );

  const store = createAiVisibilityStore({ rootDir: path.join(RUNTIME, "store") });
  const startedAt = new Date().toISOString();
  let providerResult;
  try {
    providerResult = await runVisibilityPrompt({
      provider: "openai",
      prompt,
      model: resolveDefaultModel(),
      context: {
        instructions:
          "Answer as a helpful research assistant for hotel owners. Prefer concrete brand and operator names when relevant. Do not invent citations.",
      },
    });
  } catch (err) {
    const gate = {
      ok: false,
      at: new Date().toISOString(),
      promptId: prompt.promptId,
      error: { type: err.type || "provider_error", message: err.message },
    };
    writeJson(GATE_PATH, gate);
    return gate;
  }

  const processed = await processProviderResult({
    store,
    prompt,
    providerResult,
    index: universe.index,
    runExtras: { startedAt },
  });

  const gate = {
    ok: true,
    at: new Date().toISOString(),
    promptId: prompt.promptId,
    provider: processed.response.provider,
    model: processed.response.model,
    responseSaved: true,
    mentionsExtracted: processed.mentions.length,
    citationsExtracted: processed.citations.length,
    evidenceCreated: true,
    evidenceId: processed.evidence.evidenceId,
    runId: processed.run.runId,
    responseId: processed.response.responseId,
    citationCapability: processed.response.citationCapability,
    usage: processed.response.usage,
    summary: summarizeObservation({ prompt, ...processed }),
  };
  writeJson(GATE_PATH, gate);
  writeJson(path.join(RUNTIME, "single-prompt-result.json"), {
    prompt,
    run: processed.run,
    response: {
      ...processed.response,
      // keep raw for reprocessing locally; gitignored under runtime
    },
    mentions: processed.mentions,
    citations: processed.citations,
    observation: processed.observation,
    evidence: processed.evidence,
  });
  return gate;
}

async function runCohort(universe) {
  assertLiveGates({ requireKey: true });
  if (!fs.existsSync(GATE_PATH)) {
    throw Object.assign(new Error("Single-prompt gate missing. Run --single-prompt first."), {
      code: "gate_missing",
    });
  }
  const gate = readJson(GATE_PATH);
  if (!gate.ok) {
    throw Object.assign(new Error("Single-prompt gate failed; refusing cohort."), {
      code: "gate_failed",
    });
  }

  const prompts = universe.cohort.prompts.filter((p) => p.active !== false);
  const maxRuns = resolveMaxTestRuns();
  const planned = prompts.length;
  if (planned > maxRuns) {
    throw Object.assign(
      new Error(`Planned runs ${planned} exceed AI_VISIBILITY_MAX_TEST_RUNS=${maxRuns}`),
      { code: "cap_exceeded" }
    );
  }

  console.log("=== COHORT PLAN ===");
  console.log(
    JSON.stringify(
      {
        provider: "openai",
        model: resolveDefaultModel(),
        plannedRuns: planned,
        maxConfiguredRuns: maxRuns,
        maxDailyCostUsd: resolveMaxDailyCostUsd(),
        promptIds: prompts.map((p) => p.promptId),
      },
      null,
      2
    )
  );

  const store = createAiVisibilityStore({ rootDir: path.join(RUNTIME, "store") });
  const rows = [];
  let success = 0;
  let failed = 0;
  let retries = 0;
  const usageTotal = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };

  for (const prompt of prompts) {
    const startedAt = new Date().toISOString();
    try {
      const providerResult = await runVisibilityPrompt({
        provider: "openai",
        prompt,
        model: resolveDefaultModel(),
        context: {
          instructions:
            "Answer as a helpful research assistant for hotel owners. Prefer concrete brand and operator names when relevant. Do not invent citations.",
        },
      });
      const processed = await processProviderResult({
        store,
        prompt,
        providerResult,
        index: universe.index,
        runExtras: { startedAt },
      });
      rows.push({ prompt, ...processed });
      success += 1;
      const u = providerResult.usage || {};
      usageTotal.inputTokens += Number(u.inputTokens || 0);
      usageTotal.outputTokens += Number(u.outputTokens || 0);
      usageTotal.totalTokens += Number(u.totalTokens || 0);
    } catch (err) {
      failed += 1;
      await store.saveRun({
        runId: store.generateId("run"),
        promptId: prompt.promptId,
        promptVersion: prompt.version,
        provider: "openai",
        model: resolveDefaultModel(),
        startedAt,
        completedAt: new Date().toISOString(),
        status: "failed",
        error: { type: err.type || "provider_error", message: err.message },
      });
      console.error("Prompt failed:", prompt.promptId, err.message);
    }
  }

  const peerIds = universe.index.entities.map((e) => e.id);
  const metrics = computeCohortMetrics(rows, peerIds);
  const report = {
    label: universe.cohort.label,
    generatedAt: new Date().toISOString(),
    universe: {
      brands: universe.selectedBrands.map((b) => ({ id: b.id, name: b.name })),
      operators: universe.selectedOperators.map((o) => ({ id: o.id, name: o.name })),
      entityIndexVersion: universe.index.version,
      entityIndexFingerprint: universe.index.fingerprint,
      brandLoader: universe.brandPack.meta,
      operatorLoader: universe.operatorPack.meta,
    },
    runStats: {
      plannedRuns: planned,
      liveProviderCalls: success + failed,
      successfulRuns: success,
      failedRuns: failed,
      retries,
      totalUsage: usageTotal,
      liveProviderCost: null,
    },
    observations: rows.map((r) => summarizeObservation(r)),
    metrics,
    evidenceTraces: buildEvidenceTraces(rows, 5),
    mentionClassification: summarizeMentionClassification(rows),
    citationReview: summarizeCitations(rows),
    entityMatchQuality: summarizeEntityMatch(rows, universe),
  };
  writeJson(REPORT_PATH, report);
  writeJson(path.join(RUNTIME, "cohort-rows-meta.json"), {
    responseIds: rows.map((r) => r.response.responseId),
    evidenceIds: rows.map((r) => r.evidence.evidenceId),
  });
  return report;
}

function summarizeMentionClassification(rows) {
  let canonicalMentions = 0;
  let explicit = 0;
  let first = 0;
  let passing = 0;
  let ambiguous = 0;
  const examples = { explicit_recommendation: [], discussed: [], comparator: [], negative_or_caution: [], passing_mention: [] };
  for (const row of rows) {
    for (const m of row.mentions) {
      if (!m.canonicalEntityId) continue;
      canonicalMentions += 1;
      if (m.explicitRecommendation) explicit += 1;
      if (m.recommendationPosition === 1) first += 1;
      if (m.role === "passing_mention") passing += 1;
      if (m.role === "discussed" || m.role === "comparator") ambiguous += 1;
      const bucket = examples[m.role] || null;
      if (bucket && bucket.length < 3) {
        bucket.push({
          promptId: row.prompt.promptId,
          entity: m.canonicalEntityName,
          snippet: m.contextSnippet,
        });
      }
    }
  }
  return { canonicalMentions, explicitRecommendations: explicit, firstRecommendations: first, passingMentions: passing, ambiguousClassifications: ambiguous, examples };
}

function summarizeCitations(rows) {
  let providerSupplied = 0;
  let valid = 0;
  let malformed = 0;
  let firstParty = 0;
  let thirdParty = 0;
  let unresolvedAssoc = 0;
  for (const row of rows) {
    for (const c of row.citations) {
      if (c.providerSupplied) providerSupplied += 1;
      if (c.url && c.domain) valid += 1;
      else if (c.url && !c.domain) malformed += 1;
      if (c.firstParty) firstParty += 1;
      else if (c.domain) thirdParty += 1;
      if (!c.entityAssociation) unresolvedAssoc += 1;
    }
  }
  return { providerSupplied, validUrls: valid, malformedUrls: malformed, firstParty, thirdParty, unresolvedEntityAssociation: unresolvedAssoc };
}

function summarizeEntityMatch(rows, universe) {
  const brandIds = new Set(universe.selectedBrands.map((b) => b.id));
  const opIds = new Set(universe.selectedOperators.map((o) => o.id));
  const brand = { exactOrAlias: 0, unresolvedCandidates: 0 };
  const operator = { exactOrAlias: 0, unresolvedCandidates: 0 };
  for (const row of rows) {
    for (const m of row.mentions) {
      if (brandIds.has(m.canonicalEntityId)) brand.exactOrAlias += 1;
      if (opIds.has(m.canonicalEntityId)) operator.exactOrAlias += 1;
    }
    brand.unresolvedCandidates += row.unresolvedCandidates.length;
    operator.unresolvedCandidates += row.unresolvedCandidates.filter((u) =>
      /hospitality|hotels|management|operator|lodging/i.test(u.rawMention)
    ).length;
  }
  return {
    brands: brand,
    operators: operator,
    proposedAliasesForFounderReview: collectProposedAliases(rows, universe),
  };
}

function collectProposedAliases(rows, universe) {
  const known = knownNameKeys(universe.index.entities);
  const freq = new Map();
  for (const row of rows) {
    for (const u of row.unresolvedCandidates) {
      const k = u.rawMention;
      if (known.has(normalizeMatchKey(k))) continue;
      freq.set(k, (freq.get(k) || 0) + 1);
    }
  }
  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 25)
    .map(([rawMention, count]) => ({ rawMention, count, action: "founder_review_only_do_not_write" }));
}

async function main() {
  const mode = process.argv.includes("--cohort")
    ? "cohort"
    : process.argv.includes("--single-prompt")
      ? "single-prompt"
      : process.argv.includes("--report-only")
        ? "report-only"
        : "entities-only";

  console.log(`Phase 2A mode: ${mode}`);
  const universe = await loadCohortUniverse();
  console.log(
    JSON.stringify(
      {
        brandsLoadedUniverse: universe.brandPack.meta.brandCount,
        operatorsLoadedUniverse: universe.operatorPack.meta.operatorCount,
        cohortBrands: universe.selectedBrands.map((b) => b.name),
        cohortOperators: universe.selectedOperators.map((o) => o.name),
        prompts: universe.cohort.prompts.length,
        entityIndexFingerprint: universe.index.fingerprint,
      },
      null,
      2
    )
  );

  writeJson(path.join(RUNTIME, "cohort-universe.json"), {
    brands: universe.selectedBrands,
    operators: universe.selectedOperators,
    brandMeta: universe.brandPack.meta,
    operatorMeta: universe.operatorPack.meta,
    indexMeta: {
      version: universe.index.version,
      fingerprint: universe.index.fingerprint,
      entityCount: universe.index.meta.entityCount,
    },
  });

  if (mode === "entities-only") {
    console.log("Entities-only complete. No provider calls.");
    return;
  }
  if (mode === "single-prompt") {
    const gate = await runSinglePrompt(universe);
    console.log(JSON.stringify({ singlePromptGate: gate }, null, 2));
    process.exit(gate.ok ? 0 : 2);
  }
  if (mode === "cohort") {
    const report = await runCohort(universe);
    console.log(
      JSON.stringify(
        {
          runStats: report.runStats,
          mentionClassification: report.mentionClassification,
          citationReview: report.citationReview,
          reportPath: REPORT_PATH,
        },
        null,
        2
      )
    );
    return;
  }
  if (mode === "report-only") {
    if (!fs.existsSync(REPORT_PATH)) {
      console.error("No report at", REPORT_PATH);
      process.exit(1);
    }
    console.log(fs.readFileSync(REPORT_PATH, "utf8"));
  }
}

main().catch((e) => {
  console.error(e.message || e);
  if (e.code) console.error("code:", e.code);
  process.exit(1);
});
