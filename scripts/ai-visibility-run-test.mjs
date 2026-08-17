#!/usr/bin/env node
/**
 * Controlled AI Visibility test runner (admin/dev only).
 *
 * Default: fixture pipeline (no paid provider calls).
 * Live OpenAI call requires:
 *   AI_VISIBILITY_LIVE_TEST=true
 *   OPENAI_API_KEY=...
 *
 *   node scripts/ai-visibility-run-test.mjs
 *   node scripts/ai-visibility-run-test.mjs --fixture=provider-brand-recommendations
 *   AI_VISIBILITY_LIVE_TEST=true node scripts/ai-visibility-run-test.mjs --live
 *
 * Does not write Airtable, schedule jobs, or loop.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  createAiVisibilityStore,
  normalizeProviderResponse,
  buildEntityAliasIndex,
  extractMentions,
  extractCitations,
  buildObservationFromExtractions,
  computeAiPresenceRate,
  assembleEvidenceRecord,
  runVisibilityPrompt,
  isAiVisibilityLiveTestAllowed,
  resolveDefaultModel,
  resolveMaxTestRuns,
} from "../lib/ai-visibility/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures", "ai-visibility");

function readJson(name) {
  return JSON.parse(fs.readFileSync(path.join(FIXTURES, name), "utf8"));
}

function argValue(flag) {
  const pref = `${flag}=`;
  const hit = process.argv.find((a) => a.startsWith(pref));
  return hit ? hit.slice(pref.length) : null;
}

async function main() {
  const live = process.argv.includes("--live");
  const fixtureName =
    argValue("--fixture") || "provider-brand-recommendations.json";
  const fixtureFile = fixtureName.endsWith(".json") ? fixtureName : `${fixtureName}.json`;

  const prompt = readJson("prompt-brand-mexico.json");
  const entities = readJson("entity-universe.json").entities;
  const entityIndex = buildEntityAliasIndex(entities);

  const storeRoot = path.join(ROOT, "data", "ai-visibility", "runtime", "test-runs");
  const store = createAiVisibilityStore({ rootDir: storeRoot });

  let providerResult;
  let runStatus = "completed";
  let runError = null;

  if (live) {
    if (!isAiVisibilityLiveTestAllowed()) {
      console.error("Refusing live call. Set AI_VISIBILITY_LIVE_TEST=true");
      process.exit(2);
    }
    const maxRuns = resolveMaxTestRuns();
    if (maxRuns < 1) {
      console.error("AI_VISIBILITY_MAX_TEST_RUNS must be >= 1");
      process.exit(2);
    }
    console.log(`Live OpenAI run (maxTestRuns=${maxRuns}, model=${resolveDefaultModel()})…`);
    try {
      providerResult = await runVisibilityPrompt({
        provider: "openai",
        prompt,
        model: resolveDefaultModel(),
      });
    } catch (err) {
      runStatus = "failed";
      runError = { type: err.type || "provider_error", message: err.message };
      console.error("Provider failed:", runError);
    }
  } else {
    const fx = readJson(fixtureFile);
    if (fx.error) {
      runStatus = "failed";
      runError = fx.error;
      console.log("Fixture provider error:", runError);
    } else {
      providerResult = fx.providerResult;
    }
  }

  const run = await store.saveRun({
    runId: store.generateId("run"),
    promptId: prompt.promptId,
    promptVersion: prompt.version,
    provider: "openai",
    model: providerResult?.model || resolveDefaultModel(),
    startedAt: new Date().toISOString(),
    completedAt: new Date().toISOString(),
    status: runStatus,
    latencyMs: providerResult?.latencyMs ?? null,
    usage: providerResult?.usage ?? null,
    estimatedCost: null,
    error: runError,
  });

  if (!providerResult) {
    console.log(JSON.stringify({ runId: run.runId, status: runStatus, error: runError }, null, 2));
    return;
  }

  const normalized = normalizeProviderResponse({
    runId: run.runId,
    promptId: prompt.promptId,
    providerResult,
  });
  if (!normalized.ok) {
    console.error("Normalize failed:", normalized.error);
    process.exit(1);
  }

  const response = await store.saveResponse(normalized.response);
  const mentions = extractMentions({
    responseId: response.responseId,
    text: response.text,
    entityIndex,
  });
  const citations = extractCitations({
    responseId: response.responseId,
    providerCitations: response.citations,
    entities,
  });
  await store.saveMentions(response.responseId, mentions);
  await store.saveCitations(response.responseId, citations);

  const observation = buildObservationFromExtractions({
    observationId: `obs_${response.responseId}`,
    promptId: prompt.promptId,
    provider: response.provider,
    success: true,
    mentions,
    citations,
  });

  const presence = computeAiPresenceRate([observation], "recBrandCurioFixture");
  const evidence = assembleEvidenceRecord({
    prompt,
    run,
    response,
    mentions,
    citations,
    metrics: { presence },
  });
  await store.saveEvidence(evidence);

  console.log(
    JSON.stringify(
      {
        mode: live ? "live" : "fixture",
        runId: run.runId,
        responseId: response.responseId,
        evidenceId: evidence.evidenceId,
        mentionCount: mentions.length,
        citationCount: citations.length,
        entitiesMentioned: [...new Set(mentions.map((m) => m.canonicalEntityName).filter(Boolean))],
        citationCapability: response.citationCapability,
        samplePresenceCurio: presence,
        storeRoot,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
