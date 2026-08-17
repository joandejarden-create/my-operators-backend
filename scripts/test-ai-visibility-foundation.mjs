#!/usr/bin/env node
/**
 * Foundation Phase 1 tests for lib/ai-visibility/*
 * No paid provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  createAiVisibilityStore,
  normalizeProviderResponse,
  buildEntityAliasIndex,
  resolveEntityMention,
  extractMentions,
  extractCitations,
  parseDomain,
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
  METRIC_VERSION,
  ProviderError,
} from "../lib/ai-visibility/index.js";
import { normalizeProviderHttpError } from "../lib/ai-visibility/providers/base-provider.js";
import { runVisibilityPrompt as runOpenAi } from "../lib/ai-visibility/providers/openai.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.join(__dirname, "..", "fixtures", "ai-visibility");

function readFx(name) {
  return JSON.parse(fs.readFileSync(path.join(FIXTURES, name), "utf8"));
}

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}`);
    console.error(`         ${err.message}`);
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}`);
    console.error(`         ${err.message}`);
  }
}

const entities = readFx("entity-universe.json").entities;
const entityIndex = buildEntityAliasIndex(entities);

console.log("AI Visibility Foundation Phase 1 tests\n");

console.log("Storage");
await testAsync("write/read run + response + derived", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ai-vis-"));
  const store = createAiVisibilityStore({ rootDir: root });
  const run = await store.saveRun({
    runId: "run_test_1",
    promptId: "p1",
    promptVersion: "1",
    provider: "openai",
    model: "fixture",
    startedAt: new Date().toISOString(),
    status: "completed",
  });
  assert.equal(run.runId, "run_test_1");
  assert.equal((await store.getRun("run_test_1")).promptId, "p1");

  const response = await store.saveResponse({
    responseId: "resp_test_1",
    runId: run.runId,
    promptId: "p1",
    provider: "openai",
    model: "fixture",
    text: "Hello JW Marriott",
    citations: [],
    raw: { keep: true },
    createdAt: new Date().toISOString(),
    parserVersion: "v1",
  });
  assert.equal((await store.getResponse("resp_test_1")).raw.keep, true);

  await store.saveMentions("resp_test_1", [{ mentionId: "m1" }]);
  await store.saveCitations("resp_test_1", [{ citationId: "c1" }]);
  assert.equal((await store.getMentions("resp_test_1")).length, 1);
  assert.equal((await store.getCitations("resp_test_1")).length, 1);

  const ev = await store.saveEvidence({ evidenceId: "ev1", responseId: "resp_test_1" });
  assert.equal((await store.getEvidence("ev1")).evidenceId, "ev1");
  assert.notEqual(store.generateId("run"), store.generateId("run"));
});

console.log("\nNormalization");
test("normalized provider output", () => {
  const fx = readFx("provider-brand-recommendations.json");
  const n = normalizeProviderResponse({
    runId: "run_x",
    promptId: "p_x",
    providerResult: fx.providerResult,
    responseId: "resp_x",
  });
  assert.equal(n.ok, true);
  assert.equal(n.response.provider, "openai");
  assert.ok(n.response.text.includes("Curio Collection"));
  assert.equal(n.response.citations.length, 2);
});

test("malformed provider response", () => {
  const n = normalizeProviderResponse({
    runId: "run_x",
    promptId: "p_x",
    providerResult: null,
  });
  assert.equal(n.ok, false);
  assert.equal(n.error.type, "malformed_response");
});

console.log("\nBrand matching");
test("canonical JW Marriott", () => {
  const r = resolveEntityMention("JW Marriott", entityIndex);
  assert.equal(r.canonicalEntityId, "recBrandJwMarriottFixture");
});

test("JW Marriott ≠ Marriott Hotels", () => {
  const mentions = extractMentions({
    responseId: "r1",
    text: "JW Marriott is frequently discussed.",
    entityIndex,
  });
  const ids = mentions.map((m) => m.canonicalEntityId);
  assert.ok(ids.includes("recBrandJwMarriottFixture"));
  assert.ok(!ids.includes("recBrandMarriottHotelsFixture"));
});

test("parent company ≠ brand collapse", () => {
  const fx = readFx("provider-parent-vs-brand.json");
  const mentions = extractMentions({
    responseId: "r1",
    text: fx.providerResult.text,
    entityIndex,
  });
  const ids = new Set(mentions.map((m) => m.canonicalEntityId));
  assert.ok(ids.has("recBrandJwMarriottFixture"));
  assert.ok(ids.has("recParentMarriottIntlFixture"));
  assert.ok(!ids.has("recBrandMarriottHotelsFixture"));
});

test("alias Autograph Collection Hotels", () => {
  const r = resolveEntityMention("Autograph Collection Hotels", entityIndex);
  assert.equal(r.canonicalEntityId, "recBrandAutographFixture");
});

test("false-positive bare Marriott avoided", () => {
  const mentions = extractMentions({
    responseId: "r1",
    text: "Some owners prefer Marriott for loyalty reasons.",
    entityIndex,
  });
  assert.equal(mentions.length, 0);
});

console.log("\nOperator matching");
test("canonical Arbor Lodging", () => {
  const r = resolveEntityMention("Arbor Lodging", entityIndex, { entityType: "operator" });
  assert.equal(r.canonicalEntityId, "recOpArborFixture");
});

test("Aimbridge alias", () => {
  const r = resolveEntityMention("Aimbridge", entityIndex);
  assert.equal(r.canonicalEntityId, "recOpAimbridgeFixture");
});

test("unresolved operator stays unresolved", () => {
  const fx = readFx("provider-unresolved-operator.json");
  const mentions = extractMentions({
    responseId: "r1",
    text: fx.providerResult.text,
    entityIndex,
  });
  assert.equal(mentions.length, 0);
  const r = resolveEntityMention("Contoso Hospitality Partners", entityIndex);
  assert.equal(r.entityType, "unresolved");
  assert.equal(r.canonicalEntityId, null);
});

console.log("\nCitation parsing");
test("provider supplied + first-party domain", () => {
  const fx = readFx("provider-brand-recommendations.json");
  const citations = extractCitations({
    responseId: "r1",
    providerCitations: fx.providerResult.citations,
    entities,
  });
  assert.equal(citations.length, 2);
  assert.equal(citations[0].providerSupplied, true);
  assert.equal(citations[0].domain, "hilton.com");
  assert.equal(citations[0].firstParty, true);
  assert.equal(citations[0].entityAssociation, "recBrandCurioFixture");
});

test("missing citations", () => {
  const citations = extractCitations({ responseId: "r1", providerCitations: [], entities });
  assert.equal(citations.length, 0);
});

test("malformed URL", () => {
  const fx = readFx("provider-malformed-citation-url.json");
  const citations = extractCitations({
    responseId: "r1",
    providerCitations: fx.providerResult.citations,
    entities,
  });
  assert.equal(citations[0].domain, null);
  assert.equal(citations[0].firstParty, null);
  assert.equal(parseDomain("not-a-valid-url"), null);
});

console.log("\nMetrics");
test("presence / share / first / citation on ranked fixture", () => {
  const fx = readFx("provider-ranked-brands.json");
  const mentions = extractMentions({
    responseId: "r1",
    text: fx.providerResult.text,
    entityIndex,
  });
  const obs = buildObservationFromExtractions({
    observationId: "o1",
    promptId: "p_rank",
    success: true,
    mentions,
    citations: [],
  });
  assert.equal(obs.recommendedEntityIds[0], "recBrandCurioFixture");
  assert.equal(obs.recommendedEntityIds[1], "recBrandAutographFixture");
  assert.equal(obs.recommendedEntityIds[2], "recBrandTributeFixture");

  const presence = computeAiPresenceRate([obs], "recBrandCurioFixture");
  assert.equal(presence.value, 1);
  assert.equal(presence.metricVersion, METRIC_VERSION);

  const share = computeRecommendationShare([obs], "recBrandCurioFixture");
  assert.equal(share.numerator, 1);
  assert.equal(share.denominator, 4);

  const first = computeFirstRecommendationRate([obs], "recBrandCurioFixture");
  assert.equal(first.value, 1);

  const missing = computeQuestionsMissing([obs], "recBrandHyattRegencyFixture", ["p_rank"]);
  assert.deepEqual(missing.missingPromptIds, ["p_rank"]);
});

test("questions won ties are not sole wins", () => {
  const fx = readFx("metrics-tie-case.json");
  const curio = computeQuestionsWon(fx.observations, "recBrandCurioFixture");
  const hyatt = computeQuestionsWon(fx.observations, "recBrandHyattRegencyFixture");
  assert.equal(curio.count, 0);
  assert.equal(hyatt.count, 0);
  assert.deepEqual(curio.tiedPromptIds, ["p_tie"]);
  assert.deepEqual(hyatt.tiedPromptIds, ["p_tie"]);
});

test("competitive position ranks by presence", () => {
  const observations = [
    {
      observationId: "a",
      promptId: "p1",
      success: true,
      presentEntityIds: ["recBrandCurioFixture", "recBrandHyattRegencyFixture"],
      recommendedEntityIds: ["recBrandCurioFixture"],
      firstPartyCitationEntityIds: ["recBrandCurioFixture"],
    },
    {
      observationId: "b",
      promptId: "p1",
      success: true,
      presentEntityIds: ["recBrandCurioFixture"],
      recommendedEntityIds: ["recBrandCurioFixture"],
      firstPartyCitationEntityIds: [],
    },
  ];
  const rank = computeCompetitivePosition(observations, [
    "recBrandCurioFixture",
    "recBrandHyattRegencyFixture",
    "recBrandAutographFixture",
  ]);
  assert.equal(rank.rankingMetric, "ai_presence_rate");
  assert.equal(rank.peers[0].entityId, "recBrandCurioFixture");
  assert.equal(rank.peers[0].rank, 1);
  assert.equal(rank.peers[1].entityId, "recBrandHyattRegencyFixture");
  assert.equal(rank.peers[2].presenceRate, 0);

  const cite = computeCitationRate(observations, "recBrandCurioFixture");
  assert.equal(cite.value, 0.5);
});

test("no target entity → presence 0", () => {
  const fx = readFx("provider-no-target.json");
  const mentions = extractMentions({
    responseId: "r1",
    text: fx.providerResult.text,
    entityIndex,
  });
  const obs = buildObservationFromExtractions({
    observationId: "o1",
    promptId: "p1",
    success: true,
    mentions,
    citations: [],
  });
  assert.equal(computeAiPresenceRate([obs], "recBrandCurioFixture").value, 0);
});

console.log("\nEvidence");
test("metric traces to evidence payload", () => {
  const prompt = readFx("prompt-brand-mexico.json");
  const fx = readFx("provider-brand-recommendations.json");
  const n = normalizeProviderResponse({
    runId: "run_e",
    promptId: prompt.promptId,
    providerResult: fx.providerResult,
    responseId: "resp_e",
  });
  const mentions = extractMentions({
    responseId: "resp_e",
    text: n.response.text,
    entityIndex,
  });
  const citations = extractCitations({
    responseId: "resp_e",
    providerCitations: n.response.citations,
    entities,
  });
  const obs = buildObservationFromExtractions({
    observationId: "o_e",
    promptId: prompt.promptId,
    success: true,
    mentions,
    citations,
  });
  const presence = computeAiPresenceRate([obs], "recBrandCurioFixture");
  const evidence = assembleEvidenceRecord({
    prompt,
    run: { runId: "run_e", promptVersion: prompt.version, status: "completed" },
    response: n.response,
    mentions,
    citations,
    metrics: { presence },
  });
  assert.equal(evidence.promptId, prompt.promptId);
  assert.equal(evidence.metricVersion, METRIC_VERSION);
  assert.ok(evidence.payload.rawResponseText.includes("Curio"));
  assert.equal(evidence.mentionIds.length, mentions.length);
  const trace = metricEvidenceTrace({
    metricResult: presence,
    evidenceId: evidence.evidenceId,
    observationIds: [obs.observationId],
  });
  assert.equal(trace.formula.value, 1);
});

console.log("\nProvider errors");
test("429 rate limit normalization", () => {
  const err = normalizeProviderHttpError(429, "Too many requests");
  assert.ok(err instanceof ProviderError);
  assert.equal(err.type, "rate_limit");
  assert.equal(err.retryable, true);
});

test("timeout normalization", () => {
  const err = normalizeProviderHttpError(504, "Gateway timeout");
  assert.equal(err.type, "timeout");
});

await testAsync("openai adapter refuses missing key", async () => {
  await assert.rejects(
    () =>
      runOpenAi({
        prompt: { text: "test" },
        apiKey: "",
        enableWebSearch: false,
      }),
    (err) => err instanceof ProviderError && err.type === "config_error"
  );
});

await testAsync("openai adapter maps 429 via fetch mock", async () => {
  await assert.rejects(
    () =>
      runOpenAi({
        prompt: { text: "test" },
        apiKey: "sk-test-not-real",
        enableWebSearch: false,
        fetchImpl: async () => ({
          ok: false,
          status: 429,
          json: async () => ({ error: { message: "rate limited" } }),
        }),
      }),
    (err) => err.type === "rate_limit"
  );
});

await testAsync("openai adapter normalizes successful responses payload", async () => {
  const result = await runOpenAi({
    prompt: { text: "Which brands?" },
    apiKey: "sk-test-not-real",
    enableWebSearch: true,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        id: "resp_fixture",
        status: "completed",
        usage: { input_tokens: 10, output_tokens: 20, total_tokens: 30 },
        output: [
          { type: "web_search_call" },
          {
            type: "message",
            content: [
              {
                type: "output_text",
                text: "Consider Curio Collection.",
                annotations: [
                  {
                    type: "url_citation",
                    url: "https://www.hilton.com/en/curio/",
                    title: "Curio",
                    start_index: 0,
                    end_index: 10,
                  },
                ],
              },
            ],
          },
        ],
      }),
    }),
  });
  assert.equal(result.provider, "openai");
  assert.equal(result.citationCapability, "supported");
  assert.equal(result.citations.length, 1);
  assert.equal(result.citations[0].providerSupplied, true);
  assert.equal(result.usage.totalTokens, 30);
});

console.log(`\nResults: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
