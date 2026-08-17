#!/usr/bin/env node
/**
 * Phase 2A tests — live loaders (mocked Airtable), entity index, classification.
 * No paid provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadLiveBrandEntities,
  selectBrandsByCanonicalNames,
} from "../lib/ai-visibility/load-brands-live.js";
import {
  loadLiveOperatorEntities,
  selectOperatorsByCanonicalNames,
  parseOperatorAliases,
} from "../lib/ai-visibility/load-operators-live.js";
import {
  buildAiVisibilityEntityIndex,
  buildFixtureAiVisibilityEntityIndex,
  ENTITY_INDEX_VERSION,
} from "../lib/ai-visibility/entity-index.js";
import {
  extractMentions,
  normalizeProviderResponse,
  assembleEvidenceRecord,
  buildObservationFromExtractions,
  computeAiPresenceRate,
  metricEvidenceTrace,
} from "../lib/ai-visibility/index.js";
import { classifyMentionRole } from "../lib/ai-visibility/mention-classification.js";
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
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("AI Visibility Phase 2A tests\n");

console.log("Live Brand Loader (mocked)");
await testAsync("canonical Active/Live load + alias attach", async () => {
  const result = await loadLiveBrandEntities({
    fetchBrandRecords: async () => [
      {
        id: "recJw",
        fields: {
          "Brand Name": "JW Marriott",
          "Parent Company": "Marriott International",
          "Brand Status": "Active",
        },
      },
      {
        id: "recMh",
        fields: {
          "Brand Name": "Marriott Hotels",
          "Parent Company": "Marriott International",
          "Brand Status": "Active",
        },
      },
      {
        id: "recDraft",
        fields: {
          "Brand Name": "Draft Brand",
          "Brand Status": "Draft",
        },
      },
    ],
    loadAliases: async () => [
      {
        canonicalBrandName: "JW Marriott",
        aliasSourceBrandName: "JW Marriott Hotels",
        parentCompany: "Marriott International",
        active: true,
      },
      {
        canonicalBrandName: "Not In Active Set",
        aliasSourceBrandName: "Ghost Alias",
        active: true,
      },
    ],
  });
  assert.equal(result.entities.length, 2);
  assert.equal(result.meta.aliasesAttached, 1);
  const jw = result.entities.find((e) => e.id === "recJw");
  assert.ok(jw.aliases.includes("JW Marriott Hotels"));
  assert.ok(result.meta.unmatchedAliasCanonicalSample.includes("Not In Active Set"));
});

await testAsync("parent collision prevention via longest match", async () => {
  const result = await loadLiveBrandEntities({
    fetchBrandRecords: async () => [
      {
        id: "recJw",
        fields: { "Brand Name": "JW Marriott", "Parent Company": "Marriott International", "Brand Status": "Live" },
      },
      {
        id: "recMh",
        fields: { "Brand Name": "Marriott Hotels", "Parent Company": "Marriott International", "Brand Status": "Active" },
      },
      {
        id: "recParent",
        fields: { "Brand Name": "Marriott International", "Brand Status": "Active" },
      },
    ],
    loadAliases: async () => [],
  });
  const index = buildAiVisibilityEntityIndex({ brands: result.entities, operators: [] });
  const mentions = extractMentions({
    responseId: "r1",
    text: "JW Marriott is frequently discussed for luxury conversions.",
    entityIndex: index.aliasIndex,
  });
  const ids = mentions.map((m) => m.canonicalEntityId);
  assert.ok(ids.includes("recJw"));
  assert.ok(!ids.includes("recMh"));
});

test("selectBrandsByCanonicalNames missing stays missing", () => {
  const { selected, missing } = selectBrandsByCanonicalNames(
    [{ id: "1", name: "Autograph Collection" }],
    ["Autograph Collection", "Hyatt Regency"]
  );
  assert.equal(selected.length, 1);
  assert.deepEqual(missing, ["Hyatt Regency"]);
});

console.log("\nLive Operator Loader (mocked)");
await testAsync("canonical Active load + aliases + unresolved field gaps", async () => {
  const result = await loadLiveOperatorEntities({
    fetchMasterRecords: async () => [
      {
        id: "recArbor",
        fields: {
          company_name: "Arbor Lodging (CALA)",
          "Operator Aliases": "Arbor Lodging; Arbor Lodging Partners",
          "Operator Website": "https://www.arborlodging.com",
          submission_status: "Active",
          "Record Purpose": "Production",
        },
      },
      {
        id: "recInactive",
        fields: {
          company_name: "Inactive Op",
          submission_status: "Draft",
          "Record Purpose": "Production",
        },
      },
      {
        id: "recTest",
        fields: {
          company_name: "QA Test Fixture Op",
          submission_status: "Active",
          "Record Purpose": "Test Fixture",
        },
      },
    ],
  });
  assert.equal(result.entities.length, 1);
  assert.equal(result.entities[0].aliases.length, 2);
  assert.deepEqual(result.entities[0].firstPartyDomains, ["arborlodging.com"]);
});

test("parseOperatorAliases", () => {
  assert.deepEqual(parseOperatorAliases("Aimbridge; Aimbridge Hospitality"), [
    "Aimbridge",
    "Aimbridge Hospitality",
  ]);
});

test("operator missing alias remains unresolved via resolve path", () => {
  const { selected, missing } = selectOperatorsByCanonicalNames(
    [{ id: "1", name: "Highgate" }],
    ["Highgate", "Contoso Hospitality"]
  );
  assert.equal(selected.length, 1);
  assert.deepEqual(missing, ["Contoso Hospitality"]);
});

console.log("\nUnified Entity Index");
test("deterministic ordering + fingerprint + duplicate protection", () => {
  const a = buildAiVisibilityEntityIndex({
    brands: [
      { id: "b2", name: "B Brand", entityType: "brand", aliases: [] },
      { id: "b1", name: "A Brand", entityType: "brand", aliases: ["A"] },
      { id: "b1", name: "A Brand", entityType: "brand", aliases: ["A"] },
    ],
    operators: [{ id: "o1", name: "Op One", entityType: "operator", aliases: [] }],
  });
  const b = buildAiVisibilityEntityIndex({
    brands: [
      { id: "b1", name: "A Brand", entityType: "brand", aliases: ["A"] },
      { id: "b2", name: "B Brand", entityType: "brand", aliases: [] },
    ],
    operators: [{ id: "o1", name: "Op One", entityType: "operator", aliases: [] }],
  });
  assert.equal(a.version, ENTITY_INDEX_VERSION);
  assert.equal(a.fingerprint, b.fingerprint);
  assert.equal(a.entities.length, 3);
  assert.equal(a.entities[0].entityType, "brand");
  assert.equal(a.entities[0].name, "A Brand");
  assert.equal(a.entities[0].sourceSystem, null);
});

test("fixture index does not require Airtable", () => {
  const fx = readFx("entity-universe.json");
  const index = buildFixtureAiVisibilityEntityIndex(fx);
  assert.ok(index.entities.length > 5);
});

console.log("\nProvider normalization (Responses + citations)");
await testAsync("web_search citations fixture", async () => {
  const result = await runOpenAi({
    prompt: { text: "Which brands?" },
    apiKey: "sk-test",
    enableWebSearch: true,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        id: "resp_1",
        usage: { input_tokens: 5, output_tokens: 7, total_tokens: 12 },
        output: [
          { type: "web_search_call" },
          {
            type: "message",
            content: [
              {
                type: "output_text",
                text: "Consider Autograph Collection.",
                annotations: [
                  {
                    type: "url_citation",
                    url: "https://www.marriott.com/autograph",
                    title: "Autograph",
                  },
                ],
              },
            ],
          },
        ],
      }),
    }),
  });
  assert.equal(result.citationCapability, "supported");
  assert.equal(result.citations.length, 1);
});

await testAsync("no citations fixture", async () => {
  const result = await runOpenAi({
    prompt: { text: "Which brands?" },
    apiKey: "sk-test",
    enableWebSearch: true,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        output: [{ type: "message", content: [{ type: "output_text", text: "It depends." }] }],
      }),
    }),
  });
  assert.equal(result.citationCapability, "unavailable");
  assert.equal(result.citations.length, 0);
});

console.log("\nRecommendation classification");
test("roles: recommend / comparator / passing", () => {
  assert.equal(
    classifyMentionRole({
      explicitRecommendation: true,
      recommendationPosition: 1,
      contextSnippet: "We recommend Autograph Collection for flexibility.",
    }),
    "first_recommendation"
  );
  assert.equal(
    classifyMentionRole({
      explicitRecommendation: false,
      contextSnippet: "…compared to Hilton Hotels & Resorts…",
    }),
    "comparator"
  );
  assert.equal(
    classifyMentionRole({
      explicitRecommendation: false,
      contextSnippet: "…such as Hotel Indigo in urban markets…",
    }),
    "passing_mention"
  );
});

console.log("\nEvidence live-shaped fixture");
test("metric → evidence chain", () => {
  const prompt = readFx("prompt-brand-mexico.json");
  const fx = readFx("provider-brand-recommendations.json");
  // Remap fixture names to phase2a live-like Curio Collection by Hilton style via fixture universe
  const universe = readFx("entity-universe.json");
  const index = buildFixtureAiVisibilityEntityIndex(universe);
  const n = normalizeProviderResponse({
    runId: "run_p2a",
    promptId: prompt.promptId,
    providerResult: fx.providerResult,
    responseId: "resp_p2a",
  });
  const mentions = extractMentions({
    responseId: "resp_p2a",
    text: n.response.text,
    entityIndex: index.aliasIndex,
  });
  const obs = buildObservationFromExtractions({
    observationId: "obs_p2a",
    promptId: prompt.promptId,
    success: true,
    mentions,
    citations: [],
  });
  const presence = computeAiPresenceRate([obs], "recBrandCurioFixture");
  const evidence = assembleEvidenceRecord({
    prompt,
    run: { runId: "run_p2a", promptVersion: "1", status: "completed" },
    response: n.response,
    mentions,
    citations: [],
    metrics: { presence },
  });
  const trace = metricEvidenceTrace({
    metricResult: presence,
    evidenceId: evidence.evidenceId,
    observationIds: [obs.observationId],
  });
  assert.equal(trace.evidenceId, evidence.evidenceId);
  assert.ok(evidence.payload.rawResponseText);
});

console.log("\nCohort config integrity");
test("phase2a cohort has prompts and exact entity lists", () => {
  const cohort = readFx("phase2a-cohort.json");
  assert.ok(cohort.brands.length >= 10);
  assert.ok(cohort.operators.length >= 5);
  assert.ok(cohort.prompts.length >= 10 && cohort.prompts.length <= 20);
  assert.ok(cohort.label.includes("NOT PRODUCTION BENCHMARK"));
});

console.log(`\nResults: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
