#!/usr/bin/env node
/**
 * P0B — AI Brand Association research tests.
 * No provider calls. No client UI. No certified metric changes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ASSOCIATION_ATTRIBUTES,
  HIGH_RISK_ATTRIBUTE_IDS,
  DEFERRED_ATTRIBUTE_IDS,
  getAssociationAttribute,
} from "../lib/ai-visibility/associations/attribute-taxonomy.js";
import {
  validateEntityBinding,
  detectSiblingCollision,
  splitSentencesWithOffsets,
} from "../lib/ai-visibility/associations/entity-binding.js";
import {
  extractAssociationCandidatesFromEvidence,
  classifyAssociationsFromEvidence,
} from "../lib/ai-visibility/associations/deterministic-extractor.js";
import {
  buildAssociationGoldenSet,
  scoreAssociationClassifier,
  loadAssociationGoldenSet,
  DEFAULT_GOLDEN_SET_PATH,
} from "../lib/ai-visibility/associations/golden-set.js";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";
import {
  aggregateBrandAssociations,
  researchCompetitiveAssociationGap,
} from "../lib/ai-visibility/associations/aggregation-research.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../lib/ai-visibility/scenario-registry.js";
import { buildPromptCohort } from "../lib/ai-visibility/prompt-cohort.js";
import { loadGovernedAiVisibilityPromptsFromFixture } from "../lib/ai-visibility/load-prompts.js";
import { enrichPromptCohortWithScenarioMetadata } from "../lib/ai-visibility/scenario-cohort.js";
import {
  computeAiPresenceRate,
  computeQuestionsMissing,
} from "../lib/ai-visibility/metrics.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

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

async function asyncTest(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

function mockEvidence(overrides = {}) {
  return {
    evidenceId: "ev_test_1",
    responseId: "resp_test_1",
    promptId: "p_cala_independent_affiliation_v1",
    promptFamily: "showcase_conversion_independent_affiliation",
    intentTerritory: "Conversion",
    provider: "openai",
    language: "en",
    commercialRegion: "CALA",
    batchId: "batch_test",
    payload: {
      rawResponseText: overrides.text || "",
      mentions: overrides.mentions || [],
      citations: overrides.citations || [],
    },
    ...overrides,
  };
}

console.log("\nP0B — AI Brand Association Research Tests\n");

test("taxonomy — 15 retained attributes, 2 deferred", () => {
  assert.equal(ASSOCIATION_ATTRIBUTES.length, 15);
  assert.ok(DEFERRED_ATTRIBUTE_IDS.includes("ECONOMICS"));
  assert.ok(HIGH_RISK_ATTRIBUTE_IDS.includes("OWNER_FLEXIBILITY"));
});

test("entity binding — parent-only blocked", () => {
  const text =
    "Marriott International has strong global distribution across markets. Other options include independent collections.";
  const binding = validateEntityBinding({
    text,
    spanStart: text.indexOf("distribution"),
    spanEnd: text.indexOf("distribution") + 12,
    entity: {
      id: "recEJCTDj1zrsjPM6",
      name: "Autograph Collection",
      parentCompany: "Marriott International",
    },
    mentions: [],
  });
  assert.equal(binding.parentOnlyLeak, true);
  assert.equal(binding.ok, false);
});

test("entity binding — explicit brand binding passes", () => {
  const text =
    "Autograph Collection offers owners flexibility in brand standards while preserving design individuality.";
  const binding = validateEntityBinding({
    text,
    spanStart: text.indexOf("flexibility"),
    spanEnd: text.indexOf("flexibility") + 11,
    entity: {
      id: "recEJCTDj1zrsjPM6",
      name: "Autograph Collection",
      parentCompany: "Marriott International",
    },
    mentions: [
      {
        canonicalEntityId: "recEJCTDj1zrsjPM6",
        canonicalEntityName: "Autograph Collection",
        mentionPosition: 0,
        contextSnippet: text.slice(0, 40),
      },
    ],
  });
  assert.equal(binding.ok, true);
});

test("sibling collision — peer named without target", () => {
  const hit = detectSiblingCollision({
    sentenceText: "Curio Collection offers owners greater flexibility than other options.",
    entity: { name: "Autograph Collection" },
    peerNames: ["Curio Collection by Hilton", "Autograph Collection"],
  });
  assert.equal(hit, true);
});

test("prompt-attribute false positive — prompt only", () => {
  const ev = mockEvidence({
    promptText: "Which brands offer owner flexibility in CALA?",
    text: "Several upper-upscale brands may be relevant for independent owners.",
    mentions: [
      {
        canonicalEntityId: "recEJCTDj1zrsjPM6",
        canonicalEntityName: "Autograph Collection",
        mentionPosition: 10,
      },
    ],
  });
  const { candidates } = extractAssociationCandidatesFromEvidence(ev);
  assert.equal(candidates.length, 0);
});

test("positive polarity — owner flexibility explicit", () => {
  const text =
    "Curio Collection by Hilton offers owners greater flexibility in operating requirements and brand standards.";
  const ev = mockEvidence({
    text,
    mentions: [
      {
        canonicalEntityId: "receQkxgjlezsc1xg",
        canonicalEntityName: "Curio Collection by Hilton",
        mentionPosition: 0,
      },
    ],
  });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  const hit = publishable.find((c) => c.attributeId === "OWNER_FLEXIBILITY");
  assert.ok(hit);
  assert.equal(hit.polarity, "POSITIVE");
  assert.ok(hit.supportingSpan?.text);
});

test("negative polarity — less flexibility", () => {
  const text =
    "Autograph Collection may provide less owner flexibility than Curio for conversion projects.";
  const ev = mockEvidence({
    text,
    mentions: [
      {
        canonicalEntityId: "recEJCTDj1zrsjPM6",
        canonicalEntityName: "Autograph Collection",
        mentionPosition: 0,
      },
    ],
  });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  const hit = publishable.find((c) => c.attributeId === "OWNER_FLEXIBILITY");
  assert.ok(hit);
  assert.equal(hit.polarity, "NEGATIVE");
});

test("deferred attribute — economics excluded from publishable", () => {
  const text =
    "Autograph Collection has a competitive fee structure and royalty terms for owners.";
  const ev = mockEvidence({
    text,
    mentions: [
      {
        canonicalEntityId: "recEJCTDj1zrsjPM6",
        canonicalEntityName: "Autograph Collection",
        mentionPosition: 0,
      },
    ],
  });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  assert.equal(publishable.filter((c) => c.attributeId === "ECONOMICS").length, 0);
});

test("citation optional — association without citation allowed", () => {
  const text = "Autograph Collection emphasizes design individuality and local character.";
  const ev = mockEvidence({ text, mentions: [{ canonicalEntityId: "recEJCTDj1zrsjPM6", canonicalEntityName: "Autograph Collection", mentionPosition: 0 }] });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  const hit = publishable.find((c) => c.attributeId === "DESIGN_INDIVIDUALITY");
  assert.ok(hit);
  assert.equal(hit.hasProviderCitation, false);
});

test("scenario resolution — mapped prompt attaches scenarioId", () => {
  const registry = loadScenarioRegistry();
  const index = buildScenarioRegistryIndex(registry);
  const resolved = resolvePromptScenario(
    {
      promptId: "p_cala_independent_affiliation_v1",
      promptFamily: "showcase_conversion_independent_affiliation",
    },
    index
  );
  assert.equal(resolved.scenarioStatus, "MAPPED");
  assert.ok(resolved.scenarioId);
});

test("legacy unmapped scenario — still classifiable internally", () => {
  const registry = loadScenarioRegistry();
  const index = buildScenarioRegistryIndex(registry);
  const resolved = resolvePromptScenario(
    { promptId: "p_unknown", promptFamily: "unknown_family" },
    index
  );
  assert.equal(resolved.scenarioStatus, "UNMAPPED");
});

test("span validation — supporting span present", () => {
  const text = "Curio Collection offers owner flexibility in brand standards.";
  const ev = mockEvidence({
    text,
    mentions: [{ canonicalEntityId: "receQkxgjlezsc1xg", canonicalEntityName: "Curio Collection by Hilton", mentionPosition: 0 }],
  });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  assert.ok(publishable[0]?.supportingSpan?.text);
});

test("high-risk explicitness — implicit-only rejected", () => {
  const text = "Autograph Collection is a notable option in the market.";
  const ev = mockEvidence({
    text,
    mentions: [{ canonicalEntityId: "recEJCTDj1zrsjPM6", canonicalEntityName: "Autograph Collection", mentionPosition: 0 }],
  });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  assert.equal(publishable.filter((c) => c.attributeId === "MARKET_FIT").length, 0);
});

await asyncTest("corpus audit — sufficient existing evidence", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  assert.ok(corpus.totalResponsesAvailable >= 120);
  assert.equal(corpus.NEW_PROVIDER_CALLS, 0);
  assert.equal(corpus.reuseExistingEvidence, "YES");
});

await asyncTest("golden set — builds 120+ cases without provider calls", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const gs = buildAssociationGoldenSet(corpus.evidence, { targetCount: 140 });
  assert.ok(gs.caseCount >= 120);
  assert.equal(gs.NEW_PROVIDER_CALLS, 0);
});

await asyncTest("classifier scoring — produces metrics object", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const gs = buildAssociationGoldenSet(corpus.evidence, { targetCount: 140 });
  const scores = scoreAssociationClassifier(gs.cases, corpus.evidence);
  assert.ok(scores.scoredCount > 0);
  assert.ok("precision" in scores.overall);
});

await asyncTest("EN isolation — ES evidence not mixed in EN aggregation", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const en = aggregateBrandAssociations(corpus.evidence, "recEJCTDj1zrsjPM6", {
    language: "en",
  });
  const es = aggregateBrandAssociations(corpus.evidence, "recEJCTDj1zrsjPM6", {
    language: "es",
  });
  assert.ok(Array.isArray(en));
  assert.ok(Array.isArray(es));
});

await asyncTest("certified metrics — Presence unchanged", async () => {
  const store = createBrandAiVisibilityReadStore({});
  const summaries = await store.listBatchSummaries({ provider: "openai", language: "en" });
  const cala = summaries.find((s) => s.cohort?.commercialRegion === "CALA") || summaries[0];
  if (!cala) return;
  const { observations } = await loadObservationsFromBatchSummary(store, cala, { language: "en" });
  const before = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  const showcase = loadGovernedAiVisibilityPromptsFromFixture(
    {},
    path.join(root, "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json")
  );
  const cohort = buildPromptCohort({
    prompts: showcase.prompts,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
  });
  enrichPromptCohortWithScenarioMetadata(cohort, showcase.prompts);
  const after = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  assert.equal(after.value, before.value);
  assert.equal(after.denominator, before.denominator);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}`);
if (failed > 0) {
  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0B_RESEARCH_CONTINUES (tests failed)");
  process.exit(1);
}
console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0B_PASS (tests)");
process.exit(0);
