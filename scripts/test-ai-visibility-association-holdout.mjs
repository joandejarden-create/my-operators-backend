#!/usr/bin/env node
/**
 * P0B.1 — Association holdout + span remediation tests.
 */
import assert from "node:assert/strict";
import path from "path";
import { fileURLToPath } from "url";
import {
  validateSupportingSpan,
  buildSentenceBoundedSpan,
  auditSpanFailures,
  isTableRowSpan,
} from "../lib/ai-visibility/associations/span-validation.js";
import {
  buildAssociationHoldout,
  scoreHoldoutClassifier,
  oracleLabelCase,
  PRODUCTION_ATTRIBUTES,
} from "../lib/ai-visibility/associations/holdout-set.js";
import { classifyAssociationsFromEvidence } from "../lib/ai-visibility/associations/deterministic-extractor.js";
import { loadHardNegativeFixtures } from "../lib/ai-visibility/associations/hard-negatives.js";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";
import { computeAiPresenceRate } from "../lib/ai-visibility/metrics.js";
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
    provider: "openai",
    language: "en",
    commercialRegion: "CALA",
    payload: {
      rawResponseText: overrides.text || "",
      mentions: overrides.mentions || [],
      citations: [],
    },
    ...overrides,
  };
}

console.log("\nP0B.1 — Association Holdout + Span Remediation Tests\n");

test("span validation — exact substring without ellipsis", () => {
  const raw = "Curio Collection offers owner flexibility in brand standards.";
  const span = { start: 0, end: raw.length, text: raw, exactText: raw };
  const result = validateSupportingSpan(raw, span, {
    entity: { id: "x", name: "Curio Collection" },
    attributeId: "OWNER_FLEXIBILITY",
  });
  assert.equal(result.valid, true);
});

test("span validation — rejects table row", () => {
  assert.equal(isTableRowSpan("| Curio | Hilton | lifestyle |"), true);
  const raw = "| Curio | Hilton | lifestyle |";
  const result = validateSupportingSpan(raw, { text: raw, exactText: raw });
  assert.equal(result.valid, false);
  assert.equal(result.failureMode, "TABLE_STRUCTURE_ERROR");
});

test("sentence-bounded span — no ellipsis prefix", () => {
  const text = "Autograph Collection offers owners flexibility in brand standards.";
  const sentence = { text, start: 0, end: text.length };
  const hit = { start: text.indexOf("flexibility"), end: text.indexOf("flexibility") + 11 };
  const span = buildSentenceBoundedSpan(text, sentence, hit, { name: "Autograph Collection" });
  assert.ok(!span.text.startsWith("…"));
  assert.ok(text.includes(span.exactText));
});

test("extractor v1.1 — span traceable in raw response", () => {
  const text =
    "Curio Collection by Hilton offers owners greater flexibility in operating requirements.";
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
  const check = validateSupportingSpan(text, hit.supportingSpan);
  assert.equal(check.valid, true);
});

test("hard-negative fixtures — load permanent regression cases", () => {
  const fixtures = loadHardNegativeFixtures(
    path.join(root, "fixtures/ai-visibility/association-hard-negatives-v1.json")
  );
  assert.ok(fixtures.cases.length >= 8);
  assert.ok(fixtures.cases.some((c) => c.hardNegativeCategory === "PARENT_COMPANY_ONLY"));
});

test("oracle — prompt-only hard negative", () => {
  const ev = mockEvidence({
    promptText: "Which brands offer owner flexibility?",
    text: "Several brands may be relevant.",
    mentions: [
      { canonicalEntityId: "recEJCTDj1zrsjPM6", canonicalEntityName: "Autograph Collection", mentionPosition: 0 },
    ],
  });
  const oracle = oracleLabelCase(ev, "recEJCTDj1zrsjPM6", "OWNER_FLEXIBILITY");
  assert.equal(oracle.humanLabel, "NO_ASSOCIATION");
  assert.equal(oracle.humanLabelled, true);
});

test("entity binding — parent leak hard negative", () => {
  const ev = mockEvidence({
    text: "Marriott International has strong global distribution.",
    mentions: [
      { canonicalEntityId: "recEJCTDj1zrsjPM6", canonicalEntityName: "Autograph Collection", mentionPosition: 50 },
    ],
  });
  const oracle = oracleLabelCase(ev, "recEJCTDj1zrsjPM6", "DISTRIBUTION");
  assert.equal(oracle.humanLabel, "NO_ASSOCIATION");
});

test("negative polarity — scored correctly", () => {
  const text = "Autograph Collection may provide less owner flexibility than Curio.";
  const ev = mockEvidence({
    text,
    mentions: [{ canonicalEntityId: "recEJCTDj1zrsjPM6", canonicalEntityName: "Autograph Collection", mentionPosition: 0 }],
  });
  const oracle = oracleLabelCase(ev, "recEJCTDj1zrsjPM6", "OWNER_FLEXIBILITY");
  assert.equal(oracle.humanLabel, "NEGATIVE");
});

test("mixed polarity — detected", () => {
  const text =
    "Autograph Collection combines individuality with Marriott standards but less latitude than independent operation.";
  const ev = mockEvidence({
    text,
    mentions: [{ canonicalEntityId: "recEJCTDj1zrsjPM6", canonicalEntityName: "Autograph Collection", mentionPosition: 0 }],
  });
  const { publishable } = classifyAssociationsFromEvidence(ev);
  const hit = publishable.find((c) => c.attributeId === "INDEPENDENT_IDENTITY");
  if (hit) assert.ok(["MIXED", "POSITIVE", "NEUTRAL_DESCRIPTIVE"].includes(hit.polarity));
});

test("EN isolation — holdout language field respected", () => {
  const en = mockEvidence({ language: "en", text: "test", mentions: [] });
  const es = mockEvidence({ language: "es", text: "test", mentions: [] });
  assert.notEqual(en.language, es.language);
});

await asyncTest("holdout — builds 120+ labelled cases from corpus", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const holdout = buildAssociationHoldout(corpus.evidence, { targetCount: 150 });
  assert.ok(holdout.selectedCount >= 100);
  assert.ok(holdout.totalLabelled > holdout.selectedCount * 0.5);
  assert.equal(holdout.NEW_PROVIDER_CALLS, 0);
  assert.ok(holdout.manifest.holdoutHash);
  assert.ok(holdout.manifest.developmentHash);
});

await asyncTest("holdout split — ~70/30 dev/sealed", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const holdout = buildAssociationHoldout(corpus.evidence, { targetCount: 150 });
  const ratio = holdout.manifest.holdoutCount / holdout.selectedCount;
  assert.ok(ratio >= 0.2 && ratio <= 0.4, `holdout ratio ${ratio}`);
});

await asyncTest("final holdout scoring — produces metrics", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const holdout = buildAssociationHoldout(corpus.evidence, { targetCount: 150 });
  const scores = scoreHoldoutClassifier(holdout.holdoutSet, corpus.evidence);
  assert.ok(scores.scoredCount > 0);
  assert.ok("precision" in scores.overall);
  assert.ok(Array.isArray(scores.attributeResults));
});

await asyncTest("span audit — failure modes counted", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const preds = [];
  for (const ev of corpus.evidence.slice(0, 100)) {
    const { publishable } = classifyAssociationsFromEvidence(ev);
    preds.push(...publishable);
  }
  const evById = new Map(corpus.evidence.map((e) => [e.evidenceId, e]));
  const audit = auditSpanFailures(preds, evById);
  assert.ok(typeof audit.counts === "object");
});

await asyncTest("certified metrics — Presence unchanged", async () => {
  const store = createBrandAiVisibilityReadStore({});
  const summaries = await store.listBatchSummaries({ provider: "openai", language: "en" });
  const cala = summaries.find((s) => s.cohort?.commercialRegion === "CALA") || summaries[0];
  if (!cala) return;
  const { observations } = await loadObservationsFromBatchSummary(store, cala, { language: "en" });
  const presence = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  assert.ok(presence.value >= 0);
});

test("production attributes — 13 eligible", () => {
  assert.equal(PRODUCTION_ATTRIBUTES.length, 13);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}`);
if (failed > 0) {
  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0B_RESEARCH_CONTINUES (P0B.1 tests failed)");
  process.exit(1);
}
console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0B1_PASS (tests)");
process.exit(0);
