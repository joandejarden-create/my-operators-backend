#!/usr/bin/env node
/**
 * Recommendation Hardening 2 + ground-truth invalidation regression tests.
 * HOLDOUT not accessed. No provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  classifyMentionRoleV3,
  detectRankMarker,
  assignFirstRecommendationAcrossMentionsV3,
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { findEntitySpans } from "../lib/ai-visibility/normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import {
  amendGoldenSetV2GroundTruth,
  AMENDMENT_ACTIONS,
} from "../lib/ai-visibility/validation/golden-set-ground-truth-amendment.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

console.log("Ground-Truth Amendment + Recommendation Hardening 2\n");

const index = buildGoldenSetScoringEntityIndex({});

test("EXPLICIT_STRONG_OPTION_IS_RECOMMENDATION", () => {
  const text = "For owners, Autograph Collection and Curio Collection by Hilton are strong options.";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
  });
  assert.equal(r.role, "explicit_recommendation");
});

test("RECOMMENDED_SHORTLIST_IS_RECOMMENDATION", () => {
  const text =
    "Recommended shortlist\n\n| Brand | Notes |\n| Autograph Collection | strong choice |";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
    promptIntentTerritory: "Conversion",
  });
  assert.ok(
    ["explicit_recommendation", "first_recommendation", "ranked_recommendation"].includes(r.role),
    r.role
  );
});

test("FIRST_RANKED_ENTITY_IS_FIRST_RECOMMENDATION", () => {
  const text =
    "| Priority | Brand |\n|---|---|\n| **1** | **Autograph Collection** |\n| **2** | **Curio Collection by Hilton** |\n";
  const mentions = extractMentions({
    responseId: "t",
    text,
    entityIndex: index.aliasIndex,
  });
  const auto = mentions.find((m) => m.canonicalEntityName === "Autograph Collection");
  assert.ok(auto);
  assert.equal(auto.role, "first_recommendation");
});

test("SECOND_RANKED_ENTITY_IS_RANKED_RECOMMENDATION", () => {
  const text =
    "| Priority | Brand |\n|---|---|\n| **1** | **Autograph Collection** |\n| **2** | **Curio Collection by Hilton** |\n";
  const mentions = extractMentions({
    responseId: "t",
    text,
    entityIndex: index.aliasIndex,
  });
  const curio = mentions.find((m) => m.canonicalEntityName === "Curio Collection by Hilton");
  assert.ok(curio);
  assert.equal(curio.role, "ranked_recommendation");
});

test("SPANISH_PRIMERA_OPCION_FIRST_RECOMMENDATION", () => {
  const text = "La primera opción para conversiones es Autograph Collection.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
    promptIntentTerritory: "Conversion",
  });
  assert.equal(r.role, "first_recommendation");
});

test("SPANISH_RECOMENDADO_EXPLICIT_RECOMMENDATION", () => {
  const text = "Curio Collection by Hilton es una marca recomendada para lifestyle.";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
  });
  assert.equal(r.role, "explicit_recommendation");
});

test("ASSOCIATED_OPTION_NOT_OVERPROMOTED", () => {
  const text =
    "Branded residences are commonly associated with Autograph Collection in this market.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
    promptIntentTerritory: "Branded Residences",
  });
  assert.equal(r.role, "associated_option");
});

test("DISCUSSION_NOT_OVERPROMOTED", () => {
  const text = "Autograph Collection launched in 2010 and expanded soft-brand distribution.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "discussed");
});

test("COMPARATOR_NOT_RECOMMENDED", () => {
  const text = "Design Hotels is an alternative to Autograph Collection for boutique assets.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "comparator");
});

test("NEGATIVE_NOT_RECOMMENDED", () => {
  const text = "Autograph Collection is not recommended for this midscale conversion.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "negative_or_qualified");
});

test("QUESTION_STATUS_DERIVES_CONSISTENTLY", () => {
  const src = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/golden-set.js"),
    "utf8"
  );
  assert.ok(src.includes('role === "associated_option" || role === "passing_mention"'));
  assert.ok(src.includes('return "PRESENT"'));
  assert.ok(src.includes('return "FIRST_RECOMMENDED"'));
});

test("FULL_RESPONSE_HYDRATION_USED", () => {
  const hydrate = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/hydrate-golden-set-texts.js"),
    "utf8"
  );
  assert.ok(hydrate.includes("monitoring_store_rawText"));
  assert.ok(RECOMMENDATION_CLASSIFIER_VERSION.includes("v3"));
});

test("NO_CASE_SPECIFIC_RULES", () => {
  const clf = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/recommendation-classifier-v3.js"),
    "utf8"
  );
  assert.ok(!/v2_cand_/.test(clf));
  assert.ok(!/caseId\s*===\s*['\"]/.test(clf));
});

test("MARKDOWN_TABLE_RANK_OFFSETS_MAP_TO_ORIGINAL", () => {
  const text =
    "| Priority | Brand |\n|---|---|\n| **1** | **Autograph Collection** |\n";
  const spans = findEntitySpans(text, index.aliasIndex);
  const auto = spans.find((s) => s.entity.name === "Autograph Collection");
  assert.ok(auto);
  assert.equal(detectRankMarker(text, auto.start), 1);
  assert.ok(text.slice(auto.start, auto.end).includes("Autograph"));
});

test("INVALIDATED_CASES_EXCLUDED_FROM_ACTIVE_GOLDEN_SET", () => {
  const golden = loadGoldenSet();
  assert.ok(!golden.cases.some((c) => c.groundTruthInvalidated === true));
  assert.ok(!golden.cases.some((c) => c.reviewStatus === "INVALIDATED_CANDIDATE_SUBJECT"));
  const raw = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "fixtures/ai-visibility/ai-intelligence-golden-set-v2.json"),
      "utf8"
    )
  );
  const invalidated = (raw.cases || []).filter(
    (c) => c.groundTruthInvalidated === true || c.reviewStatus === "INVALIDATED_CANDIDATE_SUBJECT"
  );
  assert.equal(invalidated.length, 12);
  assert.ok(invalidated.every((c) => Array.isArray(c.labelAmendmentHistory) && c.labelAmendmentHistory.length >= 1));
});

test("GROUND_TRUTH_AMENDMENT_REQUIRES_HUMAN", () => {
  let threw = false;
  try {
    amendGoldenSetV2GroundTruth({
      caseId: "v2_cand_1674948a",
      action: AMENDMENT_ACTIONS.CORRECT_HUMAN_LABEL,
      amendedLabels: { expectedEntityPresent: false },
    });
  } catch (err) {
    threw = err.code === "REVIEWER_REQUIRED";
  }
  assert.ok(threw);
});

test("HOLDOUT_NOT_ACCESSED", () => {
  const reportPath = path.join(
    ROOT,
    "data/ai-visibility/validation/ground-truth-amendment-and-recommendation-hardening-2.json"
  );
  assert.ok(fs.existsSync(reportPath));
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  assert.equal(report.HOLDOUT_ACCESSED, false);
  assert.equal(report.HOLDOUT_CASES_INSPECTED, 0);
  assert.equal(report.HOLDOUT_METRICS_RUN, false);
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
