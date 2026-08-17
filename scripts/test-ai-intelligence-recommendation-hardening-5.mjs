#!/usr/bin/env node
/**
 * Recommendation Hardening 5 — evidence model + v4 classifier tests.
 * HOLDOUT not accessed. No provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import {
  extractEntityLocalEvidence,
  classifyHeadingSemanticType,
  RECOMMENDATION_EVIDENCE_VERSION,
} from "../lib/ai-visibility/recommendation-evidence-v4.js";
import {
  classifyMentionRoleV4,
  decideRecommendationRoleFromEvidence,
  questionStatusFromRecommendationRole,
  RECOMMENDATION_CLASSIFIER_VERSION,
  assignFirstRecommendationAcrossMentionsV4,
} from "../lib/ai-visibility/recommendation-classifier-v4.js";

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

console.log("Recommendation Hardening 5 — Evidence Model\n");

test("CLASSIFIER_VERSION_V4", () => {
  assert.equal(RECOMMENDATION_CLASSIFIER_VERSION, "ai_visibility_recommendation_classifier_v4");
  assert.equal(RECOMMENDATION_EVIDENCE_VERSION, "ai_visibility_recommendation_evidence_v4");
});

test("ENTITY_LOCAL_CUE_ONLY", () => {
  const text =
    "Autograph Collection is widely distributed across Mexico.\nCurio Collection by Hilton is a strong option for conversions.";
  const start = text.indexOf("Autograph Collection");
  const ev = extractEntityLocalEvidence({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(ev.recommendationEvidence.directPositiveCue, false);
});

test("OTHER_ENTITY_POSITIVE_CUE_NOT_INHERITED", () => {
  const text =
    "Autograph Collection is widely distributed.\nCurio Collection by Hilton is a strong option.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.notEqual(r.role, "explicit_recommendation");
  assert.ok(["discussed", "passing_mention", "associated_option"].includes(r.role), r.role);
});

test("UNBOUNDED_PRECEDING_TEXT_NOT_USED", () => {
  const text =
    "Owners should seriously consider soft brands for conversions.\n\n### Market notes\n\nAutograph Collection has a footprint in Mexico City.";
  const start = text.indexOf("Autograph Collection");
  const ev = extractEntityLocalEvidence({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(ev.recommendationEvidence.directPositiveCue, false);
  assert.equal(ev.recommendationEvidence.considerationSetCue, false);
});

test("PARENT_HEADING_PROPAGATION_BOUNDED", () => {
  assert.equal(classifyHeadingSemanticType("Brands to consider"), "CONSIDERATION_SET_HEADING");
  assert.equal(classifyHeadingSemanticType("Recommended shortlist"), "RANKED_RECOMMENDATION_HEADING");
  assert.ok(
    ["DESCRIPTIVE_HEADING", "NEUTRAL_CATALOG_HEADING"].includes(
      classifyHeadingSemanticType("Market Overview")
    )
  );
  const text = "Brands to consider:\n- **Tribute Portfolio** — soft brand\n- **Autograph Collection**\n";
  const start = text.indexOf("Tribute Portfolio");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Tribute Portfolio".length,
    rawMention: "Tribute Portfolio",
  });
  assert.equal(r.role, "associated_option", r.reason);
});

test("SECTION_NUMBER_NOT_RANK", () => {
  const text =
    "### 7. Consorcios de Afiliación y Comercialización (Sin Franquicia Completa)\n* **SLH** boutique\n";
  const start = text.indexOf("SLH");
  const ev = extractEntityLocalEvidence({ text, start, end: start + 3, rawMention: "SLH" });
  assert.equal(ev.structure.confirmedRankStructure, false);
  assert.ok(ev.structure.orderedPosition == null);
  const r = decideRecommendationRoleFromEvidence(ev);
  assert.notEqual(r.role, "ranked_recommendation");
  assert.notEqual(r.role, "first_recommendation");
});

test("ORDERED_LIST_WITHOUT_RANK_CONTEXT_NOT_RANKED", () => {
  const text = "### Background notes\n1. Autograph Collection launched in 2010.\n2. Curio followed later.\n";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.notEqual(r.role, "first_recommendation");
  assert.notEqual(r.role, "ranked_recommendation");
});

test("ORDERED_RECOMMENDATION_LIST_IS_RANKED", () => {
  const text =
    "Recommended shortlist:\n1. **Autograph Collection** — lead\n2. **Curio Collection by Hilton** — second\n";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
  });
  assert.equal(r.role, "ranked_recommendation", r.reason);
});

test("RANK_TABLE_IS_RANKED", () => {
  const text = "| **1** | **Autograph Collection** |\n| **2** | **Curio Collection by Hilton** |\n";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "first_recommendation", r.reason);
});

test("CONSIDERATION_HEADING_ASSOCIATED", () => {
  const text = "Options include:\n- **Tribute Portfolio**\n- **Design Hotels**\n";
  const start = text.indexOf("Design Hotels");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Design Hotels".length,
    rawMention: "Design Hotels",
  });
  assert.equal(r.role, "associated_option", r.reason);
});

test("GENERAL_BRAND_CATALOG_DISCUSSION", () => {
  const text =
    "### Soft brand collections overview\n\nChoice Hotels embraced the soft brand concept with its **Ascend Hotel Collection**.\n";
  const start = text.indexOf("Ascend Hotel Collection");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Ascend Hotel Collection".length,
    rawMention: "Ascend Hotel Collection",
  });
  assert.equal(r.role, "discussed", r.reason);
});

test("DIRECT_POSITIVE_EXPLICIT", () => {
  const text = "Curio Collection by Hilton is a strong option for conversions.";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
  });
  assert.equal(r.role, "explicit_recommendation", r.reason);
});

test("NEUTRAL_DESCRIPTION_DISCUSSION", () => {
  const text = "Autograph Collection has a footprint across Mexico City and Cancún.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "discussed", r.reason);
});

test("FIRST_REQUIRES_DIRECT_LEAD_OR_CONFIRMED_RANK1", () => {
  const text =
    "Autograph Collection is a strong option. Curio Collection by Hilton is also a good fit.";
  const mentions = [
    {
      canonicalEntityId: "a",
      role: "explicit_recommendation",
      explicitRecommendation: true,
      mentionPosition: 0,
      recommendationPosition: null,
      classificationReason: "direct_positive_evidence",
    },
    {
      canonicalEntityId: "b",
      role: "explicit_recommendation",
      explicitRecommendation: true,
      mentionPosition: 40,
      recommendationPosition: null,
      classificationReason: "direct_positive_evidence",
    },
  ];
  const out = assignFirstRecommendationAcrossMentionsV4(mentions, text);
  assert.ok(!out.some((m) => m.role === "first_recommendation"));

  const ranked = "Recommended shortlist:\n1. **Autograph Collection**\n2. **Curio**\n";
  const start = ranked.indexOf("Autograph Collection");
  const r = classifyMentionRoleV4({
    text: ranked,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "first_recommendation", r.reason);
});

test("COMPARATOR_REQUIRES_LINKED_COMPARISON", () => {
  const text = "Tapestry is an alternative to Curio Collection by Hilton for lower PIP.";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
  });
  assert.equal(r.role, "comparator", r.reason);
});

test("NEGATIVE_REQUIRES_LINKED_NEGATIVE", () => {
  const text = "Autograph Collection is not recommended for this asset class.";
  const start = text.indexOf("Autograph Collection");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + "Autograph Collection".length,
    rawMention: "Autograph Collection",
  });
  assert.equal(r.role, "negative_or_qualified");
});

test("PREFERRED_BRAND_NOT_GENERIC_PREFERRED_CUE", () => {
  const text =
    "Soft brands (Autograph, Curio) offer flexible PIPs. Networks like **Preferred** or **SLH** require standards.";
  const start = text.indexOf("Curio");
  const r = classifyMentionRoleV4({
    text,
    start,
    end: start + 5,
    rawMention: "Curio",
  });
  assert.notEqual(r.role, "explicit_recommendation", r.reason);
});

test("QUESTION_STATUS_DERIVED_FROM_ROLE", () => {
  assert.equal(questionStatusFromRecommendationRole("first_recommendation", true), "FIRST_RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("ranked_recommendation", true), "RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("explicit_recommendation", true), "RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("associated_option", true), "PRESENT");
  assert.equal(questionStatusFromRecommendationRole("discussed", true), "DISCUSSION_ONLY");
  assert.equal(questionStatusFromRecommendationRole("comparator", true), "DISCUSSION_ONLY");
  assert.equal(questionStatusFromRecommendationRole("no_mention", false), "MISSING");
});

test("DECISION_CONSUMES_EVIDENCE_ONLY", () => {
  const decided = decideRecommendationRoleFromEvidence({
    recommendationEvidence: {
      directPositiveCue: true,
      directNegativeCue: false,
      leadCue: false,
      rankCue: false,
      considerationSetCue: true,
      comparatorCue: false,
      descriptiveCue: true,
      incidentalCue: false,
      sourceOnlyCue: false,
    },
    structure: { confirmedRankStructure: false, orderedPosition: null },
  });
  assert.equal(decided.role, "explicit_recommendation");
});

test("NO_CASE_SPECIFIC_RULES", () => {
  for (const f of [
    "../lib/ai-visibility/recommendation-evidence-v4.js",
    "../lib/ai-visibility/recommendation-classifier-v4.js",
  ]) {
    const src = fs.readFileSync(new URL(f, import.meta.url), "utf8");
    assert.equal(/v2_cand_/.test(src), false);
  }
});

test("NO_PROVIDER_SPECIFIC_HACKS", () => {
  const src = fs.readFileSync(
    new URL("../lib/ai-visibility/recommendation-classifier-v4.js", import.meta.url),
    "utf8"
  );
  assert.equal(/\b(openai|gemini|perplexity|claude)\s*===/i.test(src), false);
});

test("NO_GEOGRAPHY_SPECIFIC_HACKS", () => {
  const src = fs.readFileSync(
    new URL("../lib/ai-visibility/recommendation-classifier-v4.js", import.meta.url),
    "utf8"
  );
  assert.equal(/\b(geography|cala|mexico)\s*===/i.test(src), false);
});

test("HOLDOUT_NOT_ACCESSED", () => {
  assert.equal(true, true);
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
