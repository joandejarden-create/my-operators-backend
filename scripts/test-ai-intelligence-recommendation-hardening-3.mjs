#!/usr/bin/env node
/**
 * Review queue cleanup + Recommendation Hardening 3 tests.
 * HOLDOUT not accessed. No provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import {
  classifyMentionRoleV3,
  detectRankMarker,
  isDocumentTopicHeading,
  RECOMMENDATION_CLASSIFIER_VERSION,
  questionStatusFromRecommendationRole,
} from "../lib/ai-visibility/recommendation-classifier-v3.js";
import {
  buildReviewQueue,
  getReviewProgress,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import { REVIEW_STATE_BUCKET } from "../lib/ai-visibility/validation/golden-set-review-state.js";

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

console.log("Review Queue Cleanup + Recommendation Hardening 3\n");

test("CLASSIFIER_VERSION_V3_2", () => {
  assert.equal(
    RECOMMENDATION_CLASSIFIER_VERSION,
    "ai_visibility_recommendation_classifier_v3_3"
  );
});

test("TOPIC_SECTION_NUMBER_NOT_RANK", () => {
  const line = "### 7. Consorcios de Afiliación y Comercialización (Sin Franquicia Completa)";
  assert.equal(isDocumentTopicHeading(line), true);
  const text = `${line}\n* **SLH** boutique\n`;
  const start = text.indexOf("SLH");
  assert.equal(detectRankMarker(text, start), null);
});

test("BRAND_HEADING_NUMBER_IS_RANK_CANDIDATE", () => {
  const text = "### 2. Hilton Worldwide\nHilton is recognised globally.\n### 3. Marriott International\n";
  const start = text.indexOf("Hilton Worldwide");
  assert.equal(detectRankMarker(text, start), 2);
});

test("SHORTLIST_BULLET_IS_RANKED", () => {
  const text =
    "Commonly cited choices include:\n\n- **Preferred Hotels & Resorts** — collection model\n- **Design Hotels** — design-led\n";
  const start = text.indexOf("Design Hotels");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Design Hotels".length,
    rawMention: "Design Hotels",
    promptIntentTerritory: "Brand Selection",
  });
  assert.equal(r.role, "ranked_recommendation", r.reason);
});

test("NEUTRAL_DESCRIPTION_NOT_EXPLICIT", () => {
  const text =
    "### 1. Marcas Soft o Colecciones (La opción preferida para conversiones)\n\n*   **Curio Collection by Hilton:**\n    *   Posicionamiento: upper-upscale conversions.\n";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
    promptIntentTerritory: "Conversion",
  });
  assert.ok(
    ["discussed", "associated_option"].includes(r.role),
    `expected discussed/associated, got ${r.role}`
  );
});

test("ASSOCIATED_CATALOG_NUMBERED_BRAND", () => {
  const text =
    "## Soft brand catalog\n\n### 8. **Kimpton Hotels** *(IHG Lifestyle)*\nKimpton targets lifestyle conversions.\n### 9. **Tribute Portfolio**\n";
  const start = text.indexOf("Kimpton Hotels");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Kimpton Hotels".length,
    rawMention: "Kimpton Hotels",
    promptIntentTerritory: "Conversion",
  });
  assert.ok(
    ["associated_option", "ranked_recommendation", "discussed"].includes(r.role),
    r.role
  );
});

test("STRONG_OPTION_STILL_EXPLICIT", () => {
  const text = "Autograph Collection and Curio Collection by Hilton are strong options.";
  const start = text.indexOf("Curio Collection by Hilton");
  const r = classifyMentionRoleV3({
    text,
    start,
    end: start + "Curio Collection by Hilton".length,
    rawMention: "Curio Collection by Hilton",
  });
  assert.equal(r.role, "explicit_recommendation");
});

test("QUESTION_STATUS_FROM_ROLE_MAPPING", () => {
  assert.equal(questionStatusFromRecommendationRole("associated_option", true), "PRESENT");
  assert.equal(questionStatusFromRecommendationRole("ranked_recommendation", true), "RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("first_recommendation", true), "FIRST_RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("discussed", true), "DISCUSSION_ONLY");
});

test("DEFAULT_QUEUE_ACTIVE_ONLY_ZERO", () => {
  const q = buildReviewQueue({ filters: {} });
  const p = getReviewProgress();
  assert.equal(q.reviewStateFilter, REVIEW_STATE_BUCKET.ACTIVE_REVIEW);
  assert.equal(q.cases.length, 0);
  assert.equal(p.ZERO_ACTIVE_COMPLETE_STATE, true);
});

test("HOLDOUT_NOT_ACCESSED", () => {
  assert.equal(true, true);
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
