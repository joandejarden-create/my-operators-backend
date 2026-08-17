#!/usr/bin/env node
/**
 * Recommendation taxonomy review workflow tests.
 * No Apply to production Golden Set. No holdout. No provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import {
  validateTaxonomyReviewArtifact,
  getTaxonomyReviewReadySummary,
  buildTaxonomyReviewQueue,
  setTaxonomyReviewDecision,
  previewTaxonomyReviewApply,
  applyTaxonomyReviewDecisions,
  TAXONOMY_REVIEW_ACTIONS,
} from "../lib/ai-visibility/validation/recommendation-taxonomy-review.js";
import {
  questionStatusFromRecommendationRole,
  isDocumentTopicHeading,
  detectRankMarker,
} from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";

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

console.log("Recommendation Taxonomy Review\n");

const ready = getTaxonomyReviewReadySummary();
const validation = validateTaxonomyReviewArtifact();

test("TAXONOMY_REVIEW_ONLY_DEV_CASES", () => {
  assert.equal(validation.TOTAL_CASES, 52);
  assert.equal(validation.VALID, 52);
  assert.equal(validation.VALIDATION, "VALID");
  const queue = buildTaxonomyReviewQueue({ filter: "ALL" });
  assert.equal(queue.cases.length, 52);
  for (const c of queue.cases) {
    assert.notEqual(c.holdoutSplit, "holdout");
  }
});

test("TAXONOMY_REVIEW_EXCLUDES_HOLDOUT", () => {
  assert.equal(validation.HOLDOUT_CASES, 0);
  assert.equal(ready.HOLDOUT_CASES, 0);
  const golden = loadGoldenSet();
  const holdoutIds = new Set(
    (golden.cases || []).filter((c) => c.holdoutSplit === "holdout").map((c) => c.caseId)
  );
  for (const c of validation.cases) {
    assert.equal(holdoutIds.has(c.CASE_ID), false, c.CASE_ID);
  }
});

test("TAXONOMY_BULK_APPLY_REQUIRES_HUMAN", () => {
  assert.throws(
    () =>
      applyTaxonomyReviewDecisions({
        explicitApply: false,
        confirmToken: "APPLY_TAXONOMY_REVIEW",
        reviewer: "x",
      }),
    (err) => err.code === "EXPLICIT_APPLY_REQUIRED"
  );
  assert.throws(
    () =>
      applyTaxonomyReviewDecisions({
        explicitApply: true,
        confirmToken: "WRONG",
        reviewer: "test@dealality.com",
      }),
    (err) => err.code === "CONFIRM_TOKEN_REQUIRED"
  );
});

test("AMENDMENT_PRESERVES_ORIGINAL_LABEL", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "trx-"));
  const decisionsPath = path.join(tmp, "decisions.json");
  const opts = { decisionsPath };
  const row = validation.cases.find((c) => c.CURRENT_HUMAN_LABEL !== c.TAXONOMY_DECISION_PROPOSED);
  assert.ok(row);
  setTaxonomyReviewDecision(
    {
      caseId: row.CASE_ID,
      action: TAXONOMY_REVIEW_ACTIONS.ACCEPT_TAXONOMY_PROPOSAL,
      reviewer: "test@dealality.com",
    },
    opts
  );
  const again = validateTaxonomyReviewArtifact();
  const same = again.cases.find((c) => c.CASE_ID === row.CASE_ID);
  assert.equal(same.CURRENT_HUMAN_LABEL, row.CURRENT_HUMAN_LABEL);
});

test("QUESTION_STATUS_RECOMPUTED_FROM_FINAL_ROLE", () => {
  assert.equal(questionStatusFromRecommendationRole("first_recommendation", true), "FIRST_RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("ranked_recommendation", true), "RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("explicit_recommendation", true), "RECOMMENDED");
  assert.equal(questionStatusFromRecommendationRole("associated_option", true), "PRESENT");
  assert.equal(questionStatusFromRecommendationRole("passing_mention", true), "PRESENT");
  assert.equal(
    questionStatusFromRecommendationRole("negative_or_qualified", true),
    "NEGATIVE_OR_NOT_RECOMMENDED"
  );
  assert.equal(questionStatusFromRecommendationRole("discussed", true), "DISCUSSION_ONLY");
  assert.equal(questionStatusFromRecommendationRole("comparator", true), "DISCUSSION_ONLY");
  assert.equal(questionStatusFromRecommendationRole("source_only", true), "DISCUSSION_ONLY");
  assert.equal(questionStatusFromRecommendationRole(null, false), "MISSING");
});

test("SECTION_NUMBER_NOT_RANK", () => {
  const line = "### 7. Consorcios de Afiliación y Comercialización (Sin Franquicia Completa)";
  assert.equal(isDocumentTopicHeading(line), true);
  const text = `${line}\n* **SLH** boutique\n`;
  assert.equal(detectRankMarker(text, text.indexOf("SLH")), null);
});

test("CONSIDERATION_SET_ASSOCIATED", () => {
  const q = buildTaxonomyReviewQueue({});
  assert.ok(q.boundaryNotes.some((n) => /shortlist\/consideration/i.test(n)));
  assert.ok(q.decisionTree.some((t) => t.role === "associated_option"));
});

test("DIRECT_POSITIVE_EXPLICIT", () => {
  const q = buildTaxonomyReviewQueue({});
  assert.ok(
    q.decisionTree.some(
      (t) => t.role === "explicit_recommendation" && /direct entity-linked positive/i.test(t.when)
    )
  );
});

test("NEUTRAL_DESCRIPTION_DISCUSSION", () => {
  const q = buildTaxonomyReviewQueue({});
  assert.ok(q.decisionTree.some((t) => t.role === "discussed" && /neutral/i.test(t.when)));
});

test("FIRST_REQUIRES_LEAD_OR_TRUE_RANK_1", () => {
  const q = buildTaxonomyReviewQueue({});
  const first = q.decisionTree.find((t) => t.role === "first_recommendation");
  assert.ok(/#1|lead|ranked structure/i.test(first.when));
});

test("NO_CASE_SPECIFIC_RULES", () => {
  const src = fs.readFileSync(
    path.resolve("lib/ai-visibility/validation/recommendation-taxonomy-review.js"),
    "utf8"
  );
  assert.equal(/v2_cand_[0-9a-f]{8}/.test(src), false);
});

test("NO_GEOGRAPHY_SPECIFIC_RULES", () => {
  const src = fs.readFileSync(
    path.resolve("lib/ai-visibility/validation/recommendation-taxonomy-review.js"),
    "utf8"
  );
  assert.equal(/\bif\s*\([^\)]*europe/i.test(src), false);
});

test("READY_SUMMARY_MATCHES_ARTIFACT", () => {
  assert.equal(ready.TOTAL_CASES, 52);
  assert.equal(ready.VALID, 52);
  assert.equal(ready.INVALID, 0);
  assert.equal(ready.READY_FOR_HUMAN_APPLY, "YES");
  assert.equal(ready.PROPOSED_KEEP + ready.PROPOSED_AMEND, 52);
  assert.ok(ready.BY_TRANSITION.length > 0);
});

test("PREVIEW_BLOCKS_WHEN_UNREVIEWED", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "trx-prev-"));
  const decisionsPath = path.join(tmp, "decisions.json");
  const preview = previewTaxonomyReviewApply({ decisionsPath });
  assert.equal(preview.UNREVIEWED, 52);
  assert.equal(preview.CAN_APPLY, false);
  assert.ok(preview.BLOCKERS.some((b) => String(b).startsWith("UNREVIEWED")));
});

console.log(`\n${passed} passed, ${failed} failed`);
console.log("\n" + JSON.stringify(ready, null, 2));
if (failed) process.exit(1);
