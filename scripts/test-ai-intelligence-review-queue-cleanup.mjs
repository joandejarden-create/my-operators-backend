#!/usr/bin/env node
/**
 * Golden Set Review Queue Cleanup tests.
 * Default queue = ACTIVE_REVIEW only. No data deletion. No holdout. No Airtable.
 */
import assert from "node:assert/strict";
import {
  buildReviewQueue,
  getReviewProgress,
  REVIEW_STATUS,
} from "../lib/ai-visibility/validation/golden-set-human-review.js";
import {
  getGoldenSetReviewState,
  REVIEW_STATE_BUCKET,
  summarizeReviewStateBuckets,
} from "../lib/ai-visibility/validation/golden-set-review-state.js";

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

console.log("AI Intelligence Review Queue Cleanup\n");

const buckets = summarizeReviewStateBuckets();
const progress = getReviewProgress();

test("DEFAULT_REVIEW_QUEUE_ACTIVE_ONLY", () => {
  const q = buildReviewQueue({ filters: {} });
  assert.equal(q.reviewStateFilter, REVIEW_STATE_BUCKET.ACTIVE_REVIEW);
  assert.ok(q.cases.every((c) => c.reviewStateBucket === REVIEW_STATE_BUCKET.ACTIVE_REVIEW));
  assert.equal(q.cases.length, progress.OUTSTANDING_REVIEW);
});

test("COMPLETED_NOT_IN_ACTIVE_QUEUE", () => {
  const q = buildReviewQueue({ filters: { state: "active" } });
  assert.ok(
    q.cases.every(
      (c) =>
        c.reviewStatus !== REVIEW_STATUS.CONFIRMED &&
        c.reviewStatus !== REVIEW_STATUS.CORRECTED
    )
  );
});

test("CORRECTED_NOT_IN_ACTIVE_QUEUE", () => {
  const q = buildReviewQueue({ filters: { state: "active" } });
  assert.equal(q.cases.filter((c) => c.reviewStatus === REVIEW_STATUS.CORRECTED).length, 0);
});

test("CONFIRMED_NOT_IN_ACTIVE_QUEUE", () => {
  const q = buildReviewQueue({ filters: { state: "active" } });
  assert.equal(q.cases.filter((c) => c.reviewStatus === REVIEW_STATUS.CONFIRMED).length, 0);
});

test("INVALIDATED_NOT_IN_ACTIVE_QUEUE", () => {
  const q = buildReviewQueue({ filters: { state: "active" } });
  assert.equal(q.cases.filter((c) => c.reviewStateBucket === REVIEW_STATE_BUCKET.INVALIDATED).length, 0);
});

test("INVALIDATED_NOT_IN_COMPLETED_CURRENT_STATE", () => {
  const completed = buildReviewQueue({ filters: { state: "completed" } });
  assert.ok(
    completed.cases.every((c) => c.reviewStateBucket === REVIEW_STATE_BUCKET.COMPLETED)
  );
  assert.equal(
    completed.cases.filter((c) => c.reviewStateBucket === REVIEW_STATE_BUCKET.INVALIDATED).length,
    0
  );
  assert.equal(buckets.COMPLETED + buckets.INVALIDATED, 330);
  assert.equal(buckets.COMPLETED, 318);
  assert.equal(buckets.INVALIDATED, 12);
});

test("DEFERRED_REMAINS_ACTIVE", () => {
  assert.equal(
    getGoldenSetReviewState({ caseId: "x", reviewStatus: REVIEW_STATUS.DEFERRED }),
    REVIEW_STATE_BUCKET.ACTIVE_REVIEW
  );
});

test("REOPENED_REMAINS_ACTIVE", () => {
  assert.equal(
    getGoldenSetReviewState({ caseId: "x", reviewStatus: "REOPENED" }),
    REVIEW_STATE_BUCKET.ACTIVE_REVIEW
  );
  assert.equal(
    getGoldenSetReviewState({ caseId: "x", reviewStatus: REVIEW_STATUS.SECOND_REVIEW_REQUIRED }),
    REVIEW_STATE_BUCKET.ACTIVE_REVIEW
  );
});

test("COMPLETED_FILTER_RETURNS_COMPLETED", () => {
  const q = buildReviewQueue({ filters: { state: "completed" } });
  assert.equal(q.reviewStateFilter, REVIEW_STATE_BUCKET.COMPLETED);
  assert.equal(q.cases.length, buckets.COMPLETED);
  assert.ok(q.cases.every((c) => c.reviewStateBucket === REVIEW_STATE_BUCKET.COMPLETED));
});

test("INVALIDATED_FILTER_RETURNS_INVALIDATED", () => {
  const q = buildReviewQueue({ filters: { state: "invalidated" } });
  assert.equal(q.reviewStateFilter, REVIEW_STATE_BUCKET.INVALIDATED);
  assert.equal(q.cases.length, buckets.INVALIDATED);
  assert.ok(q.cases.every((c) => c.reviewStateBucket === REVIEW_STATE_BUCKET.INVALIDATED));
});

test("ALL_FILTER_RETURNS_HISTORY", () => {
  const q = buildReviewQueue({ filters: { state: "all" } });
  assert.equal(q.reviewStateFilter, "ALL");
  assert.equal(q.cases.length, buckets.TOTAL_HISTORICAL);
  assert.ok(q.cases.length >= buckets.COMPLETED + buckets.INVALIDATED);
});

test("ZERO_ACTIVE_SHOWS_COMPLETE_STATE", () => {
  assert.equal(progress.OUTSTANDING_REVIEW, 0);
  assert.equal(progress.ZERO_ACTIVE_COMPLETE_STATE, true);
  assert.equal(buckets.ACTIVE_REVIEW, 0);
  const q = buildReviewQueue({ filters: { state: "active" } });
  assert.equal(q.cases.length, 0);
});

test("NO_DOUBLE_COUNT_PRIMARY_BUCKETS", () => {
  assert.equal(
    buckets.ACTIVE_REVIEW + buckets.COMPLETED + buckets.INVALIDATED,
    330
  );
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
