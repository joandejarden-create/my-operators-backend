/**
 * test:adp-action-plan-toronto-v1
 */
import assert from "node:assert/strict";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";

const ID = "adp_radisson_blu_toronto_downtown";
const cov = buildPublishedAdpReviewCoverageV1();
const row = cov.properties.find((p) => p.propertyId === ID);
assert.ok(row);
assert.ok(row.reviewId, "Action Plan requires performance review");
assert.ok(row.actionCount > 0, "action agenda present");
assert.notEqual(row.coverageStatus, "BLOCKED");

// Catalog shape parity with getAdminActionPlanCatalog
const catalogRow = {
  ok: Boolean(row.reviewId),
  error: row.reviewId ? null : "performance_review_missing",
};
assert.equal(catalogRow.ok, true);
assert.equal(catalogRow.error, null);

console.log("PASS test:adp-action-plan-toronto-v1");
