/**
 * test:adp-admin-full-surface-toronto-v1
 */
import assert from "node:assert/strict";
import { resolveAdpMonthlyReviewEligibilityV1 } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { resolveAvaAdminHotelRow } from "../lib/ai-visibility-audit-action-plan/ava-admin-hotel-row-v1.js";

const ID = "adp_radisson_blu_toronto_downtown";
const CORE = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";

const elig = resolveAdpMonthlyReviewEligibilityV1(ID);
assert.equal(elig.eligible, true);
assert.ok(!(elig.blockers || []).includes("published_manifest_missing"));
assert.equal(elig.currentPeriodId, CORE);

const cov = buildPublishedAdpReviewCoverageV1().properties.find((p) => p.propertyId === ID);
assert.ok(cov, "coverage row");
assert.notEqual(cov.coverageStatus, "BLOCKED");
assert.ok(cov.reviewId, "performance review present");
assert.equal(cov.hasPdf, true);
assert.equal(cov.coverageStatus, "READY");

const ava = resolveAvaAdminHotelRow(ID);
assert.equal(ava.auditStatus, "FOUNDER_REVIEWED_STAGED");
assert.equal(ava.recommendationCount, 6);
assert.notEqual(ava.pdfStatus, "MISSING");
assert.equal(ava.customerPublicationAuthorized, false);

console.log("PASS test:adp-admin-full-surface-toronto-v1");
