/**
 * test:adp-admin-internal-vs-customer-publication-v1
 */
import assert from "node:assert/strict";
import { resolveAdpMonthlyReviewEligibilityV1 } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import { resolveAvaAdminHotelRow } from "../lib/ai-visibility-audit-action-plan/ava-admin-hotel-row-v1.js";

const ID = "adp_radisson_blu_toronto_downtown";
const elig = resolveAdpMonthlyReviewEligibilityV1(ID);
const ava = resolveAvaAdminHotelRow(ID);

assert.equal(elig.eligible, true, "internal admin eligible");
assert.equal(elig.customerPublicationAuthorized, false);
assert.equal(ava.founderReviewed, true);
assert.equal(ava.founderApprovedForCustomer, false);
assert.equal(ava.customerPublicationAuthorized, false);
assert.notEqual(ava.auditStatus, "PUBLISHED");

console.log("PASS test:adp-admin-internal-vs-customer-publication-v1");
