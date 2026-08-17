/**
 * Unit tests — Core Identity census priority + gates (no live writes).
 */
import test from "node:test";
import assert from "node:assert/strict";
import { rankHbxPriorityGeographies } from "../lib/research-engine-v2/full-cala-core-identity-hbx-priority-v1.js";
import { listDealalityCalaGeographies } from "../lib/research-engine-v2/dealality-cala-geography-registry-v1.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { CENSUS_TABLE_ID } from "../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js";

test("target table is production Hotel Property Census", () => {
  assert.equal(CENSUS_TABLE_ID, PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID);
  assert.equal(CENSUS_TABLE_ID, "tbl9aY5ijiuIzzWam");
});

test("HBX priority plan stays within request budget and prefers Brazil", () => {
  const plan = rankHbxPriorityGeographies({ requestBudget: 40, pageSize: 1000 });
  assert.ok(plan.plan.length > 0);
  assert.ok(plan.requests_allocated <= 40);
  const names = plan.plan.map((p) => p.name);
  assert.ok(
    names.includes("Brazil") || plan.ranked_all[0].name === "Brazil",
    "Brazil should rank near top given HOLD mass"
  );
  assert.ok(plan.page_size === 1000);
});

test("canonical registry still has 52 in-scope including Bermuda", () => {
  const geos = listDealalityCalaGeographies({ includeScopeReview: false });
  assert.ok(geos.length >= 52);
  assert.ok(geos.some((g) => g.geography_id === "bermuda"));
});
