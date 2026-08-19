#!/usr/bin/env node
/**
 * Radisson incorporation + alias collision + peer v3 freeze tests.
 * No provider calls.
 */
import assert from "node:assert/strict";
import {
  auditRadissonAliasCollision,
  auditRadissonMeasurementEligibility,
} from "../lib/ai-visibility/brand-longitudinal/radisson-gate.js";
import {
  loadSelectedBrandUniverse,
  RADISSON_BRAND_ID,
  RADISSON_FAMILY_IDS,
  SELECTED_BRANDS_EXPECTED,
} from "../lib/ai-visibility/brand-longitudinal/selected-universe.js";
import {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  PEER_SET_ID_V2,
  PEER_SET_ID_V3,
} from "../lib/ai-visibility/peer-sets.js";
import { getShowcaseCompany } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { buildMonthlyExecutionMatrix } from "../lib/ai-visibility/brand-longitudinal/cohort-v1.js";
import { buildLongitudinalCostModel } from "../lib/ai-visibility/brand-longitudinal/cost-model.js";
import { MULTI_PARENT_HARD_CAP_USD } from "../lib/ai-visibility/brand-longitudinal/multi-parent-wave-orchestrator.js";

let pass = 0;
let fail = 0;
function test(name, fn) {
  try {
    fn();
    console.log("  PASS", name);
    pass += 1;
  } catch (err) {
    console.error("  FAIL", name, err.message);
    fail += 1;
  }
}

console.log("\nBrand longitudinal Radisson gate + 19-brand universe\n");

test("19 selected brands across 4 parents including Radisson", () => {
  const u = loadSelectedBrandUniverse();
  assert.equal(u.TOTAL_PARENT_COMPANIES, 4);
  assert.equal(u.TOTAL_SELECTED_BRANDS, SELECTED_BRANDS_EXPECTED);
  const choice = u.parents.find((p) => p.companyKey === "choice");
  assert.ok(choice.brandIds.includes(RADISSON_BRAND_ID));
  assert.ok(choice.BRANDS.includes("Radisson"));
  assert.equal(choice.BRAND_COUNT, 5);
});

test("Radisson distinct from Blu / RED / Individuals in showcase", () => {
  const choice = getShowcaseCompany("choice");
  const ids = new Set(choice.brandIds);
  assert.ok(ids.has(RADISSON_FAMILY_IDS.radisson));
  assert.ok(ids.has(RADISSON_FAMILY_IDS.blu));
  assert.ok(ids.has(RADISSON_FAMILY_IDS.red));
  assert.ok(ids.has(RADISSON_FAMILY_IDS.individuals));
  assert.equal(ids.size, 5);
});

test("alias collision longest-match PASS", () => {
  const r = auditRadissonAliasCollision();
  assert.equal(r.RADISSON_ALIAS_COLLISION, "PASS", JSON.stringify(r.failures));
});

test("frozen peer v2 unchanged; v3 adds Radisson only", () => {
  const cfg = loadPeerSetConfig();
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, cfg);
  const v3 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V3 }, cfg);
  assert.equal(v2.entityIds.length, 15);
  assert.ok(!v2.entityIds.includes(RADISSON_BRAND_ID));
  assert.ok(!v2.entityIds.includes(RADISSON_FAMILY_IDS.red));
  assert.equal(v3.entityIds.length, 16);
  assert.ok(v3.entityIds.includes(RADISSON_BRAND_ID));
  for (const id of v2.entityIds) assert.ok(v3.entityIds.includes(id));
});

test("Radisson measurement eligible", () => {
  const g = auditRadissonMeasurementEligibility();
  assert.equal(g.RADISSON_MEASUREMENT_ELIGIBLE, "YES", JSON.stringify(g));
});

test("monthly matrix still 86 calls under $60 conservative", () => {
  const monthly = buildMonthlyExecutionMatrix();
  const cost = buildLongitudinalCostModel();
  assert.equal(monthly.callCount, 86);
  assert.ok(cost.conservativeExpectedCostUsd <= MULTI_PARENT_HARD_CAP_USD);
});

console.log(`\n${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
