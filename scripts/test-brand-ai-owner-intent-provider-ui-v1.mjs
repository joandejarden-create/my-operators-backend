#!/usr/bin/env node
/**
 * Brand AI Owner Intent provider-consistent UI contract V1.
 * PROVIDER_CALLS = 0
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  buildOwnerIntentBenchmarksForBrand,
  getCustomerScenarioDisplayLabel,
  CUSTOMER_SCENARIO_DISPLAY_LABELS,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-customer-service.js";
import { verifyFrozenBaseline } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

console.log("\nBrand AI Owner Intent Provider UI V1\n");

await test("proper-case display labels", () => {
  assert.equal(CUSTOMER_SCENARIO_DISPLAY_LABELS[S.SOFT_BRAND], "Soft Brand Affiliation");
  const soft = getCustomerScenarioDisplayLabel(S.SOFT_BRAND);
  assert.equal(soft, "Soft Brand Affiliation");
  assert.match(getCustomerScenarioDisplayLabel(S.CONVERSION_SUITABILITY), /Conversion Suitability/);
  assert.doesNotMatch(soft, /_/);
  assert.doesNotMatch(soft, /AFFILIATION/);
});

await test("all providers certified index preserved", () => {
  const all = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, { allProvidersMode: true });
  assert.ok(all.ownerIntentBenchmarks.length >= 1);
  assert.equal(all.OWNER_INTENT_VISIBLE, true);
  const soft = all.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.ok(soft);
  assert.equal(soft.indexValue, 103);
  assert.ok(Math.abs(soft.relativeGapPct - 3) <= 0.5);
});

await test("single provider exact-scope — no stale all-providers index", () => {
  const openai = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
    observations: [],
  });
  const soft = openai.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.notEqual(soft?.indexValue, 103);
  if (soft?.indexValue != null) {
    assert.equal(soft.benchmarkStatus, "CERTIFIED");
    assert.equal(soft.indexValue, 100);
  } else {
    assert.equal(soft?.benchmarkStatus, "Benchmark still developing");
  }
});

await test("exact-scope certified provider may render numeric index", () => {
  const perplexity = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "perplexity",
    observations: [],
  });
  const soft = perplexity.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  if (soft?.indexValue != null) {
    assert.equal(soft.benchmarkStatus, "CERTIFIED");
  }
});

await test("uncertified rows use developing copy not numeric index", () => {
  const all = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, { allProvidersMode: true });
  const conv = all.ownerIntentBenchmarks.find(
    (r) => r.intentLabel === "Conversion Suitability"
  );
  if (conv) {
    assert.equal(conv.indexValue, null);
    assert.equal(conv.benchmarkStatus, "Benchmark still developing");
  }
});

await test("frozen certified baseline unchanged", () => {
  const frozen = verifyFrozenBaseline();
  assert.equal(frozen.ok, true);
  assert.equal(frozen.AUTOGRAPH_103_DIFF, 0);
  assert.equal(frozen.TAPESTRY_103_DIFF, 0);
  assert.equal(frozen.ASCEND_67_DIFF, 0);
});

await test("UI source contract", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
    "utf8"
  );
  assert.match(js, /renderOwnerIntentBenchmarks/);
  assert.match(js, /Benchmark still developing/);
  assert.doesNotMatch(js, /Select <strong>All Providers<\/strong> to view AI Presence by Owner Intent/);
  assert.match(js, /Your Presence/);
  assert.match(js, /AI Presence Index/);
});

await test("no provider calls", () => {
  assert.equal(process.env.LIVE_PROVIDER_CALLS || "0", "0");
});

const token =
  failed === 0
    ? "BRAND_AI_OWNER_INTENT_PROVIDER_UI_PASS"
    : "BRAND_AI_OWNER_INTENT_PROVIDER_UI_REMEDIATION_REQUIRED";

console.log(`\n${passed} passed, ${failed} failed\n`);
console.log(token + "\n");
process.exit(failed ? 1 : 0);
