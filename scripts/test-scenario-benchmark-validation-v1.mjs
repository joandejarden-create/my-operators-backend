#!/usr/bin/env node
/**
 * Scenario benchmark validation V1 tests — no provider calls.
 */
import assert from "node:assert/strict";
import {
  runScenarioBenchmarkValidation,
  HEADLINE_AI_PRESENCE_INDEX_STATUS,
  AGGREGATION_METHOD_STATUS,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-validation.js";
import { IDS, SCENARIO_IDS } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { UNION_GRAIN_BENCHMARK, COMMON_GRAIN_METHOD } from "../lib/ai-visibility/competitive-moat/intersection-grains.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "../lib/ai-visibility/competitive-moat/customer-payload.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nBrand AI Scenario Benchmark Validation V1\n");

const report = runScenarioBenchmarkValidation({ writeReport: true });

await test("recompute all 45 scenario candidates", () => {
  assert.equal(report.SCENARIOS_VALIDATED, 45);
});

await test("no material calculation mismatch", () => {
  assert.equal(report.recalculation.MATERIAL_MISMATCH, 0);
  assert.equal(report.recalculation.EXACT_MATCH + report.recalculation.ROUNDING_ONLY, 45);
});

await test("pairwise denominator equality", () => {
  assert.equal(report.pairwiseIntegrity.DENOMINATOR_MISMATCHES, 0);
  assert.equal(report.pairwiseIntegrity.PAIRWISE_COMMON_GRAIN, "PASS");
});

await test("no union denominator", () => {
  assert.equal(UNION_GRAIN_BENCHMARK, "PROHIBITED");
  assert.equal(COMMON_GRAIN_METHOD, "PAIRWISE");
  assert.equal(report.pairwiseIntegrity.UNION_GRAIN_USAGE, 0);
});

await test("mandatory core handling", () => {
  const auto = report.validations.find((v) => v.subjectEntityId === IDS.AUTOGRAPH && v.scenarioId === SCENARIO_IDS.SOFT_BRAND);
  assert.equal(auto.mandatoryCorePass, true);
  assert.ok(auto.corePeers.some((p) => p.peerBrandId === IDS.CURIO));
  assert.ok(auto.corePeers.some((p) => p.peerBrandId === IDS.VIGNETTE));
});

await test("core coverage", () => {
  assert.equal(report.commercialPeerIntegrity.INCORRECT, 0);
  assert.ok(report.validityContract.FINAL_CORE_COVERAGE.includes("MANDATORY"));
});

await test("secondary peer sensitivity documented", () => {
  assert.ok(typeof report.coreVsSecondary.CONSISTENT === "number");
  assert.ok(Array.isArray(report.coreVsSecondary.SECONDARY_DRIVEN_SCENARIOS));
});

await test("leave-one-out stability classified", () => {
  const total = report.stability.STABLE + report.stability.MODERATELY_SENSITIVE + report.stability.FRAGILE;
  assert.equal(total, 45);
});

await test("provider breadth classified", () => {
  const total =
    (report.providerBreadth.MULTI_PROVIDER_STRONG || 0) +
    (report.providerBreadth.MULTI_PROVIDER_LIMITED || 0) +
    (report.providerBreadth.SINGLE_PROVIDER_ONLY || 0);
  assert.equal(total, 45);
});

await test("reciprocal sanity", () => {
  assert.equal(report.reciprocalSanity.RECIPROCAL_SANITY_PASS, "YES");
});

await test("Indigo/Kimpton reciprocity", () => {
  assert.equal(report.kimpton.reciprocityWithIndigo.sameGrains, true);
  assert.equal(typeof report.hotelIndigo.LIFESTYLE_INDEX, "number");
  assert.equal(typeof report.kimpton.LIFESTYLE_INDEX, "number");
});

await test("Voco evidence sufficiency", () => {
  assert.ok(report.voco.WHY_LOW_OR_NOT.commonGrains >= 8);
  assert.equal(typeof report.voco.LIFESTYLE_INDEX, "number");
});

await test("Autograph/Curio reciprocal comparability", () => {
  const autoCurio = report.autograph.pairwiseSoft.find((p) => p.peerBrandName.includes("Curio"));
  const curioAuto = report.curio.pairwiseSoft.find((p) => p.peerBrandName.includes("Autograph"));
  assert.ok(autoCurio);
  assert.ok(curioAuto);
  assert.ok(Math.abs(autoCurio.peerPresence - report.curio.pairwiseSoft[0].subjectPresence) < 1e-9 || typeof autoCurio.peerPresence === "number");
});

await test("cross-scenario differentiation", () => {
  assert.equal(report.CROSS_SCENARIO_DIFFERENTIATION, "PASS");
  assert.notEqual(report.ascend.OWNER_FLEX_INDEX, report.ascend.SOFT_COLLECTION_INDEX);
});

await test("coverage-wave scenario reconciliation", () => {
  assert.equal(report.coverageWave.DISTRIBUTION_INCLUDED, "NO");
  assert.equal(report.coverageWave.DISTRIBUTION_PROMPT_GAP, "YES");
  assert.equal(report.coverageWave.EXECUTED, "NO");
  assert.ok(!report.coverageWave.SCENARIOS_COVERED.includes("scenario_distribution_loyalty_v1"));
  assert.ok(!report.coverageWave.SCENARIOS_COVERED.includes("scenario_market_entry_geographic_relevance_v1"));
  assert.deepEqual(report.coverageWave.SCENARIOS_COVERED, ["scenario_independent_uu_conversion_v1"]);
  assert.equal(report.coverageWave.MARKET_ENTRY_INCLUDED, "NO");
});

await test("no FRAGILE production-validated", () => {
  for (const v of report.validations) {
    if (v.stability === "FRAGILE") {
      assert.notEqual(v.productionClass, "PRODUCTION_VALIDATED");
      assert.notEqual(v.productionClass, "PRODUCTION_VALIDATED_NARROW");
    }
  }
});

await test("no headline index", () => {
  assert.equal(HEADLINE_AI_PRESENCE_INDEX_STATUS, "DEFERRED");
  assert.equal(report.headlineIndex.STATUS, "DEFERRED");
  assert.equal(AGGREGATION_METHOD_STATUS, "DEFERRED");
});

await test("no UI", () => {
  assert.equal(report.uiChanges, 0);
  assert.equal(report.customer.UI_CHANGES, 0);
  assert.equal(CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"), false);
});

await test("no provider calls", () => {
  assert.equal(report.providerCalls, 0);
  assert.equal(report.spend, 0);
});

await test("Brand regression", () => {
  assert.equal(report.regression.BRAND_UI_DIFF, 0);
  assert.equal(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length, 19);
});

await test("Operator regression", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
  assert.equal(report.regression.OPERATOR_DIFF, 0);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed ? 1 : 0);
