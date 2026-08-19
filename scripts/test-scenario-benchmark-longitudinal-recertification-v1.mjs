#!/usr/bin/env node
/**
 * Longitudinal benchmark recertification readiness tests — no provider calls.
 */
import assert from "node:assert/strict";
import {
  auditPeriodArchitecture,
  buildCanonicalComparableGrainIdentity,
  buildComparisonGrainKey,
  detectCrossPeriodDedupViolations,
  partitionResponsesByPeriod,
  BASELINE_MEASUREMENT_PERIOD,
  CROSS_PERIOD_DEDUPLICATION,
  POOLED_ALL_PERIODS_INDEX,
} from "../lib/ai-visibility/competitive-moat/period-scoped-grain.js";
import {
  runLongitudinalRecertificationReadiness,
  runLongitudinalRecertification,
  classifyProviderConflictTransition,
  comparePeriodResults,
  evaluateCandidateRecertification,
  NEAR_TERM_RECERTIFICATION_CANDIDATES,
  EXCLUDED_SCENARIO_STATUS,
  AUTOMATIC_CUSTOMER_PROMOTION,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";

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

console.log("\nBrand AI Longitudinal Benchmark Recertification Readiness V1\n");

await test("measurement period is separate dimension", () => {
  const audit = auditPeriodArchitecture();
  assert.equal(audit.MEASUREMENT_PERIOD_PRESENT, true);
  assert.equal(audit.PERIOD_PART_OF_COMPARISON_SCOPE, true);
  assert.equal(audit.CROSS_PERIOD_DEDUPLICATION, "PROHIBITED");
  assert.equal(audit.HISTORICAL_IDS_CHANGED, false);
});

await test("same prompt/provider/geo in two periods stays distinct", () => {
  const base = {
    scenarioId: S.SOFT_BRAND,
    promptId: "p_cala_collection_affiliation_v1",
    provider: "openai",
    language: "en",
    geography: "CALA",
    promptVersion: "1",
  };
  const canonical = buildCanonicalComparableGrainIdentity(base);
  const keyA = buildComparisonGrainKey(base, "period_202608");
  const keyB = buildComparisonGrainKey(base, "period_202609");
  assert.equal(canonical, buildCanonicalComparableGrainIdentity(base));
  assert.notEqual(keyA, keyB);
  assert.ok(keyA.includes("period_202608"));
  assert.ok(keyB.includes("period_202609"));
});

await test("cross-period observations remain distinct in partition", () => {
  const responses = [
    { promptId: "p1", provider: "openai", measurementPeriodId: "period_a" },
    { promptId: "p1", provider: "openai", measurementPeriodId: "period_b" },
  ];
  const parts = partitionResponsesByPeriod(responses);
  assert.equal(parts.size, 2);
  assert.equal(parts.get("period_a").length, 1);
  assert.equal(parts.get("period_b").length, 1);
  const dedup = detectCrossPeriodDedupViolations([
    { ...responses[0], scenarioId: S.SOFT_BRAND, language: "en", geography: "CALA", promptVersion: "1" },
    { ...responses[1], scenarioId: S.SOFT_BRAND, language: "en", geography: "CALA", promptVersion: "1" },
  ]);
  assert.equal(dedup.ok, true);
  assert.equal(dedup.distinctPeriods.length, 2);
});

await test("no period pooling during certification constant", () => {
  assert.equal(POOLED_ALL_PERIODS_INDEX, "PROHIBITED");
  assert.equal(CROSS_PERIOD_DEDUPLICATION, "PROHIBITED");
});

await test("provider conflict evaluated per period", () => {
  const prior = { providerDirection: "CONFLICT" };
  const current = { providerDirection: "CONSISTENT" };
  assert.equal(classifyProviderConflictTransition(prior, current), "CONFLICT_RESOLVED");
  assert.equal(
    classifyProviderConflictTransition({ providerDirection: "CONFLICT" }, { providerDirection: "CONFLICT" }),
    "CONFLICT_PERSISTENT"
  );
});

await test("compare period results contract", () => {
  const cmp = comparePeriodResults(
    {
      measurementPeriodId: "DEMO_VALIDATION",
      indexValue: 100,
      subjectPresence: 0.9,
      benchmarkPresence: 0.9,
      providerDirection: "CONFLICT",
      corePeerSet: ["Curio Collection by Hilton"],
    },
    {
      measurementPeriodId: "period_2",
      indexValue: 103,
      subjectPresence: 0.95,
      benchmarkPresence: 0.92,
      providerDirection: "CONSISTENT",
      corePeerSet: ["Curio Collection by Hilton"],
    }
  );
  assert.equal(cmp.comparable, true);
  assert.equal(cmp.CONFLICT_STATE, "CONFLICT_RESOLVED");
  assert.equal(cmp.POOLED_ALL_PERIODS_INDEX, "PROHIBITED");
});

await test("Curio future recertification contract", () => {
  const curio = NEAR_TERM_RECERTIFICATION_CANDIDATES.find((c) => c.label === "CURIO_SOFT_BRAND");
  const evalResult = evaluateCandidateRecertification(
    curio,
    { measurementPeriodId: "p1", indexValue: 100, providerDirection: "CONFLICT", corePeerSet: [], corePeerCount: 5, commonGrainCount: 31, stability: "STABLE" },
    { measurementPeriodId: "p2", indexValue: 103, providerDirection: "CONSISTENT", corePeerSet: [], corePeerCount: 5, commonGrainCount: 31, stability: "STABLE" }
  );
  assert.equal(evalResult.automaticCandidateState, "CERTIFY");
  assert.equal(evalResult.READY_FOR_CERTIFICATION, true);
  assert.equal(evalResult.AUTOMATIC_CUSTOMER_PROMOTION, false);
});

await test("Tribute future recertification keeps limited on persistent conflict", () => {
  const tribute = NEAR_TERM_RECERTIFICATION_CANDIDATES.find((c) => c.label === "TRIBUTE_SOFT_BRAND");
  const evalResult = evaluateCandidateRecertification(
    tribute,
    { measurementPeriodId: "p1", providerDirection: "CONFLICT", corePeerCount: 5, commonGrainCount: 31, stability: "STABLE", indexValue: 100, corePeerSet: [] },
    { measurementPeriodId: "p2", providerDirection: "CONFLICT", corePeerCount: 5, commonGrainCount: 31, stability: "STABLE", indexValue: 100, corePeerSet: [] }
  );
  assert.equal(evalResult.automaticCandidateState, "KEEP_LIMITED");
  assert.equal(evalResult.READY_FOR_CERTIFICATION, false);
});

await test("Vignette soft brand contract", () => {
  const row = NEAR_TERM_RECERTIFICATION_CANDIDATES.find((c) => c.label === "VIGNETTE_SOFT_BRAND");
  assert.ok(row);
  assert.equal(row.scenarioId, S.SOFT_BRAND);
});

await test("Ascend owner flex contract", () => {
  const row = NEAR_TERM_RECERTIFICATION_CANDIDATES.find((c) => c.label === "ASCEND_OWNER_FLEX");
  assert.ok(row);
  assert.equal(row.scenarioId, S.OWNER_FLEXIBILITY);
});

await test("current 103/103/67 immutable", () => {
  const report = runLongitudinalRecertificationReadiness({ writeReport: false });
  assert.equal(report.frozenBaseline.AUTOGRAPH_103_DIFF, 0);
  assert.equal(report.frozenBaseline.TAPESTRY_103_DIFF, 0);
  assert.equal(report.frozenBaseline.ASCEND_67_DIFF, 0);
  assert.equal(report.frozenBaseline.ok, true);
});

await test("no UI promotion", () => {
  assert.equal(AUTOMATIC_CUSTOMER_PROMOTION, false);
  const report = runLongitudinalRecertificationReadiness({ writeReport: false });
  assert.equal(report.AUTOMATIC_CUSTOMER_PROMOTION, false);
  assert.equal(report.LIVE_CERTIFIED_VALUES_ONLY, "UNCHANGED");
  assert.equal(report.uiChanges, 0);
});

await test("no provider calls", () => {
  const report = runLongitudinalRecertificationReadiness({ writeReport: false });
  assert.equal(report.providerCalls, 0);
  assert.equal(report.spend, 0);
  assert.equal(report.postWaveRecertification.PROVIDER_CALLS_REQUIRED, false);
});

await test("post-wave recert requires current period", () => {
  const result = runLongitudinalRecertification({ writeReport: false });
  assert.equal(result.ok, false);
  assert.equal(result.reason, "CURRENT_PERIOD_REQUIRED");
});

await test("lifestyle excluded", () => {
  assert.equal(EXCLUDED_SCENARIO_STATUS[S.LIFESTYLE], "SCENARIO_REDESIGN_REQUIRED");
  const report = runLongitudinalRecertificationReadiness({ writeReport: false });
  assert.equal(report.lifestyle.STATUS, "SCENARIO_REDESIGN_REQUIRED");
});

await test("distribution excluded", () => {
  assert.equal(EXCLUDED_SCENARIO_STATUS[S.DISTRIBUTION_LOYALTY], "PROMPT_DESIGN_REQUIRED");
});

await test("conversion secondary repeat evidence only", () => {
  const autograph = NEAR_TERM_RECERTIFICATION_CANDIDATES.find((c) => c.label === "AUTOGRAPH_CONVERSION");
  const evalResult = evaluateCandidateRecertification(
    autograph,
    { measurementPeriodId: "p1", indexValue: 164, providerDirection: "CONSISTENT", corePeerCount: 5, commonGrainCount: 24, stability: "FRAGILE", corePeerSet: [] },
    { measurementPeriodId: "p2", indexValue: 160, providerDirection: "CONSISTENT", corePeerCount: 5, commonGrainCount: 24, stability: "FRAGILE", corePeerSet: [] }
  );
  assert.equal(evalResult.automaticCandidateState, "REPEAT_EVIDENCE_ONLY");
  assert.equal(evalResult.READY_FOR_CERTIFICATION, false);
});

await test("operator regression", () => {
  const report = runLongitudinalRecertificationReadiness({ writeReport: false });
  assert.equal(report.OPERATOR_DIFF, 0);
  assert.equal(report.PRIMARY_MONITORED_OPERATORS, PRIMARY_OPERATOR_COUNT);
});

console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);

console.log("BRAND_AI_LONGITUDINAL_BENCHMARK_RECERTIFICATION_READINESS_PASS");
