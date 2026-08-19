#!/usr/bin/env node
/**
 * Brand AI Intelligence — Longitudinal Foundation V1 tests.
 */

import assert from "node:assert/strict";
import {
  normalizeMeasurementDate,
  buildLongitudinalGrainKey,
  areTrendComparable,
  classifyPromptVersionCompatibility,
  PROMPT_VERSION_STATES,
  computeCommonCohort,
  buildMonthlyExecutionMatrix,
  buildCohortExecutionMatrix,
  buildLongitudinalCostModel,
  acquireMeasurementLock,
  isDuplicateMeasurementCycle,
  buildCurrentVsPriorComparison,
  buildMeasurementPeriodManifest,
  qualifyMeasurementPeriod,
  resolveTrendClientState,
  TREND_CLIENT_STATES,
  assertNoForbiddenMetrics,
  STAGE_B_NON_AUTHORITATIVE_WAVE_IDS,
  labelLongitudinalMovement,
  MOVEMENT_LABELS,
  PRIMARY_BASELINE_DATE,
} from "../lib/ai-visibility/brand-longitudinal/index.js";

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

console.log("\nBrand Longitudinal Foundation V1\n");

test("same-day repeats are not separate trend dates", () => {
  const a = { measurementDate: "2026-08-17", provider: "openai", promptId: "p1", promptVersion: "1", geographyKey: "CALA", language: "en", providerModel: "gpt-5.6" };
  const b = { ...a, measurementDate: "2026-08-17" };
  const cmp = areTrendComparable(a, b);
  assert.equal(cmp.comparable, false);
  assert.equal(cmp.reasonCode, "SAME_MEASUREMENT_DATE");
});

test("two distinct dates = CURRENT_VS_PRIOR not TREND", () => {
  assert.equal(resolveTrendClientState(2), TREND_CLIENT_STATES.CURRENT_VS_PRIOR);
  assert.equal(resolveTrendClientState(2) !== TREND_CLIENT_STATES.TREND, true);
});

test("three distinct dates = EARLY_TREND eligible", () => {
  assert.equal(resolveTrendClientState(3), TREND_CLIENT_STATES.EARLY_TREND);
});

test("common-cohort denominator uses intersection only", () => {
  const a = [{ promptId: "p1", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA" }];
  const b = [
    { promptId: "p1", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA" },
    { promptId: "p2", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA" },
  ];
  const common = computeCommonCohort(a, b);
  assert.equal(common.commonCount, 1);
  assert.equal(common.periodACount, 1);
  assert.equal(common.periodBCount, 2);
  assert.equal(common.cohortChanged, true);
});

test("prompt version incompatibility starts new series", () => {
  const r = classifyPromptVersionCompatibility(
    { promptId: "p1", promptVersion: "2", promptTextHash: "aaa" },
    { promptId: "p1", promptVersion: "1", promptTextHash: "bbb" }
  );
  assert.equal(r.state, PROMPT_VERSION_STATES.NEW_SERIES_REQUIRED);
});

test("model boundary blocks comparability", () => {
  const cmp = areTrendComparable(
    {
      measurementDate: "2026-09-01",
      provider: "openai",
      providerModel: "gpt-6",
      promptId: "p1",
      promptVersion: "1",
      geographyKey: "CALA",
      language: "en",
    },
    {
      measurementDate: "2026-08-14",
      provider: "openai",
      providerModel: "gpt-5.6",
      promptId: "p1",
      promptVersion: "1",
      geographyKey: "CALA",
      language: "en",
    }
  );
  assert.equal(cmp.comparable, false);
  assert.equal(cmp.comparabilityState, "MODEL_CHANGE_BOUNDARY");
});

test("duplicate measurement lock returns NO_SECOND_EXECUTION", () => {
  const storeRoot = `data/ai-visibility/runtime/brand-longitudinal-test-${Date.now()}`;
  const key = "test|v1|2026-08-18|demo";
  const first = acquireMeasurementLock(key, "period_a", storeRoot);
  assert.equal(first.acquired, true);
  const dup = acquireMeasurementLock(key, "period_b", storeRoot);
  assert.equal(dup.acquired, false);
  assert.equal(dup.reason, "NO_SECOND_EXECUTION");
});

test("partial period excluded from valid qualification", () => {
  const partial = buildMeasurementPeriodManifest({
    plannedCalls: 100,
    successfulCalls: 90,
    qualityState: "PARTIAL_PERIOD",
  });
  const q = qualifyMeasurementPeriod(partial);
  assert.equal(q.valid, false);
});

test("valid period requires >=95% success", () => {
  const valid = buildMeasurementPeriodManifest({ plannedCalls: 100, successfulCalls: 96 });
  assert.equal(qualifyMeasurementPeriod(valid).valid, true);
  const invalid = buildMeasurementPeriodManifest({ plannedCalls: 100, successfulCalls: 94 });
  assert.equal(qualifyMeasurementPeriod(invalid).valid, false);
});

test("no synthetic history — normalize date is calendar only", () => {
  assert.equal(normalizeMeasurementDate("2026-08-14T23:59:59.000Z"), "2026-08-14");
  assert.equal(buildLongitudinalGrainKey({ measurementDate: "2026-08-14" }).includes("2026-08-14"), true);
});

test("archived Stage B wave excluded from trend comparability", () => {
  const excluded = STAGE_B_NON_AUTHORITATIVE_WAVE_IDS[0];
  const cmp = areTrendComparable(
    {
      measurementDate: "2026-08-17",
      waveId: excluded,
      provider: "openai",
      providerModel: "gpt-5.6",
      promptId: "p1",
      promptVersion: "1",
      geographyKey: "CALA",
      language: "en",
    },
    {
      measurementDate: "2026-08-14",
      provider: "openai",
      providerModel: "gpt-5.6",
      promptId: "p1",
      promptVersion: "1",
      geographyKey: "CALA",
      language: "en",
    }
  );
  assert.equal(cmp.comparable, false);
  assert.equal(cmp.reasonCode, "ARCHIVED_STAGE_B_WAVE");
});

test("movement labels avoid improved/worsened", () => {
  assert.equal(labelLongitudinalMovement(0.6, 0.4), MOVEMENT_LABELS.INCREASED);
  assert.notEqual(labelLongitudinalMovement(0.6, 0.4), "IMPROVED");
});

test("no Recommendation metrics in longitudinal set", () => {
  const r = assertNoForbiddenMetrics(["AI_PRESENCE", "RECOMMENDATION_RATE"]);
  assert.equal(r.ok, false);
  assert.ok(r.violations.includes("RECOMMENDATION_RATE"));
});

test("cohort V1 is 30-50 prompts with cost under $75 target", () => {
  const matrix = buildCohortExecutionMatrix();
  assert.ok(matrix.promptCount >= 30 && matrix.promptCount <= 50, `promptCount=${matrix.promptCount}`);
  const cost = buildLongitudinalCostModel();
  assert.ok(cost.historicExpectedCostUsd <= 75, `cost=${cost.historicExpectedCostUsd}`);
  assert.ok(cost.costGatePass);
});

test("primary baseline date constant", () => {
  assert.equal(PRIMARY_BASELINE_DATE, "2026-08-14");
});

test("current vs prior on two valid periods", () => {
  const prior = buildMeasurementPeriodManifest({
    completedAt: "2026-08-14T12:00:00.000Z",
    plannedCalls: 100,
    successfulCalls: 100,
    grains: [{ promptId: "p1", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA", promptVersion: "1", providerModel: "gpt-5.6" }],
    observations: [{ promptId: "p1", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA", promptVersion: "1", providerModel: "gpt-5.6", value: 0.4 }],
  });
  const current = buildMeasurementPeriodManifest({
    completedAt: "2026-09-14T12:00:00.000Z",
    plannedCalls: 100,
    successfulCalls: 100,
    grains: [{ promptId: "p1", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA", promptVersion: "1", providerModel: "gpt-5.6" }],
    observations: [{ promptId: "p1", brandId: "b1", provider: "openai", language: "en", geographyKey: "CALA", promptVersion: "1", providerModel: "gpt-5.6", value: 0.5 }],
  });
  const cmp = buildCurrentVsPriorComparison({ currentPeriod: current, priorPeriod: prior });
  assert.equal(cmp.ok, true);
  assert.equal(cmp.movement, MOVEMENT_LABELS.INCREASED);
});

console.log(`\nLongitudinal foundation tests: ${pass} passed, ${fail} failed\n`);
process.exit(fail > 0 ? 1 : 0);
