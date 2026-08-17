import test from "node:test";
import assert from "node:assert/strict";
import {
  classifyCoverageGap,
  coverageRatio,
  approximateAggregateMatchCount,
  scoreDiscoveryPriority,
  GAP_CLASS,
  BENCHMARK_ROLE,
} from "../lib/research-engine-v2/cala-census-benchmark-coverage-reconciliation-v1.js";

test("benchmark role is BENCHMARK_ONLY", () => {
  assert.equal(BENCHMARK_ROLE, "BENCHMARK_ONLY");
});

test("classifyCoverageGap classes", () => {
  assert.equal(
    classifyCoverageGap({ dealalityCount: 0, benchmarkCount: 50 }),
    GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO
  );
  assert.equal(
    classifyCoverageGap({ dealalityCount: 90, benchmarkCount: 100 }),
    GAP_CLASS.BENCHMARK_ALIGNED
  );
  assert.equal(
    classifyCoverageGap({ dealalityCount: 70, benchmarkCount: 100 }),
    GAP_CLASS.POSSIBLE_MINOR_GAP
  );
  assert.equal(
    classifyCoverageGap({ dealalityCount: 50, benchmarkCount: 100 }),
    GAP_CLASS.POSSIBLE_MODERATE_GAP
  );
  assert.equal(
    classifyCoverageGap({ dealalityCount: 20, benchmarkCount: 100 }),
    GAP_CLASS.POSSIBLE_MAJOR_GAP
  );
  assert.equal(
    classifyCoverageGap({ dealalityCount: 200, benchmarkCount: 100 }),
    GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK
  );
  assert.equal(
    classifyCoverageGap({ dealalityCount: 0, benchmarkCount: 0 }),
    GAP_CLASS.INSUFFICIENT_COMPARISON_DATA
  );
});

test("coverageRatio", () => {
  assert.equal(coverageRatio(50, 100), 0.5);
  assert.equal(coverageRatio(10, 0), null);
});

test("approximateAggregateMatchCount is aggregate-only", () => {
  const d = [
    { name: "Hotel Palm", city: "Willemstad", country: "Curaçao" },
    { name: "Other Inn", city: "Westpunt", country: "Curaçao" },
  ];
  const b = [
    { name: "Hotel Palm", city: "Willemstad" },
    { name: "Benchmark Only Resort", city: "Willemstad" },
  ];
  const n = approximateAggregateMatchCount(d, b);
  assert.equal(n, 1);
});

test("scoreDiscoveryPriority elevates zero Dealality with benchmark", () => {
  const zero = scoreDiscoveryPriority({
    dealality_census_count: 0,
    BENCHMARK_COUNT: 100,
    tourism_priority: "S",
    dealality_coverage_status: "ZERO_CONFIRMED_PROPERTIES",
    active_holds: 0,
    gap_class: GAP_CLASS.ZERO_DEALALITY_BENCHMARK_NONZERO,
  });
  const higher = scoreDiscoveryPriority({
    dealality_census_count: 500,
    BENCHMARK_COUNT: 200,
    tourism_priority: "S",
    dealality_coverage_status: "CORE_COVERAGE_STRONG",
    active_holds: 0,
    gap_class: GAP_CLASS.DEALALITY_HIGHER_THAN_BENCHMARK,
  });
  assert.ok(zero > higher);
});
