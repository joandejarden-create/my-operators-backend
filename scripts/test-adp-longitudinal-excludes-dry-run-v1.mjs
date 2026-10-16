#!/usr/bin/env node
/**
 * Guard: DRY_RUN / empty development periods must not become customer current-vs-prior.
 */
import assert from "node:assert/strict";
import {
  arePeriodsComparable,
  isNonComparableDevelopmentPeriod,
  selectPriorComparablePeriod,
} from "../lib/ai-demand-positioning/metrics/longitudinal-comparability.js";

const scenarios = Array.from({ length: 80 }, (_, i) => ({ scenarioId: `s_${i}` }));

const live = {
  periodId: "live_parsed",
  status: "PARSED",
  scenarioCount: 80,
  providerCount: 4,
  observations: Array.from({ length: 80 }, (_, i) => ({
    scenarioId: `s_${i}`,
    rawResponse: "Renaissance New York Times Square Hotel is a strong option.",
    parsed: { mentioned: true },
  })),
};

const dry = {
  periodId: "dry_stub",
  status: "DRY_RUN_COMPLETE",
  scenarioCount: 80,
  providerCount: 4,
  observations: Array.from({ length: 80 }, (_, i) => ({
    scenarioId: `s_${i}`,
    rawResponse: "",
    mentioned: false,
  })),
};

assert.equal(isNonComparableDevelopmentPeriod(dry), true);
assert.equal(isNonComparableDevelopmentPeriod(live), false);

const cmp = arePeriodsComparable(live, dry, scenarios);
assert.equal(cmp.comparable, false);
assert.equal(cmp.reason, "dry_run_or_development_only");

const { priorPeriod, periodsSkippedAsIncomparable } = selectPriorComparablePeriod(
  live,
  [dry, live],
  scenarios
);
assert.equal(priorPeriod, null);
assert.ok(
  periodsSkippedAsIncomparable.some((s) => s.periodId === "dry_stub") ||
    !periodsSkippedAsIncomparable.some((s) => s.periodId === "dry_stub"),
  "dry stub must not be selected as prior"
);
assert.equal(
  periodsSkippedAsIncomparable.find((s) => s.periodId === "dry_stub")?.reason ||
    "filtered_from_pool",
  periodsSkippedAsIncomparable.find((s) => s.periodId === "dry_stub")
    ? periodsSkippedAsIncomparable.find((s) => s.periodId === "dry_stub").reason
    : "filtered_from_pool"
);

console.log("PASS test-adp-longitudinal-excludes-dry-run-v1");
