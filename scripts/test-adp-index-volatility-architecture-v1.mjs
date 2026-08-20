#!/usr/bin/env node
/**
 *   npm run test:adp-index-volatility-architecture-v1
 */

import assert from "assert";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { runIndexVolatilityArchitectureDecision } from "../lib/ai-demand-positioning/metrics/index-volatility-architecture-v1.js";
import { expectedShareEqualFair } from "../lib/ai-demand-positioning/metrics/aci-research-engine.js";
import { coreIdsForIntent } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { periodComparableForPresenceV2 } from "../lib/ai-demand-positioning/metrics/presence-index-v2-audit.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const period =
    [...periods].reverse().find((p) => p.scenarioCount === 65 && (p.observations || []).some((o) => o.parsed)) ||
    [...periods].reverse().find((p) => (p.observations || []).some((o) => o.parsed));
  const scenarios = buildScenarioUniverse(profile);

  const comparable = periods.filter((p) => periodComparableForPresenceV2(p, period).comparable);
  assert.ok(comparable.length >= 1);

  const report = runIndexVolatilityArchitectureDecision({
    period,
    scenarios,
    propertyProfile: profile,
    allPeriods: periods,
  });

  assert.ok(report.periods.COMPARABLE_PERIODS.length >= 1);
  assert.ok(report.periods.PERIODS_EXCLUDED.length >= 1);
  assert.equal(report.volatilityRootCause.length, 8);
  assert.equal(report.aci.EXPECTED_SHARE_PERIOD_VARIANCE, 0);
  assert.equal(expectedShareEqualFair(coreIdsForIntent("business").length), 1 / 7);

  const wellness = report.scenarioDensity.find((r) => r.TERRITORY === "Wellness");
  assert.equal(wellness.SCENARIO_COUNT, 8);

  assert.equal(report.indexStrategy.choice, "NO_INDEX_YET_RATES_AND_BENCHMARKS");
  assert.equal(report.recommendedExecutiveRepresentation.choice, "SUBJECT_RATE_PLUS_BENCHMARK");
  assert.equal(report.nextMeasurementWave.RUN_NEW_WAVE_NOW, "NO");
  assert.equal(report.wellness.RECOMMENDATION, "EXPAND_SCENARIOS");
  assert.equal(report.regression.ADP_UI_DIFF, 0);
  assert.equal(report.execution.PROVIDER_CALLS, 0);

  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  assert.ok(owner.intentPresenceIndex);
  assert.ok(!owner.presenceIndexV2);
  assert.ok(!owner.volatilityArchitecture);

  const couples = report.extremePresenceIndexValues.find((e) => e.TERRITORY.includes("Couples"));
  if (couples) {
    assert.ok(couples.INDEX > 200);
    assert.ok(couples.CLASSIFICATION);
  }

  console.log("test:adp-index-volatility-architecture-v1 — PASS");
}

main();
