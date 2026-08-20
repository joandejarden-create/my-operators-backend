#!/usr/bin/env node
/**
 *   npm run test:adp-scenario-density-benchmark-rate-v1
 */

import assert from "assert";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { FINAL_NEW_SCENARIOS } from "../lib/ai-demand-positioning/prompt-universe/scenario-expansion-catalog-v1.js";
import { runScenarioDensityAndBenchmarkRateArchitecture } from "../lib/ai-demand-positioning/metrics/scenario-density-benchmark-rate-architecture-v1.js";
import {
  ZERO_CORE_PEERS_INCLUDED,
  SECONDARY_IN_BENCHMARK,
} from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const period = [...periods].reverse().find((p) => (p.observations || []).some((o) => o.parsed));
  const scenarios = buildScenarioUniverse(profile);

  assert.equal(scenarios.length, 78, "live universe must be 78 after catalog activation");
  assert.equal(
    scenarios.filter((s) => String(s.scenarioId).startsWith("cand_")).length,
    0,
    "catalog candidates must not enter live universe"
  );

  const report = runScenarioDensityAndBenchmarkRateArchitecture({
    period,
    scenarios,
    propertyProfile: profile,
    allPeriods: periods,
  });

  assert.equal(report.liveScenarioCount, 78);
  assert.equal(report.family.TARGET, 9);
  assert.equal(report.family.PROPOSED_NEW.length, 4);
  assert.equal(report.wellness.KEEP_DISTINCT, "YES");
  assert.equal(report.wellness.TARGET_COUNT, 8);
  assert.equal(report.adventure.INDEXABLE_FUTURE, "NO");
  assert.equal(report.celebrations.TARGET, 8);
  assert.equal(report.businessResortCouples.Business, "NO_CHANGE");
  assert.equal(report.benchmarkRateContract.ZERO_CORE_PEERS_INCLUDED, ZERO_CORE_PEERS_INCLUDED);
  assert.equal(report.benchmarkRateContract.SECONDARY_IN_BENCHMARK, SECONDARY_IN_BENCHMARK);
  assert.equal(report.futureCustomerRepresentation.CUSTOMER_INDEX_REQUIRED, "NO");
  assert.equal(report.futureMeasurementWave.RUN_NOW, "NO");
  assert.equal(report.aci.STATUS, "FROZEN_RESEARCH_READY");
  assert.equal(report.regression.ADP_UI_DIFF, 0);
  assert.equal(report.execution.PROVIDER_CALLS, 0);
  assert.equal(FINAL_NEW_SCENARIOS.length, 13);
  assert.ok(report.FINAL_NEW_SCENARIOS.every((s) => s.promptTextExposed === false));
  assert.ok(report.existingScenarioAudit.every((s) => s.promptTextExposed === false));
  assert.ok(!JSON.stringify(report.FINAL_NEW_SCENARIOS).includes("queryInternal"));

  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  assert.ok(owner.intentPresenceIndex);
  assert.ok(!owner.coreBenchmarkRate);
  assert.ok(!owner.presenceIndexV2);
  assert.strictEqual(owner.intentPresenceIndex.wellness.index, null);

  const familyRow = report.currentTerritoryReadiness.find((t) => t.intent === "family");
  assert.equal(familyRow.SCENARIO_COUNT, 9);

  const wellnessRow = report.currentTerritoryReadiness.find((t) => t.intent === "wellness");
  assert.equal(wellnessRow.SCENARIO_COUNT, 8);

  console.log("test:adp-scenario-density-benchmark-rate-v1 — PASS");
}

main();
