#!/usr/bin/env node
/**
 *   npm run test:adp-governed-ai-presence-index-remediation-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  LOO_CORE_PP_MAX,
  LOO_SUBJECT_PP_MAX,
  assertNumericIndexHasRates,
  LEGACY_INDEX_CUSTOMER_RENDER,
} from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { scenarioLeaveOneOutRates } from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";
import { coreIdsForIntent } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";

const SOURCE_PERIOD_ID = "adp_period_adp_waterstone_boca_raton_20260820053047_9cb18e";
const REPORT = join(process.cwd(), "reports/ai-demand-positioning/governed-ai-presence-index-remediation-v2.json");

function main() {
  const report = JSON.parse(readFileSync(REPORT, "utf-8"));
  assert.equal(report.sourcePeriod.SOURCE_PERIOD_MUTATED, "NO");
  assert.equal(report.gemini.CALLS_EXECUTED, 0);
  assert.equal(report.adventure.FABRICATED_PEER, "NO");
  assert.equal(report.adventure.CORE_COUNT, 3);
  assert.equal(LOO_SUBJECT_PP_MAX, 10);
  assert.equal(LOO_CORE_PP_MAX, 5);

  const profile = loadPropertyProfile("adp_waterstone_boca_raton");
  const period = loadAllPeriods("adp_waterstone_boca_raton").find((p) => p.periodId === SOURCE_PERIOD_ID);
  const scenarios = buildScenarioUniverse(profile);
  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: loadAllPeriods("adp_waterstone_boca_raton") });

  const validated = Object.values(owner.intentPresenceIndex).filter((r) => r.status === "PRODUCTION_VALIDATED");
  assert.ok(validated.length >= 4, "MODEL_D should certify at least 4 territories");

  const leisure = owner.intentPresenceIndex[TRAVELER_INTENTS.LEISURE];
  const couples = owner.intentPresenceIndex[TRAVELER_INTENTS.COUPLES];
  const family = owner.intentPresenceIndex[TRAVELER_INTENTS.FAMILY];
  const cel = owner.intentPresenceIndex[TRAVELER_INTENTS.CELEBRATION];
  assert.equal(leisure.status, "PRODUCTION_VALIDATED");
  assert.equal(couples.status, "PRODUCTION_VALIDATED");
  assert.equal(family.status, "PRODUCTION_VALIDATED");
  assert.equal(cel.status, "PRODUCTION_VALIDATED");
  assert.ok(couples.index > 200, "large index allowed");

  const adventure = owner.intentPresenceIndex[TRAVELER_INTENTS.ADVENTURE];
  assert.equal(adventure.status, "BENCHMARK_DEVELOPING");
  assert.equal(adventure.coreCount, 3);

  const business = owner.intentPresenceIndex[TRAVELER_INTENTS.BUSINESS];
  assert.equal(business.status, "CONDITIONALLY_ELIGIBLE");
  assert.ok(business.blockers.includes("provider_concentration"));

  for (const row of Object.values(owner.intentPresenceIndex)) {
    const gate = assertNumericIndexHasRates(row);
    assert.ok(gate.ok);
    if (row.index != null) {
      assert.equal(row.status, "PRODUCTION_VALIDATED");
    }
  }

  assert.equal(LEGACY_INDEX_CUSTOMER_RENDER, 0);

  const looCouples = scenarioLeaveOneOutRates(period.observations, scenarios, TRAVELER_INTENTS.COUPLES);
  assert.ok(looCouples.maxBenchmarkPpMove <= LOO_CORE_PP_MAX);
  assert.ok(looCouples.maxSubjectPpMove <= LOO_SUBJECT_PP_MAX);

  const failed = period.observations.filter((o) => o.error && o.provider === "gemini");
  assert.equal(failed.length, 3);
  assert.equal(filterComparableObservations(period.observations).length, 309);

  console.log("test:adp-governed-ai-presence-index-remediation-v2 — PASS");
  console.log("  PRODUCTION_VALIDATED:", validated.length);
}

main();
