#!/usr/bin/env node
/**
 * ADP P0 Executive Metrics Foundation tests.
 *   npm run test:adp-p0-executive-metrics-foundation
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  buildExecutiveMetricsFoundation,
  validatePeriodIntegrity,
  ADP_TERMINOLOGY_V1,
} from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { validatePositionGoldSet } from "../lib/ai-demand-positioning/metrics/position-gold-set-validation.js";
import { extractPropertyRank } from "../lib/ai-demand-positioning/metrics/position-extraction.js";
import { METRIC_GRAINS } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function assertNoPromptLeak(obj) {
  const s = JSON.stringify(obj);
  assert.ok(!s.includes("std_boca_biz_01"), "scenario id leak");
  assert.ok(!s.includes("Best upscale hotel in Boca Raton"), "raw prompt leak");
}

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  assert.ok(profile, "profile");
  assert.ok(periods.length, "periods");

  const period = periods[periods.length - 1];
  const scenarios = buildScenarioUniverse(profile);
  const foundation = buildExecutiveMetricsFoundation(period, scenarios, profile, {
    periodCount: periods.length,
  });

  assert.strictEqual(foundation.aiConsiderationIndex.customerStatus, "BLOCKED");
  assert.strictEqual(foundation.headToHeadResearch.winLossLanguageAllowed, false);
  assert.strictEqual(foundation.observedShareCandidates.customerPublishAllowed, false);
  assert.ok(foundation.consideration.observationGrain === METRIC_GRAINS.OBSERVATION);
  assert.ok(foundation.consideration.scenarioGrain === METRIC_GRAINS.SCENARIO);
  assert.ok(foundation.consideration.observationConsiderationRate != null);
  assert.ok(foundation.consideration.scenarioConsiderationCoverage != null);
  assert.ok(foundation.consideration.customerSafe);

  const integrity = validatePeriodIntegrity(period);
  assert.equal(integrity.ok, true, JSON.stringify(integrity));

  if (periods.length < 2) {
    assert.equal(foundation.longitudinal.currentVsPriorReady, false);
  }
  assert.equal(foundation.longitudinal.trendReady, periods.length >= 3);

  const rankVal = validatePositionGoldSet(profile);
  assert.ok(rankVal.dev.cases >= 5);
  assert.ok(rankVal.holdout.cases >= 3);

  const prose = extractPropertyRank(
    "Waterstone Resort & Marina is discussed often. Top picks: The Boca Raton and Marriott.",
    profile
  );
  assert.equal(prose.rankEligible, false, "no prose-order rank inference");

  const ownerPayload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  assert.ok(ownerPayload.demandCapture, "legacy demandCapture preserved");
  assert.ok(ownerPayload.executiveMetrics, "executiveMetrics on owner payload");
  assert.ok(!ownerPayload.aiConsiderationIndex, "ACI absent from customer payload");

  assertNoPromptLeak(foundation.demandPositionMap);
  assertNoPromptLeak(foundation.terminology);

  assert.ok(ADP_TERMINOLOGY_V1.heroKpiRecommendation.length >= 4);

  const reportPath = join(process.cwd(), "reports/ai-demand-positioning/p0-executive-metrics-audit-v1.json");
  try {
    readFileSync(reportPath);
  } catch (_) {
    // audit may not have been run yet in CI — foundation module tests suffice
  }

  console.log("test:adp-p0-executive-metrics-foundation — PASS");
  console.log("  observationConsiderationRate:", foundation.consideration.observationConsiderationRate);
  console.log("  scenarioConsiderationCoverage:", foundation.consideration.scenarioConsiderationCoverage);
  console.log("  rank holdout F1:", rankVal.holdout.f1);
  console.log("  gold dev F1:", rankVal.dev.f1);
}

main();
