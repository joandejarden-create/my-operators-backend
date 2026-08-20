#!/usr/bin/env node
/**
 *   npm run test:adp-targeted-multi-property-governed-benchmark-measurement-v1
 */

import assert from "assert";
import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  TARGET_TERRITORIES_BY_PROPERTY,
  TARGET_PROPERTY_IDS,
  buildPropertyPreflightPlan,
  validateTargetedExecutionArchitecture,
  selectTargetScenarios,
  certifyTargetedTerritory,
  runTargetedMultiPropertyGovernedBenchmarkMeasurementV1,
  COST_CAP_USD,
  MEASUREMENT_VERSION,
} from "../lib/ai-demand-positioning/execution/targeted-multi-property-governed-benchmark-measurement-v1.js";
import { loadPropertyProfile, loadLatestPeriod, loadPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  CUSTOMER_NUMERIC_INDEX_PROMOTION,
  PROPERTY_STABILIZED_CORE_IDS,
  MIN_CORE_HOTELS_PRODUCTION,
} from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { coreIdsForIntent } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { LOO_CORE_PP_MAX, LOO_SUBJECT_PP_MAX } from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { runPropertyCoreGovernance } from "../lib/ai-demand-positioning/metrics/property-specific-core-benchmark-governance-v1.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";

const REPORT = join(
  process.cwd(),
  "reports/ai-demand-positioning/targeted-multi-property-governed-benchmark-measurement-v1.json"
);
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);
const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");

const DEVELOPING_INTENTS = {
  adp_renaissance_times_square: [
    TRAVELER_INTENTS.CELEBRATION,
    TRAVELER_INTENTS.WELLNESS,
    TRAVELER_INTENTS.ADVENTURE,
  ],
  adp_cambridge_beaches_bermuda: [
    TRAVELER_INTENTS.BUSINESS,
    TRAVELER_INTENTS.GROUP_MEETING,
    TRAVELER_INTENTS.ADVENTURE,
  ],
  adp_now_now_noho: [
    TRAVELER_INTENTS.CELEBRATION,
    TRAVELER_INTENTS.FAMILY,
    TRAVELER_INTENTS.GROUP_MEETING,
    TRAVELER_INTENTS.WELLNESS,
    TRAVELER_INTENTS.ADVENTURE,
  ],
};

async function main() {
  const plans = TARGET_PROPERTY_IDS.map((id) => buildPropertyPreflightPlan(id));
  const architecture = validateTargetedExecutionArchitecture(plans);

  assert.equal(architecture.pass, true, "targeted execution architecture must pass");
  assert.notEqual(architecture.status, "TARGETED_EXECUTION_ARCHITECTURE_REMEDIATION_REQUIRED");
  assert.equal(architecture.TOTAL_TARGET_TERRITORIES, 12);
  assert.ok(architecture.TOTAL_PROVIDER_CALLS < plans.reduce((n, p) => n + p.fullUniverseCalls, 0));
  assert.ok(architecture.ESTIMATED_COST <= COST_CAP_USD);

  for (const plan of plans) {
    assert.ok(plan.SCENARIO_COUNT > 0);
    assert.equal(plan.PLANNED_CALLS, plan.SCENARIO_COUNT * 4);
    for (const intent of TARGET_TERRITORIES_BY_PROPERTY[plan.propertyId]) {
      const gov = runPropertyCoreGovernance(plan.propertyId);
      const label = territoryLabelForIntent(intent);
      const row = gov.coreGovernance?.find((t) => t.TERRITORY === label);
      assert.equal(row?.STATUS, "CORE_TRUTH_READY", `${plan.propertyId} ${intent} must be CORE_TRUTH_READY`);
    }
    for (const intent of DEVELOPING_INTENTS[plan.propertyId] || []) {
      assert.ok(
        !TARGET_TERRITORIES_BY_PROPERTY[plan.propertyId].includes(intent),
        `${plan.propertyId} must not target developing intent ${intent}`
      );
    }
  }

  for (const propertyId of TARGET_PROPERTY_IDS) {
    const profile = loadPropertyProfile(propertyId);
    for (const intent of TARGET_TERRITORIES_BY_PROPERTY[propertyId]) {
      const coreIds = coreIdsForIntent(intent, profile);
      assert.ok(coreIds.length >= MIN_CORE_HOTELS_PRODUCTION || coreIds.length === 0);
      const frozen = PROPERTY_STABILIZED_CORE_IDS[propertyId]?.[intent] || [];
      assert.deepEqual(coreIds, frozen, `property-specific CORE for ${propertyId} ${intent}`);
    }
  }

  const report = await runTargetedMultiPropertyGovernedBenchmarkMeasurementV1({ apply: false, forkFromPrior: true });
  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(REPORT, JSON.stringify(report, null, 2));
  assert.equal(report.waterstone.PROVIDER_CALLS, 0);
  assert.equal(report.waterstone.INDEX_DIFF, 0);
  assert.equal(report.waterstone.CERTIFIED_TERRITORIES, 4);
  assert.equal(report.aci.CUSTOMER_STATUS, "BLOCKED");
  assert.equal(report.customerSafety.LEGACY_INDEX_FALLBACK, 0);
  assert.equal(report.customerSafety.MIXED_METHODOLOGY_ROWS, 0);
  assert.equal(report.trendSafety.TARGETED_PERIODS_MARKED, "YES");
  assert.equal(report.regression.ADP_VISIBLE_SECTION_DIFF, 0);

  for (const exec of report.execution.perProperty) {
    const period = loadPeriod(exec.NEW_PERIOD);
    assert.ok(period, `period ${exec.NEW_PERIOD}`);
    assert.equal(period.measurementScope?.type, "TARGETED_CORE_TRUTH_V1");
    assert.equal(period.measurementScope?.notComparableToFullPropertyPeriod, true);
  }

  for (const row of report.territoryCertification) {
    if (row.CUSTOMER_STATUS === "PRODUCTION_VALIDATED") {
      assert.ok(row.YOUR_AI_PRESENCE != null);
      assert.ok(row.CORE_BENCHMARK != null);
      assert.ok(row.AI_PRESENCE_INDEX != null);
      assert.ok(row.CORE_COUNT >= MIN_CORE_HOTELS_PRODUCTION);
    }
    if (row.STATUS === "PRODUCTION_VALIDATED") {
      assert.ok(row.AI_PRESENCE_INDEX == null || row.AI_PRESENCE_INDEX > 0);
    }
  }

  assert.ok(CUSTOMER_NUMERIC_INDEX_PROMOTION.adp_renaissance_times_square);
  assert.ok(CUSTOMER_NUMERIC_INDEX_PROMOTION.adp_cambridge_beaches_bermuda);
  assert.equal(CUSTOMER_NUMERIC_INDEX_PROMOTION.adp_now_now_noho, false);

  const ui = readFileSync(UI_JS, "utf-8");
  assert.ok(ui.includes("Based on"));
  assert.ok(ui.includes("CORE comparable hotels"));

  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE);
  assert.equal(regression.PHASE1_METRIC_DIFF, 0);
  assert.equal(regression.INDEX_DIFF, 0);

  const brandSmoke = await getPublishedOwnerReport("adp_waterstone_boca_raton");
  assert.ok(brandSmoke.ok !== false);

  assert.ok(existsSync(REPORT), "run script must write report");

  console.log("test:adp-targeted-multi-property-governed-benchmark-measurement-v1 PASS");
  console.log("  territories certified:", report.summary.PRODUCTION_VALIDATED);
  console.log("  customer numeric:", report.summary.CUSTOMER_NUMERIC_PROMOTED);
  console.log("  final:", report.final);
  console.log("  next:", report.next);
  console.log("  MODEL_D gates:", `subject<=${LOO_SUBJECT_PP_MAX}pp core<=${LOO_CORE_PP_MAX}pp`);
  console.log("  version:", MEASUREMENT_VERSION);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
