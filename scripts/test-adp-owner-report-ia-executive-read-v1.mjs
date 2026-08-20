#!/usr/bin/env node
/**
 *   npm run test:adp-owner-report-ia-executive-read-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  classifyCurrentPosition,
  classifyPrimaryStrength,
  classifyPrimaryConstraint,
  classifyTrend,
  buildExecutiveRead,
  assertNoUnsupportedCausalLanguage,
  PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE,
  TREND_STATES,
  MATERIALITY_PP_THRESHOLD,
} from "../lib/ai-demand-positioning/customer/executive-read-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const REPORT = join(process.cwd(), "reports/ai-demand-positioning/owner-report-ia-executive-read-v1.json");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const SHARE = join(process.cwd(), "public/owner-ai-demand-share.html");
const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

const EXPECTED = [
  "executive-summary",
  "property-snapshot",
  "ai-demand-positioning-metrics",
  "ai-presence-by-demand-territory",
  "trends",
  "provider-presence",
  "ai-reality-gaps",
  "ai-competitive-set",
  "competitive-context-priority-actions",
  "evidence-sources-discovery",
];

function orderFrom(html) {
  return [...html.matchAll(/data-adp-section="([^"]+)"/g)].map((m) => m[1]);
}

async function main() {
  const report = JSON.parse(readFileSync(REPORT, "utf8"));
  assert.equal(report.execution.PROVIDER_CALLS, 0);
  assert.equal(report.ORDER_MATCH, "YES");
  assert.equal(PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE, 0);

  const html = readFileSync(HTML, "utf8");
  const share = readFileSync(SHARE, "utf8");
  const ui = readFileSync(UI, "utf8");
  assert.deepStrictEqual(orderFrom(html), EXPECTED);
  assert.deepStrictEqual(orderFrom(share), EXPECTED);
  assert.ok(ui.includes("renderExecutiveRead"));
  assert.ok(html.includes("AI Presence by Demand Territory"));
  assert.ok(!html.includes("AI Presence by Intent Category"));
  assert.ok(html.includes("adpExecutiveMetricsSection"));
  assert.ok(html.includes("adpKpiRow"));

  // Classifiers
  assert.ok(
    classifyCurrentPosition({
      scenarioPresence: 76,
      consideration: 50,
      top3: 72,
      reality: 41,
    }).includes("BROAD") ||
      classifyCurrentPosition({
        scenarioPresence: 76,
        consideration: 50,
        top3: 72,
        reality: 41,
      }).includes("VISIBILITY")
  );
  assert.equal(
    classifyCurrentPosition({ scenarioPresence: 14, consideration: 4 }),
    "LIMITED_AI_DEMAND_REACH"
  );
  assert.equal(classifyTrend(null, { hasComparablePrior: false, periodCount: 1 }), TREND_STATES.BASELINE_PERIOD);
  assert.equal(classifyTrend({ considerationRate: 0.5 }, { hasComparablePrior: true }), TREND_STATES.BROADLY_STABLE);
  assert.equal(
    classifyTrend({ considerationRate: 4.5 }, { hasComparablePrior: true }),
    TREND_STATES.IMPROVING
  );
  assert.ok(MATERIALITY_PP_THRESHOLD >= 1);

  const strength = classifyPrimaryStrength({ scenarioPresence: 80, consideration: 40, top3: 70 });
  assert.ok(strength.label);
  const constraint = classifyPrimaryConstraint({
    scenarioPresence: 80,
    consideration: 40,
    reality: 30,
  });
  assert.ok(constraint.label);

  for (const propertyId of [
    "adp_waterstone_boca_raton",
    "adp_cambridge_beaches_bermuda",
    "adp_renaissance_times_square",
    "adp_now_now_noho",
  ]) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const periods = loadAllPeriods(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
    assert.ok(payload.ok);
    assert.ok(payload.executiveRead?.narrative, `${propertyId} executive read`);
    assert.equal(payload.executiveRead.PROPERTY_SPECIFIC_EXECUTIVE_READ_CODE, 0);
    assert.equal(payload.executiveRead.safety.FREE_FORM_UNGOVERNED_NARRATIVE, 0);
    const causal = assertNoUnsupportedCausalLanguage(payload.executiveRead.narrative);
    assert.ok(causal.ok, `${propertyId} causal: ${causal.hits}`);
    // Metric formulas untouched — rates still present when previously available
    if (propertyId === "adp_waterstone_boca_raton") {
      assert.ok(payload.executiveMetrics?.considerationRate?.rate != null);
      assert.ok(payload.executiveMetrics?.scenarioPresence?.rate != null);
    }
  }

  // Insufficient metrics path
  const emptyRead = buildExecutiveRead({ property: { name: "Test Hotel" } }, null, [], {});
  assert.equal(emptyRead.available, false);
  assert.equal(emptyRead.CURRENT_READ_AVAILABLE, false);
  assert.equal(emptyRead.narrative, null);
  assert.ok(emptyRead.trend?.narrative);

  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE);
  assert.equal(regression.PHASE1_METRIC_DIFF, 0);
  assert.equal(regression.INDEX_DIFF, 0);

  assert.equal(report.regression.WATERSTONE_INDEX_DIFF, 0);
  assert.equal(report.evidence.EVIDENCE_LAST, "YES");
  assert.equal(report.evidence.RAW_PROMPT_LEAKS, 0);

  console.log("test:adp-owner-report-ia-executive-read-v1 — PASS");
  console.log("  final:", report.final);
  console.log("  ORDER_MATCH:", report.ORDER_MATCH);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
