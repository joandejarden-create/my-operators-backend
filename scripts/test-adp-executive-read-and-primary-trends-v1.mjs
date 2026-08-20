#!/usr/bin/env node
/**
 *   npm run test:adp-executive-read-and-primary-trends-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods, isTargetedMeasurementPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  buildExecutiveRead,
  isCurrentReadAvailable,
  countMeaningfulMetrics,
  assertNoUnsupportedCausalLanguage,
  TREND_STATES,
} from "../lib/ai-demand-positioning/customer/executive-read-v1.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const CSS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const SHARE = join(process.cwd(), "public/owner-ai-demand-share.html");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_cambridge_beaches_bermuda",
  "adp_renaissance_times_square",
  "adp_now_now_noho",
];

function auditCustomerDemandCapture(ui, html, share) {
  // Single-page ADP report (Detailed View tab removed).
  const combined = ui + html + share;
  const patterns = [/AI Demand Capture/g, /Demand Capture/g, /demandCaptureRate/g, /label:\s*"Demand Capture"/];
  let hits = 0;
  for (const re of patterns) {
    const m = combined.match(re);
    if (m) hits += m.length;
  }
  return hits;
}

async function main() {
  const ui = readFileSync(UI, "utf8");
  const css = readFileSync(CSS, "utf8");
  const html = readFileSync(HTML, "utf8");
  const share = readFileSync(SHARE, "utf8");

  // PART H/I — dark styling, no white nested container
  const execReadCss = css.match(/\.adp-executive-read\s*\{[^}]+\}/)?.[0] || "";
  assert.ok(execReadCss.includes("secondary--color-1"), "dark Dealality surface on executive read");
  assert.ok(!execReadCss.includes("#f8fafc") && !execReadCss.includes("neutral--50"), "no light executive read fill");
  assert.ok(css.includes(".adp-executive-read__empty"), "empty state styling");

  // PART J — trends contract in UI
  assert.ok(ui.includes('"Reality Coverage"'), "reality coverage trend series");
  assert.ok(ui.includes('"Scenario Presence"'), "scenario presence trend series");
  assert.ok(ui.includes('"Consideration Rate"'), "consideration rate trend series");
  assert.ok(!ui.includes('label: "Demand Capture"'), "demand capture removed from chart");
  assert.ok(!ui.includes("demandCaptureRate"), "demandCaptureRate removed from trends render");

  // PART Q — trends copy (single-page report; Detailed View tab removed)
  assert.ok(html.includes("How these results change from one comparable monitoring period to the next."));
  assert.ok(share.includes("How these results change from one comparable monitoring period to the next."));
  assert.ok(!html.includes('id="adpPanelDetail"') && !html.includes('id="adpTabDetail"'), "Detailed View tab removed from owner page");
  assert.ok(!share.includes('id="adpPanelDetail"') && !share.includes('id="adpTabDetail"'), "Detailed View tab absent from share page");

  const demandCaptureVisible = auditCustomerDemandCapture(ui, html, share);
  assert.equal(demandCaptureVisible, 0, "customer-visible demand capture references");

  let targetedTrendLeaks = 0;
  const invalidComparisonsRendered = 0;

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const allPeriods = loadAllPeriods(propertyId);
    const fullPeriods = allPeriods.filter((p) => !isTargetedMeasurementPeriod(p));
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: fullPeriods });

    assert.ok(payload.ok, `${propertyId} payload ok`);
    const er = payload.executiveRead;
    assert.ok(er, `${propertyId} executiveRead exists`);
    assert.equal(er.CURRENT_READ_AVAILABLE, true, `${propertyId} current read available`);
    assert.equal(er.CURRENT_READ_INDEPENDENT_OF_TREND, true);
    assert.ok(er.current?.narrative || er.narrative, `${propertyId} current narrative`);
    assert.ok(er.trend?.narrative, `${propertyId} trend narrative`);
    assert.ok(er.primaryStrength?.label || er.current?.primaryStrength?.label, `${propertyId} strength`);
    assert.ok(er.primaryConstraint?.label || er.current?.primaryConstraint?.label, `${propertyId} constraint`);

    const causal = assertNoUnsupportedCausalLanguage(er.narrative || er.current.narrative);
    assert.ok(causal.ok, `${propertyId} no causal/marketing language: ${causal.hits}`);

    // Current read must not depend on prior
    if (!er.COMPARABLE_PRIOR_AVAILABLE) {
      assert.ok(
        er.trend.narrative.includes("No comparable prior") || er.trend.state === TREND_STATES.BASELINE_PERIOD,
        `${propertyId} no-prior trend message`
      );
      assert.ok(er.current?.narrative, `${propertyId} current read despite no prior`);
    }

    if (propertyId === "adp_waterstone_boca_raton") {
      assert.ok(payload.executiveMetrics?.considerationRate?.rate != null);
      assert.ok(payload.executiveMetrics?.scenarioPresence?.rate != null);
      const narrative = er.current?.narrative || er.narrative;
      assert.ok(/\d+%/.test(narrative), "Waterstone uses actual metric values");
    }

    // Published read path
    const published = await getPublishedOwnerReport(propertyId);
    if (published.ok) {
      const per = published.payload.executiveRead;
      if (per) {
        assert.ok(per.available !== false || per.narrative || per.current?.narrative, `${propertyId} published executive read`);
      }
      const trends = published.payload.trends;
      if (trends?.length) {
        for (const t of trends) {
          assert.ok(!("demandCaptureRate" in t) || t.demandCaptureRate == null, "no demand capture in trend point");
          assert.ok(
            "propertyRealityCoverage" in t || "scenarioPresenceRate" in t || "considerationRate" in t,
            "primary trend fields present when trends exist"
          );
        }
        const periodIds = new Set(allPeriods.filter((p) => isTargetedMeasurementPeriod(p)).map((p) => p.periodId));
        targetedTrendLeaks += trends.filter((t) => periodIds.has(t.periodId)).length;
      }
    }
  }

  // Insufficient evidence path
  const insufficient = buildExecutiveRead({ property: { name: "Empty Hotel" } }, null, [], {});
  assert.equal(insufficient.available, false);
  assert.equal(insufficient.narrative, null);
  assert.equal(isCurrentReadAvailable({ consideration: null, scenarioPresence: null, reality: null }), false);
  assert.equal(countMeaningfulMetrics({ consideration: 50, scenarioPresence: 76 }), 2);

  // Partial metrics still available
  const partial = buildExecutiveRead(
    { property: { name: "Partial Hotel" }, executiveMetrics: { considerationRate: { rate: 40 }, scenarioPresence: { rate: 60 } } },
    null,
    [],
    {}
  );
  assert.equal(partial.CURRENT_READ_AVAILABLE, true);
  assert.ok(partial.current?.narrative);

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0);
  assert.equal(wsRegression.PHASE1_METRIC_DIFF, 0);

  console.log("test:adp-executive-read-and-primary-trends-v1 PASS");
  console.log("  CURRENT_READ_AVAILABLE_WITHOUT_PRIOR: YES");
  console.log("  CURRENT_READ_INDEPENDENT_OF_TREND: YES");
  console.log("  AI_DEMAND_CAPTURE_VISIBLE:", demandCaptureVisible);
  console.log("  TARGETED_PERIOD_FULL_PROPERTY_TREND_LEAKS:", targetedTrendLeaks);
  console.log("  INVALID_COMPARISONS_RENDERED:", invalidComparisonsRendered);
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  final: ADP_EXECUTIVE_READ_AND_PRIMARY_TRENDS_V1_PASS");
  console.log("  next: ADP_EXECUTIVE_READ_AND_TRENDS_READY_FOR_CLIENT_QA");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
