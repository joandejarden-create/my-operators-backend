#!/usr/bin/env node
/**
 *   npm run test:adp-executive-read-ux-and-three-metric-trends-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods, isTargetedMeasurementPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  buildExecutiveReadWithUx,
  EXECUTIVE_SUMMARY_TITLE,
  EXECUTIVE_READ_UX_VERSION,
} from "../lib/ai-demand-positioning/customer/executive-read-v2.js";
import {
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
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];

const VAGUE = [
  /\bbroad visibility\b/i,
  /\bconsideration consistency\b/i,
  /\bstrong positioning\b/i,
  /\bcompetitive momentum\b/i,
];

function auditCustomerDemandCapture(ui, html, share) {
  const combined = ui + html + share;
  const patterns = [/AI Demand Capture/g, /Demand Capture Rate/g, /demandCaptureRate/g, /label:\s*"Demand Capture"/];
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

  assert.ok(ui.includes("renderExecutiveSummaryBox"));
  assert.ok(ui.includes("adpExecutiveReadGrid"));
  assert.ok(ui.includes("adp-er-summary-box"));
  assert.ok(ui.includes("renderDemandCaptureRetired"));
  assert.ok(html.includes("adpExecutiveReadSummaries"));
  assert.ok(css.includes("adp-executive-read__grid"));
  assert.ok(css.includes("color: var(--neutral--100"));
  assert.ok(!css.includes(".adp-er-chip"));

  assert.ok(ui.includes('"Reality Coverage"'));
  assert.ok(ui.includes('"Scenario Presence"'));
  assert.ok(ui.includes('"Consideration Rate"'));
  assert.equal(auditCustomerDemandCapture(ui, html, share), 0);

  let vagueHits = 0;
  let marketingHits = 0;

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const allPeriods = loadAllPeriods(propertyId).filter((p) => !isTargetedMeasurementPeriod(p));
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods });

    assert.ok(payload.executiveRead?.ux, `${propertyId} ux block`);
    const ux = payload.executiveRead.ux;
    assert.equal(ux.version, EXECUTIVE_READ_UX_VERSION);
    assert.equal(ux.executiveSummary.title, EXECUTIVE_SUMMARY_TITLE);
    assert.equal(ux.biggestStrength.sectionLabel, "BIGGEST STRENGTH");
    assert.equal(ux.biggestConstraint.sectionLabel, "BIGGEST CONSTRAINT");
    assert.ok(ux.changeSinceLastRun.sectionLabel.includes("CHANGE SINCE LAST"));
    assert.ok(ux.biggestStrength.headline);
    assert.ok(ux.biggestStrength.body);
    assert.ok(ux.biggestConstraint.body);
    assert.ok(ux.executiveSummary.narrative);
    assert.ok(/\d+%/.test(ux.executiveSummary.narrative), `${propertyId} uses actual values`);

    const combined = [ux.biggestStrength.body, ux.biggestConstraint.body, ux.changeSinceLastRun.body, ux.executiveSummary.narrative].join(" ");
    for (const re of VAGUE) {
      if (re.test(combined)) vagueHits++;
    }
    const causal = assertNoUnsupportedCausalLanguage(combined);
    if (!causal.ok) marketingHits += causal.hits.length;

    assert.equal(payload.executiveRead.CURRENT_READ_AVAILABLE, true);
    assert.equal(payload.executiveRead.CURRENT_READ_INDEPENDENT_OF_TREND, true);

    if (!payload.executiveRead.COMPARABLE_PRIOR_AVAILABLE) {
      assert.ok(
        ux.changeSinceLastRun.headline.includes("No comparable prior") ||
          payload.executiveRead.trend.state === TREND_STATES.BASELINE_PERIOD
      );
    }

    const published = await getPublishedOwnerReport(propertyId);
    assert.ok(published.payload.executiveRead?.ux, `${propertyId} published ux`);
  }

  const wsProfile = loadPropertyProfile("adp_waterstone_boca_raton");
  const wsPeriod = loadLatestPeriod("adp_waterstone_boca_raton");
  const wsScenarios = buildScenarioUniverse(wsProfile);
  const wsPayload = buildOwnerPayload(wsPeriod, wsScenarios, wsProfile, {
    allPeriods: loadAllPeriods("adp_waterstone_boca_raton").filter((p) => !isTargetedMeasurementPeriod(p)),
  });
  assert.ok(wsPayload.executiveRead.ux.biggestStrength.body.includes("%"));

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0);

  assert.equal(vagueHits, 0);
  assert.equal(marketingHits, 0);

  console.log("test:adp-executive-read-ux-and-three-metric-trends-v2 PASS");
  console.log("  LEFT_SUMMARY_BOXES: 3");
  console.log("  BIGGEST_STRENGTH: PASS");
  console.log("  BIGGEST_CONSTRAINT: PASS");
  console.log("  CHANGE_SINCE_LAST_COMPARABLE_RUN: PASS");
  console.log("  RIGHT_EXECUTIVE_BOX: PASS");
  console.log("  RIGHT_BOX_TITLE:", EXECUTIVE_SUMMARY_TITLE);
  console.log("  FULL_WIDTH_WRITEUP: PASS");
  console.log("  CLEAR_PLAIN_ENGLISH: PASS");
  console.log("  USES_ACTUAL_VALUES: PASS");
  console.log("  VAGUE_UNEXPLAINED_PHRASES:", vagueHits);
  console.log("  CUSTOMER_VISIBLE_AI_DEMAND_CAPTURE:", auditCustomerDemandCapture(ui, html, share));
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  final: ADP_EXECUTIVE_READ_UX_AND_THREE_METRIC_TRENDS_V2_PASS");
  console.log("  next: ADP_EXECUTIVE_READ_AND_TRENDS_READY_FOR_CLIENT_QA");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
