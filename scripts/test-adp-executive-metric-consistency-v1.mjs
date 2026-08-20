#!/usr/bin/env node
/**
 * ADP Executive Metric Consistency + Legacy Demand Capture Retirement V1 tests.
 *   npm run test:adp-executive-metric-consistency-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { computePositionMetrics, MIN_RANK_SAMPLE } from "../lib/ai-demand-positioning/metrics/position-metrics.js";
import {
  AI_DEMAND_CAPTURE_CUSTOMER_RENDER,
  EXECUTIVE_METRICS_CARD_COUNT,
  EXECUTIVE_METRICS_ALWAYS_VISIBLE,
} from "../lib/ai-demand-positioning/customer/adp-customer-display-contract-v1.js";

const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");

function extractFunctionBody(source, name) {
  const start = source.indexOf(`function ${name}`);
  assert.ok(start >= 0, `missing ${name}`);
  const brace = source.indexOf("{", start);
  let depth = 0;
  for (let i = brace; i < source.length; i += 1) {
    if (source[i] === "{") depth += 1;
    if (source[i] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, i + 1);
    }
  }
  throw new Error(`unclosed ${name}`);
}

function assertUiContract() {
  const ui = readFileSync(UI_JS, "utf-8");
  const html = readFileSync(HTML, "utf-8");
  const metricsFn = extractFunctionBody(ui, "renderExecutiveMetricEnhancements");
  const snapshotFn = extractFunctionBody(ui, "renderExecKpis");

  assert.strictEqual(EXECUTIVE_METRICS_CARD_COUNT, 5);
  assert.strictEqual(EXECUTIVE_METRICS_ALWAYS_VISIBLE, true);
  assert.strictEqual(AI_DEMAND_CAPTURE_CUSTOMER_RENDER, 0);

  assert.ok(!html.includes('id="adpExecutiveMetricsSection" hidden'), "metrics section always visible in HTML");
  const cardTitles = [
    "AI Consideration Rate",
    "AI Scenario Presence",
    "#1 Appearance Rate",
    "Top-3 Appearance Rate",
    "Competitor-Present Scenarios",
  ];
  for (const title of cardTitles) {
    const occurrences = (metricsFn.match(new RegExp(`"${title.replace(/[#]/g, "\\$&")}"`, "g")) || []).length;
    assert.ok(occurrences >= 2, `${title} must render calculable and unavailable states`);
  }
  assert.ok(!metricsFn.includes("section.hidden = true"), "metrics section never hidden");
  assert.ok(!metricsFn.includes("cards.slice(0, 5)"), "no card slicing");
  assert.ok(!metricsFn.includes("Capped at 200"), "no legacy cap copy");

  assert.ok(!snapshotFn.includes('kpiCard("AI Demand Capture"'), "AI Demand Capture removed from Property Snapshot");
  assert.ok(ui.includes("propertyRealityCoverageSnapshot"), "Property Reality Coverage helper in snapshot");
  assert.ok(ui.includes("Property Reality Coverage"), "Property Reality Coverage label");
  assert.ok(snapshotFn.includes("propertyRealityCoverageSnapshot"), "reality card uses helper first");
  assert.ok(snapshotFn.includes("Scenarios Monitored"), "Scenarios Monitored restored");
  assert.ok(snapshotFn.includes("scenariosMonitoredSnapshot"), "scenarios monitored helper");
  assert.ok(snapshotFn.includes("Traveler Needs Where Hotel Appeared"), "Traveler Needs Appeared label");
  assert.ok(snapshotFn.includes("Traveler Needs Where Hotel Was Missing"), "Traveler Needs Missing label");
  assert.ok(snapshotFn.includes("travelerNeedsAppearanceSnapshot"), "traveler needs snapshot helper");
  assert.ok(ui.includes("adpTravelerNeedsAppearedTip"), "appeared tip");
  assert.ok(ui.includes("adpTravelerNeedsMissingTip"), "missing tip");
  assert.ok(ui.includes("adpScenariosMonitoredTip"), "scenarios monitored tip");
  assert.ok(ui.includes("adpPropertyRealityCoverageTip"), "reality tip");
  assert.ok(ui.includes("adpTopObservedAlternativeTip"), "top alternative tip");
  assert.ok(ui.includes("What this shows:"), "structured tip sections");
  assert.ok(ui.includes("How it\\u2019s calculated:") || ui.includes("How it\u2019s calculated:"), "formula tip section");
  assert.ok(ui.includes("Why track it:"), "why-track tip section");
  assert.ok(ui.includes("Important:"), "important tip section");
  assert.equal((snapshotFn.match(/kpiCardWithInfo\(/g) || []).length, 5, "all 5 snapshot cards use info icons");
  assert.ok(!snapshotFn.includes("Strongest Demand Territory"), "Strongest Demand Territory removed from Property Snapshot");
  assert.ok(!snapshotFn.includes("strongestTerritory"), "strongestTerritory not in snapshot render");
  assert.ok(!snapshotFn.includes("Strongest Segment"), "legacy Strongest Segment removed");
  assert.ok(!snapshotFn.includes('kpiCard("Questions Missing"'), "legacy Questions Missing snapshot label removed");
  assert.ok(snapshotFn.includes("Top Observed AI Alternative"), "Top Observed AI Alternative label");
  assert.ok(!snapshotFn.includes('kpiCard("Top AI Competitor"'), "legacy Top AI Competitor removed");

  // Exact card order in renderExecKpis (reality uses helper label; others are string literals)
  const orderMarkers = [
    "propertyRealityCoverageSnapshot",
    '"Scenarios Monitored"',
    '"Traveler Needs Where Hotel Appeared"',
    '"Traveler Needs Where Hotel Was Missing"',
    '"Top Observed AI Alternative"',
  ];
  let cursor = -1;
  for (const marker of orderMarkers) {
    const idx = snapshotFn.indexOf(marker);
    assert.ok(idx > cursor, `card order: ${marker} after previous`);
    cursor = idx;
  }
  assert.ok(ui.includes("Insufficient data"), "insufficient data copy");
  assert.ok(ui.includes("Insufficient ranked responses"), "insufficient ranked copy");
  assert.ok(ui.includes("Insufficient comparable data"), "insufficient comparable copy");
  assert.ok(ui.includes("Insufficient property data"), "insufficient property copy");
  assert.ok(ui.includes("card shows Insufficient ranked responses rather than 0%"), "rank tooltip updated");
}

function metricStateExpectations(payload) {
  const em = payload.executiveMetrics || {};
  const cr = em.considerationRate;
  const sp = em.scenarioPresence;
  const rm = em.rankMetrics;
  const rankEligibleN = rm && Number.isFinite(Number(rm.rankEligibleN)) ? Number(rm.rankEligibleN) : null;

  return {
    considerationCalculable: !!(cr && Number.isFinite(Number(cr.rate)) && Number(cr.comparableObservations) > 0),
    scenarioCalculable: !!(sp && Number.isFinite(Number(sp.rate)) && Number(sp.eligibleScenarios) > 0),
    rankCalculable: !!(rm && Number(rm.rankEligibleN) >= MIN_RANK_SAMPLE),
    rankEligibleN,
    competitorCalculable:
      !!(cr && Number(cr.comparableObservations) > 0) ||
      !!(sp && Number(sp.eligibleScenarios) > 0) ||
      !!(payload.period && Number(payload.period.scenarioCount) > 0),
    competitorCount: em.competitorPresentScenarios?.scenarioCount ?? 0,
    realityCoverage:
      payload.realityGap?.totalAttributes > 0
        ? (payload.realityGap.recognizedCount / payload.realityGap.totalAttributes) * 100
        : null,
  };
}

async function assertProperty(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const periods = loadAllPeriods(propertyId);
  const period = loadLatestPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  payload._observations = period.observations;

  assert.ok(payload.demandCapture, `${propertyId}: demandCapture preserved`);
  assert.ok(Number.isFinite(payload.demandCapture.overallRate), `${propertyId}: demandCapture rate internal`);

  const states = metricStateExpectations(payload);
  if (propertyId === "adp_now_now_noho") {
    const obs = enrichObservationsWithRank(period.observations.filter((o) => o.parsed), profile);
    const rankEligible = computePositionMetrics(obs, scenarios, profile).rankEligibleObservations;
    assert.ok(rankEligible < MIN_RANK_SAMPLE, "Now Now rank sample below threshold");
  }
  if (propertyId === "adp_waterstone_boca_raton") {
    assert.ok(states.rankCalculable, "Waterstone rank metrics calculable");
    assert.ok(states.considerationCalculable, "Waterstone consideration calculable");
  }
  if (propertyId === "adp_cambridge_beaches_bermuda") {
    assert.ok(states.realityCoverage != null, "Cambridge reality coverage available");
  }

  return { propertyId, states, payload };
}

async function main() {
  assertUiContract();

  const waterstone = await assertProperty("adp_waterstone_boca_raton");
  const cambridge = await assertProperty("adp_cambridge_beaches_bermuda");
  const nowNow = await assertProperty("adp_now_now_noho");

  const cambridgeRead = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridgeRead.ok);
  assert.ok(cambridgeRead.payload.demandCapture, "Cambridge published demandCapture preserved");

  console.log("test:adp-executive-metric-consistency-v1 — PASS");
  console.log("  Waterstone rankEligibleN:", waterstone.states.rankEligibleN);
  console.log("  Cambridge realityCoverage:", cambridge.states.realityCoverage?.toFixed(1));
  console.log("  Now Now rankEligibleN:", nowNow.states.rankEligibleN);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
