#!/usr/bin/env node
/**
 * ADP Phase 1 Customer UI Promotion tests.
 *   npm run test:adp-phase1-customer-ui-promotion
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { computeCompetitorPresentGaps } from "../lib/ai-demand-positioning/metrics/competitor-present-gaps.js";
import { classifyObservedEntityQuality } from "../lib/ai-demand-positioning/metrics/entity-quality.js";
import { formatPpDelta } from "../lib/ai-demand-positioning/metrics/longitudinal-comparability.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { MIN_RANK_SAMPLE } from "../lib/ai-demand-positioning/metrics/position-metrics.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";
const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML_PATH = join(process.cwd(), "public/owner-ai-demand.html");

const LEGACY_TERMS = [
  "AI Demand Capture",
  "Lost Demand",
  "Demand Lost",
  "Competitive Win Rate",
  "Source Influence",
  "Demand White Space",
  "Top Recommendation Rate",
];

const ALLOWED_DISPLACEMENT = ["data-adp-displacement", "openAdpDisplacementEvidence", "type=displacement"];

function countLegacyTerms(text) {
  const counts = {};
  for (const term of LEGACY_TERMS) {
    counts[term] = (text.match(new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g")) || []).length;
  }
  return counts;
}

function assertNear(actual, expected, label, tolerance = 0.2) {
  assert.ok(
    actual != null && Math.abs(actual - expected) <= tolerance,
    `${label}: expected ~${expected}, got ${actual}`
  );
}

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  assert.ok(profile, "profile");
  assert.ok(periods.length >= 2, "need periods for longitudinal");

  const period = periods[periods.length - 1];
  const scenarios = buildScenarioUniverse(profile);
  const observations = enrichObservationsWithRank(period.observations || [], profile);
  const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });

  assert.ok(payload.ok, payload.message);
  assert.ok(payload.executiveMetrics, "executiveMetrics wired");
  const em = payload.executiveMetrics;
  const h = em.hero;

  assertNear(h.aiConsiderationRate, 53.6, "AI Consideration Rate");
  assertNear(h.aiScenarioPresence, 75.4, "AI Scenario Presence");
  assert.ok(h.competitorPresentScenarios > 0, "competitor present scenarios");
  assert.ok(Math.abs(h.competitorPresentObservations - 113) <= 3, "observation gaps ~113");
  assert.ok(h.propertyRealityCoverage != null, "property reality coverage");
  assertNear(h.numberOneAppearanceRate, 16.4, "#1 appearance");
  assertNear(h.top3AppearanceRate, 78.2, "top-3 appearance");
  assert.strictEqual(h.rankEligibleN, 55, "rank eligible N");
  assert.equal(h.rankDenominatorVisible, true);
  assert.equal(h.thinSampleSuppression, "PASS");

  const gaps = computeCompetitorPresentGaps(observations, scenarios, profile);
  assert.strictEqual(gaps.competitorPresentObservations, h.competitorPresentObservations);

  assert.strictEqual(em.blockedMetrics.aiConsiderationIndex, "BLOCKED");
  assert.strictEqual(em.blockedMetrics.competitiveWinRate, "BLOCKED");
  assert.strictEqual(em.blockedMetrics.compositeOpportunityScore, "BLOCKED");
  assert.strictEqual(em.aiOpportunityScenarios.compositeOpportunityScore, "BLOCKED");
  assert.strictEqual(em.sourceLandscape.sourceInfluenceLanguage, 0);

  assert.ok(em.demandPositionMap.rows.length >= 8, "territory rows");
  assert.ok(em.executiveFindings.length >= 3 && em.executiveFindings.length <= 5);

  assert.strictEqual(formatPpDelta(2.5), "+2.5 pp");
  assert.strictEqual(formatPpDelta(-1.2), "-1.2 pp");
  assert.strictEqual(formatPpDelta(null), "—");

  assert.equal(em.longitudinal.customerTrendReady, false);
  assert.ok(em.longitudinal.totalPeriodFiles >= 11);

  const entityQ = classifyObservedEntityQuality(
    payload.competitiveSet.observed,
    profile
  );
  assert.ok(entityQ.filteredArtifactCount >= 0);
  assert.ok(entityQ.canonicalEntityCount <= entityQ.rawEntityCount);

  const uiJs = readFileSync(UI_JS, "utf-8");
  const html = readFileSync(HTML_PATH, "utf-8");
  const combined = uiJs + html;

  const legacy = countLegacyTerms(combined);
  assert.strictEqual(legacy["AI Demand Capture"], 0, "AI Demand Capture in customer UI");
  assert.strictEqual(legacy["Lost Demand"], 0);
  assert.strictEqual(legacy["Competitive Win Rate"], 0);
  assert.strictEqual(legacy["Source Influence"], 0);

  assert.ok(combined.includes("AI Consideration Rate"));
  assert.ok(combined.includes("AI Scenario Presence"));
  assert.ok(combined.includes("Competitor-Present"));
  assert.ok(combined.includes("Property Reality Coverage"));
  assert.ok(combined.includes("AI Opportunity Scenarios"));
  assert.ok(combined.includes("AI Demand Position by Territory"));

  assert.ok(!payload.aiConsiderationIndex, "no customer ACI");
  assert.ok(em.promptMoat.rawPromptCustomerLeaks === 0 || em.promptMoat.rawPromptCustomerLeaks == null);

  const hardcodedWaterstone = uiJs.match(/53\.6|133\s*\/\s*248|115.*hardcode/gi);
  assert.ok(!hardcodedWaterstone, "no hardcoded Waterstone metrics in UI JS");

  for (const row of em.demandPositionMap.rows) {
    if (row.top3Rate == null) {
      assert.ok(row.numberOneRate == null || row.fieldsWithheld?.includes("numberOneRate"));
    }
  }
  assert.ok(h.rankEligibleN >= MIN_RANK_SAMPLE);

  console.log("test:adp-phase1-customer-ui-promotion — PASS");
  console.log("  AI Consideration Rate:", h.aiConsiderationRate);
  console.log("  AI Scenario Presence:", h.aiScenarioPresence);
  console.log("  Competitor-Present Scenarios:", h.competitorPresentScenarios);
  console.log("  CURRENT_VS_PRIOR_READY:", em.longitudinal.currentVsPriorReady);
  console.log("  REAL_COMPARABLE_PERIODS:", em.longitudinal.realComparablePeriods);
}

main();
