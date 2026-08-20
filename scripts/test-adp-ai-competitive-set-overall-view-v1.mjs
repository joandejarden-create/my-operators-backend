#!/usr/bin/env node
/**
 *   npm run test:adp-ai-competitive-set-overall-view-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  buildOverallCompetitiveRanking,
  buildAllTerritoryCompetitiveRankings,
  OVERALL_RANKING_KEY,
  OVERALL_BASE_ROWS,
  OVERALL_TOP_SLICE_WHEN_SUBJECT_APPENDED,
  TERRITORY_INTENT_ORDER,
} from "../lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js";
import { buildTerritoryCompetitiveRanking } from "../lib/ai-demand-positioning/customer/competitive-ranking-core-transparency-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const CSS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css");
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

async function main() {
  const ui = readFileSync(UI, "utf8");
  const html = readFileSync(HTML, "utf8");
  const css = readFileSync(CSS, "utf8");

  assert.ok(ui.includes("isOverallCompView"));
  assert.ok(ui.includes('"overall"'));
  assert.ok(ui.includes("adp-comp-table--overall"));
  assert.ok(html.includes("plus all comparable hotels used in the selected territory analysis"));
  assert.ok(css.includes("adp-comp-table--overall"));

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: [period] });
    const block = payload.competitiveRankingByTerritory;

    assert.ok(block?.byTerritory?.[OVERALL_RANKING_KEY], `${propertyId} overall ranking`);
    assert.equal(block.defaultView, OVERALL_RANKING_KEY, `${propertyId} overall default`);
    assert.equal(block.PROPERTY_SPECIFIC_OVERALL_RANKING_CODE, 0);
    assert.equal(block.selectorOrder[0], OVERALL_RANKING_KEY);

    const overall = block.byTerritory[OVERALL_RANKING_KEY];
    assert.equal(overall.viewType, "overall");
    assert.equal(overall.reconciliation, null);
    assert.equal(overall.hideTerritoryColumn, false);
    assert.equal(overall.territoryColumnMode, "top_demand_territory");
    assert.equal(overall.meta.OVERALL_CORE_BENCHMARK, false);
    assert.equal(overall.meta.OVERALL_AI_PRESENCE_INDEX, false);
    assert.ok(overall.displayRows.length >= 1);
    assert.ok(overall.meta.SUBJECT_ALWAYS_VISIBLE, `${propertyId} subject visible`);

    const subject = overall.displayRows.find((r) => r.isSubject);
    assert.ok(subject, `${propertyId} subject row`);
    assert.ok(subject.topDemandTerritory, `${propertyId} subject top territory`);

    for (const row of overall.displayRows) {
      assert.equal(row.isCore, false, `${propertyId} no CORE in overall`);
      assert.ok(row.topDemandTerritory, `${propertyId} topDemandTerritory on ${row.name}`);
      if (row.observedRank != null && row.displayRank !== "—") {
        assert.equal(row.displayRank, row.observedRank, `${propertyId} true rank`);
      }
    }

    if (subject.observedRank != null && subject.observedRank > OVERALL_BASE_ROWS) {
      assert.equal(overall.displayRows.length, OVERALL_TOP_SLICE_WHEN_SUBJECT_APPENDED + 1);
      assert.ok(subject.isAppendedSubject);
    }

    // Territory regression — leisure or first available intent
    const territoryKey = TERRITORY_INTENT_ORDER.find((k) => block.byTerritory[k] && k !== OVERALL_RANKING_KEY);
    if (territoryKey) {
      const tr = block.byTerritory[territoryKey];
      if (tr.reconciliation?.CORE_COUNT) {
        assert.equal(tr.reconciliation.COUNT_MATCH, true, `${propertyId} ${territoryKey} CORE match`);
      }
    }

    const published = await getPublishedOwnerReport(propertyId);
    assert.ok(published.payload.competitiveRankingByTerritory?.byTerritory?.overall);
  }

  const wsProfile = loadPropertyProfile("adp_waterstone_boca_raton");
  const wsPeriod = loadLatestPeriod("adp_waterstone_boca_raton");
  const wsScenarios = buildScenarioUniverse(wsProfile);
  const wsOverall = buildOverallCompetitiveRanking(wsPeriod.observations, wsScenarios, wsProfile);
  const wsLeisure = buildTerritoryCompetitiveRanking(wsPeriod.observations, wsScenarios, "leisure", wsProfile);

  assert.ok(wsOverall.comparableN > 0);
  assert.ok(wsLeisure.reconciliation.COUNT_MATCH);
  assert.ok(wsOverall.displayRows.length > wsLeisure.baseObservedRows || wsOverall.displayRows.length <= 11);

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0);

  const rankings = buildAllTerritoryCompetitiveRankings(wsPeriod.observations, wsScenarios, wsProfile);
  assert.ok(rankings.byTerritory.overall);
  assert.ok(rankings.byTerritory.leisure);

  console.log("test:adp-ai-competitive-set-overall-view-v1 PASS");
  console.log("  OVERALL_DEFAULT: YES");
  console.log("  SUBJECT_ALWAYS_VISIBLE: YES");
  console.log("  OVERALL_CORE_BENCHMARK_CREATED: NO");
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  final: ADP_AI_COMPETITIVE_SET_OVERALL_VIEW_V1_PASS");
  console.log("  next: ADP_AI_COMPETITIVE_SET_OVERALL_READY_FOR_CLIENT_QA");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
