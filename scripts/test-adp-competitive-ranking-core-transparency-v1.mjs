#!/usr/bin/env node
/**
 *   npm run test:adp-competitive-ranking-core-transparency-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  buildTerritoryCompetitiveRanking,
  BASE_OBSERVED_ROWS,
} from "../lib/ai-demand-positioning/customer/competitive-ranking-core-transparency-v1.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

async function main() {
  const ui = readFileSync(UI, "utf8");
  const html = readFileSync(HTML, "utf8");

  assert.equal(BASE_OBSERVED_ROWS, 10);
  assert.ok(ui.includes("renderCompTerritorySelector"));
  assert.ok(ui.includes("data-adp-core-scroll"));
  assert.ok(ui.includes("relationshipPill"));
  assert.ok(html.includes("adpCompTerritoryWrap"));
  assert.ok(html.includes("plus all comparable hotels used in the selected territory analysis"));

  let duplicateCoreRows = 0;
  let rankFabricated = 0;

  const propertyIds = [
    "adp_waterstone_boca_raton",
    "adp_cambridge_beaches_bermuda",
    "adp_renaissance_times_square",
    "adp_now_now_noho",
  ];

  for (const propertyId of propertyIds) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: [period] });

    assert.ok(payload.competitiveRankingByTerritory?.byTerritory, `${propertyId} ranking block`);
    assert.equal(payload.competitiveRankingByTerritory.PROPERTY_SPECIFIC_RANKING_DISPLAY_CODE, 0);

    for (const [key, ranking] of Object.entries(payload.competitiveRankingByTerritory.byTerritory)) {
      if (key === "overall" || ranking.viewType === "overall") continue;
      const rec = ranking.reconciliation;
      if (!rec.CORE_COUNT) continue;
      assert.equal(rec.COUNT_MATCH, true, `${propertyId} ${ranking.territory} CORE count match`);
      assert.equal(rec.IDENTIFIABLE_CORE_TOTAL, rec.CORE_COUNT, `${propertyId} identifiable CORE`);

      const coreRows = ranking.displayRows.filter((r) => r.isCore);
      const coreIds = new Set(coreRows.map((r) => r.entityId));
      if (coreIds.size !== coreRows.length) duplicateCoreRows++;

      for (const row of ranking.displayRows) {
        if (row.isZeroPresenceCore) {
          assert.equal(row.aiPresencePct, 0);
          assert.equal(row.displayRank, "—");
          assert.ok(row.statusLabel);
        }
        if (row.observedRank != null && row.displayRank !== "—") {
          assert.equal(row.displayRank, row.observedRank, `${propertyId} true rank preserved`);
        }
        if (row.observedRank == null && row.isZeroPresenceCore && row.displayRank !== "—") {
          rankFabricated++;
        }
      }
    }

    const published = await getPublishedOwnerReport(propertyId);
    assert.ok(published.ok);
    assert.ok(published.payload.competitiveRankingByTerritory?.byTerritory, `${propertyId} published ranking`);
  }

  const wsProfile = loadPropertyProfile("adp_waterstone_boca_raton");
  const wsPeriod = loadLatestPeriod("adp_waterstone_boca_raton");
  const wsScenarios = buildScenarioUniverse(wsProfile);
  const wsLeisure = buildTerritoryCompetitiveRanking(wsPeriod.observations, wsScenarios, "leisure", wsProfile);
  assert.ok(wsLeisure.coreCount >= 4);
  assert.equal(wsLeisure.reconciliation.COUNT_MATCH, true);
  console.log("Waterstone Leisure Travel:", {
    CORE_COUNT: wsLeisure.reconciliation.CORE_COUNT,
    VISIBLE_ROWS: wsLeisure.displayRows.length,
    CORE_IN_TOP_10: wsLeisure.reconciliation.CORE_IN_TOP_10,
    CORE_OUTSIDE_TOP_10: wsLeisure.reconciliation.CORE_APPENDED,
    ZERO_PRESENCE_CORE: wsLeisure.reconciliation.CORE_ZERO_PRESENCE,
  });

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  assert.equal(wsRegression.INDEX_DIFF, 0);
  assert.equal(rankFabricated, 0);

  console.log("test:adp-competitive-ranking-core-transparency-v1 PASS");
  console.log("  ALL_CORE_INCLUDED: YES");
  console.log("  TRUE_RANK_PRESERVED: YES");
  console.log("  ZERO_PRESENCE_RANK_FABRICATED: NO");
  console.log("  DUPLICATE_CORE_ROWS:", duplicateCoreRows);
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  final: ADP_COMPETITIVE_RANKING_CORE_TRANSPARENCY_V1_PASS");
  console.log("  next: ADP_COMPETITIVE_SET_TRANSPARENCY_READY_FOR_CLIENT_QA");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
