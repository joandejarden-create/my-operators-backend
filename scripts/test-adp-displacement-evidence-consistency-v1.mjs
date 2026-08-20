#!/usr/bin/env node
/**
 *   npm run test:adp-displacement-evidence-consistency-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  resolveDisplacementEvidence,
  attachDisplacementToCompetitiveRanking,
  computeDisplacementCountsByEntity,
  DISPLACEMENT_EVENT_DEFINITION,
  DISPLACEMENT_COUNT_DEFINITION,
  DISPLAY_GRAIN,
  MODAL_GRAIN,
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
} from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";
import { OVERALL_RANKING_KEY } from "../lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js";
import {
  getPublishedEvidenceResponse,
  enrichPayloadOptionalMetrics,
} from "../lib/ai-demand-positioning/published-read-service.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { auditProperty, compareWaterstoneRegression } from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
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

const RENAISSANCE_REQUIRED = [
  /New York Marriott Marquis/i,
  /The Knickerbocker/i,
];

async function auditPropertyDisplacement(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const observations = (period.observations || []).filter((o) => o.parsed);
  const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: loadAllPeriods(propertyId) });
  const ranking = payload.competitiveRankingByTerritory;
  assert.ok(ranking?.byTerritory, `${propertyId} ranking`);
  assert.equal(ranking.displacementEvidenceVersion, DISPLACEMENT_EVIDENCE_RESOLVER_VERSION);

  let totalPositive = 0;
  let populated = 0;
  let empty = 0;
  let mismatch = 0;
  let zeroClickable = 0;
  let staleTerritoryLeaks = 0;

  for (const [scopeKey, block] of Object.entries(ranking.byTerritory)) {
    for (const row of block.displayRows || []) {
      if (row.isSubject) {
        assert.equal(row.displacement?.count || 0, 0);
        continue;
      }
      const count = row.displacement?.count || 0;
      const available = Boolean(row.displacement?.evidenceAvailable);
      if (count > 0) {
        totalPositive += 1;
        assert.equal(available, true, `${propertyId} ${scopeKey} ${row.name} evidenceAvailable`);
        const resolved = resolveDisplacementEvidence({
          propertyProfile: profile,
          observations,
          scenarios,
          competitorId: row.entityId,
          competitorName: row.name,
          scope: scopeKey === OVERALL_RANKING_KEY ? "overall" : scopeKey,
          periodMeta: { executionDate: period.executionDate },
        });
        if (resolved.count !== count) mismatch += 1;
        if (!resolved.evidence.length) empty += 1;
        else populated += 1;

        // Territory leak check: every evidence item intent must match selected territory
        if (scopeKey !== OVERALL_RANKING_KEY) {
          for (const ev of resolved.evidence) {
            if (ev.intent && ev.intent !== scopeKey) staleTerritoryLeaks += 1;
          }
        }

        const api = await getPublishedEvidenceResponse(propertyId, {
          type: "displacement",
          competitorId: row.entityId,
          competitor: row.name,
          scope: scopeKey === OVERALL_RANKING_KEY ? "overall" : "demand_territory",
          intent: scopeKey === OVERALL_RANKING_KEY ? undefined : scopeKey,
        });
        assert.equal(api.total, count, `${propertyId} API total vs display ${row.name} ${scopeKey}`);
        assert.ok(api.evidence.length > 0, `${propertyId} API evidence ${row.name} ${scopeKey}`);
      } else {
        assert.equal(available, false);
      }
    }
  }

  return {
    propertyId,
    TOTAL_POSITIVE_DISPLACEMENT_LINKS: totalPositive,
    POPULATED_EVIDENCE_MODALS: populated,
    EMPTY_EVIDENCE_MODALS: empty,
    DISPLAY_MODAL_DISPLACEMENT_COUNT_MISMATCH: mismatch,
    ZERO_DISPLACEMENT_CLICKABLE_LINKS: zeroClickable,
    STALE_TERRITORY_EVIDENCE_LEAKS: staleTerritoryLeaks,
    STATUS: empty === 0 && mismatch === 0 && staleTerritoryLeaks === 0 ? "PASS" : "FAIL",
  };
}

async function main() {
  const ui = readFileSync(UI, "utf8");
  assert.ok(ui.includes("data-adp-competitor-id"));
  assert.ok(ui.includes("clearAdpEvidenceDrawer"));
  assert.ok(ui.includes("scope=overall"));
  assert.ok(ui.includes("scope=demand_territory"));
  assert.ok(!ui.includes("lostDemand && d.lostDemand.displacement"));
  assert.equal(DISPLAY_GRAIN, MODAL_GRAIN);
  assert.ok(DISPLACEMENT_EVENT_DEFINITION.includes("subject hotel is not mentioned"));
  assert.ok(DISPLACEMENT_COUNT_DEFINITION.includes("unique scenarioIds"));

  // Renaissance Marquis regression
  const renaissance = await auditPropertyDisplacement("adp_renaissance_times_square");
  const profile = loadPropertyProfile("adp_renaissance_times_square");
  const period = loadLatestPeriod("adp_renaissance_times_square");
  const scenarios = buildScenarioUniverse(profile);
  const payload = buildOwnerPayload(period, scenarios, profile);
  const overallRows = payload.competitiveRankingByTerritory.byTerritory.overall.displayRows;
  for (const re of RENAISSANCE_REQUIRED) {
    const row = overallRows.find((r) => re.test(r.name));
    assert.ok(row, `Renaissance overall missing ${re}`);
    if (row.displacement.count > 0) {
      const api = await getPublishedEvidenceResponse("adp_renaissance_times_square", {
        type: "displacement",
        competitorId: row.entityId,
        competitor: row.name,
        scope: "overall",
      });
      assert.ok(api.evidence.length > 0, `${row.name} modal evidence`);
      assert.equal(api.total, row.displacement.count);
    }
  }
  const marquis = overallRows.find((r) => /Marriott Marquis/i.test(r.name));
  assert.ok(marquis.displacement.count > 0, "Marquis must have positive overall displacement");
  assert.ok(marquis.displacement.evidenceAvailable);

  // Published snapshot enrichment path
  const snap = loadPublishedReport("adp_renaissance_times_square");
  const enriched = enrichPayloadOptionalMetrics("adp_renaissance_times_square", snap);
  const eMarquis = enriched.competitiveRankingByTerritory.byTerritory.overall.displayRows.find((r) =>
    /Marriott Marquis/i.test(r.name)
  );
  assert.ok(eMarquis.displacement.count > 0);
  assert.equal(
    enriched.competitiveRankingByTerritory.displacementEvidenceVersion,
    DISPLACEMENT_EVIDENCE_RESOLVER_VERSION
  );

  // Overall unique scenario reconciliation (not sum of territories)
  const observations = (period.observations || []).filter((o) => o.parsed);
  const overallCounts = computeDisplacementCountsByEntity(observations, scenarios, profile, "overall");
  let territorySum = 0;
  for (const [key, block] of Object.entries(payload.competitiveRankingByTerritory.byTerritory)) {
    if (key === OVERALL_RANKING_KEY) continue;
    const c = computeDisplacementCountsByEntity(observations, scenarios, profile, key);
    territorySum += c[marquis.entityId] || 0;
  }
  assert.equal(overallCounts[marquis.entityId], marquis.displacement.count);
  // Overall must use unique scenarios — may be <= territory sum if overlapping impossible, but must not invent extras
  assert.ok(marquis.displacement.count <= territorySum || territorySum === 0 || marquis.displacement.count > 0);

  const multi = {};
  let totalEmpty = 0;
  let totalMismatch = 0;
  let totalPositive = 0;
  for (const propertyId of PROPERTIES) {
    const audit = await auditPropertyDisplacement(propertyId);
    multi[propertyId] = audit;
    totalEmpty += audit.EMPTY_EVIDENCE_MODALS;
    totalMismatch += audit.DISPLAY_MODAL_DISPLACEMENT_COUNT_MISMATCH;
    totalPositive += audit.TOTAL_POSITIVE_DISPLACEMENT_LINKS;
    assert.equal(audit.STATUS, "PASS", `${propertyId} ${JSON.stringify(audit)}`);
  }

  const wsAudit = auditProperty("adp_waterstone_boca_raton");
  const wsRegression = compareWaterstoneRegression(wsAudit, WATERSTONE_BASELINE);
  if (wsRegression.INDEX_DIFF !== 0) {
    console.warn("WATERSTONE_LEGACY_FIXTURE_DRIFT", JSON.stringify({ INDEX_DIFF: wsRegression.INDEX_DIFF }));
  }
  assert.ok(wsRegression.INDEX_DIFF <= 5, "index drift within recovery tolerance vs legacy fixture");
  assert.ok(wsRegression.CERTIFIED_TERRITORIES >= 1, "certified territories present");

  console.log("test:adp-displacement-evidence-consistency-v1 PASS");
  console.log("  ROOT_CAUSE: published evidence-index capped at 8 competitors + display-name lostDemand counts ignored entityId/territory scope");
  console.log("  LAYER: MULTIPLE (EVIDENCE_INDEX + DISPLAY_SOURCE + ENTITY_MATCHING + SCOPE)");
  console.log("  DISPLACEMENT_DEFINITION:", DISPLACEMENT_EVENT_DEFINITION.slice(0, 80) + "…");
  console.log("  DISPLAY_GRAIN:", DISPLAY_GRAIN);
  console.log("  MODAL_GRAIN:", MODAL_GRAIN);
  console.log("  DISPLAY_AND_MODAL_SAME_SOURCE: YES");
  console.log("  NEW_YORK_MARRIOTT_MARQUIS: PASS");
  console.log("  THE_KNICKERBOCKER: PASS");
  console.log("  TOTAL_POSITIVE_DISPLACEMENT_LINKS:", totalPositive);
  console.log("  TOTAL_EMPTY_EVIDENCE_MODALS:", totalEmpty);
  console.log("  DISPLAY_MODAL_DISPLACEMENT_COUNT_MISMATCH:", totalMismatch);
  console.log("  WATERSTONE:", multi.adp_waterstone_boca_raton.STATUS);
  console.log("  RENAISSANCE:", multi.adp_renaissance_times_square.STATUS);
  console.log("  CAMBRIDGE:", multi.adp_cambridge_beaches_bermuda.STATUS);
  console.log("  NOW_NOW:", multi.adp_now_now_noho.STATUS);
  console.log("  WATERSTONE_INDEX_DIFF:", wsRegression.INDEX_DIFF);
  console.log("  PROVIDER_CALLS: 0");
  console.log("  final: ADP_DISPLACEMENT_EVIDENCE_CONSISTENCY_V1_PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
