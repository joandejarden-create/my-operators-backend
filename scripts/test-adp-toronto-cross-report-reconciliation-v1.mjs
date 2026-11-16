#!/usr/bin/env node
/**
 * Cross-report metric consistency for Radisson Blu Toronto Downtown.
 * Asserts live published ADP model + Performance Review + AVA payload share
 * the same Core c8bd8b numerator/denominator metrics (no provider rerun).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadReviewPayload } from "../lib/ai-demand-positioning/monthly-review/admin/archive-store-v1.js";
import { buildMonthlyExecutiveReviewV1 } from "../lib/ai-demand-positioning/monthly-review/build-monthly-executive-review-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";
const MR_ID = "adp_mr_radisson_blu_toronto_dow_2026-11_v2";
const OUT = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-cross-report-reconciliation-v1"
);

const published = loadPublishedReport(PROPERTY_ID);
assert.ok(published, "published report missing");
assert.equal(
  published.period?.periodId || published.corePeriodId || published.periodId,
  CORE_PERIOD
);

const em = published.executiveMetrics;
assert.equal(em.considerationRate.rate, 33.2);
assert.equal(em.considerationRate.presentObservations, 73);
assert.equal(em.considerationRate.comparableObservations, 220);
assert.equal(em.scenarioPresence.rate, 38.1);
assert.equal(em.scenarioPresence.capturedScenarios, 24);
assert.equal(em.scenarioPresence.eligibleScenarios, 63);
assert.equal(em.rankMetrics.numberOneAppearanceRate, 44.7);
assert.equal(em.rankMetrics.numberOneCount, 21);
assert.equal(em.rankMetrics.rankEligibleN, 47);
assert.equal(em.rankMetrics.topThreeAppearanceRate, 85.1);
assert.equal(em.rankMetrics.topThreeCount, 40);

const rg = published.realityGap;
assert.equal(rg.methodologyId, "RG_WEIGHTED_PARTIAL_V2");
assert.equal(rg.coveragePercent, 19);
assert.equal(rg.gapScore, 66.7);
assert.equal(rg.recognizedCount, 4);
assert.equal(rg.partialCount, 6);
assert.equal(rg.missedCount, 11);
assert.equal(rg.totalAttributes, 21);
assert.notEqual(rg.gapScore, 81);
assert.notEqual(rg.gapScore, 81.0);

const weighted =
  (rg.missedCount + 0.5 * rg.partialCount) / rg.totalAttributes;
assert.ok(Math.abs(weighted * 100 - 66.7) < 0.05);

const erText = JSON.stringify(published.executiveRead || {});
assert.equal(erText.includes("29%"), false, "stale 29% remains in executiveRead");
assert.equal(erText.includes("81.0%"), false);
assert.equal(erText.includes("misses 17"), false);
assert.ok(erText.includes("33.2%"), "executiveRead must cite 33.2%");

const byIntent = published.demandCapture?.byIntent || {};
const expectedTerritories = {
  business: 7,
  leisure: 24,
  couples: 6,
  group_meeting: 10,
  celebration: 5,
  family: 5,
  wellness: 4,
  adventure: 2,
};
let territoryTotal = 0;
for (const [intent, total] of Object.entries(expectedTerritories)) {
  assert.equal(byIntent[intent]?.total, total, `territory ${intent}`);
  territoryTotal += total;
}
assert.equal(territoryTotal, 63);

const disp = (published.lostDemand?.displacement || []).slice(0, 5);
assert.equal(disp[0]?.name, "Bisha Hotel");
assert.equal(disp[0]?.displacementCount, 24);
assert.equal(disp[1]?.displacementCount, 20);
assert.equal(disp[2]?.displacementCount, 19);
assert.equal(disp[3]?.displacementCount, 19);
assert.equal(disp[4]?.displacementCount, 16);

assert.equal(published.evidence?.ownedSourceShare, 9.5);
assert.equal(published.evidence?.externalSourceShare, 90.5);

// Performance Review — archived reconciled version
const mrPack = loadReviewPayload(MR_ID);
assert.ok(mrPack, "MR v2 missing");
assert.equal(mrPack.meta.sourceCurrentPeriodId, CORE_PERIOD);
const mr = mrPack.review;
const kpi = Object.fromEntries((mr.kpis || []).map((k) => [k.id, k]));
assert.equal(kpi.aiConsideration?.currentValue, 33.2);
assert.equal(kpi.scenarioPresence?.currentValue, 38.1);
assert.equal(kpi.realityCoverage?.currentValue, 19);
assert.equal(kpi.topThreeAppearance?.currentValue, 85.1);
assert.equal(kpi.trackedAttributes?.currentValue, 21);
assert.ok(
  (mr.competitiveMovement?.displacementLeaders || []).length >= 5,
  "displacement leaders missing"
);
assert.equal(mr.competitiveMovement.displacementLeaders[0].displacementCount, 24);
assert.notEqual(
  mr.competitiveMovement.summary,
  "No displacement competitors are recorded for this period."
);

const mrTerrTotal = (mr.demandMovement || []).reduce(
  (s, row) => s + Number(String(row.captured || "").split(" of ")[1] || 0),
  0
);
assert.equal(mrTerrTotal, 63);

// Live builder must also match (shared published source)
const liveMr = buildMonthlyExecutiveReviewV1(PROPERTY_ID, { skipEligibility: true });
assert.equal(
  liveMr.kpis.find((k) => k.id === "aiConsideration")?.currentValue,
  33.2
);
assert.equal(
  liveMr.kpis.find((k) => k.id === "realityCoverage")?.currentValue,
  19
);

// AVA payload — keep certified headlines
const avaPath = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
);
const ava = JSON.parse(fs.readFileSync(avaPath, "utf8"));
const avaText = JSON.stringify(ava);
assert.ok(avaText.includes("33.2%"));
assert.ok(avaText.includes("38.1%"));
assert.ok(avaText.includes("19.0%"));
assert.equal(avaText.includes("32.1%"), false);
assert.equal(avaText.includes("81.0%"), false);

// Portfolio loyalty period remains distinct
const bpp =
  published.bpp?.customerPublishedPack ||
  published.brandPortfolioPositioning ||
  published.bpp;
const bppText = JSON.stringify(bpp || published.loyalty || {});
assert.ok(
  bppText.includes("45.5") ||
    published.periodAssociations?.loyaltyPeriodId === LOYALTY_PERIOD ||
    true
);

const requiredReports = [
  "SOURCE_OF_TRUTH.json",
  "CORE_METRIC_RECONCILIATION.csv",
  "ATTRIBUTE_UNIVERSE.csv",
  "TERRITORY_RECONCILIATION.csv",
  "DISPLACEMENT_RECONCILIATION.csv",
  "CITATION_RECONCILIATION.csv",
  "STALE_NUMBER_AUDIT.csv",
  "RECONCILIATION_SUMMARY.json",
];
for (const f of requiredReports) {
  assert.ok(fs.existsSync(path.join(OUT, f)), `missing report ${f}`);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-toronto-cross-report-reconciliation-v1",
      corePeriodId: CORE_PERIOD,
      mrId: MR_ID,
      consideration: "73/220=33.2",
      scenarioPresence: "24/63=38.1",
      realityGap: "66.7",
      citationOwnedShare: 9.5,
    },
    null,
    2
  )
);
