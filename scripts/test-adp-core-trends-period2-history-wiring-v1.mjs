#!/usr/bin/env node
/**
 * Core ADP Trends Period-2 history wiring gates.
 * npm run test:adp-core-trends-period2-history-wiring-v1
 */

import assert from "assert";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { reconcileCorePriorRun } from "../lib/ai-demand-positioning/monitoring/core-prior-run-reconciliation-v1.js";
import {
  CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER,
  TREND_CURRENT_PRIOR_RECONCILIATION,
  TREND_METRIC_VALUES_MATCH_CERTIFIED_PERIOD_METRICS,
  TREND_NO_STALE_SINGLE_PERIOD_AFTER_CERTIFIED_PUBLICATION,
  TREND_PERIOD_COUNT_MATCHES_COMPARABLE_HISTORY,
  buildCertifiedTrendMetricPoint,
  buildTrendsFromCanonicalResolution,
  isStaleSinglePeriodTrends,
  resolveCanonicalComparablePeriods,
} from "../lib/ai-demand-positioning/metrics/canonical-comparable-period-resolver-v1.js";
import { enrichPayloadOptionalMetrics } from "../lib/ai-demand-positioning/published-read-service.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning/core-trends-period2-wiring");
const results = {
  stamp: new Date().toISOString(),
  gates: {},
  properties: [],
};

function near(a, b, eps = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= eps;
}

async function main() {
  const readSrc = readFileSync(
    join(process.cwd(), "lib/ai-demand-positioning/published-read-service.js"),
    "utf8"
  );
  const snapSrc = readFileSync(
    join(process.cwd(), "lib/ai-demand-positioning/published-snapshot.js"),
    "utf8"
  );
  const resolverSrc = readFileSync(
    join(process.cwd(), "lib/ai-demand-positioning/metrics/canonical-comparable-period-resolver-v1.js"),
    "utf8"
  );

  const usesCanonical =
    readSrc.includes("resolveCanonicalComparablePeriods") &&
    readSrc.includes("buildTrendsFromCanonicalResolution") &&
    snapSrc.includes("resolveCanonicalComparablePeriods") &&
    resolverSrc.includes(CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER);
  results.gates[CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER] = usesCanonical
    ? "PASS"
    : "FAIL";
  assert.ok(usesCanonical, CORE_TRENDS_USES_CANONICAL_COMPARABLE_PERIOD_RESOLVER);

  let periodCountPass = true;
  let metricPass = true;
  let reconcilePass = true;
  let stalePass = true;

  for (const propertyId of ADP_CERTIFIED_PROPERTY_IDS) {
    const profile = loadPropertyProfile(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const allPeriods = loadAllPeriods(propertyId);
    const published = await getPublishedOwnerReport(propertyId);
    assert.ok(published?.ok, `${propertyId} published ok`);
    const payload = published.payload;
    const currentPeriodId = payload.period?.periodId;
    const resolution = resolveCanonicalComparablePeriods({
      allPeriods,
      scenarios,
      currentPeriodId,
    });
    const trends = payload.trends || [];
    const prior = payload.executiveMetrics?.currentVsPrior;
    const recon = reconcileCorePriorRun(propertyId, currentPeriodId);

    const row = {
      propertyId,
      currentPeriodId,
      comparablePeriodCount: resolution.periodCount,
      trendPeriodCount: trends.length,
      trendDates: trends.map((t) => String(t.date || "").slice(0, 10)),
      priorPeriodId: prior?.priorComparablePeriodId || null,
      expectedPriorPeriodId: recon.expectedPriorPeriodId,
      deltas: prior?.deltas || null,
    };

    if (resolution.periodCount !== 2 || trends.length !== 2) {
      periodCountPass = false;
      row.periodCountDefect = true;
    }
    if (
      !prior?.priorComparablePeriodId ||
      prior.priorComparablePeriodId !== resolution.priorPeriodId ||
      prior.priorComparablePeriodId !== trends[0]?.periodId ||
      trends[1]?.periodId !== resolution.currentPeriodId
    ) {
      reconcilePass = false;
      row.reconcileDefect = true;
    }

    const expectedPoints = buildTrendsFromCanonicalResolution({
      resolution,
      scenarios,
      propertyProfile: profile,
      measurementContractVersion: payload.measurementContractVersion,
    });
    for (let i = 0; i < trends.length; i++) {
      const t = trends[i];
      const e = expectedPoints?.[i];
      const cert = buildCertifiedTrendMetricPoint({
        period: resolution.comparablePeriods[i],
        scenarios,
        propertyProfile: profile,
        measurementContractVersion: payload.measurementContractVersion,
      });
      if (
        !near(t.considerationRate, e?.considerationRate) ||
        !near(t.scenarioPresenceRate, e?.scenarioPresenceRate) ||
        !near(t.propertyRealityCoverage, e?.propertyRealityCoverage) ||
        !near(t.considerationRate, cert?.considerationRate) ||
        !near(t.scenarioPresenceRate, cert?.scenarioPresenceRate) ||
        !near(t.propertyRealityCoverage, cert?.propertyRealityCoverage)
      ) {
        metricPass = false;
        row.metricDefect = { trend: t, expected: e, certified: cert };
      }
    }

    // Stale single-period must be repaired on enrich
    const baked = loadPublishedReport(propertyId);
    const stalePayload = {
      ...baked,
      trends: (baked?.trends || []).slice(0, 1),
    };
    const repaired = enrichPayloadOptionalMetrics(propertyId, stalePayload);
    const repairedLen = repaired?.trends?.length || 0;
    if (!isStaleSinglePeriodTrends(stalePayload.trends, resolution)) {
      // If resolution says 2, slice(0,1) must be detected stale
      if (resolution.periodCount >= 2) {
        stalePass = false;
        row.staleDetectDefect = true;
      }
    }
    if (resolution.periodCount >= 2 && repairedLen < 2) {
      stalePass = false;
      row.staleRepairDefect = true;
    }
    if (/Awaiting next comparable period/i.test(JSON.stringify(repaired?.trends || []))) {
      stalePass = false;
    }

    results.properties.push(row);
  }

  results.gates[TREND_PERIOD_COUNT_MATCHES_COMPARABLE_HISTORY] = periodCountPass ? "PASS" : "FAIL";
  results.gates[TREND_METRIC_VALUES_MATCH_CERTIFIED_PERIOD_METRICS] = metricPass ? "PASS" : "FAIL";
  results.gates[TREND_CURRENT_PRIOR_RECONCILIATION] = reconcilePass ? "PASS" : "FAIL";
  results.gates[TREND_NO_STALE_SINGLE_PERIOD_AFTER_CERTIFIED_PUBLICATION] = stalePass
    ? "PASS"
    : "FAIL";
  results.CORE_TRENDS_PERIOD_2_READY =
    periodCountPass && metricPass && reconcilePass && stalePass && usesCanonical ? "PASS" : "FAIL";

  assert.equal(results.gates[TREND_PERIOD_COUNT_MATCHES_COMPARABLE_HISTORY], "PASS");
  assert.equal(results.gates[TREND_METRIC_VALUES_MATCH_CERTIFIED_PERIOD_METRICS], "PASS");
  assert.equal(results.gates[TREND_CURRENT_PRIOR_RECONCILIATION], "PASS");
  assert.equal(results.gates[TREND_NO_STALE_SINGLE_PERIOD_AFTER_CERTIFIED_PUBLICATION], "PASS");
  assert.equal(results.CORE_TRENDS_PERIOD_2_READY, "PASS");

  mkdirSync(OUT_DIR, { recursive: true });
  const outPath = join(OUT_DIR, `core-trends-period2-wiring-${Date.now()}.json`);
  writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(JSON.stringify({ ...results, outPath }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
