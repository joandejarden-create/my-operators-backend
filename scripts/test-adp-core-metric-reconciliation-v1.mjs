#!/usr/bin/env node
/**
 * test:adp-core-metric-reconciliation-v1
 * Assert numerators/denominators for Toronto Core Independent Unprompted metrics.
 */
import assert from "assert";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_ID = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";

const period = loadPeriod(CORE_ID);
const pub = loadPublishedReport(PROPERTY_ID);
const profile = loadPropertyProfile(PROPERTY_ID);
assert.ok(period && pub && profile);

const frozen = new Set(pub.executiveMetrics?.considerationRate?.eligibleScenarioIds || []);
assert.equal(frozen.size, 55);

const enriched = enrichObservationsWithRank(period.observations, profile);
const comparable = filterComparableObservations(enriched).filter((o) =>
  frozen.has(o.scenarioId)
);
const mentioned = comparable.filter((o) => o.mentioned);
assert.equal(comparable.length, 220);
assert.equal(mentioned.length, 73);
assert.equal(roundAdpPercent((mentioned.length / comparable.length) * 100), 33.2);

const scenarioIds = period.scenarioIds || [];
assert.equal(scenarioIds.length, 63);
const present = new Set();
for (const o of filterComparableObservations(enriched)) {
  if (o.mentioned) present.add(o.scenarioId);
}
assert.equal([...present].filter((id) => scenarioIds.includes(id)).length, 24);
assert.equal(roundAdpPercent((24 / 63) * 100), 38.1);

const rankElig = comparable.filter(
  (o) => o.mentioned && o.rankEligible && o.position != null
);
assert.equal(rankElig.length, 47);
assert.equal(rankElig.filter((o) => o.position === 1).length, 21);
assert.equal(rankElig.filter((o) => o.position <= 3).length, 40);
assert.equal(roundAdpPercent((21 / 47) * 100), 44.7);
assert.equal(roundAdpPercent((40 / 47) * 100), 85.1);

const rg = pub.realityGap;
assert.equal(rg.methodologyId, "RG_WEIGHTED_PARTIAL_V2");
assert.equal(rg.recognizedCount, 4);
assert.equal(rg.partialCount, 6);
assert.equal(rg.missedCount, 11);
assert.equal(rg.coveragePercent, 19);
assert.equal(rg.gapScore, 66.7);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-core-metric-reconciliation-v1",
      consideration: { num: 73, den: 220, pct: 33.2 },
      presence: { num: 24, den: 63, pct: 38.1 },
      n1: { num: 21, den: 47, pct: 44.7 },
      t3: { num: 40, den: 47, pct: 85.1 },
      realityGap: { coverage: 19, gap: 66.7 },
    },
    null,
    2
  )
);
