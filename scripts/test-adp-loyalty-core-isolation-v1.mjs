#!/usr/bin/env node
/**
 * test:adp-loyalty-core-isolation-v1
 */
import assert from "assert";
import {
  loadPeriod,
  loadLatestCustomerPeriod,
  loadLatestLoyaltyPeriod,
  isLoyaltyMeasurementPeriod,
} from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_ID = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY_ID =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";

const core = loadPeriod(CORE_ID);
const loyalty = loadPeriod(LOYALTY_ID);
assert.equal(core.measurementFamily, "CORE_ADP");
assert.equal(loyalty.measurementFamily, "LOYALTY_BPP");
assert.equal(core.observations.length, 252);
assert.equal(core.scenarioIds.length, 63);
assert.equal(loyalty.observations.length, 112);
assert.equal((loyalty.scenarioIds || []).length || loyalty.scenarioCount, 28);

assert.equal(isLoyaltyMeasurementPeriod(core), false);
assert.equal(isLoyaltyMeasurementPeriod(loyalty), true);
assert.equal(loadLatestCustomerPeriod(PROPERTY_ID)?.periodId, CORE_ID);
assert.equal(loadLatestLoyaltyPeriod(PROPERTY_ID)?.periodId, LOYALTY_ID);

const pub = loadPublishedReport(PROPERTY_ID);
assert.equal(pub.period?.periodId, CORE_ID);
assert.equal(pub.brandPortfolioPosition?.measurementFamily, "LOYALTY_BPP");
assert.equal(pub.executiveMetrics?.considerationRate?.rate, 33.2);

// Loyalty observation ids must not appear in Core period
const coreObs = new Set(core.observations.map((o) => o.observationId));
for (const o of loyalty.observations) {
  assert.equal(coreObs.has(o.observationId), false);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-loyalty-core-isolation-v1",
      core: CORE_ID,
      loyalty: LOYALTY_ID,
    },
    null,
    2
  )
);
