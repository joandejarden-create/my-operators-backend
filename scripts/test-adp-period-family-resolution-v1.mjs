#!/usr/bin/env node
/**
 * test:adp-period-family-resolution-v1
 * Core vs Loyalty period family must not collapse to "latest file".
 */
import assert from "assert";
import {
  loadLatestCustomerPeriod,
  loadLatestLoyaltyPeriod,
  loadLatestPeriod,
  isLoyaltyMeasurementPeriod,
  loadPropertyProfile,
} from "../lib/ai-demand-positioning/data-model.js";

const TORONTO = "adp_radisson_blu_toronto_downtown";
const CORE = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY = "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";

const REGRESSION = [
  "adp_bethesda_marriott",
  "adp_waterstone_boca_raton",
  "adp_mayflower_hotel_washington_dc",
  "adp_wyndham_san_diego_bayside",
  "adp_yotel_geneva_lake",
  "adp_nobu_london_portman",
];

const core = loadLatestCustomerPeriod(TORONTO);
assert.ok(core, "Toronto Core period required");
assert.equal(core.periodId, CORE);
assert.equal(isLoyaltyMeasurementPeriod(core), false);
assert.equal(core.measurementFamily || "CORE_ADP", "CORE_ADP");

const loyalty = loadLatestLoyaltyPeriod(TORONTO);
assert.ok(loyalty, "Toronto Loyalty period required");
assert.equal(loyalty.periodId, LOYALTY);
assert.equal(isLoyaltyMeasurementPeriod(loyalty), true);

const latestNoLoyalty = loadLatestPeriod(TORONTO, { includeLoyalty: false });
assert.ok(latestNoLoyalty);
assert.equal(isLoyaltyMeasurementPeriod(latestNoLoyalty), false);

for (const id of REGRESSION) {
  const profile = loadPropertyProfile(id);
  if (!profile) continue;
  const p = loadLatestCustomerPeriod(id);
  if (!p) continue;
  assert.equal(
    isLoyaltyMeasurementPeriod(p),
    false,
    `${id} customer period must not be Loyalty`
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-period-family-resolution-v1",
      torontoCore: core.periodId,
      torontoLoyalty: loyalty.periodId,
    },
    null,
    2
  )
);
