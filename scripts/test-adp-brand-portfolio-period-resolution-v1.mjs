#!/usr/bin/env node
import assert from "assert";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { resolveReportPeriodIds } from "../lib/ai-demand-positioning/brand-portfolio/resolve-measurement-available-bpp-v1.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY = "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";

const profile = loadPropertyProfile(PROPERTY_ID);
const bpp = resolveBrandPortfolioPosition(PROPERTY_ID, profile, { query: {} });
const periods = resolveReportPeriodIds(PROPERTY_ID, bpp);

assert.equal(bpp.measurementFamily, "LOYALTY_BPP");
assert.equal(bpp.periodId || bpp.portfolioPeriodId, LOYALTY);
assert.equal(periods.corePeriodId, CORE);
assert.equal(periods.portfolioPeriodId, LOYALTY);
assert.notEqual(periods.corePeriodId, periods.portfolioPeriodId);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-brand-portfolio-period-resolution-v1",
      corePeriodId: periods.corePeriodId,
      portfolioPeriodId: periods.portfolioPeriodId,
    },
    null,
    2
  )
);
