#!/usr/bin/env node
import assert from "assert";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const LOYALTY = "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";
const CORE = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";

const bpp = resolveBrandPortfolioPosition(
  PROPERTY_ID,
  loadPropertyProfile(PROPERTY_ID),
  { query: {} }
);

assert.ok(bpp.evidence, "evidence pack required");
const all = [
  ...(bpp.evidence.positive || []),
  ...(bpp.evidence.missing || []),
  ...(bpp.evidence.displacement || []),
];
assert.ok(all.length > 0, "evidence items required");
for (const ev of all) {
  assert.equal(ev.periodId || ev.measurementPeriodId, LOYALTY);
  assert.notEqual(ev.periodId || ev.measurementPeriodId, CORE);
  assert.ok(String(ev.aiResponse || ev.rawResponse || "").trim().length > 0);
}

assert.equal(bpp.evidence.positive.length, 51);
assert.equal(bpp.evidence.missing.length, 61);
assert.equal(
  bpp.evidence.measurementPeriodId || bpp.periodId || bpp.portfolioPeriodId,
  LOYALTY
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-brand-portfolio-evidence-family-v1",
      periodId: LOYALTY,
      positive: bpp.evidence.positive.length,
      missing: bpp.evidence.missing.length,
      displacement: bpp.evidence.displacement.length,
    },
    null,
    2
  )
);
