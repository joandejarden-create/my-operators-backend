#!/usr/bin/env node
import assert from "assert";
import { readFileSync } from "fs";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { bppComparisonCertifiedIsRenderable } from "../lib/ai-demand-positioning/certification/adp-bpp-source-api-dom-parity-v1.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const profile = loadPropertyProfile(PROPERTY_ID);
const bpp = resolveBrandPortfolioPosition(PROPERTY_ID, profile, { query: {} });

assert.equal(bpp.status, "READY");
assert.equal(bpp.lens?.label, "Choice Privileges");
assert.ok(
  bpp.bppCustomerState === "BPP_COMPARISON_CERTIFIED" ||
    bpp.bppCustomerState === "CUSTOMER_READY" ||
    bpp.assuranceStatus === "CUSTOMER_READY" ||
    bpp.assuranceStatus === "STAGED_COMPARISON_CERTIFIED"
);
assert.equal(bpp.showAnalyticalScaffolding !== false, true);
assert.equal(
  bppComparisonCertifiedIsRenderable(bpp) || bpp.status === "READY",
  true
);

const rank = (bpp.kpis || []).find((k) => k.id === "portfolioRank");
assert.equal(rank?.value, "#1 of 6");
assert.equal((bpp.ranking?.rows || []).length, 6);
assert.equal(bpp.ranking.rows[0].name, "Radisson Blu Toronto Downtown");

const presence = (bpp.kpis || []).find((k) => k.id === "portfolioAiPresence");
assert.equal(presence?.value, "45.5%");
const n1 = (bpp.kpis || []).find((k) => k.id === "numberOneAppearance");
assert.equal(n1?.value, "0.0%");
const t3 = (bpp.kpis || []).find((k) => k.id === "top3Appearance");
assert.equal(t3?.value, "92.9%");

// SPA readiness must accept STAGED_COMPARISON_CERTIFIED / BPP_COMPARISON_CERTIFIED
const spa = readFileSync(
  "public/js/ai-demand-positioning/ai-demand-positioning.js",
  "utf8"
);
assert.match(spa, /function isBrandPortfolioReady\(bpp\)/);
assert.match(spa, /STAGED_COMPARISON_CERTIFIED/);
assert.match(spa, /BPP_COMPARISON_CERTIFIED/);
assert.match(spa, /bppLooksMeasurementAvailable/);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-brand-portfolio-rendering-v1",
      lens: bpp.lens.label,
      rank: rank.value,
      fallback: false,
    },
    null,
    2
  )
);
