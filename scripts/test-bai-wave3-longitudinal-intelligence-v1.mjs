#!/usr/bin/env node
/**
 * BAI Wave 3 Longitudinal Intelligence + KPI info-icon polish gates.
 * Stored corpus only. PROVIDER_CALLS = 0. Period 2 stays UNPROMOTED.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BAI_VIEW_MODE,
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER,
  BAI_UNPROMOTED_PERIOD_ISOLATION,
  resolveBaiPriorComparablePeriodV1,
  assertBaiCustomerPublicationIsolation,
  getBaiPeriodPublicationState,
} from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";
import {
  BAI_PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY,
  BAI_ABSOLUTE_RELATIVE_PERFORMANCE_SEPARATION,
  BAI_BRAND_LONGITUDINAL_IDENTITY_INTEGRITY,
  BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY,
  BAI_LONGITUDINAL_MEMBERSHIP_STATE_INTEGRITY,
  BAI_INTENT_PRIOR_RUN_RECONCILIATION,
  BAI_DELTA_DISPLAY_SEMANTICS_INTEGRITY,
  BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION,
  BAI_PRIOR_RUN_SAME_CANONICAL_SOURCE,
  BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION,
  BAI_WAVE3_MARRIOTT_BRAND_IDS,
  buildBaiWave3LongitudinalIntelligenceV1,
  rankBrandsInPeriodUniverse,
  PERFORMANCE_DIRECTION,
} from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";
import {
  formatGovernedDeltaDisplay,
  DELTA_UNIT,
} from "../lib/ai-demand-positioning/longitudinal/resolve-row-level-prior-comparison-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BRAND_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
  "utf8"
);
const SHARE_HTML = fs.readFileSync(
  path.join(ROOT, "public/brand-ai-visibility-share.html"),
  "utf8"
);
const AUTH_HTML = fs.readFileSync(
  path.join(ROOT, "public/ai-visibility-brand.html"),
  "utf8"
);
const API_JS = fs.readFileSync(path.join(ROOT, "api/ai-visibility-brand.js"), "utf8");
const SERVER_JS = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");

const BAI_KPI_INFO_ICON_PARITY = "BAI_KPI_INFO_ICON_PARITY";

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

await test(BAI_KPI_INFO_ICON_PARITY, () => {
  assert.match(BRAND_JS, /function renderBaiKpiCardWithInfo/);
  assert.match(BRAND_JS, /function baiKpiTipHtml/);
  assert.match(BRAND_JS, /data-bai-kpi-info-icon-parity/);
  assert.match(BRAND_JS, /#aiv-info-icon/);
  assert.match(BRAND_JS, /Why track it/);
  const tipStart = BRAND_JS.indexOf("function baiKpiTipHtml");
  const tipEnd = BRAND_JS.indexOf("function buildBaiExecutiveReadPresentation");
  assert.ok(tipStart > 0 && tipEnd > tipStart);
  const tipBlock = BRAND_JS.slice(tipStart, tipEnd);
  assert.doesNotMatch(tipBlock, /promptId|canonical prompt|scoring weight/i);
  assert.match(BRAND_JS, /cards\.map\(renderBaiKpiCardWithInfo\)/);
  assert.ok((BRAND_JS.match(/tip:\s*baiKpiTipHtml\(/g) || []).length === 5);
});

await test(BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER, () => {
  const customer = resolveBaiPriorComparablePeriodV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
  });
  assert.equal(customer.currentPeriodId, BAI_CUSTOMER_PUBLISHED_PERIOD_ID);
  assert.equal(customer.priorPeriodId, null);
  assert.equal(customer.comparable, false);
  assert.equal(customer.period2Exposed, false);
  assert.equal(customer.gate, BAI_SINGLE_CANONICAL_PRIOR_PERIOD_RESOLVER);

  const candidate = resolveBaiPriorComparablePeriodV1({
    viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  });
  assert.equal(candidate.currentPeriodId, BAI_PERIOD_2_CANDIDATE_ID);
  assert.ok(candidate.priorPeriodId, "candidate must resolve a prior period");
  assert.notEqual(candidate.priorPeriodId, BAI_PERIOD_2_CANDIDATE_ID);
  assert.equal(candidate.period2Exposed, true);
});

await test(BAI_UNPROMOTED_PERIOD_ISOLATION, () => {
  assert.equal(
    getBaiPeriodPublicationState(BAI_PERIOD_2_CANDIDATE_ID),
    "CERTIFIED_UNPROMOTED"
  );
  const blocked = assertBaiCustomerPublicationIsolation(
    BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
    "current_published"
  );
  assert.equal(blocked.ok, false);
  const ok = assertBaiCustomerPublicationIsolation(
    BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
    "current_published"
  );
  assert.equal(ok.ok, true);
  assert.match(API_JS, /assertBaiCustomerPublicationIsolation/);
  assert.match(API_JS, /share_forbidden/);
  assert.match(SERVER_JS, /internal-longitudinal-qa/);
  assert.doesNotMatch(SHARE_HTML, /internal-longitudinal-qa/);
  assert.doesNotMatch(SHARE_HTML, /20260902_d3d713/);
});

await test(BAI_PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY, () => {
  assert.equal(
    formatGovernedDeltaDisplay({ delta: 0, deltaUnit: DELTA_UNIT.PP }),
    "0.0 pp"
  );
  assert.equal(
    formatGovernedDeltaDisplay({ delta: null, deltaUnit: DELTA_UNIT.PP }),
    null
  );
  assert.equal(
    formatGovernedDeltaDisplay({ delta: -10, deltaUnit: DELTA_UNIT.PP }),
    "-10.0 pp"
  );
  assert.equal(
    formatGovernedDeltaDisplay({ delta: 10, deltaUnit: DELTA_UNIT.PP }),
    "+10.0 pp"
  );
});

await test(BAI_DELTA_DISPLAY_SEMANTICS_INTEGRITY, () => {
  const pp = formatGovernedDeltaDisplay({ delta: 5.5, deltaUnit: DELTA_UNIT.PP });
  assert.match(pp, /pp$/);
  assert.doesNotMatch(pp, /%$/);
  const rank = formatGovernedDeltaDisplay({ delta: 2, deltaUnit: DELTA_UNIT.RANK });
  assert.match(rank, /↑2/);
});

await test(BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY, () => {
  const rows = [
    { brandId: "a", CURRENT_PRESENCE: 10, PRIOR_PRESENCE: 90 },
    { brandId: "b", CURRENT_PRESENCE: 80, PRIOR_PRESENCE: 20 },
  ];
  const cur = rankBrandsInPeriodUniverse(rows, "CURRENT_PRESENCE");
  const pri = rankBrandsInPeriodUniverse(rows, "PRIOR_PRESENCE");
  assert.equal(cur.rankById.get("b"), 1);
  assert.equal(cur.rankById.get("a"), 2);
  assert.equal(pri.rankById.get("a"), 1);
  assert.equal(pri.rankById.get("b"), 2);
  assert.equal(cur.gate, BAI_HISTORICAL_RANK_PERIOD_UNIVERSE_INTEGRITY);
});

const intel = buildBaiWave3LongitudinalIntelligenceV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  parentCompanyName: "Marriott",
  brandIds: BAI_WAVE3_MARRIOTT_BRAND_IDS,
});

await test(BAI_PRIOR_RUN_SAME_CANONICAL_SOURCE, () => {
  assert.equal(intel.ok, true);
  assert.equal(intel.periodResolve.currentPeriodId, BAI_PERIOD_2_CANDIDATE_ID);
  assert.equal(intel.LIVE_PROVIDER_CALLS, 0);
  assert.equal(intel.PERIOD_2_PUBLICATION_STATE, "UNPROMOTED");
});

await test(BAI_BRAND_LONGITUDINAL_IDENTITY_INTEGRITY, () => {
  assert.equal(intel.brands.length, 5);
  const ids = intel.brands.map((b) => b.brandId);
  for (const id of BAI_WAVE3_MARRIOTT_BRAND_IDS) {
    assert.ok(ids.includes(id), "missing brandId " + id);
  }
  const names = intel.brands.map((b) => b.brandName);
  assert.ok(names.includes("Autograph Collection"));
  assert.ok(names.includes("Tribute Portfolio"));
  assert.ok(names.includes("Design Hotels"));
  assert.ok(names.includes("Westin"));
  assert.ok(names.includes("AC Hotels by Marriott"));
});

await test(BAI_ABSOLUTE_RELATIVE_PERFORMANCE_SEPARATION, () => {
  for (const b of intel.brands) {
    assert.ok(b.absoluteRelative);
    assert.ok(
      Object.values(PERFORMANCE_DIRECTION).includes(b.absoluteRelative.absolutePerformance)
    );
    assert.ok(
      Object.values(PERFORMANCE_DIRECTION).includes(b.absoluteRelative.relativePerformance)
    );
    assert.match(b.absoluteRelativeNarrative, /absolute|Comparable/i);
    // Forbidden: single word "strengthened" without absolute/relative framing when both available
    if (
      b.absoluteRelative.absolutePerformance !== PERFORMANCE_DIRECTION.UNAVAILABLE &&
      b.absoluteRelative.relativePerformance !== PERFORMANCE_DIRECTION.UNAVAILABLE
    ) {
      assert.match(b.absoluteRelativeNarrative, /absolute/);
      assert.match(b.absoluteRelativeNarrative, /relative/);
    }
  }
  // Tribute improved absolutely in certified artifact; Autograph declined.
  const tribute = intel.brands.find((b) => b.brandName === "Tribute Portfolio");
  const auto = intel.brands.find((b) => b.brandName === "Autograph Collection");
  assert.ok(tribute.deltaPp > 0);
  assert.ok(auto.deltaPp < 0);
});

await test(BAI_LONGITUDINAL_MEMBERSHIP_STATE_INTEGRITY, () => {
  for (const b of intel.brands) {
    assert.ok(
      ["SAME", "NEW", "EXITED", "RETURNED", "NOT_COMPARABLE"].includes(b.membershipState)
    );
  }
});

await test(BAI_LONGITUDINAL_NARRATIVE_RECONCILIATION, () => {
  assert.equal(intel.executiveLongitudinal.available, true);
  const n = intel.executiveLongitudinal.narrative;
  assert.match(n, /Portfolio AI Presence|prior comparable/i);
  assert.match(n, /absolute|relative/i);
  if (intel.portfolio.portfolioDeltaDisplay) {
    assert.ok(
      n.includes(intel.portfolio.portfolioDeltaDisplay) ||
        n.includes(String(intel.portfolio.portfolioDeltaPp))
    );
  }
});

await test(BAI_INTENT_PRIOR_RUN_RECONCILIATION, () => {
  const first = intel.brands[0];
  const intent = intel.intentByBrand[first.brandId];
  assert.equal(intent.ok, true);
  assert.ok(Array.isArray(intent.intents));
  const blob = JSON.stringify(intent);
  assert.doesNotMatch(blob, /"promptId"|"promptText"|exact production prompt/i);
});

await test(BAI_WAVE3_NO_CUSTOMER_PUBLICATION_MUTATION, () => {
  const customerMode = buildBaiWave3LongitudinalIntelligenceV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
  });
  assert.equal(customerMode.ok, false);
  assert.equal(customerMode.longitudinal, null);
  // Customer HTML must not embed Period 2 id
  assert.doesNotMatch(AUTH_HTML + SHARE_HTML, /aiv_brand_longitudinal_period_20260902_d3d713/);
  assert.match(API_JS, /PERIOD_2_PUBLICATION_STATE: "UNPROMOTED"/);
});

console.log("");
console.log(
  failed === 0
    ? `BAI Wave 3 gates PASS (${passed})`
    : `BAI Wave 3 gates FAIL (${failed} failed, ${passed} passed)`
);
process.exit(failed === 0 ? 0 : 1);
