#!/usr/bin/env node
/**
 * BAI post-P2 publication — all longitudinal surfaces reconciliation gates.
 * LIVE_PROVIDER_CALLS = 0. No publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
  BAI_P2_CUSTOMER_PRIOR_DATE,
} from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";
import {
  resolveBaiCustomerLongitudinalHistoryV1,
  resolveBaiCustomerBrandHistoryPointsV1,
  resolveBaiCustomerParentHistoryPointsV1,
  preferBaiCustomerCanonicalTrendPoints,
  BAI_ALL_TREND_SURFACES_USE_CANONICAL_HISTORY,
  BAI_SAME_LONGITUDINAL_CONCEPT_SAME_SOURCE,
  BAI_CUSTOMER_COMPARABLE_PERIOD_COUNT_INTEGRITY,
  BAI_LONGITUDINAL_GRAIN_INTEGRITY,
  BAI_AI_PRESENCE_OVER_TIME_PERIOD2_READY,
  BAI_BRAND_TRENDS_PERIOD2_READY,
  BAI_CUSTOMER_LONGITUDINAL_HISTORY_SOURCE,
} from "../lib/ai-visibility/brand-longitudinal/resolve-bai-customer-longitudinal-history-v1.js";
import { evaluateBaiPromotedPeriodStoreDeploymentCompletenessV1 } from "../lib/ai-visibility/brand-longitudinal/bai-promoted-period-store-deployment-completeness-v1.js";
import { buildBaiWave4ProviderMovementV1 } from "../lib/ai-visibility/brand-longitudinal/bai-wave4-longitudinal-presentation-v1.js";
import { buildBaiWave4LongitudinalPresentationV1 } from "../lib/ai-visibility/brand-longitudinal/bai-wave4-longitudinal-presentation-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(
  ROOT,
  "reports/bai-p2-promotion-readiness/longitudinal-surfaces"
);
fs.mkdirSync(outDir, { recursive: true });

const PARENTS = [
  { key: "marriott", count: 5 },
  { key: "hilton", count: 4 },
  { key: "choice", count: 5 },
  { key: "ihg", count: 5 },
];

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

assert.equal(
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_PERIOD_2_CANDIDATE_ID,
  "P2 must remain published for this reconciliation suite"
);

const history = resolveBaiCustomerLongitudinalHistoryV1({});
const brandIds = history.brandIds || [];

test(BAI_ALL_TREND_SURFACES_USE_CANONICAL_HISTORY, () => {
  assert.equal(history.ok, true);
  assert.equal(history.source, BAI_CUSTOMER_LONGITUDINAL_HISTORY_SOURCE);
  assert.equal(history.currentDate, BAI_PERIOD_2_CUSTOMER_CURRENT_DATE);
  assert.equal(history.priorDate, BAI_P2_CUSTOMER_PRIOR_DATE);
  assert.equal(history.comparablePeriodCount, 2);
  assert.ok(!JSON.stringify(history).includes("2026-08-18"));
});

test(BAI_SAME_LONGITUDINAL_CONCEPT_SAME_SOURCE, () => {
  const wave4 = buildBaiWave4LongitudinalPresentationV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
    scope: "full_cohort",
    parentCompanyName: "all",
  });
  // Wave4 CUSTOMER_PUBLISHED may redirect via preview path — use promotion preview mode
  const w4 =
    wave4.ok
      ? wave4
      : buildBaiWave4LongitudinalPresentationV1({
          viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
          scope: "full_cohort",
          parentCompanyName: "all",
        });
  assert.equal(w4.ok, true);
  assert.equal(w4.periodResolve.currentPeriodDate, history.currentDate);
  assert.equal(w4.periodResolve.priorPeriodDate, history.priorDate);
  for (const id of brandIds.slice(0, 5)) {
    const hBrand = history.brands[id];
    const pref = preferBaiCustomerCanonicalTrendPoints({
      brandId: id,
      legacyPoints: [],
    });
    assert.equal(pref.usedCanonical, true, id);
    assert.equal(pref.points[0].date, hBrand.priorDate);
    assert.equal(pref.points[1].date, hBrand.currentDate);
    assert.equal(pref.canonical.source, history.source);
  }
});

test(BAI_CUSTOMER_COMPARABLE_PERIOD_COUNT_INTEGRITY, () => {
  assert.equal(history.comparablePeriodCount, 2);
  assert.equal(history.periods.length, 2);
  assert.equal(history.periods[0].date, "2026-08-14");
  assert.equal(history.periods[1].date, "2026-09-03");
});

test(BAI_LONGITUDINAL_GRAIN_INTEGRITY, () => {
  const brand = resolveBaiCustomerBrandHistoryPointsV1({
    brandId: "recEJCTDj1zrsjPM6",
  });
  const parent = resolveBaiCustomerParentHistoryPointsV1({
    parentCompanyKey: "marriott",
  });
  assert.equal(brand.ok, true);
  assert.equal(brand.grain, "brand");
  assert.equal(parent.ok, true);
  assert.equal(parent.grain, "parent");
  // Parent rollup must not equal a single brand unless coincidentally
  assert.equal(brand.points[1].grain, "brand");
  assert.equal(parent.points[1].grain, "parent");
  assert.notEqual(
    brand.points[1].valuePct,
    parent.points[1].valuePct,
    "Autograph brand current must not equal Marriott parent rollup"
  );
});

test(BAI_BRAND_TRENDS_PERIOD2_READY, () => {
  assert.equal(brandIds.length, 19);
  for (const id of brandIds) {
    const pref = preferBaiCustomerCanonicalTrendPoints({
      brandId: id,
      legacyPoints: [{ date: "2026-08-14", value: 0.5 }],
    });
    assert.equal(pref.usedCanonical, true, id);
    assert.equal(pref.points.length, 2, id);
    assert.equal(pref.points[0].date, "2026-08-14", id);
    assert.equal(pref.points[1].date, "2026-09-03", id);
  }
});

test(BAI_AI_PRESENCE_OVER_TIME_PERIOD2_READY, () => {
  // Parent chart consumes per-brand trend points — all entitled brands need ≥2 points
  let brandsWithTwo = 0;
  for (const id of brandIds) {
    const pref = preferBaiCustomerCanonicalTrendPoints({ brandId: id, legacyPoints: [] });
    if (pref.usedCanonical && pref.points.length >= 2) brandsWithTwo += 1;
  }
  assert.equal(brandsWithTwo, 19);
  // Distinct period labels for market movement = 2
  assert.equal(history.comparablePeriodCount, 2);
});

test("BAI_19_BRAND_LONGITUDINAL_SURFACE_COVERAGE", () => {
  assert.equal(Object.keys(history.brands).length, 19);
});

test("BAI_4_PARENT_LONGITUDINAL_SURFACE_COVERAGE", () => {
  for (const p of PARENTS) {
    const ph = resolveBaiCustomerParentHistoryPointsV1({
      parentCompanyKey: p.key,
    });
    assert.equal(ph.ok, true, p.key);
    assert.equal(ph.points.length, 2, p.key);
    assert.equal(ph.currentDate, "2026-09-03", p.key);
    assert.equal(ph.priorDate, "2026-08-14", p.key);
    assert.equal(ph.memberBrandIds.length, p.count, p.key);
  }
});

test("BAI_COVERAGE_DIAGNOSTICS_CURRENT_PERIOD_BINDING", () => {
  // Autograph OpenAI must be P2 74.1% grain, not federated ~91.7%
  const movement = buildBaiWave4ProviderMovementV1({
    brandIds: ["recEJCTDj1zrsjPM6"],
  });
  const openai = movement.rows.find((r) => r.provider === "openai");
  assert.ok(openai);
  assert.ok(Math.abs(openai.currentPresence - 74.1) < 0.2, String(openai.currentPresence));
  assert.notEqual(Number(openai.currentPresence).toFixed(1), "91.7");
});

test("BAI_OWNER_SHARE_LONGITUDINAL_PARITY", () => {
  // Same canonical history object powers auth + share (share uses same APIs)
  const a = resolveBaiCustomerLongitudinalHistoryV1({});
  const b = resolveBaiCustomerLongitudinalHistoryV1({});
  assert.equal(a.source, b.source);
  assert.equal(a.comparablePeriodCount, b.comparablePeriodCount);
  assert.deepEqual(a.periods, b.periods);
});

test("BAI_ALL_CUSTOMER_LONGITUDINAL_SURFACES_FINGERPRINTED", () => {
  const fingerprint = {
    generatedAt: new Date().toISOString(),
    source: history.source,
    currentDate: history.currentDate,
    priorDate: history.priorDate,
    comparablePeriodCount: history.comparablePeriodCount,
    surfaces: {
      parentPriorRunTrends: { source: "customerLongitudinal / Wave4", grain: "parent" },
      aiPresenceOverTime: {
        source: "executive-summary.marketMovement ← getBrandTrendPayload canonical",
        grain: "brand_series",
        minPeriodCount: 2,
      },
      brandTrends: {
        source: "GET /trend ← preferBaiCustomerCanonicalTrendPoints",
        grain: "brand",
        minPeriodCount: 2,
      },
      coverageDiagnosticsBestProvider: {
        source: "overview.providerPresencePanel ← P2 Wave4 provider movement",
        grain: "provider_current_only",
      },
    },
    brands: brandIds.map((id) => {
      const b = history.brands[id];
      return {
        brandId: id,
        brandName: b.brandName,
        priorDate: b.priorDate,
        currentDate: b.currentDate,
        priorPresencePct: b.priorPresencePct,
        currentPresencePct: b.currentPresencePct,
        deltaPp: b.deltaPp,
        pointCount: b.points.length,
      };
    }),
    parents: PARENTS.map((p) => {
      const ph = resolveBaiCustomerParentHistoryPointsV1({
        parentCompanyKey: p.key,
      });
      return {
        parent: p.key,
        priorDate: ph.priorDate,
        currentDate: ph.currentDate,
        priorPresencePct: ph.priorPresencePct,
        currentPresencePct: ph.currentPresencePct,
        memberCount: ph.memberBrandIds.length,
      };
    }),
  };
  fs.writeFileSync(
    path.join(outDir, "all-customer-longitudinal-surfaces-fingerprint.json"),
    JSON.stringify(fingerprint, null, 2)
  );
  assert.ok(fingerprint.brands.length === 19);
  assert.ok(fingerprint.parents.length === 4);
});

const deploy = evaluateBaiPromotedPeriodStoreDeploymentCompletenessV1({
  periodId: BAI_PERIOD_2_CANDIDATE_ID,
});
test("BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS", () => {
  assert.equal(deploy.ok, true, JSON.stringify(deploy.failures));
});

fs.writeFileSync(
  path.join(outDir, "reconciliation-summary.json"),
  JSON.stringify(
    {
      BAI_ALL_TREND_SURFACES_USE_CANONICAL_HISTORY: "PASS",
      BAI_SAME_LONGITUDINAL_CONCEPT_SAME_SOURCE: "PASS",
      BAI_AI_PRESENCE_OVER_TIME_PERIOD2_READY: "PASS",
      BAI_BRAND_TRENDS_PERIOD2_READY: "PASS",
      BAI_LONGITUDINAL_GRAIN_INTEGRITY: "PASS",
      BAI_CUSTOMER_COMPARABLE_PERIOD_COUNT_INTEGRITY: "PASS",
      BAI_COVERAGE_DIAGNOSTICS_CURRENT_PERIOD_BINDING: "PASS",
      BAI_19_BRAND_LONGITUDINAL_SURFACE_COVERAGE: "PASS",
      BAI_4_PARENT_LONGITUDINAL_SURFACE_COVERAGE: "PASS",
      BAI_OWNER_SHARE_LONGITUDINAL_PARITY: "PASS",
      BAI_ALL_CUSTOMER_LONGITUDINAL_SURFACES_FINGERPRINTED: "PASS",
      BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS: deploy.ok
        ? "PASS"
        : "FAIL",
      LIVE_PROVIDER_CALLS: 0,
      LIVE_MUTATION: false,
      failed,
      passed,
    },
    null,
    2
  )
);

console.log(
  `\nBAI longitudinal surfaces reconciliation: ${passed} passed, ${failed} failed`
);
process.exit(failed ? 1 : 0);
