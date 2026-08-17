#!/usr/bin/env node
/**
 * Phase 3A.4 — Two-tab consolidation + Recommendation Rate / Top-3 (no provider calls).
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  POSITIVE_RECOMMENDATION_ROLES,
  isPositiveRecommendationRole,
  computeRecommendationRate,
  computeTop3RecommendationRate,
  computeFirstRecommendationRate,
  computeRecommendationShare,
  buildObservationFromExtractions,
  METRIC_VERSION,
} from "../lib/ai-visibility/metrics.js";
import {
  buildOpenAiDiscoverabilityExecutivePlaceholder,
  buildOpenAiDiscoverabilityDetailPlaceholder,
  OPENAI_DISCOVERABILITY_STATUS,
} from "../lib/ai-visibility/future-discoverability.js";
import { BRAND_AI_VISIBILITY_EXPECTED_ROUTES } from "../lib/ai-visibility/route-registration-guard.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3A.4 — Two-Tab Consolidation + Metric Reconciliation\n");

test("exactly two visible tabs; Exec default; no HDV nav", () => {
  const html = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
  const tabs = [...html.matchAll(/id="aivTab(\w+)"/g)].map((m) => m[1]);
  assert.deepEqual(tabs, ["Executive", "Detail"]);
  assert.match(html, /class="section-nav-item active"[^>]*id="aivTabExecutive"/);
  assert.ok(!/aivTabHdv|aivHdvView|aivHdvSubject/.test(html));
});

test("stale HDV tab state falls back to executive in client JS", () => {
  const js = fs.readFileSync(
    path.join(root, "public/js/ai-visibility/ai-visibility-brand.js"),
    "utf8"
  );
  assert.match(js, /Stale HDV/);
  assert.match(js, /else state\.tab = "executive"/);
  assert.ok(!/hotel-decision-visibility/.test(js));
  assert.ok(!/setActiveTab\("hdv"\)/.test(js));
});

test("HDV intelligence preserved in Exec + Detail markup", () => {
  const html = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
  assert.match(html, /aivThemePosition/); // Portfolio Snapshot
  assert.match(html, /aivDetailThemeDecision/); // Decision Patterns
  assert.match(html, /aivDetailIntentCoverage/);
  assert.match(html, /aivDetailThemeAiVs/);
  assert.match(html, /aivDetailReviewItems/);
  assert.match(html, /aivExecDiscoverability/);
  assert.match(html, /aivDetailDiscoverability/);
  // No duplicate HDV portfolio table
  assert.ok(!/aivHdvPortfolioTable/.test(html));
});

test("public HDV route retired; internal service file remains", () => {
  assert.ok(
    !BRAND_AI_VISIBILITY_EXPECTED_ROUTES.some((r) =>
      r.path.includes("hotel-decision-visibility")
    )
  );
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  assert.ok(
    !/app\.get\(\s*"\/api\/ai-visibility\/brand\/hotel-decision-visibility"/.test(server)
  );
  assert.ok(
    fs.existsSync(path.join(root, "lib/ai-visibility/hotel-decision-visibility.js"))
  );
});

test("positive recommendation roles — explicit set", () => {
  assert.deepEqual([...POSITIVE_RECOMMENDATION_ROLES], [
    "first_recommendation",
    "ranked_recommendation",
    "explicit_recommendation",
  ]);
  assert.equal(isPositiveRecommendationRole("associated_option"), false);
  assert.equal(isPositiveRecommendationRole("comparator"), false);
  assert.equal(isPositiveRecommendationRole("discussed"), false);
  assert.equal(isPositiveRecommendationRole("passing_mention"), false);
  assert.equal(isPositiveRecommendationRole("first_recommendation"), true);
});

test("Recommendation Rate: roles, dedupe, exclusions", () => {
  const observations = [
    buildObservationFromExtractions({
      observationId: "1",
      promptId: "p1",
      success: true,
      mentions: [
        { canonicalEntityId: "brandA", role: "explicit_recommendation", recommendationPosition: 2 },
        { canonicalEntityId: "brandA", role: "ranked_recommendation", recommendationPosition: 2 },
      ],
    }),
    buildObservationFromExtractions({
      observationId: "2",
      promptId: "p2",
      success: true,
      mentions: [{ canonicalEntityId: "brandA", role: "first_recommendation", recommendationPosition: 1 }],
    }),
    buildObservationFromExtractions({
      observationId: "3",
      promptId: "p3",
      success: true,
      mentions: [{ canonicalEntityId: "brandA", role: "associated_option" }],
    }),
    buildObservationFromExtractions({
      observationId: "4",
      promptId: "p4",
      success: true,
      mentions: [{ canonicalEntityId: "brandA", role: "comparator" }],
    }),
    buildObservationFromExtractions({
      observationId: "5",
      promptId: "p5",
      success: false,
      mentions: [{ canonicalEntityId: "brandA", role: "first_recommendation", recommendationPosition: 1 }],
    }),
  ];
  const rate = computeRecommendationRate(observations, "brandA");
  assert.equal(rate.numerator, 2); // obs1 + obs2; associated/comparator excluded; failed excluded; no double-count
  assert.equal(rate.denominator, 4);
  assert.equal(rate.eligibilityModel, "cohort_level");
});

test("Top-3 Recommendation Rate: 1–3 count; 4+ and unranked do not", () => {
  const observations = [
    buildObservationFromExtractions({
      observationId: "a",
      promptId: "p",
      success: true,
      mentions: [{ canonicalEntityId: "B", role: "first_recommendation", recommendationPosition: 1 }],
    }),
    buildObservationFromExtractions({
      observationId: "b",
      promptId: "p",
      success: true,
      mentions: [{ canonicalEntityId: "B", role: "ranked_recommendation", recommendationPosition: 3 }],
    }),
    buildObservationFromExtractions({
      observationId: "c",
      promptId: "p",
      success: true,
      mentions: [{ canonicalEntityId: "B", role: "ranked_recommendation", recommendationPosition: 4 }],
    }),
    buildObservationFromExtractions({
      observationId: "d",
      promptId: "p",
      success: true,
      mentions: [{ canonicalEntityId: "B", role: "explicit_recommendation" }], // unranked
    }),
  ];
  const top3 = computeTop3RecommendationRate(observations, "B");
  const rec = computeRecommendationRate(observations, "B");
  assert.equal(top3.numerator, 2);
  assert.equal(top3.denominator, 4);
  assert.equal(rec.numerator, 4); // unranked still counts for Rec Rate
  assert.ok(rec.numerator > top3.numerator);
});

test("Recommendation Share is sole share metric (no Competitive Rec Share product name)", () => {
  const share = computeRecommendationShare(
    [
      buildObservationFromExtractions({
        observationId: "1",
        promptId: "p",
        success: true,
        mentions: [
          { canonicalEntityId: "A", role: "first_recommendation", recommendationPosition: 1 },
          { canonicalEntityId: "C", role: "ranked_recommendation", recommendationPosition: 2 },
        ],
      }),
    ],
    "A"
  );
  assert.equal(share.metric, "recommendation_share");
  assert.match(share.note || "", /Do not expose a separate Competitive Recommendation Share/);
  const html = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
  assert.ok(!/Competitive Recommendation Share/.test(html));
});

test("First Recommendation Rate still consistent", () => {
  const observations = [
    buildObservationFromExtractions({
      observationId: "1",
      promptId: "p",
      success: true,
      mentions: [
        { canonicalEntityId: "A", role: "first_recommendation", recommendationPosition: 1 },
        { canonicalEntityId: "B", role: "ranked_recommendation", recommendationPosition: 2 },
      ],
    }),
    buildObservationFromExtractions({
      observationId: "2",
      promptId: "p",
      success: true,
      mentions: [{ canonicalEntityId: "B", role: "first_recommendation", recommendationPosition: 1 }],
    }),
  ];
  const first = computeFirstRecommendationRate(observations, "A");
  assert.equal(first.numerator, 1);
  assert.equal(first.denominator, 2);
});

test("metric version remains additive ai_visibility_metrics_v1", () => {
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
});

test("discoverability placeholders — no synthetic zeros", () => {
  const exec = buildOpenAiDiscoverabilityExecutivePlaceholder();
  const detail = buildOpenAiDiscoverabilityDetailPlaceholder();
  assert.equal(OPENAI_DISCOVERABILITY_STATUS.SYNTHETIC_VALUES_DISPLAYED, false);
  assert.equal(exec.CRAWL_DATA_CONNECTED, false);
  assert.equal(exec.ANALYTICS_CONNECTED, false);
  assert.equal(exec.title, "AI Discoverability");
  assert.equal(detail.title, "AI Discoverability");
  for (const m of exec.technical.metrics) {
    assert.equal(m.value, null);
    assert.ok(!/^0/.test(String(m.display)));
    assert.match(String(m.display), /connection required/i);
  }
  for (const m of detail.aiOriginatedBusinessImpact.metrics) {
    assert.equal(m.value, null);
    assert.equal(m.brandActionRequired, false);
    assert.match(String(m.display), /connection required/i);
  }
  assert.match(String(exec.comingLaterNote), /Coming later — Dealality build-out/i);
  assert.match(String(detail.comingLaterNote), /Coming later — Dealality build-out/i);
  assert.match(String(detail.technicalCrawlVisibility.helper), /Dealality will add/i);
});

test("trend copy avoids fake 30d/90d in client", () => {
  const js = fs.readFileSync(
    path.join(root, "public/js/ai-visibility/ai-visibility-brand.js"),
    "utf8"
  );
  assert.ok(!/30-Day Change|90-Day Change|vs prior 30 days/i.test(js));
  assert.match(js, /vs prior comparable monitoring run/);
});

test("provider purity — no All AI blend helpers in new metrics", () => {
  const metricsSrc = fs.readFileSync(path.join(root, "lib/ai-visibility/metrics.js"), "utf8");
  assert.ok(!/All AI|all_ai|blendProviders/i.test(metricsSrc));
});

console.log(`\nPhase 3A.4 results: ${passed} passed, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
