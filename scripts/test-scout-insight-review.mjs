/**
 * Scout Phase 5C — Insight calibration & evidence review tests (read-only).
 *
 * Usage: node scripts/test-scout-insight-review.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildMarketInsightsReport,
  buildInsightReviewReport,
} from "../lib/scout/market-insights.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertCalibratedShape(ins) {
  const required = [
    "insightQuality",
    "evidenceSummary",
    "evidenceItems",
    "dataGaps",
    "confidenceExplanation",
    "priorityExplanation",
    "commercialInterpretation",
    "suggestedReviewQuestions",
    "relatedHotelExamples",
    "relatedDemandDriverExamples",
    "relatedSignalExamples",
    "suggestedReviewAction",
  ];
  for (const k of required) assert(k in ins, `calibrated insight missing ${k}`);
  assert(
    ["Strong", "Directional", "Weak", "Suppressed"].includes(ins.insightQuality),
    `invalid insightQuality ${ins.insightQuality}`
  );
  assert(Array.isArray(ins.evidenceItems), "evidenceItems must be array");
  for (const e of ins.evidenceItems) {
    assert(e.evidenceType, "evidenceType required");
    assert(e.label != null, "evidence label required");
    assert(["High", "Medium", "Low"].includes(e.confidence), `bad evidence confidence ${e.confidence}`);
  }
}

function checkRegression() {
  const appJs = fs.readFileSync(path.join(ROOT, "public", "app.js"), "utf8");
  assert(
    appJs.includes("'/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' }"),
    "opportunity-radar unchanged"
  );
  const radar = fs.readFileSync(
    path.join(ROOT, "public", "deal-capture-radar-with-ranked-list.html"),
    "utf8"
  );
  assert(radar.includes("map-legend"), "radar file unchanged structure");
  const be = fs.readFileSync(path.join(ROOT, "public", "brand-explorer-combined.html"), "utf8");
  assert(be.length > 100, "brand explorer exists");
}

function assertNoWrites(filePath) {
  const src = fs.readFileSync(path.join(ROOT, filePath), "utf8");
  assert(!src.includes(".create("), `${filePath} must not create`);
  assert(!src.includes(".update("), `${filePath} must not update`);
  assert(!src.includes(".destroy("), `${filePath} must not delete`);
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
    process.exit(1);
  }

  console.log("=== Scout Insight Review tests (Phase 5C) ===\n");
  let passed = 0;

  console.log("Regression...");
  checkRegression();
  console.log("  PASS\n");
  passed++;

  console.log("GET insight-review Mexico + Mexican Caribbean...");
  const reviewMx = await buildInsightReviewReport({
    country: "Mexico",
    market: "Mexican Caribbean",
    includeDemandOverlays: "1",
  });
  assert(reviewMx.ok, reviewMx.error || "failed");
  assert(reviewMx.source.readOnly === true && reviewMx.source.writes === false);
  assert(Array.isArray(reviewMx.insightReviews));
  assert(reviewMx.summary.insightsReviewed >= 0);
  assert("strongInsights" in reviewMx.summary);
  assert("directionalInsights" in reviewMx.summary);
  assert("weakInsights" in reviewMx.summary);
  assert("suppressedInsights" in reviewMx.summary);
  if (reviewMx.insightReviews.length) {
    reviewMx.insightReviews.forEach(assertCalibratedShape);
  }
  console.log("  reviewed:", reviewMx.summary.insightsReviewed);
  console.log("  PASS\n");
  passed++;

  console.log("Playa Del Carmen + Choice parent...");
  const choiceReview = await buildInsightReviewReport({
    country: "Mexico",
    market: "Mexican Caribbean",
    submarket: "Playa Del Carmen",
    parentCompany: "Choice Hotels International, Inc.",
    includeDemandOverlays: "1",
  });
  assert(choiceReview.ok, choiceReview.error);
  const parentReview = choiceReview.insightReviews.find(
    (i) => i.insightType === "parent_company_underrepresentation" && i.insightQuality !== "Suppressed"
  );
  assert(parentReview, "expected calibrated parent underrepresentation insight");
  assert(parentReview.evidenceItems.length > 0, "expected evidenceItems");
  assert(parentReview.suggestedReviewQuestions.length >= 3, "expected review questions");
  console.log("  quality:", parentReview.insightQuality);
  console.log("  evidence items:", parentReview.evidenceItems.length);
  console.log("  PASS\n");
  passed++;

  console.log("market-insights includeInsightReview=1...");
  const mxInsights = await buildMarketInsightsReport({
    country: "Mexico",
    includeInsightReview: "1",
    includeDemandOverlays: "1",
    limit: 50,
  });
  assert(mxInsights.ok, mxInsights.error);
  assert(mxInsights.insightQualitySummary != null);
  assert(mxInsights.dataQualityNotes != null);
  assert(mxInsights.suppressedInsightCount != null);
  assert(Array.isArray(mxInsights.insightReviews));
  if (mxInsights.insights.length) {
    assert(mxInsights.insights[0].insightQuality, "insights should be calibrated");
    assert(mxInsights.insights[0].evidenceItems, "insights should have evidenceItems");
  }
  console.log("  PASS\n");
  passed++;

  console.log("includeSuppressed=1...");
  const suppressed = await buildInsightReviewReport({
    country: "Mexico",
    market: "Mexican Caribbean",
    parentCompany: "Choice Hotels International, Inc.",
    submarket: "Playa Del Carmen",
    includeSuppressed: "1",
    includeDemandOverlays: "1",
  });
  assert(suppressed.ok, suppressed.error);
  assert(Array.isArray(suppressed.suppressedInsights));
  console.log("  suppressed count:", suppressed.summary.suppressedInsights);
  console.log("  PASS\n");
  passed++;

  console.log("Country-only data gaps...");
  const broad = await buildInsightReviewReport({ country: "Mexico", includeDemandOverlays: "0" });
  assert(broad.ok, broad.error);
  const hasGeoGap = broad.insightReviews.some((i) =>
    (i.dataGaps || []).some((g) => /Market not specified/i.test(g.label))
  );
  assert(hasGeoGap || (broad.dataQualityNotes || []).length > 0, "expected geography data quality notes");
  console.log("  PASS\n");
  passed++;

  console.log("No writes in calibration modules...");
  assertNoWrites("lib/scout/market-insights.js");
  assertNoWrites("lib/scout/insight-calibration.js");
  assertNoWrites("lib/scout/demand-overlays.js");
  assertNoWrites("api/scout-insight-review.js");
  console.log("  PASS\n");
  passed++;

  if (parentReview) {
    console.log("Example calibrated parent insight:");
    console.log(
      JSON.stringify(
        {
          insightId: parentReview.insightId,
          insightType: parentReview.insightType,
          insightQuality: parentReview.insightQuality,
          evidenceSummary: parentReview.evidenceSummary,
          evidenceItems: parentReview.evidenceItems.slice(0, 3),
          dataGaps: parentReview.dataGaps,
          suggestedReviewAction: parentReview.suggestedReviewAction,
        },
        null,
        2
      )
    );
  }

  console.log(`\n=== ${passed} checks passed ===`);
}

main().catch((err) => {
  console.error("\nFAIL:", err.message);
  process.exit(1);
});
