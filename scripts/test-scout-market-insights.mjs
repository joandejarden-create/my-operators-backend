/**
 * Scout Phase 5B — Market Insight Engine tests (read-only).
 *
 * Usage: node scripts/test-scout-market-insights.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildMarketInsightsReport, INSIGHT_TYPES } from "../lib/scout/market-insights.js";
import { buildMarketMapReport } from "../lib/scout/market-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertInsightShape(ins) {
  const required = [
    "insightId",
    "insightType",
    "title",
    "geographyLabel",
    "priority",
    "confidence",
    "insightText",
    "whyItMatters",
    "supportingMetrics",
    "recommendedNextStep",
    "evidenceLevel",
  ];
  for (const k of required) assert(k in ins, `insight missing ${k}`);
  assert(INSIGHT_TYPES.includes(ins.insightType), `unknown insightType ${ins.insightType}`);
}

function checkRegression() {
  const appJs = fs.readFileSync(path.join(ROOT, "public", "app.js"), "utf8");
  assert(
    appJs.includes("'/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' }"),
    "opportunity-radar unchanged"
  );
  assert(fs.existsSync(path.join(ROOT, "public", "app", "scout-market-map.html")));
  const be = fs.readFileSync(path.join(ROOT, "public", "brand-explorer-combined.html"), "utf8");
  assert(be.length > 100, "brand explorer file exists");
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

  console.log("=== Scout Market Insights tests (Phase 5B) ===\n");
  let passed = 0;

  console.log("Regression...");
  checkRegression();
  console.log("  PASS\n");
  passed++;

  console.log("Mexico + Mexican Caribbean + overlays...");
  const mx = await buildMarketInsightsReport({
    country: "Mexico",
    market: "Mexican Caribbean",
    includeDemandOverlays: "1",
    includeSavedSignals: "1",
    limit: 100,
  });
  assert(mx.ok, mx.error || "failed");
  assert(mx.source.readOnly === true && mx.source.writes === false);
  mx.insights.forEach(assertInsightShape);
  console.log("  insights:", mx.summary.insightsReturned, "| ranked:", mx.rankedOpportunities.length);
  console.log("  PASS\n");
  passed++;

  console.log("Playa Del Carmen + Choice parent...");
  const choice = await buildMarketInsightsReport({
    country: "Mexico",
    market: "Mexican Caribbean",
    submarket: "Playa Del Carmen",
    parentCompany: "Choice Hotels International, Inc.",
    includeDemandOverlays: "1",
    limit: 100,
  });
  assert(choice.ok, choice.error);
  const parentInsight = choice.insights.find(
    (i) => i.insightType === "parent_company_underrepresentation"
  );
  assert(parentInsight, "expected parent_company_underrepresentation for Choice with 0 hotels");
  console.log("  parent insight:", parentInsight.title);
  console.log("  PASS\n");
  passed++;

  console.log("Courtyard + Marriott Mexico...");
  const brand = await buildMarketInsightsReport({
    brand: "Courtyard by Marriott",
    parentCompany: "Marriott International",
    country: "Mexico",
    limit: 100,
  });
  assert(brand.ok, brand.error);
  const brandInsight = brand.insights.find((i) => i.insightType === "brand_underrepresentation");
  console.log("  brand insight:", brandInsight?.title || "(none — brand may have national presence)");
  console.log("  PASS\n");
  passed++;

  console.log("Independent conversion (Mexico)...");
  const ind = await buildMarketInsightsReport({ country: "Mexico", limit: 50 });
  assert(ind.ok, ind.error);
  if ((ind.summary.independentHotels || 0) >= 15) {
    assert(
      ind.insights.some((i) => i.insightType === "independent_conversion_potential"),
      "expected independent_conversion_potential"
    );
  }
  console.log("  independents:", ind.summary.independentHotels);
  console.log("  PASS\n");
  passed++;

  console.log("market-map includeInsights=1...");
  const map = await buildMarketMapReport({
    country: "Mexico",
    includeInsights: "1",
    includeSignals: "0",
    includeSavedSignals: "0",
    includeDemandOverlays: "0",
    limit: 50,
  });
  assert(map.ok, map.error);
  assert(Array.isArray(map.insights));
  assert(map.insightSummary != null);
  console.log("  insights:", map.insights.length);
  console.log("  PASS\n");
  passed++;

  console.log("No writes in insight modules...");
  assertNoWrites("lib/scout/market-insights.js");
  assertNoWrites("lib/scout/demand-overlays.js");
  console.log("  PASS\n");
  passed++;

  if (parentInsight) {
    console.log("Example parent insight:");
    console.log(JSON.stringify(parentInsight, null, 2));
  }
  if (brandInsight) {
    console.log("\nExample brand insight:");
    console.log(JSON.stringify(brandInsight, null, 2));
  }
  if (mx.rankedOpportunities[0]) {
    console.log("\nExample ranked opportunity:");
    console.log(JSON.stringify(mx.rankedOpportunities[0], null, 2));
  }

  console.log(`\n=== ${passed} checks passed ===`);
}

main().catch((err) => {
  console.error("\nFAIL:", err.message);
  process.exit(1);
});
