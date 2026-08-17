/**
 * Read-only checks for Scout market coverage intelligence.
 *
 * Usage: node scripts/test-scout-market-coverage.mjs
 */
import "../load-env.js";
import { buildMarketCoverageReport, SCOUT_STR_GEOGRAPHY_MAPPING } from "../lib/scout/market-coverage.js";

const CASES = [
  {
    label: "Choice Hotels International, Inc.",
    query: { parentCompany: "Choice Hotels International, Inc.", includePipeline: "1" },
    expectMinOpenHotels: 1,
  },
  {
    label: "Marriott International",
    query: { parentCompany: "Marriott International", includePipeline: "1" },
    expectMinOpenHotels: 1,
  },
  {
    label: "Colombia",
    query: { country: "Colombia", includePipeline: "1" },
    expectMinOpenHotels: 1,
  },
  {
    label: "Mexico",
    query: { country: "Mexico", includePipeline: "1" },
    expectMinOpenHotels: 1,
  },
  {
    label: "Courtyard by Marriott (brand alias)",
    query: { brand: "Courtyard by Marriott", parentCompany: "Marriott International", includePipeline: "1" },
    expectMinOpenHotels: 1,
    expectAlias: true,
  },
];

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertNoStrFieldMissingWarnings(warnings) {
  const bad = (warnings || []).filter((w) =>
    /STR_MARKET_FIELDS_MISSING|STR_MARKET_FALLBACK|STR_SUBMARKET_SPARSE/i.test(w)
  );
  assert(bad.length === 0, `unexpected STR field warnings: ${bad.join("; ")}`);
}

function assertFieldMapping(source) {
  assert(source?.fieldMapping?.strMarket === "Market", "fieldMapping.strMarket should be Market");
  assert(source?.fieldMapping?.strSubmarket === "Submarket", "fieldMapping.strSubmarket should be Submarket");
  assert(source?.fieldMapping?.market === "Market", "fieldMapping.market should be Market");
  assert(source?.fieldMapping?.submarket === "Submarket", "fieldMapping.submarket should be Submarket");
  assert(
    JSON.stringify(source.fieldMapping) === JSON.stringify(SCOUT_STR_GEOGRAPHY_MAPPING),
    "source.fieldMapping should match SCOUT_STR_GEOGRAPHY_MAPPING"
  );
}

async function discoverStrMarket() {
  const report = await buildMarketCoverageReport({ country: "Mexico", includePipeline: "1" });
  const top = report.breakdowns?.bySTRMarket?.find((r) => r.label && r.label !== "Unknown");
  return top?.label || null;
}

async function discoverSubmarket(country, market) {
  const report = await buildMarketCoverageReport({ country, strMarket: market, includePipeline: "1" });
  const top = report.breakdowns?.bySTRSubmarket?.find((r) => r.label && r.label !== "Unknown");
  return top?.label || null;
}

async function runCase(testCase) {
  console.log("\n---", testCase.label, "---");
  const report = await buildMarketCoverageReport(testCase.query);

  assert(report.ok, report.error || "report failed");
  assert(report.source?.readOnly === true, "expected readOnly source");
  assert(report.source?.writes === false, "expected no writes");
  assertFieldMapping(report.source);
  assertNoStrFieldMissingWarnings(report.warnings);

  console.log("metrics", report.metrics);
  console.log("fieldMapping", report.source.fieldMapping);
  console.log("whiteSpace count", report.whiteSpace?.length ?? 0);
  console.log("warnings", report.warnings);

  if (testCase.expectMinOpenHotels != null) {
    assert(
      report.metrics.openHotels >= testCase.expectMinOpenHotels,
      `expected >= ${testCase.expectMinOpenHotels} open hotels, got ${report.metrics.openHotels}`
    );
  }

  if (testCase.expectAlias) {
    assert(report.filters?.brandResolution?.usedAliasTable, "expected brand alias table resolution");
  }

  const requiredMetrics = [
    "openHotels",
    "openRooms",
    "pipelineHotels",
    "pipelineRooms",
    "brandedHotels",
    "independentHotels",
    "parentCompanyCount",
    "brandCount",
    "countryCount",
    "cityCount",
    "strMarketCount",
    "strSubmarketCount",
  ];
  for (const key of requiredMetrics) {
    assert(typeof report.metrics[key] === "number", `missing metric ${key}`);
  }

  const requiredBreakdowns = [
    "byParentCompany",
    "byBrand",
    "byCountry",
    "byCity",
    "bySTRMarket",
    "bySTRSubmarket",
    "byChainScale",
    "byLocationType",
    "byStatus",
    "byProjectPhase",
  ];
  for (const key of requiredBreakdowns) {
    assert(Array.isArray(report.breakdowns?.[key]), `missing breakdown ${key}`);
  }
}

async function testStrMarketAliasFilter(marketLabel) {
  console.log("\n--- strMarket vs market alias filter ---");
  const byStrMarket = await buildMarketCoverageReport({
    country: "Mexico",
    strMarket: marketLabel,
    includePipeline: "1",
  });
  const byMarket = await buildMarketCoverageReport({
    country: "Mexico",
    market: marketLabel,
    includePipeline: "1",
  });

  assert(byStrMarket.ok && byMarket.ok, "alias filter reports failed");
  assertNoStrFieldMissingWarnings(byStrMarket.warnings);
  assert(
    byStrMarket.metrics.openHotels === byMarket.metrics.openHotels,
    `strMarket (${byStrMarket.metrics.openHotels}) and market (${byMarket.metrics.openHotels}) should match for ${marketLabel}`
  );
  console.log("PASS: strMarket and market return same openHotels", byStrMarket.metrics.openHotels);
}

async function testStrSubmarketAliasFilter(marketLabel, submarketLabel) {
  console.log("\n--- strSubmarket vs submarket alias filter ---");
  const byStrSubmarket = await buildMarketCoverageReport({
    country: "Mexico",
    strMarket: marketLabel,
    strSubmarket: submarketLabel,
    includePipeline: "1",
  });
  const bySubmarket = await buildMarketCoverageReport({
    country: "Mexico",
    market: marketLabel,
    submarket: submarketLabel,
    includePipeline: "1",
  });

  assert(byStrSubmarket.ok && bySubmarket.ok, "submarket alias filter reports failed");
  assertNoStrFieldMissingWarnings(byStrSubmarket.warnings);
  assert(
    byStrSubmarket.metrics.openHotels === bySubmarket.metrics.openHotels,
    `strSubmarket (${byStrSubmarket.metrics.openHotels}) and submarket (${bySubmarket.metrics.openHotels}) should match`
  );
  console.log("PASS: strSubmarket and submarket return same openHotels", byStrSubmarket.metrics.openHotels);
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT in .env");
    process.exit(1);
  }

  const strMarket = await discoverStrMarket();
  if (strMarket) {
    CASES.push({
      label: `STR Market: ${strMarket}`,
      query: { strMarket, includePipeline: "1" },
      expectMinOpenHotels: 1,
    });
  } else {
    console.warn("No populated Market found in Mexico sample; skipping STR market case.");
  }

  let failed = 0;
  for (const c of CASES) {
    try {
      await runCase(c);
      console.log("PASS:", c.label);
    } catch (e) {
      failed += 1;
      console.error("FAIL:", c.label, e.message);
    }
  }

  if (strMarket) {
    try {
      await testStrMarketAliasFilter(strMarket);
    } catch (e) {
      failed += 1;
      console.error("FAIL: strMarket alias filter", e.message);
    }

    const submarket = await discoverSubmarket("Mexico", strMarket);
    if (submarket) {
      try {
        await testStrSubmarketAliasFilter(strMarket, submarket);
      } catch (e) {
        failed += 1;
        console.error("FAIL: strSubmarket alias filter", e.message);
      }
    } else {
      console.warn(`No Submarket found for ${strMarket}; skipping submarket alias test.`);
    }
  }

  console.log("\n--- sample source metadata ---");
  const sample = await buildMarketCoverageReport({ country: "Mexico", includePipeline: "1" });
  console.log(JSON.stringify({ fieldMapping: sample.source?.fieldMapping, strGeographyNote: sample.source?.strGeographyNote }, null, 2));

  process.exit(failed > 0 ? 1 : 0);
}

main();
