/**
 * Scout Market Map API tests (read-only Hotel Census + signals; no Radar/Brand Explorer writes).
 *
 * Usage: node scripts/test-scout-market-map.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildMarketMapReport, isValidCoordinate } from "../lib/scout/market-map.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertHotelMarkerShape(marker) {
  const required = [
    "markerId",
    "markerType",
    "airtableRecordId",
    "hotelName",
    "latitude",
    "longitude",
    "source",
  ];
  for (const key of required) assert(key in marker, `hotel marker missing ${key}`);
  assert(marker.markerType === "hotel", "expected hotel markerType");
  assert(marker.source === "Hotel Census", "hotel source must be Hotel Census");
  assert(isValidCoordinate(marker.latitude, marker.longitude), "invalid hotel coordinates");
}

function assertSignalMarkerShape(marker) {
  assert(marker.markerType === "generated_signal", "expected generated_signal");
  assert(marker.signalId, "signal marker missing signalId");
  if (marker.latitude != null) {
    assert(isValidCoordinate(marker.latitude, marker.longitude), "invalid signal coordinates");
  }
}

function assertClusterShape(cluster) {
  const required = [
    "clusterId",
    "country",
    "market",
    "submarket",
    "signalCount",
    "representativeLatitude",
    "representativeLongitude",
    "coordinateSource",
  ];
  for (const key of required) assert(key in cluster, `cluster missing ${key}`);
  assert(
    cluster.coordinateSource === "centroid_from_hotels" ||
      cluster.coordinateSource === "missing_coordinates",
    "unexpected coordinateSource"
  );
}

const CASES = [
  {
    label: "Mexico + signals + saved",
    query: {
      country: "Mexico",
      includeSignals: "1",
      includeSavedSignals: "1",
      includePipeline: "1",
      limit: 500,
    },
    expectMinHotels: 1,
  },
  {
    label: "Market Mexican Caribbean",
    query: {
      market: "Mexican Caribbean",
      includeSignals: "1",
      includePipeline: "1",
      limit: 500,
    },
    expectMinHotels: 1,
  },
  {
    label: "Choice parent company Mexico",
    query: {
      parentCompany: "Choice Hotels International, Inc.",
      country: "Mexico",
      includeSignals: "1",
      includePipeline: "1",
      limit: 500,
    },
    expectMinHotels: 0,
  },
];

function checkOpportunityRadarUnchanged() {
  const appJs = fs.readFileSync(path.join(ROOT, "public", "app.js"), "utf8");
  assert(
    appJs.includes("'/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' }"),
    "opportunity-radar route must remain unchanged"
  );
  assert(
    appJs.includes("'/scout-market-map'"),
    "scout-market-map route should be registered"
  );

  const radarHtml = path.join(ROOT, "public", "deal-capture-radar-with-ranked-list.html");
  assert(fs.existsSync(radarHtml), "Opportunity Radar HTML must exist");

  const scoutJs = fs.readFileSync(path.join(ROOT, "public", "js", "scout-market-map.js"), "utf8");
  assert(
    !scoutJs.includes("opportunity-radar") && !scoutJs.includes("deal-capture-radar"),
    "scout-market-map.js must not reference Opportunity Radar modules"
  );
  assert(
    scoutJs.includes("scout_market_map_filters_v1"),
    "scout page must use scout-specific localStorage key"
  );
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  console.log("=== Scout Market Map tests ===\n");
  let passed = 0;

  console.log("Regression: Opportunity Radar unchanged...");
  checkOpportunityRadarUnchanged();
  console.log("  PASS — /opportunity-radar route intact; separate scout page\n");
  passed += 1;

  for (const testCase of CASES) {
    console.log(`Case: ${testCase.label}`);
    const report = await buildMarketMapReport(testCase.query);
    assert(report.ok, report.error || "buildMarketMapReport failed");

    assert(report.source.readOnly === true, "source.readOnly must be true");
    assert(report.source.writes === false, "source.writes must be false");
    assert(report.source.hotelSource === "Hotel Census", "hotelSource mismatch");
    assert(report.source.marketField === "Market", "marketField should be Market");
    assert(report.source.submarketField === "Submarket", "submarketField should be Submarket");

    if (testCase.expectMinHotels != null) {
      assert(
        report.summary.hotelMarkers >= testCase.expectMinHotels,
        `expected >= ${testCase.expectMinHotels} hotel markers, got ${report.summary.hotelMarkers}`
      );
    }

    for (const m of report.hotelMarkers) {
      assertHotelMarkerShape(m);
    }

    for (const m of report.signalMarkers) {
      assertSignalMarkerShape(m);
    }

    for (const c of report.marketClusters) {
      assertClusterShape(c);
    }

    const marketLevelSignals = (report.generatedSignals || []).filter(
      (s) => !report.signalMarkers.some((m) => m.signalId === s.signalId)
    );
    if (marketLevelSignals.length > 0) {
      const clusterWithSignals = report.marketClusters.filter((c) => c.signalCount > 0);
      assert(
        clusterWithSignals.length > 0,
        "market/submarket-level signals should appear in marketClusters"
      );
    }

    console.log(
      `  hotels=${report.summary.hotelMarkers} signals=${report.summary.signalMarkers} saved=${report.summary.savedSignalMarkers} clusters=${report.marketClusters.length}`
    );
    console.log("  PASS\n");
    passed += 1;
  }

  console.log("Confirm: no Hotel Census table writes in market-map module...");
  const libSource = fs.readFileSync(path.join(ROOT, "lib", "scout", "market-map.js"), "utf8");
  assert(!libSource.includes(".create("), "market-map must not create census records");
  assert(!libSource.includes(".update("), "market-map must not update census records");
  assert(!libSource.includes(".destroy("), "market-map must not delete census records");
  assert(libSource.includes(HOTEL_CENSUS_TABLE), "reads Hotel Census only");
  console.log("  PASS — read-only census access\n");
  passed += 1;

  console.log(`=== ${passed} checks passed ===`);
}

main().catch((err) => {
  console.error("\nFAIL:", err.message);
  process.exit(1);
});
