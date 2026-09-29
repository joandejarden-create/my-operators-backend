/**
 * Tests: GDI market opportunity graph + NYC cross-hotel parity V1
 *   node scripts/test-gdi-market-opportunity-graph-nyc-parity-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  MARKET_LEVEL,
  classifyOpportunityGeography,
  fanoutDecisionForGeography,
  wouldBlindFanoutToMidtown,
  computeMarketOpportunityId,
  buildNycControlGeographyProfiles,
  evaluateHotelGeographicApplicability,
  GEO_APPLICABILITY,
  extractMarketOpportunityPacket,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/market-opportunity-graph-nyc-parity-v1"
);

// Market hierarchy / geography classification
{
  const ts = classifyOpportunityGeography({
    title: "Theater event",
    destinationStatus: "Times Square",
  });
  assert.equal(ts.level, MARKET_LEVEL.MICRO_AREA);
  assert.match(ts.microArea, /Times Square/i);

  const bk = classifyOpportunityGeography({
    title: "Barclays housing",
    destinationStatus: "Downtown Brooklyn",
  });
  assert.equal(bk.borough, "Brooklyn");
  assert.equal(wouldBlindFanoutToMidtown(bk), false);

  const metro = classifyOpportunityGeography({
    title: "Association meeting",
    destinationStatus: "New York City",
  });
  assert.equal(metro.confidence, "METRO_ONLY");
  const fan = fanoutDecisionForGeography(metro);
  assert.equal(fan.autoEvaluateMetroWide, false);
}

// Brooklyn safeguard: Brooklyn-only must NOT auto-evaluate Midtown
{
  const bk = classifyOpportunityGeography({
    title: "Williamsburg tournament housing",
    destinationStatus: "Williamsburg, Brooklyn",
  });
  assert.equal(wouldBlindFanoutToMidtown(bk), false);
  const fan = fanoutDecisionForGeography(bk);
  assert.equal(fan.autoEvaluateMetroWide, false);
}

// Shared market opportunity identity stable
{
  const a = computeMarketOpportunityId({
    organizationName: "SSSP",
    eventName: "76th Annual Meeting",
    eventStartDate: "2027-08-01",
    venueOrLocation: "Times Square",
  });
  const b = computeMarketOpportunityId({
    organizationName: "SSSP",
    eventName: "76th Annual Meeting",
    eventStartDate: "2027-08-01",
    venueOrLocation: "Times Square",
  });
  assert.equal(a, b);
  assert.match(a, /^gdi_mkt_/);
}

// Hotel geography profiles
{
  const geo = buildNycControlGeographyProfiles();
  assert.match(geo.RENAISSANCE.submarket, /Times Square/i);
  assert.match(geo.HILTON.submarket, /Times Square/i);
  assert.match(geo.NOW_NOW.submarket, /NoHo/i);
  assert.equal(geo.RENAISSANCE.borough, "Manhattan");
  assert.ok(geo.HILTON.lat);
  assert.ok(geo.NOW_NOW.lat);
}

// Cross-hotel: ignore locked territory from seed hotel
{
  const geo = buildNycControlGeographyProfiles();
  const opp = {
    title: "76th Annual Meeting",
    destinationStatus: "New York City / Times Square",
    demandTerritoryFitLocked: true,
    demandTerritoryFit: "TERRITORY_CORE",
    demandTerritoryRationale: "Renaissance-specific lock",
  };
  const nowApp = evaluateHotelGeographicApplicability(opp, geo.NOW_NOW);
  assert.notEqual(nowApp.applicability, GEO_APPLICABILITY.DIRECT);
  const hilApp = evaluateHotelGeographicApplicability(opp, geo.HILTON);
  assert.ok(
    hilApp.applicability === GEO_APPLICABILITY.DIRECT ||
      hilApp.applicability === GEO_APPLICABILITY.STRONG
  );
}

// Packet separates hotel-specific fields
{
  const pkt = extractMarketOpportunityPacket({
    id: "gdi_opp_x",
    title: "Event",
    organizationName: "Org Association",
    hotelFitScore: 99,
    summaryWhyHotel: "Because Renaissance",
    recommendedAction: "Call Ren sales",
    eventStartDate: "2027-01-01",
    destinationStatus: "Times Square",
  });
  assert.ok(pkt.marketOpportunityId);
  assert.equal(pkt.hotelSpecificExcluded.hotelFitScore, 99);
  assert.ok(!("summaryWhyHotel" in pkt) || pkt.summaryWhyHotel === undefined);
}

// Report artifact expectations (run script first)
{
  assert.ok(fs.existsSync(path.join(OUT, "RUN_SUMMARY.json")), "run parity script first");
  const summary = JSON.parse(fs.readFileSync(path.join(OUT, "RUN_SUMMARY.json"), "utf8"));
  assert.equal(summary.counts.renaissanceReady, 11);
  assert.equal(summary.counts.hiltonReadyBefore, 0);
  assert.equal(summary.customerMutation, false);
  assert.equal(summary.rootCause.NOT_EVALUATED_FOR_HILTON, 11);
  assert.ok(summary.counts.hiltonReadyAfterShadow >= 1);
  const brooklyn = JSON.parse(
    fs.readFileSync(path.join(OUT, "MARKET_BOUNDARY_TESTS.json"), "utf8")
  );
  // Product expectation: Brooklyn does not automatically reach Midtown
  assert.equal(brooklyn.brooklynSafeguard.fanout.autoEvaluateMetroWide, false);
}

console.log("test:gdi-market-opportunity-graph-nyc-parity-v1 OK");
