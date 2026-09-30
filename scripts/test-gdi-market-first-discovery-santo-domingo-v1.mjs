/**
 * Tests: GDI Market-First Discovery Santo Domingo V1
 *   node scripts/test-gdi-market-first-discovery-santo-domingo-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SANTO_DOMINGO_HOTELS,
  santoDomingoMarketHints,
  buildSantoDomingoMarketQueries,
  isSantoDomingoInMarket,
  santoDomingoMarketDevelopmentState,
  buildSantoDomingoGeographyProfiles,
  classifyOpportunityGeography,
  evaluateHotelGeographicApplicability,
  computeMarketOpportunityId,
  GEO_APPLICABILITY,
  MARKET_LEVEL,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";
import { decideMarketEvidenceNextAction } from "../lib/group-demand-intelligence/market-opportunity-graph/market-first-discovery-santo-domingo-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/market-first-discovery-santo-domingo-v1"
);

function loadJson(name) {
  return JSON.parse(fs.readFileSync(path.join(OUT, name), "utf8"));
}

// Market-first target generation
{
  const qs = buildSantoDomingoMarketQueries({ max: 40 });
  assert.ok(qs.length >= 15);
  assert.ok(qs.every((q) => q.family && q.query));
  assert.ok(qs.some((q) => /Santo Domingo/i.test(q.query)));
  assert.ok(qs.some((q) => /alojamiento|hotel oficial|housing/i.test(q.query)));
  // Not hotel-branched JW/Radisson duplicate universes
  assert.ok(!qs.some((q) => /JW Marriott|Radisson Hotel Santo Domingo/i.test(q.query)));
}

// Santo Domingo geography + Piantini vs Naco
{
  const hints = santoDomingoMarketHints();
  const profiles = buildSantoDomingoGeographyProfiles();
  assert.equal(profiles.JW.hotelId, SANTO_DOMINGO_HOTELS.JW);
  assert.equal(profiles.RADISSON.hotelId, SANTO_DOMINGO_HOTELS.RADISSON);
  assert.match(profiles.JW.submarket, /Piantini/i);
  assert.match(profiles.RADISSON.submarket, /Naco/i);
  assert.notEqual(profiles.JW.microArea, profiles.RADISSON.microArea);
  assert.equal(profiles.JW.metro, "Santo Domingo");
  assert.notEqual(profiles.JW.borough, "Manhattan");

  const piantini = classifyOpportunityGeography(
    { destinationStatus: "Santo Domingo / Piantini / Blue Mall", title: "Congreso" },
    hints
  );
  assert.equal(piantini.submarket, "Piantini / Blue Mall");
  assert.equal(piantini.confidence, "SPECIFIC");

  const naco = classifyOpportunityGeography(
    { destinationStatus: "Naco Tiradentes Santo Domingo", title: "Foro" },
    hints
  );
  assert.equal(naco.submarket, "Naco / Tiradentes");

  const metro = classifyOpportunityGeography(
    { destinationStatus: "Santo Domingo", title: "Evento" },
    hints
  );
  assert.equal(metro.confidence, "METRO_ONLY");
}

// Same-metro no blind fanout
{
  const hints = santoDomingoMarketHints();
  const profiles = buildSantoDomingoGeographyProfiles();
  const metroOpp = {
    title: "Congreso Nacional 2027",
    destinationStatus: "Santo Domingo",
  };
  const jw = evaluateHotelGeographicApplicability(metroOpp, profiles.JW, {
    marketHints: hints,
  });
  const rad = evaluateHotelGeographicApplicability(metroOpp, profiles.RADISSON, {
    marketHints: hints,
  });
  assert.equal(jw.applicability, GEO_APPLICABILITY.UNKNOWN);
  assert.equal(rad.applicability, GEO_APPLICABILITY.UNKNOWN);

  const piantiniOpp = {
    title: "Gala Piantini",
    destinationStatus: "Piantini Blue Mall Santo Domingo",
  };
  const jwP = evaluateHotelGeographicApplicability(piantiniOpp, profiles.JW, {
    marketHints: hints,
  });
  const radP = evaluateHotelGeographicApplicability(piantiniOpp, profiles.RADISSON, {
    marketHints: hints,
  });
  assert.ok(
    jwP.applicability === GEO_APPLICABILITY.DIRECT ||
      jwP.applicability === GEO_APPLICABILITY.STRONG
  );
  // Radisson may be NEARBY/STRONG or COMPETITIVE/PLAUSIBLE — not DIRECT on Piantini core
  assert.notEqual(radP.applicability, GEO_APPLICABILITY.DIRECT);
  assert.ok(
    [
      GEO_APPLICABILITY.STRONG,
      GEO_APPLICABILITY.PLAUSIBLE,
      GEO_APPLICABILITY.NEARBY,
    ].includes(radP.applicability) ||
      radP.applicability === GEO_APPLICABILITY.STRONG ||
      radP.applicability === GEO_APPLICABILITY.PLAUSIBLE
  );
}

// Shared market opportunity identity
{
  const a = computeMarketOpportunityId({
    organizationName: "Asociación Médica Dominicana",
    eventName: "Congreso Nacional",
    eventYear: "2027",
    venueOrLocation: "Santo Domingo / Piantini",
  });
  const b = computeMarketOpportunityId({
    organizationName: "Asociación Médica Dominicana",
    eventName: "Congreso Nacional",
    eventYear: "2027",
    venueOrLocation: "Santo Domingo / Piantini",
  });
  assert.equal(a, b);
  assert.match(a, /^gdi_mkt_/);
}

// In-market filter
{
  assert.equal(isSantoDomingoInMarket("Punta Cana wedding expo 2027"), false);
  assert.equal(isSantoDomingoInMarket("Congreso Santo Domingo 2027 alojamiento"), true);
}

// Development ladder + Jev market routing
{
  assert.equal(
    santoDomingoMarketDevelopmentState({
      validEntity: true,
      futureCycle: true,
      lodgingEvidence: "DIRECT",
      commercialStatus: "OPEN / UNRESOLVED",
    }),
    "HOTEL_MATCHABLE"
  );
  const actionLoc = decideMarketEvidenceNextAction({
    validEntity: true,
    futureCycle: true,
    lodgingEvidence: "NONE",
    geoConfidence: "METRO_ONLY",
  });
  assert.equal(actionLoc.action, "VERIFY_EVENT_LOCATION");
  const actionLodge = decideMarketEvidenceNextAction({
    validEntity: true,
    futureCycle: true,
    lodgingEvidence: "NONE",
    venueHint: "Santo Domingo / Piantini",
    geoConfidence: "SPECIFIC",
  });
  assert.equal(actionLodge.action, "FIND_OFFICIAL_HOUSING_PAGE");
}

// Report artifacts (from last discovery run)
{
  assert.ok(fs.existsSync(path.join(OUT, "RUN_SUMMARY.json")));
  const summary = loadJson("RUN_SUMMARY.json");
  assert.equal(summary.nycRegression.renaissanceReady, 11);
  assert.equal(summary.nycRegression.hiltonReady, 10);
  assert.equal(summary.customerMutationNyc, false);
  assert.equal(summary.cron, "HELD");
  assert.ok(summary.executive.marketOpportunitiesDiscovered >= 1);

  const universe = loadJson("MARKET_OPPORTUNITY_UNIVERSE.json");
  const ids = universe.map((r) => r.marketOpportunityId);
  assert.equal(ids.length, new Set(ids).size);

  const geo = loadJson("HOTEL_GEOGRAPHY.json");
  assert.match(geo.JW.submarket, /Piantini/i);
  assert.match(geo.RADISSON.submarket, /Naco/i);

  const diff = loadJson("DIFFERENTIATION.json");
  assert.ok("both" in diff && "jwOnly" in diff && "radOnly" in diff && "neither" in diff);
  // Live SD corpus was metro-only → no blind fanout
  assert.equal(summary.executive.neither, summary.executive.marketOpportunitiesDiscovered);
  assert.equal(summary.executive.bothHotels, 0);
  assert.ok(summary.geoEffect.metroOnlyPlausibleOrUnknown > 0);
}

console.log("test-gdi-market-first-discovery-santo-domingo-v1: PASS");
