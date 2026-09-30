/**
 * Tests: Santo Domingo Geography Resolution + Venue-TBD Applicability V2
 *   node scripts/test-gdi-santo-domingo-geography-resolution-v2.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  LOCATION_STATUS,
  classifyLocationStatusV2,
  locationStatusAllowsHotelFit,
  hasAffirmativeVenueTbd,
  hasAffirmativeHotelTbd,
  decideLocationJevAction,
  buildSantoDomingoGeographyProfiles,
  buildNycControlGeographyProfiles,
  santoDomingoMarketHints,
  evaluateHotelGeographicApplicability,
  classifyOpportunityGeography,
  GEO_APPLICABILITY,
  applicabilityIsCandidate,
  wouldBlindFanoutToMidtown,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/santo-domingo-geography-resolution-v2"
);

function loadJson(name) {
  return JSON.parse(fs.readFileSync(path.join(OUT, name), "utf8"));
}

// Affirmative TBD requirement
{
  assert.equal(hasAffirmativeVenueTbd("conference in Santo Domingo 2027"), false);
  assert.equal(hasAffirmativeVenueTbd("venue to be announced for Santo Domingo"), true);
  assert.equal(hasAffirmativeHotelTbd("alojamiento próximamente"), true);
  assert.equal(hasAffirmativeHotelTbd("hotels near the venue"), false);
}

// Ontology classifications
{
  const wrong = classifyLocationStatusV2({
    text: "Special Olympics Puerto Rico basketball",
    url: "https://facebook.com/pr",
  });
  assert.equal(wrong.locationStatus, LOCATION_STATUS.WRONG_MARKET);

  // Geography stamp must not rescue wrong-market seed (V1 contamination bug)
  const stampRescue = classifyLocationStatusV2({
    text: "Special Olympics Puerto Rico basketball Santo Domingo hotel guide",
    seedText: "Special Olympics Puerto Rico",
    geography: "Santo Domingo",
    researchAttempted: true,
    researchExhausted: true,
  });
  assert.equal(stampRescue.locationStatus, LOCATION_STATUS.WRONG_MARKET);

  const known = classifyLocationStatusV2({
    text: "Congreso en Piantini Blue Mall Santo Domingo 2027",
    geography: "Santo Domingo",
  });
  assert.equal(known.locationStatus, LOCATION_STATUS.SUBMARKET_KNOWN);
  assert.match(known.submarket, /Piantini/i);

  const venueTbd = classifyLocationStatusV2({
    text: "Santo Domingo 2027 congress — venue to be announced",
    geography: "Santo Domingo",
    researchAttempted: true,
  });
  assert.equal(venueTbd.locationStatus, LOCATION_STATUS.VENUE_TBD);

  const hotelTbd = classifyLocationStatusV2({
    text: "Santo Domingo Piantini congress — host hotel pending",
    geography: "Santo Domingo / Piantini",
  });
  assert.equal(hotelTbd.locationStatus, LOCATION_STATUS.HOTEL_TBD);

  const metro = classifyLocationStatusV2({
    text: "citywide Santo Domingo festival across multiple venues",
    geography: "Santo Domingo",
  });
  assert.equal(metro.locationStatus, LOCATION_STATUS.METRO_WIDE);

  const ceiling = classifyLocationStatusV2({
    text: "Congreso Santo Domingo 2027",
    geography: "Santo Domingo",
    researchAttempted: true,
    researchExhausted: true,
  });
  assert.equal(
    ceiling.locationStatus,
    LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING
  );

  const incomplete = classifyLocationStatusV2({
    text: "Congreso Santo Domingo 2027",
    geography: "Santo Domingo",
    researchAttempted: true,
    researchExhausted: false,
  });
  assert.equal(
    incomplete.locationStatus,
    LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE
  );
}

// Fit gate
{
  assert.equal(locationStatusAllowsHotelFit(LOCATION_STATUS.VENUE_TBD), true);
  assert.equal(locationStatusAllowsHotelFit(LOCATION_STATUS.METRO_WIDE), true);
  assert.equal(
    locationStatusAllowsHotelFit(LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE),
    false
  );
  assert.equal(locationStatusAllowsHotelFit(LOCATION_STATUS.WRONG_MARKET), false);
  assert.equal(
    locationStatusAllowsHotelFit(LOCATION_STATUS.LOCATION_UNKNOWN_PUBLIC_DATA_CEILING, {
      santoDomingoConfirmed: true,
      commercialOpen: true,
    }),
    true
  );
}

// Specific geo overrides metro; venue-TBD reaches fit; JW Piantini / Radisson Naco
{
  const hints = santoDomingoMarketHints();
  const profiles = buildSantoDomingoGeographyProfiles();

  const piantini = evaluateHotelGeographicApplicability(
    { title: "Gala", destinationStatus: "Piantini Blue Mall Santo Domingo" },
    profiles.JW,
    { marketHints: hints, locationStatus: LOCATION_STATUS.SUBMARKET_KNOWN }
  );
  const radP = evaluateHotelGeographicApplicability(
    { title: "Gala", destinationStatus: "Piantini Blue Mall Santo Domingo" },
    profiles.RADISSON,
    { marketHints: hints, locationStatus: LOCATION_STATUS.SUBMARKET_KNOWN }
  );
  assert.ok(
    piantini.applicability === GEO_APPLICABILITY.DIRECT ||
      piantini.applicability === GEO_APPLICABILITY.STRONG
  );
  assert.notEqual(radP.applicability, GEO_APPLICABILITY.DIRECT);

  const venueTbdJw = evaluateHotelGeographicApplicability(
    { title: "Congress", destinationStatus: "Santo Domingo" },
    profiles.JW,
    {
      marketHints: hints,
      locationStatus: LOCATION_STATUS.VENUE_TBD,
      santoDomingoConfirmed: true,
      commercialOpen: true,
    }
  );
  assert.equal(venueTbdJw.applicability, GEO_APPLICABILITY.PLAUSIBLE);
  assert.ok(applicabilityIsCandidate(venueTbdJw.applicability));

  const incompleteHold = evaluateHotelGeographicApplicability(
    { title: "Congress", destinationStatus: "Santo Domingo" },
    profiles.JW,
    {
      marketHints: hints,
      locationStatus: LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE,
    }
  );
  assert.equal(incompleteHold.applicability, GEO_APPLICABILITY.UNKNOWN);
}

// Jev location routing
{
  const a = decideLocationJevAction(LOCATION_STATUS.LOCATION_UNKNOWN_RESEARCH_INCOMPLETE);
  assert.equal(a.action, "VERIFY_EVENT_LOCATION");
  const b = decideLocationJevAction(LOCATION_STATUS.VENUE_TBD);
  assert.equal(b.action, "STOP_NO_PUBLIC_PATH");
}

// NYC regression — Brooklyn safeguard + Times Square specificity
{
  const nyc = buildNycControlGeographyProfiles();
  const bk = evaluateHotelGeographicApplicability(
    { title: "Brooklyn housing", destinationStatus: "Downtown Brooklyn" },
    nyc.HILTON,
    {}
  );
  assert.equal(bk.applicability, GEO_APPLICABILITY.NONE);

  const ts = classifyOpportunityGeography({
    destinationStatus: "Times Square",
    title: "Meeting",
  });
  assert.ok(
    ts.confidence === "SPECIFIC" || /Times Square|Midtown/i.test(ts.microArea || ts.submarket || "")
  );

  assert.equal(
    wouldBlindFanoutToMidtown(
      classifyOpportunityGeography({ destinationStatus: "Williamsburg, Brooklyn" })
    ),
    false
  );

  // Bare NYC metro without V2 locationStatus stays UNKNOWN (no blind PLAUSIBLE)
  const bareNyc = evaluateHotelGeographicApplicability(
    { title: "Association", destinationStatus: "New York City" },
    nyc.HILTON,
    {}
  );
  assert.equal(bareNyc.applicability, GEO_APPLICABILITY.UNKNOWN);
}

// Report artifacts
{
  assert.ok(fs.existsSync(path.join(OUT, "RUN_SUMMARY.json")));
  assert.ok(fs.existsSync(path.join(OUT, "SANTO_DOMINGO_V1_CORPUS_BEFORE.json")));
  assert.ok(fs.existsSync(path.join(OUT, "PRIORITY_12_MATRIX.json")));
  const summary = loadJson("RUN_SUMMARY.json");
  assert.equal(summary.shadowOnly, true);
  assert.equal(summary.customerMutationNyc, false);
  assert.equal(summary.nycRegression.renaissanceReady, 11);
  assert.equal(summary.nycRegression.hiltonReady, 10);
  assert.equal(summary.nycRegression.brooklynSafeguard, "PASS");
  assert.equal(summary.v1ToV2.v1NotApplicable, 86);
  const matrix = loadJson("PRIORITY_12_MATRIX.json");
  assert.equal(matrix.length, 12);
  assert.ok(matrix.every((r) => r.locationState && r.jwFinal && r.radissonFinal));
}

console.log("test-gdi-santo-domingo-geography-resolution-v2: PASS");
