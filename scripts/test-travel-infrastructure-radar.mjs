#!/usr/bin/env node
/**
 * Travel Infrastructure → Radar Map Point tests.
 *   node scripts/test-travel-infrastructure-radar.mjs
 */
import "../load-env.js";
import {
  normalizeTravelInfrastructureToRadarPoint,
  filterRadarVisiblePoints,
  toLegacyInfrastructureItem,
} from "../lib/travel-infrastructure/normalize-radar-map-point.js";
import { applyPointTypeDefaults, inferPointSubtype } from "../lib/travel-infrastructure/point-type-defaults.js";
import {
  groupTravelInfrastructureLayers,
  calculateTravelInfrastructureStatistics,
} from "../lib/travel-infrastructure/radar-map-layers.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function record(fields) {
  return { id: "recTEST" + Math.random().toString(36).slice(2, 8), fields };
}

function testLegacyAirport() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Cancun International",
      Type: "Airport",
      "Infrastructure Role": "International Hub (Airport)",
      Latitude: 21.03,
      Longitude: -86.87,
      City: "Cancun",
      Country: "Mexico",
      Region: "Mexico",
    })
  );
  assert(p.pointType === "Airport", "legacy Type → pointType Airport");
  assert(p.pointSubtype === "International Airport", "role → International Airport subtype");
  assert(p.mapIconType === "Airport", "airport map icon");
  assert(p.includeOnRadarMap === true, "include on radar default true");
  assert(p.type === "Airport", "legacy type field preserved");
}

function testCruisePort() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Miami Cruise Port",
      Type: "Cruise Port",
      "Infrastructure Role": "Cruise Homeport",
      Latitude: 25.77,
      Longitude: -80.17,
      Country: "United States",
    })
  );
  assert(p.pointType === "Cruise Port", "cruise point type");
  assert(p.pointSubtype === "Cruise Terminal", "cruise subtype");
  assert(p.mapIconType === "Cruise Port", "cruise map icon");
  assert(p.demandRelevance === "High", "cruise default demand relevance");
}

function testTrainStation() {
  const input = applyPointTypeDefaults({
    name: "Central Station",
    pointType: "Train Station",
    pointSubtype: "Rail Hub",
  });
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: input.name,
      "Point Type": input.pointType,
      "Point Subtype": input.pointSubtype,
      Latitude: 40.75,
      Longitude: -73.99,
    })
  );
  assert(p.pointType === "Train Station", "train station type");
  assert(p.mapIconType === "Train", "train icon");
  assert(p.demandRelevance === "Medium", "train default relevance");
}

function testHighwayAccess() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "I-95 Exit 12",
      "Point Type": "Highway Access",
      "Point Subtype": "Highway Exit",
      Latitude: 26.1,
      Longitude: -80.2,
    })
  );
  assert(p.mapIconType === "Highway", "highway icon");
  assert(p.demandPattern.includes("Drive-To"), "highway drive-to pattern");
}

function testBusTerminal() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Central Bus Terminal",
      "Point Type": "Bus Terminal",
      Latitude: 19.43,
      Longitude: -99.13,
    })
  );
  assert(p.pointSubtype === "Intercity Bus Terminal", "bus subtype inferred");
  assert(p.demandRelevance === "Low", "bus low relevance default");
}

function testFerryTerminal() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Island Ferry",
      "Point Type": "Ferry Terminal",
      "Point Subtype": "Island Access",
      Latitude: 18.4,
      Longitude: -66.1,
    })
  );
  assert(p.mapIconType === "Ferry", "ferry icon");
  assert(p.relevantHotelTypes.includes("Marina / Waterfront"), "ferry hotel types");
}

function testPortMaritime() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Cargo Port Alpha",
      "Point Type": "Port / Maritime",
      "Point Subtype": "Cargo Port",
      Latitude: 10,
      Longitude: -75,
    })
  );
  assert(p.mapIconType === "Port", "port icon");
  assert(p.demandPattern.includes("Crew"), "port crew pattern");
}

function testMissingOptionalFields() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({ Name: "Minimal", Type: "Airport", Latitude: 1, Longitude: 2 })
  );
  assert(p.name === "Minimal", "name preserved");
  assert(p.dataConfidence === "", "empty confidence when missing");
  assert(Array.isArray(p.demandPattern) && p.demandPattern.length > 0, "defaults applied for airport patterns");
}

function testIncludeOnRadarMapFalse() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Hidden Point",
      Type: "Airport",
      "Include On Radar Map": false,
      Latitude: 1,
      Longitude: 2,
    })
  );
  assert(p.includeOnRadarMap === false, "explicit false honored");
  const visible = filterRadarVisiblePoints([p]);
  assert(visible.length === 0, "hidden point filtered from radar");
}

function testLayerGrouping() {
  const points = [
    normalizeTravelInfrastructureToRadarPoint(record({ Name: "A", Type: "Airport", Latitude: 1, Longitude: 1 })),
    normalizeTravelInfrastructureToRadarPoint(record({ Name: "B", Type: "Cruise Port", Latitude: 2, Longitude: 2 })),
    normalizeTravelInfrastructureToRadarPoint(record({ Name: "C", Type: "Airport", Latitude: 3, Longitude: 3 })),
  ];
  const grouped = groupTravelInfrastructureLayers(points, "Airport");
  assert(grouped.points.length === 2, "layer filter Airport → 2 points");
  const all = groupTravelInfrastructureLayers(points, "all");
  assert(all.layers["Travel Infrastructure"].pointCount === 3, "all layer count 3");
}

function testUnknownSubtypeFallback() {
  const sub = inferPointSubtype("Unknown Type", "", "", "");
  assert(sub === "Unknown", "unknown subtype fallback");
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({ Name: "X", Type: "Convention Center", Latitude: 0, Longitude: 0 })
  );
  assert(p.pointType === "Convention Center", "legacy convention center preserved");
  assert(p.mapIconType === "Convention", "convention icon");
}

function testLegacyApiShape() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({ Name: "Legacy", Type: "Airport", Latitude: 10, Longitude: 20, City: "X", Country: "Y" })
  );
  const legacy = toLegacyInfrastructureItem(p);
  assert(legacy.lat === 10 && legacy.lng === 20, "legacy lat/lng");
  assert(legacy.type === "Airport", "legacy type field");
  assert(legacy.pointType === "Airport", "extended pointType on legacy item");
}

function testStatistics() {
  const points = [
    normalizeTravelInfrastructureToRadarPoint(record({ Name: "A", Type: "Airport", Country: "Mexico", Region: "Mexico", Latitude: 1, Longitude: 1 })),
    normalizeTravelInfrastructureToRadarPoint(record({ Name: "B", Type: "Cruise Port", Country: "Mexico", Region: "Mexico", Latitude: 2, Longitude: 2 })),
  ];
  const stats = calculateTravelInfrastructureStatistics(points);
  assert(stats.totalInfrastructure === 2, "stats total");
  assert(stats.typeCounts.Airport === 1, "stats airport count");
}

function testSubmarketField() {
  const p = normalizeTravelInfrastructureToRadarPoint(
    record({
      Name: "Ceiba Ferry",
      Type: "Ferry Terminal",
      Country: "Puerto Rico",
      Region: "Caribbean",
      Submarket: "East Coast / Island Access",
      Latitude: 18.2,
      Longitude: -65.6,
    })
  );
  assert(p.submarket === "East Coast / Island Access", "normalized point includes submarket");
  const legacy = toLegacyInfrastructureItem(p);
  assert(legacy.submarket === "East Coast / Island Access", "legacy API shape includes submarket");
}

async function main() {
  testLegacyAirport();
  testCruisePort();
  testTrainStation();
  testHighwayAccess();
  testBusTerminal();
  testFerryTerminal();
  testPortMaritime();
  testMissingOptionalFields();
  testIncludeOnRadarMapFalse();
  testLayerGrouping();
  testUnknownSubtypeFallback();
  testLegacyApiShape();
  testStatistics();
  testSubmarketField();

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll travel infrastructure radar tests passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
