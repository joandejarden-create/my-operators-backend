#!/usr/bin/env node
/**
 * Demand Anchors radar layer unit tests.
 *   node scripts/test-demand-anchors-radar.mjs
 */
import "../load-env.js";
import { applyPointTypeDefaults, getPointTypeDefaults } from "../lib/demand-anchors/point-type-defaults.js";
import {
  normalizeDemandAnchorToRadarPoint,
  filterRadarVisiblePoints,
  toLegacyDemandAnchorItem,
} from "../lib/demand-anchors/normalize-demand-anchor.js";
import {
  groupDemandAnchorsLayers,
  calculateDemandAnchorsStatistics,
  getDemandAnchorLayerFilters,
} from "../lib/demand-anchors/radar-map-layers.js";
import { POINT_TYPES } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";

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

function testConventionCenterDefaults() {
  const d = getPointTypeDefaults("Convention Center");
  assert(d.demandSegment === "Group / Event", "convention demand segment");
  assert(d.mapIconType === "Event", "convention map icon");
  assert(d.demandRelevance === "High", "convention demand relevance");
}

function testMedicalCampusNormalize() {
  const p = normalizeDemandAnchorToRadarPoint(
    record({
      "Demand Anchor Name": "Sample Medical Campus",
      "Point Type": "Medical Campus",
      "Point Subtype": "Hospital / Medical District",
      Latitude: 18.41,
      Longitude: -66.06,
      City: "San Juan",
      Country: "Puerto Rico",
      Region: "Puerto Rico",
      "Radar Category": "Demand Anchors",
      "Include On Radar Map": true,
    })
  );
  assert(p.pointType === "Medical Campus", "medical point type");
  assert(p.radarCategory === "Demand Anchors", "radar category");
  assert(p.demandSegment === "Medical" || p.demandRelevance === "High", "medical defaults applied");
  assert(p.includeOnRadarMap === true, "include on radar");
}

function testLayerGrouping() {
  const points = POINT_TYPES.slice(0, 3).map((pt, i) =>
    normalizeDemandAnchorToRadarPoint(
      record({
        "Demand Anchor Name": "Anchor " + i,
        "Point Type": pt,
        Latitude: 18 + i * 0.01,
        Longitude: -66,
        "Include On Radar Map": true,
      })
    )
  );
  const grouped = groupDemandAnchorsLayers(points, "Medical Campus");
  assert(grouped.points.length === 1, "filter by Medical Campus");
  const all = groupDemandAnchorsLayers(points, "all");
  assert(all.points.length === 3, "all filters return 3");
}

function testStatistics() {
  const points = [
    normalizeDemandAnchorToRadarPoint(
      record({
        "Demand Anchor Name": "A",
        "Point Type": "Sports Venue",
        "Map Icon Type": "Sports",
        "Include On Radar Map": true,
      })
    ),
    normalizeDemandAnchorToRadarPoint(
      record({
        "Demand Anchor Name": "B",
        "Point Type": "Sports Venue",
        "Map Icon Type": "Sports",
        "Include On Radar Map": false,
      })
    ),
  ];
  const stats = calculateDemandAnchorsStatistics(points);
  assert(stats.totalDemandAnchors === 1, "includeHidden false excludes hidden");
  const visible = filterRadarVisiblePoints(points);
  assert(visible.length === 1, "filterRadarVisiblePoints");
}

function testLegacyShape() {
  const p = normalizeDemandAnchorToRadarPoint(
    record({
      "Demand Anchor Name": "Test",
      "Point Type": "Business District",
      Latitude: 1,
      Longitude: 2,
    })
  );
  const leg = toLegacyDemandAnchorItem(p);
  assert(leg.name === "Test", "legacy name");
  assert(leg.pointType === "Business District", "legacy pointType");
}

function testLayerFilters() {
  const points = [
    normalizeDemandAnchorToRadarPoint(
      record({ "Demand Anchor Name": "X", "Point Type": "Beach / Waterfront", "Include On Radar Map": true })
    ),
  ];
  const filters = getDemandAnchorLayerFilters(points);
  assert(filters.some((f) => f.id === "all" && f.count === 1), "layer filter all count");
}

function testApplyDefaults() {
  const applied = applyPointTypeDefaults({ name: "Beach", pointType: "Beach / Waterfront" });
  assert(applied.demandSegment === "Leisure", "beach defaults applied");
}

function main() {
  testConventionCenterDefaults();
  testMedicalCampusNormalize();
  testLayerGrouping();
  testStatistics();
  testLegacyShape();
  testLayerFilters();
  testApplyDefaults();

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll demand anchors radar tests passed.");
}

main();
