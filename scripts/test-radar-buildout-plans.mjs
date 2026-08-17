#!/usr/bin/env node
/**
 * Unit tests for CALA Radar Buildout plans.
 *   node scripts/test-radar-buildout-plans.mjs
 */
import "../load-env.js";
import {
  BUILD_STRATEGY_TYPES,
  getBuildStrategyDefinition,
  resolveStrategyTargets,
} from "../lib/radar-buildout/country-build-strategies.js";
import { COUNTRY_CONFIGS, getCountryConfig, listCountryConfigs } from "../lib/radar-buildout/country-configs.js";
import {
  calculateSourceCoveragePct,
  calculateCoordinateCoveragePct,
  calculateDataConfidenceMix,
  summarizeCountryRadarPoints,
} from "../lib/radar-buildout/coverage-metrics.js";
import { evaluateBuildStatus, recommendNextAction } from "../lib/radar-buildout/evaluate-build-status.js";
import { buildCountryPlanPayload } from "../lib/radar-buildout/build-plan-generator.js";
import { buildRadarBuildPlanAirtableFields, mergeBuildPlanWithExisting } from "../lib/radar-buildout/airtable-radar-build-plans-io.js";
import { extractSubmarketFromNotes } from "../lib/radar-submarket.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

assert(!!getBuildStrategyDefinition(BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE), "island strategy loads");
assert(!!getBuildStrategyDefinition(BUILD_STRATEGY_TYPES.CORRIDOR_BASED), "corridor strategy loads");
assert(!!getBuildStrategyDefinition(BUILD_STRATEGY_TYPES.MARKET_BY_MARKET), "market-by-market strategy loads");

assert(COUNTRY_CONFIGS["Puerto Rico"], "Puerto Rico config exists");
assert(COUNTRY_CONFIGS["Dominican Republic"], "DR config exists");
assert(listCountryConfigs({ tier: "Tier 1" }).length >= 5, "tier 1 configs");

const prTargets = resolveStrategyTargets(COUNTRY_CONFIGS["Puerto Rico"]);
assert(prTargets.demandAnchors >= 40, "PR target demand anchors");

const points = [
  { latitude: 18.4, longitude: -66.1, sourceReference: "https://example.com", dataConfidence: "High", includeOnRadarMap: true, pointType: "Beach / Waterfront", submarket: "San Juan Metro" },
  { lat: 18.5, lng: -66.2, source: ["Public Source"], dataConfidence: "Medium", includeOnRadarMap: true, pointType: "Medical Campus", submarket: "San Juan Metro" },
];
assert(calculateSourceCoveragePct(points) === 100, "source coverage 100%");
assert(calculateCoordinateCoveragePct(points) === 100, "coordinate coverage 100%");
const mix = calculateDataConfidenceMix(points);
assert(mix.High === 1 && mix.Medium === 1, "confidence mix");

const prLive = {
  demandAnchors: Array.from({ length: 60 }, (_, i) => ({
    pointType: ["Beach / Waterfront", "Medical Campus", "Tourist Attraction", "Business District"][i % 4],
    submarket: ["San Juan Metro", "North Coast Resort Corridor", "East Coast / Island Access"][i % 3],
    sourceReference: "https://example.com/" + i,
    latitude: 18.4,
    longitude: -66.1,
    dataConfidence: "High",
    includeOnRadarMap: true,
  })),
  travelInfrastructure: Array.from({ length: 25 }, () => ({
    pointType: "Ferry Terminal",
    submarket: "East Coast / Island Access",
    sourceReference: "https://example.com",
    latitude: 18.2,
    longitude: -65.6,
    dataConfidence: "Medium",
    includeOnRadarMap: true,
  })),
};
prLive.summary = summarizeCountryRadarPoints(prLive.demandAnchors, prLive.travelInfrastructure);

const prPlan = buildCountryPlanPayload("Puerto Rico", COUNTRY_CONFIGS["Puerto Rico"], prLive);
assert(prPlan.current.totalRadarPoints === 85, "PR live total 85");
assert(
  ["Market Ready", "Deal Ready", "Intelligence Ready"].includes(prPlan.buildStatus),
  "PR status at least Market Ready (got " + prPlan.buildStatus + ")"
);

const drPlan = buildCountryPlanPayload("Dominican Republic", COUNTRY_CONFIGS["Dominican Republic"], {
  demandAnchors: [],
  travelInfrastructure: [],
  summary: summarizeCountryRadarPoints([], []),
});
assert(
  ["Market Ready", "Planned", "Not Started"].includes(drPlan.buildStatus),
  "DR status reflects manual Market Ready or planned (got " + drPlan.buildStatus + ")"
);
assert(
  /Dominican Republic|fixtures|preview|travel infrastructure|Deal Ready|corridor/i.test(drPlan.nextRecommendedAction),
  "DR next action (got: " + drPlan.nextRecommendedAction + ")"
);

const fields = buildRadarBuildPlanAirtableFields(prPlan);
assert(fields["Country"] === "Puerto Rico", "airtable field map country");
assert(fields["Target Demand Anchors"] != null, "target DA field");

const merged = mergeBuildPlanWithExisting(
  { ...prPlan, notes: "generated" },
  { existingRecord: { notes: "manual note" }, force: false }
);
assert(merged.notes === "manual note", "preserve manual notes without force");

assert(
  extractSubmarketFromNotes("Submarket: East Coast / Island Access. Corridor note.") ===
    "East Coast / Island Access",
  "notes submarket prefix parser"
);

if (failed) {
  console.error("\n" + failed + " test(s) failed");
  process.exit(1);
}
console.log("\nAll radar buildout plan tests passed.");
