#!/usr/bin/env node
/**
 * Unit tests for CALA coverage dashboard (no Airtable).
 */
import assert from "node:assert/strict";
import {
  estimateUniverse,
  coverageStatusFromPct,
  discoveryValueFrom,
  COVERAGE_STATUS,
  DISCOVERY_VALUE,
  rollupToRegistry,
  buildCalaCoverageDashboard,
} from "../lib/hotel-intelligence/coverage-dashboard/index.js";

const est = estimateUniverse({ census: 494, cvent: 5165, hbx: 0, holds: 4842 });
assert.equal(est.estimated_hotel_universe, 5336); // census+holds
assert.ok(est.estimation_source.includes("holds") || est.estimation_source.includes("census"));

assert.equal(coverageStatusFromPct(96, true), COVERAGE_STATUS.EXCELLENT);
assert.equal(coverageStatusFromPct(85, true), COVERAGE_STATUS.GOOD);
assert.equal(coverageStatusFromPct(70, true), COVERAGE_STATUS.FAIR);
assert.equal(coverageStatusFromPct(40, true), COVERAGE_STATUS.POOR);
assert.equal(coverageStatusFromPct(10, true), COVERAGE_STATUS.CRITICAL);

const rolled = rollupToRegistry({
  "Turks and Caicos": 10,
  "Turks and Caicos Islands": 5,
});
assert.equal(rolled["Turks and Caicos Islands"], 15);

const dash = buildCalaCoverageDashboard(
  { Brazil: 494, Mexico: 2181, Bermuda: 0 },
  { root: process.cwd() }
);
assert.ok(dash.rows.length >= 50);
assert.equal(dash.rows[0].coverage_pct <= dash.rows[dash.rows.length - 1].coverage_pct, true);
assert.ok(dash.brazil_detail);
assert.ok(dash.heat_map.CRITICAL);
assert.ok(dash.priority_ranking[0]);

const dv = discoveryValueFrom({
  estimated_missing_hotels: 4000,
  discovery_candidates_available: 4000,
  coverage_pct: 9,
});
assert.equal(dv, DISCOVERY_VALUE.HIGH);

console.log("test:hotel-intelligence-cala-coverage-dashboard OK");
