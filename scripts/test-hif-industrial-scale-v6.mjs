#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  SCALE_AMBITIONS_V6,
  HOTEL_INTELLIGENCE_METRIC,
  COMMERCIAL_PROSPECT_LISTS_V6,
  emptyHotelIntelligenceScorecard,
  assertMetricTaxonomyIntegrity,
} from "../lib/hotel-intelligence/sec-edgar/hif-industrial-scale-operating-contract-v6.js";
import {
  buildEmptyRegionalRegister,
  recordYieldObservation,
  summarizeCoverage,
  mapCountryToIntensiveRegion,
} from "../lib/hotel-intelligence/sec-edgar/hif-regional-coverage-yield-register-v6.js";

assert.equal(SCALE_AMBITIONS_V6.geographicExclusions, "NONE");
assert.equal(SCALE_AMBITIONS_V6.aspirationalMilestones.distinctResearchedHotelsInitialScale, 10000);
assert.equal(SCALE_AMBITIONS_V6.supersededTargets.status, "REPLACED_BY_INDUSTRIAL_SCALE_AMBITIONS");
assert.equal(COMMERCIAL_PROSPECT_LISTS_V6.thirdListAuthorized, false);
assert.equal(COMMERCIAL_PROSPECT_LISTS_V6.leaseDedicatedProduct, false);

const sc = emptyHotelIntelligenceScorecard("2026-11-21");
assert.equal(Object.keys(sc.metrics).length, Object.keys(HOTEL_INTELLIGENCE_METRIC).length);
sc.metrics.DISCOVERED_HOTELS = 10;
sc.metrics.QUALIFIED_COMMERCIAL_TARGETS = 11;
assert.equal(assertMetricTaxonomyIntegrity(sc).ok, false);

sc.metrics.QUALIFIED_COMMERCIAL_TARGETS = 5;
sc.metrics.VERIFIED_EXPIRATION_HOTELS = 3;
assert.equal(assertMetricTaxonomyIntegrity(sc).ok, true);

assert.equal(mapCountryToIntensiveRegion("ES"), "SOUTHERN_EUROPE");
assert.equal(mapCountryToIntensiveRegion("BR"), "SOUTH_AMERICA");
assert.equal(mapCountryToIntensiveRegion("JP"), "OTHER_ELIGIBLE");

let reg = buildEmptyRegionalRegister("2026-11-21");
assert.equal(reg.byRegion.WESTERN_EUROPE.covered, false);
assert.match(reg.byRegion.WESTERN_EUROPE.coverageNote, /ELIGIBLE_NOT_COVERED/);

reg = recordYieldObservation(reg, {
  country: "FR",
  hotelsDiscovered: 2,
  documentsExamined: 1,
  contractDatesVerified: 1,
  lastResearchedDate: "2026-11-21",
});
assert.equal(reg.byRegion.WESTERN_EUROPE.covered, true);
assert.equal(reg.byRegion.WESTERN_EUROPE.totals.hotelsDiscovered, 2);

const summary = summarizeCoverage(reg);
assert.equal(summary.intensiveRegionsEligible, 9);
assert.ok(summary.intensiveRegionsWithActualResearch >= 1);

console.log("test-hif-industrial-scale-v6: PASS");
