#!/usr/bin/env node
/**
 * Gate: HIF worldwide eligibility — priority markets ≠ geographic restrictions.
 */
import assert from "node:assert/strict";
import {
  WORLDWIDE_ELIGIBILITY_POLICY_V6,
  isCountryEligibleForDiscovery,
  isPriorityMarket,
  shouldDiscardForNonPriorityCountry,
  countryBasedRejectionReason,
  buildDualGeographyViews,
  assertWorldwideEligibilityInvariants,
  normalizeHotelCountryCode,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-worldwide-eligibility-v6.js";
import { buildWeeklySnapshot } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import { COUNTRY_CONCENTRATION_RULES_V6 } from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";

const inv = assertWorldwideEligibilityInvariants();
assert.equal(inv.ok, true, inv.violations.join(","));

assert.equal(WORLDWIDE_ELIGIBILITY_POLICY_V6.geographicEligibilityExclusions, "NONE");
assert.equal(WORLDWIDE_ELIGIBILITY_POLICY_V6.countryAllowlists, "PROHIBITED");
assert.equal(COUNTRY_CONCENTRATION_RULES_V6.countryRejectionGates, "PROHIBITED");

for (const c of ["JP", "ES", "CO", "DO", "DE", "AE", "FR", "AU", "IN", "SG"]) {
  assert.equal(isCountryEligibleForDiscovery(c), true, `${c} must be eligible`);
  assert.equal(shouldDiscardForNonPriorityCountry(c), false, `${c} must not be discarded`);
  assert.equal(countryBasedRejectionReason(c), null, `${c} must not be rejected for country`);
}

assert.equal(isPriorityMarket("US"), true);
assert.equal(isPriorityMarket("GB"), true);
assert.equal(isPriorityMarket("UK"), true);
assert.equal(isPriorityMarket("JP"), false);
assert.equal(normalizeHotelCountryCode("United Kingdom"), "GB");

const dual = buildDualGeographyViews({
  US: { distinctEvidenceBackedProspects: 100, distinctHotelsResearched: 200 },
  JP: { distinctEvidenceBackedProspects: 12, distinctHotelsResearched: 20 },
  ES: { distinctEvidenceBackedProspects: 5, distinctHotelsResearched: 8 },
  MX: { distinctEvidenceBackedProspects: 3, distinctHotelsResearched: 10 },
});
assert.equal(dual.globalOpportunityInventory.countryCount, 4);
assert.ok(dual.globalOpportunityInventory.byCountry.JP);
assert.equal(dual.priorityMarketPerformance.byCountry.US.prospects, 100);
assert.ok(dual.globalOpportunityInventory.otherThanPriorityMarkets.some((c) => c.countryCode === "JP"));

const snap = buildWeeklySnapshot({
  weekId: "2026-W47-worldwide-eligibility-test",
  asOf: "2026-11-21",
  byCountry: {
    US: { distinctEvidenceBackedProspects: 50 },
    DO: { distinctEvidenceBackedProspects: 2 },
  },
  allowOverwrite: true,
});
assert.equal(snap.geographicExclusions, "NONE");
assert.equal(snap.worldwideEligibility.platformScope, "WORLDWIDE");
assert.ok(snap.globalOpportunityInventory);
assert.ok(snap.priorityMarketPerformance);
assert.ok(snap.globalOpportunityInventory.byCountry.DO);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test-hif-v6-worldwide-eligibility-v6",
      policy: WORLDWIDE_ELIGIBILITY_POLICY_V6.priorityMarketsControl,
    },
    null,
    2,
  ),
);
