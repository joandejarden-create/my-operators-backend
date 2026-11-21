#!/usr/bin/env node
/**
 * Gate: HIF worldwide eligibility — priority markets ≠ geographic restrictions.
 * Scenarios A–H (founder IC directive).
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
  resolvePhysicalCountryAttribution,
  geographyEligibilityDisposition,
  globalContractEventKey,
  isOpportunityMilestoneCeiling,
  ensureCountryScorecardRow,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-worldwide-eligibility-v6.js";
import { buildWeeklySnapshot } from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import {
  COUNTRY_CONCENTRATION_RULES_V6,
  emptyCountryScorecardRow,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";

const inv = assertWorldwideEligibilityInvariants();
assert.equal(inv.ok, true, inv.violations.join(","));

assert.equal(WORLDWIDE_ELIGIBILITY_POLICY_V6.geographicEligibilityExclusions, "NONE");
assert.equal(WORLDWIDE_ELIGIBILITY_POLICY_V6.countryAllowlists, "PROHIBITED");
assert.equal(WORLDWIDE_ELIGIBILITY_POLICY_V6.milestoneCeilings, "NONE");
assert.equal(COUNTRY_CONCENTRATION_RULES_V6.countryRejectionGates, "PROHIBITED");

// A — Priority country accepted
{
  const us = geographyEligibilityDisposition("US");
  assert.equal(us.disposition, "ACCEPT");
  assert.equal(us.countsTowardInventory, true);
  assert.equal(isPriorityMarket("US"), true);
  assert.equal(isCountryEligibleForDiscovery("US"), true);
}

// B — Nonpriority country (JP / ES) accepted under identical rules
for (const c of ["JP", "ES", "CO", "DO", "DE", "AE", "FR", "AU", "IN", "SG"]) {
  assert.equal(isCountryEligibleForDiscovery(c), true, `${c} must be eligible`);
  assert.equal(shouldDiscardForNonPriorityCountry(c), false, `${c} must not be discarded`);
  assert.equal(countryBasedRejectionReason(c), null, `${c} must not be rejected for country`);
  assert.equal(geographyEligibilityDisposition(c).disposition, "ACCEPT", `${c} must ACCEPT`);
}
assert.equal(isPriorityMarket("JP"), false);
assert.equal(isPriorityMarket("ES"), false);

// C — Cross-border issuer: US SEC filing, physical hotel in MX/CA/JP
for (const physical of ["MX", "CA", "JP"]) {
  const attr = resolvePhysicalCountryAttribution({
    physicalCountry: physical,
    sourceJurisdiction: "US",
  });
  assert.equal(attr.hotelCountry, physical, `physical must be ${physical}`);
  assert.equal(attr.sourceJurisdiction, "US");
  assert.equal(attr.crossBorderFiling, true);
  assert.notEqual(attr.hotelCountry, "US");
}

// D — Unknown geography → HOLD
{
  const hold = geographyEligibilityDisposition(null);
  assert.equal(hold.disposition, "HOLD_GEOGRAPHY_UNVERIFIED");
  assert.equal(hold.countsTowardInventory, false);
  assert.equal(geographyEligibilityDisposition("").disposition, "HOLD_GEOGRAPHY_UNVERIFIED");
  assert.equal(geographyEligibilityDisposition("UNATTRIBUTED").disposition, "HOLD_GEOGRAPHY_UNVERIFIED");
}

// E — Country scorecards: nonpriority appears in Global Inventory, not misattributed to priority
{
  const dual = buildDualGeographyViews({
    US: { distinctEvidenceBackedProspects: 100, distinctHotelsResearched: 200 },
    JP: { distinctEvidenceBackedProspects: 12, distinctHotelsResearched: 20 },
    ES: { distinctEvidenceBackedProspects: 5, distinctHotelsResearched: 8 },
    MX: { distinctEvidenceBackedProspects: 3, distinctHotelsResearched: 10 },
  });
  assert.equal(dual.globalOpportunityInventory.countryCount, 4);
  assert.ok(dual.globalOpportunityInventory.byCountry.JP);
  assert.equal(dual.globalOpportunityInventory.byCountry.JP.prospects, 12);
  assert.equal(dual.priorityMarketPerformance.byCountry.US.prospects, 100);
  assert.equal(dual.priorityMarketPerformance.byCountry.JP, undefined);
  assert.ok(
    dual.globalOpportunityInventory.otherThanPriorityMarkets.some((c) => c.countryCode === "JP"),
  );
  assert.ok(
    dual.globalOpportunityInventory.otherThanPriorityMarkets.some((c) => c.countryCode === "ES"),
  );
}

// F — No opportunity cap at 1k / 2.5k / 5k / 10k
for (const n of [999, 1000, 2500, 5000, 10000, 10001, 25000]) {
  assert.equal(isOpportunityMilestoneCeiling(n), false, `count ${n} must not ceiling`);
}
assert.deepEqual(WORLDWIDE_ELIGIBILITY_POLICY_V6.progressMilestones, [1000, 2500, 5000, 10000]);

// G — Global deduplication across filing systems
{
  const a = globalContractEventKey("Hotel Arts Barcelona", "ES", "2031-06-30");
  const b = globalContractEventKey("Hotel Arts Barcelona", "ES", "2031-06-30");
  const c = globalContractEventKey("Hotel Arts Barcelona", "ES", "2032-06-30");
  const d = globalContractEventKey("Hotel Arts Barcelona", "US", "2031-06-30");
  assert.equal(a, b);
  assert.notEqual(a, c);
  assert.notEqual(a, d);
}

// H — Evidence quality: worldwide eligibility does not weaken commercial list / historical rules
{
  assert.deepEqual(COUNTRY_CONCENTRATION_RULES_V6.listsOnly, [
    "BRAND_CONVERSION_TARGETS",
    "MANAGEMENT_CONTRACT_TARGETS",
  ]);
  assert.equal(COUNTRY_CONCENTRATION_RULES_V6.historicalNotAutomaticallyActive, true);
  // Unresolved contractual observations must not auto-promote — policy remains BCT/MCT lists only
  assert.equal(WORLDWIDE_ELIGIBILITY_POLICY_V6.priorityMarketsControl, "PRIORITIZATION_ONLY_NOT_ELIGIBILITY");
}

// Dynamic scorecard rows for any country
{
  const byCountry = {};
  ensureCountryScorecardRow(byCountry, "JP", emptyCountryScorecardRow);
  ensureCountryScorecardRow(byCountry, "US", emptyCountryScorecardRow);
  assert.ok(byCountry.JP);
  assert.ok(byCountry.US);
  assert.equal(byCountry.JP.countryCode, "JP");
}

assert.equal(normalizeHotelCountryCode("United Kingdom"), "GB");
assert.equal(isPriorityMarket("GB"), true);
assert.equal(isPriorityMarket("UK"), true);

const snap = buildWeeklySnapshot({
  weekId: "2026-W47-worldwide-eligibility-test",
  asOf: "2026-11-21",
  byCountry: {
    US: { distinctEvidenceBackedProspects: 50 },
    DO: { distinctEvidenceBackedProspects: 2 },
    JP: { distinctEvidenceBackedProspects: 7 },
  },
  allowOverwrite: true,
});
assert.equal(snap.geographicExclusions, "NONE");
assert.equal(snap.worldwideEligibility.platformScope, "WORLDWIDE");
assert.ok(snap.globalOpportunityInventory);
assert.ok(snap.priorityMarketPerformance);
assert.ok(snap.globalOpportunityInventory.byCountry.DO);
assert.ok(snap.globalOpportunityInventory.byCountry.JP);
assert.equal(snap.priorityMarketPerformance.byCountry.JP, undefined);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test-hif-v6-worldwide-eligibility-v6",
      scenarios: ["A", "B", "C", "D", "E", "F", "G", "H"],
      policy: WORLDWIDE_ELIGIBILITY_POLICY_V6.priorityMarketsControl,
    },
    null,
    2,
  ),
);
