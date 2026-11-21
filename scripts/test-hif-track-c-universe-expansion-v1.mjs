#!/usr/bin/env node
/**
 * Unit tests — HIF Track C universe expansion helpers (no Airtable).
 */
import assert from "node:assert/strict";
import {
  MATCH_CLASS,
  HANDOFF_TRACK,
  normalizeHotelKey,
  normalizeCountry,
  isUsCountry,
  isCalaCountry,
  classifySecToHpcMatch,
  routeHandoff,
  buildTrackCDiscoveryHotel,
  mapCreateReadyRowToTrackC,
  aggregateHpcAudit,
  dedupeDiscoveries,
  summarizeMatchClasses,
  buildCalaPreservationCheck,
  inferBrandFamilyFromLabel,
  TRACK_C_STANDING_AUTH,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
import {
  classifyWriteCategory,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";

assert.equal(TRACK_C_STANDING_AUTH.customerBlocked, true);
assert.equal(TRACK_C_STANDING_AUTH.forbidOperatorInferenceFromBrand, true);
assert.equal(TRACK_C_STANDING_AUTH.preserveCalaRecords, true);

assert.equal(normalizeCountry("US"), "United States");
assert.equal(normalizeCountry("uk"), "United Kingdom");
assert.equal(isUsCountry("United States"), true);
assert.equal(isCalaCountry("Mexico"), true);
assert.equal(isCalaCountry("United States"), false);

assert.ok(normalizeHotelKey("Courtyard by Marriott Lufkin").includes("lufkin"));

assert.equal(
  classifySecToHpcMatch({
    collisionOutcome: "HPC_EXISTING_RECORD_VERIFIED",
  }),
  MATCH_CLASS.MATCHED_EXISTING_HPC,
);
assert.equal(
  classifySecToHpcMatch({
    collisionOutcome: "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT",
    hasExactAddress: true,
    hasOfficialBrandSource: true,
  }),
  MATCH_CLASS.CREATE_READY,
);
assert.equal(
  classifySecToHpcMatch({
    collisionOutcome: "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW",
  }),
  MATCH_CLASS.AMBIGUOUS,
);

const handoffs = routeHandoff({
  hasFranchiseOrManagementSignal: true,
  hasContractualAgreementEvidence: true,
});
assert.ok(handoffs.includes(HANDOFF_TRACK.TRACK_A_COMMERCIAL));
assert.ok(handoffs.includes(HANDOFF_TRACK.TRACK_B_CONTRACTUAL));

const mapped = mapCreateReadyRowToTrackC({
  candidateId: "hpc_pilot_001",
  hotelName: "Fairfield Inn & Suites Lebanon",
  brand: "Fairfield",
  officialHotelBrandSource: "https://www.marriott.com/en-us/hotels/bnalb-fairfield-inn-and-suites-lebanon/rooms/",
  historicalSecCmbsSource: "https://www.sec.gov/Archives/edgar/data/1541480/000153949720000149/n1968-x5_424h.htm",
  relevantContractMilestone: { meaning: "FRANCHISE_EXPIRATION" },
  intendedHpcFields: {
    "Property Name": "Fairfield Inn & Suites Lebanon",
    Country: "United States",
    City: "Lebanon",
    "State / Region": "TN",
    "Street Address": "1145 Franklin Road",
    "Current Brand": "Fairfield",
    "Publication State": "CUSTOMER_BLOCKED",
  },
});
assert.equal(mapped.hotel.physicalCountry, "United States");
assert.match(String(mapped.hotel.notes || ""), /CUSTOMER_BLOCKED/);
assert.equal(mapped.hpcFields["Family / Source Family"], "Marriott");
assert.equal(mapped.hotel.matchClass, MATCH_CLASS.CREATE_READY);
assert.ok(!("Rooms / Keys" in mapped.hpcFields), "must not invent room counts");
assert.ok(!("Operator / Management Company" in mapped.hpcFields), "must not invent operator");

assert.equal(inferBrandFamilyFromLabel("Courtyard"), "MARRIOTT");
assert.equal(inferBrandFamilyFromLabel("Candlewood Suites"), "IHG");

const audit = aggregateHpcAudit([
  { fields: { Country: "United States", City: "Dallas", "Current Brand": "Courtyard", "Property Name": "Courtyard Dallas" } },
  { fields: { Country: "Mexico", City: "Cancún", "Property Name": "Hotel MX" } },
  { fields: { Country: "Brazil", "Property Name": "Hotel BR" } },
]);
assert.equal(audit.priorityCountries.US, 1);
assert.equal(audit.calaTotal, 2);

const deduped = dedupeDiscoveries([
  { hotelName: "Courtyard Lufkin", city: "Lufkin", stateRegion: "TX", country: "US" },
  { hotelName: "Courtyard Lufkin", city: "Lufkin", state: "TX", country: "United States" },
]);
assert.equal(deduped.length, 1);

const summary = summarizeMatchClasses([
  { matchClass: MATCH_CLASS.MATCHED_EXISTING_HPC },
  { matchClass: MATCH_CLASS.CREATE_READY },
  { matchClass: MATCH_CLASS.AMBIGUOUS },
]);
assert.equal(summary.matchedExistingHpc, 1);
assert.equal(summary.unmatchedVerified, 1);
assert.equal(summary.uncertain, 1);

const cala = buildCalaPreservationCheck({ Mexico: 3772, Brazil: 6179 }, 18000, 18000);
assert.equal(cala.preserved, true);

const inv = classifyWriteCategory({
  destination: "HI Hotel Discovery Inventory",
  verified: true,
  evidenceSupported: true,
  sourceUrl: "https://example.com",
  identityClass: "VERIFIED_PROPERTY",
});
assert.equal(inv.category, WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE);

const hpcForbid = classifyWriteCategory({
  destination: "Hotel Property Census",
  createHpc: true,
  verified: true,
  evidenceSupported: true,
  sourceUrl: "https://example.com",
});
assert.equal(hpcForbid.category, WRITE_CATEGORY.RESEARCH_ONLY);

const hotel = buildTrackCDiscoveryHotel({
  hotelName: "Test Hotel",
  city: "Austin",
  stateRegion: "TX",
  country: "US",
  sourceUrl: "https://example.com/hotel",
});
assert.equal(hotel.physicalCountry, "United States");
assert.match(hotel.discoveryPropertyId, /^disc_tc_/);

console.log("test-hif-track-c-universe-expansion-v1: PASS");
