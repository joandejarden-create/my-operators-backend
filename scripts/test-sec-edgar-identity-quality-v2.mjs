#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  classifyHotelIdentity,
  classifyInventoryHotels,
  evaluateVerifiedPrecision,
  IDENTITY_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";
import {
  buildEntityAttribution,
  locationAttributionLooksSuspicious,
  ENTITY_ROLE,
} from "../lib/hotel-intelligence/sec-edgar/entity-attribution-v2.js";
import { isPlausibleHotelName } from "../lib/hotel-intelligence/sec-edgar/hotel-discovery-v1.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("artifacts_and_generics_rejected", () => {
  assert.equal(
    classifyHotelIdentity({ hotelName: "Comparable Hotel" }).identityClass,
    IDENTITY_CLASS.EXTRACTION_ARTIFACT,
  );
  assert.equal(
    classifyHotelIdentity({ hotelName: "Blue Chip Economic Indicators. Hotel" }).identityClass,
    IDENTITY_CLASS.EXTRACTION_ARTIFACT,
  );
  assert.equal(
    classifyHotelIdentity({ hotelName: "Embassy Suites" }).identityClass,
    IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY,
  );
  assert.equal(
    classifyHotelIdentity({ hotelName: "Courtyard by Marriott" }).identityClass,
    IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY,
  );
  assert.equal(
    classifyHotelIdentity({ hotelName: "AC Hotel" }).identityClass,
    IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY,
  );
  assert.equal(isPlausibleHotelName("Embassy Suites"), false);
  assert.equal(isPlausibleHotelName("Comparable Hotel"), false);
});

test("multi_hotel_conjunction_classified", () => {
  const c = classifyHotelIdentity({
    hotelName: "Andaz Miami Beach and Marriott Long Beach Downtown",
  });
  assert.equal(c.identityClass, IDENTITY_CLASS.MULTIPLE_PROPERTIES);
  assert.ok(c.splitCandidates?.length >= 2);
  // Resort and Spa is OK as single property
  assert.notEqual(
    classifyHotelIdentity({ hotelName: "Hyatt Regency Maui Resort and Spa" }).identityClass,
    IDENTITY_CLASS.MULTIPLE_PROPERTIES,
  );
});

test("state_prefix_and_incomplete_location_not_verified", () => {
  // "AZ Residence Inn" normalizes to bare brand → GENERIC (not a property)
  const az = classifyHotelIdentity({ hotelName: "AZ Residence Inn" });
  assert.ok(
    [IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY, IDENTITY_CLASS.AMBIGUOUS_PROPERTY].includes(
      az.identityClass,
    ),
  );
  assert.equal(az.promotionEligible, false);
  assert.equal(
    classifyHotelIdentity({ hotelName: "Courtyard Downtown" }).identityClass,
    IDENTITY_CLASS.AMBIGUOUS_PROPERTY,
  );
});

test("schedule_backed_city_state_is_verified", () => {
  const c = classifyHotelIdentity({
    hotelName: "Courtyard Denver Downtown",
    city: "Denver",
    stateRegion: "CO",
    discoveryMethod: "FRANCHISE_EXPIRATION_SCHEDULE",
  });
  assert.equal(c.identityClass, IDENTITY_CLASS.VERIFIED_PROPERTY);
  assert.equal(c.promotionEligible, true);
});

test("never_verify_from_brand_and_city_alone", () => {
  const c = classifyHotelIdentity({
    hotelName: "Embassy Suites",
    city: "Houston",
    stateRegion: "TX",
  });
  assert.equal(c.identityClass, IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY);
  assert.equal(c.promotionEligible, false);
});

test("duplicate_alias_marking", () => {
  const rows = classifyInventoryHotels([
    {
      hotelName: "Courtyard Boston Downtown",
      city: "Boston",
      stateRegion: "MA",
      discoveryMethod: "FRANCHISE_EXPIRATION_SCHEDULE",
    },
    {
      hotelName: "Courtyard Boston Downtown",
      city: "Boston",
      stateRegion: "MA",
      discoveryMethod: "PROSE_MENTION",
    },
  ]);
  assert.equal(rows[0].identityClass, IDENTITY_CLASS.VERIFIED_PROPERTY);
  assert.equal(rows[1].identityClass, IDENTITY_CLASS.DUPLICATE_ALIAS);
});

test("registrant_is_not_automatic_owner", () => {
  const a = buildEntityAttribution({
    hotelName: "Courtyard Denver Downtown",
    registrant: "DiamondRock Hospitality Company",
    cik: "0001298946",
  });
  assert.ok(a.relationships.some((r) => r.role === ENTITY_ROLE.FILING_REGISTRANT));
  assert.ok(!a.relationships.some((r) => r.role === ENTITY_ROLE.LEGAL_OWNER));
  assert.ok(a.warnings.some((w) => /not automatically the legal owner/i.test(w) || /Do not treat filing registrant as legal owner/i.test(w)));
});

test("location_suspicion_flags", () => {
  const s = locationAttributionLooksSuspicious({
    hotelName: "AZ Residence Inn",
    city: "C Bay Hotel Owner",
  });
  assert.equal(s.suspicious, true);
  assert.ok(s.issues.includes("city_looks_like_legal_entity"));
});

test("verified_precision_gate_not_weakened", () => {
  const low = evaluateVerifiedPrecision([
    { correct: true },
    { correct: true },
    { correct: false },
  ]);
  assert.equal(low.meetsGate, false);
  assert.equal(low.gateThreshold, 0.95);
  const high = evaluateVerifiedPrecision(Array.from({ length: 20 }, () => ({ correct: true })));
  assert.equal(high.meetsGate, true);
});

console.log(`\n${passed} tests passed`);
