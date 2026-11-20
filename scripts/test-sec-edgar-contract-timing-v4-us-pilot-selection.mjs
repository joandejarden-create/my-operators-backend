#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  isUnitedStatesHotel,
  scoreUsPilotCandidate,
  classifyPreHpc,
  classifyAfterCollision,
  aggregateFindingsToHotels,
  selectUsPilotCandidates,
  PILOT_DISPOSITION,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v4-us-pilot-selection-v1.js";

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

test("us_state_detection", () => {
  assert.equal(isUnitedStatesHotel({ hotelName: "Hampton Inn X", city: "Austin", stateRegion: "TX" }), true);
  assert.equal(isUnitedStatesHotel({ hotelName: "Hotel Y", city: "Paris", stateRegion: "IDF", country: "France" }), false);
});

test("rejects_rv_and_generic", () => {
  const scored = scoreUsPilotCandidate({
    hotelName: "Maple Grove RV Resort",
    city: "Everett",
    stateRegion: "WA",
    snippetSample: "Manufactured Housing",
    milestoneCount: 1,
    hotelAttributed: true,
  });
  assert.ok(scored.penalties.includes("non_hotel_asset_class"));
  assert.equal(classifyPreHpc({}, scored), PILOT_DISPOSITION.REJECT_AMBIGUOUS);
});

test("rejects_street_address_as_hotel_name", () => {
  const scored = scoreUsPilotCandidate({
    hotelName: "7538 East Hampton Avenue",
    city: "Mesa",
    stateRegion: "AZ",
    brandFamily: "HILTON",
    brandLabel: "Hampton",
    addressHint: "7538 East Hampton Avenue",
    milestoneCount: 1,
    hotelAttributed: true,
  });
  assert.ok(scored.penalties.includes("street_address_as_name"));
  assert.equal(classifyPreHpc({}, scored), PILOT_DISPOSITION.REJECT_AMBIGUOUS);
});

test("strong_candidate_pending_collision", () => {
  const hotel = {
    hotelName: "Hampton Inn Mobile East Bay Daphne",
    city: "Daphne",
    stateRegion: "AL",
    brandLabel: "Hampton Inn",
    brandFamily: "HILTON",
    addressHint: "123 Main St",
    milestoneCount: 1,
    futureMilestoneCount: 1,
    hotelAttributed: true,
    portfolioWide: false,
    loanOnly: false,
  };
  const scored = scoreUsPilotCandidate(hotel);
  assert.ok(scored.score >= 55);
  assert.equal(classifyPreHpc(hotel, scored), "CREATE_READY_PENDING_COLLISION");
});

test("collision_verified_absent_promotes_create_ready", () => {
  assert.equal(
    classifyAfterCollision("CREATE_READY_PENDING_COLLISION", {
      outcome: "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT",
    }),
    PILOT_DISPOSITION.CREATE_READY,
  );
  assert.equal(
    classifyAfterCollision("CREATE_READY_PENDING_COLLISION", {
      outcome: "HPC_EXISTING_RECORD_VERIFIED",
    }),
    PILOT_DISPOSITION.LINK_EXISTING,
  );
});

test("does_not_pad_create_ready", () => {
  const findings = [
    {
      findingId: "a",
      hotelName: "Generic Property",
      city: "X",
      stateRegion: "TX",
      isHotelContractDate: true,
      isLoanMaturity: false,
      primaryDate: "2030-01-01",
      dateMeaning: "FRANCHISE_EXPIRATION",
      hotelAttributed: true,
      portfolioWide: false,
    },
  ];
  const hotels = aggregateFindingsToHotels(findings);
  const sel = selectUsPilotCandidates(hotels, { targetMin: 25, targetMax: 50 });
  assert.ok(sel.ranked.length < 25);
  assert.equal(
    sel.ranked.filter((r) => r.disposition === PILOT_DISPOSITION.CREATE_READY).length,
    0,
  );
});

console.log(`\n${passed} tests passed`);
