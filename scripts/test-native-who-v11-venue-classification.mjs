#!/usr/bin/env node
/**
 * Offline venue-locked classification V11 tests.
 */
import assert from "node:assert/strict";
import {
  classifyVenueLockedOpportunity,
  applyVenueLockedPriorityCap,
  VENUE_LOCKED_SUBSTATE,
} from "../lib/group-demand-intelligence/venue-locked-classification-v11.js";
import {
  OPPORTUNITY_TYPE,
  VENUE_SOURCING_STATUS,
  PRIORITY,
} from "../lib/group-demand-intelligence/claim-types.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("A. venue TBD → PRIMARY possible (unchanged)", () => {
  const r = classifyVenueLockedOpportunity({
    opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    venueStatus: "TBD",
    venueSourcingStatus: VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
  });
  assert.equal(r.substate, VENUE_LOCKED_SUBSTATE.NOT_VENUE_LOCKED);
  assert.equal(r.opportunityType, OPPORTUNITY_TYPE.PRIMARY_PURSUIT);
  assert.equal(r.changed, false);
});

test("B. RFP active → PRIMARY possible", () => {
  const r = classifyVenueLockedOpportunity({
    opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    sourcingStatus: "RFP_ACTIVE_SOURCING",
    housingEvidence: "RFP open for hotels",
  });
  assert.equal(r.substate, VENUE_LOCKED_SUBSTATE.NOT_VENUE_LOCKED);
  assert.equal(r.opportunityType, OPPORTUNITY_TYPE.PRIMARY_PURSUIT);
});

test("C. primary hotel selected + overflow evidence → OVERFLOW", () => {
  const r = classifyVenueLockedOpportunity({
    opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    venue: "Competitor Grand Hotel",
    venueWatch: "Venue already Competitor Grand Hotel — overflow/housing path",
    housingEvidence: "Official hotels and overflow housing program listed",
  });
  assert.equal(r.opportunityType, OPPORTUNITY_TYPE.OVERFLOW_HOUSING);
  assert.equal(
    r.venueSourcingStatus,
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE
  );
  assert.equal(r.changed, true);
});

test("D. primary selected + no overflow → not PRIMARY", () => {
  const r = classifyVenueLockedOpportunity({
    opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    venue: "Rival Marriott Downtown",
    venueWatch: "Hosted at Rival Marriott Downtown — primary venue selected",
  });
  assert.notEqual(r.opportunityType, OPPORTUNITY_TYPE.PRIMARY_PURSUIT);
  assert.equal(
    r.venueSourcingStatus,
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE
  );
  assert.equal(
    applyVenueLockedPriorityCap(PRIORITY.HIGH, r),
    PRIORITY.WATCHLIST
  );
});

test("E. fully placed → CLOSED", () => {
  const r = classifyVenueLockedOpportunity({
    opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    venue: "City Convention Hotel",
    housingEvidence: "Fully placed — housing closed",
    venueSourcingStatus: VENUE_SOURCING_STATUS.FULLY_PLACED,
  });
  assert.equal(r.opportunityType, OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED);
  assert.equal(r.substate, VENUE_LOCKED_SUBSTATE.FULLY_PLACED);
});

test("F. meeting venue selected but rooms open → sleeping-room pursuit possible", () => {
  const r = classifyVenueLockedOpportunity({
    opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    venue: "Downtown Convention Center Hotel",
    housingEvidence: "Meeting venue only; sleeping rooms still open for bids",
  });
  assert.equal(r.opportunityType, OPPORTUNITY_TYPE.PRIMARY_PURSUIT);
  assert.equal(r.substate, VENUE_LOCKED_SUBSTATE.MEETING_VENUE_ROOMS_OPEN);
});

let pass = 0;
let fail = 0;
const failures = [];
for (const t of tests) {
  try {
    t.fn();
    pass += 1;
    console.log("PASS", t.name);
  } catch (err) {
    fail += 1;
    failures.push({ name: t.name, error: String(err.message || err) });
    console.log("FAIL", t.name, err.message || err);
  }
}
console.log(JSON.stringify({ suite: "venue-locked-v11", pass, fail, total: tests.length, failures }, null, 2));
process.exit(fail ? 1 : 0);
