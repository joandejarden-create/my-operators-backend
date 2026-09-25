/**
 * Guard: qualification recommended actions must not hardcode Bethesda for other hotels.
 */
import assert from "node:assert/strict";
import { enrichQualificationPrecision } from "../lib/group-demand-intelligence/qualification-precision.js";
import { buildOpportunity } from "../lib/group-demand-intelligence/opportunity-factory.js";

const REN = "recG66DQJKP2c0UNh";

const precision = enrichQualificationPrecision({
  hotelId: REN,
  hotelName: "Renaissance New York Times Square Hotel",
  title: "Sample Association Annual Meeting",
  organizationName: "Sample Association",
  venueStatus: "Hotel TBD — sourcing open",
  destinationStatus: "New York, NY",
  bookingWindowStatus: "CONTACT_NOW",
  primaryContact: { name: "Alex Planner" },
  demandTerritoryFit: "TERRITORY_CORE",
  venueSourcingStatus: "OPEN_UNRESOLVED",
  roomDemandStatus: "ESTIMATED_ROOM_DEMAND",
  estimatedPeakRooms: 80,
});

const action = precision.recommendedAction;
assert.ok(action, "recommendedAction present");
assert.ok(!/bethesda marriott/i.test(action), `bleed in action: ${action}`);
assert.ok(!/nih \/ metro/i.test(action), `nih bleed in action: ${action}`);
assert.equal(precision.opportunityType, "PRIMARY_PURSUIT", precision.opportunityType);
assert.ok(
  /renaissance new york times square hotel/i.test(action),
  `missing hotel name: ${action}`
);

const opp = buildOpportunity({
  hotelId: REN,
  organizationName: "Sample Association",
  title: "Sample Meeting",
  demandTerritoryFit: "TERRITORY_CORE",
  demandTerritoryRationale: "Core Midtown demand",
  venueStatus: "TBD",
  destinationStatus: "New York, NY",
});
const labels = (opp.knownVsEstimated?.inferred || []).map((x) => x.field);
assert.ok(!labels.includes("DMV competitiveness"), `DMV label still present: ${labels}`);
assert.ok(
  labels.includes("Demand territory rationale"),
  `generic label missing: ${labels}`
);

console.log("PASS bethesda_action_bleed_guard");
