#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  HOTEL_PROCESSING_CLASS,
  DEMAND_MIX_POSTURE,
  runHotelOperationalPreflight,
  runHotelOperationalPreflightBatch,
  prioritizeDiscoverySegments,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-operational-preflight-v1.js";
import {
  DESIGN_HOTELS_PILOT_COHORT_V1,
  MEMMO_PRINCIPE_REAL_TARGET_V1_1,
  ANTIGUA_REMOVED_FROM_COHORT_ONLY,
} from "./gdi-design-hotels-operational-pilot-v1.mjs";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("fourteen_segments_universe", () => {
  const ids = listFourteenWorkflowSegmentIds();
  assert.equal(ids.length, 14);
  const ordered = prioritizeDiscoverySegments(DEMAND_MIX_POSTURE.URBAN_MEETINGS);
  assert.equal(ordered.universeCount, 14);
  assert.equal(ordered.prioritized.length, 14);
});

check("meeting_facilities_not_required", () => {
  const pf = runHotelOperationalPreflight("rec1t35e3sPerzxYu");
  assert.equal(pf.checks.roomsAndMeetingSpace.meetingFacilitiesRequired, false);
  assert.equal(pf.readyForDryRun, true);
  assert.equal(pf.processingClass, HOTEL_PROCESSING_CLASS.NEW_HOTEL);
  assert.equal(pf.hotel.roomsCount, 41);
  assert.equal(pf.hotel.marshaCode, "LISDM");
});

check("design_hotels_cohort_preflight", () => {
  const ids = DESIGN_HOTELS_PILOT_COHORT_V1.map((h) => h.hpcId);
  assert.ok(ids.every(Boolean), "runnable cohort must use verified HPC IDs only");
  assert.deepEqual(ids, [
    "rece0NdV9FCnaFlVS",
    "recaokE8psIbuVeuH",
    "rec1t35e3sPerzxYu",
  ]);
  assert.ok(!ids.includes("recGL1epYhEle5SDM"), "Antigua must not be in runnable cohort");
  const batch = runHotelOperationalPreflightBatch(ids);
  assert.equal(batch.count, 3);
  assert.equal(batch.readyCount, 3);
  assert.equal(batch.blockedCount, 0);
  const byId = Object.fromEntries(batch.results.map((r) => [r.hotelId, r]));
  assert.equal(byId.rece0NdV9FCnaFlVS.demandMixPosture, DEMAND_MIX_POSTURE.URBAN_MEETINGS);
  assert.equal(
    byId.recaokE8psIbuVeuH.demandMixPosture,
    DEMAND_MIX_POSTURE.LEISURE_DESTINATION
  );
  assert.equal(
    byId.rec1t35e3sPerzxYu.demandMixPosture,
    DEMAND_MIX_POSTURE.BOUTIQUE_LIMITED_MEETINGS
  );
  for (const r of batch.results) {
    assert.equal(r.hotel.affiliation, "Design Hotels");
    assert.equal(r.publication.authorized, false);
    assert.equal(r.persistence.airtableWritesAuthorized, false);
    assert.equal(r.processingClass, HOTEL_PROCESSING_CLASS.NEW_HOTEL);
  }
});

check("memmo_europe_hpc_verified_enrolled", () => {
  assert.equal(MEMMO_PRINCIPE_REAL_TARGET_V1_1.hpcId, "rec1t35e3sPerzxYu");
  assert.equal(MEMMO_PRINCIPE_REAL_TARGET_V1_1.marriottMarsha, "LISDM");
  assert.equal(MEMMO_PRINCIPE_REAL_TARGET_V1_1.status, "HPC_VERIFIED_ENROLLED");
  assert.equal(
    DESIGN_HOTELS_PILOT_COHORT_V1[2].cohortRole,
    "HOTEL_3_EUROPE_VERIFIED"
  );
  assert.equal(ANTIGUA_REMOVED_FROM_COHORT_ONLY.hpcRecordDeleted, false);
  assert.equal(ANTIGUA_REMOVED_FROM_COHORT_ONLY.hpcId, "recGL1epYhEle5SDM");
});

check("invalid_hotel_incomplete", () => {
  const pf = runHotelOperationalPreflight("not-a-rec");
  assert.equal(pf.readyForDryRun, false);
  assert.equal(pf.processingClass, HOTEL_PROCESSING_CLASS.CONFIGURATION_INCOMPLETE);
});

console.log(JSON.stringify({ suite: "gdi-hotel-operational-preflight-v1", passed, failed }, null, 2));
process.exit(failed ? 1 : 0);
