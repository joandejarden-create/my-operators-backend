#!/usr/bin/env node
/**
 * Dry-run / fixture validation for Track C V7 relationship map.
 * No Airtable writes. No HPC creates.
 */
import assert from "node:assert/strict";
import {
  TRACK_A_CALA_V1_FINDING_IDS,
  TRACK_C_SYNTHETIC_FINDING_IDS,
  PARENT_FINN_INTERNALIZATION_FINDING_ID,
  VERIFIED_MEXICO_HOTELS_V7,
  DEFERRED_ENTITIES_V7,
  resolveDealSignalFindingId,
  getCourtyardWyndhamStewardship,
  listHeDeepLinkTargets,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-v7-dealsignal-relationship-map-v1.js";

function testSyntheticRedirect() {
  for (const id of TRACK_C_SYNTHETIC_FINDING_IDS) {
    const r = resolveDealSignalFindingId(id);
    assert.equal(r.ok, true);
    assert.equal(r.resolution, "SYNTHETIC_TO_PARENT");
    assert.equal(r.resolvedFindingId, PARENT_FINN_INTERNALIZATION_FINDING_ID);
    assert.equal(r.createFinding, false);
  }
}

function testCorpusDirect() {
  for (const id of TRACK_A_CALA_V1_FINDING_IDS) {
    const r = resolveDealSignalFindingId(id);
    assert.equal(r.ok, true);
    assert.equal(r.resolution, "CORPUS_DIRECT");
    assert.equal(r.resolvedFindingId, id);
    assert.equal(r.createFinding, false);
  }
}

function testNoDuplicateFindingCreation() {
  const all = [...TRACK_C_SYNTHETIC_FINDING_IDS, ...TRACK_A_CALA_V1_FINDING_IDS];
  for (const id of all) {
    assert.equal(resolveDealSignalFindingId(id).createFinding, false);
  }
  assert.equal(resolveDealSignalFindingId("find_invented_new_for_nav_2026").ok, false);
}

function testHotelMapIntegrity() {
  assert.equal(VERIFIED_MEXICO_HOTELS_V7.length, 4);
  const hpc = new Set(VERIFIED_MEXICO_HOTELS_V7.map((h) => h.canonicalHpcRecordId));
  assert.equal(hpc.size, 4, "no duplicate canonical HPC");
  for (const h of VERIFIED_MEXICO_HOTELS_V7) {
    assert.ok(h.discoveryPropertyId.startsWith("disc_"));
    assert.ok(h.canonicalHpcRecordId.startsWith("rec"));
    if (h.findingAlignment === "SYNTHETIC_REDIRECT_TO_PARENT") {
      assert.equal(h.dealsignalFindingId, null);
      assert.equal(h.dealsignalEvidenceFindingId, PARENT_FINN_INTERNALIZATION_FINDING_ID);
      assert.ok(TRACK_C_SYNTHETIC_FINDING_IDS.includes(h.syntheticFindingIdNote));
    } else {
      assert.ok(TRACK_A_CALA_V1_FINDING_IDS.includes(h.dealsignalFindingId));
    }
  }
}

function testCourtyardStewardship() {
  const s = getCourtyardWyndhamStewardship();
  assert.equal(s.canonicalCurrentHpc, "rechcqksn3VfFD3c6");
  assert.equal(s.historicalHpc, "recf65FZ1FuVVqIrz");
  assert.notEqual(s.canonicalCurrentHpc, s.historicalHpc);
  assert.ok(s.forbiddenWithoutFounderApproval.includes("MERGE_HPC"));
  assert.ok(s.forbiddenWithoutFounderApproval.includes("DELETE_HPC"));
  assert.equal(s.recommendedAction, "STEWARD_ALIAS_DOCUMENTATION");
}

function testDeferredNotHotels() {
  for (const d of DEFERRED_ENTITIES_V7) {
    assert.ok(["PARENT_ONLY", "UNRESOLVED_HOTEL_IDENTITY"].includes(d.status));
  }
}

function testHeTargets() {
  const targets = listHeDeepLinkTargets();
  assert.equal(targets.length, 4);
  for (const t of targets) {
    assert.match(t.researchApiPath, /^\/api\/hotel-intelligence\/hotels\/rec/);
  }
}

function main() {
  testSyntheticRedirect();
  testCorpusDirect();
  testNoDuplicateFindingCreation();
  testHotelMapIntegrity();
  testCourtyardStewardship();
  testDeferredNotHotels();
  testHeTargets();
  console.log(
    JSON.stringify(
      {
        ok: true,
        suite: "test-hif-track-c-v7-relationship-integrity-v1",
        airtableWrites: 0,
        hpcCreates: 0,
      },
      null,
      2,
    ),
  );
}

main();
