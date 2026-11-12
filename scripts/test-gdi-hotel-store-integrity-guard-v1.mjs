#!/usr/bin/env node
/**
 * Zero-paid integrity guard tests — no live store writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  BETHESDA_EXPECTED_CANONICAL_COUNT,
  BETHESDA_EXPECTED_UNION_COUNT,
  VERIFIED_59_ROW_SHA256,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  compareLocalVsCandidateStore,
  dryRunMergeProtectedBodies,
  loadHotelOpportunityDoc,
  sha256File,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SOURCE =
  "C:/Dev/dealality/authorized-backups/primary-working-state-e2ae6c7d3-20261111/files/data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json";
const BODIES_DIR = path.join(
  ROOT,
  "reports/gdi-bethesda-sc-integrity-repair-v1/01-recovered-bodies"
);

assert.equal(BETHESDA_PROTECTED_SCALING_IDS.length, 5);
assert.equal(BETHESDA_EXPECTED_CANONICAL_COUNT, 59);
assert.equal(BETHESDA_EXPECTED_UNION_COUNT, 88);

const current = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root: ROOT });
assert.equal(current.exists, true);

// Guard refuses protected-ID loss when stripping the five SC IDs.
const stripped = current.opportunities.filter(
  (r) => !BETHESDA_PROTECTED_SCALING_IDS.includes(r.id || r.opportunityId)
);
const loss = assertNoProtectedIdLoss({
  hotelId: BETHESDA_HOTEL_ID,
  nextOpportunities: stripped,
  protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
});
assert.equal(loss.ok, false);
assert.equal(loss.code, "PROTECTED_ID_LOSS_REFUSED");
assert.equal(loss.missing.length, 5);

// Guard refuses count drop.
const drop = assertCountNotDropped({
  hotelId: BETHESDA_HOTEL_ID,
  priorCount: 59,
  nextCount: 54,
});
assert.equal(drop.ok, false);

// After restore: local must hold all five protected IDs and refuse lean git replacement.
const presentGate = assertNoProtectedIdLoss({
  hotelId: BETHESDA_HOTEL_ID,
  nextOpportunities: current.opportunities,
  protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
});
assert.equal(presentGate.ok, true);

const richerCompare = compareLocalVsCandidateStore({
  hotelId: BETHESDA_HOTEL_ID,
  candidateOpportunities: stripped,
  protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  root: ROOT,
});
assert.equal(richerCompare.refuseReplace, true);
assert.equal(richerCompare.protectedLost.length, 5);

// Recovered bodies must exist (from dry-run or authorized backup).
assert.equal(fs.existsSync(SOURCE), true, "verified 59-row backup must be present");
assert.equal(sha256File(SOURCE), VERIFIED_59_ROW_SHA256);

const source = JSON.parse(fs.readFileSync(SOURCE, "utf8"));
const bodies = BETHESDA_PROTECTED_SCALING_IDS.map((id) => {
  const row = (source.opportunities || []).find((r) => (r.id || r.opportunityId) === id);
  assert.ok(row, `missing body ${id}`);
  return row;
});

const dry = dryRunMergeProtectedBodies({
  hotelId: BETHESDA_HOTEL_ID,
  recoveredBodies: bodies,
  protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  root: ROOT,
});
assert.equal(dry.ok, true);
assert.equal(dry.proposedCount, BETHESDA_EXPECTED_CANONICAL_COUNT);
// After live restore, dry-run adds 0 (already present) but still ends at 59.
assert.ok(dry.added.length === 0 || dry.added.length === 5);
assert.equal(dry.writeAuthorized, false);

// Live store + research-stage union must be 88 after approved restore.
assert.equal(current.count, BETHESDA_EXPECTED_CANONICAL_COUNT);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  current.opportunities,
  { root: ROOT }
);
assert.equal(union.counts.canonical, 59);
assert.equal(union.counts.historicalAdded, 29);
assert.equal(union.counts.union, 88);

// Exa three still present in research-stage.
const stagePath = path.join(
  ROOT,
  "data/group-demand-intelligence/research-stage/recLuxvwwxID7U2B8/reconstructed-opportunities-v1.json"
);
assert.equal(fs.existsSync(stagePath), true);
const stage = JSON.parse(fs.readFileSync(stagePath, "utf8"));
assert.equal((stage.opportunities || []).length, 29);
for (const id of [
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "gdi_pep_live_c165c897d233",
]) {
  const row = stage.opportunities.find((r) => r.id === id);
  assert.ok(row, id);
  assert.equal(row.exaAuthorizedBatch?.structured != null, true);
}

// Spice Island store unchanged presence check (no mutation).
const spice = loadHotelOpportunityDoc("recKRJjcPnb4tVDDS", { root: ROOT });
assert.equal(spice.exists, true);
assert.ok(spice.count > 0);

// Recovered body files should exist after dry-run (local reports; may be gitignored).
if (fs.existsSync(BODIES_DIR)) {
  for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
    assert.equal(fs.existsSync(path.join(BODIES_DIR, `${id}.json`)), true);
  }
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-hotel-store-integrity-guard-v1",
      currentCount: current.count,
      dryRunProposedCount: dry.proposedCount,
      simulatedUnion: union.counts.union,
      writePerformed: false,
      paidCalls: 0,
    },
    null,
    2
  )
);
