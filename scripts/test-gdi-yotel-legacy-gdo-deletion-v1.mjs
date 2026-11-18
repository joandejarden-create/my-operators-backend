#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  YOTEL_LEGACY_DELETE_AUTH,
  preflightYotelLegacyDeletion,
  chunkIds,
  remainingDeleteIds,
  applyBatchResultToLedger,
  buildDeletionLedgerSkeleton,
  assertAuthorizedYotelDelete,
  fingerprintIds,
} from "../lib/group-demand-intelligence/workflow/gdi-yotel-legacy-gdo-deletion-v1.js";

const allow = {
  airtableRecordIdsExact: ["recA", "recB", "recC"],
};

// exact match pass
{
  const pf = preflightYotelLegacyDeletion({
    allowlist: {
      ...allow,
      airtableRecordIdsExact: Array.from({ length: 108 }, (_, i) => `rec${i}`),
    },
    liveRawRecords: Array.from({ length: 108 }, (_, i) => ({
      id: `rec${i}`,
      fields: { hotelId: YOTEL_LEGACY_DELETE_AUTH.hotelId, opportunityId: `opp_${i}` },
    })),
    localOpportunityCount: 42,
  });
  assert.equal(pf.ok, true);
  assert.equal(pf.deletableIds.length, 108);
}

// cross-hotel blocked
{
  const pf = preflightYotelLegacyDeletion({
    allowlist: { airtableRecordIdsExact: ["recA"] },
    liveRawRecords: [
      { id: "recA", fields: { hotelId: "recOTHER", opportunityId: "x" } },
    ],
    localOpportunityCount: 42,
  });
  assert.equal(pf.ok, false);
  assert.ok(pf.hotelMismatches.length);
}

// live not in allowlist blocked
{
  const pf = preflightYotelLegacyDeletion({
    allowlist: { airtableRecordIdsExact: ["recA"] },
    liveRawRecords: [
      { id: "recA", fields: { hotelId: YOTEL_LEGACY_DELETE_AUTH.hotelId } },
      { id: "recNEW", fields: { hotelId: YOTEL_LEGACY_DELETE_AUTH.hotelId } },
    ],
    localOpportunityCount: 42,
  });
  assert.equal(pf.ok, false);
  assert.equal(pf.liveNotInAllowlist.length, 1);
}

// local 42 required
{
  const pf = preflightYotelLegacyDeletion({
    allowlist: {
      airtableRecordIdsExact: Array.from({ length: 108 }, (_, i) => `rec${i}`),
    },
    liveRawRecords: Array.from({ length: 108 }, (_, i) => ({
      id: `rec${i}`,
      fields: { hotelId: YOTEL_LEGACY_DELETE_AUTH.hotelId },
    })),
    localOpportunityCount: 41,
  });
  assert.equal(pf.ok, false);
  assert.equal(pf.localBaselineOk, false);
}

// chunk + resume
assert.deepEqual(chunkIds(["a", "b", "c", "d", "e"], 2).map((c) => c.length), [2, 2, 1]);
assert.deepEqual(remainingDeleteIds(["a", "b", "c"], ["b"]), ["a", "c"]);

{
  let ledger = buildDeletionLedgerSkeleton({
    allowlist: { airtableRecordIdsExact: ["a", "b"] },
    preflight: { ok: true },
    backupPath: "/tmp/x",
  });
  ledger = applyBatchResultToLedger(ledger, { ok: true, ids: ["a"], index: 0 });
  assert.deepEqual(ledger.deletedIds, ["a"]);
  ledger = applyBatchResultToLedger(ledger, {
    ok: false,
    ids: ["b"],
    index: 1,
    error: "x",
  });
  assert.equal(ledger.failed.length, 1);
}

assert.throws(
  () => assertAuthorizedYotelDelete({ authorizeYotelLegacyDelete: false }),
  (e) => e.code === "YOTEL_LEGACY_DELETE_NOT_AUTHORIZED",
);
assert.throws(
  () =>
    assertAuthorizedYotelDelete({
      authorizeYotelLegacyDelete: true,
      exactWriteSetAuthorization: "wrong",
      apply: true,
    }),
  (e) => e.code === "YOTEL_DELETE_AUTH_ID_MISMATCH",
);
assert.equal(
  assertAuthorizedYotelDelete({
    authorizeYotelLegacyDelete: true,
    exactWriteSetAuthorization: YOTEL_LEGACY_DELETE_AUTH.authorizationId,
    apply: true,
  }),
  true,
);

assert.equal(fingerprintIds(["b", "a"]), fingerprintIds(["a", "b"]));

console.log(
  JSON.stringify({ ok: true, suite: "test-gdi-yotel-legacy-gdo-deletion-v1" }, null, 2),
);
