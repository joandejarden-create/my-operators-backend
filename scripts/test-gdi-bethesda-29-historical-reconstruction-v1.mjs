/**
 * Tests for Bethesda 29 historical reconstruction (research-stage only).
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_29_ATTESTED_IDS,
} from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import {
  buildBethesda29HistoricalReconstruction,
  stageBethesda29HistoricalReconstruction,
  RECONSTRUCTION_STATUS,
} from "../lib/group-demand-intelligence/research/bethesda-29-historical-reconstruction-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const checks = [];
function check(name, fn) {
  try {
    fn();
    checks.push({ name, ok: true });
  } catch (err) {
    checks.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const oppPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json"
);
const before = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");

const doc = buildBethesda29HistoricalReconstruction({ root: ROOT });

check("all_29_ids_accounted", () => {
  assert.equal(doc.opportunities.length, 29);
  assert.deepEqual(
    doc.opportunities.map((o) => o.id).sort(),
    [...BETHESDA_29_ATTESTED_IDS].sort()
  );
});

check("group_partition_10_8_11", () => {
  assert.equal(doc.counts.localCatering, 10);
  assert.equal(doc.counts.partnerships, 8);
  assert.equal(doc.counts.otherGroupCorporate, 11);
});

check("schema_valid_scaling_fields", () => {
  for (const o of doc.opportunities) {
    assert.equal(
      o.schemaValidation?.scalingCanonical?.ok,
      true,
      `${o.id} missing ${JSON.stringify(o.schemaValidation?.scalingCanonical?.missing)}`
    );
    assert.equal(o.customerVisible, false);
    assert.equal(o.customerFacing, false);
    assert.equal(o.salesReady, false);
    assert.equal(o.publicationApproved, false);
    assert.equal(o.inventedFields, false);
  }
  assert.equal(doc.counts.schemaValid, 29);
});

check("no_sales_ready_or_customer_visibility", () => {
  assert.equal(doc.counts.salesReadyForced, 0);
  assert.equal(doc.counts.customerVisible, 0);
  assert.equal(doc.safety.publicationApproved, false);
  for (const o of doc.opportunities) {
    assert.notEqual(o.commercialMaturity, "SALES_READY");
  }
});

check("overflow_and_partnership_watches_preserved", () => {
  assert.equal(doc.dispositions.OVERFLOW_WATCH, 5);
  assert.equal(doc.dispositions.PARTNERSHIP_WATCH, 8);
});

check("reconstruction_statuses_honest", () => {
  const statuses = new Set(doc.opportunities.map((o) => o.reconstructionStatus));
  for (const s of statuses) {
    assert.ok(Object.values(RECONSTRUCTION_STATUS).includes(s));
  }
  assert.ok(doc.counts.reconstructedRecords === 29);
});

check("no_duplicate_of_canonical_59", () => {
  assert.equal(doc.counts.duplicateOfCanonical, 0);
});

const staged = stageBethesda29HistoricalReconstruction({
  root: ROOT,
  writeFixture: true,
  writeMemoryCheckpoints: true,
});

check("stage_persist_ok_and_idempotent", () => {
  assert.equal(staged.ok, true);
  assert.equal(staged.hotelStoreUntouched, true);
  const again = stageBethesda29HistoricalReconstruction({
    root: ROOT,
    writeFixture: true,
    writeMemoryCheckpoints: false,
  });
  assert.equal(again.ok, true);
  assert.equal(again.persist?.count, 29);
});

check("canonical_store_hash_unchanged", () => {
  const after = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");
  assert.equal(after, before);
  assert.equal(
    before,
    "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad"
  );
});

check("share_registry_untouched_contract", () => {
  assert.equal(doc.safety.shareRegistryWrites, false);
});

const failed = checks.filter((c) => !c.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      before,
      counts: doc.counts,
      dispositions: doc.dispositions,
      checks,
    },
    null,
    2
  )
);
if (failed.length) process.exit(1);
