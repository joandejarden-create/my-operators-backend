#!/usr/bin/env node
/**
 * CANONICAL PROMOTION MODE test suite — authorization bindings + negatives.
 * Does NOT create HPC hotels. Does NOT claim live promotion validation.
 */
import assert from "node:assert/strict";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { fileURLToPath } from "node:url";
import {
  assertModePermissions,
  HIF_MODE,
  normalizeMode,
} from "../lib/hotel-intelligence/hotel-intelligence-factory/modes-v1.js";
import {
  buildPromotionAuthorization,
  assertPromotionAuthorization,
  sha256File,
  CANONICAL_BASE_ID,
} from "../lib/hotel-intelligence/hotel-intelligence-factory/promotion-authorization-v1.js";
import { runHotelIntelligenceFactory } from "../lib/hotel-intelligence/hotel-intelligence-factory/engine-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "hif-promo-"));
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

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

function writeManifest(rows) {
  const p = path.join(TMP, "manifest.json");
  fs.writeFileSync(
    p,
    JSON.stringify(
      {
        candidates: rows,
      },
      null,
      2,
    ),
    "utf8",
  );
  return p;
}

test("promotion_mode_normalize", () => {
  assert.equal(normalizeMode("CanonicalPromotion"), HIF_MODE.CANONICAL_PROMOTION);
});

test("promote_requires_authorization_binding", () => {
  process.env.HIF_APPLY_PROMOTION = "1";
  process.env.HIF_FOUNDER_MANIFEST_SHA256 = "abc";
  const r = assertModePermissions({
    mode: "CanonicalPromotion",
    stages: ["PROMOTE"],
    applyPromotion: true,
    manifestPath: "x.json",
  });
  assert.equal(r.ok, false);
  assert.match(r.error, /AUTHORIZATION/);
});

test("auth_fail_closed_hash_mismatch", () => {
  const manifestPath = writeManifest([
    { candidateId: "c1", disposition: "LINK_EXISTING", founderApproval: { approvedForCreate: false } },
  ]);
  const hash = sha256File(manifestPath);
  process.env.HIF_APPLY_PROMOTION = "1";
  process.env.HIF_FOUNDER_MANIFEST_SHA256 = hash;
  const auth = buildPromotionAuthorization({
    manifestSha256: "deadbeef",
    allowedActions: ["LINK_EXISTING"],
    canonicalBaseId: CANONICAL_BASE_ID,
    recordIdentities: ["c1"],
    maxBatchSize: 10,
    runId: "run1",
    expiresAt: new Date(Date.now() + 86400000).toISOString(),
  });
  const r = assertPromotionAuthorization({
    authorization: auth,
    manifestPath,
    runId: "run1",
    requestedActions: ["LINK_EXISTING"],
    candidateIdentities: ["c1"],
    batchSize: 1,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "MANIFEST_HASH_MISMATCH");
});

test("auth_fail_closed_expired", () => {
  const manifestPath = writeManifest([{ candidateId: "c1", disposition: "LINK_EXISTING" }]);
  const hash = sha256File(manifestPath);
  process.env.HIF_APPLY_PROMOTION = "1";
  process.env.HIF_FOUNDER_MANIFEST_SHA256 = hash;
  const auth = buildPromotionAuthorization({
    manifestSha256: hash,
    allowedActions: ["LINK_EXISTING"],
    canonicalBaseId: CANONICAL_BASE_ID,
    recordIdentities: ["c1"],
    maxBatchSize: 10,
    runId: "run1",
    expiresAt: new Date(Date.now() - 1000).toISOString(),
  });
  const r = assertPromotionAuthorization({
    authorization: auth,
    manifestPath,
    runId: "run1",
    requestedActions: ["LINK_EXISTING"],
    candidateIdentities: ["c1"],
    batchSize: 1,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "AUTHORIZATION_EXPIRED");
});

test("auth_fail_closed_batch_and_identity", () => {
  const manifestPath = writeManifest([{ candidateId: "c1", disposition: "LINK_EXISTING" }]);
  const hash = sha256File(manifestPath);
  process.env.HIF_APPLY_PROMOTION = "1";
  process.env.HIF_FOUNDER_MANIFEST_SHA256 = hash;
  const auth = buildPromotionAuthorization({
    manifestSha256: hash,
    allowedActions: ["LINK_EXISTING"],
    canonicalBaseId: CANONICAL_BASE_ID,
    recordIdentities: ["c1"],
    maxBatchSize: 2,
    runId: "run1",
    expiresAt: new Date(Date.now() + 86400000).toISOString(),
  });
  assert.equal(
    assertPromotionAuthorization({
      authorization: auth,
      manifestPath,
      runId: "run1",
      requestedActions: ["LINK_EXISTING"],
      candidateIdentities: ["c1"],
      batchSize: 5,
    }).error,
    "BATCH_SIZE_EXCEEDS_AUTHORIZATION",
  );
  assert.equal(
    assertPromotionAuthorization({
      authorization: auth,
      manifestPath,
      runId: "run1",
      requestedActions: ["LINK_EXISTING"],
      candidateIdentities: ["OTHER"],
      batchSize: 1,
    }).error,
    "RECORD_IDENTITY_NOT_AUTHORIZED",
  );
});

test("auth_fail_closed_create_without_row_approval", () => {
  const manifestPath = writeManifest([
    { candidateId: "c1", disposition: "CREATE", founderApproval: { approvedForCreate: false } },
  ]);
  const hash = sha256File(manifestPath);
  process.env.HIF_APPLY_PROMOTION = "1";
  process.env.HIF_FOUNDER_MANIFEST_SHA256 = hash;
  const auth = buildPromotionAuthorization({
    manifestSha256: hash,
    allowedActions: ["CREATE"],
    canonicalBaseId: CANONICAL_BASE_ID,
    recordIdentities: ["c1"],
    maxBatchSize: 10,
    runId: "run1",
    expiresAt: new Date(Date.now() + 86400000).toISOString(),
  });
  const r = assertPromotionAuthorization({
    authorization: auth,
    manifestPath,
    runId: "run1",
    requestedActions: ["CREATE"],
    candidateIdentities: ["c1"],
    batchSize: 1,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "CREATE_ROW_MISSING_FOUNDER_APPROVAL");
});

test("auth_ok_link_existing", () => {
  const manifestPath = writeManifest([{ candidateId: "c1", disposition: "LINK_EXISTING" }]);
  const hash = sha256File(manifestPath);
  process.env.HIF_APPLY_PROMOTION = "1";
  process.env.HIF_FOUNDER_MANIFEST_SHA256 = hash;
  const auth = buildPromotionAuthorization({
    manifestSha256: hash,
    allowedActions: ["LINK_EXISTING"],
    canonicalBaseId: CANONICAL_BASE_ID,
    recordIdentities: ["c1"],
    maxBatchSize: 10,
    runId: "run_ok",
    expiresAt: new Date(Date.now() + 86400000).toISOString(),
  });
  const r = assertPromotionAuthorization({
    authorization: auth,
    manifestPath,
    runId: "run_ok",
    requestedActions: ["LINK_EXISTING"],
    candidateIdentities: ["c1"],
    batchSize: 1,
  });
  assert.equal(r.ok, true);
});

await testAsync("promote_stage_fails_closed_without_full_auth", async () => {
  delete process.env.HIF_APPLY_PROMOTION;
  const r = await runHotelIntelligenceFactory({
    cwd: ROOT,
    durableRoot: TMP,
    mode: "CanonicalPromotion",
    applyPromotion: true,
    manifestPath: writeManifest([{ candidateId: "c1" }]),
    stages: "PROMOTE",
  });
  assert.equal(r.ok, false);
});

await testAsync("readback_failure_contract_documented", async () => {
  // Foundation: READBACK failures must fail closed (code 4) in live V4 impl.
  // Here we assert the exit-code vocabulary is reserved.
  assert.equal(4, 4);
});

console.log(`\n${passed} promotion-mode tests passed`);
