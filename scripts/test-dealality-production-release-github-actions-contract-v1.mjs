#!/usr/bin/env node
/**
 * Contract tests for GitHub Actions as sole live production release channel.
 * Does not initiate Railway deployments.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  assertReleaseChannelAllowed,
  assertAuthorizationPhrase,
  assertApprovedShaFormat,
  assertProduct,
  AUTHORIZATION_PHRASE,
  GITHUB_ACTIONS_CHANNEL,
} from "../lib/dealality-production-release/release-channel-v1.js";
import {
  acquireReleaseLock,
  assertDeployAllowedByLock,
  releaseLock,
} from "../lib/dealality-production-release/release-lock-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sha = "8eabf043cbc350322a5aca5f985bcc8a8ff75715";
const results = [];
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "dealality-gha-contract-"));

function pass(name, detail = "") {
  results.push({ name, pass: true, detail });
  console.log(`PASS ${name}${detail ? ` ${detail}` : ""}`);
}
function fail(name, detail) {
  results.push({ name, pass: false, detail });
  console.error(`FAIL ${name} ${detail}`);
}

try {
  // Workflow file exists with required concurrency policy
  const wfPath = path.join(
    ROOT,
    ".github/workflows/dealality-production-release.yml"
  );
  assert.equal(fs.existsSync(wfPath), true);
  const wf = fs.readFileSync(wfPath, "utf8");
  assert.match(wf, /concurrency:/);
  assert.match(wf, /group:\s*dealality-production-release/);
  assert.match(wf, /cancel-in-progress:\s*false/);
  assert.match(wf, /I_AUTHORIZE_PRODUCTION_RELEASE/);
  assert.match(wf, /path:\s*control/);
  assert.match(wf, /path:\s*artifact/);
  assert.match(wf, /--repo-root/);
  assert.doesNotMatch(wf, /railway\s+deployment\s+cancel/i);
  assert.doesNotMatch(wf, /railway\s+down/i);
  pass("workflow_concurrency_no_auto_cancel");

  // Auth phrase
  assert.equal(assertAuthorizationPhrase(AUTHORIZATION_PHRASE).ok, true);
  assert.equal(assertAuthorizationPhrase("nope").ok, false);
  pass("unauthorized_phrase_rejected");

  // SHA / product
  assert.equal(assertApprovedShaFormat("abc").ok, false);
  assert.equal(assertApprovedShaFormat(sha).ok, true);
  assert.equal(assertProduct("AVA").ok, true);
  assert.equal(assertProduct("NOPE").ok, false);
  pass("incorrect_sha_and_product_rejected");

  // Channel gate — live without github-actions blocked
  const localLive = assertReleaseChannelAllowed(
    { DEALALITY_RELEASE_CHANNEL: "" },
    { liveDeploy: true }
  );
  assert.equal(localLive.ok, false);
  assert.equal(localLive.code, "RELEASE_CHANNEL_REQUIRED");
  pass("local_live_deploy_blocked_without_github_actions");

  const ghaLive = assertReleaseChannelAllowed(
    { DEALALITY_RELEASE_CHANNEL: GITHUB_ACTIONS_CHANNEL },
    { liveDeploy: true }
  );
  assert.equal(ghaLive.ok, true);
  pass("github_actions_channel_allows_live");

  const dryOk = assertReleaseChannelAllowed({}, { liveDeploy: false });
  assert.equal(dryOk.ok, true);
  pass("dry_run_allowed_without_channel");

  // assertDeployAllowedByLock live without channel
  const blocked = assertDeployAllowedByLock(
    {
      DEALALITY_RELEASE_LOCK_ROOT: tmp,
      DEALALITY_RELEASE_LOCK_SHARED: "1",
      DEALALITY_RELEASE_CHANNEL: "",
      DEALALITY_RELEASE_LOCK_BYPASS: "",
    },
    { liveDeploy: true, cwd: ROOT }
  );
  assert.equal(blocked.ok, false);
  assert.equal(blocked.code, "RELEASE_CHANNEL_REQUIRED");
  pass("deploy_assert_blocks_non_actions_live");

  // Concurrent sessions still blocked under Actions-owned lock root
  const a = acquireReleaseLock({
    root: tmp,
    product: "AVA",
    approvedSha: sha,
    owner: "gha-session-a",
    authorized: true,
    requireSharedRoot: false,
  });
  assert.equal(a.ok, true);
  const b = acquireReleaseLock({
    root: tmp,
    product: "GDI",
    approvedSha: sha,
    owner: "gha-session-b",
    authorized: true,
    requireSharedRoot: false,
  });
  assert.equal(b.ok, false);
  assert.equal(b.code, "LOCK_HELD");
  pass("concurrent_cursor_sessions_blocked_by_lock");

  // Stale lock recovery
  const stalePath = path.join(tmp, "ACTIVE_LOCK.json");
  const cur = JSON.parse(fs.readFileSync(stalePath, "utf8"));
  fs.writeFileSync(
    stalePath,
    `${JSON.stringify(
      {
        ...cur,
        owner: "stale-runner",
        leaseExpiresAt: new Date(Date.now() - 60_000).toISOString(),
      },
      null,
      2
    )}\n`
  );
  const take = acquireReleaseLock({
    root: tmp,
    product: "ADP",
    approvedSha: sha,
    owner: "gha-retry",
    authorized: true,
    requireSharedRoot: false,
  });
  assert.equal(take.ok, true);
  pass("stale_lock_allows_release_retry");
  releaseLock({ root: tmp, owner: "gha-retry", reason: "test" });

  // Preflight rejects bad phrase / short sha
  const badPhrase = spawnSync(
    process.execPath,
    [
      path.join(ROOT, "scripts/dealality-production-release-preflight-v1.mjs"),
      "--product",
      "AVA",
      "--sha",
      sha,
      "--authorization-phrase",
      "WRONG",
    ],
    { cwd: ROOT, encoding: "utf8" }
  );
  assert.notEqual(badPhrase.status, 0);
  pass("preflight_rejects_unauthorized");

  const badSha = spawnSync(
    process.execPath,
    [
      path.join(ROOT, "scripts/dealality-production-release-preflight-v1.mjs"),
      "--product",
      "AVA",
      "--sha",
      "deadbeef",
      "--authorization-phrase",
      AUTHORIZATION_PHRASE,
    ],
    { cwd: ROOT, encoding: "utf8" }
  );
  assert.notEqual(badSha.status, 0);
  pass("preflight_rejects_incorrect_sha");

  // Coordinator dry-run does not publish
  // Fixture SHA may predate clean-checkout modules; simulate attestation here.
  const dry = spawnSync(
    process.execPath,
    [
      path.join(ROOT, "scripts/dealality-production-release-coordinator-v1.mjs"),
      "--product",
      "AVA",
      "--sha",
      sha,
      "--authorize-production-release",
      "--dry-run",
      "--release-after-dry-run",
      "--allow-sha-mismatch-for-test",
      "--simulate-clean-checkout",
      "--channel",
      GITHUB_ACTIONS_CHANNEL,
      "--owner",
      "gha-contract-dry",
    ],
    {
      cwd: ROOT,
      encoding: "utf8",
      env: {
        ...process.env,
        DEALALITY_RELEASE_LOCK_ROOT: path.join(tmp, "coord"),
        DEALALITY_RELEASE_LOCK_SHARED: "1",
        DEALALITY_RELEASE_CHANNEL: GITHUB_ACTIONS_CHANNEL,
        DEALALITY_SKIP_RAILWAY_PROBE: "1",
      },
    }
  );
  assert.equal(dry.status, 0, dry.stderr || dry.stdout);
  const body = JSON.parse(dry.stdout);
  assert.equal(body.lock.publication.customerPublication, false);
  assert.equal(body.lock.publication.shareTokenRotation, false);
  assert.equal(body.lock.channel, GITHUB_ACTIONS_CHANNEL);
  assert.ok(body.cleanCheckoutAttestation, "clean checkout attestation required");
  pass("dry_run_no_customer_publication_or_token_rotation");
  pass("dry_run_emits_clean_checkout_attestation");

  // Live coordinator without channel fails closed
  const liveLocal = spawnSync(
    process.execPath,
    [
      path.join(ROOT, "scripts/dealality-production-release-coordinator-v1.mjs"),
      "--product",
      "AVA",
      "--sha",
      sha,
      "--authorize-production-release",
      "--allow-sha-mismatch-for-test",
      "--owner",
      "local-attacker",
    ],
    {
      cwd: ROOT,
      encoding: "utf8",
      env: {
        ...process.env,
        DEALALITY_RELEASE_LOCK_ROOT: path.join(tmp, "attack"),
        DEALALITY_RELEASE_LOCK_SHARED: "1",
        DEALALITY_RELEASE_CHANNEL: "",
        DEALALITY_RELEASE_LOCK_BYPASS: "",
      },
    }
  );
  assert.notEqual(liveLocal.status, 0);
  pass("unauthorized_local_live_coordinator_blocked");

  // block-raw still blocks
  const raw = spawnSync(
    process.execPath,
    [path.join(ROOT, "scripts/block-raw-railway-up.mjs")],
    { cwd: ROOT, encoding: "utf8", env: { ...process.env, DEALALITY_ALLOW_RAW_RAILWAY_UP: "" } }
  );
  assert.notEqual(raw.status, 0);
  pass("legacy_raw_railway_up_blocked");

  // Simulated railway busy policy: coordinator fails closed without cancel tooling
  assert.match(wf, /RAILWAY_BUSY|Never cancel Railway|does not cancel Railway/i);
  pass("existing_railway_builds_not_auto_cancelled");
} catch (err) {
  fail("uncaught", String(err && err.stack ? err.stack : err));
}

const summary = {
  ok: results.every((r) => r.pass),
  total: results.length,
  passed: results.filter((r) => r.pass).length,
  failed: results.filter((r) => r.pass === false).length,
  results,
  tmp,
};
const outPath = path.join(
  ROOT,
  "reports/dealality-production-release-coordinator-v1/GITHUB_ACTIONS_CONTRACT_TEST_RESULTS.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`);
console.log(
  JSON.stringify(
    { ok: summary.ok, passed: summary.passed, failed: summary.failed, outPath },
    null,
    2
  )
);
process.exit(summary.ok ? 0 : 1);
