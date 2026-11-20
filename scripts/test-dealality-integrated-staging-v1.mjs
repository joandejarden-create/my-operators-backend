#!/usr/bin/env node
/**
 * Regression tests — Dealality Integrated Staging V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  mayBindPort8080,
  isProductWorktreeCheckout,
  isIntegratedStagingCheckout,
  assertPort8080Ownership,
  emptyStagingState,
  writeStagingState,
  readStagingState,
  promoteKnownGood,
  readKnownGood,
  writeStagingMarker,
  buildEnvironmentStatusPayload,
  INTEGRATED_STAGING_BASENAME,
} from "../lib/dealality-dev-os/integrated-staging-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}:`, err.message);
  }
}

function tmpDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

test("product worktree cannot claim 8080", () => {
  assert.equal(mayBindPort8080("C:/Dev/dealality/ws-gdi"), false);
  assert.equal(mayBindPort8080("C:/Dev/dealality/ws-adp"), false);
  assert.equal(mayBindPort8080("C:/Dev/dealality/ws-hotel-explorer"), false);
  assert.equal(mayBindPort8080("C:/Dev/dealality/ws-brand-explorer"), false);
  assert.equal(isProductWorktreeCheckout("C:/Dev/dealality/ws-gdi"), true);
});

test("DEALALITY_CANONICAL_CHECKOUT does not unlock product 8080", () => {
  assert.equal(
    mayBindPort8080("C:/Dev/dealality/ws-gdi", {
      DEALALITY_CANONICAL_CHECKOUT: "1",
      PORT: "8080",
    }),
    false,
  );
});

test("integrated staging basename may bind 8080", () => {
  assert.equal(
    mayBindPort8080(`C:/Dev/dealality/${INTEGRATED_STAGING_BASENAME}`),
    true,
  );
});

test("staging marker enables ownership", () => {
  const dir = tmpDir("dealality-staging-marker-");
  writeStagingMarker(dir);
  assert.equal(isIntegratedStagingCheckout(dir), true);
  assert.equal(mayBindPort8080(dir), true);
  fs.rmSync(dir, { recursive: true, force: true });
});

test("assertPort8080Ownership throws for product cwd", () => {
  assert.throws(
    () =>
      assertPort8080Ownership("C:/Dev/dealality/ws-hotel-explorer", {
        PORT: "8080",
      }),
    (err) => err && err.code === "PORT_8080_OWNERSHIP_DENIED",
  );
});

test("assertPort8080Ownership allows non-8080 product ports", () => {
  const r = assertPort8080Ownership("C:/Dev/dealality/ws-gdi", {
    PORT: "8091",
  });
  assert.equal(r.ok, true);
  assert.equal(r.reason, "non_8080");
});

test("multiple simultaneous launches: only staging binds 8080", () => {
  const staging = `C:/Dev/dealality/${INTEGRATED_STAGING_BASENAME}`;
  const he = "C:/Dev/dealality/ws-hotel-explorer";
  assert.equal(mayBindPort8080(staging), true);
  assert.equal(mayBindPort8080(he), false);
});

test("staging state + known-good rollback snapshot", () => {
  const dir = tmpDir("dealality-staging-state-");
  writeStagingMarker(dir);
  const state = writeStagingState(
    dir,
    emptyStagingState({
      stagingPath: dir,
      baseMainSha: "aaa111",
      headSha: "aaa111",
      validationStatus: "PASS",
    }),
  );
  promoteKnownGood(dir, { ...state, reason: "test" });
  const kg = readKnownGood(dir);
  assert.equal(kg.headSha, "aaa111");

  writeStagingState(dir, {
    ...state,
    headSha: "bbb222",
    validationStatus: "FAIL",
    notes: ["failed refresh"],
  });
  const afterFail = readStagingState(dir);
  assert.equal(afterFail.headSha, "bbb222");
  assert.equal(readKnownGood(dir).headSha, "aaa111");
  fs.rmSync(dir, { recursive: true, force: true });
});

test("environment status distinguishes merged vs staging-only", () => {
  const payload = buildEnvironmentStatusPayload(
    emptyStagingState({
      baseMainSha: "mainsha0001",
      stagedFeatureCommits: [{ sha: "featsha0001", label: "preview-x" }],
      includedFeatures: {
        mergedIntoMain: [{ id: "surfe", label: "Surfe Usage" }],
        stagingOnly: [
          { id: "preview-x", label: "preview-x", productionReady: false },
        ],
        notIntegrated: [{ id: "wip-y", label: "wip-y" }],
      },
      lastRefreshAt: "2026-11-20T00:00:00.000Z",
      validationStatus: "PASS",
      serverStatus: "RUNNING",
    }),
  );
  assert.equal(payload.environment, "INTEGRATED_STAGING");
  assert.equal(payload.autoDeployToProduction, false);
  assert.equal(payload.features.mergedIntoMain.length, 1);
  assert.equal(payload.features.includedOnlyInLocalStaging.length, 1);
  assert.equal(payload.features.notYetIntegrated.length, 1);
  assert.match(payload.disclaimer, /NOT production-ready/i);
});

test("dirty product worktrees are not required for staging ownership checks", () => {
  // Preservation: ownership API never inspects or mutates product trees
  assert.equal(isProductWorktreeCheckout("C:/Dev/dealality/ws-gdi"), true);
  assert.equal(mayBindPort8080("C:/Dev/dealality/ws-gdi"), false);
});

test("production isolation flags are explicit on empty state", () => {
  const s = emptyStagingState();
  assert.equal(s.productionIsolated, true);
  assert.equal(s.autoDeployToProduction, false);
  assert.equal(s.port, 8080);
});

test("contract doc + proposal + CLI + API + panel assets exist", () => {
  const files = [
    "docs/ai-build-system/INTEGRATED_STAGING_V1.md",
    "docs/ai-build-system/integrated-staging-registry-proposal-v1.json",
    "lib/dealality-dev-os/integrated-staging-v1.js",
    "scripts/dealality-integrated-staging-v1.mjs",
    "api/admin-integrated-staging-status.js",
    "public/js/integrated-staging-env-status.js",
    "public/css/integrated-staging-env-status.css",
  ];
  for (const f of files) {
    assert.ok(fs.existsSync(path.join(root, f)), `missing ${f}`);
  }
});

test("server.js denies product CANONICAL override pattern", () => {
  const txt = fs.readFileSync(path.join(root, "server.js"), "utf8");
  assert.match(txt, /mayBindPort8080/);
  assert.match(txt, /Product worktrees must use an isolated PORT/);
  assert.doesNotMatch(
    txt,
    /cwdBase === "my-operators-backend".*DEALALITY_CANONICAL_CHECKOUT === "1"/,
  );
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
