#!/usr/bin/env node
/**
 * Shared ADP/AVA/GDI production release coordinator (Node core).
 *
 * Does NOT publish customer reports or rotate share tokens.
 * Orchestrates existing npm run deploy:production after lock acquisition.
 *
 *   node scripts/dealality-production-release-coordinator-v1.mjs \
 *     --product AVA --sha <40hex> --authorize-production-release [--dry-run]
 */
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  RELEASE_STATES,
  acquireReleaseLock,
  renewReleaseLock,
  releaseLock,
  readActiveLock,
  resolveLockRoot,
} from "../lib/dealality-production-release/release-lock-v1.js";
import {
  assertReleaseChannelAllowed,
  GITHUB_ACTIONS_CHANNEL,
} from "../lib/dealality-production-release/release-channel-v1.js";

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function has(flag) {
  return process.argv.includes(flag);
}

const ROOT = path.resolve(
  arg("--repo-root") || path.join(path.dirname(fileURLToPath(import.meta.url)), "..")
);

function fail(code, message, extra = {}) {
  console.error(JSON.stringify({ ok: false, code, message, ...extra }, null, 2));
  process.exit(2);
}

const product = String(arg("--product") || "").trim().toUpperCase();
const sha = String(arg("--sha") || "").trim().toLowerCase();
const dryRun = has("--dry-run");
const authorized = has("--authorize-production-release");
const statusOnly = has("--status");
const releaseOnly = has("--release-lock");
const channelArg = String(arg("--channel") || "").trim();
if (channelArg) {
  process.env.DEALALITY_RELEASE_CHANNEL = channelArg;
}
const owner =
  String(arg("--owner") || process.env.DEALALITY_RELEASE_OWNER || "").trim() ||
  `${os.hostname()}:${process.pid}`;

if (statusOnly) {
  const root = resolveLockRoot();
  const lock = readActiveLock(root);
  console.log(JSON.stringify({ ok: true, root, lock }, null, 2));
  process.exit(0);
}

if (releaseOnly) {
  const r = releaseLock({
    owner,
    reason: arg("--reason") || "manual_release",
    finalPhase: arg("--final-phase") || "COMPLETE",
    force: has("--force"),
  });
  console.log(JSON.stringify(r, null, 2));
  process.exit(r.ok ? 0 : 2);
}

if (!["ADP", "AVA", "GDI"].includes(product)) {
  fail("INVALID_PRODUCT", "Pass --product ADP|AVA|GDI");
}
if (!/^[0-9a-f]{40}$/.test(sha)) {
  fail("INVALID_SHA", "Pass --sha <full-40-char-hex>");
}
if (!authorized) {
  fail(
    "AUTHORIZATION_REQUIRED",
    "Pass --authorize-production-release after founder authorization"
  );
}

const channelGate = assertReleaseChannelAllowed(process.env, {
  liveDeploy: !dryRun,
  channel: process.env.DEALALITY_RELEASE_CHANNEL,
});
if (!channelGate.ok) {
  fail(channelGate.code, channelGate.message, { channel: channelGate.channel });
}

// Verify working tree SHA when not dry-simulation
const head = spawnSync("git", ["rev-parse", "HEAD"], {
  cwd: ROOT,
  encoding: "utf8",
});
const headSha = String(head.stdout || "").trim().toLowerCase();
if (head.status === 0 && headSha && headSha !== sha && !has("--allow-sha-mismatch-for-test")) {
  fail("SHA_MISMATCH", `HEAD ${headSha} != approved ${sha}`);
}

const acquired = acquireReleaseLock({
  product,
  approvedSha: sha,
  owner,
  authorized: true,
  note: arg("--note") || `coordinator ${product}`,
  // Local unit tests pass requireSharedRoot:false via env ALLOW_LOCAL / SHARED.
});
if (!acquired.ok) {
  fail(acquired.code || "LOCK_FAILED", acquired.message || "lock failed", {
    lock: acquired.lock,
  });
}

const manifestDir = path.join(
  ROOT,
  "reports",
  "dealality-production-release-coordinator-v1",
  "manifests"
);
fs.mkdirSync(manifestDir, { recursive: true });
const manifestPath = path.join(
  manifestDir,
  `${acquired.lock.releaseId}.json`
);

function writeManifest(phase, extra = {}) {
  const body = {
    contract: "DEALALITY_PRODUCTION_RELEASE_MANIFEST_V1",
    ...acquired.lock,
    phase,
    dryRun,
    product,
    approvedSha: sha,
    updatedAt: new Date().toISOString(),
    states: RELEASE_STATES,
    publication: {
      customerPublication: false,
      shareTokenRotation: false,
      paidResearch: false,
    },
    channel: channelGate.channel || process.env.DEALALITY_RELEASE_CHANNEL || null,
    requiredLiveChannel: GITHUB_ACTIONS_CHANNEL,
    ...extra,
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(body, null, 2)}\n`);
  renewReleaseLock({
    root: acquired.root,
    owner,
    phase,
    railwayDeploymentId: extra.railwayDeploymentId,
  });
  return body;
}

writeManifest(dryRun ? "READY" : "WAITING_FOR_RAILWAY", {
  checkpoint: "lock_acquired",
});

// Check Railway non-terminal via CLI when available
function railwayNonTerminalCount() {
  const bin = process.platform === "win32" ? "railway.cmd" : "railway";
  const r = spawnSync(bin, ["deployment", "list", "--limit", "5", "--json"], {
    cwd: ROOT,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (r.status !== 0) return { ok: false, error: String(r.stderr || r.stdout || "railway failed") };
  try {
    const deps = JSON.parse(r.stdout || "[]");
    const terminal = new Set([
      "SUCCESS",
      "FAILED",
      "REMOVED",
      "CRASHED",
      "CANCELED",
      "CANCELLED",
    ]);
    const non = deps.filter((d) => !terminal.has(d.status));
    return { ok: true, nonTerminal: non.length, non, active: deps[0] || null };
  } catch (err) {
    return { ok: false, error: String(err.message || err) };
  }
}

const skipRailwayProbe =
  has("--skip-railway-probe") ||
  String(process.env.DEALALITY_SKIP_RAILWAY_PROBE || "").trim() === "1";
const rail = skipRailwayProbe
  ? { ok: true, skipped: true, nonTerminal: 0, non: [] }
  : railwayNonTerminalCount();
if (rail.ok && rail.nonTerminal > 0) {
  writeManifest("WAITING_FOR_RAILWAY", {
    checkpoint: "railway_busy",
    railway: rail,
  });
  // Never cancel. Fail closed and wait for terminal state.
  fail("RAILWAY_BUSY", "non-terminal Railway deployment in progress — do not cancel; wait then retry", {
    railway: rail,
  });
}

if (dryRun) {
  const body = writeManifest("READY", {
    checkpoint: "dry_run_complete",
    railway: rail.ok ? rail : { probeFailed: rail.error },
    next: "Re-run without --dry-run to execute npm run deploy:production under this lock",
  });
  console.log(JSON.stringify({ ok: true, dryRun: true, manifestPath, lock: body }, null, 2));
  // keep lock for resume demos unless --release-after-dry-run
  if (has("--release-after-dry-run")) {
    releaseLock({ owner, finalPhase: "COMPLETE", reason: "dry_run" });
  }
  process.exit(0);
}

writeManifest("DEPLOYING", { checkpoint: "deploy_production_start" });
process.env.DEALALITY_RELEASE_OWNER = owner;
process.env.DEALALITY_RELEASE_ID = acquired.lock.releaseId;
process.env.DEALALITY_REQUIRE_RELEASE_LOCK = "1";
process.env.DEALALITY_RELEASE_CHANNEL =
  process.env.DEALALITY_RELEASE_CHANNEL || GITHUB_ACTIONS_CHANNEL;

const deploy = spawnSync(
  process.platform === "win32" ? "npm.cmd" : "npm",
  ["run", "deploy:production"],
  {
    cwd: ROOT,
    stdio: "inherit",
    env: process.env,
    shell: process.platform === "win32",
  }
);

if (deploy.status !== 0) {
  writeManifest("FAILED", {
    checkpoint: "deploy_failed",
    deployStatus: deploy.status,
  });
  releaseLock({
    owner,
    finalPhase: "FAILED",
    reason: "deploy_failed",
  });
  fail("DEPLOY_FAILED", "npm run deploy:production failed", {
    deployStatus: deploy.status,
    manifestPath,
    recovery:
      "Do not cancel Railway. Wait for terminal state, then re-dispatch workflow with same or rollback SHA.",
  });
}

writeManifest("VERIFYING", { checkpoint: "post_deploy_verify" });

// Product-specific lightweight post checks (no publication)
const checks = [];
const share = spawnSync(
  process.execPath,
  ["scripts/verify-production-share-contract.mjs", "--live"],
  { cwd: ROOT, encoding: "utf8" }
);
checks.push({
  name: "share_contract_live",
  ok: share.status === 0,
  detail: share.status === 0 ? "PASS" : String(share.stderr || share.stdout || "").slice(0, 500),
});

const allOk = checks.every((c) => c.ok);
const finalPhase = allOk ? "COMPLETE" : "FAILED";
writeManifest(finalPhase, {
  checkpoint: "complete",
  checks,
  customerPublication: false,
});
releaseLock({
  owner,
  finalPhase,
  reason: allOk ? "release_complete" : "verify_failed",
});

console.log(
  JSON.stringify(
    {
      ok: allOk,
      product,
      approvedSha: sha,
      phase: finalPhase,
      manifestPath,
      checks,
      publicationSeparated: true,
    },
    null,
    2
  )
);
process.exit(allOk ? 0 : 2);
