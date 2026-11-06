#!/usr/bin/env node
/**
 * Shared ADP/AVA/GDI production release coordinator (Node core).
 *
 * Does NOT publish customer reports or rotate share tokens.
 * Orchestrates clean-checkout integrity → existing npm run deploy:production
 * after lock acquisition.
 *
 * Required sequence:
 *   Acquire lock → Railway idle → Resolve SHA → Clean checkout → Install/build
 *   → Asset/route/product gates → Artifact verify → Founder auth → Deploy
 *   → Runtime SHA verify → Post-deploy regressions → Release lock
 *
 *   node scripts/dealality-production-release-coordinator-v1.mjs \
 *     --product AVA --sha <40hex> --authorize-production-release [--dry-run]
 *
 * Emergency expedited suite: --expedited (still requires clean checkout).
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
import {
  runCleanCheckoutReleaseGate,
  createCleanCheckoutWorktree,
  removeCleanCheckoutWorktree,
  verifyCheckoutIntegrity,
  verifyRuntimeIdentityMatch,
} from "../lib/dealality-production-release/clean-checkout-release-gate-v1.js";
import {
  writeReleaseManifest,
  loadLatestProductionDeployRecord,
  assertManifestReadyForProduction,
} from "../lib/dealality-production-release/release-manifest-v1.js";
import {
  resolveProductImpact,
  resolveMandatoryTests,
  loadRegressionMatrix,
} from "../lib/dealality-production-release/product-impact-resolver-v1.js";

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
const expedited = has("--expedited");
const skipRealCheckout = has("--simulate-clean-checkout");
const keepCheckout = has("--keep-checkout");
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

writeManifest("READY", {
  checkpoint: "lock_acquired",
});

function gitPorcelain() {
  const r = spawnSync("git", ["status", "--porcelain"], {
    cwd: ROOT,
    encoding: "utf8",
  });
  return r.status === 0 ? String(r.stdout || "") : "";
}

function gitChangedFiles() {
  const r = spawnSync(
    "git",
    ["diff", "--name-only", "origin/main...HEAD"],
    { cwd: ROOT, encoding: "utf8" }
  );
  if (r.status !== 0) return [];
  return String(r.stdout || "")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function runNpmScript(npmName, cwd) {
  const r = spawnSync(
    process.platform === "win32" ? "npm.cmd" : "npm",
    ["run", npmName],
    {
      cwd,
      encoding: "utf8",
      shell: process.platform === "win32",
      env: process.env,
    }
  );
  return {
    id: npmName,
    status: r.status === 0 ? "PASS" : "FAIL",
    detail:
      r.status === 0
        ? null
        : String(r.stderr || r.stdout || `exit ${r.status}`).slice(0, 800),
  };
}

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

writeManifest("WAITING_FOR_RAILWAY", { checkpoint: "railway_idle_probe" });
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
  releaseLock({
    owner,
    finalPhase: "FAILED",
    reason: "railway_busy",
  });
  fail("RAILWAY_BUSY", "non-terminal Railway deployment in progress", { railway: rail });
}

/* ------------------------------------------------------------------ */
/* Mandatory clean-checkout integrity phase                            */
/* ------------------------------------------------------------------ */
writeManifest("BACKING_UP", { checkpoint: "clean_checkout_start" });

const matrix = loadRegressionMatrix(ROOT);
const changedFiles = gitChangedFiles();
const impact = resolveProductImpact({
  changedFiles,
  primaryProduct: product,
  matrix,
  root: ROOT,
});
const porcelain = gitPorcelain();
const attestationOutDir = path.join(
  ROOT,
  "reports",
  "dealality-production-release-coordinator-v1",
  "clean-checkout-attestations"
);
const previous = loadLatestProductionDeployRecord(ROOT);
const rollbackTarget = previous
  ? { sha: previous.sha || null, record: "reports/deployments/latest-production.json" }
  : { sha: null, record: null, warning: "NO_PREVIOUS_DEPLOY_RECORD" };

let checkoutPath = null;
let gate;

if (skipRealCheckout) {
  gate = runCleanCheckoutReleaseGate({
    repoRoot: ROOT,
    approvedSha: sha,
    product,
    changedFiles,
    matrix,
    gitStatusPorcelain: "",
    simulate: true,
    expedited,
    attestationOutDir,
    releaseId: acquired.lock.releaseId,
    authorizationReference: `--authorize-production-release owner=${owner}`,
    previousProductionDeployment: previous,
    rollbackTarget,
    sourceBranch: spawnSync("git", ["rev-parse", "--abbrev-ref", "HEAD"], {
      cwd: ROOT,
      encoding: "utf8",
    }).stdout?.trim(),
    writeAttestation: true,
  });
} else {
  // Even if the developer worktree is dirty, the release source is a fresh
  // detached worktree at the approved SHA — never the dirty tree itself.
  const created = createCleanCheckoutWorktree({
    repoRoot: ROOT,
    sha,
  });
  if (!created.ok) {
    writeManifest("FAILED", {
      checkpoint: "clean_checkout_failed",
      checkout: created,
    });
    releaseLock({ owner, finalPhase: "FAILED", reason: "clean_checkout_failed" });
    fail(created.code || "CLEAN_CHECKOUT_FAILED", created.message || "checkout failed", {
      checkout: created,
    });
  }
  checkoutPath = created.outDir;

  const integrity = verifyCheckoutIntegrity({
    checkoutRoot: checkoutPath,
    expectedSha: sha,
    requireGitHead: true,
  });
  if (!integrity.ok) {
    writeManifest("FAILED", {
      checkpoint: "checkout_integrity_failed",
      integrity,
    });
    if (!keepCheckout) removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
    releaseLock({ owner, finalPhase: "FAILED", reason: "checkout_integrity_failed" });
    fail("CHECKOUT_INTEGRITY_FAILED", "clean checkout missing required modules", {
      integrity,
    });
  }

  // Install from committed lockfile inside the clean checkout
  const install = spawnSync(
    process.platform === "win32" ? "npm.cmd" : "npm",
    ["ci", "--ignore-scripts"],
    {
      cwd: checkoutPath,
      encoding: "utf8",
      shell: process.platform === "win32",
      env: process.env,
    }
  );
  // Fall back to npm install if ci fails on environments without perfect lock sync
  let installOk = install.status === 0;
  if (!installOk) {
    const install2 = spawnSync(
      process.platform === "win32" ? "npm.cmd" : "npm",
      ["install", "--ignore-scripts"],
      {
        cwd: checkoutPath,
        encoding: "utf8",
        shell: process.platform === "win32",
        env: process.env,
      }
    );
    installOk = install2.status === 0;
    if (!installOk) {
      writeManifest("FAILED", {
        checkpoint: "dependency_install_failed",
        detail: String(install2.stderr || install.stderr || "").slice(0, 1000),
      });
      if (!keepCheckout) removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
      releaseLock({ owner, finalPhase: "FAILED", reason: "dependency_install_failed" });
      fail("MISSING_DEPENDENCY", "npm install failed in clean checkout");
    }
  }

  const mandatory = resolveMandatoryTests({
    products: impact.products,
    matrix,
    root: checkoutPath,
    expedited,
  });
  if (!mandatory.ok) {
    writeManifest("FAILED", {
      checkpoint: "mandatory_tests_missing",
      missing: mandatory.missing,
    });
    if (!keepCheckout) removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
    releaseLock({ owner, finalPhase: "FAILED", reason: "mandatory_tests_missing" });
    fail("MANDATORY_TEST_MISSING", "required regression script/alias missing", {
      missing: mandatory.missing,
    });
  }

  const testResults = [];
  for (const t of mandatory.tests) {
    console.log(`\n--- clean-checkout gate: npm run ${t.npm} ---`);
    testResults.push(runNpmScript(t.npm, checkoutPath));
  }
  const testsFailed = testResults.filter((t) => t.status !== "PASS");
  if (testsFailed.length) {
    writeManifest("FAILED", {
      checkpoint: "regression_failed",
      testResults,
    });
    if (!keepCheckout) removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
    releaseLock({ owner, finalPhase: "FAILED", reason: "regression_failed" });
    fail("REGRESSION_FAILED", "mandatory clean-checkout regressions failed", {
      testResults,
    });
  }

  gate = runCleanCheckoutReleaseGate({
    repoRoot: ROOT,
    approvedSha: sha,
    product,
    changedFiles,
    matrix,
    // Source tree dirtiness is OK only because we deploy from checkoutPath.
    gitStatusPorcelain: "",
    checkoutRoot: checkoutPath,
    requireGitHead: true,
    expedited,
    attestationOutDir,
    releaseId: acquired.lock.releaseId,
    authorizationReference: `--authorize-production-release owner=${owner}`,
    previousProductionDeployment: previous,
    rollbackTarget,
    testResults,
    buildArtifactHash: integrity.lockfileHash,
    sourceBranch: spawnSync("git", ["rev-parse", "--abbrev-ref", "HEAD"], {
      cwd: ROOT,
      encoding: "utf8",
    }).stdout?.trim(),
    writeAttestation: true,
  });
}

if (!gate.ok || !gate.readyForProduction) {
  writeManifest("FAILED", {
    checkpoint: "clean_checkout_gate_failed",
    failures: gate.failures,
  });
  if (checkoutPath && !keepCheckout) {
    removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
  }
  releaseLock({ owner, finalPhase: "FAILED", reason: "clean_checkout_gate_failed" });
  fail("CLEAN_CHECKOUT_GATE_FAILED", "clean-checkout integrity gate returned NO-GO", {
    failures: gate.failures,
  });
}

const manifestReady = assertManifestReadyForProduction(gate.manifest);
if (!manifestReady.ok) {
  writeManifest("FAILED", {
    checkpoint: "manifest_not_ready",
    manifestReady,
  });
  if (checkoutPath && !keepCheckout) {
    removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
  }
  releaseLock({ owner, finalPhase: "FAILED", reason: "manifest_not_ready" });
  fail(manifestReady.code || "MANIFEST_NOT_READY", "release manifest not READY_FOR_PRODUCTION");
}

const evidenceDir = path.join(
  ROOT,
  "reports",
  "dealality-production-release-coordinator-v1",
  "release-manifests"
);
const writtenManifest = writeReleaseManifest({
  outDir: evidenceDir,
  manifest: gate.manifest,
});

writeManifest("READY", {
  checkpoint: "clean_checkout_attested",
  cleanCheckoutAttestation: gate.attestation?.path || null,
  releaseManifestPath: writtenManifest.path,
  products: impact.products,
  railway: rail.ok ? rail : { probeFailed: rail.error },
  dirtyDeveloperTree: Boolean(porcelain.trim()),
  note: porcelain.trim()
    ? "Developer worktree dirty — deploy uses isolated clean checkout attestation only"
    : "Developer worktree clean",
});

if (dryRun) {
  const body = writeManifest("READY", {
    checkpoint: "dry_run_complete",
    railway: rail.ok ? rail : { probeFailed: rail.error },
    cleanCheckoutAttestation: gate.attestation?.path || null,
    releaseManifestPath: writtenManifest.path,
    next: "Re-run without --dry-run to execute npm run deploy:production under this lock",
  });
  console.log(
    JSON.stringify(
      {
        ok: true,
        dryRun: true,
        manifestPath,
        releaseManifestPath: writtenManifest.path,
        cleanCheckoutAttestation: gate.attestation?.path || null,
        lock: body,
        readyForProduction: true,
      },
      null,
      2
    )
  );
  if (checkoutPath && !keepCheckout) {
    removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
  }
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
process.env.DEALALITY_CLEAN_CHECKOUT_ATTESTATION = gate.attestation?.path || "";
process.env.DEALALITY_APPROVED_RELEASE_SHA = sha;
// Deploy must run from the clean checkout, not the dirty developer tree.
const deployCwd = checkoutPath || ROOT;
if (checkoutPath) {
  // Ensure release lock env + attestation are visible; copy attestation path as-is.
  process.env.DEALALITY_RELEASE_LOCK_ROOT =
    process.env.DEALALITY_RELEASE_LOCK_ROOT || acquired.root;
}

const deploy = spawnSync(
  process.platform === "win32" ? "npm.cmd" : "npm",
  ["run", "deploy:production"],
  {
    cwd: deployCwd,
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
  if (checkoutPath && !keepCheckout) {
    removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
  }
  releaseLock({ owner, finalPhase: "FAILED", reason: "deploy_failed" });
  fail("DEPLOY_FAILED", "npm run deploy:production failed", {
    deployStatus: deploy.status,
    manifestPath,
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

// Runtime identity must match approved SHA when production base is reachable.
const baseUrl =
  process.env.DEALALITY_PRODUCTION_BASE_URL ||
  "https://my-operators-backend-production.up.railway.app";
try {
  const identityUrl = `${baseUrl.replace(/\/$/, "")}/api/dealality/runtime-identity`;
  const healthUrl = `${baseUrl.replace(/\/$/, "")}/health`;
  const fetched = await Promise.race([
    fetch(identityUrl).then(async (res) => ({
      ok: res.ok,
      json: res.ok ? await res.json() : null,
      status: res.status,
      url: identityUrl,
    })),
    new Promise((resolve) =>
      setTimeout(() => resolve({ ok: false, timeout: true, url: identityUrl }), 20000)
    ),
  ]);
  let runtimePayload = fetched.json;
  if (!fetched.ok) {
    const health = await Promise.race([
      fetch(healthUrl).then(async (res) => ({
        ok: res.ok,
        json: res.ok ? await res.json() : null,
      })),
      new Promise((resolve) =>
        setTimeout(() => resolve({ ok: false, timeout: true }), 15000)
      ),
    ]);
    runtimePayload = health.json || null;
  }
  const match = verifyRuntimeIdentityMatch({
    expectedSha: sha,
    runtimePayload: runtimePayload || {},
  });
  checks.push({
    name: "runtime_sha_match",
    ok: match.ok,
    detail: match.ok
      ? `PASS ${match.actual}`
      : `${match.code} expected=${sha} actual=${match.actual || "none"}`,
  });
} catch (err) {
  checks.push({
    name: "runtime_sha_match",
    ok: false,
    detail: String(err.message || err),
  });
}

const allOk = checks.every((c) => c.ok);
const finalPhase = allOk ? "COMPLETE" : "FAILED";
writeManifest(finalPhase, {
  checkpoint: "complete",
  checks,
  customerPublication: false,
  publicationMutationsAuthorized: false,
  cleanCheckoutAttestation: gate.attestation?.path || null,
  releaseManifestPath: writtenManifest.path,
});
releaseLock({
  owner,
  finalPhase,
  reason: allOk ? "release_complete" : "verify_failed",
});
if (checkoutPath && !keepCheckout) {
  removeCleanCheckoutWorktree({ repoRoot: ROOT, outDir: checkoutPath });
}

console.log(
  JSON.stringify(
    {
      ok: allOk,
      product,
      approvedSha: sha,
      phase: finalPhase,
      manifestPath,
      releaseManifestPath: writtenManifest.path,
      cleanCheckoutAttestation: gate.attestation?.path || null,
      checks,
      publicationSeparated: true,
    },
    null,
    2
  )
);
process.exit(allOk ? 0 : 2);
