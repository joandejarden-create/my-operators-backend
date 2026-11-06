#!/usr/bin/env node
/**
 * Canonical production deploy wrapper.
 *
 *   npm run deploy:production
 *
 * Must be invoked under the shared Dealality production release coordinator
 * after a mandatory clean-checkout attestation for the approved SHA.
 *
 * Refuses incomplete trees, wrong branches (unless hotfix override), and
 * dirty trees. Dirty-tree override alone is NOT sufficient — clean-checkout
 * attestation is mandatory for all production uploads including emergency
 * hotfix (expedited tests allowed only via the coordinator --expedited flag).
 *
 * Do NOT use raw `railway up` for production.
 * Do NOT treat _railway-lean-stage as the release source of truth (packaging
 * only, built from the attested clean checkout).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync, spawnSync } from "node:child_process";
import { assertDeployAllowedByLock } from "../lib/dealality-production-release/release-lock-v1.js";
import { assertReleaseChannelAllowed } from "../lib/dealality-production-release/release-channel-v1.js";
import { assertCleanCheckoutAttestationForDeploy } from "../lib/dealality-production-release/clean-checkout-release-gate-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(
  fs.readFileSync(path.join(root, "config", "production-required-assets.json"), "utf8")
);

const allowHotfix = process.env.DEALALITY_ALLOW_HOTFIX_DEPLOY === "1";
const allowDirty = process.env.DEALALITY_ALLOW_DIRTY_DEPLOY === "1";
const skipPost = process.env.DEALALITY_SKIP_POSTDEPLOY_SMOKE === "1";
const dryRun = process.argv.includes("--dry-run");

const channelGate = assertReleaseChannelAllowed(process.env, {
  liveDeploy: !dryRun,
});
if (!channelGate.ok) {
  console.error(
    `FAIL RELEASE_CHANNEL_GATE: ${channelGate.code} — ${channelGate.message}`
  );
  console.error(
    "Dispatch GitHub Actions workflow: dealality-production-release (Actions → Run workflow)."
  );
  process.exit(1);
}

const lockGate = assertDeployAllowedByLock(process.env, {
  liveDeploy: !dryRun,
  cwd: root,
});
if (!lockGate.ok) {
  console.error(
    `FAIL RELEASE_LOCK_GATE: ${lockGate.code || "LOCK"} — ${lockGate.message}`
  );
  if (lockGate.lock) {
    console.error(
      `  heldBy=${lockGate.lock.owner} product=${lockGate.lock.product} phase=${lockGate.lock.phase}`
    );
  }
  console.error(
    "Use GitHub Actions workflow dealality-production-release (sole live production entry)."
  );
  process.exit(1);
}
if (lockGate.warn) {
  console.warn(`WARN RELEASE_LOCK: ${lockGate.warn}`);
}

function sh(cmd, opts = {}) {
  return execSync(cmd, {
    cwd: root,
    encoding: "utf8",
    stdio: opts.stdio || ["ignore", "pipe", "pipe"],
    ...opts,
  }).trim();
}

function runNode(script, args = []) {
  const r = spawnSync(process.execPath, [script, ...args], {
    cwd: root,
    stdio: "inherit",
    env: process.env,
  });
  if (r.status !== 0) {
    console.error(`FAIL deploy gate: ${script}`);
    process.exit(r.status || 1);
  }
}

function git(cmd) {
  try {
    return sh(`git ${cmd}`);
  } catch (err) {
    return "";
  }
}

const branch = git("rev-parse --abbrev-ref HEAD") || "unknown";
const sha = git("rev-parse HEAD") || "unknown";
const dirty = git("status --porcelain");
const normalBranch = manifest.deploy_policy?.normal_branch || "main";

console.log("=== Dealality deploy:production ===");
console.log(`branch: ${branch}`);
console.log(`sha:    ${sha}`);
console.log(`dirty:  ${dirty ? "YES" : "no"}`);

// Mandatory clean-checkout attestation (company-wide). Dry-run still requires
// attestation unless explicitly simulating via coordinator.
const cleanGate = assertCleanCheckoutAttestationForDeploy({
  env: process.env,
  sha,
});
if (!cleanGate.ok) {
  // Live deploy never bypasses. Dry-run may proceed with WARN when attestation
  // is absent (asset gates still run; no Railway upload). Prefer coordinator
  // dry-run which produces a real attestation via --simulate-clean-checkout.
  const allowDryWithout = dryRun === true;
  if (!allowDryWithout) {
    console.error(
      `FAIL CLEAN_CHECKOUT_GATE: ${cleanGate.code || "ATTESTATION"} — ${cleanGate.detail || cleanGate.message || ""}`
    );
    console.error(
      "Run: npm run dealality:production-release -- --product <ADP|GDI|AVA> --sha <40hex> --authorize-production-release"
    );
    process.exit(1);
  }
  console.warn(
    "WARN CLEAN_CHECKOUT: dry-run without attestation (DEALALITY_DEPLOY_DRY_RUN_WITHOUT_ATTESTATION=1)"
  );
} else {
  console.log(
    `PASS CLEAN_CHECKOUT_ATTESTATION ${cleanGate.attestation.attestationId} sha=${cleanGate.attestation.gitSha.slice(0, 12)}`
  );
}

const approvedEnvSha = String(process.env.DEALALITY_APPROVED_RELEASE_SHA || "")
  .trim()
  .toLowerCase();
if (approvedEnvSha && approvedEnvSha !== sha) {
  console.error(
    `FAIL APPROVED_SHA_MISMATCH: DEALALITY_APPROVED_RELEASE_SHA=${approvedEnvSha} HEAD=${sha}`
  );
  process.exit(1);
}

if (branch !== normalBranch && !allowHotfix) {
  if (dryRun) {
    console.warn(
      `WARN WRONG_BRANCH: dry-run on '${branch}' (normal='${normalBranch}'); live deploy requires main or DEALALITY_ALLOW_HOTFIX_DEPLOY=1`
    );
  } else {
    console.error(
      `FAIL WRONG_BRANCH_GUARD: expected '${normalBranch}', got '${branch}'.\n` +
        `For emergency hotfix set DEALALITY_ALLOW_HOTFIX_DEPLOY=1 (still requires clean-checkout attestation + asset gates).`
    );
    process.exit(1);
  }
}
if (branch !== normalBranch && allowHotfix) {
  console.warn(
    `WARN HOTFIX OVERRIDE: deploying from '${branch}' (not ${normalBranch})`
  );
}

// Dirty developer trees must not be production upload sources. Clean-checkout
// attestation implies live deploy cwd is the isolated checkout (clean).
if (dirty) {
  if (dryRun) {
    console.warn(
      "WARN DIRTY_TREE: dry-run allows dirty cwd for local asset gates; live deploy will refuse."
    );
    console.warn(dirty.split("\n").slice(0, 20).join("\n"));
  } else {
    console.error(
      "FAIL DIRTY_TREE_GUARD: uncommitted changes present in deploy cwd.\n" +
        "Production uploads must run from the coordinator clean checkout (clean tree).\n" +
        "DEALALITY_ALLOW_DIRTY_DEPLOY no longer authorizes dirty-worktree production deploys."
    );
    console.error(dirty.split("\n").slice(0, 30).join("\n"));
    if (allowDirty) {
      console.error(
        "NOTE: DEALALITY_ALLOW_DIRTY_DEPLOY=1 was set but is ignored for production integrity."
      );
    }
    process.exit(1);
  }
}

// Diff summary vs origin/main when available
try {
  const removed = git(
    "diff --name-only --diff-filter=D origin/main...HEAD"
  );
  if (removed) {
    const critical = removed
      .split("\n")
      .filter(
        (f) =>
          /hotel-intelligence|owner-ai-demand|brand-explorer-share|operator-explorer-share|research-center\.css/.test(
            f
          )
      );
    console.log("=== CRITICAL ASSETS REMOVED vs origin/main (in this commit range) ===");
    if (critical.length) {
      for (const f of critical) console.log(`  REMOVED ${f}`);
      console.error(
        "FAIL PRODUCTION_SURFACE_REMOVAL_DETECTED relative to origin/main tip ancestry"
      );
      // Only fail if files are also missing on disk
      const missingOnDisk = critical.filter(
        (f) => !fs.existsSync(path.join(root, f))
      );
      if (missingOnDisk.length) {
        for (const f of missingOnDisk) console.error(`  missing on disk: ${f}`);
        process.exit(1);
      }
    } else {
      console.log("  (none critical)");
    }
  }
} catch {
  console.warn("WARN could not diff against origin/main");
}

console.log("\n--- smoke:esm-imports ---");
runNode("scripts/smoke-esm-imports.mjs");

console.log("\n--- assert:production-assets ---");
runNode("scripts/assert-production-assets.mjs", ["--write-snapshot"]);

console.log("\n--- verify:production-share-contract ---");
runNode("scripts/verify-production-share-contract.mjs");

console.log("\n--- test:production-routes-local ---");
runNode("scripts/test-production-routes-local.mjs");

// Optional heavier gates when scripts exist
const optional = [
  ["scripts/test-hotel-explorer-share-research-parity.mjs", "share research parity"],
];
for (const [script, label] of optional) {
  if (fs.existsSync(path.join(root, script))) {
    console.log(`\n--- optional: ${label} ---`);
    runNode(script);
  }
}

const ts = new Date().toISOString().replace(/[:.]/g, "-");
const recordDir = path.join(root, "reports", "deployments");
fs.mkdirSync(recordDir, { recursive: true });
const recordPath = path.join(recordDir, `${ts}-production.json`);

const record = {
  branch,
  sha,
  dirty: Boolean(dirty),
  hotfixOverride: allowHotfix,
  dirtyOverride: allowDirty,
  startedAt: new Date().toISOString(),
  manifestVersion: manifest.version,
  dryRun,
  railwayDeployId: null,
  gates: {
    assertProductionAssets: "PASS",
    localRouteSmoke: "PASS",
  },
};

if (dryRun) {
  record.finishedAt = new Date().toISOString();
  record.status = "DRY_RUN";
  fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
  console.log(`\nDRY RUN — would railway up now. Record: ${recordPath}`);
  process.exit(0);
}

console.log("\n--- railway up --detach ---");
// Railway CLI 4.x dropped `--message`; keep the string on the deploy record only.
const msg = `deploy:production ${branch} ${sha.slice(0, 7)}`;
// Windows npm shims are railway.cmd / railway.ps1 ? bare `railway` ENOENTs under spawnSync.
const railwayBin = process.platform === "win32" ? "railway.cmd" : "railway";
const railwayService =
  process.env.RAILWAY_SERVICE ||
  process.env.DEALALITY_RAILWAY_SERVICE ||
  "my-operators-backend";
// Railway CLI ignore-crate packing has repeatedly exceeded Cloudflare's ~200MB
// upload limit on this repo even when .railwayignore estimates ~173MB. Default to
// an explicit filtered staging tree (same keep-set as .railwayignore) unless
// DEALALITY_RAILWAY_DIRECT_UP=1 is set for emergency debugging.
const useLeanStage = process.env.DEALALITY_RAILWAY_DIRECT_UP !== "1";
let uploadCwd = root;
let upArgs = ["up", "--detach", "--service", railwayService];
if (useLeanStage) {
  const stageRoot =
    process.env.DEALALITY_RAILWAY_LEAN_STAGE_DIR ||
    path.join(root, "..", "_railway-lean-stage");
  console.log(`\n--- build lean Railway stage → ${stageRoot} ---`);
  runNode("scripts/build-railway-lean-stage.mjs", ["--out", stageRoot]);
  uploadCwd = root; // keep linked project context
  upArgs = [
    "up",
    stageRoot,
    "--detach",
    "--service",
    railwayService,
    "--path-as-root",
  ];
  record.leanStage = { path: stageRoot, mode: "path-as-root" };
}
const up = spawnSync(railwayBin, upArgs, {
  // Do not force CI=true ? Railway CLI 4.x/5.x treats it as non-upload CI mode.
  cwd: uploadCwd,
  stdio: "inherit",
  env: process.env,
  shell: process.platform === "win32",
});
if (up.status !== 0) {
  record.status = "RAILWAY_UP_FAILED";
  record.finishedAt = new Date().toISOString();
  fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
  process.exit(up.status || 1);
}

record.status = "UPLOADED";
record.railwayMessage = msg;

if (!skipPost) {
  console.log("\n--- waiting briefly for Railway roll-out ---");
  spawnSync(process.execPath, ["-e", "setTimeout(() => {}, 45000)"], {
    cwd: root,
    stdio: "ignore",
  });
  console.log("\n--- test:production-critical-surfaces ---");
  const smoke = spawnSync(
    process.execPath,
    ["scripts/test-production-critical-surfaces.mjs"],
    { cwd: root, stdio: "inherit", env: process.env }
  );
  record.gates.postdeploySmoke = smoke.status === 0 ? "PASS" : "FAIL";
  if (smoke.status !== 0) {
    record.status = "POSTDEPLOY_SMOKE_FAILED";
    record.finishedAt = new Date().toISOString();
    fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
    console.error("FAIL: production smoke failed after upload — treat deploy as FAILED");
    process.exit(smoke.status || 1);
  }
  console.log("\n--- verify:production-share-contract --live ---");
  const shareLive = spawnSync(
    process.execPath,
    ["scripts/verify-production-share-contract.mjs", "--live"],
    { cwd: root, stdio: "inherit", env: process.env }
  );
  record.gates.postdeployShareContract = shareLive.status === 0 ? "PASS" : "FAIL";
  if (shareLive.status !== 0) {
    record.status = "POSTDEPLOY_SHARE_CONTRACT_FAILED";
    record.finishedAt = new Date().toISOString();
    fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
    console.error("FAIL: existing client share token failed after deploy — treat as FAILED");
    process.exit(shareLive.status || 1);
  }
} else {
  console.warn("WARN skipped postdeploy smoke (DEALALITY_SKIP_POSTDEPLOY_SMOKE=1)");
}

record.status = "SUCCESS";
record.finishedAt = new Date().toISOString();
fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
fs.writeFileSync(
  path.join(recordDir, "latest-production.json"),
  JSON.stringify(record, null, 2)
);
console.log(`\nPASS deploy:production — record ${recordPath}`);
process.exit(0);
