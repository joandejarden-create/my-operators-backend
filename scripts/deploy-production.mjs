#!/usr/bin/env node
/**
 * Canonical production deploy wrapper.
 *
 *   npm run deploy:production
 *
 * Refuses incomplete trees, wrong branches (unless override), and dirty trees
 * (unless override). Runs asset assert → local route smoke → railway up →
 * postdeploy smoke. Records reports/deployments/<timestamp>-production.json.
 *
 * Emergency hotfix:
 *   DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production
 *
 * Do NOT use raw `railway up` for production.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync, spawnSync } from "node:child_process";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(
  fs.readFileSync(path.join(root, "config", "production-required-assets.json"), "utf8")
);

const allowHotfix = process.env.DEALALITY_ALLOW_HOTFIX_DEPLOY === "1";
const allowDirty = process.env.DEALALITY_ALLOW_DIRTY_DEPLOY === "1";
const skipPost = process.env.DEALALITY_SKIP_POSTDEPLOY_SMOKE === "1";
const dryRun = process.argv.includes("--dry-run");

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

if (branch !== normalBranch && !allowHotfix) {
  console.error(
    `FAIL WRONG_BRANCH_GUARD: expected '${normalBranch}', got '${branch}'.\n` +
      `For emergency hotfix set DEALALITY_ALLOW_HOTFIX_DEPLOY=1 (still runs asset gates).`
  );
  process.exit(1);
}
if (branch !== normalBranch && allowHotfix) {
  console.warn(
    `WARN HOTFIX OVERRIDE: deploying from '${branch}' (not ${normalBranch})`
  );
}

if (dirty && !allowDirty) {
  console.error(
    "FAIL DIRTY_TREE_GUARD: uncommitted changes present.\n" +
      "Commit/stash first, or set DEALALITY_ALLOW_DIRTY_DEPLOY=1 for emergency."
  );
  console.error(dirty.split("\n").slice(0, 30).join("\n"));
  process.exit(1);
}
if (dirty && allowDirty) {
  console.warn("WARN DIRTY TREE OVERRIDE enabled");
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

console.log("\n--- assert:production-assets ---");
runNode("scripts/assert-production-assets.mjs", ["--write-snapshot"]);

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
const msg = `deploy:production ${branch} ${sha.slice(0, 7)}`;
const up = spawnSync(
  "railway",
  ["up", "--detach", "--message", msg],
  { cwd: root, stdio: "inherit", env: { ...process.env, CI: "true" } }
);
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
