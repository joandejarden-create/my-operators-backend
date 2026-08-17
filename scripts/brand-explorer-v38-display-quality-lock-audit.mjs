#!/usr/bin/env node
/**
 * v38 — Brand Explorer display quality lock audit.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_BRANDS,
  runV38DisplayQualityLockAudit,
  writeV38Reports,
} from "../lib/partner-intelligence/brand-explorer-v38-display-quality-lock-audit.js";
import { V38_QUALITY_LOCK_VERSION } from "../lib/partner-intelligence/brand-explorer-display-quality-lock.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...DEFAULT_BRANDS];
  return { brands, dryRun: !argv.includes("--apply") };
}

function runNpm(args) {
  const res = spawnSync("npm", ["run", ...args], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return { args, status: res.status === 0 ? "pass" : "fail", exitCode: res.status };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V38_QUALITY_LOCK_VERSION}] Display quality lock audit (dryRun=${opts.dryRun})`);

  const report = await runV38DisplayQualityLockAudit(opts);
  const paths = writeV38Reports(report);
  console.log(`Wrote ${paths.jsonPath}`);

  const qualityLockTest = runNpm([
    "test:brand-explorer-external-quality-lock",
    "--",
    "--brands",
    opts.brands.join(","),
  ]);
  const regressions = [
    runNpm(["test:brand-explorer-active-profile-staged-apply"]),
    runNpm(["test:partner-intelligence-publish-readiness"]),
    runNpm(["test:partner-intelligence-profile-governance-publish"]),
  ];

  report.regression = { qualityLockTest, regressions };
  writeV38Reports(report);

  const failed = [qualityLockTest, ...regressions].filter((r) => r.status !== "pass");
  if (!report.summary.allExternalQualityLockPass) {
    console.error("External quality lock failed for one or more brands.");
    process.exit(1);
  }
  if (failed.length) {
    console.error("One or more regression/test commands failed.");
    process.exit(1);
  }
  console.log("v38 audit complete — all brands passed external quality lock.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
