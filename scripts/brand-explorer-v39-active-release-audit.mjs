#!/usr/bin/env node
/**
 * v39 — Brand Explorer Active Profile Release Audit (dry-run / read-only).
 *
 * Does NOT apply active release. Live API is source of truth.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_BRANDS,
  runV39ActiveReleaseAudit,
  writeV39Reports,
  V39_AUDIT_VERSION,
} from "../lib/partner-intelligence/brand-explorer-active-release-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...DEFAULT_BRANDS];
  if (argv.includes("--apply")) {
    console.error(
      "v39 audit refuses --apply. Active release apply is designed but not implemented in this command."
    );
    process.exit(2);
  }
  return { brands, dryRun: true };
}

function runNpm(args) {
  const res = spawnSync("npm", ["run", ...args], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return {
    args,
    status: res.status === 0 ? "pass" : "fail",
    exitCode: res.status,
    stderrTail: (res.stderr || "").slice(-400),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V39_AUDIT_VERSION}] Active release audit (dryRun=${opts.dryRun}, apply=never)`);

  const report = await runV39ActiveReleaseAudit(opts);
  const paths = writeV39Reports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(
    `Summary: unlock-after-approval=${report.summary.safeToUnlockAfterApproval} remediation=${report.summary.remediationRequired} incompleteControl=${report.summary.incompleteControlPass}`
  );

  for (const b of report.brandResults) {
    console.log(
      `  [${b.cohort}] ${b.brandSlug}: ${b.classification.outcome} | state=${b.displayState} | failed=${(b.gateInventory.failedGates || []).length}`
    );
  }

  const qualityLockBrands = opts.brands.join(",");
  const validations = [
    runNpm([
      "test:brand-explorer-external-quality-lock",
      "--",
      "--brands",
      qualityLockBrands,
    ]),
    runNpm(["test:brand-explorer-active-profile-staged-apply"]),
    runNpm(["test:partner-intelligence-publish-readiness"]),
    runNpm(["test:partner-intelligence-profile-governance-publish"]),
  ];

  report.regression = validations;
  writeV39Reports(report);

  const failed = validations.filter((v) => v.status !== "pass");
  if (!report.summary.incompleteControlPass) {
    console.error("Incomplete brand control check failed.");
    process.exit(1);
  }
  if (failed.length) {
    console.error("One or more validation scripts failed.");
    for (const f of failed) console.error(`  fail: npm run ${f.args.join(" ")}`);
    process.exit(1);
  }
  console.log("v39 audit complete — no Airtable writes; no active release apply.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
