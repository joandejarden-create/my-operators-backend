#!/usr/bin/env node
/**
 * v40 — Brand Explorer Batch Active Release Remediation.
 *
 * Default: dry-run. Apply requires all confirm flags and only patches
 * Presentation Title/Body/Case Summary / External Display Status.
 * Never sets active approval or Company Validated.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_RELEASE_CANDIDATES,
  DEFAULT_INCOMPLETE_CONTROL,
  parseV40ApplyFlags,
  runV40ActiveReleaseRemediation,
  writeV40Reports,
  V40_REMEDIATION_VERSION,
} from "../lib/partner-intelligence/brand-explorer-active-release-remediation.js";

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
      : [...DEFAULT_RELEASE_CANDIDATES];
  const flags = parseV40ApplyFlags(argv);
  return { brands, dryRun: !flags.apply, apply: flags.apply, flags };
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
  console.log(
    `[${V40_REMEDIATION_VERSION}] Active release remediation (apply=${opts.apply}, dryRun=${opts.dryRun})`
  );

  const report = await runV40ActiveReleaseRemediation(opts);
  const paths = writeV40Reports(report);
  console.log(`Wrote ${paths.jsonPath}`);

  if (report.applyBlocked) {
    console.error("Apply blocked by gates:");
    for (const b of report.applyGateCheck?.blockers || []) console.error(`  - ${b}`);
    process.exit(1);
  }

  console.log(
    `Summary: patches=${report.summary.totalPatches} blockers=${report.summary.totalBlockers} copyCleanProjected=${report.summary.ownerCopyProjectedCleanCount} recordsPatched=${report.summary.recordsPatched || 0} incompleteControl=${report.summary.incompleteControlPass}`
  );

  for (const b of report.brandResults) {
    const applied = b.applyResult
      ? ` applied=${b.applyResult.recordsTouched} err=${b.applyResult.errors?.length || 0}`
      : "";
    console.log(
      `  ${b.brandSlug}: patches=${b.patchPlan.summary.patchCount} copyAfter=${b.projection.ownerCopyBlockersProjectedZero ? "clean" : "dirty"} founderBlocked=yes approvalBlocked=yes${applied}`
    );
  }

  // Skip long regressions when user is chaining their own validation commands after apply.
  const skipRegression = process.argv.includes("--skip-regression");
  if (!skipRegression) {
    const qualityBrands = [...new Set([...opts.brands, ...DEFAULT_INCOMPLETE_CONTROL])].join(",");
    const validations = [
      runNpm(["test:brand-explorer-external-quality-lock", "--", "--brands", qualityBrands]),
      runNpm(["test:brand-explorer-active-profile-staged-apply"]),
      runNpm(["test:partner-intelligence-publish-readiness"]),
      runNpm(["test:partner-intelligence-profile-governance-publish"]),
    ];
    report.regression = validations;
    writeV40Reports(report);
    const failed = validations.filter((v) => v.status !== "pass");
    if (!report.summary.incompleteControlPass) {
      console.error("Incomplete brand control check failed.");
      process.exit(1);
    }
    if (failed.length) {
      console.error("Validation failed:");
      for (const f of failed) console.error(`  npm run ${f.args.join(" ")}`);
      process.exit(1);
    }
  }

  if (report.summary.applyErrors > 0) {
    console.error(`Apply finished with ${report.summary.applyErrors} Airtable error(s).`);
    process.exit(1);
  }

  console.log(
    report.applyExecuted
      ? "v40 apply complete — Presentation copy scrubbed; no unlock; no active approval."
      : "v40 dry-run complete — patch plans only; no Airtable writes; no unlock."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
