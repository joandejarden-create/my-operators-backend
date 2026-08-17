#!/usr/bin/env node
/**
 * v40C — Economics chrome + residual owner-copy remediation.
 * Default dry-run. Apply requires all confirm flags; Presentation-only writes.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V40C_VERSION,
  V40C_DEFAULT_BRANDS,
  parseV40CApplyFlags,
  runV40CEconomicsChromeRemediation,
  writeV40CReports,
} from "../lib/partner-intelligence/brand-explorer-v40c-economics-chrome-remediation-run.js";

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
      : [...V40C_DEFAULT_BRANDS];
  const flags = parseV40CApplyFlags(argv);
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
    `[${V40C_VERSION}] Economics chrome + residual owner-copy (apply=${opts.apply}, dryRun=${opts.dryRun})`
  );

  const report = await runV40CEconomicsChromeRemediation(opts);
  const paths = writeV40CReports(report);
  console.log(`Wrote ${paths.jsonPath}`);

  if (report.applyBlocked) {
    console.error("Apply blocked by gates:");
    for (const b of report.applyGateCheck?.blockers || []) console.error(`  - ${b}`);
    process.exit(1);
  }

  console.log(
    `Summary: residualPatches=${report.summary.totalResidualPatches} recordsPatched=${report.summary.recordsPatched || 0} applyErrors=${report.summary.applyErrors || 0} internalCleanProjected=${report.summary.internalPreviewCleanProjected}/${report.summary.brandsRemediated} incompleteControl=${report.summary.incompleteControlPass}`
  );

  for (const b of report.brandResults) {
    const applied = b.applyResult
      ? ` applied=${b.applyResult.recordsTouched} err=${b.applyResult.errors?.length || 0}`
      : "";
    console.log(
      `  ${b.brandSlug}: ${b.founderDecision.decision} | internal ${b.projection.internalPreviewForbiddenBeforeCount}→${b.projection.internalPreviewForbiddenAfterCount} | residualPatches=${b.residualPresentation.summary.patchCount}${applied}`
    );
  }

  const skipRegression = process.argv.includes("--skip-regression");
  if (!skipRegression) {
    const brandList = opts.brands.join(",");
    const allLock = [
      ...opts.brands,
      "hotel-indigo",
      "mgallery-collection",
      "design-hotels",
      "small-luxury-hotels-of-the-world",
    ].join(",");
    const validations = [
      runNpm(["test:brand-explorer-internal-preview-owner-copy", "--", "--brands", brandList]),
      runNpm(["test:brand-explorer-external-quality-lock", "--", "--brands", allLock]),
      runNpm(["test:brand-explorer-active-profile-staged-apply"]),
      runNpm(["test:partner-intelligence-publish-readiness"]),
      runNpm(["test:partner-intelligence-profile-governance-publish"]),
    ];
    report.regression = validations;
    writeV40CReports(report);
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
      ? "v40C apply complete — Presentation residual scrubbed; no unlock; no active approval."
      : "v40C dry-run complete — chrome in code; no Airtable writes; no unlock; no active approval."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
