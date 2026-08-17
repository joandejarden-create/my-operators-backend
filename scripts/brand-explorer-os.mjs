#!/usr/bin/env node
/**
 * v41 — Brand Explorer OS (read-only consolidation).
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V41_OS_VERSION,
  DEFAULT_OS_BRANDS,
  runBrandExplorerOs,
  writeV41OsReports,
} from "../lib/partner-intelligence/brand-explorer-os-run.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const stageIdx = argv.indexOf("--stage");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...DEFAULT_OS_BRANDS];
  const stage = stageIdx >= 0 && argv[stageIdx + 1] ? argv[stageIdx + 1] : "release-readiness";
  if (argv.includes("--apply")) {
    console.error("brand-explorer-os refuses --apply. Read-only consolidation only.");
    process.exit(2);
  }
  return { brands, stage, dryRun: true };
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
  console.log(`[${V41_OS_VERSION}] Brand Explorer OS (stage=${opts.stage}, dryRun=true)`);

  const report = await runBrandExplorerOs(opts);
  const paths = writeV41OsReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(
    `Summary: states=${JSON.stringify(report.summary.byState)} actions=${JSON.stringify(report.summary.byAction)} v40cApplyAllowed=${report.summary.v40cApplyAllowed}`
  );

  for (const row of report.table) {
    console.log(
      `  ${row.brandSlug}: ${row.canonicalState} → ${row.allowedNextAction} | founder=${row.founderReviewAllowed} active=${row.activeReleaseAllowed}`
    );
  }

  const skipRegression = process.argv.includes("--skip-regression");
  if (!skipRegression) {
    const primary = "everhome-suites,kimpton,radisson-individuals-by-choice";
    const all = opts.brands.join(",");
    const validations = [
      runNpm(["test:brand-explorer-golden-release-suite", "--", "--brands", primary]),
      runNpm(["test:brand-explorer-internal-preview-owner-copy", "--", "--brands", primary]),
      runNpm(["test:brand-explorer-external-quality-lock", "--", "--brands", all]),
      runNpm(["test:brand-explorer-active-profile-staged-apply"]),
      runNpm(["test:partner-intelligence-publish-readiness"]),
      runNpm(["test:partner-intelligence-profile-governance-publish"]),
    ];
    report.regression = validations;
    writeV41OsReports(report);
    const failed = validations.filter((v) => v.status !== "pass");
    if (failed.length) {
      console.error("Validation failed:");
      for (const f of failed) console.error(`  npm run ${f.args.join(" ")}`);
      process.exit(1);
    }
  }

  console.log("v41 OS complete — read-only; no Airtable writes; no unlock; no active release.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
