#!/usr/bin/env node
/**
 * v37C-R2 — External Display Gating Verification + UI Proof Test.
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_BRANDS,
  runV37CR2ExternalDisplayGatingVerification,
  writeV37CR2Reports,
  V37C_R2_VERSION,
} from "../lib/partner-intelligence/brand-explorer-v37c-r2-external-display-gating-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const VALIDATION_SCRIPTS = [
  "test:brand-explorer-external-display-gating",
  "test:brand-explorer-active-profile-staged-apply",
  "test:partner-intelligence-publish-readiness",
  "test:partner-intelligence-profile-governance-publish",
];

const POST_DRY_RUNS = [
  ["brand-explorer-active-profile-asset-pack", "hotel-indigo"],
  ["brand-explorer-active-profile-build-draft", "hotel-indigo"],
  ["brand-explorer-active-profile-asset-pack", "mgallery-collection"],
  ["brand-explorer-active-profile-build-draft", "mgallery-collection"],
];

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
  return { args, status: res.status === 0 ? "pass" : "fail", exitCode: res.status, stderr: res.stderr?.slice(0, 500) };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V37C_R2_VERSION}] External display gating verification (dryRun=${opts.dryRun})`);

  const report = await runV37CR2ExternalDisplayGatingVerification(opts);
  const paths = writeV37CR2Reports(report);
  console.log(`Wrote ${paths.jsonPath}`);

  const postRuns = [];
  for (const [script, brand] of POST_DRY_RUNS) {
    if (!opts.brands.includes(brand)) continue;
    postRuns.push(runNpm([script, "--", "--brand", brand, "--dry-run"]));
  }

  const validations = [
    runNpm(["test:brand-explorer-external-display-gating", "--", "--brands", opts.brands.join(",")]),
    runNpm(["test:brand-explorer-active-profile-staged-apply"]),
    runNpm(["test:partner-intelligence-publish-readiness"]),
    runNpm(["test:partner-intelligence-profile-governance-publish"]),
  ];

  report.postDryRuns = postRuns;
  report.regression = validations;
  writeV37CR2Reports(report);

  const failed = [...postRuns, ...validations].filter((r) => r.status !== "pass");
  if (!report.summary.allUiProofPass) {
    console.error("UI proof failed for one or more brands.");
    process.exit(1);
  }
  if (failed.length) {
    console.error("One or more regression/post dry-runs failed.");
    process.exit(1);
  }
  console.log("v37C-R2 complete — UI proof and regressions passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
