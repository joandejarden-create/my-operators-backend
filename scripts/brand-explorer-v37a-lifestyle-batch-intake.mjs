#!/usr/bin/env node
/**
 * Brand Explorer v37A Lifestyle Batch Intake (read-only).
 *
 *   npm run brand-explorer-v37a-lifestyle-batch-intake -- --brands hotel-indigo,mgallery-collection --dry-run
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEFAULT_V37A_BRANDS,
  runV37ALifestyleBatchIntake,
  writeV37AReports,
  V37A_VERSION,
} from "../lib/partner-intelligence/brand-explorer-v37a-lifestyle-batch-intake.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const VALIDATION_SCRIPTS = [
  "test:brand-explorer-active-profile-staged-apply",
  "test:partner-intelligence-publish-readiness",
  "test:partner-intelligence-profile-governance-publish",
];

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...DEFAULT_V37A_BRANDS];
  const dryRun = !argv.includes("--apply");
  return { brands, dryRun };
}

function runValidation(scriptName) {
  const res = spawnSync("npm", ["run", scriptName], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return {
    script: scriptName,
    status: res.status === 0 ? "pass" : "fail",
    exitCode: res.status,
  };
}

async function main() {
  const { brands, dryRun } = parseArgs(process.argv.slice(2));
  if (!dryRun) {
    console.error("v37A is read-only — use --dry-run (default). No Airtable writes.");
    process.exit(1);
  }

  const report = await runV37ALifestyleBatchIntake({ brands, dryRun: true });
  const validationTests = VALIDATION_SCRIPTS.map(runValidation);
  report.validationTests = validationTests;
  const paths = writeV37AReports(report, ROOT);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  for (const [slug, p] of Object.entries(paths.perBrand)) {
    console.log(`  ${slug}: knowledge + visual + presentation + external-owner reports`);
  }
  console.log(`Validation: ${V37A_VERSION} (read-only)`);
  for (const t of validationTests) {
    console.log(`  ${t.script}: ${t.status}${t.exitCode != null ? ` (${t.exitCode})` : ""}`);
  }

  for (const brand of report.brandResults) {
    console.log(
      `  ${brand.brandName}: record=${brand.discovery.found ? "yes" : "NO"} model=${brand.modelClassification.selectedBrandModelType} gallery=${brand.visualAssetPack.summary.galleryImageUrlReady}/6`
    );
  }

  const failed = validationTests.filter((t) => t.status !== "pass");
  if (failed.length) {
    console.error("One or more regression tests failed.");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
