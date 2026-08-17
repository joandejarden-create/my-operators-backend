#!/usr/bin/env node
/**
 * Brand Explorer v37B — Lifestyle Batch Config + Source Library Seeding.
 *
 *   npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands hotel-indigo,mgallery-collection --dry-run
 */
import "dotenv/config";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_BRANDS_ONLY,
  APPLY_FLAG_NO_ACTIVE_APPROVAL,
  APPLY_FLAG_NO_IMAGE_FIELDS,
  APPLY_FLAG_NO_PRESENTATION,
  APPLY_FLAG_NO_REGISTRY,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_SOURCE_ONLY,
  DEFAULT_V37B_BRANDS,
  runV37BLifestyleBatchSourceSeeding,
  writeV37BReports,
  V37B_VERSION,
} from "../lib/partner-intelligence/brand-explorer-v37b-lifestyle-batch-source-seeding.js";

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
      : [...DEFAULT_V37B_BRANDS];
  const apply = argv.includes("--apply");
  return {
    brands,
    dryRun: !apply,
    apply,
    approveBatch: argv.includes(APPLY_FLAG_APPROVE),
    sourceOnly: argv.includes(APPLY_FLAG_SOURCE_ONLY),
    noValidationClaim: argv.includes(APPLY_FLAG_NO_VALIDATION),
    noPresentation: argv.includes(APPLY_FLAG_NO_PRESENTATION),
    noRegistry: argv.includes(APPLY_FLAG_NO_REGISTRY),
    noImageFields: argv.includes(APPLY_FLAG_NO_IMAGE_FIELDS),
    noActiveApproval: argv.includes(APPLY_FLAG_NO_ACTIVE_APPROVAL),
    brandsOnly: argv.includes(APPLY_FLAG_BRANDS_ONLY),
  };
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
  const opts = parseArgs(process.argv.slice(2));

  const report = await runV37BLifestyleBatchSourceSeeding(opts);
  const validationTests = VALIDATION_SCRIPTS.map(runValidation);
  report.validationTests = validationTests;
  const paths = writeV37BReports(report, ROOT);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  for (const [key, p] of Object.entries(paths.perBrand || {})) {
    console.log(`  ${key}: ${p}`);
  }

  console.log(`Validation: ${V37B_VERSION} (${opts.dryRun ? "dry-run" : "apply"})`);
  for (const t of validationTests) {
    console.log(`  ${t.script}: ${t.status}${t.exitCode != null ? ` (${t.exitCode})` : ""}`);
  }

  console.log("Batch readiness:");
  for (const brand of report.brandResults) {
    console.log(
      `  ${brand.slug}: config=${brand.configValidation?.ok ? "pass" : "fail"} sources=${brand.sourceSeeding?.proposedCreates?.length || 0}c/${brand.sourceSeeding?.proposedUpdates?.length || 0}u images=${brand.imageReview?.status} next=${brand.readiness?.allowedNextAction}`
    );
  }

  if (opts.apply && !report.canApply) {
    console.error("Apply blocked:", report.applyBlockers.join(", "));
    process.exit(1);
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
