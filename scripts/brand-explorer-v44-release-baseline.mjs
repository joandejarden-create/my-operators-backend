#!/usr/bin/env node
/**
 * v44 — Brand Explorer OS Release Baseline + Next Batch Router (read-only).
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V44_VERSION,
  V44_DEFAULT_BRANDS,
  runV44ReleaseBaseline,
  writeV44Reports,
} from "../lib/partner-intelligence/brand-explorer-v44-release-baseline.js";

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
      : [...V44_DEFAULT_BRANDS];
  const dryRun = !argv.includes("--apply");
  if (argv.includes("--apply")) {
    throw new Error("v44 has no --apply path. Read-only baseline only. Use --dry-run.");
  }
  return { brands, dryRun };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V44_VERSION}] Release baseline + next-batch router (dryRun=${opts.dryRun})`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  console.log("  guardrails: no Airtable writes · no unlock · no Company Validated · no released content changes");

  const report = await runV44ReleaseBaseline(opts);
  const paths = writeV44Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.nextBatchPath}`);
  console.log(
    `Summary: releasedActive=${report.summary.releasedAllActive} incompleteLocked=${report.summary.incompleteAllLocked} regression=${report.summary.regressionPass ? "PASS" : "FAIL"} preferredBatch=${report.summary.preferredNextBatch}`
  );

  for (const row of report.releaseBaseline || []) {
    console.log(
      `  [released] ${row.brandSlug}: full=${row.shouldRenderFullProfile} gallery=${row.galleryImageUrlCount} property=${row.propertyImageUrlCount} tabs=${row.externalTabCount} cv=${row.companyValidated} lock=${row.externalQualityLockPass}`
    );
  }
  for (const row of report.nextBatch?.incompleteRows || []) {
    console.log(
      `  [incomplete] ${row.brandSlug}: state=${row.currentState} action=${row.allowedNextAction} (expected=${row.expectedNextAction}) batchOk=${row.batchProcessingPossible}`
    );
  }

  console.log(`Preferred next batch: ${report.nextBatch?.recommendation?.preferred}`);
  console.log(report.nextBatch?.recommendation?.rationale || "");

  if (!report.regression.pass) {
    console.error("Regression failures:");
    for (const f of report.regression.failures) console.error(`  - ${f}`);
    process.exit(1);
  }

  console.log("v44 complete — baseline frozen; incomplete brands routed; no writes.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
