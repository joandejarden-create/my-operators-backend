#!/usr/bin/env node
/**
 * PVQL failure scrub — dry-run by default.
 */
import "dotenv/config";
import {
  SCRUB_VERSION,
  PVQL_SCRUB_TARGETS,
  REQUIRED_APPLY_FLAGS,
  parsePvqlScrubApplyFlags,
  planPvqlFailureScrub,
  applyPvqlFailureScrub,
  writePvqlFailureScrubReports,
  buildPublicFullCohortDriftReport,
} from "../lib/partner-intelligence/brand-explorer-pvql-failure-scrub.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...PVQL_SCRUB_TARGETS];
  return {
    brands,
    apply: argv.includes("--apply"),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${SCRUB_VERSION}] PVQL failure scrub`);
  console.log(`Brands: ${opts.brands.join(", ")}`);
  console.log(`Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  const drift = await buildPublicFullCohortDriftReport();
  console.log(
    `Public-full cohort: observed=${drift.observedPublicFull} (expected baseline 11 / after restore 18)`
  );

  const report = await planPvqlFailureScrub({ brands: opts.brands });

  if (opts.apply) {
    const flags = parsePvqlScrubApplyFlags(opts.argv);
    if (!flags.ok) {
      console.error("Missing apply flags:", flags.missing.join(", "));
      console.error("Required:", REQUIRED_APPLY_FLAGS.join(" "));
      process.exit(1);
    }
  }

  const applyResult = await applyPvqlFailureScrub({
    report,
    apply: opts.apply,
    argv: opts.argv,
  });
  const paths = writePvqlFailureScrubReports(report, applyResult, drift);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: patches=${report.summary.patches} remaining=${report.summary.uncleanAfterProjection.join(",") || "none"} applied=${applyResult.applied === true}`
  );
  if (!report.validation.pass) process.exitCode = 2;
  if (opts.apply && applyResult.applied !== true) process.exitCode = 3;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
