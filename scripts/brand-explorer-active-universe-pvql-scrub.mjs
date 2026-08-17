#!/usr/bin/env node
/**
 * Active-universe PVQL scrub — 16 public-full brands. Dry-run by default.
 */
import "dotenv/config";
import {
  ACTIVE_UNIVERSE_PVQL_SCRUB_VERSION,
  ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS,
  REQUIRED_APPLY_FLAGS,
  parseActiveUniversePvqlScrubFlags,
  planActiveUniversePvqlScrub,
  applyActiveUniversePvqlScrub,
  writeActiveUniversePvqlScrubReports,
} from "../lib/partner-intelligence/brand-explorer-active-universe-pvql-scrub.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS];
  return {
    brands,
    apply: argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${ACTIVE_UNIVERSE_PVQL_SCRUB_VERSION}]`);
  console.log(`Targets: ${opts.brands.join(", ")}`);
  console.log(`Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  if (opts.apply) {
    const flags = parseActiveUniversePvqlScrubFlags(opts.argv);
    if (!flags.ok) {
      console.error("Missing apply flags:", flags.missing.join(", "));
      console.error("Required:", REQUIRED_APPLY_FLAGS.join(" "));
      process.exit(1);
    }
  }

  const report = await planActiveUniversePvqlScrub({ brands: opts.brands });
  const applyResult = await applyActiveUniversePvqlScrub({
    report,
    apply: opts.apply,
    argv: opts.argv,
  });
  const paths = writeActiveUniversePvqlScrubReports(report, applyResult);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(
    `Summary: offenders=${report.summary.offenders} patches=${report.summary.patches} remaining=${(report.summary.uncleanAfterProjection || []).join(",") || "none"} applied=${applyResult.applied === true}`
  );

  if (!report.validation.pass) process.exitCode = 2;
  if (opts.apply && applyResult.applied !== true) {
    console.error("Apply failed:", applyResult.reason, applyResult.missing || applyResult.failedChecks || "");
    process.exitCode = 3;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
