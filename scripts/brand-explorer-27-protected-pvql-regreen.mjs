#!/usr/bin/env node
/**
 * Protected 27 PVQL re-green — Preferred / RI / SLH Target Guest Segments only.
 * Dry-run by default.
 */
import "../load-env.js";
import {
  REGREEN_VERSION,
  REQUIRED_APPLY_FLAGS,
  parseProtected27PvqlRegreenFlags,
  extractProtected27PvqlRegreenFailures,
  writeProtected27PvqlRegreenFailureReports,
  planProtected27PvqlRegreen,
  applyProtected27PvqlRegreen,
  writeProtected27PvqlRegreenReports,
} from "../lib/partner-intelligence/brand-explorer-27-protected-pvql-regreen.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : undefined;
  return {
    brands,
    apply: argv.includes("--apply"),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${REGREEN_VERSION}]`);
  console.log(
    `Targets: ${(opts.brands || ["preferred-hotels-and-resorts", "radisson-individuals-by-choice", "small-luxury-hotels-of-the-world"]).join(", ")}`
  );
  console.log(`Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  if (opts.apply) {
    const flags = parseProtected27PvqlRegreenFlags(opts.argv);
    if (!flags.ok) {
      console.error("Missing apply flags:", flags.missing.join(", "));
      console.error("Required:", REQUIRED_APPLY_FLAGS.join(" "));
      process.exit(1);
    }
  }

  const extract = await extractProtected27PvqlRegreenFailures(opts.brands);
  const failurePaths = writeProtected27PvqlRegreenFailureReports(extract);
  console.log(`Wrote ${failurePaths.jsonPath}`);
  console.log(`Wrote ${failurePaths.mdPath}`);

  const report = await planProtected27PvqlRegreen({ brands: opts.brands });
  const applyResult = await applyProtected27PvqlRegreen({
    report,
    apply: opts.apply,
    argv: opts.argv,
  });
  const paths = writeProtected27PvqlRegreenReports(report, applyResult);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  for (const p of paths.perBrandPaths || []) console.log(`Wrote ${p}`);
  console.log(
    `Summary: needingFix=${report.summary.needingFix} patches=${report.summary.patches} projectedClean=${report.summary.projectedClean} applied=${applyResult.applied === true}`
  );

  if (!report.validation.pass) process.exitCode = 2;
  if (opts.apply && applyResult.applied !== true) {
    console.error(
      "Apply failed:",
      applyResult.reason,
      applyResult.missing || applyResult.failedChecks || ""
    );
    process.exitCode = 3;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
