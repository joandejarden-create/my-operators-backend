#!/usr/bin/env node
/**
 * v42A — Founder minor cleanup for Everhome + Kimpton.
 *
 * Dry-run by default. Apply requires explicit approval flags.
 * Does not unlock, approve active profile, or touch Radisson / incomplete brands.
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V42A_VERSION,
  V42A_DEFAULT_BRANDS,
  V42A_APPLY_FLAGS,
  parseV42AApplyFlags,
  runV42AFounderMinorCleanup,
  writeV42AReports,
} from "../lib/partner-intelligence/brand-explorer-v42a-founder-minor-cleanup.js";

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
      : [...V42A_DEFAULT_BRANDS];

  const flags = parseV42AApplyFlags(argv);
  return {
    brands,
    dryRun: !flags.apply,
    apply: flags.apply,
    flags,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(
    `[${V42A_VERSION}] Founder minor cleanup (dryRun=${opts.dryRun}, apply=${opts.apply})`
  );
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  if (opts.apply) {
    console.log(`  apply flags: ${Object.values(V42A_APPLY_FLAGS).join(" ")}`);
  }

  const report = await runV42AFounderMinorCleanup(opts);
  const paths = writeV42AReports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: patches=${report.summary.totalPatches} recordsPatched=${report.summary.recordsPatched} projectedApprove=${report.summary.projectedApproveForActiveRelease}/${report.summary.brands} incompleteLocked=${report.summary.incompleteLocked} applyExecuted=${report.applyExecuted} applyBlocked=${report.applyBlocked}`
  );

  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: ${b.projection.recommendation} | property=${b.propertyExamples.decision.decision} ${b.propertyExamples.beforeCount}→${b.propertyExamples.afterCount} | tonePatches=${b.toneCleanup.patchCount} | patches=${b.patchSummary.total}`
    );
    console.log(`    packet: ${paths.brandPaths[b.brandSlug]}`);
  }

  if (opts.apply && report.applyBlocked) {
    console.error("Apply blocked:");
    for (const b of report.applyGateCheck.blockers || []) console.error(`  ${b}`);
    process.exit(2);
  }

  if (opts.apply && report.summary.applyErrors > 0) {
    console.error(`Apply completed with ${report.summary.applyErrors} error(s)`);
    process.exit(1);
  }

  console.log(
    "v42A complete — Radisson untouched; no unlock; no active approval; no Company Validated; no active release."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
