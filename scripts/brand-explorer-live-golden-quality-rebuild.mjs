#!/usr/bin/env node
/**
 * Live golden-quality rebuild CLI.
 */
import "dotenv/config";
import {
  TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  runLiveGoldenQualityRebuild,
  writeLiveGoldenRebuildReports,
  parseLiveGoldenApplyFlags,
  LIVE_GOLDEN_REBUILD_VERSION,
} from "../lib/partner-intelligence/brand-explorer-live-golden-quality-rebuild.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...TARGET_BRANDS];
  const applyRequested = argv.includes("--apply");
  const flagCheck = parseLiveGoldenApplyFlags(argv);
  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `Apply requires all gates:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }
  return { brands, dryRun: !applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${LIVE_GOLDEN_REBUILD_VERSION}] dryRun=${opts.dryRun}`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  const report = await runLiveGoldenQualityRebuild(opts);
  const paths = writeLiveGoldenRebuildReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  for (const [slug, p] of Object.entries(paths.brandPaths || {})) {
    console.log(`  ${slug}: ${p}`);
  }
  console.log(
    `Summary: defects=${report.summary.totalDefects} patches=${report.summary.totalPatches} blocked=${report.summary.blockedBrands} writes=${report.summary.writes}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: defects=${b.audit.defectCount} patches=${b.patches.length}${b.blocked ? " [BLOCKED]" : ""}`
    );
  }
  if (report.summary.blockedBrands > 0) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
