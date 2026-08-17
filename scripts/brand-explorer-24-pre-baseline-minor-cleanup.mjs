#!/usr/bin/env node
/**
 * 24-brand pre-baseline minor cleanup.
 *
 * Dry-run:
 *   npm run brand-explorer-24-pre-baseline-minor-cleanup -- --brands ... --dry-run
 *
 * Apply (all flags required):
 *   npm run brand-explorer-24-pre-baseline-minor-cleanup -- --brands ... --apply \
 *     --approve-pre-baseline-minor-cleanup \
 *     --confirm-targeted-findings-only \
 *     ...
 */
import "dotenv/config";
import {
  CLEANUP_VERSION,
  DEFAULT_TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  normalizeCleanupSlug,
  runPreBaselineMinorCleanup,
  writePreBaselineMinorCleanupReports,
} from "../lib/partner-intelligence/brand-explorer-24-pre-baseline-minor-cleanup.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  let brands = [...DEFAULT_TARGET_BRANDS];
  if (brandsIdx >= 0 && argv[brandsIdx + 1]) {
    brands = argv[brandsIdx + 1]
      .split(",")
      .map((s) => normalizeCleanupSlug(s))
      .filter(Boolean);
  }
  const apply = argv.includes("--apply");
  const dryRun = argv.includes("--dry-run") || !apply;
  return { brands, apply, dryRun: apply ? false : dryRun };
}

async function main() {
  const argv = process.argv.slice(2);
  const opts = parseArgs(argv);

  if (opts.apply) {
    const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
    if (missing.length) {
      console.error("Missing required apply flags:");
      for (const m of missing) console.error(`  ${m}`);
      process.exit(2);
    }
  } else if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run or --apply with confirmation flags.");
    process.exit(2);
  }

  console.log(
    `[${CLEANUP_VERSION}] pre-baseline minor cleanup (${opts.apply ? "APPLY" : "dry-run"})`
  );
  console.log(`  brands: ${opts.brands.join(", ")}`);

  const report = await runPreBaselineMinorCleanup(opts);
  const paths = writePreBaselineMinorCleanupReports(report);

  console.log(`Patches: ${report.summary.totalPatches}`);
  console.log(`Image actions: ${report.summary.totalImageActions}`);
  for (const b of report.brandResults) {
    console.log(`  ${b.slug}: patches=${b.patchCount} images=${b.imageActionCount}`);
  }
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.imgMdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
