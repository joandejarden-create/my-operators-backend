#!/usr/bin/env node
/**
 * Distinct gallery rematerialization for MGallery + SLH.
 * Dry-run by default. Apply only with full confirmation flags.
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DISTINCT_GALLERY_VERSION,
  TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  runDistinctGalleryRematerialization,
  writeDistinctGalleryReports,
  parseDistinctGalleryApplyFlags,
} from "../lib/partner-intelligence/brand-explorer-distinct-gallery-rematerialization.js";

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
      : [...TARGET_BRANDS];

  const applyRequested = argv.includes("--apply");
  const flagCheck = parseDistinctGalleryApplyFlags(argv);
  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `distinct-gallery --apply requires all gates:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }
  return { brands, dryRun: !applyRequested, apply: applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${DISTINCT_GALLERY_VERSION}] dryRun=${opts.dryRun}`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);

  const report = await runDistinctGalleryRematerialization({
    brands: opts.brands,
    dryRun: opts.dryRun,
    argv: opts.argv,
  });
  const paths = writeDistinctGalleryReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  for (const [slug, p] of Object.entries(paths.brandPaths || {})) {
    console.log(`  ${slug}: ${p}`);
  }

  console.log(
    `Summary: projectedPass=${report.summary.projectedPassCount}/${report.summary.brandCount} canApply=${report.summary.canApplyCount} applied=${report.summary.applied}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: before g=${b.before.galleryDistinctCount} → projected g=${b.projected.galleryDistinctCount} canApply=${b.canApply} action=${b.requiredAction}`
    );
  }

  if (!report.auditPass && opts.dryRun) {
    console.error("Projected uniqueness did not pass for all brands — do not apply.");
    process.exit(3);
  }
  if (opts.apply && report.applyResult?.applied !== true) {
    console.error("Apply did not complete successfully:", report.applyResult);
    process.exit(2);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
