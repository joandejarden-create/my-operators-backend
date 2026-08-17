#!/usr/bin/env node
/**
 * v42 — Brand Explorer Founder Visual Review Packet + Release Recommendation (read-only).
 *
 * Produces founder-facing packets for internal-preview judgment before active release.
 * Does not unlock, approve, or write Airtable.
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V42_VERSION,
  V42_DEFAULT_BRANDS,
  runBrandExplorerFounderVisualReview,
  writeV42Reports,
} from "../lib/partner-intelligence/brand-explorer-founder-visual-review.js";

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
      : [...V42_DEFAULT_BRANDS];

  if (argv.includes("--apply")) {
    console.error("v42 refuses --apply. Founder visual review is read-only / dry-run only.");
    process.exit(2);
  }

  return { brands, dryRun: true };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(
    `[${V42_VERSION}] Founder visual review (dryRun=true, internalPreview=on, no Airtable writes)`
  );
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);

  const report = await runBrandExplorerFounderVisualReview(opts);
  const paths = writeV42Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: approve=${report.summary.approveForActiveRelease} minor_cleanup=${report.summary.approveAfterMinorCleanup} remediation=${report.summary.remediationRequired} not_owner_ready=${report.summary.notOwnerReady} incomplete_locked=${report.summary.incompleteControlLocked}`
  );

  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: ${b.releaseRecommendation.recommendation} | tabs p/c/f=${b.tabStatusCounts.pass}/${b.tabStatusCounts.concern}/${b.tabStatusCounts.fail} | gallery=${b.visualAssets.gallery.count} openings=${b.visualAssets.propertyExamples.count} | os=${b.os.canonicalState}`
    );
    console.log(`    packet: ${paths.founderPaths[b.brandSlug]}`);
  }

  console.log(
    "v42 complete — read-only; no unlock; no active approval; no Company Validated; no active release."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
