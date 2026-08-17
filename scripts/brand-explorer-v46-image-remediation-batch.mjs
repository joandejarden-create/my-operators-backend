#!/usr/bin/env node
/**
 * v46 — Image remediation batch for Hotel Indigo, MGallery, SLH (read-only).
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V46_VERSION,
  V46_TARGET_BRANDS,
  runV46ImageRemediationBatch,
  writeV46Reports,
} from "../lib/partner-intelligence/brand-explorer-v46-image-remediation-batch.js";

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
      : [...V46_TARGET_BRANDS];
  if (argv.includes("--apply")) {
    throw new Error("v46 has no --apply path in this stage. Read-only image remediation dry-run only.");
  }
  return { brands, dryRun: true };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V46_VERSION}] Image remediation batch (dryRun=true)`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  console.log("  guardrails: no unlock · no Presentation writes · no released brand changes · no generic images");

  const report = await runV46ImageRemediationBatch(opts);
  const paths = writeV46Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.baselinePath}`);
  for (const [slug, p] of Object.entries(paths.brandPaths || {})) {
    console.log(`  ${slug}: ${p}`);
  }

  console.log(
    `Summary: osRouting=${report.summary.osRoutingPass} baseline=${report.summary.baselineProtectionPass} eligibility=${JSON.stringify(report.summary.byEligibility)}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: live g/p=${b.liveApi.galleryImageUrlCount}/${b.liveApi.propertyImageUrlCount} accepted g/p=${b.visualAssetPack.acceptedCounts.gallery}/${b.visualAssetPack.acceptedCounts.propertyExamples} → ${b.eligibility.status}`
    );
  }

  if (!report.osConfirm.pass) {
    console.error("OS routing confirmation failed:");
    for (const b of report.osConfirm.blockers || []) console.error(`  ${b}`);
    process.exit(2);
  }
  if (!report.baselineProtection.pass) {
    console.error("Released baseline protection failed:");
    for (const f of report.baselineProtection.failures || []) console.error(`  ${f}`);
    process.exit(1);
  }

  console.log("v46 complete — image remediation audited; no writes; released brands protected.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
