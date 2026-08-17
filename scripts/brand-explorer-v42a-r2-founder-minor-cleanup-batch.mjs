#!/usr/bin/env node
/**
 * v42A-R2 — Founder minor cleanup batch (Indigo, MGallery, SLH).
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V42A_R2_VERSION,
  V42A_R2_TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  runV42AR2FounderMinorCleanupBatch,
  writeV42AR2Reports,
  parseV42AR2ApplyFlags,
} from "../lib/partner-intelligence/brand-explorer-v42a-r2-founder-minor-cleanup-batch.js";

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
      : [...V42A_R2_TARGET_BRANDS];

  const applyRequested = argv.includes("--apply");
  const flagCheck = parseV42AR2ApplyFlags(argv);
  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `v42A-R2 --apply requires all gates:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }
  return { brands, dryRun: !applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V42A_R2_VERSION}] Founder minor cleanup batch (dryRun=${opts.dryRun})`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  console.log(
    "  guardrails: no unlock · no CV · no source/registry/images · no released brand changes"
  );

  const report = await runV42AR2FounderMinorCleanupBatch({
    brands: opts.brands,
    dryRun: opts.dryRun,
    argv: opts.argv,
  });
  const paths = writeV42AR2Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  for (const [slug, p] of Object.entries(paths.brandPaths || {})) {
    console.log(`  ${slug}: ${p}`);
  }

  console.log(
    `Summary: os=${report.summary.osRoutingPass} baseline=${report.summary.baselineProtectionPass} patches=${report.summary.totalPatches} projectedApprove=${report.summary.projectedApproveForActiveRelease} writes=${report.summary.presentationWrites}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: patches=${b.patches.length} → ${b.projection.founderDecision}${b.blocked ? " [BLOCKED]" : ""}`
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
  if (report.summary.blockedBrands > 0) {
    console.error("One or more brands blocked.");
    process.exit(3);
  }

  console.log(
    opts.dryRun
      ? "v42A-R2 complete — cleanup planned; no writes; released brands protected."
      : "v42A-R2 complete — standards checklist applied; external profiles remain locked."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
