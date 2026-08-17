#!/usr/bin/env node
/**
 * v47 — Batch image materialization + draft readiness (Indigo, MGallery, SLH).
 */
import "dotenv/config";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  V47_VERSION,
  V47_TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  runV47BatchImageMaterialization,
  writeV47Reports,
  parseV47ApplyFlags,
} from "../lib/partner-intelligence/brand-explorer-v47-batch-image-materialization.js";

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
      : [...V47_TARGET_BRANDS];

  const applyRequested = argv.includes("--apply");
  const flagCheck = parseV47ApplyFlags(argv);
  let dryRun = !applyRequested;

  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `v47 --apply requires all gates:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }

  return { brands, dryRun, apply: applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${V47_VERSION}] Batch image materialization (dryRun=${opts.dryRun})`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  console.log(`  cwd: ${ROOT}`);
  console.log(
    "  guardrails: no unlock · no Company Validated · no released brand changes · no Source Library · external locked"
  );

  const report = await runV47BatchImageMaterialization({
    brands: opts.brands,
    dryRun: opts.dryRun,
    argv: opts.argv,
  });
  const paths = writeV47Reports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.baselinePath}`);
  for (const [slug, p] of Object.entries(paths.brandPaths || {})) {
    console.log(`  ${slug}: ${p}`);
  }

  console.log(
    `Summary: osRouting=${report.summary.osRoutingPass} baseline=${report.summary.baselineProtectionPass} eligibility=${JSON.stringify(report.summary.byEligibility)} writes=${report.summary.presentationWrites}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: live g/p=${b.liveApi.galleryImageUrlCount}/${b.liveApi.propertyImageUrlCount} project ${b.projection.galleryProjected}/${b.projection.propertyProjected} → ${b.eligibility.status}${b.materializationBlocked ? " [BLOCKED]" : ""}`
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
  if (report.summary.materializationBlocked > 0) {
    console.error("One or more brands failed materialization plan validation.");
    process.exit(3);
  }

  console.log(
    opts.dryRun
      ? "v47 complete — materialization planned; no writes; released brands protected."
      : "v47 complete — materialization applied for gated brands; external profiles remain locked."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
