#!/usr/bin/env node
/**
 * Brand Explorer 62 — background validation vs new Hotel Property Census.
 * Read-only. Patch proposals only.
 *
 * Usage:
 *   node scripts/brand-explorer-62-background-validation.mjs --dry-run
 */
import "../load-env.js";
import {
  VALIDATION_VERSION,
  runBrandExplorer62BackgroundValidation,
  writeBackgroundValidationReports,
} from "../lib/partner-intelligence/brand-explorer-62-background-validation.js";

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. Background validation is patch-plan only.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (validation + patch proposals only; no writes).");
    process.exit(2);
  }

  console.log(`[${VALIDATION_VERSION}] starting (dry-run, no Census/BE writes)`);
  const plan = await runBrandExplorer62BackgroundValidation({ dryRun: true });
  const paths = writeBackgroundValidationReports(plan);

  console.log(`Status: ${plan.status}`);
  console.log(`Active 62 OK: ${plan.active62Validation.ok}`);
  console.log(`Census contract OK: ${plan.censusContract.contractOk}`);
  console.log(`Patch proposals: ${plan.patchProposals.length}`);
  console.log(`Founder decisions: ${plan.founderDecisionsNeeded.length}`);
  console.log(`Wrote ${paths.planJson}`);
  console.log(`Wrote ${paths.planMd}`);
  console.log(`Wrote ${paths.censusJson}`);
  console.log(`Wrote ${paths.censusMd}`);
  console.log(`Wrote ${paths.webflowJson}`);
  console.log(`Wrote ${paths.webflowMd}`);
  console.log(`Wrote ${paths.docsMd}`);

  if (plan.status === "brand_explorer_62_background_validation_hold_before_patch") {
    process.exitCode = 3;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
