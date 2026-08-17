#!/usr/bin/env node
/**
 * Public restore governance — dry-run by default.
 * Apply only with founder confirm flags (does not rewrite content/images/CV/Source/Registry).
 */
import "dotenv/config";
import {
  PUBLIC_RESTORE_GOVERNANCE_VERSION,
  REQUIRED_APPLY_FLAGS,
  resolvePublicRestoreBrands,
  parsePublicRestoreApplyFlags,
  planPublicRestoreGovernance,
  applyPublicRestoreGovernance,
  writePublicRestoreGovernanceReports,
} from "../lib/partner-intelligence/brand-explorer-public-restore-governance.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;
  return {
    brands: resolvePublicRestoreBrands(brands),
    apply: argv.includes("--apply"),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${PUBLIC_RESTORE_GOVERNANCE_VERSION}] public restore governance`);
  console.log(`Brands: ${opts.brands.join(", ")}`);
  console.log(`Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  const plan = await planPublicRestoreGovernance({ brands: opts.brands });
  let applyResult = null;
  if (opts.apply) {
    const flags = parsePublicRestoreApplyFlags(opts.argv);
    if (!flags.ok) {
      console.error("Missing required apply flags:");
      for (const f of flags.missing) console.error(`  ${f}`);
      console.error("\nRequired:");
      for (const f of REQUIRED_APPLY_FLAGS) console.error(`  ${f}`);
      process.exitCode = 1;
      return;
    }
    applyResult = await applyPublicRestoreGovernance({
      plan,
      apply: true,
      argv: opts.argv,
    });
    if (!applyResult.applied) {
      console.error(`Apply refused: ${applyResult.reason}`);
      if (applyResult.missing?.length) {
        for (const f of applyResult.missing) console.error(`  missing ${f}`);
      }
      process.exitCode = 1;
    } else {
      console.log(`Restored: ${(applyResult.restoredSlugs || []).join(", ")}`);
    }
  } else {
    console.log(
      `Dry-run: eligible=${plan.summary.eligibleRestoreCount} heldAccidental=${plan.summary.heldAccidentalUnlockCount}`
    );
    console.log("Public restore NOT applied — founder approval + --apply flags required.");
  }

  const report = { ...plan, dryRun: !opts.apply, applyResult };
  const paths = writePublicRestoreGovernanceReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
