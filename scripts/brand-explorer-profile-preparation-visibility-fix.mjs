#!/usr/bin/env node
/**
 * Brand Explorer — Profile in Preparation visibility fix.
 * Dry-run by default. Apply requires explicit confirmation flags.
 */
import "dotenv/config";
import {
  runProfilePreparationVisibilityFix,
  writeProfilePreparationVisibilityFixReports,
  applyProfilePreparationVisibilityFix,
  parseVisibilityFixApplyFlags,
  VISIBILITY_FIX_VERSION,
} from "../lib/partner-intelligence/brand-explorer-profile-preparation-visibility-fix.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : null;
  return { brands, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const flags = parseVisibilityFixApplyFlags(opts.argv);
  console.log(`[${VISIBILITY_FIX_VERSION}] dryRun=${!flags.apply}`);

  const report = await runProfilePreparationVisibilityFix({ slugs: opts.brands });
  report.dryRun = !flags.apply;
  const paths = writeProfilePreparationVisibilityFixReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: restorePlanned=${report.summary.restorePlannedCount} · ${ (report.summary.restoredProfiles || []).join(", ") }`
  );
  for (const p of report.plannedRestores || []) {
    console.log(
      `  ${p.slug}: willRestore=${p.willRestore} class=${p.classification} fields=${Object.keys(p.patch?.fields || {}).join(",") || "—"}`
    );
  }

  if (flags.apply) {
    if (!flags.ok) {
      console.error(`Missing apply flags: ${flags.missing.join(", ")}`);
      process.exit(2);
    }
    const applied = await applyProfilePreparationVisibilityFix({
      report,
      apply: true,
      argv: opts.argv,
    });
    console.log(`Apply: ${JSON.stringify(applied, null, 2)}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
