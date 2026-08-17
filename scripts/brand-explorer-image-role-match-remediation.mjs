#!/usr/bin/env node
/**
 * Image role-match remediation (MGallery primary).
 * Dry-run by default.
 */
import "dotenv/config";
import {
  TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  parseRoleMatchApplyFlags,
  runImageRoleMatchRemediation,
  writeImageRoleMatchRemediationReports,
} from "../lib/partner-intelligence/brand-explorer-image-role-match-remediation.js";

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
  const flagCheck = parseRoleMatchApplyFlags(argv);
  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `role-match remediation --apply requires:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }
  return { brands, dryRun: !applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[image-role-match-remediation] dryRun=${opts.dryRun} brands=${opts.brands.join(",")}`);
  const report = await runImageRoleMatchRemediation(opts);
  const paths = writeImageRoleMatchRemediationReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: projectedPass=${report.summary.projectedRolePass}/${report.summary.brandCount} canApply=${report.summary.canApplyCount} applied=${report.summary.applied}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: unresolvedBefore=${b.before.unresolvedRoleMismatchCount} → rolePass=${b.projected.roleMatchPass} uniq=${b.projected.uniquenessPass} canApply=${b.canApply}`
    );
  }
  if (!report.auditPass && opts.dryRun) process.exit(3);
  if (!opts.dryRun && report.applyResult?.applied !== true) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
