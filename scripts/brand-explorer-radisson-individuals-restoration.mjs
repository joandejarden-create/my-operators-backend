#!/usr/bin/env node
/**
 * Radisson Individuals by Choice restoration — dry-run by default.
 */
import "dotenv/config";
import {
  runRadissonIndividualsRestoration,
  writeRadissonIndividualsRestorationReports,
  applyRadissonIndividualsRestoration,
  verifyRadissonIndividualsRestorationAfterApply,
  parseRestorationApplyFlags,
  RESTORATION_VERSION,
} from "../lib/partner-intelligence/brand-explorer-radisson-individuals-restoration.js";

async function main() {
  const argv = process.argv.slice(2);
  const flags = parseRestorationApplyFlags(argv);
  console.log(
    `[${RESTORATION_VERSION}] dryRun=${!flags.apply} contentOnly=${flags.contentOnly} forceImages=${flags.forceImages}`
  );

  const report = await runRadissonIndividualsRestoration({
    dryRun: !flags.apply,
    contentOnly: flags.contentOnly,
    forceImages: flags.forceImages,
  });
  report.dryRun = !flags.apply;
  let paths = writeRadissonIndividualsRestorationReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Validation pass=${report.validation.pass} contentOnly=${report.contentOnly} fieldGatePatches=${report.fieldGate?.patchCount || 0} residual=${report.residualScrub?.appliedPatchCount || 0} propertyDistinct=${report.projected.uniqueness.propertyExampleDistinctCount} roleMatch=${report.projected.roleMatch.pass}`
  );
  if (report.validation.failedChecks?.length) {
    console.log(`Failed checks: ${report.validation.failedChecks.join(", ")}`);
  }
  if (report.fieldGate?.blockers?.length) {
    console.log(`Field-gate blockers: ${JSON.stringify(report.fieldGate.blockers)}`);
  }

  if (flags.apply) {
    if (!flags.ok) {
      console.error(`Missing apply flags: ${flags.missing.join(", ")}`);
      process.exit(2);
    }
    const applied = await applyRadissonIndividualsRestoration({
      report,
      apply: true,
      argv,
    });
    console.log(`Apply: ${JSON.stringify(applied, null, 2)}`);
    if (!applied.applied) process.exit(2);

    // Allow Airtable CDN / presentation settle
    await new Promise((r) => setTimeout(r, 3500));
    const after = await verifyRadissonIndividualsRestorationAfterApply();
    report.after = after;
    report.dryRun = false;
    paths = writeRadissonIndividualsRestorationReports(report);
    console.log(`After OS=${after.osState} → ${after.osAction}`);
    console.log(`Acceptance: ${JSON.stringify(after.acceptance)}`);
    console.log(`Remaining failed gates: ${(after.osFailedGates || []).join("; ") || "—"}`);
    console.log(`Updated ${paths.mdPath}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
