#!/usr/bin/env node
/**
 * Targeted ADR scrub for flagged valueOwners.scenario.* Body rows.
 *
 *   npm run brand-explorer-39-live-adr-scenario-scrub -- --dry-run
 *   npm run brand-explorer-39-live-adr-scenario-scrub -- --apply ...flags
 *   npm run brand-explorer-39-live-adr-scenario-scrub -- --extract-only
 */
import "../load-env.js";
import {
  APPLY_FLAGS,
  run39LiveAdrScenarioScrub,
} from "../lib/partner-intelligence/brand-explorer-39-live-adr-scenario-scrub.js";

const argv = process.argv.slice(2);
if (argv.includes("--dry-run") && argv.includes("--apply")) {
  console.error("Pass only one of --dry-run or --apply");
  process.exit(1);
}
const apply = argv.includes("--apply");
const extractOnly = argv.includes("--extract-only");

const report = await run39LiveAdrScenarioScrub({ apply, argv, extractOnly });

if (report.paths) {
  if (report.paths.jsonPath) console.log(`Wrote ${report.paths.jsonPath}`);
  if (report.paths.mdPath) console.log(`Wrote ${report.paths.mdPath}`);
  if (report.paths.docPath) console.log(`Wrote ${report.paths.docPath}`);
}
console.log(`Wrote reports/brand-explorer-39-live-adr-scenario-scrub-failures.json`);
console.log(
  JSON.stringify(
    {
      version: report.version,
      applyPerformed: report.applyPerformed,
      writePerformed: report.writePerformed,
      plannedPatchCount: report.plannedPatchCount,
      appliedPatchCount: report.appliedPatchCount,
      blockedPatchCount: report.blockedPatchCount,
      readyStatement: report.readyStatement,
      missingFlags: apply ? report.flagCheck?.missing : undefined,
    },
    null,
    2
  )
);

if (apply && report.flagCheck && !report.flagCheck.ok) {
  console.error(`Missing flags: ${APPLY_FLAGS.filter((f) => !argv.includes(f)).join(", ")}`);
  process.exit(1);
}
if (report.blockedPatchCount > 0 && !extractOnly) {
  console.error(`Blocked patches: ${report.blockedPatchCount}`);
  process.exitCode = 1;
}
