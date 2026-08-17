#!/usr/bin/env node
/**
 * Freeze / report protected 46 Active/Live public-full Brand Explorer baseline (read-only).
 *
 *   npm run brand-explorer-46-active-public-full-baseline -- --dry-run
 *
 * No Airtable writes. Requires recent quality audit + PVQL reports on disk
 * (or run those first in the acceptance sequence).
 */
import "../load-env.js";
import {
  BASELINE_VERSION_46,
  build46ActivePublicFullBaseline,
  write46ActivePublicFullBaselineReports,
} from "../lib/partner-intelligence/brand-explorer-46-active-public-full-baseline.js";

async function main() {
  const argv = process.argv.slice(2);
  if (!argv.includes("--dry-run") && !argv.includes("--report-only")) {
    console.error("Refusing to run without --dry-run (report-only freeze).");
    process.exit(1);
  }

  console.log(`[${BASELINE_VERSION_46}] public-full freeze report (read-only)`);
  const report = await build46ActivePublicFullBaseline({
    requireReports: true,
    evaluateEvidence: !argv.includes("--skip-evidence"),
  });
  const paths = write46ActivePublicFullBaselineReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Freeze decision: ${report.freezeDecision}`);
  console.log(`Summary: ${JSON.stringify(report.summary)}`);
  console.log(
    `Writes: airtable=${report.airtableWrites} presentation=${report.presentationWrites} image=${report.imageWrites} cv=${report.companyValidatedWrites}`
  );
  if (!report.frozen) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
