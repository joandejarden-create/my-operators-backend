#!/usr/bin/env node
/**
 * Freeze / report protected 62 Active/Live public-full Brand Explorer baseline (read-only).
 *
 *   npm run brand-explorer-62-active-public-full-baseline -- --dry-run
 */
import "../load-env.js";
import {
  BASELINE_VERSION_62,
  build62ActivePublicFullBaseline,
  write62ActivePublicFullBaselineReports,
} from "../lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js";

async function main() {
  const argv = process.argv.slice(2);
  if (!argv.includes("--dry-run") && !argv.includes("--report-only")) {
    console.error("Refusing to run without --dry-run (report-only freeze).");
    process.exit(1);
  }

  console.log(`[${BASELINE_VERSION_62}] public-full freeze report (read-only)`);
  const report = await build62ActivePublicFullBaseline({
    requireReports: true,
    evaluateEvidence: !argv.includes("--skip-evidence"),
  });
  const paths = write62ActivePublicFullBaselineReports(report);
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
