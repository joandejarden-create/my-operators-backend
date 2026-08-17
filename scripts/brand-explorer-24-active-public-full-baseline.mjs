#!/usr/bin/env node
/**
 * Freeze protected 24 Active/Live Brand Explorer baseline (read-only).
 *
 *   npm run brand-explorer-24-active-public-full-baseline -- --dry-run
 */
import "dotenv/config";
import {
  BASELINE_VERSION,
  run24ActivePublicFullBaselineFreeze,
  write24ActivePublicFullBaselineReports,
} from "../lib/partner-intelligence/brand-explorer-24-active-public-full-baseline.js";

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("No --apply path. This baseline freeze is report-only. Use --dry-run.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (no Airtable writes).");
    process.exit(2);
  }

  console.log(`[${BASELINE_VERSION}] freeze protected 24 Active/Live baseline (dry-run / report-only)`);
  console.log("  guardrails: no Airtable · no Presentation · no image · no CV · no Source · no Registry · no Brand Status");

  const report = await run24ActivePublicFullBaselineFreeze();
  const paths = write24ActivePublicFullBaselineReports(report);

  console.log(`Active count: ${report.activeCount}`);
  console.log(`Freeze decision: ${report.freezeDecision}`);
  console.log(`Public-full: ${report.summary.publicFullCount}`);
  console.log(`PVQL pass: ${report.summary.pvqlPassCount}`);
  console.log(`Quality freeze: ${report.summary.freezeRecommendationCount}`);
  console.log(`Cross-brand reuse: ${report.summary.crossBrandImageReuse}`);
  console.log(`Excluded: ${report.excludedNonActive.map((e) => `${e.slug}(${e.brandStatus})`).join(", ")}`);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);

  if (!report.frozen) {
    console.error("Freeze incomplete — baseline artifact written but frozen=false.");
    process.exit(1);
  }
  console.log("Baseline frozen (report-only).");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
