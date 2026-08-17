#!/usr/bin/env node
/**
 * Validate CALA submarket growth signal profiles against registry.
 */
import {
  validateCalaGrowthProfiles,
  buildGrowthSignalCoverageSummary,
} from "../lib/radar-buildout/growth-signals/index.js";

const validation = validateCalaGrowthProfiles();
const coverage = buildGrowthSignalCoverageSummary();

let failed = false;

for (const row of validation.results) {
  if (row.errors.length) {
    failed = true;
    console.error(
      `ERROR ${row.profile.country} / ${row.profile.submarket}:`,
      row.errors.join("; ")
    );
  }
  for (const w of row.warnings) {
    console.warn(`WARN ${row.profile.country} / ${row.profile.submarket}: ${w}`);
  }
}

for (const [country, row] of Object.entries(coverage.byCountry)) {
  if (row.submarketsMissing.length) {
    console.log(
      `Coverage gap ${country}: ${row.profileCount}/${row.submarketTotal} submarkets (${row.submarketsMissing.length} missing profiles)`
    );
  } else if (row.profileCount > 0) {
    console.log(`ok: ${country} — ${row.profileCount} profiles, ${row.signalCount} signals`);
  }
}

console.log("\nTotals:", coverage.totals);

if (failed) {
  console.error("\nValidation FAILED");
  process.exit(1);
}

console.log("\nValidation passed.");
