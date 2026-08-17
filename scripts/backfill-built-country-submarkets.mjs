#!/usr/bin/env node
/**
 * Backfill Submarket from Notes for all completed radar market countries.
 *
 *   node scripts/backfill-built-country-submarkets.mjs
 *   node scripts/backfill-built-country-submarkets.mjs --apply
 */
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { BUILT_RADAR_COUNTRIES } from "../lib/radar-submarket-registry.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const TABLE_ARG = (() => {
  const idx = process.argv.indexOf("--table");
  if (idx < 0) return "all";
  return process.argv[idx + 1];
})();

const totals = {
  countries: 0,
  demandUpdated: 0,
  demandFailed: 0,
  travelUpdated: 0,
  travelFailed: 0,
};

for (const country of BUILT_RADAR_COUNTRIES) {
  console.log(`\n######## ${country} ########`);
  const args = [
    join(root, "scripts/backfill-country-submarkets-from-notes.mjs"),
    "--country",
    country,
    "--table",
    TABLE_ARG,
  ];
  if (APPLY) args.push("--apply");

  const result = spawnSync(process.execPath, args, {
    cwd: root,
    stdio: "inherit",
    env: process.env,
  });
  totals.countries += 1;
  if (result.status !== 0) {
    console.error(`Backfill failed for ${country} (exit ${result.status})`);
    process.exit(result.status || 1);
  }
}

console.log("\n=== Built-country submarket backfill complete ===");
console.log("Countries processed:", totals.countries);
if (!APPLY) {
  console.log("Dry run only — re-run with --apply to write Submarket values to Airtable.");
}
