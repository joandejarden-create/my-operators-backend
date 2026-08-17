#!/usr/bin/env node
/**
 * Verify demand anchor candidates against Google Places (pre-import QA only).
 * Google metadata stays in the report file — never in Airtable-ready fixtures.
 */
import "../load-env.js";
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  parseGoogleVerificationCli,
  resolveGoogleApiKey,
  printMissingApiKeyInstructions,
} from "../lib/location-verification/google-api-config.js";
import {
  runGooglePlacesVerification,
  printVerificationPreflight,
} from "../lib/location-verification/run-google-places-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const cli = parseGoogleVerificationCli();

if (!cli.file) {
  console.error(
    "Usage: node scripts/verify-demand-anchors-google.mjs --file <fixture.json> --country <name> " +
      "[--city <name>] [--output <report.json>] [--verified-output <clean.json>] " +
      "[--allow-medium] [--limit N] [--max-requests N] [--delay-ms N] [--max-results N] " +
      "[--verbose] [--dry-run] [--force-refresh] [--cache|--no-cache]"
  );
  process.exit(1);
}

const payload = JSON.parse(readFileSync(join(root, cli.file), "utf8"));
const country = cli.country || payload.country || "";
const output =
  cli.output ||
  `fixtures/demand-anchors-${String(country || "market")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")}-google-verification-report.json`;
const verifiedOutput =
  cli.verifiedOutput ||
  `fixtures/demand-anchors-${String(country || "market")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")}-verified.json`;

const apiKey = resolveGoogleApiKey();
if (!apiKey) {
  printMissingApiKeyInstructions();
  process.exit(0);
}

const options = {
  ...cli,
  country,
  file: cli.file,
  inputFile: cli.file,
  output,
  verifiedOutput,
};

const result = await runGooglePlacesVerification({ payload, options });

if (result.preflight) printVerificationPreflight(result.preflight);

if (!result.ok) {
  if (result.error === "missing_api_key") {
    printMissingApiKeyInstructions();
    process.exit(0);
  }
  console.error(result.message || result.error);
  process.exit(1);
}

if (result.dryRun) {
  console.log("Dry run complete — no API calls made and no files written.");
  process.exit(0);
}

writeFileSync(join(root, output), JSON.stringify(result.report, null, 2) + "\n");
writeFileSync(join(root, verifiedOutput), JSON.stringify(result.cleanFixture, null, 2) + "\n");

if (verifiedOutput.startsWith("fixtures/")) {
  const publicPath = verifiedOutput.replace(/^fixtures\//, "public/fixtures/");
  writeFileSync(join(root, publicPath), JSON.stringify(result.cleanFixture, null, 2) + "\n");
}

const s = result.summary;
console.log("Verification report:", output);
console.log("Clean verified fixture:", verifiedOutput);
console.log(
  "Verified:",
  s.verifiedRecords,
  "Excluded:",
  s.excludedRecords,
  "Total:",
  s.candidateCount,
  "| API requests:",
  s.apiRequestsMade,
  "| Cache hits:",
  s.cacheHits,
  "| Cache misses:",
  s.cacheMisses
);
