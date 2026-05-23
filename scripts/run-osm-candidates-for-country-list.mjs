/**
 * Phase 4M — OSM hotel-focused expansion for Choice CALA countries.
 *
 * Default: dry-run per country + combined summary.
 * Apply: --apply + INDEPENDENT_CENSUS_PIPELINE_ENABLED=true
 */
import "../load-env.js";
import { join } from "path";
import {
  parseCountryList,
  runOsmCountryList,
} from "../lib/independent-census/osm-country-list-runner.js";
import {
  isIndependentCensusPipelineEnabled,
} from "../lib/independent-census/platform-base.js";
import { DEFAULT_MAX_ELEMENTS } from "../lib/independent-census/sources/osm.js";

const REPORTS_DIR = join(process.cwd(), "reports");

const DEFAULT_COUNTRIES =
  "Colombia,Mexico,Chile,Dominican Republic,Costa Rica,Panama,Trinidad and Tobago,Ecuador,Argentina,Bahamas,Honduras,Puerto Rico,Peru,Brazil";

function parseArgs() {
  let countries = DEFAULT_COUNTRIES;
  let batchId = "choice-cala-osm-expansion-2026-05-20";
  let apply = false;
  let maxElements = DEFAULT_MAX_ELEMENTS;
  let minQualityTier = "medium";
  let includeApartments = false;
  let includeUnnamed = false;
  let delayMs = 8000;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--countries" && argv[i + 1])
      countries = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--countries="))
      countries = a.slice("--countries=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") apply = true;
    else if (a === "--include-apartments") includeApartments = true;
    else if (a === "--include-unnamed") includeUnnamed = true;
    else if (a === "--min-quality" && argv[i + 1])
      minQualityTier = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--min-quality="))
      minQualityTier = a.slice("--min-quality=".length).replace(/^"|"$/g, "");
    else if (a === "--max-elements" && argv[i + 1])
      maxElements = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-elements="))
      maxElements = parseInt(a.slice("--max-elements=".length), 10);
    else if (a === "--delay-ms" && argv[i + 1])
      delayMs = parseInt(argv[++i], 10);
    else if (a.startsWith("--delay-ms="))
      delayMs = parseInt(a.slice("--delay-ms=".length), 10);
    else if (a === "--uncapped-overpass") maxElements = null;
  }

  return {
    countries: parseCountryList(countries),
    summaryBatchId: batchId,
    runSuffix: "choice-cala-2026-05-20",
    apply,
    maxElements,
    minQualityTier,
    includeApartments,
    includeUnnamed,
    delayBetweenCountriesMs: delayMs,
  };
}

async function main() {
  const args = parseArgs();

  if (args.apply && !isIndependentCensusPipelineEnabled()) {
    throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
  }

  console.log("=== OSM country-list expansion (Phase 4M) ===\n");
  console.log(`Mode:           ${args.apply ? "DRY-RUN + APPLY" : "DRY-RUN"}`);
  console.log(`Countries:      ${args.countries.length}`);
  console.log(`Summary batch:  ${args.summaryBatchId}`);
  console.log(`Hotel-focused:  yes`);
  console.log(`Min quality:    ${args.minQualityTier}`);
  console.log(`Max elements:   ${args.maxElements ?? "uncapped"}`);
  console.log(`Delay (ms):     ${args.delayBetweenCountriesMs}\n`);

  const { summary, summaryJsonPath, summaryCsvPath, countryResults } =
    await runOsmCountryList(args.countries, {
      reportsDir: REPORTS_DIR,
      summaryBatchId: args.summaryBatchId,
      runSuffix: args.runSuffix,
      apply: args.apply,
      hotelFocused: true,
      includeApartments: args.includeApartments,
      includeUnnamed: args.includeUnnamed,
      minQualityTier: args.minQualityTier,
      maxElements: args.maxElements,
      delayBetweenCountriesMs: args.delayBetweenCountriesMs,
    });

  console.log("--- Per country (dry-run candidates) ---");
  for (const cr of countryResults) {
    const line = `  ${cr.country}: ${cr.candidateCount ?? 0}${cr.error ? ` ERROR: ${cr.error}` : ""}`;
    console.log(line);
    if (cr.jsonPath) console.log(`    ${cr.jsonPath}`);
  }

  console.log(`\nTotal candidates (dry-run): ${summary.totalCandidatesDryRun}`);

  if (args.apply) {
    console.log(`Total written:            ${summary.totalWritten}`);
    console.log(`Total skipped duplicate:  ${summary.totalSkippedDuplicate}`);
  }

  if (summary.errors?.length) {
    console.log("\nErrors:");
    summary.errors.forEach((e) => console.log(`  ${e.country}: ${e.error}`));
  }

  console.log("\nCombined summary:");
  console.log(`  ${summaryJsonPath}`);
  console.log(`  ${summaryCsvPath}`);
  console.log(
    "\n✓ Candidates table only when --apply. Hotel Census, Brand Setup, Alias, Evidence, Verified untouched."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
