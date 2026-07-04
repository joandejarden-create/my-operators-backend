/**
 * Apply curated Choice targeted OSM candidates from completed lookup report.
 */
import "../load-env.js";
import { join } from "path";
import { applyCuratedCandidatesFromLookupReport } from "../lib/independent-census/targeted-osm-lookup.js";
import { isIndependentCensusPipelineEnabled } from "../lib/independent-census/platform-base.js";

function parseArgs() {
  let lookupReport =
    "reports/independent-census-choice-targeted-osm-lookup-choice-targeted-osm-lookup-2026-05-20.json";
  let targetList =
    "reports/independent-census-choice-target-list-choice-targets-2026-05-20.json";
  let batchId = "choice-targeted-osm-lookup-apply-2026-05-20";
  let retentionReport =
    "reports/independent-census-candidate-coverage-dedupe-2026-05-20.json";
  let apply = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--lookup-report" && argv[i + 1])
      lookupReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--lookup-report="))
      lookupReport = a.slice("--lookup-report=".length).replace(/^"|"$/g, "");
    else if (a === "--target-list" && argv[i + 1])
      targetList = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--target-list="))
      targetList = a.slice("--target-list=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-retention-report" && argv[i + 1])
      retentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      retentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--apply") apply = true;
    else if (a === "--dry-run") apply = false;
  }

  if (apply && !isIndependentCensusPipelineEnabled()) {
    throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
  }

  return {
    lookupReportPath: join(process.cwd(), lookupReport),
    targetListPath: join(process.cwd(), targetList),
    retentionReportPath: join(process.cwd(), retentionReport),
    batchId,
    apply,
  };
}

async function main() {
  const args = parseArgs();
  const result = await applyCuratedCandidatesFromLookupReport(args);

  console.log("\n--- Choice curated candidate apply ---");
  console.log(`Mode:                    ${result.apply ? "apply" : "dry-run"}`);
  console.log(`Curated selected:        ${result.curatedSelected}`);
  console.log(`Prepared for write:      ${result.preparedForWrite}`);
  console.log(`Written:                 ${result.writtenCount}`);
  console.log(`Duplicates skipped:      ${result.skippedDuplicateApply}`);
  console.log(`Existing OSM skipped:    ${result.skippedExistingOsmSourceId ?? 0}`);
  console.log(`Fetch failed:            ${result.fetchFailed}`);
  console.log(`Missing core fields:     ${result.missingCoreFields}`);
  console.log("By country:", result.byCountry);
  console.log("By brand:", result.byBrand);
  console.log("\nSafety:");
  console.log(`  Tables written:        ${(result.tablesWritten || []).join(", ") || "none"}`);
  console.log(`  Hotel Census writes:   ${result.hotelCensusWrites}`);
  console.log(`  Google API:            ${result.googleApiUsed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
