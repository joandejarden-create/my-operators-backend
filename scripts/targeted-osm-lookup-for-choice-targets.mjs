/**
 * Phase Choice-C — Targeted Overpass lookup for Choice targets missing OSM match.
 */
import "../load-env.js";
import { join } from "path";
import {
  runTargetedOsmLookupForChoice,
  TARGETED_LOOKUP_CSV_COLUMNS,
} from "../lib/independent-census/targeted-osm-lookup.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let targetMatchReport = "";
  let retentionReport = "";
  let batchId = "";
  let minMatchConfidence = "medium";
  let radiusMeters = 500;
  let limitLookups = null;
  let requestDelayMs = 400;
  let apply = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--target-match-report" && argv[i + 1])
      targetMatchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--target-match-report="))
      targetMatchReport = a.slice("--target-match-report=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-retention-report" && argv[i + 1])
      retentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      retentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--min-match-confidence" && argv[i + 1])
      minMatchConfidence = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--min-match-confidence="))
      minMatchConfidence = a.slice("--min-match-confidence=".length).toLowerCase();
    else if (a === "--radius-meters" && argv[i + 1])
      radiusMeters = parseInt(argv[++i], 10);
    else if (a.startsWith("--radius-meters="))
      radiusMeters = parseInt(a.slice("--radius-meters=".length), 10);
    else if (a === "--limit-lookups" && argv[i + 1])
      limitLookups = parseInt(argv[++i], 10);
    else if (a.startsWith("--limit-lookups="))
      limitLookups = parseInt(a.slice("--limit-lookups=".length), 10);
    else if (a === "--request-delay-ms" && argv[i + 1])
      requestDelayMs = parseInt(argv[++i], 10);
    else if (a === "--apply") apply = true;
  }

  if (!targetMatchReport) throw new Error("Required: --target-match-report");
  if (!batchId) throw new Error("Required: --batch-id");

  return {
    targetMatchReportPath: join(process.cwd(), targetMatchReport),
    retentionReportPath: retentionReport
      ? join(process.cwd(), retentionReport)
      : "",
    batchId,
    minMatchConfidence,
    radiusMeters,
    limitLookups,
    requestDelayMs,
    apply,
  };
}

function printSummary(result, jsonPath, csvPath) {
  console.log("\n--- Choice targeted OSM lookup ---");
  console.log(`Mode:                      ${result.apply ? "apply" : "dry-run"}`);
  console.log(`Targets needing lookup:    ${result.targetsNeedingLookup}`);
  console.log(`Lookups performed:         ${result.lookupsPerformed}`);
  console.log(`Would create curated:      ${result.wouldCreateCuratedCount}`);
  if (result.apply) console.log(`Written to Candidates:     ${result.writtenCount}`);
  console.log(
    `Lookup high/medium/low/none: ${result.highFromLookup} / ${result.mediumFromLookup} / ${result.lowFromLookup} / ${result.noneFromLookup}`
  );
  console.log(`Overpass errors:           ${result.overpassErrors}`);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
}

async function main() {
  const args = parseArgs();
  const result = await runTargetedOsmLookupForChoice(args);

  const base = `independent-census-choice-targeted-osm-lookup-${args.batchId}`;
  const jsonPath = join(REPORTS_DIR, `${base}.json`);
  const csvPath = join(REPORTS_DIR, `${base}.csv`);

  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "Choice-C-targeted-osm-lookup",
    ...result,
  });
  await writeCsv(csvPath, result.results, TARGETED_LOOKUP_CSV_COLUMNS);

  printSummary(result, jsonPath, csvPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
