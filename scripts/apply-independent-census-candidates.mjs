/**
 * Phase 3A — Apply OSM (and future) candidates to Independent Hotel Source Candidates.
 *
 * Default: dry-run. Requires --apply + INDEPENDENT_CENSUS_PIPELINE_ENABLED=true to write.
 * Does NOT write to Hotel Census, Verified table, Evidence, or Brand Alias Mapping.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join, basename } from "path";
import {
  CANDIDATES_TABLE,
  SOURCE_TYPES,
} from "../lib/independent-census/fields.js";
import {
  getIndependentCensusBase,
  isIndependentCensusPipelineEnabled,
} from "../lib/independent-census/platform-base.js";
import {
  indexMatchReport,
  parseExcludeActions,
  parseMinQualityTier,
  selectCandidatesForApply,
  loadExistingCandidateKeys,
  createCandidateRecords,
} from "../lib/independent-census/candidate-apply.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let input = "";
  let matchReport = "";
  let batchId = "";
  let minQuality = "";
  let excludeActions = "";
  let sourceType = SOURCE_TYPES.OSM;
  let apply = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1]) input = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--input=")) input = a.slice("--input=".length).replace(/^"|"$/g, "");
    else if (a === "--match-report" && argv[i + 1])
      matchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--match-report="))
      matchReport = a.slice("--match-report=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--min-quality" && argv[i + 1]) minQuality = argv[++i];
    else if (a.startsWith("--min-quality=")) minQuality = a.slice("--min-quality=".length);
    else if (a === "--exclude-actions" && argv[i + 1])
      excludeActions = argv[++i];
    else if (a.startsWith("--exclude-actions="))
      excludeActions = a.slice("--exclude-actions=".length);
    else if (a === "--source-type" && argv[i + 1]) sourceType = argv[++i];
    else if (a.startsWith("--source-type=")) sourceType = a.slice("--source-type=".length);
    else if (a === "--apply") apply = true;
  }

  if (!input) {
    throw new Error("Missing --input (OSM dry-run JSON report path)");
  }

  const inputPath = join(process.cwd(), input);
  const matchReportPath = matchReport ? join(process.cwd(), matchReport) : "";

  return { inputPath, matchReportPath, batchId, minQuality, excludeActions, sourceType, apply };
}

function loadOsmReport(inputPath) {
  if (!existsSync(inputPath)) throw new Error(`Input not found: ${inputPath}`);
  const data = JSON.parse(readFileSync(inputPath, "utf8"));
  if (!Array.isArray(data.candidates)) {
    throw new Error("Input must be OSM dry-run JSON with candidates array");
  }
  const inferredBatch =
    data.batchId ||
    basename(inputPath, ".json").replace(/^independent-census-osm-dry-run-/, "");
  return { data, candidates: data.candidates, inferredBatch };
}

function loadMatchReport(path) {
  if (!path || !existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

async function main() {
  const args = parseArgs();
  const { inputPath, matchReportPath, apply } = args;
  const minQualityTier = parseMinQualityTier(args.minQuality);
  const excludeActions = parseExcludeActions(args.excludeActions);

  const { data: osmReport, candidates, inferredBatch } = loadOsmReport(inputPath);
  const batchId = args.batchId || inferredBatch;
  const matchReport = loadMatchReport(matchReportPath);
  const matchBySourceId = indexMatchReport(matchReport);

  if (normalizeSourceType(args.sourceType) !== SOURCE_TYPES.OSM) {
    throw new Error(`Phase 3A v1 supports --source-type osm only (got ${args.sourceType})`);
  }

  const selection = selectCandidatesForApply(candidates, {
    minQualityTier,
    excludeActions,
    sourceType: args.sourceType,
    matchBySourceId,
  });

  const reportBase = `independent-census-candidate-apply-${batchId}`;
  const jsonPath = join(REPORTS_DIR, `${reportBase}.json`);
  const csvPath = join(REPORTS_DIR, `${reportBase}.csv`);

  console.log("=== Independent census candidate apply (Phase 3A) ===\n");
  console.log(`Mode:              ${apply ? "APPLY (Airtable writes)" : "DRY-RUN"}`);
  console.log(`Input:             ${inputPath}`);
  console.log(`Match report:      ${matchReportPath || "(none)"}`);
  console.log(`Batch ID:          ${batchId}`);
  console.log(`Source type:       ${args.sourceType}`);
  console.log(`Min quality:       ${minQualityTier || "(none)"}`);
  console.log(`Exclude actions:   ${excludeActions.size ? [...excludeActions].join(", ") : "(none)"}`);
  console.log(`Input candidates:  ${candidates.length}`);
  console.log(`Selected:          ${selection.selected.length}`);
  console.log(`Skipped quality:   ${selection.skippedByQuality.length}`);
  console.log(`Skipped action:    ${selection.skippedByAction.length}`);
  console.log(`Skipped source:    ${selection.skippedBySourceType.length}\n`);

  let duplicateSkipped = [];
  let written = [];
  let writtenCount = 0;

  if (apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error(
        "Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true in environment"
      );
    }
    const base = getIndependentCensusBase();
    if (!base) {
      throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
    }

    console.log("Loading existing staging rows for duplicate check…");
    const existingKeys = await loadExistingCandidateKeys(base, batchId, CANDIDATES_TABLE);
    console.log(`  Existing keys in batch: ${existingKeys.size}\n`);

    console.log("Writing to Independent Hotel Source Candidates only…");
    const result = await createCandidateRecords(
      base,
      CANDIDATES_TABLE,
      selection.selected,
      existingKeys
    );
    written = result.created;
    writtenCount = result.writtenCount;
    duplicateSkipped = result.skippedDuplicate;
    console.log(`  Written: ${writtenCount}`);
    console.log(`  Skipped duplicate: ${duplicateSkipped.length}\n`);
  } else {
    console.log("Dry-run: no Airtable API calls.\n");
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "3A-candidate-apply",
    mode: apply ? "apply" : "dry-run",
    batchId,
    inputReport: inputPath,
    matchReport: matchReportPath || null,
    filters: {
      minQuality: minQualityTier,
      excludeActions: [...excludeActions],
      sourceType: args.sourceType,
    },
    counts: {
      inputCandidates: candidates.length,
      selected: selection.selected.length,
      skippedByQuality: selection.skippedByQuality.length,
      skippedByAction: selection.skippedByAction.length,
      skippedBySourceType: selection.skippedBySourceType.length,
      skippedDuplicate: duplicateSkipped.length,
      written: writtenCount,
    },
    dryRun: !apply,
    airtableWrites: apply,
    tablesWritten: apply ? [CANDIDATES_TABLE] : [],
    hotelCensusWrites: false,
    verifiedTableWrites: false,
    evidenceTableWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    stagingTableWrites: apply,
    writtenRecords: written,
    skippedDuplicateSample: duplicateSkipped.slice(0, 20).map((d) => ({
      sourceRecordId: d.candidate.sourceRecordId,
      key: d.key,
    })),
    selectedSample: selection.selected.slice(0, 5).map(({ candidate, matchRow }) => ({
      sourceRecordId: candidate.sourceRecordId,
      rawHotelName: candidate.rawHotelName,
      qualityTier: candidate.qualityTier,
      matchConfidence: matchRow?.matchConfidence,
      matchRecommendedAction: matchRow?.recommendedAction,
    })),
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  const csvRows = selection.selected.map(({ candidate, matchRow }) => ({
    sourceRecordId: candidate.sourceRecordId,
    rawHotelName: candidate.rawHotelName,
    qualityTier: candidate.qualityTier,
    qualityScore: candidate.qualityScore,
    matchConfidence: matchRow?.matchConfidence || "",
    matchRecommendedAction: matchRow?.recommendedAction || "",
    wouldWrite: apply ? "yes" : "dry-run",
  }));

  writeJson(jsonPath, report);
  writeCsv(csvPath, csvRows, [
    "sourceRecordId",
    "rawHotelName",
    "qualityTier",
    "qualityScore",
    "matchConfidence",
    "matchRecommendedAction",
    "wouldWrite",
  ]);

  console.log("Report files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log("\n✓ Hotel Census untouched. Verified / Evidence / Brand Alias untouched.");
}

function normalizeSourceType(s) {
  return String(s ?? "").trim().toLowerCase();
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
