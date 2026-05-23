/**
 * Phase 4L — Apply Choice brand-directory property URL leads to staging candidates (gated).
 *
 * Default: dry-run. Requires --apply, --source-policy-approved,
 * INDEPENDENT_CENSUS_PIPELINE_ENABLED=true.
 */
import "../load-env.js";
import { join } from "path";
import { existsSync } from "fs";
import { CANDIDATES_TABLE } from "../lib/independent-census/fields.js";
import {
  getIndependentCensusBase,
  isIndependentCensusPipelineEnabled,
} from "../lib/independent-census/platform-base.js";
import {
  applyBrandDirectoryPropertyCandidates,
  parseIncludeMatchActions,
  selectPropertiesForApply,
  loadMatchReport,
  filterDuplicatesBeforeWrite,
  loadExistingBrandDirectoryDedupeIndex,
  APPLY_CSV_COLUMNS,
  selectedRowToCsv,
} from "../lib/independent-census/brand-directory-property-apply.js";
import {
  loadPropertyUrlExtractReport,
  filterChoicePropertiesForMatch,
} from "../lib/independent-census/match-brand-directory-properties.js";
import { createCandidateRecords } from "../lib/independent-census/candidate-apply.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let propertyUrlReport =
    "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";
  let matchReport =
    "reports/independent-census-choice-property-match-cala-2026-05-20.json";
  let parentCompany = "Choice Hotels International";
  let batchId = "choice-cala-property-url-candidates-2026-05-20";
  let includeActions = "";
  let apply = false;
  let sourcePolicyApproved = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--property-url-report" && argv[i + 1])
      propertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-url-report="))
      propertyUrlReport = a.slice("--property-url-report=".length).replace(/^"|"$/g, "");
    else if (a === "--match-report" && argv[i + 1])
      matchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--match-report="))
      matchReport = a.slice("--match-report=".length).replace(/^"|"$/g, "");
    else if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--include-actions" && argv[i + 1])
      includeActions = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--include-actions="))
      includeActions = a.slice("--include-actions=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") apply = true;
    else if (a === "--source-policy-approved") sourcePolicyApproved = true;
  }

  return {
    propertyUrlReportPath: join(process.cwd(), propertyUrlReport),
    matchReportPath: matchReport ? join(process.cwd(), matchReport) : "",
    parentCompany,
    batchId,
    includeMatchActions: parseIncludeMatchActions(includeActions),
    apply,
    sourcePolicyApproved,
    jsonPath: join(
      REPORTS_DIR,
      `independent-census-brand-directory-property-apply-${batchId}.json`
    ),
    csvPath: join(
      REPORTS_DIR,
      `independent-census-brand-directory-property-apply-${batchId}.csv`
    ),
  };
}

async function main() {
  const args = parseArgs();

  if (!existsSync(args.propertyUrlReportPath)) {
    throw new Error(`Property URL report not found: ${args.propertyUrlReportPath}`);
  }
  if (args.matchReportPath && !existsSync(args.matchReportPath)) {
    throw new Error(`Match report not found: ${args.matchReportPath}`);
  }

  if (args.apply && !args.sourcePolicyApproved) {
    throw new Error("Apply requires --source-policy-approved");
  }
  if (args.apply && !isIndependentCensusPipelineEnabled()) {
    throw new Error("Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true");
  }

  console.log("=== Brand-directory property candidate apply (Phase 4L) ===\n");
  console.log(`Mode:                 ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Property URL report:  ${args.propertyUrlReportPath}`);
  console.log(`Match report:         ${args.matchReportPath || "(none)"}`);
  console.log(`Parent company:       ${args.parentCompany}`);
  console.log(`Import batch ID:      ${args.batchId}`);
  console.log(
    `Include match actions: ${[...args.includeMatchActions].join(", ")}`
  );
  console.log(`Source policy approved: ${args.sourcePolicyApproved ? "yes" : "no"}\n`);

  const { rows: extractRows } = loadPropertyUrlExtractReport(args.propertyUrlReportPath);
  const eligibleExtract = filterChoicePropertiesForMatch(extractRows);
  const matchIndex = loadMatchReport(args.matchReportPath);

  const selection = selectPropertiesForApply(eligibleExtract, matchIndex, {
    includeMatchActions: args.includeMatchActions,
    requireMatchReport: Boolean(args.matchReportPath),
    parentCompany: args.parentCompany,
    importBatchId: args.batchId,
  });

  console.log(`Extract rows:           ${extractRows.length}`);
  console.log(`Eligible (CALA+ready):  ${eligibleExtract.length}`);
  console.log(`Selected for apply:     ${selection.selected.length}`);
  console.log(`Skipped CALA:           ${selection.skippedByCala.length}`);
  console.log(`Skipped extract action: ${selection.skippedByExtractAction.length}`);
  console.log(`Skipped match action:   ${selection.skippedByMatchAction.length}`);
  console.log(`Skipped parent:         ${selection.skippedByParent.length}`);
  console.log(`Skipped no match row:   ${selection.skippedNoMatchRow.length}\n`);

  let skippedDuplicate = [];
  let skippedDuplicatePre = [];
  let written = [];
  let writtenCount = 0;

  if (args.apply) {
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    console.log("Loading existing brand_directory rows for dedupe…");
    const dedupeIndex = await loadExistingBrandDirectoryDedupeIndex(base);
    console.log(
      `  Record IDs: ${dedupeIndex.recordIds.size}, URLs: ${dedupeIndex.urls.size}, keys: ${dedupeIndex.batchKeys.size}\n`
    );

    const { toWrite, skippedDuplicate: preDup } = filterDuplicatesBeforeWrite(
      selection.selected,
      dedupeIndex
    );
    skippedDuplicatePre = preDup;
    console.log(`Pre-write duplicate skip: ${preDup.length}`);
    console.log(`Writing ${toWrite.length} rows to ${CANDIDATES_TABLE}…`);

    const result = await createCandidateRecords(
      base,
      CANDIDATES_TABLE,
      toWrite,
      dedupeIndex.batchKeys
    );
    written = result.created;
    writtenCount = result.writtenCount;
    skippedDuplicate = [...preDup, ...result.skippedDuplicate];
    console.log(`  Written: ${writtenCount}`);
    console.log(`  Skipped duplicate (total): ${skippedDuplicate.length}\n`);
  } else {
    console.log("Dry-run: no Airtable writes.\n");
  }

  const csvRows = selection.selected.map((row) =>
    selectedRowToCsv(row, args.apply)
  );
  for (const dup of skippedDuplicatePre || []) {
    csvRows.push({
      ...selectedRowToCsv(dup, false),
      wouldWrite: "no",
      skipReason: dup.duplicateReason || "duplicate",
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4L-brand-directory-property-apply",
    mode: args.apply ? "apply" : "dry-run",
    batchId: args.batchId,
    parentCompany: args.parentCompany,
    propertyUrlReportPath: args.propertyUrlReportPath,
    matchReportPath: args.matchReportPath || null,
    includeMatchActions: [...args.includeMatchActions],
    sourcePolicyApproved: args.sourcePolicyApproved,
    counts: {
      extractRowsTotal: extractRows.length,
      eligibleExtractRows: eligibleExtract.length,
      selected: selection.selected.length,
      skippedByCala: selection.skippedByCala.length,
      skippedByExtractAction: selection.skippedByExtractAction.length,
      skippedByMatchAction: selection.skippedByMatchAction.length,
      skippedByParent: selection.skippedByParent.length,
      skippedNoMatchRow: selection.skippedNoMatchRow.length,
      skippedDuplicate: skippedDuplicate.length,
      written: writtenCount,
    },
    dryRun: !args.apply,
    airtableWrites: args.apply,
    tablesWritten: args.apply ? [CANDIDATES_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    evidenceTableWrites: false,
    promotionPerformed: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    writtenRecords: written,
    skippedDuplicateSample: skippedDuplicate.slice(0, 25).map((d) => ({
      propertyId: d.candidate?.sourceRecordId,
      reason: d.duplicateReason || "batch_key",
    })),
    selectedSample: selection.selected.slice(0, 5).map((s) => ({
      propertyId: s.candidate.sourceRecordId,
      rawHotelName: s.candidate.rawHotelName,
      matchRecommendedAction: s.matchRecommendedAction,
    })),
    reportFiles: { json: args.jsonPath, csv: args.csvPath },
  };

  writeJson(args.jsonPath, report);
  writeCsv(args.csvPath, csvRows, APPLY_CSV_COLUMNS);

  console.log("Report files:");
  console.log(`  ${args.jsonPath}`);
  console.log(`  ${args.csvPath}`);
  console.log(
    "\n✓ Candidates table only (if apply). Hotel Census, Brand Setup, Alias, Evidence, Verified untouched."
  );
  console.log("✓ No promotion. No property HTML. No STR/CoStar/Google API.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
