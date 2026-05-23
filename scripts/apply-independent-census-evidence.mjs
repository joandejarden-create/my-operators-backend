/**
 * Phase 4A / 4Q / 4U — Apply validation evidence to Independent Hotel Source Evidence.
 *
 * Sources:
 *   wikidata (4A) — requires --match-report + --wikidata-report
 *   brand_directory (4Q) — requires --match-report (Choice property match JSON)
 *   choice_property_id_reconciliation (4U) — requires --reconciliation-report (Phase 4T)
 *
 * Default: dry-run. Requires --apply + INDEPENDENT_CENSUS_PIPELINE_ENABLED=true.
 * Does NOT write Verified, Hotel Census, Brand Alias, or Candidates.
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { EVIDENCE_TABLE, EVIDENCE_FIELDS } from "../lib/independent-census/fields.js";
import {
  getIndependentCensusBase,
  isIndependentCensusPipelineEnabled,
} from "../lib/independent-census/platform-base.js";
import {
  selectEvidenceMatches,
  selectBrandDirectoryEvidenceMatches,
  selectReconciliationEvidenceMatches,
  parseMatchTypesInclude,
  parsePropertyIdFilter,
  indexWikidataCandidatesByQid,
  buildEvidenceAirtableFields,
  buildBrandDirectoryEvidenceAirtableFields,
  buildCorrectedChoiceEvidenceAirtableFields,
  loadExistingEvidenceDedupeNames,
  loadExistingCorrectedChoiceSemanticKeys,
  partitionCorrectedChoiceEvidenceByDuplicate,
  correctedChoiceSemanticDedupeKey,
  createEvidenceRecords,
  EVIDENCE_CAPTURED_BY,
  BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY,
  CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY,
} from "../lib/independent-census/evidence-apply.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let evidenceSource = "wikidata";
  let matchReport = "";
  let reconciliationReport = "";
  let wikidataReport = "";
  let batchId = "";
  let minConfidence = "high";
  let includeMedium = false;
  let includeMatchTypes = "";
  let propertyIds = "";
  let apply = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--evidence-source" && argv[i + 1])
      evidenceSource = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--evidence-source="))
      evidenceSource = a.slice("--evidence-source=".length).replace(/^"|"$/g, "").toLowerCase();
    else if (a === "--match-report" && argv[i + 1])
      matchReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--match-report="))
      matchReport = a.slice("--match-report=".length).replace(/^"|"$/g, "");
    else if (a === "--reconciliation-report" && argv[i + 1])
      reconciliationReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--reconciliation-report="))
      reconciliationReport = a
        .slice("--reconciliation-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--wikidata-report" && argv[i + 1])
      wikidataReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--wikidata-report="))
      wikidataReport = a.slice("--wikidata-report=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--min-confidence" && argv[i + 1]) minConfidence = argv[++i];
    else if (a.startsWith("--min-confidence="))
      minConfidence = a.slice("--min-confidence=".length);
    else if (a === "--include-match-types" && argv[i + 1])
      includeMatchTypes = argv[++i];
    else if (a.startsWith("--include-match-types="))
      includeMatchTypes = a.slice("--include-match-types=".length);
    else if (a === "--property-ids" && argv[i + 1])
      propertyIds = argv[++i];
    else if (a.startsWith("--property-ids="))
      propertyIds = a.slice("--property-ids=".length);
    else if (a === "--include-medium") includeMedium = true;
    else if (a === "--apply") apply = true;
  }

  if (!batchId) {
    throw new Error("Required: --batch-id (evidence apply batch identifier)");
  }

  if (evidenceSource === "choice_property_id_reconciliation") {
    if (!reconciliationReport) {
      throw new Error(
        "choice_property_id_reconciliation requires: --reconciliation-report"
      );
    }
  } else if (evidenceSource === "brand_directory") {
    if (!matchReport) throw new Error("brand_directory requires: --match-report");
  } else {
    if (!matchReport) throw new Error("Required: --match-report");
    if (!wikidataReport) {
      throw new Error(
        "Wikidata source requires: --wikidata-report (or use --evidence-source brand_directory / choice_property_id_reconciliation)"
      );
    }
  }

  return {
    evidenceSource,
    matchReportPath: matchReport ? join(process.cwd(), matchReport) : null,
    reconciliationReportPath: reconciliationReport
      ? join(process.cwd(), reconciliationReport)
      : null,
    wikidataReportPath: wikidataReport ? join(process.cwd(), wikidataReport) : null,
    batchId,
    minConfidence,
    includeMedium,
    includeMatchTypes,
    propertyIds,
    apply,
  };
}

function loadJson(path, label) {
  if (!existsSync(path)) throw new Error(`${label} not found: ${path}`);
  return JSON.parse(readFileSync(path, "utf8"));
}

async function runBrandDirectoryEvidence(args) {
  const matchReport = loadJson(args.matchReportPath, "Match report");
  const { selected, skipped, minConfidence, allowedConfidence } =
    selectBrandDirectoryEvidenceMatches(matchReport, {
      minConfidence: args.minConfidence,
    });

  const evidenceRows = selected.map((m) =>
    buildBrandDirectoryEvidenceAirtableFields(m, args.batchId)
  );

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-evidence-apply-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-evidence-apply-${args.batchId}.csv`
  );

  console.log("=== Independent census evidence apply (Phase 4Q) ===\n");
  console.log(`Mode:              ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Evidence source:   brand_directory`);
  console.log(`Evidence batch:    ${args.batchId}`);
  console.log(`Match report:      ${args.matchReportPath}`);
  console.log(`Min confidence:    ${minConfidence} (allowed: ${allowedConfidence.join(", ")})`);
  console.log(`Captured by:       ${BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY}\n`);

  const matchTotal = (matchReport.matches || []).length;
  console.log(`Match rows total:           ${matchTotal}`);
  console.log(`Selected for evidence:      ${selected.length}`);
  console.log(`Skipped no candidate ID:  ${skipped.noCandidateRecordId.length}`);
  console.log(`Skipped low/none conf:      ${skipped.belowMinConfidence.length}`);
  console.log(`Skipped wrong action:       ${skipped.wrongRecommendedAction.length}`);
  console.log(`Skipped source policy:      ${skipped.wrongSourcePolicy.length}`);
  console.log(`Skipped non-CALA:           ${skipped.nonCala.length}`);

  let duplicateSkipped = [];
  let written = [];
  let writtenCount = 0;

  if (args.apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error(
        "Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true"
      );
    }
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    console.log("\nLoading existing evidence for dedupe…");
    const existingNames = await loadExistingEvidenceDedupeNames(
      base,
      EVIDENCE_TABLE,
      args.batchId,
      "4Q"
    );
    console.log(`  Existing evidence keys: ${existingNames.size}`);

    console.log("Writing to Independent Hotel Source Evidence only…");
    const result = await createEvidenceRecords(
      base,
      EVIDENCE_TABLE,
      evidenceRows,
      existingNames
    );
    written = result.created;
    writtenCount = result.writtenCount;
    duplicateSkipped = result.skippedDuplicate;
    console.log(`  Written: ${writtenCount}`);
    console.log(`  Skipped duplicate: ${duplicateSkipped.length}`);
  } else {
    console.log("\nDry-run: no Airtable API calls.");
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4Q-evidence-apply",
    evidenceSource: "brand_directory",
    mode: args.apply ? "apply" : "dry-run",
    evidenceBatchId: args.batchId,
    matchReportBatchId: matchReport.batchId || null,
    matchReportPath: args.matchReportPath,
    minConfidence,
    allowedConfidence,
    counts: {
      matchRowsTotal: matchTotal,
      selected: selected.length,
      skippedNoCandidateRecordId: skipped.noCandidateRecordId.length,
      skippedBelowMinConfidence: skipped.belowMinConfidence.length,
      skippedWrongRecommendedAction: skipped.wrongRecommendedAction.length,
      skippedWrongSourcePolicy: skipped.wrongSourcePolicy.length,
      skippedNonCala: skipped.nonCala.length,
      skippedDuplicate: duplicateSkipped.length,
      written: writtenCount,
    },
    dryRun: !args.apply,
    airtableWrites: args.apply,
    tablesWritten: args.apply ? [EVIDENCE_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    capturedBy: BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY,
    humanApprovalRequired: true,
    noAutoPromotion: true,
    writtenRecords: written,
    skippedDuplicateSample: duplicateSkipped.slice(0, 10).map((r) => ({
      name: r.name,
      choicePropertyId: r.choicePropertyId,
      matchedCandidateRecordId: r.matchedCandidateRecordId,
    })),
    selectedSample: evidenceRows.slice(0, 3).map((r) => ({
      name: r.name,
      choicePropertyId: r.choicePropertyId,
      matchedCandidateRecordId: r.matchedCandidateRecordId,
      matchConfidence: r.match.candidateMatchConfidence,
      matchScore: r.match.candidateMatchScore,
      propertyUrl: r.match.propertyUrl,
    })),
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  const csvRows = evidenceRows.map((r) => ({
    evidenceName: r.name,
    choicePropertyId: r.choicePropertyId,
    propertyUrl: r.match.propertyUrl,
    brandSetupBrand: r.match.brandSetupBrand,
    matchedCandidateRecordId: r.matchedCandidateRecordId,
    matchedCandidateName: r.match.matchedCandidateName,
    matchConfidence: r.match.candidateMatchConfidence,
    matchScore: r.match.candidateMatchScore,
    matchReason: r.match.candidateMatchReason,
    wouldWrite: args.apply ? "yes" : "dry-run",
  }));

  writeJson(jsonPath, report);
  writeCsv(csvPath, csvRows, [
    "evidenceName",
    "choicePropertyId",
    "propertyUrl",
    "brandSetupBrand",
    "matchedCandidateRecordId",
    "matchedCandidateName",
    "matchConfidence",
    "matchScore",
    "matchReason",
    "wouldWrite",
  ]);

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ No Verified promotion. No Hotel Census / Brand Setup / Brand Alias / Candidates."
  );
}

async function runWikidataEvidence(args) {
  const matchReport = loadJson(args.matchReportPath, "Match report");
  const wikidataReport = loadJson(args.wikidataReportPath, "Wikidata report");

  const { selected, skipped, minConfidence, allowed } = selectEvidenceMatches(
    matchReport,
    {
      minConfidence: args.minConfidence,
      includeMedium: args.includeMedium,
    }
  );

  const wikidataByQid = indexWikidataCandidatesByQid(wikidataReport);
  const evidenceRows = [];

  for (const match of selected) {
    const wikidata = wikidataByQid.get(match.wikidataQid) || null;
    evidenceRows.push(buildEvidenceAirtableFields(match, wikidata, args.batchId));
  }

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-evidence-apply-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-evidence-apply-${args.batchId}.csv`
  );

  console.log("=== Independent census evidence apply (Phase 4A) ===\n");
  console.log(`Mode:            ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Evidence batch:  ${args.batchId}`);
  console.log(`Match report:    ${args.matchReportPath}`);
  console.log(`Wikidata report: ${args.wikidataReportPath}`);
  console.log(`Min confidence:  ${minConfidence} (allowed: ${allowed.join(", ")})`);
  console.log(`Include medium:  ${args.includeMedium}`);
  console.log(`Captured by:     ${EVIDENCE_CAPTURED_BY}\n`);

  console.log(`Match rows total:     ${(matchReport.matches || []).length}`);
  console.log(`Selected for evidence: ${selected.length}`);
  console.log(`Skipped no staging ID: ${skipped.noStagingRecordId.length}`);
  console.log(`Skipped low confidence: ${skipped.belowMinConfidence.length}`);

  let duplicateSkipped = [];
  let written = [];
  let writtenCount = 0;

  if (args.apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error(
        "Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true"
      );
    }
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    console.log("\nLoading existing evidence for dedupe…");
    const existingNames = await loadExistingEvidenceDedupeNames(
      base,
      EVIDENCE_TABLE,
      args.batchId,
      "4A"
    );
    console.log(`  Existing evidence keys: ${existingNames.size}`);

    console.log("Writing to Independent Hotel Source Evidence only…");
    const result = await createEvidenceRecords(
      base,
      EVIDENCE_TABLE,
      evidenceRows,
      existingNames
    );
    written = result.created;
    writtenCount = result.writtenCount;
    duplicateSkipped = result.skippedDuplicate;
    console.log(`  Written: ${writtenCount}`);
    console.log(`  Skipped duplicate: ${duplicateSkipped.length}`);
  } else {
    console.log("\nDry-run: no Airtable API calls.");
  }

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4A-evidence-apply",
    evidenceSource: "wikidata",
    mode: args.apply ? "apply" : "dry-run",
    evidenceBatchId: args.batchId,
    wikidataBatchId: wikidataReport.batchId,
    stagingMatchBatchId: matchReport.wikidataBatchId,
    stagingOsmBatchId: matchReport.stagingBatchId,
    minConfidence,
    includeMedium: args.includeMedium,
    counts: {
      matchRowsTotal: (matchReport.matches || []).length,
      selected: selected.length,
      skippedNoStagingRecordId: skipped.noStagingRecordId.length,
      skippedBelowMinConfidence: skipped.belowMinConfidence.length,
      skippedDuplicate: duplicateSkipped.length,
      written: writtenCount,
      missingWikidataDetail: evidenceRows.filter((r) => !wikidataByQid.has(r.wikidataQid))
        .length,
    },
    dryRun: !args.apply,
    airtableWrites: args.apply,
    tablesWritten: args.apply ? [EVIDENCE_TABLE] : [],
    hotelCensusWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    brandAliasWrites: false,
    newWikidataCandidateWrites: false,
    strFieldsUsed: false,
    capturedBy: EVIDENCE_CAPTURED_BY,
    humanApprovalRequired: true,
    writtenRecords: written,
    skippedDuplicateSample: duplicateSkipped.slice(0, 10).map((r) => ({
      name: r.name,
      wikidataQid: r.wikidataQid,
      osmSourceRecordId: r.match.matchedStagingSourceRecordId,
    })),
    selectedSample: evidenceRows.slice(0, 3).map((r) => ({
      name: r.name,
      wikidataQid: r.wikidataQid,
      osmSourceRecordId: r.match.matchedStagingSourceRecordId,
      matchConfidence: r.match.matchConfidence,
      matchScore: r.match.matchScore,
      candidateLink: r.match.matchedStagingRecordId,
    })),
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  const csvRows = evidenceRows.map((r) => ({
    evidenceName: r.name,
    wikidataQid: r.wikidataQid,
    osmSourceRecordId: r.match.matchedStagingSourceRecordId,
    osmCandidateName: r.match.matchedStagingName,
    stagingRecordId: r.match.matchedStagingRecordId,
    matchConfidence: r.match.matchConfidence,
    matchScore: r.match.matchScore,
    matchReason: r.match.matchReason,
    wouldWrite: args.apply ? "yes" : "dry-run",
  }));

  writeJson(jsonPath, report);
  writeCsv(csvPath, csvRows, [
    "evidenceName",
    "wikidataQid",
    "osmSourceRecordId",
    "osmCandidateName",
    "stagingRecordId",
    "matchConfidence",
    "matchScore",
    "matchReason",
    "wouldWrite",
  ]);

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ No Verified promotion. No Hotel Census / Brand Alias / new Wikidata candidates."
  );
}

async function runReconciliationEvidence(args) {
  const reconciliationReport = loadJson(
    args.reconciliationReportPath,
    "Reconciliation report"
  );
  const includeMatchTypes = parseMatchTypesInclude(args.includeMatchTypes);
  const propertyIdsFilter = parsePropertyIdFilter(args.propertyIds);

  const { selected, skipped, propertyIdFilter } =
    selectReconciliationEvidenceMatches(reconciliationReport, {
      includeMatchTypes,
      propertyIdsFilter,
    });

  const evidenceRows = selected.map((r) =>
    buildCorrectedChoiceEvidenceAirtableFields(r, args.batchId)
  );

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-evidence-apply-${args.batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-evidence-apply-${args.batchId}.csv`
  );

  const phaseLabel = args.batchId.includes("all-direct")
    ? "4X-evidence-apply"
    : "4U-evidence-apply";

  console.log(`=== Independent census evidence apply (${phaseLabel}) ===\n`);
  console.log(`Mode:                 ${args.apply ? "APPLY" : "DRY-RUN"}`);
  console.log(`Evidence source:      choice_property_id_reconciliation`);
  console.log(`Evidence batch:       ${args.batchId}`);
  console.log(`Reconciliation report: ${args.reconciliationReportPath}`);
  console.log(`Include match types:  ${[...includeMatchTypes].join(", ")}`);
  console.log(
    `Property ID filter:   ${propertyIdFilter ? propertyIdFilter.join(", ") : "(all)"}`
  );
  console.log(`Captured by:          ${CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY}\n`);

  const totalRows = (reconciliationReport.reconciliationRows || []).length;
  console.log(`Reconciliation rows:        ${totalRows}`);
  console.log(`Selected for evidence:    ${selected.length}`);
  console.log(`Skipped match type:       ${skipped.wrongMatchType.length}`);
  console.log(`Skipped action:           ${skipped.wrongRecommendedAction.length}`);
  console.log(`Skipped missing fields:   ${skipped.missingRequiredFields.length}`);
  console.log(`Skipped property filter:  ${skipped.propertyIdFiltered.length}`);

  let duplicateSkipped = [];
  let written = [];
  let writtenCount = 0;
  let wouldWriteCount = evidenceRows.length;

  const runDedupeAndMaybeWrite = async (base) => {
    console.log("\nLoading existing evidence for dedupe…");
    const existingNames = await loadExistingEvidenceDedupeNames(
      base,
      EVIDENCE_TABLE,
      args.batchId,
      "4U"
    );
    const existingSemanticKeys = await loadExistingCorrectedChoiceSemanticKeys(
      base,
      EVIDENCE_TABLE
    );
    console.log(`  Existing batch name keys: ${existingNames.size}`);
    console.log(`  Existing semantic keys (4U/4X): ${existingSemanticKeys.size}`);

    const { toWrite, skippedDuplicate: skipped } =
      partitionCorrectedChoiceEvidenceByDuplicate(
        evidenceRows,
        existingNames,
        existingSemanticKeys
      );
    duplicateSkipped = skipped;
    wouldWriteCount = toWrite.length;

    if (!args.apply) {
      console.log(`\nWould write after dedupe: ${wouldWriteCount}`);
      console.log(`Would skip duplicate: ${duplicateSkipped.length}`);
      return;
    }

    console.log("Writing to Independent Hotel Source Evidence only…");
    const result = await createEvidenceRecords(
      base,
      EVIDENCE_TABLE,
      toWrite,
      existingNames
    );
    written = result.created;
    writtenCount = result.writtenCount;
    console.log(`  Written: ${writtenCount}`);
    console.log(`  Skipped duplicate: ${duplicateSkipped.length}`);
  };

  if (args.apply) {
    if (!isIndependentCensusPipelineEnabled()) {
      throw new Error(
        "Apply requires INDEPENDENT_CENSUS_PIPELINE_ENABLED=true"
      );
    }
    const base = getIndependentCensusBase();
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
    await runDedupeAndMaybeWrite(base);
  } else {
    const base = getIndependentCensusBase();
    if (base) {
      try {
        await runDedupeAndMaybeWrite(base);
      } catch (e) {
        console.log(`\nDry-run dedupe skipped: ${e.message}`);
        console.log("\nDry-run: no Airtable writes.");
      }
    } else {
      console.log("\nDry-run: no Airtable API (missing credentials).");
    }
  }

  const propertyIdsIncluded = [
    ...new Set(selected.map((r) => r.extractedChoicePropertyId).filter(Boolean)),
  ];

  const report = {
    generatedAt: new Date().toISOString(),
    phase: phaseLabel,
    evidenceSource: "choice_property_id_reconciliation",
    mode: args.apply ? "apply" : "dry-run",
    evidenceBatchId: args.batchId,
    reconciliationReportPath: args.reconciliationReportPath,
    reconciliationBatchId: reconciliationReport.batchId || null,
    includeMatchTypes: [...includeMatchTypes],
    propertyIdsFilter: propertyIdFilter,
    counts: {
      reconciliationRowsTotal: totalRows,
      selected: selected.length,
      skippedWrongMatchType: skipped.wrongMatchType.length,
      skippedWrongRecommendedAction: skipped.wrongRecommendedAction.length,
      skippedMissingRequiredFields: skipped.missingRequiredFields.length,
      skippedPropertyIdFiltered: skipped.propertyIdFiltered.length,
      skippedDuplicate: duplicateSkipped.length,
      wouldWriteAfterDedupe: wouldWriteCount,
      written: writtenCount,
    },
    propertyIdsIncluded,
    propertyIdsWritten: written.map((r) => r.choicePropertyId).filter(Boolean),
    dryRun: !args.apply,
    airtableWrites: args.apply,
    tablesWritten: args.apply ? [EVIDENCE_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    brandAliasWrites: false,
    priorEvidenceModified: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    capturedBy: CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY,
    humanApprovalRequired: true,
    noAutoPromotion: true,
    writtenRecords: written,
    skippedDuplicateSample: duplicateSkipped.slice(0, 10).map((r) => ({
      name: r.name,
      choicePropertyId: r.choicePropertyId,
      matchedCandidateRecordId: r.matchedCandidateRecordId,
      duplicateReason: r.duplicateReason,
    })),
    selectedSample: evidenceRows.slice(0, 5).map((r) => ({
      name: r.name,
      choicePropertyId: r.choicePropertyId,
      matchedCandidateRecordId: r.matchedCandidateRecordId,
      matchType: r.row.matchType,
      reconciliationConfidence: r.row.reconciliationConfidence,
      propertyUrl: r.row.matchedChoicePropertyUrl,
    })),
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  const skippedSemanticKeys = new Set(
    duplicateSkipped.map((r) =>
      correctedChoiceSemanticDedupeKey(
        r.choicePropertyId,
        r.matchedCandidateRecordId
      )
    )
  );
  const csvRows = evidenceRows.map((r) => {
    const semantic = correctedChoiceSemanticDedupeKey(
      r.choicePropertyId,
      r.matchedCandidateRecordId
    );
    const isDup = skippedSemanticKeys.has(semantic);
    return {
      evidenceName: r.name,
      choicePropertyId: r.choicePropertyId,
      osmCandidateRecordId: r.matchedCandidateRecordId,
      osmCandidateName: r.row.osmCandidateName,
      matchType: r.row.matchType,
      reconciliationConfidence: r.row.reconciliationConfidence,
      matchScore: r.fields[EVIDENCE_FIELDS.matchScore],
      propertyUrl: r.row.matchedChoicePropertyUrl,
      wouldWrite: isDup
        ? "skip-duplicate"
        : args.apply
          ? "yes"
          : "dry-run",
    };
  });

  writeJson(jsonPath, report);
  writeCsv(csvPath, csvRows, [
    "evidenceName",
    "choicePropertyId",
    "osmCandidateRecordId",
    "osmCandidateName",
    "matchType",
    "reconciliationConfidence",
    "matchScore",
    "propertyUrl",
    "wouldWrite",
  ]);

  console.log("\nProperty IDs included:");
  propertyIdsIncluded.forEach((id) => console.log(`  - ${id}`));

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ Corrected evidence only. Prior 4Q rows untouched. No Verified / Candidates / Hotel Census writes."
  );
}

async function main() {
  const args = parseArgs();
  if (args.evidenceSource === "choice_property_id_reconciliation") {
    await runReconciliationEvidence(args);
  } else if (args.evidenceSource === "brand_directory") {
    await runBrandDirectoryEvidence(args);
  } else {
    await runWikidataEvidence(args);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
