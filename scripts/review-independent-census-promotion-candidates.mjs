/**
 * Phase 4B / 4R / 4V — Human promotion review report (READ-ONLY).
 *
 * Wikidata (4B): Phase 4A evidence + linked OSM candidates.
 * Brand directory (4R): Phase 4Q Choice evidence + linked OSM candidates.
 * choice_property_id_reconciliation (4V/4Y): Phase 4U/4X corrected evidence only (ignores 4Q).
 *
 * No Verified promotion. No Hotel Census. No writes to staging tables.
 */
import "../load-env.js";
import { join } from "path";
import { loadEvidenceByBatch, loadCandidatesByIds } from "../lib/independent-census/promotion-review-load.js";
import {
  buildPromotionReviewRows,
  buildBrandDirectoryPromotionReviewRows,
  buildCorrectedChoicePromotionReviewRows,
  correctedChoicePromotionRowToCsv,
  CORRECTED_CHOICE_PROMOTION_CSV_COLUMNS,
  summarizePromotionReview,
} from "../lib/independent-census/promotion-review.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let evidenceSource = "wikidata";
  let evidenceBatchId = "osm-wikidata-dr-high-confidence-2026-05-20";
  let candidateBatchId = "osm-dominican-republic-hotel-focused-2026-05-20";
  let candidateSourceType = "";
  let batchId = "";
  let reportSlug = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--evidence-source" && argv[i + 1])
      evidenceSource = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--evidence-source="))
      evidenceSource = a.slice("--evidence-source=".length).replace(/^"|"$/g, "").toLowerCase();
    else if (a === "--evidence-batch-id" && argv[i + 1])
      evidenceBatchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--evidence-batch-id="))
      evidenceBatchId = a.slice("--evidence-batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-batch-id" && argv[i + 1])
      candidateBatchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-batch-id="))
      candidateBatchId = a.slice("--candidate-batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-source-type" && argv[i + 1])
      candidateSourceType = argv[++i].replace(/^"|"$/g, "").toLowerCase();
    else if (a.startsWith("--candidate-source-type="))
      candidateSourceType = a
        .slice("--candidate-source-type=".length)
        .replace(/^"|"$/g, "")
        .toLowerCase();
    else if (a === "--batch-id" && argv[i + 1])
      batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--report-slug" && argv[i + 1])
      reportSlug = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--report-slug="))
      reportSlug = a.slice("--report-slug=".length).replace(/^"|"$/g, "");
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Promotion review is report-only.");
    }
  }

  if (evidenceSource === "choice_property_id_reconciliation") {
    if (!evidenceBatchId) {
      evidenceBatchId = "choice-property-id-corrected-evidence-2026-05-20";
    }
    candidateSourceType = candidateSourceType || "osm";
    candidateBatchId = "";
    if (!reportSlug && batchId) {
      reportSlug = batchId
        .replace(/-promotion-review-/i, "-")
        .replace(/-promotion-review$/i, "");
    }
    reportSlug = reportSlug || "choice-property-id-corrected-2026-05-20";
  } else if (evidenceSource === "brand_directory") {
    if (!evidenceBatchId) {
      evidenceBatchId = "choice-brand-directory-evidence-2026-05-20";
    }
    candidateSourceType = candidateSourceType || "osm";
    candidateBatchId = "";
    if (!reportSlug) {
      reportSlug = batchId
        ? batchId.replace(/-promotion-review/i, "")
        : "choice-brand-directory-2026-05-20";
    }
  } else if (!reportSlug) {
    reportSlug = evidenceBatchId;
  }

  return {
    evidenceSource,
    evidenceBatchId,
    candidateBatchId,
    candidateSourceType,
    reviewBatchId: batchId || reportSlug,
    reportSlug,
  };
}

const WIKIDATA_CSV_COLUMNS = [
  "candidateAirtableRecordId",
  "sourceRecordId",
  "osmSourceUrl",
  "wikidataQid",
  "wikidataUrl",
  "candidateHotelName",
  "wikidataHotelName",
  "candidateCountry",
  "candidateCity",
  "candidateLatitude",
  "candidateLongitude",
  "candidateWebsite",
  "candidatePhone",
  "candidateBrand",
  "wikidataWebsite",
  "wikidataOperator",
  "wikidataOwner",
  "wikidataWikipediaUrl",
  "matchScore",
  "matchReason",
  "evidenceCount",
  "sourceCount",
  "promotionEligibility",
  "promotionRecommendation",
  "reviewRiskLevel",
  "humanReviewNotes",
  "proposedDealalityHotelId",
  "proposedVerifiedHotelName",
  "proposedVerificationStatus",
];

const BRAND_DIRECTORY_CSV_COLUMNS = [
  "candidateAirtableRecordId",
  "sourceRecordId",
  "osmSourceUrl",
  "choicePropertyUrl",
  "choicePropertyId",
  "candidateHotelName",
  "choiceBrandSetupBrand",
  "parentCompany",
  "candidateCountry",
  "candidateCity",
  "candidateLatitude",
  "candidateLongitude",
  "candidateWebsite",
  "candidatePhone",
  "candidateBrand",
  "candidateMatchConfidence",
  "matchScore",
  "matchReason",
  "evidenceCount",
  "sourceCount",
  "promotionEligibility",
  "promotionRecommendation",
  "reviewRiskLevel",
  "humanReviewNotes",
  "proposedDealalityHotelId",
  "proposedVerifiedHotelName",
  "proposedVerifiedBrandLabel",
  "proposedPrimarySourceUrl",
  "proposedVerificationStatus",
];

function wikidataRowToCsv(r) {
  const p = r.proposedVerified || {};
  return {
    candidateAirtableRecordId: r.candidateAirtableRecordId,
    sourceRecordId: r.sourceRecordId,
    osmSourceUrl: r.osmSourceUrl,
    wikidataQid: r.wikidataQid,
    wikidataUrl: r.wikidataUrl,
    candidateHotelName: r.candidateHotelName,
    wikidataHotelName: r.wikidataHotelName,
    candidateCountry: r.candidateCountry,
    candidateCity: r.candidateCity,
    candidateLatitude: r.candidateLatitude ?? "",
    candidateLongitude: r.candidateLongitude ?? "",
    candidateWebsite: r.candidateWebsite,
    candidatePhone: r.candidatePhone,
    candidateBrand: r.candidateBrand,
    wikidataWebsite: r.wikidataWebsite,
    wikidataOperator: r.wikidataOperator,
    wikidataOwner: r.wikidataOwner,
    wikidataWikipediaUrl: r.wikidataWikipediaUrl,
    matchScore: r.matchScore,
    matchReason: r.matchReason,
    evidenceCount: r.evidenceCount,
    sourceCount: r.sourceCount,
    promotionEligibility: r.promotionEligibility,
    promotionRecommendation: r.promotionRecommendation,
    reviewRiskLevel: r.reviewRiskLevel,
    humanReviewNotes: r.humanReviewNotes,
    proposedDealalityHotelId: p.dealalityHotelId,
    proposedVerifiedHotelName: p.verifiedHotelName,
    proposedVerificationStatus: p.verificationStatus,
  };
}

function brandDirectoryRowToCsv(r) {
  const p = r.proposedVerified || {};
  return {
    candidateAirtableRecordId: r.candidateAirtableRecordId,
    sourceRecordId: r.sourceRecordId,
    osmSourceUrl: r.osmSourceUrl,
    choicePropertyUrl: r.choicePropertyUrl,
    choicePropertyId: r.choicePropertyId,
    candidateHotelName: r.candidateHotelName,
    choiceBrandSetupBrand: r.choiceBrandSetupBrand,
    parentCompany: r.parentCompany,
    candidateCountry: r.candidateCountry,
    candidateCity: r.candidateCity,
    candidateLatitude: r.candidateLatitude ?? "",
    candidateLongitude: r.candidateLongitude ?? "",
    candidateWebsite: r.candidateWebsite,
    candidatePhone: r.candidatePhone,
    candidateBrand: r.candidateBrand,
    candidateMatchConfidence: r.candidateMatchConfidence,
    matchScore: r.matchScore,
    matchReason: r.matchReason,
    evidenceCount: r.evidenceCount,
    sourceCount: r.sourceCount,
    promotionEligibility: r.promotionEligibility,
    promotionRecommendation: r.promotionRecommendation,
    reviewRiskLevel: r.reviewRiskLevel,
    humanReviewNotes: r.humanReviewNotes,
    proposedDealalityHotelId: p.dealalityHotelId,
    proposedVerifiedHotelName: p.verifiedHotelName,
    proposedVerifiedBrandLabel: p.verifiedBrandLabel,
    proposedPrimarySourceUrl: p.primarySourceUrl,
    proposedVerificationStatus: p.verificationStatus,
  };
}

async function runBrandDirectoryReview(args) {
  const reportBase = `independent-census-promotion-review-${args.reportSlug}`;
  const jsonPath = join(REPORTS_DIR, `${reportBase}.json`);
  const csvPath = join(REPORTS_DIR, `${reportBase}.csv`);

  console.log("=== Independent census promotion review (Phase 4R, read-only) ===\n");
  console.log(`Evidence source:    brand_directory`);
  console.log(`Evidence batch:     ${args.evidenceBatchId}`);
  console.log(`Candidate filter:   source type = ${args.candidateSourceType}`);
  console.log(`Review batch ID:    ${args.reviewBatchId}`);
  console.log("No Verified promotion. No Hotel Census reads/writes.\n");

  console.log("Loading Independent Hotel Source Evidence…");
  const evidenceRecords = await loadEvidenceByBatch(args.evidenceBatchId, {
    dedupePrefix: "4Q",
  });
  console.log(`  Evidence rows reviewed: ${evidenceRecords.length}`);

  const candidateIds = evidenceRecords.flatMap((e) => e.candidateLinkIds);
  console.log("Loading linked Independent Hotel Source Candidates…");
  const candidateById = await loadCandidatesByIds(candidateIds, {
    candidateSourceType: args.candidateSourceType,
  });
  console.log(`  Linked candidates found: ${candidateById.size}`);

  const reviewRows = buildBrandDirectoryPromotionReviewRows(
    evidenceRecords,
    candidateById
  );
  const summary = summarizePromotionReview(reviewRows);

  const propertyIdCounts = new Map();
  const multiUrlCandidates = [];
  for (const r of reviewRows) {
    const pid = r.choicePropertyId;
    if (pid) propertyIdCounts.set(pid, (propertyIdCounts.get(pid) || 0) + 1);
    if (r.evidenceCount > 1) {
      multiUrlCandidates.push({
        candidateId: r.candidateAirtableRecordId,
        evidenceCount: r.evidenceCount,
        choicePropertyIds: r.allChoicePropertyIds,
      });
    }
  }
  const sharedPropertyIds = [...propertyIdCounts.entries()]
    .filter(([, n]) => n > 1)
    .map(([propertyId, count]) => ({ propertyId, count }));

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4R-promotion-review",
    evidenceSource: "brand_directory",
    evidenceBatchId: args.evidenceBatchId,
    reviewBatchId: args.reviewBatchId,
    candidateSourceType: args.candidateSourceType,
    dryRun: true,
    airtableWrites: false,
    hotelCensusReads: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    evidenceRowsReviewed: evidenceRecords.length,
    linkedCandidatesFound: candidateById.size,
    promotionReviewRowCount: reviewRows.length,
    summary,
    sharedChoicePropertyIds: sharedPropertyIds,
    candidatesWithMultipleChoiceUrls: multiUrlCandidates,
    humanApprovalRequired: true,
    noAutoPromotion: true,
    reportFiles: { json: jsonPath, csv: csvPath },
    reviewRows,
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, reviewRows.map(brandDirectoryRowToCsv), BRAND_DIRECTORY_CSV_COLUMNS);

  printSummary(summary, sharedPropertyIds, multiUrlCandidates, jsonPath, csvPath);
}

async function runCorrectedChoiceReview(args) {
  const reportBase = `independent-census-promotion-review-${args.reportSlug}`;
  const jsonPath = join(REPORTS_DIR, `${reportBase}.json`);
  const csvPath = join(REPORTS_DIR, `${reportBase}.csv`);

  const phaseLabel = args.evidenceBatchId.includes("all-direct")
    ? "4Y"
    : "4V";

  console.log(
    `=== Independent census promotion review (Phase ${phaseLabel}, read-only) ===\n`
  );
  console.log(
    `Evidence source:    choice_property_id_reconciliation (${phaseLabel} batch only)`
  );
  console.log(`Evidence batch:     ${args.evidenceBatchId}`);
  console.log(`Candidate filter:   source type = ${args.candidateSourceType}`);
  console.log(`Review batch ID:    ${args.reviewBatchId}`);
  console.log("Prior Phase 4Q collision evidence: ignored");
  console.log("No Verified promotion. No Hotel Census reads/writes.\n");

  console.log(`Loading Independent Hotel Source Evidence (${phaseLabel} batch)…`);
  const evidenceRecords = await loadEvidenceByBatch(args.evidenceBatchId, {
    dedupePrefix: "4U",
  });
  console.log(`  Evidence rows reviewed: ${evidenceRecords.length}`);

  const candidateIds = evidenceRecords.flatMap((e) => e.candidateLinkIds);
  console.log("Loading linked Independent Hotel Source Candidates…");
  const candidateById = await loadCandidatesByIds(candidateIds, {
    candidateSourceType: args.candidateSourceType,
  });
  console.log(`  Linked candidates reviewed: ${candidateById.size}`);

  const reviewRows = buildCorrectedChoicePromotionReviewRows(
    evidenceRecords,
    candidateById
  );
  const summary = summarizePromotionReview(reviewRows);

  const report = {
    generatedAt: new Date().toISOString(),
    phase: `${phaseLabel}-promotion-review`,
    evidenceSource: "choice_property_id_reconciliation",
    evidenceBatchId: args.evidenceBatchId,
    reviewBatchId: args.reviewBatchId,
    candidateSourceType: args.candidateSourceType,
    priorPhase4QEvidenceIgnored: true,
    dryRun: true,
    airtableWrites: false,
    hotelCensusReads: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    evidenceRowsReviewed: evidenceRecords.length,
    linkedCandidatesReviewed: candidateById.size,
    promotionReviewRowCount: reviewRows.length,
    summary,
    humanApprovalRequired: true,
    noAutoPromotion: true,
    reportFiles: { json: jsonPath, csv: csvPath },
    reviewRows,
  };

  writeJson(jsonPath, report);
  writeCsv(
    csvPath,
    reviewRows.map(correctedChoicePromotionRowToCsv),
    CORRECTED_CHOICE_PROMOTION_CSV_COLUMNS
  );

  if (reviewRows.length) {
    console.log("\n--- Proposed Verified mapping (sample) ---");
    const sample = reviewRows[0];
    console.log(JSON.stringify(sample.proposedVerified, null, 2));
  }

  printSummary(summary, [], [], jsonPath, csvPath);
}

async function runWikidataReview(args) {
  const reportBase = `independent-census-promotion-review-${args.reportSlug}`;
  const jsonPath = join(REPORTS_DIR, `${reportBase}.json`);
  const csvPath = join(REPORTS_DIR, `${reportBase}.csv`);

  console.log("=== Independent census promotion review (Phase 4B, read-only) ===\n");
  console.log(`Evidence source:    wikidata`);
  console.log(`Evidence batch:     ${args.evidenceBatchId}`);
  console.log(`Candidate batch:    ${args.candidateBatchId}`);
  console.log("No Verified promotion. No Hotel Census reads/writes.\n");

  console.log("Loading Independent Hotel Source Evidence…");
  const evidenceRecords = await loadEvidenceByBatch(args.evidenceBatchId, {
    dedupePrefix: "4A",
  });
  console.log(`  Evidence rows reviewed: ${evidenceRecords.length}`);

  const candidateIds = evidenceRecords.flatMap((e) => e.candidateLinkIds);
  console.log("Loading linked Independent Hotel Source Candidates…");
  const candidateById = await loadCandidatesByIds(candidateIds, {
    candidateBatchId: args.candidateBatchId,
  });
  console.log(`  Linked candidates found: ${candidateById.size}`);

  const reviewRows = buildPromotionReviewRows(evidenceRecords, candidateById);
  const summary = summarizePromotionReview(reviewRows);

  const duplicateQids = new Map();
  for (const r of reviewRows) {
    if (!r.wikidataQid) continue;
    duplicateQids.set(r.wikidataQid, (duplicateQids.get(r.wikidataQid) || 0) + 1);
  }
  const sharedQids = [...duplicateQids.entries()]
    .filter(([, n]) => n > 1)
    .map(([qid, n]) => ({ qid, count: n }));

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4B-promotion-review",
    evidenceSource: "wikidata",
    evidenceBatchId: args.evidenceBatchId,
    candidateBatchId: args.candidateBatchId,
    dryRun: true,
    airtableWrites: false,
    hotelCensusReads: false,
    hotelCensusWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    evidenceRowsReviewed: evidenceRecords.length,
    linkedCandidatesFound: candidateById.size,
    promotionReviewRowCount: reviewRows.length,
    summary,
    sharedWikidataQids: sharedQids,
    humanApprovalRequired: true,
    reportFiles: { json: jsonPath, csv: csvPath },
    reviewRows,
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, reviewRows.map(wikidataRowToCsv), WIKIDATA_CSV_COLUMNS);

  if (sharedQids.length) {
    console.log("\n--- Shared Wikidata QIDs (possible duplicate) ---");
    console.log(sharedQids.slice(0, 10));
  }

  printSummary(summary, sharedQids, [], jsonPath, csvPath);
}

function printSummary(summary, duplicateFlags, multiUrl, jsonPath, csvPath) {
  console.log("\n--- Promotion eligibility ---");
  console.log(`  eligible_for_review:      ${summary.eligible_for_review}`);
  console.log(`  needs_manual_research:    ${summary.needs_manual_research}`);
  console.log(`  possible_duplicate:       ${summary.possible_duplicate}`);
  console.log(`  insufficient_core_fields: ${summary.insufficient_core_fields}`);

  console.log("\n--- Promotion recommendation ---");
  console.log(`  promote_after_review:     ${summary.promote_after_review}`);
  console.log(`  review_before_promote:    ${summary.review_before_promote}`);
  console.log(`  do_not_promote_yet:       ${summary.do_not_promote_yet}`);

  console.log("\n--- Review risk ---");
  console.log(`  low:    ${summary.reviewRiskLow}`);
  console.log(`  medium: ${summary.reviewRiskMedium}`);
  console.log(`  high:   ${summary.reviewRiskHigh}`);

  if (duplicateFlags.length) {
    console.log("\n--- Duplicate flags ---");
    console.log(duplicateFlags.slice(0, 10));
  }
  if (multiUrl.length) {
    console.log(`\nCandidates with multiple Choice URLs: ${multiUrl.length}`);
    console.log(multiUrl.slice(0, 5));
  }

  const manualFlags =
    (summary.needs_manual_research || 0) +
    (summary.review_before_promote || 0);
  if (manualFlags) {
    console.log(`\nManual-review attention: see eligibility/recommendation counts above.`);
  }

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ Read-only. No Airtable writes. Verified, Hotel Census, Brand Setup, Brand Alias, Candidates, and Evidence untouched."
  );
}

async function main() {
  const args = parseArgs();
  if (args.evidenceSource === "choice_property_id_reconciliation") {
    await runCorrectedChoiceReview(args);
  } else if (args.evidenceSource === "brand_directory") {
    await runBrandDirectoryReview(args);
  } else {
    await runWikidataReview(args);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
