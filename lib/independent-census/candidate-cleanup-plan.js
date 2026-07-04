/**
 * Report-only candidate cleanup / export plan for Airtable record-limit recovery.
 * Does not archive, delete, or write to Airtable.
 */

import { readFileSync } from "fs";
import { SOURCE_TYPES } from "./fields.js";
import {
  loadCandidateRetentionReport,
} from "./match-brand-directory-properties.js";
import {
  normalizeCountry,
  normalizeKey,
  normalizeText,
} from "./match-current-census.js";
import { PROMOTION_RECOMMENDATION } from "./backwards-census-match.js";

export const CLEANUP_CLASS = {
  KEEP_IN_AIRTABLE: "keep_in_airtable",
  EVIDENCE_SUPPORTED_KEEP: "evidence_supported_keep",
  VERIFIED_LINKED_KEEP: "verified_linked_keep",
  OFFICIAL_SOURCE_KEEP: "official_source_keep",
  HIGH_CONFIDENCE_MATCH_KEEP: "high_confidence_match_keep",
  DUPLICATE_REVIEW: "duplicate_review",
  EXPORT_TO_RAW_STORE_THEN_ARCHIVE: "export_to_raw_store_then_archive",
  LOW_PRIORITY_ARCHIVE_LATER: "low_priority_archive_later",
  DO_NOT_TOUCH: "do_not_touch",
};

const ACTIVE_PARENT_COMPANY_VALIDATION = new Set(
  ["choice hotels international", "choice hotels"].map(normalizeKey)
);

const HIGH_QUALITY_SCORE = 60;

export function loadBackwardsMatchReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const rows = Array.isArray(data.reportRows) ? data.reportRows : [];
  return { data, rows, summary: data };
}

export function buildBackwardsMatchIndex(rows) {
  const byCandidateId = new Map();
  for (const r of rows) {
    if (r.osmCandidateRecordId) {
      byCandidateId.set(r.osmCandidateRecordId, r);
    }
  }
  return byCandidateId;
}

function hasEvidenceHeuristic(row) {
  const batch = normalizeKey(row.importBatchId || "");
  return (
    batch.includes("evidence") ||
    batch.includes("corrected") ||
    batch.includes("reconcile")
  );
}

function meetsArchiveExportCriteria(row, backwardsRow) {
  if (normalizeKey(row.sourceType) !== SOURCE_TYPES.OSM) return false;
  if (normalizeKey(row.retentionRecommendation) !== "low_priority_hold") {
    return false;
  }
  if (hasEvidenceHeuristic(row)) return false;
  if (row.likelyOsmEnrichForBrandDirectory) return false;
  if (
    backwardsRow?.promotionRecommendation ===
    PROMOTION_RECOMMENDATION.ALREADY_VERIFIED
  ) {
    return false;
  }
  if (backwardsRow?.matchConfidence === "high") return false;
  if (normalizeText(row.rawWebsite)) return false;
  if (normalizeText(row.rawPhone)) return false;
  if (normalizeText(row.rawBrand)) return false;
  if (normalizeText(row.rawCity)) return false;
  return true;
}

/**
 * @param {object} row — Phase 4O candidate row
 * @param {object} ctx
 */
export function classifyCandidateForCleanup(row, ctx) {
  const { backwardsById } = ctx;
  const reasons = [];
  const st = normalizeKey(row.sourceType);
  const retention = normalizeKey(row.retentionRecommendation);
  const backwards = backwardsById.get(row.airtableRecordId);

  if (st === SOURCE_TYPES.BRAND_DIRECTORY) {
    reasons.push("sourceType=brand_directory");
    return {
      classification: CLEANUP_CLASS.OFFICIAL_SOURCE_KEEP,
      reasons,
    };
  }

  if (
    backwards?.promotionRecommendation ===
    PROMOTION_RECOMMENDATION.ALREADY_VERIFIED
  ) {
    reasons.push("linked in Verified Independent Hotel Census");
    return {
      classification: CLEANUP_CLASS.VERIFIED_LINKED_KEEP,
      reasons,
    };
  }

  if (hasEvidenceHeuristic(row)) {
    reasons.push("import batch indicates evidence-supported workflow");
    return {
      classification: CLEANUP_CLASS.EVIDENCE_SUPPORTED_KEEP,
      reasons,
    };
  }

  if (backwards?.matchConfidence === "high") {
    reasons.push("high-confidence legacy census backwards match");
    return {
      classification: CLEANUP_CLASS.HIGH_CONFIDENCE_MATCH_KEEP,
      reasons,
    };
  }

  if (retention === "duplicate_review" || row.inDuplicateCluster) {
    reasons.push("duplicate_review retention or duplicate cluster");
    return {
      classification: CLEANUP_CLASS.DUPLICATE_REVIEW,
      reasons,
    };
  }

  const parentCo = normalizeKey(row.parentCompany || "");
  if (
    ACTIVE_PARENT_COMPANY_VALIDATION.has(parentCo) ||
    parentCo.includes("choice hotels")
  ) {
    reasons.push("active parent-company validation set (Choice)");
    return {
      classification: CLEANUP_CLASS.KEEP_IN_AIRTABLE,
      reasons,
    };
  }

  if (backwards?.promotionEligibility === "eligible_for_promotion") {
    reasons.push("backwards-match promotion eligible");
    return {
      classification: CLEANUP_CLASS.KEEP_IN_AIRTABLE,
      reasons,
    };
  }

  if (normalizeText(row.rawWebsite)) {
    reasons.push("has website");
    return { classification: CLEANUP_CLASS.KEEP_IN_AIRTABLE, reasons };
  }

  if (normalizeText(row.rawBrand)) {
    reasons.push("has brand");
    return { classification: CLEANUP_CLASS.KEEP_IN_AIRTABLE, reasons };
  }

  const score = Number(row.qualityScore) || 0;
  if (score >= HIGH_QUALITY_SCORE || normalizeKey(row.qualityTier) === "high") {
    reasons.push(`quality score/tier (${score || row.qualityTier})`);
    return { classification: CLEANUP_CLASS.KEEP_IN_AIRTABLE, reasons };
  }

  if (
    ["keep_high_priority", "enrich_next", "keep_for_matching"].includes(retention)
  ) {
    reasons.push(`retention=${retention}`);
    return { classification: CLEANUP_CLASS.KEEP_IN_AIRTABLE, reasons };
  }

  if (meetsArchiveExportCriteria(row, backwards)) {
    reasons.push(
      "osm low_priority_hold without evidence, verified link, brand-directory overlap, website, phone, brand, city, or high legacy match"
    );
    return {
      classification: CLEANUP_CLASS.EXPORT_TO_RAW_STORE_THEN_ARCHIVE,
      reasons,
    };
  }

  if (st === SOURCE_TYPES.OSM && retention === "low_priority_hold") {
    reasons.push("osm low_priority_hold — defer archive until raw export reviewed");
    return {
      classification: CLEANUP_CLASS.LOW_PRIORITY_ARCHIVE_LATER,
      reasons,
    };
  }

  reasons.push("default protected hold — no automated archive");
  return { classification: CLEANUP_CLASS.DO_NOT_TOUCH, reasons };
}

export function cleanupPlanRowToCsv(row) {
  return {
    airtableRecordId: row.airtableRecordId,
    sourceType: row.sourceType,
    rawHotelName: row.rawHotelName,
    rawCountry: row.rawCountry,
    rawCity: row.rawCity,
    retentionRecommendation: row.retentionRecommendation,
    qualityScore: row.qualityScore ?? "",
    classification: row.classification,
    reasons: (row.reasons || []).join("; "),
    matchConfidence: row.matchConfidence || "",
    promotionEligibility: row.promotionEligibility || "",
    promotionRecommendation: row.promotionRecommendation || "",
  };
}

export const CLEANUP_PLAN_CSV_COLUMNS = [
  "airtableRecordId",
  "sourceType",
  "rawHotelName",
  "rawCountry",
  "rawCity",
  "retentionRecommendation",
  "qualityScore",
  "classification",
  "reasons",
  "matchConfidence",
  "promotionEligibility",
  "promotionRecommendation",
];

/**
 * @param {object} opts
 */
export function runCandidateCleanupPlan(opts) {
  const coverage = loadCandidateRetentionReport(opts.coverageReportPath);
  const backwards = opts.backwardsMatchReportPath
    ? loadBackwardsMatchReport(opts.backwardsMatchReportPath)
    : { rows: [], summary: {} };
  const backwardsById = buildBackwardsMatchIndex(backwards.rows);

  const planRows = [];
  const byClassification = {};
  const byRetention = {};
  const byCountry = {};

  for (const row of coverage.rows) {
    const { classification, reasons } = classifyCandidateForCleanup(row, {
      backwardsById,
    });
    const ret = normalizeKey(row.retentionRecommendation);
    const co = normalizeCountry(row.rawCountry) || "(unknown)";
    byClassification[classification] = (byClassification[classification] || 0) + 1;
    byRetention[ret] = (byRetention[ret] || 0) + 1;
    if (!byCountry[co]) byCountry[co] = {};
    byCountry[co][classification] = (byCountry[co][classification] || 0) + 1;

    const bm = backwardsById.get(row.airtableRecordId);
    planRows.push({
      airtableRecordId: row.airtableRecordId,
      sourceType: row.sourceType,
      rawHotelName: row.rawHotelName,
      rawCountry: row.rawCountry,
      rawCity: row.rawCity,
      retentionRecommendation: row.retentionRecommendation,
      qualityScore: row.qualityScore,
      classification,
      reasons,
      matchConfidence: bm?.matchConfidence || "",
      promotionEligibility: bm?.promotionEligibility || "",
      promotionRecommendation: bm?.promotionRecommendation || "",
    });
  }

  const keepClasses = new Set([
    CLEANUP_CLASS.KEEP_IN_AIRTABLE,
    CLEANUP_CLASS.EVIDENCE_SUPPORTED_KEEP,
    CLEANUP_CLASS.VERIFIED_LINKED_KEEP,
    CLEANUP_CLASS.OFFICIAL_SOURCE_KEEP,
    CLEANUP_CLASS.HIGH_CONFIDENCE_MATCH_KEEP,
    CLEANUP_CLASS.DUPLICATE_REVIEW,
    CLEANUP_CLASS.DO_NOT_TOUCH,
  ]);

  const archiveClasses = new Set([
    CLEANUP_CLASS.EXPORT_TO_RAW_STORE_THEN_ARCHIVE,
    CLEANUP_CLASS.LOW_PRIORITY_ARCHIVE_LATER,
  ]);

  let keepInAirtableCount = 0;
  let archiveLaterCount = 0;
  for (const [cls, count] of Object.entries(byClassification)) {
    if (keepClasses.has(cls)) keepInAirtableCount += count;
    if (archiveClasses.has(cls)) archiveLaterCount += count;
  }

  const totalCandidates = coverage.rows.length;
  const estimatedAirtableRecordReduction = archiveLaterCount;
  const estimatedReductionPct =
    totalCandidates > 0
      ? Math.round((estimatedAirtableRecordReduction / totalCandidates) * 1000) /
        10
      : 0;

  return {
    batchId: opts.batchId,
    dryRun: true,
    totalCandidates,
    byClassification,
    byRetention,
    byCountryTop: Object.entries(byCountry)
      .map(([country, counts]) => ({ country, ...counts }))
      .sort((a, b) => {
        const sum = (o) =>
          Object.entries(o)
            .filter(([k]) => k !== "country")
            .reduce((s, [, v]) => s + v, 0);
        return sum(b) - sum(a);
      })
      .slice(0, 25),
    keepInAirtableCount,
    archiveLaterCount,
    exportToRawStoreCount:
      byClassification[CLEANUP_CLASS.EXPORT_TO_RAW_STORE_THEN_ARCHIVE] || 0,
    lowPriorityArchiveLaterCount:
      byClassification[CLEANUP_CLASS.LOW_PRIORITY_ARCHIVE_LATER] || 0,
    estimatedAirtableRecordReduction,
    estimatedReductionPct,
    backwardsMatchRowsUsed: backwards.rows.length,
    planRows,
    airtableWrites: false,
    archiveExecuted: false,
    deleteExecuted: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    verifiedTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}
