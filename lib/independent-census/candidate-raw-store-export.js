/**
 * Export archive-later candidates to local raw store (no Airtable writes/deletes).
 */

import { readFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { loadCandidateRetentionReport } from "./match-brand-directory-properties.js";
import { CLEANUP_CLASS } from "./candidate-cleanup-plan.js";
import { normalizeCountry, normalizeKey } from "./match-current-census.js";

const SAFE_EXPORT_FIELDS = [
  "airtableRecordId",
  "sourceType",
  "sourceRecordId",
  "importBatchId",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "rawLatitude",
  "rawLongitude",
  "rawWebsite",
  "rawBrand",
  "parentCompany",
  "qualityTier",
  "qualityScore",
  "recommendedAction",
  "reviewStatus",
  "missingFields",
  "websiteHost",
  "retentionRecommendation",
  "inDuplicateCluster",
  "likelyOsmEnrichForBrandDirectory",
];

export function loadCleanupPlanReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const planRows = Array.isArray(data.planRows) ? data.planRows : [];
  return { data, planRows };
}

function pickSafeExportRow(coverageRow, planRow) {
  const out = { classification: planRow.classification, reasons: planRow.reasons };
  for (const key of SAFE_EXPORT_FIELDS) {
    if (coverageRow[key] !== undefined) out[key] = coverageRow[key];
  }
  out.rawPhone = coverageRow.rawPhone || "";
  return out;
}

/**
 * @param {object} opts
 */
export function runCandidateRawStoreExport(opts) {
  const cleanup = loadCleanupPlanReport(opts.cleanupPlanPath);
  const coverage = loadCandidateRetentionReport(opts.coverageReportPath);

  const coverageById = new Map(
    coverage.rows.map((r) => [r.airtableRecordId, r])
  );

  const exportIds = new Set();
  for (const pr of cleanup.planRows) {
    if (pr.classification === CLEANUP_CLASS.EXPORT_TO_RAW_STORE_THEN_ARCHIVE) {
      exportIds.add(pr.airtableRecordId);
    }
  }

  const exportRows = [];
  let missingCoverage = 0;
  for (const id of exportIds) {
    const cov = coverageById.get(id);
    const plan = cleanup.planRows.find((p) => p.airtableRecordId === id);
    if (!cov) {
      missingCoverage++;
      continue;
    }
    exportRows.push(pickSafeExportRow(cov, plan || { classification: "", reasons: [] }));
  }

  exportRows.sort((a, b) => {
    const ca = normalizeCountry(a.rawCountry);
    const cb = normalizeCountry(b.rawCountry);
    if (ca !== cb) return ca.localeCompare(cb);
    return normalizeKey(a.rawHotelName).localeCompare(normalizeKey(b.rawHotelName));
  });

  const byCountry = {};
  for (const r of exportRows) {
    const co = normalizeCountry(r.rawCountry) || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
  }

  return {
    batchId: opts.batchId || "candidate-raw-store-export-2026-05-20",
    cleanupPlanPath: opts.cleanupPlanPath,
    coverageReportPath: opts.coverageReportPath,
    exportClassification: CLEANUP_CLASS.EXPORT_TO_RAW_STORE_THEN_ARCHIVE,
    exportTargetCount: exportIds.size,
    exportRowCount: exportRows.length,
    missingCoverageRows: missingCoverage,
    byCountry,
    exportRows,
    dryRun: true,
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

export function rawStoreRowToCsv(row) {
  return {
    airtableRecordId: row.airtableRecordId,
    sourceType: row.sourceType,
    sourceRecordId: row.sourceRecordId,
    importBatchId: row.importBatchId,
    rawHotelName: row.rawHotelName,
    rawCity: row.rawCity,
    rawCountry: row.rawCountry,
    rawLatitude: row.rawLatitude,
    rawLongitude: row.rawLongitude,
    rawWebsite: row.rawWebsite,
    rawBrand: row.rawBrand,
    retentionRecommendation: row.retentionRecommendation,
    qualityScore: row.qualityScore ?? "",
    classification: row.classification,
  };
}

export const RAW_STORE_CSV_COLUMNS = [
  "airtableRecordId",
  "sourceType",
  "sourceRecordId",
  "importBatchId",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "rawLatitude",
  "rawLongitude",
  "rawWebsite",
  "rawBrand",
  "retentionRecommendation",
  "qualityScore",
  "classification",
];

export function ensureParentDir(filePath) {
  mkdirSync(dirname(filePath), { recursive: true });
}

export function defaultRawStorePaths(projectRoot, dateSlug = "2026-05-20") {
  const rawDir = join(projectRoot, "data", "independent-census", "raw");
  return {
    json: join(rawDir, `airtable-candidate-export-${dateSlug}.json`),
    csv: join(rawDir, `airtable-candidate-export-${dateSlug}.csv`),
    reportJson: join(
      projectRoot,
      "reports",
      `independent-census-candidate-raw-store-export-${dateSlug}.json`
    ),
  };
}
