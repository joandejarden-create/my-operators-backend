/**
 * Phase 4L — Gated apply: Choice brand-directory property URLs → staging candidates.
 */

import { readFileSync } from "fs";
import {
  CANDIDATE_FIELDS,
  CANDIDATES_TABLE,
  MATCH_CONFIDENCE,
  RECOMMENDED_ACTION,
  REVIEW_STATUS,
  SOURCE_TYPES,
} from "./fields.js";
import { candidateDuplicateKey, createCandidateRecords } from "./candidate-apply.js";
import { getIndependentCensusBase } from "./platform-base.js";
import {
  loadPropertyUrlExtractReport,
  filterChoicePropertiesForMatch,
  deriveInferredHotelName,
} from "./match-brand-directory-properties.js";
import { SOURCE_NAME, SOURCE_LICENSE, SOURCE_POLICY } from "./brand-directory-property-url-extract.js";
import { normalizeKey, normalizeText } from "./match-current-census.js";
import { PROPERTY_MATCH_ACTIONS } from "./match-brand-directory-properties.js";

const DEFAULT_INCLUDE_MATCH_ACTIONS = new Set([
  PROPERTY_MATCH_ACTIONS.CREATE_NEW,
  PROPERTY_MATCH_ACTIONS.MANUAL,
]);

const EXCLUDED_MATCH_ACTIONS = new Set([
  PROPERTY_MATCH_ACTIONS.UNMATCHED_BRAND,
  PROPERTY_MATCH_ACTIONS.EXCLUDE_NON_CALA,
  PROPERTY_MATCH_ACTIONS.LINK_CANDIDATE,
  PROPERTY_MATCH_ACTIONS.LINK_VERIFIED,
]);

function titleCaseCity(slug) {
  return normalizeText(String(slug || "").replace(/-/g, " "));
}

export function choicePropertyDedupeKey(propertyId) {
  const id = String(propertyId || "").trim().toLowerCase();
  return `brand_directory|choice|${id}`;
}

export function loadMatchReport(filePath) {
  if (!filePath) return { data: null, byPropertyId: new Map(), byPropertyUrl: new Map() };
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const byPropertyId = new Map();
  const byPropertyUrl = new Map();
  for (const m of data.matches || []) {
    if (m.propertyId) byPropertyId.set(String(m.propertyId).toLowerCase(), m);
    if (m.propertyUrl) byPropertyUrl.set(normalizeKey(m.propertyUrl), m);
  }
  return { data, byPropertyId, byPropertyUrl };
}

export function parseIncludeMatchActions(includeActionsStr) {
  if (!includeActionsStr) return new Set(DEFAULT_INCLUDE_MATCH_ACTIONS);
  const parsed = new Set(
    includeActionsStr
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  );
  return parsed.size ? parsed : new Set(DEFAULT_INCLUDE_MATCH_ACTIONS);
}

function bestMatchConfidence(matchRow) {
  if (!matchRow) return MATCH_CONFIDENCE.NONE;
  const order = { high: 3, medium: 2, low: 1, none: 0 };
  const c = matchRow.candidateMatchConfidence || "none";
  const v = matchRow.verifiedMatchConfidence || "none";
  return order[c] >= order[v] ? c : v;
}

/**
 * @param {object} row — extract property row
 * @param {object|null} matchRow
 * @param {string} importBatchId
 */
export function propertyRowToCandidate(row, matchRow, importBatchId) {
  const propertyId = String(row.propertyId || "").trim();
  const rawHotelName =
    matchRow?.inferredHotelName ||
    deriveInferredHotelName(row) ||
    `${row.matchedBrandSetupBrand || row.inferredBrandName || "Choice"} ${titleCaseCity(row.citySlug)} ${propertyId}`.trim();

  const payload = {
    phase: "4L-brand-directory-property-apply",
    propertyUrl: row.propertyUrl,
    propertyId,
    brandSlug: row.brandSlug,
    citySlug: row.citySlug,
    countryOrRegionSegment: row.countryOrRegionSegment,
    inferredCountry: row.inferredCountry,
    parentCompany: row.parentCompany,
    matchedBrandSetupBrand: row.matchedBrandSetupBrand,
    calaFilterStatus: row.calaFilterStatus,
    extractRecommendedAction: row.recommendedAction,
    sourcePolicy: SOURCE_POLICY,
    sourcePolicyApproved: true,
    matchSummary: matchRow
      ? {
          candidateMatchConfidence: matchRow.candidateMatchConfidence,
          candidateMatchReason: matchRow.candidateMatchReason,
          matchedCandidateRecordId: matchRow.matchedCandidateRecordId,
          matchedCandidateName: matchRow.matchedCandidateName,
          verifiedMatchConfidence: matchRow.verifiedMatchConfidence,
          verifiedMatchReason: matchRow.verifiedMatchReason,
          matchedVerifiedRecordId: matchRow.matchedVerifiedRecordId,
          matchedVerifiedName: matchRow.matchedVerifiedName,
          matchRecommendedAction: matchRow.recommendedAction,
        }
      : null,
    note: "Official Choice sitemap property URL lead; not verified master record; no property HTML fetched.",
  };

  return {
    sourceName: row.sourceName || SOURCE_NAME,
    sourceType: SOURCE_TYPES.BRAND_DIRECTORY,
    sourceLicense: row.sourceLicense || SOURCE_LICENSE,
    sourceUrl: row.propertyUrl,
    sourceRecordId: propertyId,
    rawHotelName,
    rawAddress: "",
    rawCity: titleCaseCity(row.citySlug),
    rawCountry: row.inferredCountry || "",
    rawLatitude: null,
    rawLongitude: null,
    rawWebsite: row.propertyUrl,
    rawPhone: "",
    rawBrand: row.matchedBrandSetupBrand || row.inferredBrandName || "",
    rawPayloadJson: JSON.stringify(payload),
    importBatchId,
    importedAt: new Date().toISOString(),
    reviewStatus: REVIEW_STATUS.PENDING,
    possibleMatchConfidence: bestMatchConfidence(matchRow),
    recommendedAction: RECOMMENDED_ACTION.NEEDS_RESEARCH,
    candidateDedupeKey: choicePropertyDedupeKey(propertyId),
  };
}

/**
 * @param {Array<object>} extractRows
 * @param {{ byPropertyId: Map, byPropertyUrl: Map, data: object|null }} matchIndex
 * @param {object} options
 */
export function selectPropertiesForApply(extractRows, matchIndex, options = {}) {
  const {
    includeMatchActions = DEFAULT_INCLUDE_MATCH_ACTIONS,
    requireMatchReport = false,
    parentCompany = "",
  } = options;

  const selected = [];
  const skippedByCala = [];
  const skippedByExtractAction = [];
  const skippedByMatchAction = [];
  const skippedByParent = [];
  const skippedNoMatchRow = [];

  for (const row of extractRows) {
    if (parentCompany && row.parentCompany !== parentCompany) {
      skippedByParent.push(row);
      continue;
    }

    const calaOk =
      row.calaFilterStatus === "included" || row.calaFilterStatus === "likely";
    if (!calaOk) {
      skippedByCala.push(row);
      continue;
    }

    if (row.recommendedAction !== "ready_for_candidate_review") {
      skippedByExtractAction.push(row);
      continue;
    }

    const pid = String(row.propertyId || "").toLowerCase();
    const matchRow =
      matchIndex.byPropertyId.get(pid) ||
      matchIndex.byPropertyUrl.get(normalizeKey(row.propertyUrl)) ||
      null;

    if (requireMatchReport && !matchRow) {
      skippedNoMatchRow.push(row);
      continue;
    }

    const matchAction = matchRow?.recommendedAction || PROPERTY_MATCH_ACTIONS.CREATE_NEW;

    if (EXCLUDED_MATCH_ACTIONS.has(matchAction)) {
      skippedByMatchAction.push({ row, matchAction });
      continue;
    }

    if (!includeMatchActions.has(matchAction)) {
      skippedByMatchAction.push({ row, matchAction });
      continue;
    }

    const candidate = propertyRowToCandidate(row, matchRow, options.importBatchId);
    selected.push({
      candidate,
      matchRow: matchRow
        ? {
            matchConfidence: bestMatchConfidence(matchRow),
            recommendedAction: RECOMMENDED_ACTION.NEEDS_RESEARCH,
            matchReason: matchRow.candidateMatchReason || matchRow.verifiedMatchReason,
          }
        : null,
      extractRow: row,
      matchRecommendedAction: matchAction,
    });
  }

  return {
    selected,
    skippedByCala,
    skippedByExtractAction,
    skippedByMatchAction,
    skippedByParent,
    skippedNoMatchRow,
  };
}

/**
 * Existing brand_directory rows for dedupe (read-only).
 */
export async function loadExistingBrandDirectoryDedupeIndex(base, tableName = CANDIDATES_TABLE) {
  const recordIds = new Set();
  const urls = new Set();
  const batchKeys = new Set();

  const formula = `{${CANDIDATE_FIELDS.sourceType}} = '${SOURCE_TYPES.BRAND_DIRECTORY}'`;
  const fields = [
    CANDIDATE_FIELDS.sourceType,
    CANDIDATE_FIELDS.sourceRecordId,
    CANDIDATE_FIELDS.sourceUrl,
    CANDIDATE_FIELDS.importBatchId,
    CANDIDATE_FIELDS.candidateDedupeKey,
  ];

  await new Promise((resolve, reject) => {
    base(tableName)
      .select({ filterByFormula: formula, fields })
      .eachPage(
        (records, fetchNextPage) => {
          for (const rec of records) {
            const f = rec.fields;
            const st = f[CANDIDATE_FIELDS.sourceType];
            const sr = String(f[CANDIDATE_FIELDS.sourceRecordId] || "").trim().toLowerCase();
            const url = normalizeKey(f[CANDIDATE_FIELDS.sourceUrl]);
            const batch = f[CANDIDATE_FIELDS.importBatchId];
            if (sr) recordIds.add(`${normalizeKey(st)}|${sr}`);
            if (url) urls.add(url);
            batchKeys.add(
              candidateDuplicateKey(st, f[CANDIDATE_FIELDS.sourceRecordId], batch)
            );
            const dk = f[CANDIDATE_FIELDS.candidateDedupeKey];
            if (dk) batchKeys.add(normalizeKey(dk));
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return { recordIds, urls, batchKeys };
}

export function isDuplicateCandidate(candidate, index) {
  const sr = String(candidate.sourceRecordId || "").trim().toLowerCase();
  const url = normalizeKey(candidate.sourceUrl || candidate.rawWebsite);
  const batchKey = candidateDuplicateKey(
    candidate.sourceType,
    candidate.sourceRecordId,
    candidate.importBatchId
  );
  const dedupeKey = normalizeKey(candidate.candidateDedupeKey);

  if (sr && index.recordIds.has(`${SOURCE_TYPES.BRAND_DIRECTORY}|${sr}`)) return "source_record_id";
  if (url && index.urls.has(url)) return "source_url";
  if (index.batchKeys.has(batchKey)) return "batch_key";
  if (dedupeKey && index.batchKeys.has(dedupeKey)) return "dedupe_key";
  return null;
}

/**
 * @param {Array<{ candidate: object, matchRow: object|null }>} selected
 * @param {object} dedupeIndex
 */
export function filterDuplicatesBeforeWrite(selected, dedupeIndex) {
  const toWrite = [];
  const skippedDuplicate = [];

  for (const row of selected) {
    const reason = isDuplicateCandidate(row.candidate, dedupeIndex);
    if (reason) {
      skippedDuplicate.push({ ...row, duplicateReason: reason });
      continue;
    }
    toWrite.push(row);
  }

  return { toWrite, skippedDuplicate };
}

export async function applyBrandDirectoryPropertyCandidates(options) {
  const {
    propertyUrlReportPath,
    matchReportPath = "",
    parentCompany = "",
    importBatchId,
    includeMatchActions = DEFAULT_INCLUDE_MATCH_ACTIONS,
    apply = false,
    sourcePolicyApproved = false,
  } = options;

  const { rows: extractRows } = loadPropertyUrlExtractReport(propertyUrlReportPath);
  const eligibleExtract = filterChoicePropertiesForMatch(extractRows);
  const matchIndex = loadMatchReport(matchReportPath || null);

  const selection = selectPropertiesForApply(eligibleExtract, matchIndex, {
    includeMatchActions,
    requireMatchReport: Boolean(matchReportPath),
    parentCompany,
    importBatchId,
  });

  const report = {
    propertyUrlReportPath,
    matchReportPath: matchReportPath || null,
    parentCompany,
    importBatchId,
    extractRowsTotal: extractRows.length,
    eligibleExtractRows: eligibleExtract.length,
    selected: selection.selected.length,
    skippedByCala: selection.skippedByCala.length,
    skippedByExtractAction: selection.skippedByExtractAction.length,
    skippedByMatchAction: selection.skippedByMatchAction.length,
    skippedByParent: selection.skippedByParent.length,
    skippedNoMatchRow: selection.skippedNoMatchRow.length,
    skippedDuplicate: 0,
    written: 0,
    writtenRecords: [],
    apply,
    sourcePolicyApproved,
  };

  if (!apply) {
    return { ...report, dryRun: true, toWrite: selection.selected };
  }

  if (!sourcePolicyApproved) {
    throw new Error("Apply requires --source-policy-approved flag");
  }

  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const dedupeIndex = await loadExistingBrandDirectoryDedupeIndex(base);
  const { toWrite, skippedDuplicate } = filterDuplicatesBeforeWrite(
    selection.selected,
    dedupeIndex
  );

  const existingKeys = dedupeIndex.batchKeys;
  const result = await createCandidateRecords(
    base,
    CANDIDATES_TABLE,
    toWrite,
    existingKeys
  );

  report.skippedDuplicate =
    skippedDuplicate.length + result.skippedDuplicate.length;
  report.written = result.writtenCount;
  report.writtenRecords = result.created;
  report.dryRun = false;

  return { ...report, skippedDuplicatePreWrite: skippedDuplicate, createResult: result };
}

export const APPLY_CSV_COLUMNS = [
  "propertyId",
  "propertyUrl",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "rawBrand",
  "matchRecommendedAction",
  "possibleMatchConfidence",
  "candidateDedupeKey",
  "wouldWrite",
  "skipReason",
];

export function selectedRowToCsv(row, apply) {
  return {
    propertyId: row.candidate.sourceRecordId,
    propertyUrl: row.candidate.sourceUrl,
    rawHotelName: row.candidate.rawHotelName,
    rawCity: row.candidate.rawCity,
    rawCountry: row.candidate.rawCountry,
    rawBrand: row.candidate.rawBrand,
    matchRecommendedAction: row.matchRecommendedAction,
    possibleMatchConfidence: row.candidate.possibleMatchConfidence,
    candidateDedupeKey: row.candidate.candidateDedupeKey,
    wouldWrite: apply ? "yes" : "dry-run",
    skipReason: "",
  };
}
