/**
 * Promote Verified rows from a balanced global apply plan (OSM fields only).
 */

import { readFileSync } from "fs";
import {
  VERIFIED_FIELDS,
  VERIFIED_TABLE,
  RECONCILIATION_STATUS,
} from "./fields.js";
import { normalizeKey, normalizeText } from "./normalize-candidate.js";
import { parseCoords } from "./match-current-census.js";
import { normalizeCountry } from "./match-current-census.js";
import { createVerifiedRecords } from "./promote-verified.js";
import { loadVerifiedIndexWithPolicy } from "./verified-dedupe-index.js";

export const BALANCED_BACKWARDS_APPLY_NOTES =
  "Promoted through balanced global backwards-match apply. Verified record populated from OSM candidate fields only. Legacy Hotel Census used only as read-only reconciliation benchmark. No STR/CoStar-derived fields copied.";

export function loadApplyPlanReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const planRows = Array.isArray(data.planRows) ? data.planRows : [];
  if (!planRows.length) {
    throw new Error("Apply plan has no planRows");
  }
  return { data, planRows };
}

/**
 * @param {object} planRow
 * @returns {string[]}
 */
export function validateApplyPlanRow(planRow) {
  const missing = [];
  const p = planRow.proposedVerifiedFields || {};
  if (!normalizeKey(planRow.candidateRecordId)) missing.push("candidateRecordId");
  if (!normalizeKey(p.verifiedHotelName || planRow.osmName)) {
    missing.push("verifiedHotelName");
  }
  if (!normalizeKey(p.verifiedCountry || planRow.osmCountry)) {
    missing.push("verifiedCountry");
  }
  const lat = Number(p.verifiedLatitude ?? planRow.osmLatitude);
  const lng = Number(p.verifiedLongitude ?? planRow.osmLongitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) missing.push("coordinates");
  if (!normalizeKey(p.verifiedDedupeKey || planRow.verifiedDedupeKey)) {
    missing.push("verifiedDedupeKey");
  }
  return missing;
}

/**
 * Map apply-plan row → Verified create payload (OSM / proposedVerifiedFields only).
 */
export function applyPlanRowToVerifiedFields(planRow, opts) {
  const { approvedBy, batchId, approvedAt } = opts;
  const p = planRow.proposedVerifiedFields || {};

  const name = normalizeText(p.verifiedHotelName || planRow.osmName);
  const city = normalizeText(p.verifiedCity || planRow.osmCity);
  const country = normalizeText(p.verifiedCountry || planRow.osmCountry);
  const lat = Number(p.verifiedLatitude ?? planRow.osmLatitude);
  const lng = Number(p.verifiedLongitude ?? planRow.osmLongitude);
  const verifiedDedupeKey = normalizeText(
    p.verifiedDedupeKey || planRow.verifiedDedupeKey
  );

  const notes = [
    BALANCED_BACKWARDS_APPLY_NOTES,
    `Promotion batch: ${batchId}`,
    `Approved by: ${approvedBy}`,
    `Apply plan batch: ${planRow.applyPlanBatchId || batchId}`,
    `Legacy benchmark match: ${planRow.matchedLegacyRecordId || "n/a"} (${planRow.matchedLegacyName || ""})`,
    `Match reason: ${planRow.matchReason || ""}`,
    `Match score: ${planRow.matchScore ?? ""}`,
    planRow.promotionRiskNote ? `Risk note: ${planRow.promotionRiskNote}` : null,
  ]
    .filter(Boolean)
    .join(" ");

  const fields = {
    [VERIFIED_FIELDS.verifiedHotelName]: name,
    [VERIFIED_FIELDS.verifiedCity]: city,
    [VERIFIED_FIELDS.verifiedCountry]: country,
    [VERIFIED_FIELDS.verifiedLatitude]: lat,
    [VERIFIED_FIELDS.verifiedLongitude]: lng,
    [VERIFIED_FIELDS.verifiedDedupeKey]: verifiedDedupeKey,
    [VERIFIED_FIELDS.approvedAt]: approvedAt,
    [VERIFIED_FIELDS.approvedBy]: approvedBy,
    [VERIFIED_FIELDS.approvalNotes]: notes,
    [VERIFIED_FIELDS.censusReconciliationStatus]:
      RECONCILIATION_STATUS.LEGACY_CENSUS_MATCHED_READ_ONLY,
    [VERIFIED_FIELDS.active]: true,
    [VERIFIED_FIELDS.primarySourceCandidate]: [planRow.candidateRecordId],
  };

  const website = normalizeText(p.verifiedWebsite || planRow.osmWebsite);
  if (website && /^https?:\/\//i.test(website)) {
    fields[VERIFIED_FIELDS.verifiedWebsite] = website;
  }
  const phone = normalizeText(p.verifiedPhone);
  if (phone) fields[VERIFIED_FIELDS.verifiedPhone] = phone;
  const brand = normalizeText(p.verifiedBrandLabel);
  if (brand) fields[VERIFIED_FIELDS.verifiedBrandLabel] = brand;

  return {
    fields,
    verifiedDedupeKey,
    candidateAirtableRecordId: planRow.candidateRecordId,
    verifiedHotelName: name,
    osmCountry: country,
    countryNormalized: normalizeCountry(country),
    matchedLegacyRecordId: planRow.matchedLegacyRecordId,
    matchScore: planRow.matchScore,
  };
}

function isNearDuplicate(name, country, coords, geoNameKeys) {
  const nm = normalizeKey(name);
  const co = normalizeKey(country);
  if (!nm || !co || !coords) return false;

  for (const existing of geoNameKeys) {
    if (existing.nm !== nm || existing.co !== co) continue;
    const d = Math.hypot(
      (existing.coords.lat - coords.lat) * 111000,
      (existing.coords.lng - coords.lng) *
        111000 *
        Math.cos((coords.lat * Math.PI) / 180)
    );
    if (d <= 150) return true;
  }
  return false;
}

/**
 * @param {Array<object>} planRows prepared with _approvedBy/_batchId/_approvedAt
 */
export function filterApplyPlanDuplicates(planRows, index) {
  const toWrite = [];
  const skippedDuplicate = [];
  const seenDedupeInBatch = new Set();
  const seenCandidateInBatch = new Set();

  for (const row of planRows) {
    const mapped = applyPlanRowToVerifiedFields(row, {
      approvedBy: row._approvedBy,
      batchId: row._batchId,
      approvedAt: row._approvedAt,
    });

    const dk = normalizeKey(mapped.verifiedDedupeKey);
    if (index.dedupeKeys.has(dk)) {
      skippedDuplicate.push({
        planRow: row,
        reason: "verifiedDedupeKey_existing",
        key: mapped.verifiedDedupeKey,
      });
      continue;
    }
    if (seenDedupeInBatch.has(dk)) {
      skippedDuplicate.push({
        planRow: row,
        reason: "verifiedDedupeKey_batch_duplicate",
        key: mapped.verifiedDedupeKey,
      });
      continue;
    }
    if (
      mapped.candidateAirtableRecordId &&
      index.candidateLinks.has(mapped.candidateAirtableRecordId)
    ) {
      skippedDuplicate.push({
        planRow: row,
        reason: "primarySourceCandidate_existing",
        candidateId: mapped.candidateAirtableRecordId,
      });
      continue;
    }
    if (
      mapped.candidateAirtableRecordId &&
      seenCandidateInBatch.has(mapped.candidateAirtableRecordId)
    ) {
      skippedDuplicate.push({
        planRow: row,
        reason: "primarySourceCandidate_batch_duplicate",
        candidateId: mapped.candidateAirtableRecordId,
      });
      continue;
    }

    const coords = parseCoords(
      row.proposedVerifiedFields?.verifiedLatitude ?? row.osmLatitude,
      row.proposedVerifiedFields?.verifiedLongitude ?? row.osmLongitude
    );
    if (
      isNearDuplicate(
        mapped.verifiedHotelName,
        mapped.osmCountry,
        coords,
        index.geoNameKeys
      )
    ) {
      skippedDuplicate.push({
        planRow: row,
        reason: "nameCountryGeo",
        key: mapped.verifiedDedupeKey,
      });
      continue;
    }

    toWrite.push(mapped);
    seenDedupeInBatch.add(dk);
    if (mapped.candidateAirtableRecordId) {
      seenCandidateInBatch.add(mapped.candidateAirtableRecordId);
    }
  }

  return { toWrite, skippedDuplicate };
}

export function prepareApplyPlanRows(planRows, opts) {
  const prepared = [];
  const skipped = { missingRequired: [], emptyPlan: [] };

  for (const row of planRows) {
    const missing = validateApplyPlanRow(row);
    if (missing.length) {
      skipped.missingRequired.push({ row, missing });
      continue;
    }
    prepared.push({
      ...row,
      applyPlanBatchId: opts.applyPlanBatchId,
      _approvedBy: opts.approvedBy,
      _batchId: opts.promotionBatchId,
      _approvedAt: opts.approvedAt,
    });
  }

  return { prepared, skipped };
}

/**
 * @param {object} opts
 */
export async function runPromoteFromApplyPlan(opts) {
  const apply = !!opts.apply;
  const approvedBy = opts.approvedBy || "";
  const approvedAt = new Date().toISOString();
  const { planRows, data: applyPlanData } = loadApplyPlanReport(opts.applyPlanPath);

  const { prepared, skipped } = prepareApplyPlanRows(planRows, {
    approvedBy: approvedBy || "(dry-run)",
    promotionBatchId: opts.batchId,
    applyPlanBatchId: applyPlanData.batchId,
    approvedAt,
  });

  const base = opts.base;
  if (!base) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
  }

  const loaded = await loadVerifiedIndexWithPolicy(base, {
    apply,
    allowMissingVerifiedIndex: false,
  });
  if (loaded.loadFailed) {
    throw new Error("Verified dedupe index load failed; aborting apply-plan promotion.");
  }

  const index = loaded.index;
  const { toWrite, skippedDuplicate } = filterApplyPlanDuplicates(prepared, index);

  let writtenCount = 0;
  let writtenRecords = [];

  if (apply) {
    if (!approvedBy) {
      throw new Error("--approved-by is required when using --apply");
    }
    const result = await createVerifiedRecords(
      base,
      VERIFIED_TABLE,
      toWrite,
      index.dedupeKeys
    );
    writtenCount = result.writtenCount;
    writtenRecords = result.created;
  }

  const writtenByCountry = {};
  for (const w of toWrite) {
    const co = w.countryNormalized || "(unknown)";
    writtenByCountry[co] = (writtenByCountry[co] || 0) + 1;
  }

  return {
    mode: apply ? "apply" : "dry-run",
    apply,
    promotionBatchId: opts.batchId,
    applyPlanPath: opts.applyPlanPath,
    applyPlanBatchId: applyPlanData.batchId,
    applyPlanCount: planRows.length,
    selectedForPromotion: prepared.length,
    wouldWriteCount: toWrite.length,
    writtenCount,
    skippedDuplicateCount: skippedDuplicate.length,
    skippedMissingRequired: skipped.missingRequired.length,
    writtenByCountry,
    verifiedIndexMeta: loaded.meta,
    verifiedRecordsAfterApplyEstimate: apply
      ? (loaded.meta?.verifiedRecordsLoaded || 0) + writtenCount
      : loaded.meta?.verifiedRecordsLoaded,
    writtenRecords,
    skippedDuplicate,
    toWritePreview: toWrite,
    dryRun: !apply,
    airtableWrites: apply && writtenCount > 0,
    tablesWritten: apply && writtenCount > 0 ? [VERIFIED_TABLE] : [],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    verifiedTableWrites: apply && writtenCount > 0,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export function promoteApplyPlanRowToCsv(row, status) {
  return {
    candidateRecordId: row.candidateAirtableRecordId || row.planRow?.candidateRecordId,
    verifiedHotelName: row.verifiedHotelName,
    verifiedDedupeKey: row.verifiedDedupeKey,
    osmCountry: row.osmCountry || row.countryNormalized,
    matchedLegacyRecordId: row.matchedLegacyRecordId || "",
    matchScore: row.matchScore ?? "",
    status,
  };
}

export const PROMOTE_APPLY_PLAN_CSV_COLUMNS = [
  "candidateRecordId",
  "verifiedHotelName",
  "verifiedDedupeKey",
  "osmCountry",
  "matchedLegacyRecordId",
  "matchScore",
  "status",
];
