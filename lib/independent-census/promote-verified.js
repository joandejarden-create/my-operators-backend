/**
 * Phase 4C — Gated promotion to Verified Independent Hotel Census.
 */

import {
  VERIFIED_FIELDS,
  VERIFIED_TABLE,
  RECONCILIATION_STATUS,
} from "./fields.js";
import {
  PROMOTION_ELIGIBILITY,
  PROMOTION_RECOMMENDATION,
} from "./promotion-review.js";
import { computeCandidateDedupeKey, normalizeKey, normalizeText } from "./normalize-candidate.js";
import { parseCoords } from "./match-current-census.js";

export function buildVerifiedDedupeKey(name, city, country, lat, lng) {
  return computeCandidateDedupeKey(name, city, country, lat, lng);
}

/**
 * @param {object} reviewRow
 */
export function selectPromotableReviewRows(reviewRows, options = {}) {
  const selected = [];
  const skipped = {
    notPromoteAfterReview: [],
    notEligibleForReview: [],
    missingRequiredFields: [],
    overMaxRecords: [],
    candidateRecordMismatch: [],
    propertyIdMismatch: [],
  };

  const maxRecords = options.maxRecords ?? null;
  const allowReviewBeforePromote = !!options.allowReviewBeforePromote;
  const candidateRecordId = options.candidateRecordId || "";
  const requirePropertyIdMatch = !!options.requirePropertyIdMatchOnOsmWebsite;
  let promoteCount = 0;

  for (const row of reviewRows) {
    if (
      candidateRecordId &&
      row.candidateAirtableRecordId !== candidateRecordId
    ) {
      skipped.candidateRecordMismatch.push(row);
      continue;
    }

    const recommendationOk =
      row.promotionRecommendation === PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW ||
      (allowReviewBeforePromote &&
        row.promotionRecommendation === PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE);

    if (!recommendationOk) {
      skipped.notPromoteAfterReview.push(row);
      continue;
    }
    if (row.promotionEligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW) {
      skipped.notEligibleForReview.push(row);
      continue;
    }

    if (requirePropertyIdMatch && row.propertyIdMatchesOsmWebsite === false) {
      skipped.propertyIdMismatch.push(row);
      continue;
    }

    const missing = validateRequiredPromotionFields(row);
    if (missing.length) {
      skipped.missingRequiredFields.push({ row, missing });
      continue;
    }

    if (maxRecords != null && promoteCount >= maxRecords) {
      skipped.overMaxRecords.push(row);
      continue;
    }

    selected.push(row);
    promoteCount++;
  }

  return { selected, skipped };
}

/**
 * @param {object} row
 * @returns {string[]}
 */
export function validateRequiredPromotionFields(row) {
  const p = row.proposedVerified || {};
  const missing = [];

  const name = normalizeText(p.verifiedHotelName || row.candidateHotelName);
  const country = normalizeText(p.verifiedCountry || row.candidateCountry);
  const lat = p.verifiedLatitude ?? row.candidateLatitude;
  const lng = p.verifiedLongitude ?? row.candidateLongitude;
  const primaryUrl =
    normalizeText(
      p.primarySourceUrl || row.choicePropertyUrl || row.osmSourceUrl
    ) || "";

  if (!normalizeKey(name)) missing.push("verifiedHotelName");
  if (!normalizeKey(country)) missing.push("verifiedCountry");
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) missing.push("coordinates");
  if (!primaryUrl || !/^https?:\/\//i.test(primaryUrl)) {
    missing.push("primarySourceUrl");
  }

  return missing;
}

export function buildApprovalNotes(row, approvedBy, batchId, customNote = "") {
  if (normalizeText(customNote)) {
    const parts = [
      normalizeText(customNote),
      `Promotion batch: ${batchId}`,
      `Approved by: ${approvedBy}`,
      row.choicePropertyId ? `Choice property ID: ${row.choicePropertyId}` : null,
      row.candidateAirtableRecordId
        ? `Primary source candidate: ${row.candidateAirtableRecordId}`
        : null,
      row.matchReason ? `Match reason: ${row.matchReason}` : null,
      row.humanReviewNotes ? `Review notes: ${row.humanReviewNotes}` : null,
    ];
    return parts.filter(Boolean).join(" ");
  }

  if (row.choicePropertyId || row.priorPhase4QEvidenceIgnored) {
    const parts = [
      "Human-approved Phase 4W promotion from corrected Choice property ID evidence (Phase 4U).",
      `Promotion batch: ${batchId}`,
      `Approved by: ${approvedBy}`,
      `Choice property ID: ${row.choicePropertyId || ""}`,
      `Choice URL: ${row.choicePropertyUrl || row.proposedVerified?.primarySourceUrl || ""}`,
      `OSM candidate: ${row.candidateAirtableRecordId || ""}`,
      `Property ID matches OSM website: ${row.propertyIdMatchesOsmWebsite ? "yes" : "no"}`,
      `Evidence count: ${row.evidenceCount ?? 1}`,
      `Match score: ${row.matchScore ?? ""}`,
      row.humanReviewNotes ? `Review notes: ${row.humanReviewNotes}` : null,
    ];
    return parts.filter(Boolean).join(" ");
  }

  const parts = [
    "Promoted from Phase 4B OSM + Wikidata two-source validation review.",
    `Promotion batch: ${batchId}`,
    `Approved by: ${approvedBy}`,
    `Wikidata QID: ${row.wikidataQid || ""}`,
    `Evidence count: ${row.evidenceCount ?? 1}`,
    `Match score: ${row.matchScore ?? ""}`,
    `Match reason: ${row.matchReason || ""}`,
    row.humanReviewNotes ? `Review notes: ${row.humanReviewNotes}` : null,
  ];
  return parts.filter(Boolean).join(" ");
}

/**
 * Map Phase 4B review row → existing Verified table fields only.
 */
export function reviewRowToVerifiedFields(row, opts) {
  const { approvedBy, batchId, approvedAt, approvalNote = "" } = opts;
  const p = row.proposedVerified || {};
  const name = normalizeText(p.verifiedHotelName || row.candidateHotelName);
  const city = normalizeText(p.verifiedCity || row.candidateCity);
  const country = normalizeText(p.verifiedCountry || row.candidateCountry);
  const lat = Number(p.verifiedLatitude ?? row.candidateLatitude);
  const lng = Number(p.verifiedLongitude ?? row.candidateLongitude);
  const website = normalizeText(
    p.verifiedWebsite || row.choicePropertyUrl || row.candidateWebsite
  );
  const phone = normalizeText(p.verifiedPhone || row.candidatePhone);
  const brand = normalizeText(
    p.verifiedBrandLabel || row.choiceBrandSetupBrand || row.candidateBrand || ""
  );
  const primaryUrl = normalizeText(
    p.primarySourceUrl || row.choicePropertyUrl || row.osmSourceUrl
  );

  const verifiedDedupeKey = buildVerifiedDedupeKey(name, city, country, lat, lng);

  const fields = {
    [VERIFIED_FIELDS.verifiedHotelName]: name,
    [VERIFIED_FIELDS.verifiedCity]: city,
    [VERIFIED_FIELDS.verifiedCountry]: country,
    [VERIFIED_FIELDS.verifiedLatitude]: lat,
    [VERIFIED_FIELDS.verifiedLongitude]: lng,
    [VERIFIED_FIELDS.verifiedDedupeKey]: verifiedDedupeKey,
    [VERIFIED_FIELDS.approvedAt]: approvedAt,
    [VERIFIED_FIELDS.approvedBy]: approvedBy,
    [VERIFIED_FIELDS.approvalNotes]: buildApprovalNotes(
      row,
      approvedBy,
      batchId,
      approvalNote
    ),
    [VERIFIED_FIELDS.censusReconciliationStatus]: RECONCILIATION_STATUS.NOT_IN_CENSUS,
    [VERIFIED_FIELDS.active]: true,
  };

  if (website && /^https?:\/\//i.test(website)) {
    fields[VERIFIED_FIELDS.verifiedWebsite] = website;
  }
  if (phone) fields[VERIFIED_FIELDS.verifiedPhone] = phone;
  if (brand) fields[VERIFIED_FIELDS.verifiedBrandLabel] = brand;

  if (row.candidateAirtableRecordId) {
    fields[VERIFIED_FIELDS.primarySourceCandidate] = [row.candidateAirtableRecordId];
  }

  return {
    fields,
    verifiedDedupeKey,
    candidateAirtableRecordId: row.candidateAirtableRecordId,
    wikidataQid: row.wikidataQid,
    choicePropertyId: row.choicePropertyId || p.choicePropertyId,
    verifiedHotelName: name,
  };
}

/**
 * @param {import('airtable').Base} base
 */
export async function loadExistingVerifiedDedupeIndex(base, tableName = VERIFIED_TABLE, opts = {}) {
  const { loadVerifiedDedupeIndexRobust } = await import("./verified-dedupe-index.js");
  const loaded = await loadVerifiedDedupeIndexRobust(base, tableName, opts);
  return {
    dedupeKeys: loaded.dedupeKeys,
    candidateLinks: loaded.candidateLinks,
    geoNameKeys: loaded.geoNameKeys,
    meta: loaded.meta,
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
 * @param {Array<object>} selectedRows
 * @param {ReturnType<loadExistingVerifiedDedupeIndex>} index
 */
export function filterDuplicates(selectedRows, index) {
  const toWrite = [];
  const skippedDuplicate = [];

  for (const row of selectedRows) {
    const mapped = reviewRowToVerifiedFields(row, {
      approvedBy: row._approvedBy,
      batchId: row._batchId,
      approvedAt: row._approvedAt,
      approvalNote: row._approvalNote,
    });

    if (index.dedupeKeys.has(normalizeKey(mapped.verifiedDedupeKey))) {
      skippedDuplicate.push({ row, reason: "verifiedDedupeKey", key: mapped.verifiedDedupeKey });
      continue;
    }
    if (
      mapped.candidateAirtableRecordId &&
      index.candidateLinks.has(mapped.candidateAirtableRecordId)
    ) {
      skippedDuplicate.push({
        row,
        reason: "primarySourceCandidate",
        candidateId: mapped.candidateAirtableRecordId,
      });
      continue;
    }

    const coords = parseCoords(
      row.proposedVerified?.verifiedLatitude ?? row.candidateLatitude,
      row.proposedVerified?.verifiedLongitude ?? row.candidateLongitude
    );
    if (
      isNearDuplicate(
        mapped.verifiedHotelName,
        row.proposedVerified?.verifiedCountry || row.candidateCountry,
        coords,
        index.geoNameKeys
      )
    ) {
      skippedDuplicate.push({ row, reason: "nameCountryGeo", key: mapped.verifiedDedupeKey });
      continue;
    }

    toWrite.push(mapped);
  }

  return { toWrite, skippedDuplicate };
}

const CREATE_CHUNK = 10;

/**
 * @param {import('airtable').Base} base
 * @param {string} tableName
 * @param {Array<ReturnType<reviewRowToVerifiedFields>>} rows
 * @param {Set<string>} dedupeKeys mutating index
 */
export async function createVerifiedRecords(base, tableName, rows, dedupeKeys) {
  const created = [];

  for (let i = 0; i < rows.length; i += CREATE_CHUNK) {
    const chunk = rows.slice(i, i + CREATE_CHUNK);
    const payload = chunk.map((r) => ({ fields: r.fields }));
    const records = await base(tableName).create(payload, { typecast: true });
    for (const rec of records) {
      const dk = rec.fields[VERIFIED_FIELDS.verifiedDedupeKey];
      if (dk) dedupeKeys.add(normalizeKey(dk));
      created.push({
        airtableRecordId: rec.id,
        verifiedHotelName: rec.fields[VERIFIED_FIELDS.verifiedHotelName],
        verifiedDedupeKey: dk,
        wikidataQid: chunk.find(
          (c) => c.verifiedHotelName === rec.fields[VERIFIED_FIELDS.verifiedHotelName]
        )?.wikidataQid,
      });
    }
  }

  return { created, writtenCount: created.length };
}
