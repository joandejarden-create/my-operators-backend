/**
 * Phase Choice-B apply plan — Verified promotion preview from Choice promotion review.
 */

import { readFileSync } from "fs";
import {
  buildBackwardsVerifiedFields,
  buildVerifiedDedupeKeyFromOsm,
} from "./backwards-census-match.js";
import { CHOICE_PROMOTION_BUCKET } from "./choice-promotion-review.js";
import { loadChoiceTargetMatchReport } from "./targeted-osm-lookup.js";
import { loadCandidateRetentionReport } from "./match-brand-directory-properties.js";
import { mapRetentionRowToOsmCandidate } from "./match-choice-targets-to-osm.js";
import {
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
} from "./match-current-census.js";
import { VERIFIED_FIELDS } from "./fields.js";

export function loadChoicePromotionReview(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const reviewRows = Array.isArray(data.reviewRows) ? data.reviewRows : [];
  return { data, reviewRows };
}

function indexMatchRows(matchReport) {
  const byLegacy = new Map();
  for (const r of matchReport.matchRows || []) {
    byLegacy.set(r.legacyRecordId, r);
  }
  return byLegacy;
}

function indexRetentionOsm(retentionPath) {
  const bySource = new Map();
  const byAirtable = new Map();
  if (!retentionPath) return { bySource, byAirtable };
  const retention = loadCandidateRetentionReport(retentionPath);
  for (const row of retention.rows) {
    if (normalizeKey(row.sourceType) !== "osm") continue;
    const c = mapRetentionRowToOsmCandidate(row);
    if (c.sourceRecordId) bySource.set(c.sourceRecordId, c);
    if (c.airtableRecordId) byAirtable.set(c.airtableRecordId, c);
  }
  return { bySource, byAirtable };
}

function resolveOsmCandidate(reviewRow, matchByLegacy, retentionIdx) {
  const match = matchByLegacy.get(reviewRow.legacyRecordId);
  let osm =
    (reviewRow.osmSourceRecordId &&
      retentionIdx.bySource.get(reviewRow.osmSourceRecordId)) ||
    (reviewRow.osmCandidateRecordId &&
      retentionIdx.byAirtable.get(reviewRow.osmCandidateRecordId)) ||
    null;

  if (!osm && match?.osmCandidateRecordId) {
    osm = {
      airtableRecordId: match.osmCandidateRecordId,
      sourceRecordId: match.osmSourceRecordId,
      rawHotelName: match.osmName,
      rawCity: match.osmCity || "",
      rawCountry: match.osmCountry,
      rawLatitude: null,
      rawLongitude: null,
      rawWebsite: match.osmWebsite || "",
      rawPhone: "",
      rawBrand: "",
    };
  }

  return { osm, match };
}

export function isChoiceVerifiedPlanEligible(reviewRow, osm) {
  if (reviewRow.promotionBucket !== CHOICE_PROMOTION_BUCKET.READY) return false;
  if (reviewRow.effectiveMatchConfidence !== "high") return false;
  if (reviewRow.alreadyVerified === "yes" || reviewRow.alreadyVerified === true)
    return false;
  if (reviewRow.duplicateRisk === "yes" || reviewRow.duplicateRisk === true)
    return false;
  if (!osm?.airtableRecordId && !osm?.sourceRecordId) return false;

  const coords = parseCoords(osm.rawLatitude, osm.rawLongitude);
  if (!normalizeKey(osm.rawHotelName)) return false;
  if (!normalizeKey(osm.rawCountry)) return false;
  if (!coords) return false;
  return true;
}

/**
 * @param {object} opts
 */
export function buildChoiceVerifiedApplyPlan(opts) {
  const { reviewRows } = loadChoicePromotionReview(opts.promotionReviewPath);
  const { matchRows } = loadChoiceTargetMatchReport(opts.targetMatchReportPath);
  const matchReport = { matchRows };
  const matchByLegacy = indexMatchRows(matchReport);
  const retentionIdx = indexRetentionOsm(opts.retentionReportPath);

  const batchId = opts.applyPlanBatchId || "choice-verified-apply-plan-001-2026-05-20";
  const approvedByPlaceholder =
    opts.approvedByPlaceholder || "(pending human approval)";

  const eligible = [];
  const skipped = {
    notReadyBucket: 0,
    notHighConfidence: 0,
    alreadyVerified: 0,
    duplicateRisk: 0,
    missingOsmCandidate: 0,
    missingCoreFields: 0,
  };

  for (const reviewRow of reviewRows) {
    if (reviewRow.promotionBucket !== CHOICE_PROMOTION_BUCKET.READY) {
      skipped.notReadyBucket++;
      continue;
    }
    if (reviewRow.effectiveMatchConfidence !== "high") {
      skipped.notHighConfidence++;
      continue;
    }
    if (reviewRow.alreadyVerified === "yes") {
      skipped.alreadyVerified++;
      continue;
    }
    if (reviewRow.duplicateRisk === "yes") {
      skipped.duplicateRisk++;
      continue;
    }

    const { osm, match } = resolveOsmCandidate(
      reviewRow,
      matchByLegacy,
      retentionIdx
    );
    if (!osm) {
      skipped.missingOsmCandidate++;
      continue;
    }
    if (!isChoiceVerifiedPlanEligible(reviewRow, osm)) {
      skipped.missingCoreFields++;
      continue;
    }

    eligible.push({ reviewRow, osm, match: match || {} });
  }

  eligible.sort((a, b) => {
    const co =
      normalizeCountry(a.osm.rawCountry).localeCompare(
        normalizeCountry(b.osm.rawCountry)
      );
    if (co !== 0) return co;
    return (b.match?.matchScore || 0) - (a.match?.matchScore || 0);
  });

  const planRows = eligible.map((item, index) => {
    const { reviewRow, osm, match } = item;
    const matchRow = {
      matchedLegacyRecordId: reviewRow.legacyRecordId,
      matchedLegacyName: reviewRow.legacyHotelName,
      matchReason: match.matchReason || reviewRow.promotionReason,
      matchScore: match.matchScore || 0,
    };
    const mapped = buildBackwardsVerifiedFields(osm, matchRow, {
      approvedBy: approvedByPlaceholder,
      batchId,
      approvedAt: new Date().toISOString(),
    });

    const p = mapped.fields;
    return {
      applyRank: index + 1,
      legacyRecordId: reviewRow.legacyRecordId,
      legacyHotelName: reviewRow.legacyHotelName,
      legacyCountry: reviewRow.legacyCountry,
      targetBrand: reviewRow.targetBrand,
      candidateRecordId: osm.airtableRecordId,
      osmSourceRecordId: osm.sourceRecordId,
      osmName: osm.rawHotelName,
      osmCity: osm.rawCity,
      osmCountry: osm.rawCountry,
      osmLatitude: osm.rawLatitude,
      osmLongitude: osm.rawLongitude,
      matchConfidence: reviewRow.effectiveMatchConfidence,
      distanceMeters: match.distanceMeters,
      matchScore: match.matchScore,
      matchReason: match.matchReason,
      choicePropertyUrlMatch: reviewRow.choicePropertyUrlMatch,
      proposedVerifiedFields: {
        verifiedHotelName: p[VERIFIED_FIELDS.verifiedHotelName] || "",
        verifiedCity: p[VERIFIED_FIELDS.verifiedCity] || "",
        verifiedCountry: p[VERIFIED_FIELDS.verifiedCountry] || "",
        verifiedLatitude: p[VERIFIED_FIELDS.verifiedLatitude] ?? "",
        verifiedLongitude: p[VERIFIED_FIELDS.verifiedLongitude] ?? "",
        verifiedDedupeKey: mapped.verifiedDedupeKey,
        primarySourceCandidate: mapped.candidateAirtableRecordId,
      },
      promotionRiskNote: reviewRow.promotionReason,
      verifiedDedupeKey: buildVerifiedDedupeKeyFromOsm(osm),
      countryNormalized: normalizeCountry(osm.rawCountry) || "",
    };
  });

  const byCountry = {};
  const byBrand = {};
  for (const r of planRows) {
    const co = r.countryNormalized || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
    const br = r.targetBrand || "(unknown)";
    byBrand[br] = (byBrand[br] || 0) + 1;
  }

  return {
    batchId,
    sourcePromotionReview: opts.promotionReviewPath,
    sourceTargetMatchReport: opts.targetMatchReportPath,
    choiceTargetCount: reviewRows.length,
    readyBucketCount: reviewRows.filter(
      (r) => r.promotionBucket === CHOICE_PROMOTION_BUCKET.READY
    ).length,
    applyPlanCount: planRows.length,
    skipped,
    byCountry,
    byBrand,
    planRows,
    dryRun: true,
    verifiedTableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export const CHOICE_APPLY_PLAN_CSV_COLUMNS = [
  "applyRank",
  "legacyRecordId",
  "legacyHotelName",
  "targetBrand",
  "candidateRecordId",
  "osmSourceRecordId",
  "osmName",
  "osmCountry",
  "osmLatitude",
  "osmLongitude",
  "matchScore",
  "matchReason",
  "verifiedHotelName",
  "verifiedCountry",
  "verifiedLatitude",
  "verifiedLongitude",
  "verifiedDedupeKey",
  "promotionRiskNote",
];

export function choiceApplyPlanRowToCsv(row) {
  const p = row.proposedVerifiedFields || {};
  return {
    applyRank: row.applyRank,
    legacyRecordId: row.legacyRecordId,
    legacyHotelName: row.legacyHotelName,
    targetBrand: row.targetBrand,
    candidateRecordId: row.candidateRecordId,
    osmSourceRecordId: row.osmSourceRecordId,
    osmName: row.osmName,
    osmCountry: row.osmCountry,
    osmLatitude: row.osmLatitude,
    osmLongitude: row.osmLongitude,
    matchScore: row.matchScore ?? "",
    matchReason: row.matchReason,
    verifiedHotelName: p.verifiedHotelName,
    verifiedCountry: p.verifiedCountry,
    verifiedLatitude: p.verifiedLatitude,
    verifiedLongitude: p.verifiedLongitude,
    verifiedDedupeKey: p.verifiedDedupeKey,
    promotionRiskNote: row.promotionRiskNote,
  };
}
