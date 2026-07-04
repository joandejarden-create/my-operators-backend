/**
 * Phase 4D — Manual follow-up classification for review_before_promote rows.
 * Report-only; no Airtable writes.
 */

import {
  PROMOTION_ELIGIBILITY,
  PROMOTION_RECOMMENDATION,
} from "./promotion-review.js";
import {
  nameSimilarity,
  websiteHost,
  normalizeText,
  normalizeKey,
} from "./match-current-census.js";

export const MANUAL_REVIEW_REASONS = {
  MISSING_CITY: "missing_city",
  NAME_MISMATCH: "name_mismatch",
  WEBSITE_MISMATCH: "website_mismatch",
  OPERATOR_OR_BRAND_MISMATCH: "operator_or_brand_mismatch",
  WEAK_MATCH_SCORE: "weak_match_score",
  INSUFFICIENT_SOURCE_DETAIL: "insufficient_source_detail",
  OTHER: "other",
};

export const REVIEW_PRIORITY = {
  HIGH: "high",
  MEDIUM: "medium",
  LOW: "low",
};

export const SUGGESTED_NEXT_ACTION = {
  ADD_CITY: "add_city_from_manual_review",
  VERIFY_WEBSITE: "verify_official_website",
  VERIFY_BRAND: "verify_brand_or_operator",
  COMPARE_NAMES: "compare_names_manually",
  BRAND_DIRECTORY: "hold_for_brand_directory_validation",
  DO_NOT_PROMOTE: "do_not_promote_yet",
};

const STRONG_MATCH_SCORE = 65;
const NAME_MISMATCH_THRESHOLD = 0.55;

/**
 * @param {Array<object>} reviewRows
 */
export function selectManualFollowupRows(reviewRows) {
  const selected = [];
  const skipped = [];

  for (const row of reviewRows) {
    const okRec =
      row.promotionRecommendation === PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
    const okElig =
      row.promotionEligibility === PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;

    if (okRec && okElig) {
      selected.push(row);
    } else {
      skipped.push({
        candidateAirtableRecordId: row.candidateAirtableRecordId,
        promotionRecommendation: row.promotionRecommendation,
        promotionEligibility: row.promotionEligibility,
      });
    }
  }

  return { selected, skipped };
}

function detectFlags(row) {
  const notes = String(row.humanReviewNotes || "").toLowerCase();
  const name = normalizeText(row.candidateHotelName);
  const wdName = normalizeText(row.wikidataHotelName);
  const city = normalizeKey(row.candidateCity);
  const country = normalizeKey(row.candidateCountry);
  const lat = row.candidateLatitude;
  const lng = row.candidateLongitude;
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const hasName = !!normalizeKey(name);
  const hasWebsite = !!normalizeKey(row.candidateWebsite);
  const hasWdWebsite = !!normalizeKey(row.wikidataWebsite);
  const matchScore = Number(row.matchScore) || 0;

  const ns = nameSimilarity(name, wdName);
  const candHost = websiteHost(row.candidateWebsite);
  const wdHost = websiteHost(row.wikidataWebsite);
  const websiteConflict = !!(candHost && wdHost && candHost !== wdHost);

  const missingCity = !city;
  const nameMismatch =
    !!normalizeKey(wdName) && ns < NAME_MISMATCH_THRESHOLD;
  const weakMatch =
    matchScore < STRONG_MATCH_SCORE ||
    notes.includes("below strong threshold") ||
    notes.includes("match score");

  let operatorBrandMismatch = false;
  if (notes.includes("operator") && notes.includes("brand")) {
    operatorBrandMismatch = true;
  } else if (row.candidateBrand && row.wikidataOperator) {
    const op = normalizeKey(row.wikidataOperator);
    const br = normalizeKey(row.candidateBrand);
    if (
      op &&
      br &&
      !op.includes(br) &&
      !br.includes(op) &&
      nameSimilarity(op, br) < 0.4
    ) {
      operatorBrandMismatch = true;
    }
  }

  const insufficientDetail =
    !hasWdWebsite &&
    !hasWebsite &&
    (!hasName || !normalizeKey(wdName)) &&
    !missingCity;

  if (notes.includes("missing phase 4a")) {
    return {
      missingCity,
      nameMismatch: true,
      websiteConflict: true,
      operatorBrandMismatch,
      weakMatch: true,
      insufficientDetail: true,
      hasCoords,
      hasName,
      hasWebsite,
      ns,
      matchScore,
    };
  }

  return {
    missingCity,
    nameMismatch,
    websiteConflict,
    operatorBrandMismatch,
    weakMatch,
    insufficientDetail,
    hasCoords,
    hasName,
    hasWebsite,
    ns,
    matchScore,
  };
}

/**
 * @returns {{ reasons: string[], primaryReason: string }}
 */
export function classifyManualReviewReasons(row) {
  const f = detectFlags(row);
  const reasons = [];

  if (f.missingCity) reasons.push(MANUAL_REVIEW_REASONS.MISSING_CITY);
  if (f.nameMismatch) reasons.push(MANUAL_REVIEW_REASONS.NAME_MISMATCH);
  if (f.websiteConflict) reasons.push(MANUAL_REVIEW_REASONS.WEBSITE_MISMATCH);
  if (f.operatorBrandMismatch) {
    reasons.push(MANUAL_REVIEW_REASONS.OPERATOR_OR_BRAND_MISMATCH);
  }
  if (f.weakMatch) reasons.push(MANUAL_REVIEW_REASONS.WEAK_MATCH_SCORE);
  if (f.insufficientDetail) {
    reasons.push(MANUAL_REVIEW_REASONS.INSUFFICIENT_SOURCE_DETAIL);
  }

  if (!reasons.length) {
    const notes = String(row.humanReviewNotes || "");
    if (notes.includes("city missing")) reasons.push(MANUAL_REVIEW_REASONS.MISSING_CITY);
    else if (notes.includes("label differs")) {
      reasons.push(MANUAL_REVIEW_REASONS.NAME_MISMATCH);
    } else if (notes.includes("website host mismatch")) {
      reasons.push(MANUAL_REVIEW_REASONS.WEBSITE_MISMATCH);
    } else if (notes.includes("match score")) {
      reasons.push(MANUAL_REVIEW_REASONS.WEAK_MATCH_SCORE);
    } else reasons.push(MANUAL_REVIEW_REASONS.OTHER);
  }

  const priorityOrder = [
    MANUAL_REVIEW_REASONS.NAME_MISMATCH,
    MANUAL_REVIEW_REASONS.WEBSITE_MISMATCH,
    MANUAL_REVIEW_REASONS.MISSING_CITY,
    MANUAL_REVIEW_REASONS.OPERATOR_OR_BRAND_MISMATCH,
    MANUAL_REVIEW_REASONS.WEAK_MATCH_SCORE,
    MANUAL_REVIEW_REASONS.INSUFFICIENT_SOURCE_DETAIL,
    MANUAL_REVIEW_REASONS.OTHER,
  ];
  const primaryReason =
    priorityOrder.find((r) => reasons.includes(r)) || MANUAL_REVIEW_REASONS.OTHER;

  return { reasons, primaryReason, flags: f };
}

export function computeReviewPriority(row, flags) {
  const conflictCount = [
    flags.nameMismatch,
    flags.websiteConflict,
    flags.operatorBrandMismatch,
    flags.weakMatch,
    flags.insufficientDetail,
  ].filter(Boolean).length;

  const onlyCityMissing =
    flags.missingCity &&
    flags.hasName &&
    flags.hasCoords &&
    flags.hasWebsite &&
    !flags.nameMismatch &&
    !flags.websiteConflict &&
    conflictCount <= 1;

  if (onlyCityMissing) {
    return REVIEW_PRIORITY.HIGH;
  }

  if (conflictCount >= 2) {
    return REVIEW_PRIORITY.LOW;
  }

  const strongNameGeo =
    flags.hasName &&
    flags.hasCoords &&
    (flags.ns >= NAME_MISMATCH_THRESHOLD || !normalizeKey(row.wikidataHotelName));

  if (
    strongNameGeo &&
    (flags.websiteConflict || flags.nameMismatch || flags.missingCity)
  ) {
    return REVIEW_PRIORITY.MEDIUM;
  }

  if (flags.weakMatch || flags.insufficientDetail) {
    return REVIEW_PRIORITY.LOW;
  }

  return REVIEW_PRIORITY.MEDIUM;
}

export function suggestNextAction(row, flags, reasons, priority) {
  if (priority === REVIEW_PRIORITY.LOW && reasons.length >= 3) {
    return SUGGESTED_NEXT_ACTION.DO_NOT_PROMOTE;
  }

  if (flags.missingCity && flags.hasName && flags.hasCoords) {
    if (flags.websiteConflict) return SUGGESTED_NEXT_ACTION.VERIFY_WEBSITE;
    if (flags.nameMismatch) return SUGGESTED_NEXT_ACTION.COMPARE_NAMES;
    return SUGGESTED_NEXT_ACTION.ADD_CITY;
  }

  if (flags.websiteConflict) return SUGGESTED_NEXT_ACTION.VERIFY_WEBSITE;
  if (flags.nameMismatch) return SUGGESTED_NEXT_ACTION.COMPARE_NAMES;
  if (flags.operatorBrandMismatch) return SUGGESTED_NEXT_ACTION.VERIFY_BRAND;
  if (flags.weakMatch && !flags.hasWebsite) {
    return SUGGESTED_NEXT_ACTION.HOLD_FOR_BRAND_DIRECTORY;
  }
  if (flags.insufficientDetail) {
    return SUGGESTED_NEXT_ACTION.HOLD_FOR_BRAND_DIRECTORY;
  }
  if (flags.weakMatch) return SUGGESTED_NEXT_ACTION.COMPARE_NAMES;

  return SUGGESTED_NEXT_ACTION.VERIFY_WEBSITE;
}

/**
 * @param {object} row Phase 4B review row
 */
export function buildManualFollowupRow(row) {
  const { reasons, primaryReason, flags } = classifyManualReviewReasons(row);
  const reviewPriority = computeReviewPriority(row, flags);
  const suggestedNextAction = suggestNextAction(row, flags, reasons, reviewPriority);

  return {
    candidateAirtableRecordId: row.candidateAirtableRecordId,
    candidateHotelName: row.candidateHotelName,
    wikidataHotelName: row.wikidataHotelName,
    candidateCity: row.candidateCity,
    candidateCountry: row.candidateCountry,
    candidateLatitude: row.candidateLatitude,
    candidateLongitude: row.candidateLongitude,
    candidateWebsite: row.candidateWebsite,
    wikidataWebsite: row.wikidataWebsite,
    wikidataQid: row.wikidataQid,
    wikidataUrl: row.wikidataUrl,
    matchScore: row.matchScore,
    matchReason: row.matchReason,
    manualReviewReason: primaryReason,
    manualReviewReasons: reasons,
    reviewPriority,
    suggestedNextAction,
    humanNotes: "",
    readyForFuturePromotion: "",
    promotionEligibility: row.promotionEligibility,
    promotionRecommendation: row.promotionRecommendation,
    humanReviewNotesFrom4B: row.humanReviewNotes,
    nameSimilarity: flags.ns != null ? Number(flags.ns.toFixed(3)) : "",
  };
}

export function summarizeManualFollowup(rows) {
  const summary = {
    total: rows.length,
    priorityHigh: 0,
    priorityMedium: 0,
    priorityLow: 0,
    byPrimaryManualReviewReason: {},
    byAllManualReviewReasons: {},
    bySuggestedNextAction: {},
  };

  for (const r of rows) {
    if (r.reviewPriority === REVIEW_PRIORITY.HIGH) summary.priorityHigh++;
    else if (r.reviewPriority === REVIEW_PRIORITY.MEDIUM) summary.priorityMedium++;
    else if (r.reviewPriority === REVIEW_PRIORITY.LOW) summary.priorityLow++;

    summary.byPrimaryManualReviewReason[r.manualReviewReason] =
      (summary.byPrimaryManualReviewReason[r.manualReviewReason] || 0) + 1;

    for (const reason of r.manualReviewReasons || []) {
      summary.byAllManualReviewReasons[reason] =
        (summary.byAllManualReviewReasons[reason] || 0) + 1;
    }

    summary.bySuggestedNextAction[r.suggestedNextAction] =
      (summary.bySuggestedNextAction[r.suggestedNextAction] || 0) + 1;
  }

  return summary;
}
