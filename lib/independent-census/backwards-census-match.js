/**
 * Backwards-match workflow: OSM staging candidates → read-only legacy Hotel Census benchmark
 * → gated promotion to Verified Independent Hotel Census (OSM fields only).
 */

import {
  SOURCE_TYPES,
  VERIFIED_TABLE,
  VERIFIED_FIELDS,
  RECONCILIATION_STATUS,
} from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import {
  loadHotelCensusReadOnly,
  scoreCandidateAgainstCensus,
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
} from "./match-current-census.js";
import {
  loadCandidateRetentionReport,
  buildIncludedCandidateIds,
  parseRetentionIncludeList,
  DEFAULT_EXCLUDED_RETENTION,
} from "./match-brand-directory-properties.js";
import { computeCandidateDedupeKey } from "./normalize-candidate.js";
import { createVerifiedRecords } from "./promote-verified.js";
import {
  loadVerifiedIndexWithPolicy,
  logVerifiedIndexMeta,
} from "./verified-dedupe-index.js";

export const PROMOTION_RECOMMENDATION = {
  PROMOTE_AFTER_REVIEW: "promote_after_review",
  REVIEW_BEFORE_PROMOTE: "review_before_promote",
  HOLD_DUPLICATE: "hold_duplicate",
  HOLD_LOW_CONFIDENCE: "hold_low_confidence",
  ALREADY_VERIFIED: "already_verified",
};

export const PROMOTION_ELIGIBILITY = {
  ELIGIBLE: "eligible_for_promotion",
  NOT_ELIGIBLE: "not_eligible",
};

const STRONG_NAME_SIM = 0.85;
const GOOD_NAME_SIM = 0.7;
const PARTIAL_NAME_SIM = 0.5;
const PROMOTE_SCORE_THRESHOLD = 65;

const APPROVAL_NOTES_TEMPLATE =
  "Promoted through backwards-match workflow. Verified record populated from OSM candidate fields only. Legacy Hotel Census used only as read-only reconciliation benchmark; no STR/CoStar-derived fields copied.";

/**
 * @param {object} scored — from scoreCandidateAgainstCensus
 * @param {{ alternateHighCount?: number }} ctx
 */
export function assignBackwardsMatchConfidence(scored, ctx = {}) {
  const { nameSim, countryOk, distanceMeters, websiteMatch, phoneMatch } = scored;
  const censusCountry = scored.census?.country;

  if (!countryOk && normalizeCountry(censusCountry)) {
    return { confidence: "none", reason: "Country does not match legacy census row" };
  }

  if (phoneMatch && countryOk) {
    return {
      confidence: "high",
      reason: "Exact phone match, same country (legacy census benchmark)",
    };
  }

  if (websiteMatch && countryOk) {
    return {
      confidence: "high",
      reason: "Exact website host match, same country (legacy census benchmark)",
    };
  }

  if (
    nameSim >= STRONG_NAME_SIM &&
    countryOk &&
    distanceMeters != null &&
    distanceMeters <= 250 &&
    (ctx.alternateHighCount || 0) === 0
  ) {
    return {
      confidence: "high",
      reason: `Strong name similarity (${(nameSim * 100).toFixed(0)}%), same country, ${distanceMeters}m apart`,
    };
  }

  if (
    nameSim >= GOOD_NAME_SIM &&
    countryOk &&
    distanceMeters != null &&
    distanceMeters <= 750
  ) {
    return {
      confidence: "medium",
      reason: `Good name similarity (${(nameSim * 100).toFixed(0)}%), same country, ${distanceMeters}m apart`,
    };
  }

  if (
    nameSim >= PARTIAL_NAME_SIM &&
    countryOk &&
    distanceMeters != null &&
    distanceMeters <= 2000
  ) {
    return {
      confidence: "low",
      reason: `Partial name similarity (${(nameSim * 100).toFixed(0)}%), same country, within ${distanceMeters}m`,
    };
  }

  return { confidence: "none", reason: "No credible backwards match to legacy census" };
}

/**
 * Match one OSM candidate to best legacy census row (same-country pool).
 */
export function matchOsmCandidateBackwards(candidate, censusData) {
  const countryNorm = normalizeCountry(candidate.rawCountry);
  let pool = censusData.rows;
  if (countryNorm) {
    pool = censusData.byCountry.get(countryNorm) || [];
    if (!pool.length) pool = censusData.rows;
  }

  const scoredList = pool
    .map((census) => scoreCandidateAgainstCensus(candidate, census))
    .filter((s) => s.nameSim >= 0.35 || s.websiteMatch || s.phoneMatch)
    .sort((a, b) => b.score - a.score);

  if (!scoredList.length) {
    return {
      matchConfidence: "none",
      matchScore: 0,
      matchReason: "No legacy census row with sufficient similarity",
      matchedLegacyRecordId: "",
      matchedLegacyName: "",
      matchedLegacyCity: "",
      matchedLegacyCountry: "",
      distanceMeters: null,
      _scoredBest: null,
      _alternates: [],
    };
  }

  const best = scoredList[0];
  const alternates = scoredList.slice(1, 6);

  const { confidence, reason } = assignBackwardsMatchConfidence(best, {
    alternateHighCount: 0,
  });

  return {
    matchConfidence: confidence,
    matchScore: best.score,
    matchReason: reason,
    matchedLegacyRecordId: confidence === "none" ? "" : best.census.recordId,
    matchedLegacyName: confidence === "none" ? "" : best.census.name,
    matchedLegacyCity: confidence === "none" ? "" : best.census.city,
    matchedLegacyCountry: confidence === "none" ? "" : best.census.country,
    distanceMeters: best.distanceMeters,
    _scoredBest: best,
    _alternates: alternates,
  };
}

/**
 * Flag OSM rows sharing the same high-confidence legacy match.
 */
export function markLegacyDuplicateClusters(rows) {
  const byLegacy = new Map();
  for (const row of rows) {
    if (row.matchConfidence !== "high" || !row.matchedLegacyRecordId) continue;
    const lid = row.matchedLegacyRecordId;
    if (!byLegacy.has(lid)) byLegacy.set(lid, []);
    byLegacy.get(lid).push(row.osmCandidateRecordId);
  }

  const duplicateOsmIds = new Set();
  for (const [, ids] of byLegacy) {
    if (ids.length > 1) {
      for (const id of ids) duplicateOsmIds.add(id);
    }
  }

  return duplicateOsmIds;
}

export function buildVerifiedDedupeKeyFromOsm(candidate) {
  return computeCandidateDedupeKey(
    candidate.rawHotelName,
    candidate.rawCity,
    candidate.rawCountry,
    candidate.rawLatitude,
    candidate.rawLongitude
  );
}

/**
 * @param {object} candidate — mapped Airtable candidate
 * @param {object} matchRow — report row
 */
export function buildBackwardsVerifiedFields(candidate, matchRow, opts) {
  const { approvedBy, batchId, approvedAt } = opts;
  const lat = Number(candidate.rawLatitude);
  const lng = Number(candidate.rawLongitude);
  const verifiedDedupeKey = buildVerifiedDedupeKeyFromOsm(candidate);

  const notes = [
    APPROVAL_NOTES_TEMPLATE,
    `Promotion batch: ${batchId}`,
    `Approved by: ${approvedBy}`,
    `Legacy benchmark match: ${matchRow.matchedLegacyRecordId || "n/a"} (${matchRow.matchedLegacyName || ""})`,
    `Match reason: ${matchRow.matchReason || ""}`,
    `Match score: ${matchRow.matchScore ?? ""}`,
  ].join(" ");

  const fields = {
    [VERIFIED_FIELDS.verifiedHotelName]: normalizeText(candidate.rawHotelName),
    [VERIFIED_FIELDS.verifiedCity]: normalizeText(candidate.rawCity),
    [VERIFIED_FIELDS.verifiedCountry]: normalizeText(candidate.rawCountry),
    [VERIFIED_FIELDS.verifiedLatitude]: lat,
    [VERIFIED_FIELDS.verifiedLongitude]: lng,
    [VERIFIED_FIELDS.verifiedDedupeKey]: verifiedDedupeKey,
    [VERIFIED_FIELDS.approvedAt]: approvedAt,
    [VERIFIED_FIELDS.approvedBy]: approvedBy,
    [VERIFIED_FIELDS.approvalNotes]: notes,
    [VERIFIED_FIELDS.censusReconciliationStatus]:
      RECONCILIATION_STATUS.LEGACY_CENSUS_MATCHED_READ_ONLY,
    [VERIFIED_FIELDS.active]: true,
    [VERIFIED_FIELDS.primarySourceCandidate]: [candidate.airtableRecordId],
  };

  const website = normalizeText(candidate.rawWebsite);
  if (website && /^https?:\/\//i.test(website)) {
    fields[VERIFIED_FIELDS.verifiedWebsite] = website;
  }
  const phone = normalizeText(candidate.rawPhone);
  if (phone) fields[VERIFIED_FIELDS.verifiedPhone] = phone;
  const brand = normalizeText(candidate.rawBrand);
  if (brand) fields[VERIFIED_FIELDS.verifiedBrandLabel] = brand;

  return {
    fields,
    verifiedDedupeKey,
    candidateAirtableRecordId: candidate.airtableRecordId,
    verifiedHotelName: fields[VERIFIED_FIELDS.verifiedHotelName],
  };
}

/** Strict promotion signals: website/phone match OR strong name within 250m (same country). */
export function meetsStrictBackwardsPromotionSignals(row) {
  if (row.websiteMatch || row.phoneMatch) return true;
  const nameSim = Number(row.nameSimilarity);
  const dist = row.distanceMeters;
  if (
    Number.isFinite(nameSim) &&
    nameSim >= STRONG_NAME_SIM &&
    dist != null &&
    dist <= 250
  ) {
    return true;
  }
  return false;
}

export function assessBackwardsPromotion(row, ctx) {
  const { minConfidence, verifiedIndex, duplicateOsmIds } = ctx;
  const minRank = { high: 3, medium: 2, low: 1, none: 0 }[minConfidence] ?? 3;

  const confRank = { high: 3, medium: 2, low: 1, none: 0 }[row.matchConfidence] ?? 0;
  const coords = parseCoords(row.osmLatitude, row.osmLongitude);
  const hasName = !!normalizeKey(row.osmName);
  const hasCountry = !!normalizeKey(row.osmCountry);
  const hasCoords = !!coords;

  const alreadyVerified =
    verifiedIndex?.candidateLinks?.has(row.osmCandidateRecordId) ||
    verifiedIndex?.dedupeKeys?.has(normalizeKey(row.verifiedDedupeKey));

  const duplicateRisk =
    duplicateOsmIds.has(row.osmCandidateRecordId) || !!row.inDuplicateCluster;

  const notes = [];

  if (alreadyVerified) {
    return {
      promotionEligibility: PROMOTION_ELIGIBILITY.NOT_ELIGIBLE,
      promotionRecommendation: PROMOTION_RECOMMENDATION.ALREADY_VERIFIED,
      notes: "OSM candidate already linked in Verified Independent Hotel Census.",
    };
  }

  if (duplicateRisk) {
    return {
      promotionEligibility: PROMOTION_ELIGIBILITY.NOT_ELIGIBLE,
      promotionRecommendation: PROMOTION_RECOMMENDATION.HOLD_DUPLICATE,
      notes:
        "Duplicate-risk cluster: multiple OSM candidates match same legacy census row or retention duplicate cluster.",
    };
  }

  if (row.matchConfidence !== "high" || confRank < minRank) {
    return {
      promotionEligibility: PROMOTION_ELIGIBILITY.NOT_ELIGIBLE,
      promotionRecommendation: PROMOTION_RECOMMENDATION.HOLD_LOW_CONFIDENCE,
      notes: `Match confidence ${row.matchConfidence} below promotion threshold (${minConfidence}).`,
    };
  }

  if (!hasName || !hasCountry || !hasCoords) {
    if (!hasName) notes.push("Missing OSM hotel name.");
    if (!hasCountry) notes.push("Missing OSM country.");
    if (!hasCoords) notes.push("Missing OSM coordinates.");
    return {
      promotionEligibility: PROMOTION_ELIGIBILITY.NOT_ELIGIBLE,
      promotionRecommendation: PROMOTION_RECOMMENDATION.HOLD_LOW_CONFIDENCE,
      notes: notes.join(" "),
    };
  }

  if (!meetsStrictBackwardsPromotionSignals(row)) {
    return {
      promotionEligibility: PROMOTION_ELIGIBILITY.NOT_ELIGIBLE,
      promotionRecommendation: PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE,
      notes:
        "High legacy match but missing strict promotion signals (website/phone match, or strong name within 250m). Medium matches are not promoted by default.",
    };
  }

  let promotionEligibility = PROMOTION_ELIGIBILITY.ELIGIBLE;
  let promotionRecommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
  if ((row.matchScore || 0) >= PROMOTE_SCORE_THRESHOLD) {
    promotionRecommendation = PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW;
    notes.push("High-confidence legacy census match; score meets promote threshold.");
  } else {
    promotionRecommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
    notes.push(
      `High-confidence legacy match; score ${row.matchScore} below ${PROMOTE_SCORE_THRESHOLD} — human review recommended.`
    );
  }

  return { promotionEligibility, promotionRecommendation, notes: notes.join(" ") };
}

export function parseRetentionIncludeExclude(opts = {}) {
  const includeStr = String(opts.includeRetention || "").trim();
  const excludeStr = String(opts.excludeRetention || "").trim();

  const includeSet = includeStr
    ? new Set(
        includeStr
          .split(",")
          .map((s) => normalizeKey(s.trim()))
          .filter(Boolean)
      )
    : parseRetentionIncludeList("");

  const excludeSet = excludeStr
    ? new Set(
        excludeStr
          .split(",")
          .map((s) => normalizeKey(s.trim()))
          .filter(Boolean)
      )
    : includeStr
      ? new Set()
      : DEFAULT_EXCLUDED_RETENTION;

  return { includeSet, excludeSet };
}

/**
 * @param {{ allCountries?: boolean, countriesStr?: string, countryFilter?: string }} opts
 * @returns {Set<string>|null} null = all countries
 */
export function parseCountriesOption(opts = {}) {
  if (opts.allCountries) return null;
  const multi = String(opts.countriesStr || "").trim();
  if (multi) {
    return new Set(
      multi
        .split(",")
        .map((c) => normalizeCountry(c.trim()))
        .filter(Boolean)
    );
  }
  const single = opts.countryFilter
    ? normalizeCountry(opts.countryFilter)
    : "";
  if (single) return new Set([single]);
  return null;
}

export function countryMatchesFilter(rawCountry, countryFilterSet) {
  if (!countryFilterSet) return true;
  return countryFilterSet.has(normalizeCountry(rawCountry));
}

export function mapRetentionRowToOsmCandidate(r) {
  return {
    airtableRecordId: r.airtableRecordId,
    sourceType: r.sourceType,
    sourceRecordId: r.sourceRecordId,
    rawHotelName: r.rawHotelName,
    rawCity: r.rawCity || "",
    rawCountry: r.rawCountry,
    rawLatitude: r.rawLatitude,
    rawLongitude: r.rawLongitude,
    rawWebsite: r.rawWebsite || "",
    rawPhone: r.rawPhone || "",
    rawBrand: r.rawBrand || "",
    importBatchId: r.importBatchId || "",
    retentionRecommendation: r.retentionRecommendation || "",
    inDuplicateCluster: !!r.inDuplicateCluster,
    qualityScore: r.qualityScore,
    qualityTier: r.qualityTier,
    parentCompany: r.parentCompany || "",
    likelyOsmEnrichForBrandDirectory: !!r.likelyOsmEnrichForBrandDirectory,
  };
}

/**
 * Load OSM candidates from Phase 4O retention JSON only (no Candidate table reads).
 */
export function loadOsmCandidatesFromRetentionReport(opts) {
  const retention = loadCandidateRetentionReport(opts.retentionReportPath);
  const { includeSet, excludeSet } = parseRetentionIncludeExclude(opts);
  const countryFilterSet = opts.countryFilterSet ?? null;

  const candidates = [];
  let osmRowsSeen = 0;
  const byCountryLoaded = {};
  const byRetentionLoaded = {};

  for (const r of retention.rows) {
    if (normalizeKey(r.sourceType) !== SOURCE_TYPES.OSM) continue;
    osmRowsSeen++;
    const rec = normalizeKey(r.retentionRecommendation);
    byRetentionLoaded[rec] = (byRetentionLoaded[rec] || 0) + 1;
    if (!includeSet.has(rec)) continue;
    if (excludeSet.has(rec)) continue;
    if (!countryMatchesFilter(r.rawCountry, countryFilterSet)) continue;

    candidates.push(mapRetentionRowToOsmCandidate(r));
    const co = normalizeCountry(r.rawCountry) || "(unknown)";
    byCountryLoaded[co] = (byCountryLoaded[co] || 0) + 1;
  }

  return {
    retentionRowsTotal: retention.rows.length,
    osmRowsInReport: osmRowsSeen,
    targetIdCount: candidates.length,
    candidates,
    includeRetention: [...includeSet],
    excludeRetention: [...excludeSet],
    countryFilter: countryFilterSet ? [...countryFilterSet] : null,
    allCountries: !countryFilterSet,
    byCountryLoaded,
    byRetentionLoaded,
    candidateSource: "retention_report_json",
  };
}

/** @deprecated Use loadOsmCandidatesFromRetentionReport — avoids Airtable record-limit pressure. */
export async function loadRetentionFilteredOsmCandidates(opts) {
  return loadOsmCandidatesFromRetentionReport(opts);
}

export function buildBackwardsMatchReportRow(candidate, match, extra = {}) {
  const verifiedDedupeKey = buildVerifiedDedupeKeyFromOsm(candidate);
  const best = match._scoredBest;
  return {
    osmCandidateRecordId: candidate.airtableRecordId,
    osmName: candidate.rawHotelName,
    osmCity: candidate.rawCity,
    osmCountry: candidate.rawCountry,
    osmLatitude: candidate.rawLatitude,
    osmLongitude: candidate.rawLongitude,
    osmWebsite: candidate.rawWebsite,
    osmSourceRecordId: candidate.sourceRecordId,
    retentionRecommendation: candidate.retentionRecommendation || "",
    inDuplicateCluster: !!candidate.inDuplicateCluster,
    matchedLegacyRecordId: match.matchedLegacyRecordId,
    matchedLegacyName: match.matchedLegacyName,
    matchedLegacyCity: match.matchedLegacyCity,
    matchedLegacyCountry: match.matchedLegacyCountry,
    distanceMeters: match.distanceMeters,
    nameSimilarity: best?.nameSim ?? null,
    websiteMatch: !!best?.websiteMatch,
    phoneMatch: !!best?.phoneMatch,
    matchConfidence: match.matchConfidence,
    matchScore: match.matchScore,
    matchReason: match.matchReason,
    verifiedDedupeKey,
    ...extra,
  };
}

export function summarizeBackwardsMatchByCountry(reportRows) {
  const byCountry = {};
  for (const r of reportRows) {
    const co = normalizeCountry(r.osmCountry) || "(unknown)";
    if (!byCountry[co]) {
      byCountry[co] = {
        country: co,
        candidates: 0,
        matchHigh: 0,
        matchMedium: 0,
        matchLow: 0,
        matchNone: 0,
        promotionEligible: 0,
        alreadyVerified: 0,
        holdDuplicate: 0,
        wouldPromote: 0,
      };
    }
    const b = byCountry[co];
    b.candidates++;
    if (r.matchConfidence === "high") b.matchHigh++;
    else if (r.matchConfidence === "medium") b.matchMedium++;
    else if (r.matchConfidence === "low") b.matchLow++;
    else b.matchNone++;
    if (r.promotionEligibility === PROMOTION_ELIGIBILITY.ELIGIBLE) {
      b.promotionEligible++;
    }
    if (r.promotionRecommendation === PROMOTION_RECOMMENDATION.ALREADY_VERIFIED) {
      b.alreadyVerified++;
    }
    if (r.promotionRecommendation === PROMOTION_RECOMMENDATION.HOLD_DUPLICATE) {
      b.holdDuplicate++;
    }
    if (
      r.promotionRecommendation === PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW
    ) {
      b.wouldPromote++;
    }
  }
  return Object.values(byCountry).sort((a, b) => b.candidates - a.candidates);
}

export function summarizeIneligiblePromotionReasons(reportRows) {
  const counts = {};
  const notEligible = reportRows.filter(
    (r) => r.promotionEligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE
  );
  for (const r of notEligible) {
    const key = r.promotionRecommendation || "unknown";
    counts[key] = (counts[key] || 0) + 1;
  }
  const topReasons = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .map(([reason, count]) => ({ reason, count }));
  return { notEligibleCount: notEligible.length, byRecommendation: topReasons };
}

export function estimateVerifiedPossibleByCountry(reportRows) {
  const byCountry = {};
  for (const r of reportRows) {
    if (r.promotionEligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE) continue;
    const co = normalizeCountry(r.osmCountry) || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
  }
  return Object.entries(byCountry)
    .map(([country, estimatedNewVerified]) => ({ country, estimatedNewVerified }))
    .sort((a, b) => b.estimatedNewVerified - a.estimatedNewVerified);
}

export function summarizeConfidenceCounts(rows) {
  const counts = { high: 0, medium: 0, low: 0, none: 0 };
  for (const r of rows) {
    counts[r.matchConfidence] = (counts[r.matchConfidence] || 0) + 1;
  }
  return counts;
}

export function backwardsMatchRowToCsv(row) {
  return {
    osmCandidateRecordId: row.osmCandidateRecordId,
    osmName: row.osmName,
    osmCity: row.osmCity,
    osmCountry: row.osmCountry,
    osmLatitude: row.osmLatitude,
    osmLongitude: row.osmLongitude,
    osmWebsite: row.osmWebsite,
    matchedLegacyRecordId: row.matchedLegacyRecordId,
    matchedLegacyName: row.matchedLegacyName,
    matchedLegacyCity: row.matchedLegacyCity,
    matchedLegacyCountry: row.matchedLegacyCountry,
    distanceMeters: row.distanceMeters ?? "",
    matchConfidence: row.matchConfidence,
    matchScore: row.matchScore,
    matchReason: row.matchReason,
    promotionEligibility: row.promotionEligibility,
    promotionRecommendation: row.promotionRecommendation,
    verifiedDedupeKey: row.verifiedDedupeKey,
    notes: row.notes || "",
  };
}

export const BACKWARDS_MATCH_CSV_COLUMNS = [
  "osmCandidateRecordId",
  "osmName",
  "osmCity",
  "osmCountry",
  "osmLatitude",
  "osmLongitude",
  "osmWebsite",
  "matchedLegacyRecordId",
  "matchedLegacyName",
  "matchedLegacyCity",
  "matchedLegacyCountry",
  "distanceMeters",
  "matchConfidence",
  "matchScore",
  "matchReason",
  "promotionEligibility",
  "promotionRecommendation",
  "verifiedDedupeKey",
  "notes",
];

/**
 * @param {object} opts
 */
/**
 * Load Verified dedupe index only (fast check for reliability).
 */
export async function runVerifiedIndexCheck(opts) {
  const base = getIndependentCensusBase();
  if (!base) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT for Verified index check");
  }

  const loaded = await loadVerifiedIndexWithPolicy(base, {
    apply: false,
    allowMissingVerifiedIndex: !!opts.allowMissingVerifiedIndex,
  });

  return {
    batchId: opts.batchId,
    phase: "verified-index-check",
    mode: "dry-run",
    verifiedIndexLoadFailed: loaded.loadFailed,
    verifiedIndexMeta: loaded.meta,
    dryRun: true,
    airtableWrites: false,
    verifiedTableWrites: false,
    hotelCensusReads: false,
    hotelCensusWrites: false,
    candidateTableReads: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export async function runBackwardsCensusMatch(opts) {
  const apply = !!opts.apply;
  const approvedBy = opts.approvedBy || "";
  const minConfidence = (opts.minConfidence || "high").toLowerCase();
  const maxPromotions =
    opts.maxPromotions != null ? Number(opts.maxPromotions) : null;
  const approvedAt = new Date().toISOString();

  const countryFilterSet = parseCountriesOption({
    allCountries: opts.allCountries,
    countriesStr: opts.countriesStr,
    countryFilter: opts.countryFilter || "",
  });

  const osmLoad = loadOsmCandidatesFromRetentionReport({
    retentionReportPath: opts.retentionReportPath,
    includeRetention: opts.includeRetention || "",
    excludeRetention: opts.excludeRetention || "",
    countryFilterSet,
  });

  const censusCountryFilter =
    countryFilterSet && countryFilterSet.size === 1
      ? [...countryFilterSet][0]
      : opts.countryFilter || "";

  const censusData = await loadHotelCensusReadOnly({
    countryFilter: censusCountryFilter,
  });

  const paired = [];
  const total = osmLoad.candidates.length;
  for (let i = 0; i < total; i++) {
    const candidate = osmLoad.candidates[i];
    if (i > 0 && i % 5000 === 0) {
      console.error(`Backwards-match progress: ${i}/${total}…`);
    }
    const match = matchOsmCandidateBackwards(candidate, censusData);
    paired.push({
      candidate,
      row: buildBackwardsMatchReportRow(candidate, match),
    });
  }

  const duplicateOsmIds = markLegacyDuplicateClusters(paired.map((p) => p.row));

  const base = getIndependentCensusBase();
  let verifiedIndex = {
    dedupeKeys: new Set(),
    candidateLinks: new Set(),
    geoNameKeys: [],
  };
  let verifiedIndexLoadFailed = false;
  let verifiedIndexMeta = null;

  if (base) {
    const loaded = await loadVerifiedIndexWithPolicy(base, {
      apply,
      allowMissingVerifiedIndex: Boolean(opts.allowMissingVerifiedIndex),
    });
    verifiedIndex = loaded.index;
    verifiedIndexLoadFailed = loaded.loadFailed;
    verifiedIndexMeta = loaded.meta;
    if (!loaded.loadFailed) {
      logVerifiedIndexMeta(loaded.meta);
    }
  } else if (apply) {
    throw new Error("Aborting --apply: missing Airtable base for Verified index load.");
  }

  const reportRows = [];
  for (const { candidate, row } of paired) {
    const promo = assessBackwardsPromotion(row, {
      minConfidence,
      verifiedIndex,
      duplicateOsmIds,
    });
    reportRows.push({
      ...row,
      promotionEligibility: promo.promotionEligibility,
      promotionRecommendation: promo.promotionRecommendation,
      notes: promo.notes,
    });
  }

  const confidenceCounts = summarizeConfidenceCounts(reportRows);
  const promotionEligible = reportRows.filter(
    (r) => r.promotionEligibility === PROMOTION_ELIGIBILITY.ELIGIBLE
  ).length;

  const wouldPromote = reportRows.filter(
    (r) =>
      r.promotionRecommendation === PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW
  );

  let writtenCount = 0;
  let skippedDuplicate = 0;
  let writtenRecords = [];

  if (apply) {
    if (!approvedBy) {
      throw new Error("--approved-by is required when using --apply");
    }

    let toPromote = wouldPromote;
    if (maxPromotions != null && maxPromotions > 0) {
      toPromote = toPromote.slice(0, maxPromotions);
    }

    const verifiedPayloads = [];
    const candidateById = new Map(
      osmLoad.candidates.map((c) => [c.airtableRecordId, c])
    );

    for (const row of toPromote) {
      const candidate = candidateById.get(row.osmCandidateRecordId);
      if (!candidate) continue;
      const mapped = buildBackwardsVerifiedFields(candidate, row, {
        approvedBy,
        batchId: opts.batchId,
        approvedAt,
      });
      if (verifiedIndex.dedupeKeys.has(normalizeKey(mapped.verifiedDedupeKey))) {
        skippedDuplicate++;
        continue;
      }
      if (verifiedIndex.candidateLinks.has(candidate.airtableRecordId)) {
        skippedDuplicate++;
        continue;
      }
      verifiedPayloads.push(mapped);
      verifiedIndex.dedupeKeys.add(normalizeKey(mapped.verifiedDedupeKey));
      verifiedIndex.candidateLinks.add(candidate.airtableRecordId);
    }

    if (base && verifiedPayloads.length) {
      const result = await createVerifiedRecords(
        base,
        VERIFIED_TABLE,
        verifiedPayloads,
        verifiedIndex.dedupeKeys
      );
      writtenCount = result.writtenCount;
      writtenRecords = result.created;
    }
  }

  const byCountry = summarizeBackwardsMatchByCountry(reportRows);
  const ineligibleSummary = summarizeIneligiblePromotionReasons(reportRows);
  const estimatedVerifiedByCountry = estimateVerifiedPossibleByCountry(reportRows);
  const alreadyVerifiedCount = reportRows.filter(
    (r) =>
      r.promotionRecommendation === PROMOTION_RECOMMENDATION.ALREADY_VERIFIED
  ).length;
  const holdDuplicateCount = reportRows.filter(
    (r) => r.promotionRecommendation === PROMOTION_RECOMMENDATION.HOLD_DUPLICATE
  ).length;

  return {
    batchId: opts.batchId,
    countryFilter: osmLoad.countryFilter,
    allCountries: osmLoad.allCountries,
    mode: apply ? "apply" : "dry-run",
    apply,
    approvedBy: approvedBy || null,
    minConfidence,
    maxPromotions,
    candidateSource: osmLoad.candidateSource,
    osmCandidatesLoaded: osmLoad.candidates.length,
    osmRowsInRetentionReport: osmLoad.osmRowsInReport,
    osmTargetIdsFromRetention: osmLoad.targetIdCount,
    osmByCountryLoaded: osmLoad.byCountryLoaded,
    osmByRetentionLoaded: osmLoad.byRetentionLoaded,
    legacyCensusRecordsLoaded: censusData.totalLoaded,
    legacyCensusRecordsInPool: censusData.rows.length,
    confidenceCounts,
    promotionEligibleCount: promotionEligible,
    alreadyVerifiedCount,
    holdDuplicateCount,
    wouldPromoteCount: wouldPromote.length,
    byCountry,
    ineligibleSummary,
    estimatedVerifiedByCountry,
    writtenCount,
    skippedDuplicateCount: skippedDuplicate,
    writtenRecords,
    reportRows,
    retentionInclude: osmLoad.includeRetention,
    retentionExclude: osmLoad.excludeRetention,
    verifiedIndexLoadFailed,
    verifiedIndexMeta,
    dryRun: !apply,
    airtableWrites: apply && writtenCount > 0,
    candidateTableReads: false,
    tablesWritten: apply && writtenCount > 0 ? [VERIFIED_TABLE] : [],
    hotelCensusReads: true,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    verifiedTableWrites: apply && writtenCount > 0,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    fieldsLoadedFromLegacyCensus: censusData.fieldsLoaded,
  };
}
