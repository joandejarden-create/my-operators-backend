/**
 * Phase Choice-D — Promotion review buckets for Choice target ↔ OSM recovery.
 */

import { readFileSync } from "fs";
import { loadChoiceTargetMatchReport } from "./targeted-osm-lookup.js";
import { normalizePropertyUrl } from "./choice-property-id-reconciliation.js";
import { normalizeKey } from "./match-current-census.js";

export const CHOICE_PROMOTION_BUCKET = {
  READY: "ready_for_verified_review",
  NEEDS_ENRICHMENT: "needs_official_source_or_google_enrichment",
  DUPLICATE: "duplicate_review",
  NO_OSM: "no_osm_match",
  HOLD_LOW: "hold_low_confidence",
};

function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function indexLookupByLegacyId(lookupReport) {
  const m = new Map();
  if (!lookupReport?.results) return m;
  for (const r of lookupReport.results) {
    m.set(r.legacyRecordId, r);
  }
  return m;
}

function buildSitemapUrlIndex(propertyUrlReport) {
  const byNorm = new Map();
  const rows = propertyUrlReport?.propertyRows || [];
  for (const row of rows) {
    if (
      row.calaFilterStatus &&
      row.calaFilterStatus !== "included"
    ) {
      continue;
    }
    const url = row.propertyUrl || row.url || "";
    if (!url) continue;
    const norm = normalizePropertyUrl(url);
    if (norm) byNorm.set(norm, row);
  }
  return byNorm;
}

function osmWebsiteMatchesSitemap(osmWebsite, sitemapIndex) {
  if (!osmWebsite || !sitemapIndex.size) return false;
  const norm = normalizePropertyUrl(osmWebsite);
  return norm && sitemapIndex.has(norm);
}

function hasOsmCoreFields(row) {
  return (
    !!normalizeKey(row.osmName) &&
    !!normalizeKey(row.osmCountry || row.targetCountry) &&
    row.osmSourceRecordId
  );
}

const CONF_RANK = { high: 3, medium: 2, low: 1, none: 0 };

function bestConfidence(a, b) {
  const ra = CONF_RANK[a] ?? 0;
  const rb = CONF_RANK[b] ?? 0;
  return ra >= rb ? a : b;
}

export function classifyChoicePromotionRow(row, ctx) {
  const lookup = ctx.lookupByLegacy.get(row.legacyRecordId);
  const effectiveConfidence = bestConfidence(
    row.matchConfidence || "none",
    lookup?.matchConfidence || "none"
  );

  if (row.duplicateRisk || row.inDuplicateCluster) {
    return {
      bucket: CHOICE_PROMOTION_BUCKET.DUPLICATE,
      reason: "OSM candidate flagged duplicate cluster or duplicate risk",
      effectiveConfidence,
      choicePropertyUrlMatch: false,
    };
  }

  if (effectiveConfidence === "none") {
    return {
      bucket: CHOICE_PROMOTION_BUCKET.NO_OSM,
      reason: lookup?.matchReason || row.matchReason || "No OSM match",
      effectiveConfidence,
      choicePropertyUrlMatch: false,
    };
  }

  if (effectiveConfidence === "low") {
    return {
      bucket: CHOICE_PROMOTION_BUCKET.HOLD_LOW,
      reason: row.matchReason || "Low-confidence OSM match only",
      effectiveConfidence,
      choicePropertyUrlMatch: false,
    };
  }

  const urlMatch = osmWebsiteMatchesSitemap(row.osmWebsite, ctx.sitemapIndex);

  if (effectiveConfidence === "high" && !row.alreadyVerified) {
    if (!hasOsmCoreFields(row)) {
      return {
        bucket: CHOICE_PROMOTION_BUCKET.NEEDS_ENRICHMENT,
        reason: "High match but OSM candidate missing core fields",
        effectiveConfidence,
        choicePropertyUrlMatch: urlMatch,
      };
    }
    if (urlMatch || row.websiteMatch || row.phoneMatch) {
      return {
        bucket: CHOICE_PROMOTION_BUCKET.READY,
        reason: urlMatch
          ? "High-confidence OSM match with Choice sitemap URL alignment"
          : "High-confidence OSM match with website or phone alignment",
        effectiveConfidence,
        choicePropertyUrlMatch: urlMatch,
      };
    }
    return {
      bucket: CHOICE_PROMOTION_BUCKET.READY,
      reason:
        "High-confidence geo/name OSM match; add Choice sitemap URL evidence before Verified apply (preferred)",
      effectiveConfidence,
      choicePropertyUrlMatch: false,
    };
  }

  if (effectiveConfidence === "medium") {
    return {
      bucket: CHOICE_PROMOTION_BUCKET.NEEDS_ENRICHMENT,
      reason:
        "Medium-confidence match — manual approval or official source before Verified",
      effectiveConfidence,
      choicePropertyUrlMatch: urlMatch,
    };
  }

  if (row.alreadyVerified) {
    return {
      bucket: CHOICE_PROMOTION_BUCKET.HOLD_LOW,
      reason: "OSM candidate already linked in Verified census",
      effectiveConfidence,
      choicePropertyUrlMatch: urlMatch,
    };
  }

  return {
    bucket: CHOICE_PROMOTION_BUCKET.NEEDS_ENRICHMENT,
    reason: "Review before promotion",
    effectiveConfidence,
    choicePropertyUrlMatch: urlMatch,
  };
}

/**
 * @param {object} opts
 */
export async function runChoicePromotionReview(opts) {
  const { matchRows } = loadChoiceTargetMatchReport(opts.targetMatchReportPath);

  let lookupReport = null;
  if (opts.targetedLookupReportPath) {
    lookupReport = loadJson(opts.targetedLookupReportPath);
  }

  let propertyUrlReport = null;
  if (opts.choicePropertyUrlReportPath) {
    propertyUrlReport = loadJson(opts.choicePropertyUrlReportPath);
  }

  const lookupByLegacy = indexLookupByLegacyId(lookupReport);
  const sitemapIndex = buildSitemapUrlIndex(propertyUrlReport);

  const reviewRows = [];
  const bucketCounts = {};

  for (const row of matchRows) {
    const classified = classifyChoicePromotionRow(row, {
      lookupByLegacy,
      sitemapIndex,
    });
    bucketCounts[classified.bucket] = (bucketCounts[classified.bucket] || 0) + 1;

    reviewRows.push({
      legacyRecordId: row.legacyRecordId,
      legacyHotelName: row.legacyHotelName,
      legacyCountry: row.legacyCountry,
      targetBrand: row.targetBrand,
      matchConfidence: row.matchConfidence,
      effectiveMatchConfidence: classified.effectiveConfidence,
      osmCandidateRecordId: row.osmCandidateRecordId,
      osmName: row.osmName,
      osmSourceRecordId: row.osmSourceRecordId,
      promotionBucket: classified.bucket,
      promotionReason: classified.reason,
      choicePropertyUrlMatch: classified.choicePropertyUrlMatch ? "yes" : "no",
      alreadyVerified: row.alreadyVerified ? "yes" : "no",
      duplicateRisk: row.duplicateRisk ? "yes" : "no",
      legacyBenchmarkNote:
        "Legacy target is read-only benchmark only; Verified uses OSM fields only.",
    });
  }

  const effectiveConfidenceCounts = { high: 0, medium: 0, low: 0, none: 0 };
  for (const r of reviewRows) {
    const ec = r.effectiveMatchConfidence || "none";
    effectiveConfidenceCounts[ec] = (effectiveConfidenceCounts[ec] || 0) + 1;
  }

  return {
    batchId: opts.batchId,
    choiceTargetCount: matchRows.length,
    bucketCounts,
    effectiveConfidenceCounts,
    verifiedReadyCount:
      bucketCounts[CHOICE_PROMOTION_BUCKET.READY] || 0,
    highConfidenceOsmMatchCount: effectiveConfidenceCounts.high || 0,
    reviewRows,
    targetedLookupReportPath: opts.targetedLookupReportPath || null,
    choicePropertyUrlReportPath: opts.choicePropertyUrlReportPath || null,
    sitemapUrlsIndexed: sitemapIndex.size,
    dryRun: true,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    verifiedTableWrites: false,
    candidateTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export const CHOICE_PROMOTION_REVIEW_CSV_COLUMNS = [
  "legacyRecordId",
  "legacyHotelName",
  "legacyCountry",
  "targetBrand",
  "matchConfidence",
  "effectiveMatchConfidence",
  "osmCandidateRecordId",
  "osmName",
  "promotionBucket",
  "promotionReason",
  "choicePropertyUrlMatch",
  "alreadyVerified",
  "duplicateRisk",
];
