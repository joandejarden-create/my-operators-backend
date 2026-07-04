/**
 * Deterministic Market Demand Snapshot scoring (MVP — no AI / external APIs).
 */

import {
  DEMAND_CATEGORY_KEYS,
  DEMAND_CATEGORY_LABELS,
  mapDemandCategoryToKey,
  MARKET_DEMAND_SNAPSHOT_FIELDS,
  MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
  SNAPSHOT_SCORE_KEY_TO_FIELD,
} from "./airtable-market-demand-fields.js";

const DEMAND_STRENGTH_POINTS = {
  high: 20,
  medium: 12,
  low: 5,
};

const DATA_CONFIDENCE_POINTS = {
  high: 10,
  medium: 6,
  low: 3,
};

function mapLevelToPoints(value, map) {
  const v = String(value || "")
    .trim()
    .toLowerCase();
  if (!v) return 0;
  if (v.includes("high")) return map.high;
  if (v.includes("medium") || v.includes("med")) return map.medium;
  if (v.includes("low")) return map.low;
  return 0;
}

function average(nums) {
  const valid = nums.filter((n) => Number.isFinite(n));
  if (!valid.length) return 0;
  return valid.reduce((a, b) => a + b, 0) / valid.length;
}

function emptyScores() {
  return Object.fromEntries(DEMAND_CATEGORY_KEYS.map((k) => [k, 0]));
}

/**
 * @param {import('./normalize-market-demand.js').normalizeDemandCenterRecord extends Function ? ReturnType<import('./normalize-market-demand.js').normalizeDemandCenterRecord>[] : object[]} demandCenters
 * @returns {{ scores: Record<string, number>, categoryCounts: Record<string, number>, overallDataConfidence: string }}
 */
export function calculateCategoryScores(demandCenters) {
  const grouped = Object.fromEntries(DEMAND_CATEGORY_KEYS.map((k) => [k, []]));
  for (const dc of demandCenters || []) {
    const key = mapDemandCategoryToKey(dc.category);
    if (key && grouped[key]) grouped[key].push(dc);
  }

  const scores = emptyScores();
  const categoryCounts = Object.fromEntries(DEMAND_CATEGORY_KEYS.map((k) => [k, 0]));

  for (const key of DEMAND_CATEGORY_KEYS) {
    const centers = grouped[key];
    categoryCounts[key] = centers.length;
    if (!centers.length) {
      scores[key] = 0;
      continue;
    }

    const countPoints = Math.min(30, centers.length * 5);
    const relevanceVals = centers
      .map((c) => c.relevanceScore)
      .filter((n) => n != null && Number.isFinite(n));
    const relevancePoints = relevanceVals.length
      ? Math.min(40, (average(relevanceVals) / 100) * 40)
      : 0;
    const strengthPoints = Math.min(
      20,
      average(centers.map((c) => mapLevelToPoints(c.demandStrength, DEMAND_STRENGTH_POINTS)))
    );
    const confidencePoints = Math.min(
      10,
      average(centers.map((c) => mapLevelToPoints(c.dataConfidence, DATA_CONFIDENCE_POINTS)))
    );

    scores[key] = Math.round(countPoints + relevancePoints + strengthPoints + confidencePoints);
  }

  const confLevels = (demandCenters || []).map((c) => mapLevelToPoints(c.dataConfidence, DATA_CONFIDENCE_POINTS));
  const avgConf = average(confLevels);
  let overallDataConfidence = "Unknown";
  if (avgConf >= 8) overallDataConfidence = "High";
  else if (avgConf >= 5) overallDataConfidence = "Medium";
  else if (avgConf > 0) overallDataConfidence = "Low";

  return { scores, categoryCounts, overallDataConfidence };
}

/**
 * @param {number} totalDemandCenters
 * @param {Record<string, number>} scores
 */
export function deriveOverallDemandStrength(totalDemandCenters, scores) {
  if (totalDemandCenters < 3) return "Unclear";
  const sorted = Object.values(scores).sort((a, b) => b - a);
  const top = sorted[0] || 0;
  const categoriesAt60 = Object.values(scores).filter((s) => s >= 60).length;
  if (top >= 75 && categoriesAt60 >= 2) return "Strong";
  if (top >= 60) return "Moderate";
  return "Limited";
}

/**
 * @param {Record<string, number>} scores
 * @returns {string}
 */
export function derivePrimaryDemandProfile(scores) {
  const ranked = DEMAND_CATEGORY_KEYS.map((key) => ({ key, score: scores[key] || 0 }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);
  return ranked.map((x) => DEMAND_CATEGORY_LABELS[x.key]).join(", ");
}

const BRAND_IMPLICATIONS = {
  leisure:
    "May support resort, lifestyle, soft brand, or upper-upscale positioning depending on asset quality and owner objectives.",
  transportation:
    "May support select-service, transient, airport, or short-stay demand strategies.",
  corporate:
    "May support weekday transient, negotiated account, and select-service/full-service positioning.",
  medical:
    "May support extended-stay, select-service, and family/visitor demand.",
  education:
    "May support event-based, visiting family, sports, faculty, and seasonal demand.",
  group:
    "May support brands and operators with group sales capability.",
  industrial:
    "May support extended-stay, midscale, and select-service demand.",
  retailMixedUse:
    "May support lifestyle, urban, leisure, and mixed-use positioning.",
  government: "May support government-contract, extended-stay, and select-service demand near civic centers.",
};

const OPERATOR_IMPLICATIONS = {
  group: "Operator group sales capability may matter.",
  leisure:
    "Operator should understand leisure segmentation, OTA/channel strategy, and experience-driven positioning.",
  corporate: "Operator should have local sales/account management capability.",
  medical: "Operator should understand extended-stay and recurring local demand.",
  education: "Operator should understand extended-stay and recurring local demand.",
  industrial: "Operator should understand crew, project, and extended-stay business.",
  transportation:
    "Operator should understand transient, airport, crew, and short-booking-window demand.",
};

function strongCategories(scores, threshold = 60) {
  return DEMAND_CATEGORY_KEYS.filter((k) => (scores[k] || 0) >= threshold);
}

/**
 * @param {Record<string, number>} scores
 * @param {number} totalDemandCenters
 * @param {string} dataConfidence
 */
export function buildDemandSummary(scores, totalDemandCenters, dataConfidence) {
  const profile = derivePrimaryDemandProfile(scores);
  const topLabels =
    profile ||
    DEMAND_CATEGORY_KEYS.filter((k) => (scores[k] || 0) > 0)
      .sort((a, b) => (scores[b] || 0) - (scores[a] || 0))
      .slice(0, 3)
      .map((k) => DEMAND_CATEGORY_LABELS[k])
      .join(", ") ||
    "no dominant categories yet";

  const confLabel = dataConfidence && dataConfidence !== "Unknown" ? dataConfidence : "limited";
  return (
    `This location shows strongest demand signals in ${topLabels}. ` +
    `The current dataset includes ${totalDemandCenters} demand center${totalDemandCenters === 1 ? "" : "s"} with ${confLabel} data confidence. ` +
    "Additional validation may be needed for corporate demand, weekday demand, and future supply."
  );
}

/**
 * @param {Record<string, number>} scores
 * @param {Record<string, number>} categoryCounts
 */
export function buildDemandGaps(scores, categoryCounts) {
  const weak = DEMAND_CATEGORY_KEYS.filter(
    (k) => !categoryCounts[k] || (scores[k] || 0) < 30
  ).map((k) => DEMAND_CATEGORY_LABELS[k]);
  if (!weak.length) return "No major category gaps identified in the current dataset.";
  return `Weak or missing demand signals for: ${weak.join(", ")}. Consider adding demand centers or validating these drivers manually.`;
}

/**
 * @param {Record<string, number>} scores
 */
export function buildBrandImplications(scores) {
  const lines = strongCategories(scores).map((k) => BRAND_IMPLICATIONS[k]).filter(Boolean);
  if (!lines.length) return "Insufficient category strength to suggest brand positioning implications yet.";
  return lines.join(" ");
}

/**
 * @param {Record<string, number>} scores
 */
export function buildOperatorImplications(scores) {
  const lines = strongCategories(scores).map((k) => OPERATOR_IMPLICATIONS[k]).filter(Boolean);
  if (!lines.length) return "Insufficient category strength to suggest operator capability implications yet.";
  return lines.join(" ");
}

/**
 * @param {Record<string, number>} scores
 * @param {number} nearbyHotelCount
 */
export function buildRecommendedFollowUp(scores, nearbyHotelCount) {
  const parts = [
    "Validate demand center distances and drive times against the property address.",
    "Review weekday vs. weekend patterns for top demand categories.",
  ];
  if (nearbyHotelCount < 3) {
    parts.push("Add nearby hotel supply records to strengthen competitive context.");
  }
  const weakCorporate = (scores.corporate || 0) < 30;
  if (weakCorporate) parts.push("Confirm corporate demand through local business directories or broker insight.");
  return parts.join(" ");
}

/**
 * Build Airtable fields for a Market Demand Snapshot record.
 * @param {object} opts
 */
export function buildSnapshotAirtableFields(opts) {
  const {
    dealId,
    demandCenters,
    nearbyHotels,
    snapshotName,
    linkedMarketIds,
  } = opts;

  const totalCenters = (demandCenters || []).length;
  const { scores, overallDataConfidence } = calculateCategoryScores(demandCenters);
  const overall = deriveOverallDemandStrength(totalCenters, scores);
  const profile = derivePrimaryDemandProfile(scores);
  const summary = buildDemandSummary(scores, totalCenters, overallDataConfidence);
  const gaps = buildDemandGaps(scores, calculateCategoryScores(demandCenters).categoryCounts);
  const brand = buildBrandImplications(scores);
  const operator = buildOperatorImplications(scores);
  const followUp = buildRecommendedFollowUp(scores, (nearbyHotels || []).length);

  const fields = {
    [MARKET_DEMAND_SNAPSHOT_FIELDS.snapshotName]:
      snapshotName || `Market Demand Snapshot — ${new Date().toISOString().slice(0, 10)}`,
    [MARKET_DEMAND_DEAL_RECORD_ID_FIELD]: dealId,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.overallDemandStrength]: overall,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.primaryDemandProfile]: profile,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.demandSummary]: summary,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.demandGaps]: gaps,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.brandImplications]: brand,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.operatorImplications]: operator,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.recommendedFollowUp]: followUp,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.dataConfidence]: overallDataConfidence,
    [MARKET_DEMAND_SNAPSHOT_FIELDS.lastGenerated]: new Date().toISOString().slice(0, 10),
  };

  if (Array.isArray(linkedMarketIds) && linkedMarketIds.length) {
    fields[MARKET_DEMAND_SNAPSHOT_FIELDS.linkedMarket] = linkedMarketIds;
  }

  for (const [key, fieldName] of Object.entries(SNAPSHOT_SCORE_KEY_TO_FIELD)) {
    fields[fieldName] = scores[key] ?? 0;
  }

  return {
    fields,
    normalized: {
      scores,
      overallDemandStrength: overall,
      primaryDemandProfile: profile,
      demandSummary: summary,
      demandGaps: gaps,
      brandImplications: brand,
      operatorImplications: operator,
      recommendedFollowUp: followUp,
      dataConfidence: overallDataConfidence,
    },
  };
}

/**
 * Summarize demand centers for GET response.
 * @param {ReturnType<import('./normalize-market-demand.js').normalizeDemandCenterRecord>[]} demandCenters
 */
export function summarizeDemandCenters(demandCenters) {
  const categories = {};
  const byKey = {};

  for (const dc of demandCenters || []) {
    const label = dc.category || "Uncategorized";
    categories[label] = (categories[label] || 0) + 1;
    const key = mapDemandCategoryToKey(dc.category) || "other";
    if (!byKey[key]) byKey[key] = { count: 0, relevanceTotal: 0, relevanceCount: 0 };
    byKey[key].count += 1;
    if (dc.relevanceScore != null && Number.isFinite(dc.relevanceScore)) {
      byKey[key].relevanceTotal += dc.relevanceScore;
      byKey[key].relevanceCount += 1;
    }
  }

  const topCategories = Object.entries(byKey)
    .map(([key, v]) => ({
      key,
      label: DEMAND_CATEGORY_LABELS[key] || key,
      count: v.count,
      avgRelevance: v.relevanceCount ? Math.round(v.relevanceTotal / v.relevanceCount) : null,
      score: v.count * 10 + (v.relevanceCount ? v.relevanceTotal / v.relevanceCount : 0),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  const confLevels = (demandCenters || [])
    .map((c) => String(c.dataConfidence || "").toLowerCase())
    .filter(Boolean);
  let dataConfidence = "Unknown";
  if (confLevels.some((c) => c.includes("high"))) dataConfidence = "High";
  else if (confLevels.some((c) => c.includes("medium"))) dataConfidence = "Medium";
  else if (confLevels.some((c) => c.includes("low"))) dataConfidence = "Low";

  return {
    totalDemandCenters: (demandCenters || []).length,
    categories,
    topCategories,
    dataConfidence,
  };
}

/**
 * Summarize nearby hotel supply for GET response.
 * @param {ReturnType<import('./normalize-market-demand.js').normalizeNearbyHotelSupplyRecord>[]} hotels
 */
export function summarizeNearbyHotelSupply(hotels) {
  const byChainScale = {};
  const byParentCompany = {};
  const byCompetitiveRelevance = {};
  for (const h of hotels || []) {
    const scale = h.chainScale || "Unknown";
    const parent = h.parentCompany || "Unknown";
    const rel = h.competitiveRelevance || "Unknown";
    byChainScale[scale] = (byChainScale[scale] || 0) + 1;
    byParentCompany[parent] = (byParentCompany[parent] || 0) + 1;
    byCompetitiveRelevance[rel] = (byCompetitiveRelevance[rel] || 0) + 1;
  }
  return {
    totalHotels: (hotels || []).length,
    byChainScale,
    byParentCompany,
    byCompetitiveRelevance,
  };
}
