/**
 * Phase Choice-C enrichment plan — no_osm_match targets (report-only, no Google/HTML).
 */

import { readFileSync } from "fs";
import { CHOICE_PROMOTION_BUCKET } from "./choice-promotion-review.js";
import {
  indexSitemapProperties,
  isChoiceFamilyHost,
  normalizePropertyUrl,
} from "./choice-property-id-reconciliation.js";
import {
  nameSimilarity,
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
  websiteHost,
} from "./match-current-census.js";

export const ENRICHMENT_ACTION = {
  GOOGLE: "use_google_places_lookup",
  SITEMAP: "use_choice_sitemap_property_url",
  MANUAL: "manual_review",
  CLOSED: "likely_closed_or_name_changed",
  OFFICIAL: "need_official_source",
};

const CLOSED_STATUS = [
  "closed",
  "defunct",
  "demolished",
  "cancelled",
  "canceled",
  "not open",
  "permanently closed",
];

function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function indexTargets(targetListPath) {
  const data = loadJson(targetListPath);
  const byId = new Map();
  for (const t of data.targets || []) {
    byId.set(t.recordId, t);
  }
  return byId;
}

function sitemapCityMatch(target, sitemapRow) {
  const cityK = normalizeKey(target.city);
  const slugK = normalizeKey(sitemapRow.citySlug);
  if (!cityK || !slugK) return false;
  return cityK.includes(slugK) || slugK.includes(cityK.replace(/\s+/g, "-"));
}

function findSitemapCandidate(target, sitemapIndex) {
  const countryK = normalizeCountry(target.country);
  let best = null;
  let bestSim = 0;

  for (const row of sitemapIndex.byUrl.values()) {
    if (row.calaFilterStatus && row.calaFilterStatus !== "included") continue;
    const rowCountry = normalizeCountry(row.inferredCountry);
    if (countryK && rowCountry && countryK !== rowCountry) continue;
    if (!sitemapCityMatch(target, row)) continue;

    const sim = nameSimilarity(
      target.name,
      row.inferredBrandName || row.matchedBrandSetupBrand || row.propertyUrl
    );
    if (sim > bestSim) {
      bestSim = sim;
      best = row;
    }
  }

  if (bestSim >= 0.45) return { row: best, nameSimilarity: bestSim };
  return null;
}

export function classifyNoMatchEnrichment(target, reviewRow, ctx) {
  const statusK = normalizeKey(target?.status || "");
  if (CLOSED_STATUS.some((s) => statusK.includes(s))) {
    return {
      enrichmentAction: ENRICHMENT_ACTION.CLOSED,
      reason: `Legacy status suggests closed or inactive: ${target?.status || "unknown"}`,
      choicePropertyUrl: "",
      hasCoordinates: false,
    };
  }

  const web = normalizeText(target?.website);
  const webHost = websiteHost(web);
  if (web && isChoiceFamilyHost(webHost || web)) {
    return {
      enrichmentAction: ENRICHMENT_ACTION.SITEMAP,
      reason: "Legacy website on Choice family host — reconcile via sitemap URL (no HTML fetch)",
      choicePropertyUrl: normalizePropertyUrl(web) ? web : "",
      hasCoordinates: !!parseCoords(target?.lat, target?.lng),
    };
  }

  const sitemapHit = findSitemapCandidate(target, ctx.sitemapIndex);
  if (sitemapHit?.row) {
    return {
      enrichmentAction: ENRICHMENT_ACTION.SITEMAP,
      reason: `CALA Choice sitemap URL candidate (name similarity ${(sitemapHit.nameSimilarity * 100).toFixed(0)}%)`,
      choicePropertyUrl: sitemapHit.row.propertyUrl || "",
      hasCoordinates: !!parseCoords(target?.lat, target?.lng),
    };
  }

  const coords = parseCoords(target?.lat, target?.lng);
  if (coords) {
    return {
      enrichmentAction: ENRICHMENT_ACTION.GOOGLE,
      reason:
        "Legacy coordinates present but no OSM match — Google Places lookup deferred to later phase",
      choicePropertyUrl: "",
      hasCoordinates: true,
    };
  }

  if (web && webHost && !isChoiceFamilyHost(webHost)) {
    return {
      enrichmentAction: ENRICHMENT_ACTION.OFFICIAL,
      reason: "Non-Choice website on legacy row — confirm official operator source",
      choicePropertyUrl: "",
      hasCoordinates: false,
    };
  }

  if (normalizeKey(target?.telephone)) {
    return {
      enrichmentAction: ENRICHMENT_ACTION.MANUAL,
      reason: "Phone present without coordinates or OSM match — manual identity research",
      choicePropertyUrl: "",
      hasCoordinates: false,
    };
  }

  return {
    enrichmentAction: ENRICHMENT_ACTION.MANUAL,
    reason: "Insufficient automated signals — manual review",
    choicePropertyUrl: "",
    hasCoordinates: false,
  };
}

/**
 * @param {object} opts
 */
export function buildChoiceNoMatchEnrichmentPlan(opts) {
  const promotion = loadJson(opts.promotionReviewPath);
  const reviewRows = promotion.reviewRows || [];
  const targetById = indexTargets(opts.targetListPath);

  let sitemapIndex = { byPropertyId: new Map(), byUrl: new Map() };
  if (opts.choicePropertyUrlReportPath) {
    const extract = loadJson(opts.choicePropertyUrlReportPath);
    sitemapIndex = indexSitemapProperties(extract);
  }

  const noMatchRows = reviewRows.filter(
    (r) => r.promotionBucket === CHOICE_PROMOTION_BUCKET.NO_OSM
  );

  const planRows = [];
  const actionCounts = {};

  for (const reviewRow of noMatchRows) {
    const target = targetById.get(reviewRow.legacyRecordId) || {
      recordId: reviewRow.legacyRecordId,
      name: reviewRow.legacyHotelName,
      country: reviewRow.legacyCountry,
      city: "",
      targetBrand: reviewRow.targetBrand,
    };

    const classified = classifyNoMatchEnrichment(target, reviewRow, {
      sitemapIndex,
    });
    actionCounts[classified.enrichmentAction] =
      (actionCounts[classified.enrichmentAction] || 0) + 1;

    planRows.push({
      legacyRecordId: reviewRow.legacyRecordId,
      legacyHotelName: reviewRow.legacyHotelName,
      legacyCity: target.city || "",
      legacyCountry: reviewRow.legacyCountry || target.country,
      targetBrand: reviewRow.targetBrand,
      legacyStatus: target.status || "",
      enrichmentAction: classified.enrichmentAction,
      enrichmentReason: classified.reason,
      choicePropertyUrl: classified.choicePropertyUrl,
      hasLegacyCoordinates: classified.hasCoordinates ? "yes" : "no",
      effectiveMatchConfidence: reviewRow.effectiveMatchConfidence,
      notes: "No Google API or Choice HTML in this phase.",
    });
  }

  const byCountry = {};
  const byBrand = {};
  for (const r of planRows) {
    const co = normalizeCountry(r.legacyCountry) || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
    const br = r.targetBrand || "(unknown)";
    byBrand[br] = (byBrand[br] || 0) + 1;
  }

  return {
    batchId: opts.batchId || "choice-no-match-enrichment-plan-2026-05-20",
    noMatchCount: planRows.length,
    actionCounts,
    byCountry,
    byBrand,
    planRows,
    sitemapUrlsIndexed: sitemapIndex.byUrl.size,
    dryRun: true,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    verifiedTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export const NO_MATCH_ENRICHMENT_CSV_COLUMNS = [
  "legacyRecordId",
  "legacyHotelName",
  "legacyCity",
  "legacyCountry",
  "targetBrand",
  "legacyStatus",
  "enrichmentAction",
  "enrichmentReason",
  "choicePropertyUrl",
  "hasLegacyCoordinates",
];
