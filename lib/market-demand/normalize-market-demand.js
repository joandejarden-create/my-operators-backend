/**
 * Normalize Airtable Market Demand records into API / frontend objects.
 */

import {
  MARKET_FIELDS,
  DEMAND_CENTER_FIELDS,
  DEMAND_CATEGORY_FIELDS,
  NEARBY_HOTEL_SUPPLY_FIELDS,
  MARKET_DEMAND_SNAPSHOT_FIELDS,
  SNAPSHOT_SCORE_FIELD_TO_KEY,
  DEALS_MARKET_DEMAND_FIELDS,
} from "./airtable-market-demand-fields.js";
import { extractDealRecordIdsFromFields } from "./deal-link-helpers.js";

function strVal(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (v instanceof Date) return v.toISOString();
  return String(v).trim();
}

function numVal(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function linkedIds(v) {
  if (!Array.isArray(v)) return [];
  return v.filter((id) => typeof id === "string" && id.startsWith("rec"));
}

function multiSelect(v) {
  if (!Array.isArray(v)) {
    const s = strVal(v);
    return s ? [s] : [];
  }
  return v.map((x) => strVal(x)).filter(Boolean);
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 */
export function normalizeMarketRecord(record) {
  const f = record?.fields || {};
  return {
    id: record?.id || "",
    name: strVal(f[MARKET_FIELDS.name]),
    country: strVal(f[MARKET_FIELDS.country]),
    region: strVal(f[MARKET_FIELDS.region]),
    subregion: strVal(f[MARKET_FIELDS.subregion]),
    marketType: strVal(f[MARKET_FIELDS.marketType]),
    latitude: numVal(f[MARKET_FIELDS.latitude]),
    longitude: numVal(f[MARKET_FIELDS.longitude]),
    primaryDemandProfile: strVal(f[MARKET_FIELDS.primaryDemandProfile]),
    marketNotes: strVal(f[MARKET_FIELDS.marketNotes]),
    dataConfidence: strVal(f[MARKET_FIELDS.dataConfidence]),
    lastReviewed: strVal(f[MARKET_FIELDS.lastReviewed]),
  };
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 */
export function normalizeDemandCenterRecord(record) {
  const f = record?.fields || {};
  return {
    id: record?.id || "",
    name: strVal(f[DEMAND_CENTER_FIELDS.name]),
    linkedMarketIds: linkedIds(f[DEMAND_CENTER_FIELDS.linkedMarket]),
    linkedDealIds: extractDealRecordIdsFromFields(f, [DEMAND_CENTER_FIELDS.linkedDeals]),
    category: strVal(f[DEMAND_CENTER_FIELDS.demandCategory]),
    subcategory: strVal(f[DEMAND_CENTER_FIELDS.demandSubcategory]),
    address: strVal(f[DEMAND_CENTER_FIELDS.address]),
    latitude: numVal(f[DEMAND_CENTER_FIELDS.latitude]),
    longitude: numVal(f[DEMAND_CENTER_FIELDS.longitude]),
    distanceFromDeal: numVal(f[DEMAND_CENTER_FIELDS.distanceFromDeal]),
    estimatedDriveTime: numVal(f[DEMAND_CENTER_FIELDS.estimatedDriveTime]),
    demandStrength: strVal(f[DEMAND_CENTER_FIELDS.demandStrength]),
    relevanceToHotelDemand: strVal(f[DEMAND_CENTER_FIELDS.relevanceToHotelDemand]),
    demandPattern: multiSelect(f[DEMAND_CENTER_FIELDS.demandPattern]),
    relevantHotelTypes: multiSelect(f[DEMAND_CENTER_FIELDS.relevantHotelTypes]),
    source: multiSelect(f[DEMAND_CENTER_FIELDS.source]),
    sourceReference: strVal(f[DEMAND_CENTER_FIELDS.sourceReference]),
    sourcePlaceId: strVal(f[DEMAND_CENTER_FIELDS.sourcePlaceId]),
    dataConfidence: strVal(f[DEMAND_CENTER_FIELDS.dataConfidence]),
    lastVerified: strVal(f[DEMAND_CENTER_FIELDS.lastVerified]),
    notes: strVal(f[DEMAND_CENTER_FIELDS.notes]),
    aiInterpretation: strVal(f[DEMAND_CENTER_FIELDS.aiInterpretation]),
    relevanceScore: numVal(f[DEMAND_CENTER_FIELDS.relevanceScore]),
  };
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 */
export function normalizeDemandCategoryRecord(record) {
  const f = record?.fields || {};
  return {
    id: record?.id || "",
    category: strVal(f[DEMAND_CATEGORY_FIELDS.category]),
    description: strVal(f[DEMAND_CATEGORY_FIELDS.description]),
    typicalDemandPattern: multiSelect(f[DEMAND_CATEGORY_FIELDS.typicalDemandPattern]),
    mostRelevantHotelTypes: multiSelect(f[DEMAND_CATEGORY_FIELDS.mostRelevantHotelTypes]),
    brandFitImplications: strVal(f[DEMAND_CATEGORY_FIELDS.brandFitImplications]),
    operatorFitImplications: strVal(f[DEMAND_CATEGORY_FIELDS.operatorFitImplications]),
    scoringWeight: numVal(f[DEMAND_CATEGORY_FIELDS.scoringWeight]),
  };
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 */
export function normalizeNearbyHotelSupplyRecord(record) {
  const f = record?.fields || {};
  const linkedDealIds = extractDealRecordIdsFromFields(f, [NEARBY_HOTEL_SUPPLY_FIELDS.linkedDeal]);
  return {
    id: record?.id || "",
    hotelName: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.hotelName]),
    linkedMarketIds: linkedIds(f[NEARBY_HOTEL_SUPPLY_FIELDS.linkedMarket]),
    linkedDealIds,
    brand: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.brand]),
    parentCompany: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.parentCompany]),
    chainScale: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.chainScale]),
    hotelType: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.hotelType]),
    rooms: numVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.rooms]),
    distanceFromDeal: numVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.distanceFromDeal]),
    estimatedDriveTime: numVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.estimatedDriveTime]),
    competitiveRelevance: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.competitiveRelevance]),
    source: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.source]),
    dataConfidence: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.dataConfidence]),
    notes: strVal(f[NEARBY_HOTEL_SUPPLY_FIELDS.notes]),
  };
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 */
export function normalizeMarketDemandSnapshotRecord(record) {
  const f = record?.fields || {};
  const scores = {
    leisure: null,
    corporate: null,
    group: null,
    medical: null,
    education: null,
    transportation: null,
    industrial: null,
    retailMixedUse: null,
    government: null,
  };
  for (const [field, key] of Object.entries(SNAPSHOT_SCORE_FIELD_TO_KEY)) {
    scores[key] = numVal(f[field]);
  }
  const linkedDealIds = extractDealRecordIdsFromFields(f, [MARKET_DEMAND_SNAPSHOT_FIELDS.linkedDeal]);
  return {
    id: record?.id || "",
    snapshotName: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.snapshotName]),
    linkedDealIds,
    linkedMarketIds: linkedIds(f[MARKET_DEMAND_SNAPSHOT_FIELDS.linkedMarket]),
    scores,
    overallDemandStrength: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.overallDemandStrength]),
    primaryDemandProfile: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.primaryDemandProfile]),
    demandSummary: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.demandSummary]),
    demandGaps: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.demandGaps]),
    brandImplications: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.brandImplications]),
    operatorImplications: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.operatorImplications]),
    recommendedFollowUp: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.recommendedFollowUp]),
    dataConfidence: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.dataConfidence]),
    lastGenerated: strVal(f[MARKET_DEMAND_SNAPSHOT_FIELDS.lastGenerated]),
  };
}

/**
 * Extract optional Deals-table market demand fields when present.
 * @param {Record<string, unknown>} fields
 */
export function extractDealsMarketDemandFields(fields) {
  const f = fields || {};
  const out = {};
  const count = numVal(f[DEALS_MARKET_DEMAND_FIELDS.demandCenterCount]);
  if (count != null) out.demandCenterCount = count;
  const summary = strVal(f[DEALS_MARKET_DEMAND_FIELDS.demandSummary]);
  if (summary) out.demandSummary = summary;
  const strength = strVal(f[DEALS_MARKET_DEMAND_FIELDS.demandStrengthScore]);
  if (strength) out.demandStrengthScore = strength;
  const confidence = strVal(f[DEALS_MARKET_DEMAND_FIELDS.demandConfidence]);
  if (confidence) out.demandConfidence = confidence;
  const drivers = strVal(f[DEALS_MARKET_DEMAND_FIELDS.primaryDemandDrivers]);
  if (drivers) out.primaryDemandDrivers = drivers;
  const gaps = strVal(f[DEALS_MARKET_DEMAND_FIELDS.demandGapsQuestions]);
  if (gaps) out.demandGapsQuestions = gaps;
  const marketIds = linkedIds(f[DEALS_MARKET_DEMAND_FIELDS.linkedMarket]);
  if (marketIds.length) out.linkedMarketIds = marketIds;
  const marketText = strVal(f[DEALS_MARKET_DEMAND_FIELDS.linkedMarketRecordId]);
  if (marketText.startsWith("rec")) out.linkedMarketRecordId = marketText;
  return out;
}
