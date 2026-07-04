/**
 * Scout Phase 2 — Opportunity signal generation (read-only Hotel Census).
 * Transparent, explainable signals; no Airtable writes.
 */

import {
  CENSUS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "../hotel-census/fields.js";
import {
  exactMatchKey,
  normalizeParentCompanyKey,
  resolveBrandAffiliationMatchers,
} from "../hotel-census/brand-alias-resolve.js";
import { HOTEL_CENSUS_TABLE, getPlatformBase } from "../hotel-census/platform-base.js";
import { fetchScoutCensusRecords } from "./census-read-cache.js";

export const SIGNAL_TYPES = [
  "parent_company_market_gap",
  "brand_market_gap",
  "independent_conversion_cluster",
  "large_independent_asset",
  "pipeline_activity",
  "rebrand_candidate",
  "operator_opportunity_market",
];

/** Scout signal read fields — Market/Submarket are official STR geography columns. */
export const SCOUT_SIGNAL_FIELDS = {
  ...CENSUS_FIELDS,
  exAffiliation: "Ex-Affiliation",
  exAffiliation2: "Ex-Affiliation 2",
  openDate: "Open Date",
  projectedOpenDate: "projected_open_date",
  latitude: "Latitude",
  longitude: "Longitude",
};

const CORE_SELECT_FIELDS = [
  SCOUT_SIGNAL_FIELDS.name,
  SCOUT_SIGNAL_FIELDS.affiliation,
  SCOUT_SIGNAL_FIELDS.parentCompany,
  SCOUT_SIGNAL_FIELDS.status,
  SCOUT_SIGNAL_FIELDS.rooms,
  SCOUT_SIGNAL_FIELDS.country,
  SCOUT_SIGNAL_FIELDS.city,
  SCOUT_SIGNAL_FIELDS.market,
  SCOUT_SIGNAL_FIELDS.submarket,
  SCOUT_SIGNAL_FIELDS.chainScale,
  SCOUT_SIGNAL_FIELDS.location,
  SCOUT_SIGNAL_FIELDS.operationType,
  SCOUT_SIGNAL_FIELDS.managementCompany,
  SCOUT_SIGNAL_FIELDS.projectPhase,
  SCOUT_SIGNAL_FIELDS.latitude,
  SCOUT_SIGNAL_FIELDS.longitude,
];

const OPTIONAL_SELECT_FIELDS = [
  SCOUT_SIGNAL_FIELDS.exAffiliation,
  SCOUT_SIGNAL_FIELDS.exAffiliation2,
  SCOUT_SIGNAL_FIELDS.openDate,
  SCOUT_SIGNAL_FIELDS.projectedOpenDate,
];

const DEFAULT_LIMIT = 100;
const DEFAULT_MIN_ROOMS_LARGE_ASSET = 100;
const INDEPENDENT_MARKET_THRESHOLD = 10;
const INDEPENDENT_SUBMARKET_THRESHOLD = 5;
const OPERATOR_INDEPENDENT_MARKET_THRESHOLD = 8;
const OPERATOR_FRANCHISE_MARKET_THRESHOLD = 5;

/** Transparent priority scoring (cap 100). */
export const SCOUT_SIGNAL_SCORING = {
  marketSubmarketPopulated: 10,
  independentConversionRelevance: 20,
  strongMarketActivity: 15,
  pipelineActivity: 10,
  roomsAboveThreshold: 10,
  parentBrandGap: 20,
  operatorOwnershipInfo: 10,
  dataCompleteness: 5,
};

function parseRooms(value) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function normalizeChainScale(raw) {
  const s = exactMatchKey(raw);
  if (!s) return "Unknown";
  return s.replace(/\s+chain\s*$/i, "").trim() || s;
}

function normalizeStatus(raw) {
  const s = exactMatchKey(raw);
  if (!s) return "";
  const lower = s.toLowerCase();
  if (lower === "open") return STATUS_OPEN;
  if (lower === "pipeline") return STATUS_PIPELINE;
  return s;
}

function normalizeFilterText(value) {
  return exactMatchKey(value).toLowerCase();
}

function textMatches(rowValue, filterValue) {
  if (!filterValue) return true;
  return normalizeFilterText(rowValue) === normalizeFilterText(filterValue);
}

function parentCompanyMatches(rowParent, filterParent) {
  if (!filterParent) return true;
  return normalizeParentCompanyKey(rowParent) === normalizeParentCompanyKey(filterParent);
}

function isUnknownFieldError(err) {
  return /unknown field name/i.test(err?.message || String(err));
}

function isFranchiseOperation(operationType) {
  return /franchise/i.test(operationType || "");
}

function isChainManagementOperation(operationType) {
  const s = (operationType || "").toLowerCase();
  return s.includes("managed") || s.includes("management");
}

function slugPart(value) {
  return String(value || "unknown")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 48);
}

function makeSignalId(signalType, parts) {
  return `scout-${signalType}-${parts.map(slugPart).filter(Boolean).join("-")}`;
}

function sumScore(factors) {
  return Math.min(100, factors.reduce((s, n) => s + n, 0));
}

function levelFromScore(score, highAt = 70, mediumAt = 45) {
  if (score >= highAt) return "High";
  if (score >= mediumAt) return "Medium";
  return "Low";
}

function geoScoreFactors(rowOrBucket) {
  const factors = [];
  if (rowOrBucket.market) factors.push(SCOUT_SIGNAL_SCORING.marketSubmarketPopulated);
  if (rowOrBucket.submarket) factors.push(SCOUT_SIGNAL_SCORING.dataCompleteness);
  return factors;
}

function mapCensusRecord(rec, fieldsLoaded) {
  const f = rec.fields || {};
  const affiliation = exactMatchKey(f[SCOUT_SIGNAL_FIELDS.affiliation]);
  const market = exactMatchKey(f[SCOUT_SIGNAL_FIELDS.market]);
  const submarket = exactMatchKey(f[SCOUT_SIGNAL_FIELDS.submarket]);
  const exAffiliation = fieldsLoaded.has(SCOUT_SIGNAL_FIELDS.exAffiliation)
    ? exactMatchKey(f[SCOUT_SIGNAL_FIELDS.exAffiliation])
    : "";
  const exAffiliation2 = fieldsLoaded.has(SCOUT_SIGNAL_FIELDS.exAffiliation2)
    ? exactMatchKey(f[SCOUT_SIGNAL_FIELDS.exAffiliation2])
    : "";

  return {
    id: rec.id,
    name: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.name]),
    affiliation,
    parentCompany: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.parentCompany]),
    status: normalizeStatus(f[SCOUT_SIGNAL_FIELDS.status]),
    rooms: parseRooms(f[SCOUT_SIGNAL_FIELDS.rooms]),
    country: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.country]),
    city: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.city]),
    market,
    submarket,
    chainScale: normalizeChainScale(f[SCOUT_SIGNAL_FIELDS.chainScale]),
    locationType: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.location]) || "Unknown",
    operationType: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.operationType]),
    managementCompany: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.managementCompany]),
    projectPhase: exactMatchKey(f[SCOUT_SIGNAL_FIELDS.projectPhase]),
    exAffiliation,
    exAffiliation2,
    openDate: fieldsLoaded.has(SCOUT_SIGNAL_FIELDS.openDate)
      ? exactMatchKey(f[SCOUT_SIGNAL_FIELDS.openDate])
      : "",
    projectedOpenDate: fieldsLoaded.has(SCOUT_SIGNAL_FIELDS.projectedOpenDate)
      ? exactMatchKey(f[SCOUT_SIGNAL_FIELDS.projectedOpenDate])
      : "",
    latitude: f[SCOUT_SIGNAL_FIELDS.latitude] ?? null,
    longitude: f[SCOUT_SIGNAL_FIELDS.longitude] ?? null,
    isIndependent: affiliation === CENSUS_INDEPENDENT_AFFILIATION,
    isBranded: affiliation !== CENSUS_INDEPENDENT_AFFILIATION && affiliation !== "",
  };
}

async function loadCensusRows(base, geoFilters = {}) {
  const warnings = [];
  const cached = await fetchScoutCensusRecords({
    country: geoFilters.country,
    market: geoFilters.market,
  });
  if (cached.warnings?.length) warnings.push(...cached.warnings);

  const fieldsLoaded = new Set(CORE_SELECT_FIELDS);
  for (const f of cached.fieldsLoaded || []) {
    if (OPTIONAL_SELECT_FIELDS.includes(f)) fieldsLoaded.add(f);
    if (CORE_SELECT_FIELDS.includes(f)) fieldsLoaded.add(f);
  }

  const rows = cached.records.map((rec) => mapCensusRecord(rec, fieldsLoaded));
  return {
    rows,
    warnings,
    fieldsLoaded: [...fieldsLoaded],
    censusRecordsLoaded: cached.records.length,
  };
}

export function parseOpportunitySignalFilters(query = {}) {
  const includePipeline =
    query.includePipeline === "1" ||
    query.includePipeline === "true" ||
    query.includePipeline === true;

  const limitRaw = parseInt(query.limit, 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 500) : DEFAULT_LIMIT;

  const minRoomsRaw = parseInt(query.minRooms, 10);
  const minRooms =
    Number.isFinite(minRoomsRaw) && minRoomsRaw > 0 ? minRoomsRaw : DEFAULT_MIN_ROOMS_LARGE_ASSET;

  const signalType = exactMatchKey(query.signalType);
  if (signalType && !SIGNAL_TYPES.includes(signalType)) {
    return { ok: false, error: `Invalid signalType. Allowed: ${SIGNAL_TYPES.join(", ")}` };
  }

  return {
    ok: true,
    filters: {
      country: exactMatchKey(query.country),
      market: exactMatchKey(query.market),
      submarket: exactMatchKey(query.submarket),
      parentCompany: exactMatchKey(query.parentCompany || query.parent_company),
      brand: exactMatchKey(query.brand),
      chainScale: exactMatchKey(query.chainScale || query.chain_scale),
      locationType: exactMatchKey(query.locationType || query.location_type),
      signalType,
      minRooms,
      includePipeline,
      limit,
    },
  };
}

function rowInGeoScope(row, geoFilters) {
  if (!textMatches(row.country, geoFilters.country)) return false;
  if (!textMatches(row.market, geoFilters.market)) return false;
  if (!textMatches(row.submarket, geoFilters.submarket)) return false;
  if (!textMatches(row.chainScale, geoFilters.chainScale)) return false;
  if (!textMatches(row.locationType, geoFilters.locationType)) return false;
  return true;
}

function geoScopeRows(rows, filters) {
  const geoFilters = {
    country: filters.country,
    market: filters.market,
    submarket: filters.submarket,
    chainScale: filters.chainScale,
    locationType: filters.locationType,
  };
  return rows.filter(
    (row) =>
      rowInGeoScope(row, geoFilters) &&
      (row.status === STATUS_OPEN || (row.status === STATUS_PIPELINE && filters.includePipeline))
  );
}

function buildSignalBase({
  signalType,
  title,
  country,
  city,
  market,
  submarket,
  linkedHotelRecordId,
  hotelName,
  parentCompany,
  brand,
  reason,
  supportingMetrics,
  recommendedAction,
  scoreFactors,
  generatedAt,
}) {
  const priorityScore = sumScore(scoreFactors);
  const confidence = levelFromScore(priorityScore);
  const actionability =
    signalType === "parent_company_market_gap" ||
    signalType === "brand_market_gap" ||
    signalType === "large_independent_asset" ||
    signalType === "rebrand_candidate"
      ? levelFromScore(priorityScore, 60, 40)
      : levelFromScore(priorityScore, 65, 45);

  return {
    signalId: makeSignalId(signalType, [
      linkedHotelRecordId || market || country,
      submarket,
      parentCompany,
      brand,
      hotelName,
    ]),
    signalType,
    title,
    country: country || "",
    city: city || "",
    market: market || "",
    submarket: submarket || "",
    linkedHotelRecordId: linkedHotelRecordId || null,
    hotelName: hotelName || null,
    parentCompany: parentCompany || null,
    brand: brand || null,
    confidence,
    actionability,
    priorityScore,
    reason,
    supportingMetrics,
    recommendedAction,
    source: {
      table: HOTEL_CENSUS_TABLE,
      readOnly: true,
      generatedAt,
    },
  };
}

function aggregateGeoBuckets(rows) {
  const byMarket = new Map();
  const bySubmarket = new Map();

  function ensureBucket(map, key, row) {
    if (!map.has(key)) {
      map.set(key, {
        key,
        country: row.country,
        market: row.market,
        submarket: row.submarket,
        openRows: [],
        pipelineRows: [],
        chainScales: new Map(),
        parentCompanies: new Set(),
        brands: new Set(),
        managementCompanies: new Set(),
        projectPhases: new Map(),
      });
    }
    return map.get(key);
  }

  for (const row of rows) {
    const marketKey = row.market || "Unknown";
    const submarketKey = row.submarket ? `${row.market}::${row.submarket}` : "";

    for (const [map, key, bucketMarket, bucketSubmarket] of [
      [byMarket, marketKey, row.market, ""],
      ...(submarketKey ? [[bySubmarket, submarketKey, row.market, row.submarket]] : []),
    ]) {
      const bucket = ensureBucket(map, key, row);
      bucket.market = bucketMarket;
      bucket.submarket = bucketSubmarket;
      if (row.status === STATUS_OPEN) bucket.openRows.push(row);
      if (row.status === STATUS_PIPELINE) bucket.pipelineRows.push(row);
      if (row.chainScale && row.chainScale !== "Unknown") {
        bucket.chainScales.set(row.chainScale, (bucket.chainScales.get(row.chainScale) || 0) + 1);
      }
      if (row.parentCompany) bucket.parentCompanies.add(normalizeParentCompanyKey(row.parentCompany));
      if (row.affiliation) bucket.brands.add(row.affiliation);
      if (row.managementCompany) bucket.managementCompanies.add(row.managementCompany);
      if (row.projectPhase) {
        bucket.projectPhases.set(row.projectPhase, (bucket.projectPhases.get(row.projectPhase) || 0) + 1);
      }
    }
  }

  return { byMarket, bySubmarket };
}

function bucketMetrics(bucket) {
  const openBranded = bucket.openRows.filter((r) => r.isBranded);
  const openIndependent = bucket.openRows.filter((r) => r.isIndependent);
  const franchiseOpen = bucket.openRows.filter((r) => isFranchiseOperation(r.operationType));
  const chainMgmtOpen = bucket.openRows.filter((r) => isChainManagementOperation(r.operationType));

  return {
    openHotels: bucket.openRows.length,
    openRooms: bucket.openRows.reduce((s, r) => s + r.rooms, 0),
    brandedOpenHotels: openBranded.length,
    independentOpenHotels: openIndependent.length,
    independentOpenRooms: openIndependent.reduce((s, r) => s + r.rooms, 0),
    pipelineHotels: bucket.pipelineRows.length,
    pipelineRooms: bucket.pipelineRows.reduce((s, r) => s + r.rooms, 0),
    distinctParentCompanies: bucket.parentCompanies.size,
    distinctBrands: bucket.brands.size,
    chainScaleMix: Object.fromEntries(bucket.chainScales),
    franchiseHotels: franchiseOpen.length,
    chainManagementHotels: chainMgmtOpen.length,
    managementCompanyCount: bucket.managementCompanies.size,
    projectPhaseBreakdown: Object.fromEntries(bucket.projectPhases),
    topIndependentSample: openIndependent
      .sort((a, b) => b.rooms - a.rooms)
      .slice(0, 5)
      .map((r) => ({ id: r.id, name: r.name, rooms: r.rooms, city: r.city })),
  };
}

function generateParentCompanyMarketGaps(scope, filters, generatedAt) {
  if (!filters.parentCompany) return [];

  const parentNorm = normalizeParentCompanyKey(filters.parentCompany);
  const signals = [];
  const { byMarket, bySubmarket } = aggregateGeoBuckets(scope);

  for (const bucket of [...byMarket.values(), ...bySubmarket.values()]) {
    if (bucket.key === "Unknown") continue;
    const metrics = bucketMetrics(bucket);
    if (metrics.brandedOpenHotels === 0) continue;

    const parentOpen = bucket.openRows.filter(
      (r) => normalizeParentCompanyKey(r.parentCompany) === parentNorm
    ).length;
    if (parentOpen > 0) continue;

    const geoLabel = bucket.submarket || bucket.market;
    const scoreFactors = [
      SCOUT_SIGNAL_SCORING.parentBrandGap,
      ...geoScoreFactors(bucket),
      metrics.brandedOpenHotels >= 5 ? SCOUT_SIGNAL_SCORING.strongMarketActivity : 0,
      metrics.independentOpenHotels >= INDEPENDENT_MARKET_THRESHOLD
        ? SCOUT_SIGNAL_SCORING.independentConversionRelevance
        : 0,
      metrics.pipelineHotels > 0 ? SCOUT_SIGNAL_SCORING.pipelineActivity : 0,
    ].filter(Boolean);

    signals.push(
      buildSignalBase({
        signalType: "parent_company_market_gap",
        title: `${filters.parentCompany} gap — ${geoLabel}`,
        country: bucket.country,
        market: bucket.market,
        submarket: bucket.submarket,
        parentCompany: filters.parentCompany,
        reason: `${filters.parentCompany} has no open hotels in ${bucket.submarket ? `Submarket ${bucket.submarket}` : `Market ${bucket.market}`} while ${metrics.brandedOpenHotels} other branded hotel(s) operate here.`,
        supportingMetrics: {
          otherBrandedOpenHotels: metrics.brandedOpenHotels,
          independentOpenHotels: metrics.independentOpenHotels,
          pipelineHotels: metrics.pipelineHotels,
          distinctParentCompanies: metrics.distinctParentCompanies,
          chainScaleMix: metrics.chainScaleMix,
        },
        recommendedAction: "Review competitive set and evaluate greenfield, conversion, or franchise growth in this STR geography.",
        scoreFactors,
        generatedAt,
      })
    );
  }

  return signals;
}

function generateBrandMarketGaps(scope, filters, brandResolution, generatedAt) {
  if (!filters.brand || !brandResolution?.affiliationMatchers?.length) return [];

  const brandMatchers = new Set(brandResolution.affiliationMatchers);
  const parentNorm = normalizeParentCompanyKey(filters.parentCompany);
  const brandChainScales = new Set(
    scope
      .filter((r) => r.status === STATUS_OPEN && brandMatchers.has(r.affiliation))
      .map((r) => r.chainScale)
      .filter((c) => c && c !== "Unknown")
  );

  const signals = [];
  const { byMarket, bySubmarket } = aggregateGeoBuckets(scope);

  for (const bucket of [...byMarket.values(), ...bySubmarket.values()]) {
    if (bucket.key === "Unknown") continue;
    const metrics = bucketMetrics(bucket);
    const brandOpen = bucket.openRows.filter((r) => brandMatchers.has(r.affiliation)).length;
    if (brandOpen > 0) continue;

    const parentOpen = parentNorm
      ? bucket.openRows.filter(
          (r) => normalizeParentCompanyKey(r.parentCompany) === parentNorm && r.isBranded
        ).length
      : 0;

    const comparableChainScale = [...brandChainScales].filter((scale) => metrics.chainScaleMix[scale]).length;
    if (!parentOpen && comparableChainScale === 0) continue;

    const geoLabel = bucket.submarket || bucket.market;
    const scoreFactors = [
      SCOUT_SIGNAL_SCORING.parentBrandGap,
      ...geoScoreFactors(bucket),
      parentOpen > 0 ? SCOUT_SIGNAL_SCORING.strongMarketActivity : SCOUT_SIGNAL_SCORING.dataCompleteness,
      comparableChainScale > 0 ? SCOUT_SIGNAL_SCORING.strongMarketActivity : 0,
      metrics.pipelineHotels > 0 ? SCOUT_SIGNAL_SCORING.pipelineActivity : 0,
    ].filter(Boolean);

    signals.push(
      buildSignalBase({
        signalType: "brand_market_gap",
        title: `${brandResolution.canonicalBrandName || filters.brand} gap — ${geoLabel}`,
        country: bucket.country,
        market: bucket.market,
        submarket: bucket.submarket,
        parentCompany: filters.parentCompany || null,
        brand: brandResolution.canonicalBrandName || filters.brand,
        reason: parentOpen
          ? `Brand has no open hotels here; parent company has ${parentOpen} branded open hotel(s) in this geography.`
          : `Brand has no open hotels here; comparable chain scale presence detected (${[...brandChainScales].join(", ")}).`,
        supportingMetrics: {
          parentCompanyHotelsInMarket: parentOpen,
          comparableChainScaleHotels: comparableChainScale,
          independentOpenHotels: metrics.independentOpenHotels,
          pipelineHotels: metrics.pipelineHotels,
          chainScaleMix: metrics.chainScaleMix,
        },
        recommendedAction: "Validate brand fit and prioritize outreach for conversion or new build in this market.",
        scoreFactors,
        generatedAt,
      })
    );
  }

  return signals;
}

function generateIndependentConversionClusters(scope, generatedAt) {
  const signals = [];
  const { byMarket, bySubmarket } = aggregateGeoBuckets(scope);

  for (const bucket of byMarket.values()) {
    const metrics = bucketMetrics(bucket);
    if (metrics.independentOpenHotels < INDEPENDENT_MARKET_THRESHOLD) continue;

    const avgRooms =
      metrics.independentOpenHotels > 0
        ? Math.round(metrics.independentOpenRooms / metrics.independentOpenHotels)
        : 0;

    signals.push(
      buildSignalBase({
        signalType: "independent_conversion_cluster",
        title: `Independent cluster — ${bucket.market}`,
        country: bucket.country,
        market: bucket.market,
        reason: `Market has ${metrics.independentOpenHotels} open independent hotels (${metrics.independentOpenRooms} rooms).`,
        supportingMetrics: {
          independentHotelCount: metrics.independentOpenHotels,
          independentRooms: metrics.independentOpenRooms,
          averageRooms: avgRooms,
          topSampleHotels: metrics.topIndependentSample,
          brandedOpenHotels: metrics.brandedOpenHotels,
        },
        recommendedAction: "Build owner short-list and evaluate soft-brand or collection conversion plays.",
        scoreFactors: [
          SCOUT_SIGNAL_SCORING.independentConversionRelevance,
          ...geoScoreFactors(bucket),
          metrics.independentOpenHotels >= 20 ? SCOUT_SIGNAL_SCORING.strongMarketActivity : 0,
        ].filter(Boolean),
        generatedAt,
      })
    );
  }

  for (const bucket of bySubmarket.values()) {
    const metrics = bucketMetrics(bucket);
    if (metrics.independentOpenHotels < INDEPENDENT_SUBMARKET_THRESHOLD) continue;

    const avgRooms =
      metrics.independentOpenHotels > 0
        ? Math.round(metrics.independentOpenRooms / metrics.independentOpenHotels)
        : 0;

    signals.push(
      buildSignalBase({
        signalType: "independent_conversion_cluster",
        title: `Independent cluster — ${bucket.submarket}`,
        country: bucket.country,
        market: bucket.market,
        submarket: bucket.submarket,
        reason: `Submarket has ${metrics.independentOpenHotels} open independent hotels (${metrics.independentOpenRooms} rooms).`,
        supportingMetrics: {
          independentHotelCount: metrics.independentOpenHotels,
          independentRooms: metrics.independentOpenRooms,
          averageRooms: avgRooms,
          topSampleHotels: metrics.topIndependentSample,
          brandedOpenHotels: metrics.brandedOpenHotels,
        },
        recommendedAction: "Prioritize submarket for independent conversion outreach.",
        scoreFactors: [
          SCOUT_SIGNAL_SCORING.independentConversionRelevance,
          ...geoScoreFactors(bucket),
        ].filter(Boolean),
        generatedAt,
      })
    );
  }

  return signals;
}

function generateLargeIndependentAssets(scope, filters, generatedAt) {
  return scope
    .filter(
      (r) =>
        r.status === STATUS_OPEN &&
        r.isIndependent &&
        r.rooms >= filters.minRooms
    )
    .map((row) =>
      buildSignalBase({
        signalType: "large_independent_asset",
        title: `Large independent — ${row.name}`,
        country: row.country,
        city: row.city,
        market: row.market,
        submarket: row.submarket,
        linkedHotelRecordId: row.id,
        hotelName: row.name,
        reason: `Independent hotel with ${row.rooms} rooms exceeds threshold (${filters.minRooms}+).`,
        supportingMetrics: {
          rooms: row.rooms,
          chainScale: row.chainScale,
          locationType: row.locationType,
          market: row.market,
          submarket: row.submarket,
          city: row.city,
          country: row.country,
        },
        recommendedAction: "Evaluate for brand conversion, operator-led repositioning, or capital partner intro.",
        scoreFactors: [
          SCOUT_SIGNAL_SCORING.independentConversionRelevance,
          SCOUT_SIGNAL_SCORING.roomsAboveThreshold,
          ...geoScoreFactors(row),
          row.managementCompany ? SCOUT_SIGNAL_SCORING.operatorOwnershipInfo : 0,
        ].filter(Boolean),
        generatedAt,
      })
    );
}

function generatePipelineActivity(scope, generatedAt) {
  const signals = [];
  const { byMarket, bySubmarket } = aggregateGeoBuckets(scope);

  for (const bucket of [...byMarket.values(), ...bySubmarket.values()]) {
    const metrics = bucketMetrics(bucket);
    if (metrics.pipelineHotels === 0) continue;

    const geoLabel = bucket.submarket || bucket.market;
    const pipelineParents = [
      ...new Set(bucket.pipelineRows.map((r) => r.parentCompany).filter(Boolean)),
    ].slice(0, 10);
    const pipelineBrands = [...new Set(bucket.pipelineRows.map((r) => r.affiliation).filter(Boolean))].slice(
      0,
      10
    );

    signals.push(
      buildSignalBase({
        signalType: "pipeline_activity",
        title: `Pipeline activity — ${geoLabel}`,
        country: bucket.country,
        market: bucket.market,
        submarket: bucket.submarket,
        reason: `${metrics.pipelineHotels} pipeline hotel(s) (${metrics.pipelineRooms} rooms) in this geography.`,
        supportingMetrics: {
          pipelineHotelCount: metrics.pipelineHotels,
          pipelineRooms: metrics.pipelineRooms,
          projectPhaseBreakdown: metrics.projectPhaseBreakdown,
          parentCompaniesInPipeline: pipelineParents,
          brandsInPipeline: pipelineBrands,
        },
        recommendedAction: "Monitor competitive pipeline and adjust growth timing or conversion urgency.",
        scoreFactors: [
          SCOUT_SIGNAL_SCORING.pipelineActivity,
          ...geoScoreFactors(bucket),
          metrics.pipelineHotels >= 3 ? SCOUT_SIGNAL_SCORING.strongMarketActivity : 0,
        ].filter(Boolean),
        generatedAt,
      })
    );
  }

  return signals;
}

function generateRebrandCandidates(scope, fieldsLoaded, generatedAt) {
  if (
    !fieldsLoaded.has(SCOUT_SIGNAL_FIELDS.exAffiliation) &&
    !fieldsLoaded.has(SCOUT_SIGNAL_FIELDS.exAffiliation2)
  ) {
    return [];
  }

  return scope
    .filter((r) => r.status === STATUS_OPEN && (r.exAffiliation || r.exAffiliation2))
    .map((row) =>
      buildSignalBase({
        signalType: "rebrand_candidate",
        title: `Rebrand candidate — ${row.name}`,
        country: row.country,
        city: row.city,
        market: row.market,
        submarket: row.submarket,
        linkedHotelRecordId: row.id,
        hotelName: row.name,
        parentCompany: row.parentCompany,
        brand: row.affiliation,
        reason: `Hotel has prior affiliation on record (${row.exAffiliation || row.exAffiliation2}).`,
        supportingMetrics: {
          currentAffiliation: row.affiliation,
          exAffiliation: row.exAffiliation || null,
          exAffiliation2: row.exAffiliation2 || null,
          parentCompany: row.parentCompany,
          rooms: row.rooms,
          market: row.market,
          submarket: row.submarket,
        },
        recommendedAction: "Validate ownership intent and prior brand relationship for conversion outreach.",
        scoreFactors: [
          SCOUT_SIGNAL_SCORING.independentConversionRelevance,
          ...geoScoreFactors(row),
          row.rooms >= 80 ? SCOUT_SIGNAL_SCORING.roomsAboveThreshold : 0,
          row.parentCompany ? SCOUT_SIGNAL_SCORING.operatorOwnershipInfo : 0,
        ].filter(Boolean),
        generatedAt,
      })
    );
}

function generateOperatorOpportunityMarkets(scope, generatedAt) {
  const signals = [];
  const { byMarket, bySubmarket } = aggregateGeoBuckets(scope);

  for (const bucket of [...byMarket.values(), ...bySubmarket.values()]) {
    const metrics = bucketMetrics(bucket);
    const franchiseHeavy = metrics.franchiseHotels >= OPERATOR_FRANCHISE_MARKET_THRESHOLD;
    const independentHeavy = metrics.independentOpenHotels >= OPERATOR_INDEPENDENT_MARKET_THRESHOLD;
    const mgmtDiverse =
      metrics.managementCompanyCount >= 3 && metrics.independentOpenHotels >= 5;

    if (!franchiseHeavy && !independentHeavy && !mgmtDiverse) continue;

    const geoLabel = bucket.submarket || bucket.market;
    signals.push(
      buildSignalBase({
        signalType: "operator_opportunity_market",
        title: `Operator opportunity — ${geoLabel}`,
        country: bucket.country,
        market: bucket.market,
        submarket: bucket.submarket,
        reason: franchiseHeavy
          ? `Franchise-heavy supply (${metrics.franchiseHotels} franchise-operation hotels) suggests third-party operator demand.`
          : independentHeavy
            ? `High independent supply (${metrics.independentOpenHotels} hotels) may benefit from professional operator partnerships.`
            : `Diverse management landscape (${metrics.managementCompanyCount} management companies) with independent supply.`,
        supportingMetrics: {
          independentHotels: metrics.independentOpenHotels,
          franchiseHotels: metrics.franchiseHotels,
          chainManagementHotels: metrics.chainManagementHotels,
          managementCompanyCount: metrics.managementCompanyCount,
          brandedOpenHotels: metrics.brandedOpenHotels,
        },
        recommendedAction: "Screen operator partners with CALA franchise and independent conversion experience.",
        scoreFactors: [
          SCOUT_SIGNAL_SCORING.operatorOwnershipInfo,
          ...geoScoreFactors(bucket),
          independentHeavy ? SCOUT_SIGNAL_SCORING.independentConversionRelevance : 0,
          franchiseHeavy ? SCOUT_SIGNAL_SCORING.strongMarketActivity : 0,
        ].filter(Boolean),
        generatedAt,
      })
    );
  }

  return signals;
}

function buildSummary(signals) {
  const bySignalType = new Map();
  const byCountry = new Map();
  const byMarket = new Map();

  for (const sig of signals) {
    bySignalType.set(sig.signalType, (bySignalType.get(sig.signalType) || 0) + 1);
    if (sig.country) byCountry.set(sig.country, (byCountry.get(sig.country) || 0) + 1);
    if (sig.market) byMarket.set(sig.market, (byMarket.get(sig.market) || 0) + 1);
  }

  const toSorted = (map) =>
    [...map.entries()]
      .map(([label, count]) => ({ label, count }))
      .sort((a, b) => b.count - a.count);

  const averagePriorityScore =
    signals.length > 0
      ? Math.round((signals.reduce((s, sig) => s + sig.priorityScore, 0) / signals.length) * 10) / 10
      : 0;

  return {
    signalsReturned: signals.length,
    bySignalType: toSorted(bySignalType),
    byCountry: toSorted(byCountry),
    byMarket: toSorted(byMarket),
    averagePriorityScore,
  };
}

/**
 * @param {Record<string, string|boolean>} [query]
 */
export async function buildOpportunitySignalsReport(query = {}) {
  const base = getPlatformBase();
  if (!base) {
    return { ok: false, error: "Platform base not configured" };
  }

  const parsed = parseOpportunitySignalFilters(query);
  if (!parsed.ok) {
    return { ok: false, error: parsed.error };
  }

  const filters = parsed.filters;
  const warnings = [];
  const generatedAt = new Date().toISOString();

  let brandResolution = null;
  let brandMatcherSet = null;
  if (filters.brand) {
    brandResolution = await resolveBrandAffiliationMatchers(filters.brand, filters.parentCompany || null);
    if (!brandResolution.ok) {
      return { ok: false, error: brandResolution.error };
    }
    brandMatcherSet = new Set(brandResolution.affiliationMatchers);
    warnings.push(...(brandResolution.warnings || []));
  }

  let rows;
  let fieldsLoaded;
  let censusRecordsLoaded;
  try {
    const loaded = await loadCensusRows(base, filters);
    rows = loaded.rows;
    fieldsLoaded = new Set(loaded.fieldsLoaded);
    censusRecordsLoaded = loaded.censusRecordsLoaded;
    warnings.push(...loaded.warnings);
  } catch (err) {
    return { ok: false, error: err?.message || String(err) };
  }

  const geoScope = geoScopeRows(rows, filters);

  const generators = {
    parent_company_market_gap: () => generateParentCompanyMarketGaps(geoScope, filters, generatedAt),
    brand_market_gap: () => generateBrandMarketGaps(geoScope, filters, brandResolution, generatedAt),
    independent_conversion_cluster: () => generateIndependentConversionClusters(geoScope, generatedAt),
    large_independent_asset: () => generateLargeIndependentAssets(geoScope, filters, generatedAt),
    pipeline_activity: () => generatePipelineActivity(geoScope, generatedAt),
    rebrand_candidate: () => generateRebrandCandidates(geoScope, fieldsLoaded, generatedAt),
    operator_opportunity_market: () => generateOperatorOpportunityMarkets(geoScope, generatedAt),
  };

  let signals = [];
  const typesToRun = filters.signalType ? [filters.signalType] : SIGNAL_TYPES;
  for (const type of typesToRun) {
    signals = signals.concat(generators[type]());
  }

  signals.sort((a, b) => b.priorityScore - a.priorityScore || a.signalType.localeCompare(b.signalType));
  signals = signals.slice(0, filters.limit);

  if (!filters.signalType) {
    if (!filters.parentCompany) {
      warnings.push("PARENT_COMPANY_REQUIRED: parent_company_market_gap signals need parentCompany filter.");
    }
    if (!filters.brand) {
      warnings.push("BRAND_REQUIRED: brand_market_gap signals need brand filter.");
    }
  }

  return {
    ok: true,
    filters: {
      ...filters,
      brandResolution: brandResolution
        ? {
            requestedBrand: brandResolution.requestedBrand,
            canonicalBrandName: brandResolution.canonicalBrandName,
            affiliationMatchers: brandResolution.affiliationMatchers,
            usedAliasTable: brandResolution.usedAliasTable,
          }
        : null,
    },
    summary: buildSummary(signals),
    signals,
    warnings,
    source: {
      base: "Deal Capture Platform",
      table: HOTEL_CENSUS_TABLE,
      readOnly: true,
      writes: false,
      marketField: CENSUS_FIELDS.market,
      submarketField: CENSUS_FIELDS.submarket,
      fieldsLoaded: [...fieldsLoaded],
      censusRecordsLoaded,
      censusRecordsInScope: geoScope.length,
      scoringModel: SCOUT_SIGNAL_SCORING,
      generatedAt,
    },
  };
}
