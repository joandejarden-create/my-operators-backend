/**
 * Scout Phase 1 — Market Coverage Intelligence (read-only Hotel Census).
 * No Airtable writes. Reuses census field mappings and brand alias resolution.
 *
 * STR geography rule: Excel STR Market / STR Submarket populate Hotel Census
 * `Market` and `Submarket` — there are no separate STR Market / STR Submarket columns.
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

/**
 * API / Scout logical keys → official Hotel Census Airtable field names.
 * Query params `strMarket` and `market` both filter `{Market}`; same for submarket.
 */
export const SCOUT_STR_GEOGRAPHY_MAPPING = {
  strMarket: CENSUS_FIELDS.market,
  strSubmarket: CENSUS_FIELDS.submarket,
  market: CENSUS_FIELDS.market,
  submarket: CENSUS_FIELDS.submarket,
};

export const SCOUT_CENSUS_FIELDS = {
  ...CENSUS_FIELDS,
  latitude: "Latitude",
  longitude: "Longitude",
};

const SELECT_FIELDS = [
  SCOUT_CENSUS_FIELDS.name,
  SCOUT_CENSUS_FIELDS.affiliation,
  SCOUT_CENSUS_FIELDS.parentCompany,
  SCOUT_CENSUS_FIELDS.status,
  SCOUT_CENSUS_FIELDS.rooms,
  SCOUT_CENSUS_FIELDS.country,
  SCOUT_CENSUS_FIELDS.city,
  SCOUT_CENSUS_FIELDS.market,
  SCOUT_CENSUS_FIELDS.submarket,
  SCOUT_CENSUS_FIELDS.chainScale,
  SCOUT_CENSUS_FIELDS.location,
  SCOUT_CENSUS_FIELDS.operationType,
  SCOUT_CENSUS_FIELDS.projectPhase,
  SCOUT_CENSUS_FIELDS.latitude,
  SCOUT_CENSUS_FIELDS.longitude,
];

const SAMPLE_LIMIT = 12;
const WHITE_SPACE_LIMIT = 75;
const HIGH_INDEPENDENT_OPEN_HOTELS = 15;

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

function mapCensusRecord(rec) {
  const f = rec.fields || {};
  const affiliationStatus = exactMatchKey(f["Affiliation Status"]);
  let affiliation = exactMatchKey(f[SCOUT_CENSUS_FIELDS.affiliation]);
  let isIndependent = affiliation === CENSUS_INDEPENDENT_AFFILIATION;
  let isBranded = affiliation !== CENSUS_INDEPENDENT_AFFILIATION && affiliation !== "";
  let affiliationClass = isIndependent
    ? "INDEPENDENT_CONFIRMED"
    : isBranded
      ? "BRANDED_CONFIRMED"
      : "AFFILIATION_UNKNOWN";

  if (affiliationStatus) {
    if (affiliationStatus === "Independent") {
      affiliation = CENSUS_INDEPENDENT_AFFILIATION;
      isIndependent = true;
      isBranded = false;
      affiliationClass = "INDEPENDENT_CONFIRMED";
    } else if (affiliationStatus === "Brand-Unconfirmed") {
      isIndependent = false;
      isBranded = false;
      affiliationClass = "BRAND_UNCONFIRMED";
    } else if (affiliationStatus === "Unknown" || !affiliationStatus) {
      isIndependent = false;
      isBranded = false;
      affiliationClass = "AFFILIATION_UNKNOWN";
    } else if (!affiliation) {
      isIndependent = false;
      isBranded = false;
      affiliationClass = "AFFILIATION_UNKNOWN";
    } else {
      isIndependent = false;
      isBranded = true;
      affiliationClass = "BRANDED_CONFIRMED";
    }
  }

  const market = exactMatchKey(f[SCOUT_CENSUS_FIELDS.market]);
  const submarket = exactMatchKey(f[SCOUT_CENSUS_FIELDS.submarket]);
  const roomsRaw = f[SCOUT_CENSUS_FIELDS.rooms];
  const roomsKnown = roomsRaw != null && roomsRaw !== "" && Number(roomsRaw) > 0;
  const dhl = exactMatchKey(f["Dealality Hotel ID"]) || (String(rec.id || "").startsWith("dhl_") ? rec.id : null);

  return {
    id: dhl || rec.id,
    hpcRecordId: rec.hpcRecordId || null,
    dhl_: dhl,
    name: exactMatchKey(f[SCOUT_CENSUS_FIELDS.name]),
    affiliation,
    affiliationStatus: affiliationStatus || "",
    affiliationClass,
    parentCompany: exactMatchKey(f[SCOUT_CENSUS_FIELDS.parentCompany]),
    status: normalizeStatus(f[SCOUT_CENSUS_FIELDS.status]),
    rooms: parseRooms(roomsRaw),
    roomsKnown,
    country: exactMatchKey(f[SCOUT_CENSUS_FIELDS.country]),
    city: exactMatchKey(f[SCOUT_CENSUS_FIELDS.city]),
    market,
    submarket,
    region: exactMatchKey(f["State / Region"]),
    dealality_market: market || null,
    dealality_submarket: submarket || null,
    /** API alias — Dealality market when HPC; Legacy Market when Legacy. */
    strMarket: market,
    strSubmarket: submarket,
    chainScale: normalizeChainScale(f[SCOUT_CENSUS_FIELDS.chainScale]),
    locationType: exactMatchKey(f[SCOUT_CENSUS_FIELDS.location]) || "Unknown",
    operationType: exactMatchKey(f[SCOUT_CENSUS_FIELDS.operationType]),
    projectPhase: exactMatchKey(f[SCOUT_CENSUS_FIELDS.projectPhase]),
    latitude: f[SCOUT_CENSUS_FIELDS.latitude] ?? null,
    longitude: f[SCOUT_CENSUS_FIELDS.longitude] ?? null,
    isIndependent,
    isBranded,
  };
}

function parseQueryFilters(query = {}) {
  const includePipeline =
    query.includePipeline === "1" ||
    query.includePipeline === "true" ||
    query.includePipeline === true;

  const market = exactMatchKey(query.market || query.strMarket);
  const strMarket = exactMatchKey(query.strMarket || query.market);
  const submarket = exactMatchKey(query.submarket || query.strSubmarket);
  const strSubmarket = exactMatchKey(query.strSubmarket || query.submarket);

  return {
    country: exactMatchKey(query.country),
    city: exactMatchKey(query.city),
    market,
    strMarket,
    submarket,
    strSubmarket,
    parentCompany: exactMatchKey(query.parentCompany || query.parent_company),
    brand: exactMatchKey(query.brand),
    chainScale: exactMatchKey(query.chainScale || query.chain_scale),
    locationType: exactMatchKey(query.locationType || query.location_type),
    status: exactMatchKey(query.status),
    includePipeline,
  };
}

function rowPassesStatusScope(row, filters) {
  if (filters.status) {
    return textMatches(row.status, filters.status);
  }
  if (row.status === STATUS_OPEN) return true;
  if (row.status === STATUS_PIPELINE && filters.includePipeline) return true;
  return false;
}

function rowMatchesGeoAndAttributeFilters(row, filters, brandMatcherSet) {
  if (!textMatches(row.country, filters.country)) return false;
  if (!textMatches(row.city, filters.city)) return false;

  const marketFilter = filters.strMarket || filters.market;
  if (marketFilter) {
    const marketHit =
      textMatches(row.market, marketFilter) ||
      textMatches(row.region, marketFilter) ||
      textMatches(row.city, marketFilter);
    if (!marketHit) return false;
  }

  const submarketFilter = filters.strSubmarket || filters.submarket;
  if (submarketFilter && !textMatches(row.submarket, submarketFilter)) return false;

  if (!parentCompanyMatches(row.parentCompany, filters.parentCompany)) return false;
  if (brandMatcherSet && brandMatcherSet.size > 0 && !brandMatcherSet.has(row.affiliation)) {
    return false;
  }
  // Unknown chain scale: include when no filter; exclude only when a specific scale is requested.
  if (!textMatches(row.chainScale, filters.chainScale)) return false;
  if (!textMatches(row.locationType, filters.locationType)) return false;
  return true;
}

function filterRows(rows, filters, brandMatcherSet) {
  return rows.filter(
    (row) =>
      rowMatchesGeoAndAttributeFilters(row, filters, brandMatcherSet) &&
      rowPassesStatusScope(row, filters)
  );
}

function filterRowsForWhiteSpaceScope(rows, filters) {
  const scope = {
    ...filters,
    parentCompany: "",
    brand: "",
  };
  return rows.filter(
    (row) =>
      rowMatchesGeoAndAttributeFilters(row, scope, null) &&
      (row.status === STATUS_OPEN || (row.status === STATUS_PIPELINE && filters.includePipeline))
  );
}

function bumpBucket(map, key, row) {
  const k = key || "Unknown";
  if (!map[k]) {
    map[k] = { label: k, hotels: 0, rooms: 0, openHotels: 0, openRooms: 0, pipelineHotels: 0, pipelineRooms: 0 };
  }
  map[k].hotels += 1;
  map[k].rooms += row.rooms;
  if (row.status === STATUS_OPEN) {
    map[k].openHotels += 1;
    map[k].openRooms += row.rooms;
  } else if (row.status === STATUS_PIPELINE) {
    map[k].pipelineHotels += 1;
    map[k].pipelineRooms += row.rooms;
  }
}

function breakdownToArray(map) {
  return Object.values(map).sort((a, b) => b.openRooms - a.openRooms || b.openHotels - a.openHotels);
}

function computeMetrics(rows) {
  const openRows = rows.filter((r) => r.status === STATUS_OPEN);
  const pipelineRows = rows.filter((r) => r.status === STATUS_PIPELINE);
  const brandedOpen = openRows.filter((r) => r.isBranded);
  const independentOpen = openRows.filter((r) => r.isIndependent);

  const parentCompanies = new Set(openRows.map((r) => r.parentCompany).filter(Boolean));
  const brands = new Set(openRows.map((r) => r.affiliation).filter((a) => a && a !== CENSUS_INDEPENDENT_AFFILIATION));
  const countries = new Set(openRows.map((r) => r.country).filter(Boolean));
  const cities = new Set(openRows.map((r) => r.city).filter(Boolean));
  const strMarkets = new Set(openRows.map((r) => r.market).filter(Boolean));
  const strSubmarkets = new Set(openRows.map((r) => r.submarket).filter(Boolean));

  return {
    openHotels: openRows.length,
    openRooms: openRows.reduce((s, r) => s + r.rooms, 0),
    pipelineHotels: pipelineRows.length,
    pipelineRooms: pipelineRows.reduce((s, r) => s + r.rooms, 0),
    brandedHotels: brandedOpen.length,
    independentHotels: independentOpen.length,
    parentCompanyCount: parentCompanies.size,
    brandCount: brands.size,
    countryCount: countries.size,
    cityCount: cities.size,
    strMarketCount: strMarkets.size,
    strSubmarketCount: strSubmarkets.size,
  };
}

function computeBreakdowns(rows) {
  const maps = {
    byParentCompany: {},
    byBrand: {},
    byCountry: {},
    byCity: {},
    bySTRMarket: {},
    bySTRSubmarket: {},
    byChainScale: {},
    byLocationType: {},
    byStatus: {},
    byProjectPhase: {},
  };

  for (const row of rows) {
    bumpBucket(maps.byParentCompany, row.parentCompany || "Unknown", row);
    bumpBucket(maps.byBrand, row.affiliation || "Unknown", row);
    bumpBucket(maps.byCountry, row.country || "Unknown", row);
    bumpBucket(maps.byCity, row.city || "Unknown", row);
    bumpBucket(maps.bySTRMarket, row.market || "Unknown", row);
    bumpBucket(maps.bySTRSubmarket, row.submarket || "Unknown", row);
    bumpBucket(maps.byChainScale, row.chainScale, row);
    bumpBucket(maps.byLocationType, row.locationType, row);
    bumpBucket(maps.byStatus, row.status || "Unknown", row);
    if (row.status === STATUS_PIPELINE) {
      bumpBucket(maps.byProjectPhase, row.projectPhase || "Pipeline", row);
    }
  }

  return Object.fromEntries(
    Object.entries(maps).map(([key, map]) => [key, breakdownToArray(map)])
  );
}

function marketKey(row) {
  return row.market || row.city || "Unknown";
}

function submarketKey(row) {
  return row.submarket || "";
}

/** User-facing geography label — prefer Dealality submarket corridor over legacy Market field. */
function formatGeographyScopeLabel(market, submarket) {
  const sm = String(submarket || "").trim();
  const mk = String(market || "").trim();
  if (sm) return sm;
  if (mk) return mk;
  return "this market";
}

function aggregateByStrMarket(rows) {
  const byMarket = new Map();
  for (const row of rows) {
    const mk = marketKey(row);
    if (!byMarket.has(mk)) {
      byMarket.set(mk, {
        market: mk,
        submarkets: new Set(),
        openHotels: 0,
        openRooms: 0,
        pipelineHotels: 0,
        pipelineRooms: 0,
        brandedOpenHotels: 0,
        independentOpenHotels: 0,
        parentCompanies: new Set(),
        brands: new Set(),
        chainScales: new Set(),
      });
    }
    const bucket = byMarket.get(mk);
    const sm = submarketKey(row);
    if (sm) bucket.submarkets.add(sm);
    if (row.status === STATUS_OPEN) {
      bucket.openHotels += 1;
      bucket.openRooms += row.rooms;
      if (row.isBranded) bucket.brandedOpenHotels += 1;
      if (row.isIndependent) bucket.independentOpenHotels += 1;
      if (row.parentCompany) bucket.parentCompanies.add(normalizeParentCompanyKey(row.parentCompany));
      if (row.affiliation) bucket.brands.add(row.affiliation);
      if (row.chainScale) bucket.chainScales.add(row.chainScale);
    } else if (row.status === STATUS_PIPELINE) {
      bucket.pipelineHotels += 1;
      bucket.pipelineRooms += row.rooms;
    }
  }
  return byMarket;
}

function whiteSpaceEntry({ market, submarket, reason, opportunityType, confidence, supportingMetrics }) {
  return { market, submarket, reason, opportunityType, confidence, supportingMetrics };
}

function computeWhiteSpace(scopeRows, filters, brandResolution) {
  const opportunities = [];
  const openScope = scopeRows.filter((r) => r.status === STATUS_OPEN);
  const byMarket = aggregateByStrMarket(scopeRows);

  const parentNorm = normalizeParentCompanyKey(filters.parentCompany);
  const brandMatchers = new Set(brandResolution?.affiliationMatchers || []);

  const brandChainScales = new Set(
    openScope.filter((r) => brandMatchers.has(r.affiliation)).map((r) => r.chainScale).filter((c) => c && c !== "Unknown")
  );

  for (const [mk, bucket] of byMarket.entries()) {
    if (mk === "Unknown" || !bucket.brandedOpenHotels) continue;

    const submarket = bucket.submarkets.size === 1 ? [...bucket.submarkets][0] : "";
    const marketConfidence = mk !== "Unknown" ? "high" : "medium";

    if (parentNorm) {
      const parentOpen = openScope.filter(
        (r) =>
          marketKey(r) === mk &&
          normalizeParentCompanyKey(r.parentCompany) === parentNorm &&
          r.status === STATUS_OPEN
      ).length;

      if (parentOpen === 0 && bucket.brandedOpenHotels > 0) {
        opportunities.push(
          whiteSpaceEntry({
            market: mk,
            submarket,
            reason: `${filters.parentCompany} has no open hotels in ${formatGeographyScopeLabel(mk, submarket)} while ${bucket.brandedOpenHotels} other branded hotel(s) operate here.`,
            opportunityType: "parent_company_market_gap",
            confidence: marketConfidence,
            supportingMetrics: {
              otherBrandedOpenHotels: bucket.brandedOpenHotels,
              independentOpenHotels: bucket.independentOpenHotels,
              pipelineHotels: bucket.pipelineHotels,
              distinctParentCompanies: bucket.parentCompanies.size,
            },
          })
        );
      }
    }

    if (brandMatchers.size > 0) {
      const brandOpen = openScope.filter(
        (r) => marketKey(r) === mk && brandMatchers.has(r.affiliation)
      ).length;

      if (brandOpen === 0) {
        const parentPresent =
          parentNorm &&
          openScope.some(
            (r) =>
              marketKey(r) === mk &&
              normalizeParentCompanyKey(r.parentCompany) === parentNorm &&
              r.isBranded
          );

        const comparableChainScale = [...brandChainScales].some((scale) => bucket.chainScales.has(scale));

        if (parentPresent || comparableChainScale) {
          opportunities.push(
            whiteSpaceEntry({
              market: mk,
              submarket,
              reason: parentPresent
                ? `Brand has no open hotels here; parent company has branded presence in ${formatGeographyScopeLabel(mk, submarket)}.`
                : `Brand has no open hotels here; comparable chain scale (${[...brandChainScales].join(", ")}) has presence.`,
              opportunityType: "brand_market_gap",
              confidence: parentPresent ? "high" : "medium",
              supportingMetrics: {
                parentCompanyPresent: parentPresent,
                comparableChainScalePresent: comparableChainScale,
                otherBrandedOpenHotels: bucket.brandedOpenHotels,
                pipelineHotels: bucket.pipelineHotels,
              },
            })
          );
        }
      }
    }

    if (bucket.independentOpenHotels >= HIGH_INDEPENDENT_OPEN_HOTELS) {
      opportunities.push(
        whiteSpaceEntry({
          market: mk,
          submarket,
          reason: `High independent hotel supply (${bucket.independentOpenHotels} open independents).`,
          opportunityType: "independent_conversion_cluster",
          confidence: bucket.independentOpenHotels >= HIGH_INDEPENDENT_OPEN_HOTELS * 2 ? "high" : "medium",
          supportingMetrics: {
            independentOpenHotels: bucket.independentOpenHotels,
            brandedOpenHotels: bucket.brandedOpenHotels,
            openHotels: bucket.openHotels,
          },
        })
      );
    }

    if (bucket.pipelineHotels > 0) {
      opportunities.push(
        whiteSpaceEntry({
          market: mk,
          submarket,
          reason: `Pipeline activity detected (${bucket.pipelineHotels} pipeline hotel(s)).`,
          opportunityType: "pipeline_activity",
          confidence: bucket.pipelineHotels >= 3 ? "high" : "medium",
          supportingMetrics: {
            pipelineHotels: bucket.pipelineHotels,
            pipelineRooms: bucket.pipelineRooms,
            openHotels: bucket.openHotels,
          },
        })
      );
    }
  }

  const deduped = [];
  const seen = new Set();
  for (const opp of opportunities) {
    const key = `${opp.market}|${opp.submarket}|${opp.opportunityType}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(opp);
  }

  return deduped
    .sort((a, b) => {
      const rank = { high: 3, medium: 2, low: 1 };
      return (rank[b.confidence] || 0) - (rank[a.confidence] || 0);
    })
    .slice(0, WHITE_SPACE_LIMIT);
}

function buildRecordsSample(rows) {
  return rows.slice(0, SAMPLE_LIMIT).map((r) => ({
    id: r.id,
    dhl_: r.dhl_ || null,
    name: r.name,
    affiliation: r.affiliation,
    affiliationClass: r.affiliationClass || null,
    parentCompany: r.parentCompany,
    status: r.status,
    rooms: r.rooms,
    roomsKnown: r.roomsKnown !== false,
    country: r.country,
    city: r.city,
    market: r.market,
    submarket: r.submarket,
    region: r.region || null,
    strMarket: r.strMarket,
    strSubmarket: r.strSubmarket,
    chainScale: r.chainScale,
    locationType: r.locationType,
    operationType: r.operationType,
    projectPhase: r.projectPhase,
    latitude: r.latitude,
    longitude: r.longitude,
  }));
}

/**
 * Build market coverage intelligence report (read-only).
 * @param {Record<string, string|boolean>} [query]
 */
export async function buildMarketCoverageReport(query = {}) {
  const base = getPlatformBase();
  if (!base) {
    return { ok: false, error: "Platform base not configured" };
  }

  const filters = parseQueryFilters(query);
  const warnings = [];

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

  let records;
  let censusSourceMeta = null;
  // API sets `_scoutHpc` after shouldUseHpcScoutCensus. Scripts may pass useHpc=1.
  const hpcEnabled =
    query._scoutHpc === true || query.useHpc === true || query.useHpc === "1";

  try {
    const cached = await fetchScoutCensusRecords({
      country: filters.country,
      market: filters.market,
      city: filters.city,
      useHpc: hpcEnabled,
    });
    records = cached.records;
    censusSourceMeta = cached.source || null;
    if (cached.warnings?.length) warnings.push(...cached.warnings);
    if (hpcEnabled) {
      warnings.push("SCOUT_HPC: Chain Scale is UNKNOWN (SAFE_DEGRADATION for scale filters/gap insights).");
      warnings.push("SCOUT_HPC: Dealality Market/Submarket nullable — Legacy STR geography not used.");
    }
  } catch (err) {
    return { ok: false, error: err?.message || String(err) };
  }

  const allRows = records.map(mapCensusRecord);
  const filteredRows = filterRows(allRows, filters, brandMatcherSet);
  const scopeRows = filterRowsForWhiteSpaceScope(allRows, filters);

  if (!filters.parentCompany && !filters.brand) {
    warnings.push(
      "WHITE_SPACE_CONTEXT: pass parentCompany and/or brand for parent/brand gap white-space signals; independent and pipeline flags still apply."
    );
  }

  const metrics = computeMetrics(filteredRows);
  const breakdowns = computeBreakdowns(filteredRows);
  const whiteSpace = computeWhiteSpace(scopeRows, filters, brandResolution);

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
    metrics,
    breakdowns,
    whiteSpace,
    recordsSample: buildRecordsSample(filteredRows),
    warnings,
    hotelSource: hpcEnabled ? "Hotel Property Census" : "Hotel Census",
    source: {
      base: "AIRTABLE_BASE_ID_ALT",
      table: hpcEnabled ? "Hotel Property Census" : HOTEL_CENSUS_TABLE,
      hotelSource: hpcEnabled ? "Hotel Property Census" : "Hotel Census",
      censusSource: hpcEnabled ? "hpc" : "legacy",
      adapter: hpcEnabled ? "scout-hpc-v2" : null,
      readOnly: true,
      writes: false,
      fieldMapping: hpcEnabled
        ? {
            market: "Dealality Market (nullable)",
            submarket: "Dealality Submarket (nullable)",
            region: "State / Region",
            chainScale: "UNKNOWN",
          }
        : { ...SCOUT_STR_GEOGRAPHY_MAPPING },
      strGeographyNote: hpcEnabled
        ? "HPC path: Dealality geo shim only — no Legacy STR Market/Submarket."
        : "Excel STR Market → Hotel Census Market; Excel STR Submarket → Hotel Census Submarket. No separate STR Market/STR Submarket Airtable fields.",
      fieldsLoaded: SELECT_FIELDS,
      censusRecordsLoaded: records.length,
      censusRecordsMatched: filteredRows.length,
      aggregatedAt: new Date().toISOString(),
      ...(censusSourceMeta || {}),
      noLegacyReads: hpcEnabled,
      noStrJoins: hpcEnabled,
    },
  };
}
