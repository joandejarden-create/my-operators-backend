/**
 * Scout Phase 4 — Map-ready aggregates (read-only).
 * Hotel Census + generated signals + saved Scout Opportunity Signals.
 */

import {
  CENSUS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "../hotel-census/fields.js";
import { exactMatchKey, resolveBrandAffiliationMatchers } from "../hotel-census/brand-alias-resolve.js";
import { HOTEL_CENSUS_TABLE, getPlatformBase } from "../hotel-census/platform-base.js";
import { fetchScoutCensusRecords } from "./census-read-cache.js";
import { buildOpportunitySignalsReport } from "./opportunity-signals.js";
import { annotateGeneratedSignalsWithSavedStatus, listSavedSignals } from "./scout-signal-watchlist.js";
import { buildDemandOverlaysReport } from "./demand-overlays.js";
import { buildMarketInsightsReport } from "./market-insights.js";

const DEFAULT_LIMIT = 500;
const CENTROID_MIN_HOTELS = 3;

const CENSUS_SELECT = [
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.affiliation,
  CENSUS_FIELDS.parentCompany,
  CENSUS_FIELDS.status,
  CENSUS_FIELDS.rooms,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.market,
  CENSUS_FIELDS.submarket,
  CENSUS_FIELDS.chainScale,
  CENSUS_FIELDS.location,
  CENSUS_FIELDS.operationType,
  CENSUS_FIELDS.managementCompany,
  "Latitude",
  "Longitude",
];

function parseRooms(value) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : 0;
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

export function isValidCoordinate(lat, lng) {
  const la = parseFloat(lat);
  const lo = parseFloat(lng);
  if (!Number.isFinite(la) || !Number.isFinite(lo)) return false;
  if (la === 0 && lo === 0) return false;
  if (la < -90 || la > 90 || lo < -180 || lo > 180) return false;
  return true;
}

function mapCensusRow(rec) {
  const f = rec.fields || {};
  const affiliation = exactMatchKey(f[CENSUS_FIELDS.affiliation]);
  const status = normalizeStatus(f[CENSUS_FIELDS.status]);
  return {
    id: rec.id,
    name: exactMatchKey(f[CENSUS_FIELDS.name]),
    affiliation,
    parentCompany: exactMatchKey(f[CENSUS_FIELDS.parentCompany]),
    status,
    rooms: parseRooms(f[CENSUS_FIELDS.rooms]),
    country: exactMatchKey(f[CENSUS_FIELDS.country]),
    city: exactMatchKey(f[CENSUS_FIELDS.city]),
    market: exactMatchKey(f[CENSUS_FIELDS.market]),
    submarket: exactMatchKey(f[CENSUS_FIELDS.submarket]),
    chainScale: exactMatchKey(f[CENSUS_FIELDS.chainScale]) || "Unknown",
    locationType: exactMatchKey(f[CENSUS_FIELDS.location]) || "Unknown",
    operationType: exactMatchKey(f[CENSUS_FIELDS.operationType]),
    managementCompany: exactMatchKey(f[CENSUS_FIELDS.managementCompany]),
    latitude: f.Latitude ?? null,
    longitude: f.Longitude ?? null,
    isIndependent: affiliation === CENSUS_INDEPENDENT_AFFILIATION,
    isBranded: affiliation !== CENSUS_INDEPENDENT_AFFILIATION && affiliation !== "",
    isPipeline: status === STATUS_PIPELINE,
    isOpen: status === STATUS_OPEN,
  };
}

export function parseMarketMapFilters(query = {}) {
  const includePipeline =
    query.includePipeline === "1" ||
    query.includePipeline === "true" ||
    query.includePipeline === true;
  const includeSignals =
    query.includeSignals !== "0" && query.includeSignals !== "false";
  const includeSavedSignals =
    query.includeSavedSignals !== "0" && query.includeSavedSignals !== "false";
  const includeDemandOverlays =
    query.includeDemandOverlays === "1" ||
    query.includeDemandOverlays === "true" ||
    query.includeDemandOverlays === true;
  const includeInsights =
    query.includeInsights === "1" ||
    query.includeInsights === "true" ||
    query.includeInsights === true;

  const limitRaw = parseInt(query.limit, 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 2000) : DEFAULT_LIMIT;

  const minPriorityRaw = parseInt(query.minPriorityScore, 10);

  return {
    country: exactMatchKey(query.country),
    city: exactMatchKey(query.city),
    market: exactMatchKey(query.market),
    submarket: exactMatchKey(query.submarket),
    parentCompany: exactMatchKey(query.parentCompany || query.parent_company),
    brand: exactMatchKey(query.brand),
    chainScale: exactMatchKey(query.chainScale || query.chain_scale),
    locationType: exactMatchKey(query.locationType || query.location_type),
    status: exactMatchKey(query.status),
    signalType: exactMatchKey(query.signalType),
    reviewStatus: exactMatchKey(query.reviewStatus),
    minPriorityScore: Number.isFinite(minPriorityRaw) ? minPriorityRaw : null,
    includePipeline,
    includeSignals,
    includeSavedSignals,
    includeDemandOverlays,
    includeInsights,
    limit,
  };
}

function rowInScope(row, filters) {
  if (!textMatches(row.country, filters.country)) return false;
  if (!textMatches(row.city, filters.city)) return false;
  if (!textMatches(row.market, filters.market)) return false;
  if (!textMatches(row.submarket, filters.submarket)) return false;
  if (!textMatches(row.chainScale, filters.chainScale)) return false;
  if (!textMatches(row.locationType, filters.locationType)) return false;
  if (filters.status && !textMatches(row.status, filters.status)) return false;
  if (filters.parentCompany && !textMatches(row.parentCompany, filters.parentCompany)) return false;
  return true;
}

function scopeStatus(row, filters) {
  if (filters.status) return textMatches(row.status, filters.status);
  if (row.status === STATUS_OPEN) return true;
  return row.status === STATUS_PIPELINE && filters.includePipeline;
}

function buildHotelMarker(row) {
  if (!isValidCoordinate(row.latitude, row.longitude)) return null;
  const popupMetrics = {
    rooms: row.rooms,
    status: row.status,
    chainScale: row.chainScale,
    locationType: row.locationType,
  };
  return {
    markerId: `hotel-${row.id}`,
    markerType: "hotel",
    airtableRecordId: row.id,
    hotelName: row.name,
    affiliation: row.affiliation,
    parentCompany: row.parentCompany,
    status: row.status,
    rooms: row.rooms,
    country: row.country,
    city: row.city,
    market: row.market,
    submarket: row.submarket,
    chainScale: row.chainScale,
    locationType: row.locationType,
    operationType: row.operationType,
    managementCompany: row.managementCompany,
    latitude: parseFloat(row.latitude),
    longitude: parseFloat(row.longitude),
    isIndependent: row.isIndependent,
    isBranded: row.isBranded,
    isPipeline: row.isPipeline,
    popupTitle: row.name,
    popupSubtitle: [row.affiliation, row.market].filter(Boolean).join(" · "),
    popupMetrics,
    source: "Hotel Census",
  };
}

function clusterKey(country, market, submarket) {
  return [country || "", market || "", submarket || ""].join("::");
}

function ensureCluster(map, country, market, submarket) {
  const key = clusterKey(country, market, submarket);
  if (!map.has(key)) {
    map.set(key, {
      clusterId: `cluster-${key.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}`,
      country,
      market,
      submarket,
      hotelCount: 0,
      openHotels: 0,
      pipelineHotels: 0,
      brandedHotels: 0,
      independentHotels: 0,
      signalCount: 0,
      savedSignalCount: 0,
      signalTypes: new Map(),
      priorityScores: [],
      latSum: 0,
      lngSum: 0,
      coordCount: 0,
    });
  }
  return map.get(key);
}

function finalizeCluster(cluster) {
  const topSignalTypes = [...cluster.signalTypes.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([type, count]) => ({ type, count }));

  const averagePriorityScore =
    cluster.priorityScores.length > 0
      ? Math.round(
          (cluster.priorityScores.reduce((s, n) => s + n, 0) / cluster.priorityScores.length) * 10
        ) / 10
      : 0;

  let representativeLatitude = null;
  let representativeLongitude = null;
  let coordinateSource = "missing_coordinates";
  if (cluster.coordCount >= CENTROID_MIN_HOTELS) {
    representativeLatitude =
      Math.round((cluster.latSum / cluster.coordCount) * 1_000_000) / 1_000_000;
    representativeLongitude =
      Math.round((cluster.lngSum / cluster.coordCount) * 1_000_000) / 1_000_000;
    coordinateSource = "centroid_from_hotels";
  }

  return {
    clusterId: cluster.clusterId,
    country: cluster.country,
    market: cluster.market,
    submarket: cluster.submarket,
    hotelCount: cluster.hotelCount,
    openHotels: cluster.openHotels,
    pipelineHotels: cluster.pipelineHotels,
    brandedHotels: cluster.brandedHotels,
    independentHotels: cluster.independentHotels,
    signalCount: cluster.signalCount,
    savedSignalCount: cluster.savedSignalCount,
    topSignalTypes,
    averagePriorityScore,
    representativeLatitude,
    representativeLongitude,
    coordinateSource,
  };
}

function buildSignalMarker(signal, coords) {
  if (!coords || !isValidCoordinate(coords.latitude, coords.longitude)) return null;
  return {
    markerId: `gen-signal-${signal.signalId}`,
    markerType: "generated_signal",
    signalId: signal.signalId,
    signalType: signal.signalType,
    title: signal.title,
    country: signal.country,
    city: signal.city,
    market: signal.market,
    submarket: signal.submarket,
    priorityScore: signal.priorityScore,
    confidence: signal.confidence,
    actionability: signal.actionability,
    reason: signal.reason,
    recommendedAction: signal.recommendedAction,
    saved: signal.saved === true,
    savedRecordId: signal.savedRecordId || null,
    savedReviewStatus: signal.savedReviewStatus || null,
    latitude: coords.latitude,
    longitude: coords.longitude,
  };
}

function buildSavedSignalMarker(saved, coords) {
  const marker = {
    markerId: `saved-signal-${saved.signalId}`,
    markerType: "saved_signal",
    signalId: saved.signalId,
    signalType: saved.signalType,
    signalTitle: saved.signalTitle,
    country: saved.country,
    city: saved.city,
    market: saved.market,
    submarket: saved.submarket,
    hotelName: saved.hotelName,
    parentCompany: saved.parentCompany,
    brand: saved.brand,
    priorityScore: saved.priorityScore,
    confidence: saved.confidence,
    actionability: saved.actionability,
    reviewStatus: saved.reviewStatus,
    assignedTo: saved.assignedTo,
    internalNotes: saved.internalNotes,
    latitude: null,
    longitude: null,
    source: "Scout Opportunity Signals",
    savedRecordId: saved.recordId,
  };
  if (coords && isValidCoordinate(coords.latitude, coords.longitude)) {
    marker.latitude = coords.latitude;
    marker.longitude = coords.longitude;
  }
  return marker;
}

function bumpClusterFromHotel(cluster, row) {
  cluster.hotelCount += 1;
  if (row.isOpen) {
    cluster.openHotels += 1;
    if (row.isBranded) cluster.brandedHotels += 1;
    if (row.isIndependent) cluster.independentHotels += 1;
  }
  if (row.isPipeline) cluster.pipelineHotels += 1;
  if (isValidCoordinate(row.latitude, row.longitude)) {
    cluster.latSum += parseFloat(row.latitude);
    cluster.lngSum += parseFloat(row.longitude);
    cluster.coordCount += 1;
  }
}

function bumpClusterFromSignal(cluster, signalType, priorityScore) {
  cluster.signalCount += 1;
  cluster.signalTypes.set(signalType, (cluster.signalTypes.get(signalType) || 0) + 1);
  if (Number.isFinite(priorityScore)) cluster.priorityScores.push(priorityScore);
}

/**
 * @param {Record<string, string|boolean>} [query]
 */
export async function buildMarketMapReport(query = {}) {
  const base = getPlatformBase();
  if (!base) {
    return { ok: false, error: "Platform base not configured" };
  }

  const filters = parseMarketMapFilters(query);
  const warnings = [];

  let records;
  try {
    const cached = await fetchScoutCensusRecords({
      country: filters.country,
      market: filters.market,
    });
    records = cached.records;
    if (cached.warnings?.length) warnings.push(...cached.warnings);
  } catch (err) {
    return { ok: false, error: err?.message || String(err) };
  }

  const allRows = records.map(mapCensusRow);
  const scopedRows = allRows.filter((row) => rowInScope(row, filters) && scopeStatus(row, filters));

  const hotelById = new Map(scopedRows.map((r) => [r.id, r]));
  const clusterMap = new Map();

  for (const row of scopedRows) {
    const cluster = ensureCluster(clusterMap, row.country, row.market, row.submarket);
    bumpClusterFromHotel(cluster, row);
  }

  let hotelMarkers = scopedRows
    .map(buildHotelMarker)
    .filter(Boolean)
    .slice(0, filters.limit);

  const hotelMarkerIds = new Set(hotelMarkers.map((m) => m.airtableRecordId));

  if (hotelMarkers.length < scopedRows.filter((r) => isValidCoordinate(r.latitude, r.longitude)).length) {
    warnings.push("HOTEL_MARKER_LIMIT: some geocoded hotels omitted due to limit param.");
  }

  const signalQuery = filters.includeSignals
    ? {
        country: filters.country,
        city: filters.city,
        market: filters.market,
        submarket: filters.submarket,
        parentCompany: filters.parentCompany,
        brand: filters.brand,
        chainScale: filters.chainScale,
        locationType: filters.locationType,
        status: filters.status,
        signalType: filters.signalType,
        includePipeline: filters.includePipeline ? "1" : "0",
        limit: String(filters.limit),
        ...(filters.minPriorityScore != null
          ? { minPriorityScore: String(filters.minPriorityScore) }
          : {}),
      }
    : null;

  const savedQuery = filters.includeSavedSignals
    ? {
        country: filters.country,
        market: filters.market,
        submarket: filters.submarket,
        parentCompany: filters.parentCompany,
        brand: filters.brand,
        signalType: filters.signalType,
        reviewStatus: filters.reviewStatus,
        minPriorityScore: filters.minPriorityScore ?? undefined,
        limit: filters.limit,
      }
    : null;

  const overlayQuery = filters.includeDemandOverlays
    ? {
        country: filters.country,
        city: filters.city,
        market: filters.market,
        submarket: filters.submarket,
        limit: String(filters.limit),
      }
    : null;

  const [signalReport, savedList, overlayReport] = await Promise.all([
    signalQuery ? buildOpportunitySignalsReport(signalQuery) : Promise.resolve(null),
    savedQuery ? listSavedSignals(savedQuery) : Promise.resolve(null),
    overlayQuery ? buildDemandOverlaysReport(overlayQuery) : Promise.resolve(null),
  ]);

  let signalMarkers = [];
  let generatedSignals = [];

  if (signalQuery) {
    if (!signalReport?.ok) {
      warnings.push(`SIGNAL_GENERATION: ${signalReport?.error || "failed"}`);
    } else {
      generatedSignals = await annotateGeneratedSignalsWithSavedStatus(signalReport.signals);
      if (filters.minPriorityScore != null) {
        generatedSignals = generatedSignals.filter(
          (s) => (s.priorityScore || 0) >= filters.minPriorityScore
        );
      }

      for (const signal of generatedSignals) {
        const linkedId = exactMatchKey(signal.linkedHotelRecordId);
        const coords = linkedId && hotelById.get(linkedId);
        const marker = buildSignalMarker(
          signal,
          coords
            ? { latitude: parseFloat(coords.latitude), longitude: parseFloat(coords.longitude) }
            : null
        );
        if (marker) {
          signalMarkers.push(marker);
        } else {
          const cluster = ensureCluster(clusterMap, signal.country, signal.market, signal.submarket);
          bumpClusterFromSignal(cluster, signal.signalType, signal.priorityScore);
        }
      }
    }
  }

  let savedSignalMarkers = [];
  if (savedQuery) {
    if (!savedList?.ok) {
      warnings.push(`SAVED_SIGNALS: ${savedList?.error || "failed"}`);
    } else {
      for (const saved of savedList.signals) {
        const linkedId = exactMatchKey(saved.linkedHotelCensusRecordId);
        const coords = linkedId && hotelById.get(linkedId);
        const marker = buildSavedSignalMarker(
          saved,
          coords
            ? { latitude: parseFloat(coords.latitude), longitude: parseFloat(coords.longitude) }
            : null
        );

        const cluster = ensureCluster(clusterMap, saved.country, saved.market, saved.submarket);
        cluster.savedSignalCount += 1;
        bumpClusterFromSignal(cluster, saved.signalType, saved.priorityScore);

        if (marker.latitude != null && marker.longitude != null) {
          savedSignalMarkers.push(marker);
        }
      }
    }
  }

  const marketClusters = [...clusterMap.values()]
    .filter((c) => c.signalCount > 0 || c.savedSignalCount > 0 || c.hotelCount >= CENTROID_MIN_HOTELS)
    .map(finalizeCluster)
    .sort((a, b) => b.signalCount + b.savedSignalCount - (a.signalCount + a.savedSignalCount));

  const openHotels = scopedRows.filter((r) => r.isOpen).length;
  const pipelineHotels = scopedRows.filter((r) => r.isPipeline).length;
  const brandedHotels = scopedRows.filter((r) => r.isOpen && r.isBranded).length;
  const independentHotels = scopedRows.filter((r) => r.isOpen && r.isIndependent).length;
  const markets = new Set(scopedRows.map((r) => r.market).filter(Boolean));
  const submarkets = new Set(scopedRows.map((r) => r.submarket).filter(Boolean));

  let demandOverlayMarkers = [];
  let demandOverlayMarkersWithoutCoordinates = [];
  let demandOverlaySummary = null;

  if (overlayQuery) {
    if (!overlayReport?.ok) {
      warnings.push(`DEMAND_OVERLAYS: ${overlayReport?.error || "load failed"}`);
    } else {
      demandOverlayMarkers = overlayReport.overlayMarkers || [];
      demandOverlayMarkersWithoutCoordinates =
        overlayReport.overlayMarkersWithoutCoordinates || [];
      demandOverlaySummary = overlayReport.summary;
      warnings.push(...(overlayReport.warnings || []));
    }
  }

  let insightSummary = null;
  let insights = [];
  let rankedOpportunities = [];

  if (filters.includeInsights) {
    const insightReport = await buildMarketInsightsReport({
      country: filters.country,
      city: filters.city,
      market: filters.market,
      submarket: filters.submarket,
      parentCompany: filters.parentCompany,
      brand: filters.brand,
      chainScale: filters.chainScale,
      locationType: filters.locationType,
      includeDemandOverlays: filters.includeDemandOverlays ? "1" : "0",
      includeSavedSignals: filters.includeSavedSignals ? "1" : "0",
      limit: "100",
    });
    if (!insightReport.ok) {
      warnings.push(`INSIGHTS: ${insightReport.error}`);
    } else {
      insightSummary = insightReport.summary;
      insights = insightReport.insights || [];
      rankedOpportunities = insightReport.rankedOpportunities || [];
      warnings.push(...(insightReport.warnings || []));
    }
  }

  return {
    ok: true,
    filters,
    summary: {
      hotelMarkers: hotelMarkers.length,
      signalMarkers: signalMarkers.length,
      savedSignalMarkers: savedSignalMarkers.length,
      openHotels,
      pipelineHotels,
      brandedHotels,
      independentHotels,
      markets: markets.size,
      submarkets: submarkets.size,
      marketClusters: marketClusters.length,
      censusRecordsInScope: scopedRows.length,
      geocodedHotelsInScope: scopedRows.filter((r) =>
        isValidCoordinate(r.latitude, r.longitude)
      ).length,
    },
    hotelMarkers,
    signalMarkers,
    savedSignalMarkers,
    marketClusters,
    generatedSignals,
    demandOverlayMarkers,
    demandOverlayMarkersWithoutCoordinates,
    demandOverlaySummary,
    insightSummary,
    insights,
    rankedOpportunities,
    warnings,
    source: {
      hotelSource: "Hotel Census",
      signalSource: "Scout Opportunity Signals",
      readOnly: true,
      writes: false,
      marketField: CENSUS_FIELDS.market,
      submarketField: CENSUS_FIELDS.submarket,
      aggregatedAt: new Date().toISOString(),
    },
  };
}
