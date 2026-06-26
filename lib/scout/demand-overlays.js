/**
 * Scout Phase 5A — Demand overlay loader (read-only).
 * Travel Infrastructure + Demand Anchors from Deal Capture Platform (AIRTABLE_BASE_ID_ALT).
 */

import { exactMatchKey } from "../hotel-census/brand-alias-resolve.js";
import { fetchTravelInfrastructureRecords } from "../travel-infrastructure/airtable-travel-infrastructure-io.js";
import { fetchDemandAnchorRecords } from "../demand-anchors/airtable-demand-anchors-io.js";
import { getTravelInfrastructureBaseId } from "../travel-infrastructure/travel-infrastructure-base.js";
import { getDemandAnchorsBaseId } from "../demand-anchors/demand-anchors-base.js";
import { isValidCoordinate } from "./market-map.js";
import {
  fetchBaseTablesMeta,
  inventoryOverlaySources,
} from "./demand-overlay-inventory.js";

const DEFAULT_LIMIT = 500;

function strVal(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  return String(v).trim();
}

function sourceLabelFromPoint(point) {
  const src = point.source;
  if (Array.isArray(src) && src.length) return src.join(", ");
  return strVal(src) || point.sourceReference || "";
}

function marketFromPoint(point) {
  return strVal(point.market) || strVal(point.region) || "";
}

/**
 * @param {object} point — normalized radar point
 * @param {"travel_infrastructure"|"demand_anchor"} overlayType
 * @param {string} tableLabel
 */
export function normalizeRadarPointToOverlayMarker(point, overlayType, tableLabel) {
  const lat = point.latitude;
  const lng = point.longitude;
  const hasCoords = isValidCoordinate(lat, lng);
  const category = strVal(point.pointType) || strVal(point.type) || "Unknown";
  const name = strVal(point.name) || "Unnamed";
  const country = strVal(point.country);
  const city = strVal(point.city);
  const market = marketFromPoint(point);
  const submarket = strVal(point.submarket);
  const confidence = strVal(point.dataConfidence);
  const sourceLabel = sourceLabelFromPoint(point);
  const notes = strVal(point.notes);
  const status = strVal(point.visibility) || (point.includeOnRadarMap === false ? "Hidden" : "Active");

  const geoSubtitle = [country, market, submarket].filter(Boolean).join(" · ");

  return {
    overlayId: `${overlayType}-${point.id}`,
    overlayType,
    category,
    name,
    country,
    city,
    market,
    submarket,
    latitude: hasCoords ? parseFloat(lat) : null,
    longitude: hasCoords ? parseFloat(lng) : null,
    status,
    confidence,
    sourceLabel,
    notes,
    popupTitle: name,
    popupSubtitle: [category, geoSubtitle].filter(Boolean).join(" · "),
    source: {
      table: tableLabel,
      recordId: point.id,
      readOnly: true,
    },
  };
}

export function parseDemandOverlayFilters(query = {}) {
  const limitRaw = parseInt(query.limit, 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 2000) : DEFAULT_LIMIT;

  return {
    country: exactMatchKey(query.country),
    city: exactMatchKey(query.city),
    market: exactMatchKey(query.market),
    submarket: exactMatchKey(query.submarket),
    overlayType: exactMatchKey(query.overlayType),
    category: exactMatchKey(query.category),
    confidence: exactMatchKey(query.confidence),
    status: exactMatchKey(query.status),
    limit,
  };
}

function textMatches(value, filter) {
  if (!filter) return true;
  return exactMatchKey(value).toLowerCase() === exactMatchKey(filter).toLowerCase();
}

function overlayInScope(marker, filters) {
  if (!textMatches(marker.country, filters.country)) return false;
  if (!textMatches(marker.city, filters.city)) return false;
  if (!textMatches(marker.market, filters.market)) return false;
  if (!textMatches(marker.submarket, filters.submarket)) return false;
  if (filters.overlayType && marker.overlayType !== filters.overlayType) return false;
  if (filters.category && !textMatches(marker.category, filters.category)) return false;
  if (filters.confidence && !textMatches(marker.confidence, filters.confidence)) return false;
  if (filters.status && !textMatches(marker.status, filters.status)) return false;
  return true;
}

function countByKey(markers, keyFn) {
  const map = new Map();
  for (const m of markers) {
    const k = keyFn(m) || "(blank)";
    map.set(k, (map.get(k) || 0) + 1);
  }
  return [...map.entries()]
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

function buildSummary(markers) {
  return {
    overlayMarkers: markers.length,
    withCoordinates: markers.filter((m) => m.latitude != null).length,
    withoutCoordinates: markers.filter((m) => m.latitude == null).length,
    byOverlayType: countByKey(markers, (m) => m.overlayType),
    byCategory: countByKey(markers, (m) => m.category),
    byCountry: countByKey(markers, (m) => m.country),
    byMarket: countByKey(markers, (m) => m.market),
  };
}

/**
 * Inspect overlay source tables (metadata only).
 * @param {{ baseId?: string, apiKey?: string }} [opts]
 */
export async function inspectOverlaySourceTables(opts = {}) {
  const apiKey = opts.apiKey || process.env.AIRTABLE_API_KEY;
  const baseId =
    opts.baseId ||
    process.env.AIRTABLE_BASE_ID_ALT ||
    getTravelInfrastructureBaseId() ||
    getDemandAnchorsBaseId();

  if (!apiKey || !baseId) {
    return { ok: false, error: "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT" };
  }

  const tables = await fetchBaseTablesMeta(baseId, apiKey);
  const inventory = inventoryOverlaySources(tables);
  return { ok: true, baseId, ...inventory };
}

/**
 * @param {Record<string, string>} [query]
 */
export async function buildDemandOverlaysReport(query = {}) {
  const filters = parseDemandOverlayFilters(query);
  const warnings = [];
  const markers = [];

  const wantTravel =
    !filters.overlayType || filters.overlayType === "travel_infrastructure";
  const wantDemand =
    !filters.overlayType || filters.overlayType === "demand_anchor";

  if (wantTravel) {
    const tiQuery = {
      country: filters.country || undefined,
      region: filters.market || undefined,
      includeHidden: true,
    };
    const tiResult = await fetchTravelInfrastructureRecords(tiQuery);
    if (tiResult.error === "airtable_config_missing") {
      warnings.push("TRAVEL_INFRASTRUCTURE: Airtable not configured");
    } else if (tiResult.error === "travel_infrastructure_table_missing") {
      warnings.push("TRAVEL_INFRASTRUCTURE: table not found in platform base");
    } else if (tiResult.error) {
      warnings.push(`TRAVEL_INFRASTRUCTURE: ${tiResult.error}`);
    } else {
      for (const point of tiResult.allPoints || tiResult.points || []) {
        if (point.includeOnRadarMap === false) continue;
        const marker = normalizeRadarPointToOverlayMarker(
          point,
          "travel_infrastructure",
          tiResult.tableName || "Travel Infrastructure Data"
        );
        if (overlayInScope(marker, filters)) markers.push(marker);
      }
    }
  }

  if (wantDemand) {
    const daQuery = {
      country: filters.country || undefined,
      region: filters.market || undefined,
    };
    const daResult = await fetchDemandAnchorRecords(daQuery);
    if (daResult.error === "airtable_config_missing") {
      warnings.push("DEMAND_ANCHORS: Airtable not configured");
    } else if (daResult.error === "demand_anchors_table_missing") {
      warnings.push("DEMAND_ANCHORS: table not found — optional source skipped");
    } else if (daResult.error) {
      warnings.push(`DEMAND_ANCHORS: ${daResult.error}`);
    } else {
      for (const point of daResult.allPoints || daResult.points || []) {
        if (point.includeOnRadarMap === false) continue;
        const marker = normalizeRadarPointToOverlayMarker(
          point,
          "demand_anchor",
          daResult.tableName || "Demand Anchors"
        );
        if (overlayInScope(marker, filters)) markers.push(marker);
      }
    }
  }

  const limited = markers.slice(0, filters.limit);
  if (limited.length < markers.length) {
    warnings.push("OVERLAY_LIMIT: some overlay markers omitted due to limit param");
  }

  const withCoords = limited.filter((m) => m.latitude != null);
  const withoutCoords = limited.filter((m) => m.latitude == null);

  return {
    ok: true,
    filters,
    summary: buildSummary(limited),
    overlayMarkers: withCoords,
    overlayMarkersWithoutCoordinates: withoutCoords,
    warnings,
    source: {
      base: "Deal Capture Platform",
      readOnly: true,
      writes: false,
      aggregatedAt: new Date().toISOString(),
    },
  };
}
