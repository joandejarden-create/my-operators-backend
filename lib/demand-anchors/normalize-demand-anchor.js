/**
 * Normalize Demand Anchors records into radar map point objects.
 */

import {
  DEMAND_ANCHORS_FIELDS as F,
  DEMAND_ANCHORS_SOURCE_TABLE,
  RADAR_CATEGORY_DEMAND_ANCHORS,
  POINT_TYPE_TO_MAP_ICON,
} from "./airtable-demand-anchors-fields.js";
import { getPointTypeDefaults, inferPointSubtype } from "./point-type-defaults.js";
import { isDemandAnchorMapDisplayReady } from "./coordinate-verification.js";

function strVal(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  return String(v).trim();
}

function numVal(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function boolVal(v, defaultValue = true) {
  if (v == null || v === "") return defaultValue;
  if (typeof v === "boolean") return v;
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (s === "true" || s === "yes" || s === "1") return true;
    if (s === "false" || s === "no" || s === "0") return false;
  }
  return defaultValue;
}

function multiSelect(v) {
  if (!Array.isArray(v)) {
    const s = strVal(v);
    return s ? [s] : [];
  }
  return v.map((x) => strVal(x)).filter(Boolean);
}

function linkedIds(v) {
  if (!Array.isArray(v)) return [];
  return v.filter((id) => typeof id === "string" && id.startsWith("rec"));
}

function extractDealIds(fields) {
  const ids = [];
  const text = fields[F.dealRecordId];
  if (typeof text === "string" && text.startsWith("rec")) ids.push(text.trim());
  const linked = linkedIds(fields[F.linkedDeals]);
  for (const id of linked) {
    if (!ids.includes(id)) ids.push(id);
  }
  return ids;
}

/**
 * @param {Record<string, unknown>} fields
 */
export function resolvePointType(fields) {
  return strVal(fields?.[F.pointType]);
}

/**
 * @param {Record<string, unknown>} fields
 */
export function resolvePointSubtype(fields) {
  const f = fields || {};
  const explicit = strVal(f[F.pointSubtype]);
  if (explicit) return explicit;
  return inferPointSubtype(resolvePointType(f), "");
}

/**
 * @param {Record<string, unknown>} fields
 */
export function resolveMapIconType(fields) {
  const f = fields || {};
  const explicit = strVal(f[F.mapIconType]);
  if (explicit) return explicit;
  const pointType = resolvePointType(f);
  return POINT_TYPE_TO_MAP_ICON[pointType] || "Attraction";
}

/**
 * @typedef {object} DemandAnchorPoint
 * @property {string} id
 * @property {string} sourceTable
 * @property {string} name
 * @property {string} radarCategory
 * @property {string} pointType
 * @property {string} pointSubtype
 * @property {string[]} linkedMarketIds
 * @property {string[]} linkedDealIds
 * @property {string} dealRecordId
 * @property {string} city
 * @property {string} country
 * @property {string} region
 * @property {string} address
 * @property {number|null} latitude
 * @property {number|null} longitude
 * @property {number|null} distanceFromDeal
 * @property {number|null} estimatedDriveTime
 * @property {string} demandRelevance
 * @property {string} demandSegment
 * @property {string[]} demandPattern
 * @property {string[]} relevantHotelTypes
 * @property {string} hotelDemandRationale
 * @property {string[]} source
 * @property {string} sourceReference
 * @property {string} dataConfidence
 * @property {boolean} includeOnRadarMap
 * @property {string} mapLayer
 * @property {string} mapIconType
 * @property {string} visibility
 * @property {string} notes
 * @property {string} lastVerified
 * @property {string} type
 */

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 * @param {{ applyDefaults?: boolean }} [opts]
 * @returns {DemandAnchorPoint}
 */
export function normalizeDemandAnchorToRadarPoint(record, opts = {}) {
  const f = record?.fields || {};
  const pointType = resolvePointType(f);
  const defaults = opts.applyDefaults !== false ? getPointTypeDefaults(pointType) : {};

  const pointSubtype = resolvePointSubtype(f) || defaults.pointSubtype || "Unknown";
  const mapIconType = resolveMapIconType(f) || defaults.mapIconType || "";

  const sourceRef = strVal(f[F.sourceReference]);
  const sourceVals = multiSelect(f[F.source]);
  if (!sourceVals.length && sourceRef) sourceVals.push("Public Source");

  const linkedDealIds = extractDealIds(f);

  const hasIncludeField = Object.prototype.hasOwnProperty.call(f, F.includeOnRadarMap);
  const point = {
    id: record?.id || "",
    sourceTable: DEMAND_ANCHORS_SOURCE_TABLE,
    name: strVal(f[F.name]),
    radarCategory:
      strVal(f[F.radarCategory]) || defaults.radarCategory || RADAR_CATEGORY_DEMAND_ANCHORS,
    pointType,
    pointSubtype,
    linkedMarketIds: linkedIds(f[F.linkedMarket]),
    linkedDealIds,
    dealRecordId: strVal(f[F.dealRecordId]) || linkedDealIds[0] || "",
    city: strVal(f[F.city]),
    country: strVal(f[F.country]),
    region: strVal(f[F.region]),
    submarket: strVal(f[F.submarket]),
    address: strVal(f[F.address]),
    latitude: numVal(f[F.lat]),
    longitude: numVal(f[F.lng]),
    distanceFromDeal: numVal(f[F.distanceFromDeal]),
    estimatedDriveTime: numVal(f[F.estimatedDriveTime]),
    demandRelevance: strVal(f[F.demandRelevance]) || defaults.demandRelevance || "",
    demandSegment: strVal(f[F.demandSegment]) || defaults.demandSegment || "",
    demandPattern: multiSelect(f[F.demandPattern]).length
      ? multiSelect(f[F.demandPattern])
      : defaults.demandPattern || [],
    relevantHotelTypes: multiSelect(f[F.relevantHotelTypes]).length
      ? multiSelect(f[F.relevantHotelTypes])
      : defaults.relevantHotelTypes || [],
    hotelDemandRationale:
      strVal(f[F.hotelDemandRationale]) || defaults.hotelDemandRationale || "",
    source: sourceVals,
    sourceReference: sourceRef,
    dataConfidence: strVal(f[F.dataConfidence]) || "",
    includeOnRadarMap: hasIncludeField ? boolVal(f[F.includeOnRadarMap], true) : true,
    mapLayer: strVal(f[F.mapLayer]) || defaults.mapLayer || RADAR_CATEGORY_DEMAND_ANCHORS,
    mapIconType,
    visibility: strVal(f[F.visibility]) || "Internal Only",
    notes: strVal(f[F.notes]),
    lastVerified: strVal(f[F.lastVerified]),
    type: pointType || "Unknown",
  };

  return point;
}

/**
 * @param {DemandAnchorPoint} point
 */
export function toLegacyDemandAnchorItem(point) {
  return {
    id: point.id,
    name: point.name || "Unknown",
    type: point.type,
    pointType: point.pointType,
    pointSubtype: point.pointSubtype,
    lat: point.latitude ?? 0,
    lng: point.longitude ?? 0,
    latitude: point.latitude,
    longitude: point.longitude,
    city: point.city || "Unknown City",
    country: point.country || "Unknown Country",
    region: point.region || "Unknown Region",
    submarket: point.submarket || "",
    mapIconType: point.mapIconType,
    mapLayer: point.mapLayer,
    demandRelevance: point.demandRelevance,
    demandSegment: point.demandSegment,
    demandPattern: point.demandPattern,
    relevantHotelTypes: point.relevantHotelTypes,
    hotelDemandRationale: point.hotelDemandRationale,
    dataConfidence: point.dataConfidence,
    includeOnRadarMap: point.includeOnRadarMap,
    visibility: point.visibility,
    radarCategory: point.radarCategory,
    dealRecordId: point.dealRecordId,
  };
}

/**
 * @param {DemandAnchorPoint[]} points
 */
export function filterRadarVisiblePoints(points) {
  return (points || []).filter((p) => isDemandAnchorMapDisplayReady(p));
}
