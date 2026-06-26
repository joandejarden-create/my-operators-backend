/**
 * Normalize Travel Infrastructure Data records into radar map point objects.
 */

import {
  TRAVEL_INFRASTRUCTURE_FIELDS as F,
  TRAVEL_INFRASTRUCTURE_SOURCE_TABLE,
  RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
  INFRASTRUCTURE_ROLE_TO_SUBTYPE,
  LEGACY_TYPE_TO_MAP_ICON,
  POINT_TYPE_TO_MAP_ICON,
} from "./airtable-travel-infrastructure-fields.js";
import { getPointTypeDefaults, inferPointSubtype } from "./point-type-defaults.js";

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
  return ids;
}

/**
 * Resolve point type from new or legacy fields.
 * @param {Record<string, unknown>} fields
 */
export function resolvePointType(fields) {
  const f = fields || {};
  const explicit = strVal(f[F.pointType]);
  if (explicit) return explicit;
  const legacy = strVal(f[F.type]);
  if (legacy) return legacy;
  return "";
}

/**
 * @param {Record<string, unknown>} fields
 */
export function resolvePointSubtype(fields) {
  const f = fields || {};
  const explicit = strVal(f[F.pointSubtype]);
  if (explicit) return explicit;
  const role = strVal(f[F.infrastructureRole]);
  if (role && INFRASTRUCTURE_ROLE_TO_SUBTYPE[role]) {
    return INFRASTRUCTURE_ROLE_TO_SUBTYPE[role];
  }
  return inferPointSubtype(
    resolvePointType(f),
    "",
    role,
    strVal(f[F.airportType])
  );
}

/**
 * @param {Record<string, unknown>} fields
 */
export function resolveMapIconType(fields) {
  const f = fields || {};
  const explicit = strVal(f[F.mapIconType]);
  if (explicit) return explicit;
  const pointType = resolvePointType(f);
  return (
    POINT_TYPE_TO_MAP_ICON[pointType] ||
    LEGACY_TYPE_TO_MAP_ICON[pointType] ||
    "Port"
  );
}

/**
 * @typedef {object} RadarMapPoint
 * @property {string} id
 * @property {string} sourceTable
 * @property {string} name
 * @property {string} radarCategory
 * @property {string} pointType
 * @property {string} pointSubtype
 * @property {string[]} linkedMarketIds
 * @property {string[]} linkedDealIds
 * @property {string} address
 * @property {number|null} latitude
 * @property {number|null} longitude
 * @property {number|null} distanceFromDeal
 * @property {number|null} estimatedDriveTime
 * @property {string} demandRelevance
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
 * @property {string} city
 * @property {string} country
 * @property {string} region
 * @property {string} type
 */

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 * @param {{ applyDefaults?: boolean }} [opts]
 * @returns {RadarMapPoint}
 */
export function normalizeTravelInfrastructureToRadarPoint(record, opts = {}) {
  const f = record?.fields || {};
  const pointType = resolvePointType(f);
  const defaults = opts.applyDefaults !== false ? getPointTypeDefaults(pointType) : {};

  const pointSubtype = resolvePointSubtype(f) || defaults.pointSubtype || "Unknown";
  const mapIconType = resolveMapIconType(f) || defaults.mapIconType || "";

  const sourceRef =
    strVal(f[F.sourceReference]) ||
    strVal(f[F.sourceUrl]) ||
    "";

  const sourceVals = multiSelect(f[F.source]);
  if (!sourceVals.length && sourceRef) sourceVals.push("Public Source");

  const point = {
    id: record?.id || "",
    sourceTable: TRAVEL_INFRASTRUCTURE_SOURCE_TABLE,
    name: strVal(f[F.name]),
    radarCategory: strVal(f[F.radarCategory]) || defaults.radarCategory || RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
    pointType,
    pointSubtype,
    linkedMarketIds: linkedIds(f[F.linkedMarket]),
    linkedDealIds: extractDealIds(f),
    address: strVal(f[F.address]),
    latitude: numVal(f[F.lat]),
    longitude: numVal(f[F.lng]),
    distanceFromDeal: numVal(f[F.distanceFromDeal]),
    estimatedDriveTime: numVal(f[F.estimatedDriveTime]),
    demandRelevance: strVal(f[F.demandRelevance]) || defaults.demandRelevance || "",
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
    includeOnRadarMap: boolVal(f[F.includeOnRadarMap], true),
    mapLayer: strVal(f[F.mapLayer]) || defaults.mapLayer || RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
    mapIconType,
    visibility: strVal(f[F.visibility]) || "Internal Only",
    notes: strVal(f[F.notes]),
    lastVerified: strVal(f[F.lastVerified]),
    city: strVal(f[F.city]),
    country: strVal(f[F.country]),
    region: strVal(f[F.region]),
    submarket: strVal(f[F.submarket]),
    /** @deprecated use pointType — kept for radar map backward compatibility */
    type: pointType || strVal(f[F.type]) || "Unknown",
  };

  return point;
}

/**
 * Legacy API shape + selected radar fields for gradual client migration.
 * @param {RadarMapPoint} point
 */
export function toLegacyInfrastructureItem(point) {
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
    demandPattern: point.demandPattern,
    relevantHotelTypes: point.relevantHotelTypes,
    hotelDemandRationale: point.hotelDemandRationale,
    dataConfidence: point.dataConfidence,
    includeOnRadarMap: point.includeOnRadarMap,
    visibility: point.visibility,
    radarCategory: point.radarCategory,
  };
}

/**
 * @param {RadarMapPoint[]} points
 */
export function filterRadarVisiblePoints(points) {
  return (points || []).filter((p) => p.includeOnRadarMap !== false);
}
