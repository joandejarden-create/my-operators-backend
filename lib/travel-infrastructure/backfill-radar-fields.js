/**
 * Backfill Travel Infrastructure Data legacy fields → radar extension fields.
 * Pure logic — safe to unit test without Airtable.
 */

import { TRAVEL_INFRASTRUCTURE_FIELDS as F } from "./airtable-travel-infrastructure-fields.js";
import { RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE } from "./airtable-travel-infrastructure-fields.js";
import { getPointTypeDefaults } from "./point-type-defaults.js";

const LEGACY_TYPE_NORMALIZE = {
  airport: "Airport",
  "cruise port": "Cruise Port",
  "convention center": "Convention Center",
  "train station": "Train Station",
  "highway access": "Highway Access",
  "bus terminal": "Bus Terminal",
  "ferry terminal": "Ferry Terminal",
  "port / maritime": "Port / Maritime",
};

/** Backfill-specific overrides (user spec) on top of getPointTypeDefaults. */
const BACKFILL_TYPE_OVERRIDES = {
  "Convention Center": {
    mapIconType: "Event",
    demandPattern: ["Group", "Event-Based", "Weekday", "Weekend", "Seasonal"],
    relevantHotelTypes: ["Full-Service", "Upper-Upscale", "Lifestyle", "Select-Service"],
  },
  "Highway Access": {
    pointSubtype: "Major Corridor",
  },
};

const BACKFILL_MAP_ICON = {
  Airport: "Airport",
  "Cruise Port": "Cruise Port",
  "Convention Center": "Event",
  "Train Station": "Train",
  "Highway Access": "Highway",
  "Bus Terminal": "Bus",
  "Ferry Terminal": "Ferry",
  "Port / Maritime": "Port",
};

/** Airtable select fallback when Event / Event-Based not in schema yet. */
export const MAP_ICON_SCHEMA_FALLBACK = {
  Event: "Convention",
};

export const DEMAND_PATTERN_SCHEMA_FALLBACK = {
  "Event-Based": "Group",
};

export const BACKFILL_MANAGED_AIRTABLE_FIELDS = [
  F.radarCategory,
  F.mapLayer,
  F.includeOnRadarMap,
  F.pointType,
  F.pointSubtype,
  F.mapIconType,
  F.demandRelevance,
  F.demandPattern,
  F.relevantHotelTypes,
  F.hotelDemandRationale,
  F.source,
  F.sourceReference,
  F.dataConfidence,
  F.visibility,
];

function strVal(v) {
  if (v == null) return "";
  return String(v).trim();
}

function fieldIsPopulated(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === "string" && value.trim() === "") return false;
  if (Array.isArray(value) && value.length === 0) return false;
  return true;
}

function arraysEqual(a, b) {
  const aa = Array.isArray(a) ? a : [];
  const bb = Array.isArray(b) ? b : [];
  if (aa.length !== bb.length) return false;
  return aa.every((v, i) => v === bb[i]);
}

function valuesEqual(current, next) {
  if (typeof next === "boolean") return current === next;
  if (Array.isArray(next)) return arraysEqual(current, next);
  return strVal(current) === strVal(next);
}

/**
 * @param {Record<string, unknown>} fields
 */
export function hasValidCoordinates(fields) {
  const lat = Number(fields?.[F.lat]);
  const lng = Number(fields?.[F.lng]);
  return Number.isFinite(lat) && Number.isFinite(lng) && !(lat === 0 && lng === 0);
}

/**
 * @param {Record<string, unknown>} fields
 */
export function resolveBackfillPointType(fields) {
  const f = fields || {};
  const existing = strVal(f[F.pointType]);
  if (existing) return existing;
  const legacy = strVal(f[F.type]);
  if (!legacy) return "";
  const key = legacy.toLowerCase();
  return LEGACY_TYPE_NORMALIZE[key] || legacy;
}

/**
 * @param {Record<string, unknown>} fields
 * @param {string} pointType
 */
export function inferBackfillPointSubtype(fields, pointType) {
  const f = fields || {};
  const existing = strVal(f[F.pointSubtype]);
  if (existing) return existing;

  const airportType = strVal(f[F.airportType]);
  const role = strVal(f[F.infrastructureRole]);
  const scaleTier = strVal(f[F.scaleTier]);
  const name = strVal(f[F.name]).toLowerCase();

  if (pointType === "Airport") {
    if (/international/i.test(airportType)) return "International Airport";
    if (/regional/i.test(airportType)) return "Regional Airport";
    if (/international/i.test(role)) return "International Airport";
    if (/regional|domestic|secondary|spoke/i.test(role)) return "Regional Airport";
    if (/mega|major|global|primary/i.test(scaleTier)) return "International Airport";
    return "Regional Airport";
  }

  if (pointType === "Cruise Port") return "Cruise Terminal";
  if (pointType === "Convention Center") return "Convention Center";
  if (pointType === "Train Station") return "Rail Hub";
  if (pointType === "Highway Access") return "Major Corridor";
  if (pointType === "Bus Terminal") return "Intercity Bus Terminal";
  if (pointType === "Ferry Terminal") return "Ferry Terminal";

  if (pointType === "Port / Maritime") {
    if (/marina/i.test(name) || /marina/i.test(role)) return "Marina";
    if (/ferry/i.test(name) || /ferry/i.test(role)) return "Ferry Port";
    return "Cargo Port";
  }

  return "Unknown";
}

/**
 * @param {string} pointType
 */
export function getBackfillTypeDefaults(pointType) {
  const base = getPointTypeDefaults(pointType);
  const overrides = BACKFILL_TYPE_OVERRIDES[pointType] || {};
  const mapIconType = BACKFILL_MAP_ICON[pointType] || base.mapIconType || "";
  return {
    ...base,
    ...overrides,
    mapIconType: overrides.mapIconType || mapIconType,
  };
}

/**
 * @param {Record<string, unknown>} fields
 */
export function inferBackfillDataConfidence(fields) {
  const hasCoords = hasValidCoordinates(fields);
  const sourceUrl = strVal(fields[F.sourceUrl]) || strVal(fields[F.sourceReference]);
  if (hasCoords && sourceUrl) return "High";
  if (hasCoords) return "Medium";
  return "Low";
}

/**
 * @param {Record<string, unknown>} fields
 */
export function inferBackfillSource(fields) {
  if (fieldIsPopulated(fields[F.source])) return strVal(fields[F.source]);
  return "Existing Dataset";
}

/**
 * Build full target Airtable field map (airtable column names → values).
 * @param {Record<string, unknown>} fields
 */
export function buildBackfillTargetFields(fields) {
  const f = fields || {};
  const pointType = resolveBackfillPointType(f);
  const typeDefaults = getBackfillTypeDefaults(pointType);
  const hasCoords = hasValidCoordinates(f);
  const sourceUrl = strVal(f[F.sourceUrl]);

  const target = {
    [F.radarCategory]: RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
    [F.mapLayer]: RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
    [F.includeOnRadarMap]: hasCoords,
    [F.pointType]: pointType,
    [F.pointSubtype]: inferBackfillPointSubtype(f, pointType),
    [F.mapIconType]: typeDefaults.mapIconType || "",
    [F.demandRelevance]: typeDefaults.demandRelevance || "",
    [F.demandPattern]: typeDefaults.demandPattern || [],
    [F.relevantHotelTypes]: typeDefaults.relevantHotelTypes || [],
    [F.hotelDemandRationale]: typeDefaults.hotelDemandRationale || "",
    [F.source]: inferBackfillSource(f),
    [F.sourceReference]: strVal(f[F.sourceReference]) || sourceUrl,
    [F.dataConfidence]: inferBackfillDataConfidence(f),
    [F.visibility]: fieldIsPopulated(f[F.visibility]) ? strVal(f[F.visibility]) : "Internal Only",
  };

  return target;
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> }} record
 * @param {{ force?: boolean }} [options]
 */
export function buildBackfillPatch(record, options = {}) {
  const fields = record?.fields || {};
  const force = Boolean(options.force);
  const target = buildBackfillTargetFields(fields);
  const patch = {};
  const skippedFields = [];

  for (const key of BACKFILL_MANAGED_AIRTABLE_FIELDS) {
    const nextVal = target[key];
    const currentVal = fields[key];

    if (!force && fieldIsPopulated(currentVal)) {
      skippedFields.push(key);
      continue;
    }

    if (!valuesEqual(currentVal, nextVal)) {
      patch[key] = nextVal;
    }
  }

  const pointType = target[F.pointType];
  const country = strVal(fields[F.country]) || "Unknown";
  const needsUpdate = Object.keys(patch).length > 0;

  return {
    recordId: record?.id || "",
    name: strVal(fields[F.name]),
    pointType,
    country,
    hasCoordinates: hasValidCoordinates(fields),
    patch,
    skippedFields,
    needsUpdate,
    target,
  };
}

/**
 * Sanitize patch values against Airtable schema (drop unknown select options).
 * @param {Record<string, unknown>} patch
 * @param {Set<string>|null} schema
 */
export function sanitizeBackfillPatchForSchema(patch, schema) {
  if (!schema || !schema.size) return { ...patch };

  const out = {};
  for (const [key, value] of Object.entries(patch)) {
    if (!schema.has(key)) continue;

    if (key === F.mapIconType && typeof value === "string") {
      out[key] = MAP_ICON_SCHEMA_FALLBACK[value] || value;
      continue;
    }

    if (key === F.demandPattern && Array.isArray(value)) {
      out[key] = value.map((v) => DEMAND_PATTERN_SCHEMA_FALLBACK[v] || v);
      continue;
    }

    out[key] = value;
  }
  return out;
}

/**
 * @param {Array<ReturnType<typeof buildBackfillPatch>>} results
 */
export function summarizeBackfillResults(results) {
  const summary = {
    totalScanned: results.length,
    needingUpdate: 0,
    skippedAlreadyPopulated: 0,
    missingCoordinates: 0,
    byPointType: {},
    byCountry: {},
    samples: [],
  };

  for (const r of results) {
    if (!r.hasCoordinates) summary.missingCoordinates += 1;
    if (r.needsUpdate) {
      summary.needingUpdate += 1;
      const pt = r.pointType || "Unknown";
      summary.byPointType[pt] = (summary.byPointType[pt] || 0) + 1;
      summary.byCountry[r.country] = (summary.byCountry[r.country] || 0) + 1;
      if (summary.samples.length < 10) {
        summary.samples.push({
          id: r.recordId,
          name: r.name,
          pointType: r.pointType,
          country: r.country,
          patchKeys: Object.keys(r.patch),
          includeOnRadarMap: r.patch[F.includeOnRadarMap],
          pointSubtype: r.patch[F.pointSubtype] || r.target[F.pointSubtype],
        });
      }
    } else if (r.skippedFields.length > 0) {
      summary.skippedAlreadyPopulated += 1;
    }
  }

  return summary;
}
