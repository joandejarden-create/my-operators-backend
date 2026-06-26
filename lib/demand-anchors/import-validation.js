/**
 * Demand Anchors — import preview validation and duplicate detection.
 */

import {
  POINT_TYPES,
  DEMAND_RELEVANCE_OPTIONS,
  DATA_CONFIDENCE_OPTIONS,
} from "./airtable-demand-anchors-fields.js";
import { applyPointTypeDefaults } from "./point-type-defaults.js";
import { isValidSubmarketOption } from "../radar-submarket.js";

const POINT_TYPE_SET = new Set(POINT_TYPES);
const RELEVANCE_SET = new Set(DEMAND_RELEVANCE_OPTIONS);
const CONFIDENCE_SET = new Set(DATA_CONFIDENCE_OPTIONS);

/** ~150 meters at equator */
const COORD_TOLERANCE_DEG = 0.00135;
const NAME_SIMILARITY_THRESHOLD = 0.85;

export function normalizeAnchorName(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function nameSimilarity(a, b) {
  const x = normalizeAnchorName(a);
  const y = normalizeAnchorName(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  if (x.length >= 5 && y.length >= 5 && (x.includes(y) || y.includes(x))) return 0.92;

  const bigrams = (s) => {
    const out = new Map();
    for (let i = 0; i < s.length - 1; i += 1) {
      const bg = s.slice(i, i + 2);
      out.set(bg, (out.get(bg) || 0) + 1);
    }
    return out;
  };
  const bx = bigrams(x);
  const by = bigrams(y);
  let overlap = 0;
  for (const [bg, count] of bx) {
    overlap += Math.min(count, by.get(bg) || 0);
  }
  const total =
    [...bx.values()].reduce((a, c) => a + c, 0) + [...by.values()].reduce((a, c) => a + c, 0);
  return total ? (2 * overlap) / total : 0;
}

function numCoord(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export function coordsWithinTolerance(lat1, lng1, lat2, lng2, toleranceDeg = COORD_TOLERANCE_DEG) {
  const a = numCoord(lat1);
  const b = numCoord(lng1);
  const c = numCoord(lat2);
  const d = numCoord(lng2);
  if (a == null || b == null || c == null || d == null) return false;
  return Math.abs(a - c) <= toleranceDeg && Math.abs(b - d) <= toleranceDeg;
}

function normalizeCityCountry(city, country) {
  return {
    city: String(city || "").trim().toLowerCase(),
    country: String(country || "").trim().toLowerCase(),
  };
}

function normalizeMarketKey(market, city, region) {
  return String(market || city || region || "")
    .trim()
    .toLowerCase();
}

/**
 * @param {object} candidate — normalized import row
 * @param {object} existing — existing anchor (normalized radar point or import row)
 * @param {object} [ctx]
 */
export function isDuplicateCandidate(candidate, existing, ctx = {}) {
  const candName = normalizeAnchorName(candidate.name);
  const existName = normalizeAnchorName(existing.name);
  const candCC = normalizeCityCountry(candidate.city, candidate.country);
  const existCC = normalizeCityCountry(existing.city, existing.country);

  if (candName && existName && candName === existName && candCC.city === existCC.city && candCC.country === existCC.country) {
    return { duplicate: true, reason: "same_normalized_name_city_country" };
  }

  const candRef = String(candidate.sourceReference || candidate.sourceUrl || "").trim().toLowerCase();
  const existRef = String(existing.sourceReference || existing.sourceUrl || "").trim().toLowerCase();
  if (candRef && existRef && candRef === existRef) {
    return { duplicate: true, reason: "same_source_reference" };
  }

  const candLat = numCoord(candidate.latitude ?? candidate.lat);
  const candLng = numCoord(candidate.longitude ?? candidate.lng);
  const existLat = numCoord(existing.latitude ?? existing.lat);
  const existLng = numCoord(existing.longitude ?? existing.lng);
  if (
    candLat != null &&
    candLng != null &&
    existLat != null &&
    existLng != null &&
    coordsWithinTolerance(candLat, candLng, existLat, existLng)
  ) {
    return { duplicate: true, reason: "same_coordinates" };
  }

  const candType = String(candidate.pointType || "").trim();
  const existType = String(existing.pointType || "").trim();
  if (candType && existType && candType === existType) {
    const candMarket = normalizeMarketKey(ctx.market, candidate.city, candidate.region);
    const existMarket = normalizeMarketKey(ctx.market, existing.city, existing.region);
    if (candMarket && existMarket && candMarket === existMarket && candName && existName) {
      const sim = nameSimilarity(candidate.name, existing.name);
      if (sim >= NAME_SIMILARITY_THRESHOLD) {
        return { duplicate: true, reason: "similar_name_same_type_market", similarity: sim };
      }
    }
    if (candName && existName) {
      const sim = nameSimilarity(candidate.name, existing.name);
      if (sim >= 0.92) {
        return { duplicate: true, reason: "similar_name_same_point_type", similarity: sim };
      }
    }
  }

  return { duplicate: false };
}

/**
 * Coerce raw import object to common shape.
 * @param {object} raw
 * @param {object} [defaults] — market, country, region from request envelope
 */
export function coerceImportPoint(raw, defaults = {}) {
  if (!raw || typeof raw !== "object") return raw;
  return {
    name: raw.name || raw["Demand Anchor Name"] || "",
    pointType: raw.pointType || raw["Point Type"] || "",
    pointSubtype: raw.pointSubtype || raw["Point Subtype"] || "",
    city: raw.city || raw.City || defaults.market || defaults.city || "",
    country: raw.country || raw.Country || defaults.country || "",
    region: raw.region || raw.Region || defaults.region || "",
    market: defaults.market || raw.market || "",
    submarket: raw.submarket || raw.Submarket || "",
    latitude: raw.latitude ?? raw.lat ?? raw.Latitude,
    longitude: raw.longitude ?? raw.lng ?? raw.Longitude,
    address: raw.address || raw["Address / Location"] || "",
    demandSegment: raw.demandSegment || raw["Demand Segment"] || "",
    demandRelevance: raw.demandRelevance || raw["Demand Relevance"] || "",
    dataConfidence: raw.dataConfidence || raw["Data Confidence"] || "",
    source: raw.source || raw.Source,
    sourceReference: raw.sourceReference || raw.sourceUrl || raw["Source URL / Reference"] || "",
    notes: raw.notes || raw.Notes || "",
    visibility: raw.visibility || raw.Visibility,
    includeOnRadarMap: raw.includeOnRadarMap,
  };
}

/**
 * @param {object} item — coerced item
 * @param {number} index
 * @param {object} [defaults]
 */
export function validateImportPoint(item, index, defaults = {}) {
  const errors = [];
  const warnings = [];
  const coerced = coerceImportPoint(item, defaults);

  const name = String(coerced.name || "").trim();
  const pointType = String(coerced.pointType || "").trim();
  const city = String(coerced.city || "").trim();
  const market = String(coerced.market || defaults.market || "").trim();
  const country = String(coerced.country || "").trim();
  const lat = numCoord(coerced.latitude);
  const lng = numCoord(coerced.longitude);

  if (!name) errors.push("Demand Anchor Name is required");
  if (!pointType) errors.push("Point Type is required");
  else if (!POINT_TYPE_SET.has(pointType)) {
    errors.push(`Point Type "${pointType}" is not in the allowed list`);
  }
  if (!city && !market) errors.push("City or Market is required");
  if (!country) errors.push("Country is required");
  if (lat == null) errors.push("Latitude is required");
  if (lng == null) errors.push("Longitude is required");

  if (lat != null && (lat < -90 || lat > 90)) errors.push("Latitude must be between -90 and 90");
  if (lng != null && (lng < -180 || lng > 180)) errors.push("Longitude must be between -180 and 180");

  if (!coerced.demandSegment) warnings.push("Demand Segment missing — defaults will be applied");
  if (!coerced.demandRelevance) warnings.push("Demand Relevance missing — defaults will be applied");
  if (!coerced.dataConfidence) warnings.push("Data Confidence missing — defaults will be applied");
  if (!coerced.source && !coerced.sourceReference) {
    warnings.push("Source and Source URL / Reference missing — recommended for audit trail");
  }
  if (coerced.submarket && !isValidSubmarketOption(coerced.submarket, country)) {
    warnings.push(
      `Submarket "${coerced.submarket}" is not in the ${country || "global"} allowed list — will normalize on import`
    );
  }

  if (coerced.demandRelevance && !RELEVANCE_SET.has(coerced.demandRelevance)) {
    warnings.push(`Demand Relevance "${coerced.demandRelevance}" may not match schema options`);
  }
  if (coerced.dataConfidence && !CONFIDENCE_SET.has(coerced.dataConfidence)) {
    warnings.push(`Data Confidence "${coerced.dataConfidence}" may not match schema options`);
  }

  let normalized = null;
  if (!errors.length) {
    const base = {
      ...coerced,
      name,
      pointType,
      city: city || market,
      country,
      region: coerced.region || defaults.region || "",
      latitude: lat,
      longitude: lng,
      lat,
      lng,
    };
    for (const key of [
      "demandSegment",
      "demandRelevance",
      "dataConfidence",
      "source",
      "sourceReference",
      "notes",
    ]) {
      if (base[key] === "") delete base[key];
    }
    normalized = applyPointTypeDefaults(base);
  }

  return {
    index,
    valid: errors.length === 0,
    errors,
    warnings,
    normalized,
    missingCoordinates: lat == null || lng == null,
  };
}

/**
 * @param {object[]} items
 * @param {object[]} existingAnchors
 * @param {object} [ctx] — { market, country, region }
 */
export function buildDemandAnchorsImportPreview(items, existingAnchors = [], ctx = {}) {
  const preview = [];
  const warnings = [];
  const duplicates = [];
  let valid = 0;
  let warningCount = 0;
  let duplicateCount = 0;
  let missingCoordinates = 0;

  (items || []).forEach((raw, index) => {
    const validation = validateImportPoint(raw, index, ctx);
    const row = {
      index,
      include: validation.valid,
      name: validation.normalized?.name || coerceImportPoint(raw, ctx).name,
      pointType: validation.normalized?.pointType || coerceImportPoint(raw, ctx).pointType,
      demandSegment: validation.normalized?.demandSegment || "",
      city: validation.normalized?.city || coerceImportPoint(raw, ctx).city,
      country: validation.normalized?.country || coerceImportPoint(raw, ctx).country,
      latitude: validation.normalized?.latitude ?? validation.normalized?.lat,
      longitude: validation.normalized?.longitude ?? validation.normalized?.lng,
      demandRelevance: validation.normalized?.demandRelevance || "",
      dataConfidence: validation.normalized?.dataConfidence || "",
      warnings: [...validation.warnings],
      errors: validation.errors,
      duplicateStatus: "none",
      duplicateOf: null,
      valid: validation.valid,
      normalized: validation.normalized,
    };

    if (validation.missingCoordinates) missingCoordinates += 1;

    if (!validation.valid) {
      row.include = false;
      preview.push(row);
      return;
    }

    valid += 1;
    if (validation.warnings.length) {
      warningCount += 1;
      warnings.push({ index, messages: validation.warnings });
    }

    for (const ex of existingAnchors || []) {
      const dup = isDuplicateCandidate(validation.normalized, ex, ctx);
      if (dup.duplicate) {
        row.duplicateStatus = "possible_duplicate";
        row.duplicateOf = { id: ex.id, name: ex.name, reason: dup.reason };
        row.warnings.push(`Possible duplicate of existing "${ex.name}" (${dup.reason})`);
        duplicates.push({
          index,
          name: validation.normalized.name,
          existingId: ex.id,
          existingName: ex.name,
          reason: dup.reason,
        });
        duplicateCount += 1;
        break;
      }
    }

    for (let j = 0; j < preview.length; j += 1) {
      const prior = preview[j];
      if (!prior.valid || !prior.normalized) continue;
      const dup = isDuplicateCandidate(validation.normalized, prior.normalized, ctx);
      if (dup.duplicate) {
        row.duplicateStatus = "possible_duplicate";
        if (!row.duplicateOf) {
          row.duplicateOf = { index: prior.index, name: prior.name, reason: dup.reason };
        }
        row.warnings.push(`Possible duplicate of import row ${prior.index + 1} (${dup.reason})`);
        if (!duplicates.some((d) => d.index === index)) {
          duplicates.push({
            index,
            name: validation.normalized.name,
            existingName: prior.name,
            reason: dup.reason,
          });
          duplicateCount += 1;
        }
        break;
      }
    }

    preview.push(row);
  });

  return {
    preview,
    warnings,
    duplicates,
    summary: {
      totalSubmitted: items?.length || 0,
      valid,
      warnings: warningCount,
      duplicates: duplicateCount,
      missingCoordinates,
      rejected: (items?.length || 0) - valid,
    },
  };
}

/**
 * @param {object[]} previewRows
 * @param {boolean} skipDuplicates
 */
export function filterCommitRecords(previewRows, skipDuplicates = true) {
  return (previewRows || []).filter((row) => {
    if (!row.include && row.include !== undefined) return false;
    if (row.valid === false) return false;
    if (!row.normalized) return false;
    if (skipDuplicates && row.duplicateStatus === "possible_duplicate") return false;
    return true;
  });
}
