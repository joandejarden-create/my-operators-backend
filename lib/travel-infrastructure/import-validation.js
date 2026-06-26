/**
 * Travel Infrastructure — import validation (additional point types workflow).
 */

import { applyPointTypeDefaults } from "./point-type-defaults.js";
import {
  normalizeAnchorName,
  nameSimilarity,
  coordsWithinTolerance,
} from "../demand-anchors/import-validation.js";

/** Types not yet populated in the 714-record backfill set. */
export const TRAVEL_INFRA_ADDITIONAL_POINT_TYPES = [
  "Train Station",
  "Highway Access",
  "Bus Terminal",
  "Ferry Terminal",
  "Port / Maritime",
];

const TYPE_SET = new Set(TRAVEL_INFRA_ADDITIONAL_POINT_TYPES);

function numCoord(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export function coerceTravelInfraImportPoint(raw, defaults = {}) {
  if (!raw || typeof raw !== "object") return raw;
  return {
    name: raw.name || raw.Name || "",
    pointType: raw.pointType || raw.type || raw["Point Type"] || raw.Type || "",
    pointSubtype: raw.pointSubtype || raw["Point Subtype"] || "",
    city: raw.city || raw.City || defaults.market || "",
    country: raw.country || raw.Country || defaults.country || "",
    region: raw.region || raw.Region || defaults.region || "",
    submarket: raw.submarket || raw.Submarket || "",
    latitude: raw.latitude ?? raw.lat ?? raw.Latitude,
    longitude: raw.longitude ?? raw.lng ?? raw.Longitude,
    address: raw.address || "",
    sourceReference: raw.sourceReference || raw.sourceUrl || "",
    notes: raw.notes || "",
    dataConfidence: raw.dataConfidence || "Medium",
    includeOnRadarMap: raw.includeOnRadarMap !== false,
  };
}

export function isTravelInfraDuplicateCandidate(candidate, existing, ctx = {}) {
  const candName = normalizeAnchorName(candidate.name);
  const existName = normalizeAnchorName(existing.name);
  if (candName && existName && candName === candName) {
    const cc = String(candidate.city || "").toLowerCase() === String(existing.city || "").toLowerCase();
    const co = String(candidate.country || "").toLowerCase() === String(existing.country || "").toLowerCase();
    if (cc && co) return { duplicate: true, reason: "same_name_city_country" };
  }

  const candRef = String(candidate.sourceReference || "").trim().toLowerCase();
  const existRef = String(existing.sourceReference || existing.sourceUrl || "").trim().toLowerCase();
  if (candRef && existRef && candRef === existRef) {
    return { duplicate: true, reason: "same_source_reference" };
  }

  if (
    coordsWithinTolerance(
      candidate.latitude ?? candidate.lat,
      candidate.longitude ?? candidate.lng,
      existing.latitude ?? existing.lat,
      existing.longitude ?? existing.lng
    )
  ) {
    return { duplicate: true, reason: "same_coordinates" };
  }

  if (
    String(candidate.pointType || "") === String(existing.pointType || existing.type || "") &&
    nameSimilarity(candidate.name, existing.name) >= 0.85
  ) {
    return { duplicate: true, reason: "similar_name_same_type" };
  }

  return { duplicate: false };
}

export function validateTravelInfraImportPoint(item, index, defaults = {}) {
  const errors = [];
  const warnings = [];
  const coerced = coerceTravelInfraImportPoint(item, defaults);

  const name = String(coerced.name || "").trim();
  const pointType = String(coerced.pointType || "").trim();
  const city = String(coerced.city || "").trim();
  const country = String(coerced.country || "").trim();
  const lat = numCoord(coerced.latitude);
  const lng = numCoord(coerced.longitude);

  if (!name) errors.push("Name is required");
  if (!pointType) errors.push("Point Type is required");
  else if (!TYPE_SET.has(pointType)) {
    errors.push(
      `Point Type must be one of additional types: ${TRAVEL_INFRA_ADDITIONAL_POINT_TYPES.join(", ")}`
    );
  }
  if (!city && !defaults.market) errors.push("City or Market is required");
  if (!country) errors.push("Country is required");
  if (lat == null) errors.push("Latitude is required");
  if (lng == null) errors.push("Longitude is required");

  if (!coerced.sourceReference) warnings.push("Source URL / Reference recommended");

  let normalized = null;
  if (!errors.length) {
    normalized = applyPointTypeDefaults({
      ...coerced,
      name,
      pointType,
      city: city || defaults.market,
      country,
      region: coerced.region || defaults.region,
      latitude: lat,
      longitude: lng,
    });
  }

  return { index, valid: errors.length === 0, errors, warnings, normalized };
}

export function buildTravelInfraImportPreview(items, existingPoints = [], ctx = {}) {
  const preview = [];
  const warnings = [];
  const duplicates = [];
  let valid = 0;

  (items || []).forEach((raw, index) => {
    const validation = validateTravelInfraImportPoint(raw, index, ctx);
    const row = {
      index,
      include: validation.valid,
      name: validation.normalized?.name || coerceTravelInfraImportPoint(raw, ctx).name,
      pointType: validation.normalized?.pointType || coerceTravelInfraImportPoint(raw, ctx).pointType,
      city: validation.normalized?.city || "",
      country: validation.normalized?.country || "",
      latitude: validation.normalized?.latitude,
      longitude: validation.normalized?.longitude,
      warnings: [...validation.warnings],
      errors: validation.errors,
      duplicateStatus: "none",
      valid: validation.valid,
      normalized: validation.normalized,
    };

    if (!validation.valid) {
      row.include = false;
      preview.push(row);
      return;
    }
    valid += 1;

    for (const ex of existingPoints) {
      const dup = isTravelInfraDuplicateCandidate(validation.normalized, ex, ctx);
      if (dup.duplicate) {
        row.duplicateStatus = "possible_duplicate";
        row.warnings.push(`Possible duplicate of "${ex.name}" (${dup.reason})`);
        duplicates.push({ index, name: row.name, existingName: ex.name, reason: dup.reason });
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
      warnings: warnings.length,
      duplicates: duplicates.length,
      missingCoordinates: preview.filter((r) => r.latitude == null).length,
      rejected: (items?.length || 0) - valid,
    },
  };
}
