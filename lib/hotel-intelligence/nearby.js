/**
 * Radius / nearby hotel queries over a census (or candidate) slice.
 * V1: in-memory Haversine — no PostGIS.
 */

import {
  distanceMeters,
  parseCoords,
  normalizeKey,
} from "../independent-census/match-current-census.js";
import { MAP_CENSUS_FIELDS } from "./map_hotel_intelligence_fields.js";
import { createExternalIdRegistry } from "./external-ids.js";

export const NEARBY_VERSION = "hotel-intelligence-nearby-v1";

function inRange(n, min, max) {
  if (!Number.isFinite(n)) return false;
  if (min != null && Number.isFinite(Number(min)) && n < Number(min)) return false;
  if (max != null && Number.isFinite(Number(max)) && n > Number(max)) return false;
  return true;
}

/**
 * @param {object[]} censusRecords
 * @param {{ latitude: number, longitude: number, radius_km: number, filters?: object, limit?: number }} query
 * @param {object} [opts]
 */
export function findNearbyHotels(censusRecords, query, opts = {}) {
  const lat = Number(query.latitude);
  const lng = Number(query.longitude);
  const radiusKm = Number(query.radius_km ?? query.radius ?? 5);
  const limit = Math.min(200, Math.max(1, Number(query.limit) || 50));
  const filters = query.filters || {};
  const idRegistry = opts.idRegistry || createExternalIdRegistry(opts.store);

  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return {
      ok: false,
      error: "latitude_longitude_required",
      hotels: [],
      version: NEARBY_VERSION,
    };
  }
  if (!Number.isFinite(radiusKm) || radiusKm <= 0) {
    return {
      ok: false,
      error: "radius_km_must_be_positive",
      hotels: [],
      version: NEARBY_VERSION,
    };
  }

  const origin = { lat, lng };
  const out = [];

  for (const rec of censusRecords || []) {
    const f = rec.fields || {};
    const coords = parseCoords(
      f[MAP_CENSUS_FIELDS.latitude],
      f[MAP_CENSUS_FIELDS.longitude]
    );
    if (!coords) continue;
    const d = distanceMeters(origin, coords);
    if (d == null || d > radiusKm * 1000) continue;

    const brand = f[MAP_CENSUS_FIELDS.brandName] || null;
    const parent = f[MAP_CENSUS_FIELDS.parentCompanyName] || null;
    const chainScale = f[MAP_CENSUS_FIELDS.chainScale] || null;
    const status = f[MAP_CENSUS_FIELDS.status] || null;
    const rooms =
      f[MAP_CENSUS_FIELDS.roomCount] != null
        ? Number(f[MAP_CENSUS_FIELDS.roomCount])
        : null;

    if (filters.brand) {
      const b = normalizeKey(filters.brand);
      if (!normalizeKey(brand || "").includes(b) && !normalizeKey(parent || "").includes(b)) {
        continue;
      }
    }
    if (filters.parent_company) {
      const p = normalizeKey(filters.parent_company);
      if (!normalizeKey(parent || "").includes(p)) continue;
    }
    if (filters.chain_scale) {
      if (
        normalizeKey(chainScale || "") !== normalizeKey(filters.chain_scale)
      ) {
        continue;
      }
    }
    if (filters.status) {
      if (!normalizeKey(status || "").includes(normalizeKey(filters.status))) {
        continue;
      }
    }
    if (
      filters.room_count_min != null ||
      filters.room_count_max != null
    ) {
      if (!inRange(rooms, filters.room_count_min, filters.room_count_max)) {
        continue;
      }
    }

    const hotelId = idRegistry.ensureHotelIdForAirtable(rec.id, {
      property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });

    out.push({
      hotel_id: hotelId,
      hotel_name:
        f[MAP_CENSUS_FIELDS.officialName] ||
        f[MAP_CENSUS_FIELDS.propertyName] ||
        null,
      airtable_record_id: rec.id,
      distance_km: Math.round((d / 1000) * 1000) / 1000,
      distance_m: Math.round(d),
      rooms: Number.isFinite(rooms) ? rooms : null,
      brand,
      parent_company: parent,
      chain_scale: chainScale,
      status,
      confidence: f[MAP_CENSUS_FIELDS.identityConfidence] || null,
      latitude: coords.lat,
      longitude: coords.lng,
    });
  }

  out.sort((a, b) => a.distance_m - b.distance_m);

  return {
    ok: true,
    origin: { latitude: lat, longitude: lng },
    radius_km: radiusKm,
    count: Math.min(out.length, limit),
    hotels: out.slice(0, limit),
    version: NEARBY_VERSION,
  };
}
