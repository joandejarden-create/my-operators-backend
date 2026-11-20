/**
 * Hotel Explorer — Demand Drivers + Access & Connectivity read model (HE-QL-037).
 *
 * Reuses existing Airtable intelligence:
 *   - Demand Anchors (AIRTABLE_BASE_ID_ALT)
 *   - Travel Infrastructure Data (AIRTABLE_BASE_ID_ALT)
 * Canonical hotel identity: Hotel Property Census (HPC).
 *
 * GET-only. No paid providers. No writes. Empty tables → honest empty state.
 * Geographic entities are reusable across hotels (distance computed per hotel).
 */

import Airtable from "airtable";
import {
  formatHotelPropertyCensusAsBrandPresenceHotel,
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "./radar-internal-preview-hotels-v1.js";
import {
  HPC_TABLE_ID,
  GRAPH_BASE_ID_CANONICAL,
} from "./research-engine-v1/airtable-graph-contract-v1.js";
import { fetchDemandAnchorRecords } from "../demand-anchors/airtable-demand-anchors-io.js";
import { getDemandAnchorsAirtableConfig } from "../demand-anchors/demand-anchors-base.js";
import { DEMAND_ANCHORS_TABLE } from "../demand-anchors/airtable-demand-anchors-fields.js";
import { haversineKm } from "../demand-anchors/coordinate-verification.js";
import { fetchTravelInfrastructureRecords } from "../travel-infrastructure/airtable-travel-infrastructure-io.js";
import { getTravelInfrastructureAirtableConfig } from "../travel-infrastructure/travel-infrastructure-base.js";
import { TRAVEL_INFRASTRUCTURE_TABLE } from "../travel-infrastructure/airtable-travel-infrastructure-fields.js";

export const HE_DEMAND_ACCESS_READ_V1 = "he-demand-access-read-v1";

const REC_ID_RE = /^rec[A-Za-z0-9]{14}$/;

/** Location-type radii (km) — align with Hotel Explorer Area Hotels / context scope. */
export const HE_CONTEXT_RADIUS_KM = Object.freeze({
  Urban: 8.05,
  Suburban: 16.09,
  Airport: 16.09,
  Resort: 48.28,
  Rural: 40.23,
  default: 16.09,
});

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function createHpcBase() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId =
    process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID || GRAPH_BASE_ID_CANONICAL;
  if (!key || !baseId) return null;
  return { base: new Airtable({ apiKey: key }).base(baseId), baseId };
}

function pointCoords(p) {
  const lat = Number(p?.lat ?? p?.latitude);
  const lng = Number(p?.lng ?? p?.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) return null;
  return { lat, lng };
}

export function resolveContextRadiusKm(hotel = {}) {
  const loc = String(hotel.locationType || hotel.location_type || "").trim();
  if (HE_CONTEXT_RADIUS_KM[loc] != null) return HE_CONTEXT_RADIUS_KM[loc];
  if (/resort/i.test(loc)) return HE_CONTEXT_RADIUS_KM.Resort;
  if (/urban/i.test(loc)) return HE_CONTEXT_RADIUS_KM.Urban;
  if (/airport/i.test(loc)) return HE_CONTEXT_RADIUS_KM.Airport;
  if (/rural|small metro|interstate/i.test(loc)) return HE_CONTEXT_RADIUS_KM.Rural;
  return HE_CONTEXT_RADIUS_KM.default;
}

export async function resolveHpcHotelForDemandAccess(hotelId, { airtableBase = null } = {}) {
  const id = String(hotelId || "").trim();
  if (!REC_ID_RE.test(id)) {
    return { ok: false, error: "invalid_hotel_id" };
  }
  const ctx = airtableBase ? { base: airtableBase } : createHpcBase();
  if (!ctx?.base) return { ok: false, error: "airtable_not_configured" };
  try {
    const rec = await ctx.base(HOTEL_PROPERTY_CENSUS_TABLE).find(id);
    const hotel = formatHotelPropertyCensusAsBrandPresenceHotel(rec);
    const roomsRaw = rec.fields?.["Rooms / Keys"];
    const roomsN = Number(roomsRaw);
    return {
      ok: true,
      hotel: {
        ...hotel,
        rooms: Number.isFinite(roomsN) && roomsN > 0 ? roomsN : hotel.rooms || null,
        recordId: id,
        hpcTableId: HPC_TABLE_ID,
        censusSourceTable: HOTEL_PROPERTY_CENSUS_TABLE,
      },
    };
  } catch (err) {
    return {
      ok: false,
      error: "hpc_hotel_not_found",
      message: String(err?.message || err).slice(0, 240),
    };
  }
}

function enrichWithStraightLineDistance(points, origin) {
  if (!origin) return [];
  return (points || [])
    .map((p) => {
      const c = pointCoords(p);
      if (!c) return null;
      const distanceKm = haversineKm(origin.lat, origin.lng, c.lat, c.lng);
      if (!Number.isFinite(distanceKm)) return null;
      const verifiedDrive =
        p.estimatedDriveTime != null && Number.isFinite(Number(p.estimatedDriveTime))
          ? Number(p.estimatedDriveTime)
          : p.travelTime != null && String(p.travelTime).trim()
            ? p.travelTime
            : null;
      return {
        ...p,
        lat: c.lat,
        lng: c.lng,
        distanceKm,
        distanceMode: "STRAIGHT_LINE",
        // Preserve Airtable Estimated Drive Time only when present — never invent from haversine.
        estimatedDriveTime: verifiedDrive,
        travelTimeVerified: verifiedDrive != null,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.distanceKm - b.distanceKm);
}

function filterByRadius(items, radiusKm, { minAirports = 0 } = {}) {
  const inRadius = (items || []).filter((i) => i.distanceKm <= radiusKm);
  if (!minAirports) return inRadius;
  const isAirport = (i) => /airport/i.test(String(i.pointType || i.type || i.mapIconType || ""));
  const airportsIn = inRadius.filter(isAirport);
  if (airportsIn.length >= minAirports) return inRadius;
  const extras = (items || [])
    .filter(isAirport)
    .filter((a) => !inRadius.some((x) => x.id && a.id && x.id === a.id))
    .slice(0, Math.max(0, minAirports - airportsIn.length))
    .map((a) => ({ ...a, outsideSearchRadius: true }));
  return [...inRadius, ...extras].sort((a, b) => a.distanceKm - b.distanceKm);
}

function dedupeByIdOrName(items) {
  const seen = new Set();
  const out = [];
  for (const item of items || []) {
    const key =
      text(item.id) ||
      `${String(item.name || "")
        .toLowerCase()
        .trim()}|${Number(item.lat).toFixed(5)}|${Number(item.lng).toFixed(5)}`;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

/**
 * Demand Drivers read payload for one HPC hotel.
 */
export async function buildHeDemandDriversRead(hotelId, opts = {}) {
  const resolved = await resolveHpcHotelForDemandAccess(hotelId, opts);
  if (!resolved.ok) {
    return {
      ok: false,
      version: HE_DEMAND_ACCESS_READ_V1,
      tab: "demand-drivers",
      writeOnGet: false,
      paidProviders: false,
      error: resolved.error,
      message: resolved.message || null,
    };
  }
  const hotel = resolved.hotel;
  const origin = pointCoords(hotel);
  const radiusKm = opts.radiusKm != null ? Number(opts.radiusKm) : resolveContextRadiusKm(hotel);
  const daCfg = getDemandAnchorsAirtableConfig();

  const sourceMap = {
    entityStore: {
      baseId: daCfg?.baseId || null,
      tableName: DEMAND_ANCHORS_TABLE,
      role: "Reusable geographic demand entities (research once, reuse across hotels)",
    },
    hotelIdentity: {
      baseId: createHpcBase()?.baseId || GRAPH_BASE_ID_CANONICAL,
      tableId: HPC_TABLE_ID,
      tableName: HOTEL_PROPERTY_CENSUS_TABLE,
      hotelId: hotel.id,
      role: "Canonical hotel identity + coordinates",
    },
  };

  if (!origin) {
    return {
      ok: true,
      success: true,
      version: HE_DEMAND_ACCESS_READ_V1,
      tab: "demand-drivers",
      writeOnGet: false,
      paidProviders: false,
      applyAuthorized: false,
      hotel,
      sourceMap,
      items: [],
      metrics: { total: 0, inRadius: 0, radiusKm },
      emptyReason: "hotel_coordinates_missing",
      emptyMessage:
        "Canonical HPC hotel resolved, but verified coordinates are missing — Demand Drivers nearby search cannot run.",
      researchStatus: "BLOCKED_ON_HOTEL_COORDINATES",
    };
  }

  let fetchResult = { points: [], error: null, tableName: DEMAND_ANCHORS_TABLE };
  if (daCfg) {
    fetchResult = await fetchDemandAnchorRecords({
      country: hotel.country && hotel.country !== "Unknown Country" ? hotel.country : undefined,
      region: hotel.region && hotel.region !== "Unknown Region" ? hotel.region : undefined,
    });
  } else {
    fetchResult = { points: [], error: "airtable_config_missing", tableName: DEMAND_ANCHORS_TABLE };
  }

  const all = enrichWithStraightLineDistance(fetchResult.points || fetchResult.anchors || [], origin);
  const items = dedupeByIdOrName(filterByRadius(all, radiusKm)).slice(0, opts.limit || 40);
  const empty =
    !items.length &&
    (fetchResult.error
      ? "demand_anchors_fetch_failed"
      : (fetchResult.points || []).length === 0
        ? "no_demand_anchors_in_airtable_for_geography"
        : "no_demand_anchors_within_radius");

  return {
    ok: true,
    success: true,
    version: HE_DEMAND_ACCESS_READ_V1,
    tab: "demand-drivers",
    writeOnGet: false,
    paidProviders: false,
    applyAuthorized: false,
    hotel,
    sourceMap,
    radiusKm,
    distanceMode: "STRAIGHT_LINE",
    travelTimePolicy: "Do not invent verified drive/transit times; show straight-line distance only unless Airtable has verified Estimated Drive Time.",
    items,
    metrics: {
      totalCountryOrRegion: (fetchResult.points || fetchResult.anchors || []).length,
      totalWithCoords: all.length,
      inRadius: items.filter((i) => !i.outsideSearchRadius).length,
      radiusKm,
    },
    emptyReason: empty || null,
    emptyMessage: empty
      ? empty === "no_demand_anchors_in_airtable_for_geography"
        ? `No Demand Anchors records in Airtable for ${hotel.country || "this geography"} yet. Tab will populate automatically after canonical Demand Anchors research/import.`
        : empty === "no_demand_anchors_within_radius"
          ? `Demand Anchors exist for the geography, but none fall within ${radiusKm} km straight-line of this hotel.`
          : "Demand Anchors could not be loaded."
      : null,
    researchStatus: empty ? "AWAITING_RESEARCH_OR_IMPORT" : "POPULATED",
    fetchError: fetchResult.error || null,
  };
}

/**
 * Access & Connectivity read payload for one HPC hotel.
 */
export async function buildHeAccessConnectivityRead(hotelId, opts = {}) {
  const resolved = await resolveHpcHotelForDemandAccess(hotelId, opts);
  if (!resolved.ok) {
    return {
      ok: false,
      version: HE_DEMAND_ACCESS_READ_V1,
      tab: "access-connectivity",
      writeOnGet: false,
      paidProviders: false,
      error: resolved.error,
      message: resolved.message || null,
    };
  }
  const hotel = resolved.hotel;
  const origin = pointCoords(hotel);
  const radiusKm = opts.radiusKm != null ? Number(opts.radiusKm) : resolveContextRadiusKm(hotel);
  const tiCfg = getTravelInfrastructureAirtableConfig();

  const sourceMap = {
    entityStore: {
      baseId: tiCfg?.baseId || null,
      tableName: TRAVEL_INFRASTRUCTURE_TABLE,
      role: "Reusable airports / rail / roads / ports (research once, reuse across hotels)",
    },
    hotelIdentity: {
      baseId: createHpcBase()?.baseId || GRAPH_BASE_ID_CANONICAL,
      tableId: HPC_TABLE_ID,
      tableName: HOTEL_PROPERTY_CENSUS_TABLE,
      hotelId: hotel.id,
      role: "Canonical hotel identity + coordinates",
    },
  };

  if (!origin) {
    return {
      ok: true,
      success: true,
      version: HE_DEMAND_ACCESS_READ_V1,
      tab: "access-connectivity",
      writeOnGet: false,
      paidProviders: false,
      applyAuthorized: false,
      hotel,
      sourceMap,
      items: [],
      metrics: { total: 0, inRadius: 0, radiusKm },
      emptyReason: "hotel_coordinates_missing",
      emptyMessage:
        "Canonical HPC hotel resolved, but verified coordinates are missing — Access & Connectivity nearby search cannot run.",
      researchStatus: "BLOCKED_ON_HOTEL_COORDINATES",
    };
  }

  let fetchResult = { points: [], error: null, tableName: TRAVEL_INFRASTRUCTURE_TABLE };
  if (tiCfg) {
    fetchResult = await fetchTravelInfrastructureRecords({
      country: hotel.country && hotel.country !== "Unknown Country" ? hotel.country : undefined,
    });
  } else {
    fetchResult = { points: [], error: "airtable_config_missing", tableName: TRAVEL_INFRASTRUCTURE_TABLE };
  }

  const all = enrichWithStraightLineDistance(fetchResult.points || [], origin);
  const items = dedupeByIdOrName(
    filterByRadius(all, radiusKm, { minAirports: opts.minAirports != null ? opts.minAirports : 2 })
  ).slice(0, opts.limit || 24);

  const empty =
    !items.length &&
    (fetchResult.error
      ? "travel_infrastructure_fetch_failed"
      : (fetchResult.points || []).length === 0
        ? "no_travel_infrastructure_in_airtable_for_geography"
        : "no_travel_infrastructure_within_radius");

  return {
    ok: true,
    success: true,
    version: HE_DEMAND_ACCESS_READ_V1,
    tab: "access-connectivity",
    writeOnGet: false,
    paidProviders: false,
    applyAuthorized: false,
    hotel,
    sourceMap,
    radiusKm,
    distanceMode: "STRAIGHT_LINE",
    travelTimePolicy:
      "Straight-line distance only unless Travel Infrastructure Data has a verified Estimated Drive Time field value.",
    measurementToggle: "mi_km_contextual_on_access_tab",
    items,
    metrics: {
      totalCountry: (fetchResult.points || []).length,
      totalWithCoords: all.length,
      inRadius: items.filter((i) => !i.outsideSearchRadius).length,
      airports: items.filter((i) => /airport/i.test(String(i.pointType || i.type || ""))).length,
      radiusKm,
    },
    emptyReason: empty || null,
    emptyMessage: empty
      ? empty === "no_travel_infrastructure_in_airtable_for_geography"
        ? `No Travel Infrastructure Data records in Airtable for ${hotel.country || "this geography"} yet. Tab will populate automatically after canonical TI research/import.`
        : empty === "no_travel_infrastructure_within_radius"
          ? `Travel Infrastructure exists for the geography, but none fall within ${radiusKm} km straight-line of this hotel.`
          : "Travel Infrastructure could not be loaded."
      : null,
    researchStatus: empty ? "AWAITING_RESEARCH_OR_IMPORT" : "POPULATED",
    fetchError: fetchResult.error || null,
  };
}
