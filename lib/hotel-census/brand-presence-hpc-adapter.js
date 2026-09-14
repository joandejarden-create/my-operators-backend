/**
 * HPC Brand Presence adapter (P8.3 / P8.5).
 *
 * Feature flag: BRAND_PRESENCE_HPC_V2=1 enables capability.
 * Request gate (P8.5): HPC only when HE opts in via censusSource=hpc /
 * product=hotel-explorer (see brand-presence-hpc-request.js).
 * Scout/Radar omit opt-in and stay on Legacy.
 *
 * When HPC path is active: no silent Legacy Hotel Census fallback.
 * Rooms: OPTIONAL — null when not independently High+official (or High+gov open data).
 * Operator: OPTIONAL — null when absent.
 * Chain Scale: not on HPC — returned null (Scout may still need separate work).
 */

import Airtable from "airtable";
import { applyProductCensusQuarantine } from "./product-safe-census-fields.js";

export const BRAND_PRESENCE_HPC_V2_FLAG = "BRAND_PRESENCE_HPC_V2";

export const HPC_TABLE =
  process.env.AIRTABLE_HOTEL_PROPERTY_CENSUS_TABLE || "Hotel Property Census";

/** Official first-party room source types eligible for product rooms. */
export const HPC_PRODUCT_ROOM_SOURCE_TYPES = Object.freeze([
  "official_brand_directory",
  "official_property_page",
  "official_hotel_website",
]);

/** Rights-clear government open-data hosts (P8.4 Cadastur/RNT/Mincetur/BTPA). */
export const HPC_GOV_OPEN_DATA_ROOM_HOSTS = Object.freeze([
  "dados.turismo.gov.br",
  "www.datos.gov.co",
  "datos.gov.co",
  "www.datosabiertos.gob.pe",
  "datosabiertos.gob.pe",
  "www.barbadostouristaccommodation.com",
  "barbadostouristaccommodation.com",
]);

function hostFromUrl(url) {
  try {
    return new URL(String(url || "")).hostname.toLowerCase();
  } catch {
    return "";
  }
}

export const HPC_FIELDS = Object.freeze({
  name: "Property Name",
  canonicalName: "Canonical Property Name",
  dhl: "Dealality Hotel ID",
  brand: "Current Brand",
  brandFamily: "Brand Family",
  affiliationStatus: "Affiliation Status",
  statusDerived: null, // lifecycle derived client-side
  lat: "Latitude",
  lng: "Longitude",
  city: "City",
  country: "Country",
  state: "State / Region",
  postalCode: "Postal Code",
  rooms: "Rooms / Keys",
  roomsConfidence: "Rooms Confidence",
  roomsSourceType: "Rooms Source Type",
  roomsSourceUrl: "Rooms Source URL",
  market: "Market",
  submarket: "Submarket",
  website: "Official Property URL",
  address: "Address",
  futureOpening: "Future Opening Flag",
  productionUse: "Production Use Status",
  openingDate: "Opening Date",
  operator: "Operator / Management Company",
});

export function isBrandPresenceHpcV2Enabled(env = process.env) {
  const v = String(env[BRAND_PRESENCE_HPC_V2_FLAG] || "").trim();
  return v === "1" || /^true$/i.test(v) || /^yes$/i.test(v);
}

export function createBrandPresenceBase(env = process.env) {
  const key = env.AIRTABLE_API_KEY;
  const baseId = env.AIRTABLE_BASE_ID_ALT;
  if (!key || !baseId) return null;
  return new Airtable({ apiKey: key }).base(baseId);
}

function readText(fields, key) {
  if (!key) return null;
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  if (Array.isArray(raw)) {
    const joined = raw
      .map((item) => (typeof item === "string" ? item.trim() : item?.name || ""))
      .filter(Boolean)
      .join("; ");
    return joined || null;
  }
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return String(raw).trim() || null;
}

function readNumber(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

/**
 * Derive product status from HPC lifecycle signals (no Legacy status copy).
 */
export function deriveHpcLifecycleStatus(fields) {
  if (fields[HPC_FIELDS.futureOpening] === true) return "Pipeline";
  const aff = readText(fields, HPC_FIELDS.affiliationStatus);
  if (aff === "Future / Pipeline") return "Pipeline";
  const prod = readText(fields, HPC_FIELDS.productionUse);
  if (prod === "Do Not Use") return "Closed";
  return "Open";
}

/**
 * Product brand display from Affiliation Status + Current Brand.
 * Empty / Unknown affiliation → null (UI shows Unknown) — never invent Independent.
 */
export function deriveHpcBrandDisplay(fields) {
  const aff = readText(fields, HPC_FIELDS.affiliationStatus);
  const current = readText(fields, HPC_FIELDS.brand);
  const family = readText(fields, HPC_FIELDS.brandFamily);
  if (aff === "Independent") return "Independent";
  if (aff === "Soft-Branded / Collection") return current || family || "Soft-Branded / Collection";
  if (aff === "Branded" || aff === "Formerly Branded") return current || family || null;
  if (aff === "Brand-Unconfirmed") return current || null; // Unknown Brand if no Current Brand
  if (aff === "Future / Pipeline") return current || family || "Pipeline";
  if (aff === "Unknown" || !aff) return null;
  return current || null;
}

/**
 * Governed rooms: High + official first-party, OR High + trusted_secondary on rights-clear gov hosts.
 * Otherwise null (Unknown) — never Legacy.
 */
export function deriveHpcProductRooms(fields) {
  const conf = readText(fields, HPC_FIELDS.roomsConfidence);
  const src = readText(fields, HPC_FIELDS.roomsSourceType);
  const rooms = readNumber(fields, HPC_FIELDS.rooms);
  if (rooms == null) return null;
  if (conf !== "High") return null;
  if (HPC_PRODUCT_ROOM_SOURCE_TYPES.includes(src)) return rooms;
  if (src === "trusted_secondary_source") {
    const host = hostFromUrl(fields[HPC_FIELDS.roomsSourceUrl] || fields["Rooms Source URL"]);
    if (HPC_GOV_OPEN_DATA_ROOM_HOSTS.includes(host)) return rooms;
  }
  return null;
}

export const HPC_MAP_FIELDS = [
  HPC_FIELDS.name,
  HPC_FIELDS.canonicalName,
  HPC_FIELDS.dhl,
  HPC_FIELDS.brand,
  HPC_FIELDS.brandFamily,
  HPC_FIELDS.affiliationStatus,
  HPC_FIELDS.lat,
  HPC_FIELDS.lng,
  HPC_FIELDS.city,
  HPC_FIELDS.country,
  HPC_FIELDS.state,
  HPC_FIELDS.postalCode,
  HPC_FIELDS.rooms,
  HPC_FIELDS.roomsConfidence,
  HPC_FIELDS.roomsSourceType,
  HPC_FIELDS.roomsSourceUrl,
  HPC_FIELDS.market,
  HPC_FIELDS.submarket,
  HPC_FIELDS.website,
  HPC_FIELDS.address,
  HPC_FIELDS.futureOpening,
  HPC_FIELDS.productionUse,
  HPC_FIELDS.openingDate,
  HPC_FIELDS.operator,
];

/**
 * Format HPC Airtable record into Brand Presence hotel DTO.
 * Missing optional fields stay null/unknown — no Legacy fill.
 */
export function formatHpcHotelRecord(hotel) {
  const fields = hotel.fields || {};
  const lat = parseFloat(fields[HPC_FIELDS.lat]);
  const lng = parseFloat(fields[HPC_FIELDS.lng]);
  const hasCoords =
    Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);

  const brand = deriveHpcBrandDisplay(fields);
  const rooms = deriveHpcProductRooms(fields);
  const status = deriveHpcLifecycleStatus(fields);
  const dhl = readText(fields, HPC_FIELDS.dhl);

  return applyProductCensusQuarantine({
    id: hotel.id,
    dhl_: dhl,
    name: readText(fields, HPC_FIELDS.name) || readText(fields, HPC_FIELDS.canonicalName) || "Unknown Hotel",
    brand: brand || "Unknown Brand",
    parentCompany: readText(fields, HPC_FIELDS.brandFamily) || "Unknown",
    affiliationStatus: readText(fields, HPC_FIELDS.affiliationStatus),
    status,
    lat: hasCoords ? lat : 0,
    lng: hasCoords ? lng : 0,
    city: readText(fields, HPC_FIELDS.city) || "Unknown City",
    country: readText(fields, HPC_FIELDS.country) || "Unknown Country",
    region: null,
    locationType: null,
    rooms: rooms == null ? null : rooms,
    roomsDisplay: rooms == null ? "Unknown" : rooms,
    strNumber: null,
    chainScale: null,
    projectPhase: status === "Pipeline" ? "Pipeline" : null,
    propertyType: null,
    operationType: null,
    managementCompany: readText(fields, HPC_FIELDS.operator),
    website: readText(fields, HPC_FIELDS.website),
    telephone: null,
    address1: readText(fields, HPC_FIELDS.address),
    address2: null,
    market: readText(fields, HPC_FIELDS.market),
    submarket: readText(fields, HPC_FIELDS.submarket),
    openDate: readText(fields, HPC_FIELDS.openingDate),
    projectedOpenDate: null,
    starRating: null,
    amenities: null,
    hotelDescription: null,
    hotelHeadline: null,
    occupancyRate: null,
    adr: null,
    revpar: null,
    state: readText(fields, HPC_FIELDS.state),
    postalCode: readText(fields, HPC_FIELDS.postalCode),
    floors: null,
    totalMeetingSpace: null,
    dataConfidence: null,
    censusPropertyType: null,
    hotelServiceModel: null,
    amenityFlags: {},
    _source: "hotel_property_census",
    _adapter: "brand-presence-hpc-v2",
    _provenanceFlags: {
      rooms_optional_unknown_ok: rooms == null,
      chainScale_absent_on_hpc: true,
      strNumber_not_on_hpc: true,
      no_legacy_fallback: true,
    },
  });
}

export function getHpcTableSelectOptions({ limit, page, brand, status, region, search } = {}) {
  const selectOptions = {
    fields: [...HPC_MAP_FIELDS],
  };
  if (limit) {
    selectOptions.maxRecords = limit;
    selectOptions.pageSize = Math.min(100, limit);
  }
  // Formula filters — Affiliation Status / Property Name search only (no Legacy fields)
  const parts = [];
  if (brand) {
    const b = String(brand).replace(/'/g, "\\'");
    parts.push(
      `OR({${HPC_FIELDS.brand}}='${b}',{${HPC_FIELDS.brandFamily}}='${b}',{${HPC_FIELDS.affiliationStatus}}='${b}')`
    );
  }
  if (status) {
    // Map product Open/Pipeline/Closed to HPC signals
    const s = String(status).toLowerCase();
    if (s === "open") {
      parts.push(
        `AND({${HPC_FIELDS.futureOpening}}!=TRUE(),{${HPC_FIELDS.affiliationStatus}}!='Future / Pipeline',{${HPC_FIELDS.productionUse}}!='Do Not Use')`
      );
    } else if (s === "pipeline") {
      parts.push(
        `OR({${HPC_FIELDS.futureOpening}}=TRUE(),{${HPC_FIELDS.affiliationStatus}}='Future / Pipeline')`
      );
    }
  }
  if (search) {
    const q = String(search).replace(/'/g, "\\'");
    parts.push(
      `OR(FIND(LOWER('${q}'),LOWER({${HPC_FIELDS.name}})),FIND(LOWER('${q}'),LOWER({${HPC_FIELDS.city}})),FIND(LOWER('${q}'),LOWER({${HPC_FIELDS.country}})),FIND(LOWER('${q}'),LOWER({${HPC_FIELDS.dhl}})))`
    );
  }
  if (region) {
    const r = String(region).replace(/'/g, "\\'");
    parts.push(`{${HPC_FIELDS.country}}='${r}'`);
  }
  if (parts.length) selectOptions.filterByFormula = parts.length === 1 ? parts[0] : `AND(${parts.join(",")})`;
  void page;
  return selectOptions;
}
