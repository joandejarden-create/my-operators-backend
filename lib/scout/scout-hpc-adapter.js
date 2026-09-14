/**
 * P8.8 — Scout HPC census adapter (shadow / opt-in only).
 *
 * Maps Hotel Property Census → Scout census row shape.
 * No Legacy Hotel Census reads. No STR Number. No Chain Scale copy.
 * No Legacy Market/Submarket fact copy — Dealality geo shim only (nullable).
 * Bridge is identity-only when Legacy rec ids appear on request paths.
 */

import {
  createBrandPresenceBase,
  formatHpcHotelRecord,
  HPC_MAP_FIELDS,
  HPC_TABLE,
  isBrandPresenceHpcV2Enabled,
} from "../hotel-census/brand-presence-hpc-adapter.js";
import { CENSUS_FIELDS, CENSUS_INDEPENDENT_AFFILIATION } from "../hotel-census/fields.js";
import { exactMatchKey } from "../hotel-census/brand-alias-resolve.js";

export const SCOUT_HPC_ADAPTER = "scout-hpc-v2";
export const SCOUT_HPC_TABLE = HPC_TABLE;

/** @type {{ hpc: number, legacy: number, silentFallback: number }} */
export const scoutCensusReadCounters = {
  hpc: 0,
  legacy: 0,
  silentFallback: 0,
};

export function resetScoutCensusReadCounters() {
  scoutCensusReadCounters.hpc = 0;
  scoutCensusReadCounters.legacy = 0;
  scoutCensusReadCounters.silentFallback = 0;
}

export function snapshotScoutCensusReadCounters() {
  return { ...scoutCensusReadCounters, at: new Date().toISOString() };
}

/**
 * Affiliation Status → Scout affiliation semantics (never UNKNOWN=Independent).
 * @param {string|null} affiliationStatus
 * @param {string|null} brandDisplay
 * @param {string|null} brandFamily
 */
export function mapHpcAffiliationToScout(affiliationStatus, brandDisplay, brandFamily) {
  const status = exactMatchKey(affiliationStatus);
  const brand = exactMatchKey(brandDisplay);
  const family = exactMatchKey(brandFamily);

  if (status === "Independent") {
    return {
      affiliation: CENSUS_INDEPENDENT_AFFILIATION,
      affiliationClass: "INDEPENDENT_CONFIRMED",
      isIndependent: true,
      isBranded: false,
    };
  }
  if (status === "Brand-Unconfirmed") {
    return {
      affiliation: brand || "",
      affiliationClass: "BRAND_UNCONFIRMED",
      isIndependent: false,
      isBranded: false,
    };
  }
  if (!status || status === "Unknown") {
    return {
      affiliation: brand || "",
      affiliationClass: "AFFILIATION_UNKNOWN",
      isIndependent: false,
      isBranded: false,
    };
  }
  if (
    status === "Branded" ||
    status === "Soft-Branded / Collection" ||
    status === "Formerly Branded" ||
    status === "Future / Pipeline"
  ) {
    const aff = brand || family || "";
    return {
      affiliation: aff,
      affiliationClass: aff ? "BRANDED_CONFIRMED" : "AFFILIATION_UNKNOWN",
      isIndependent: false,
      isBranded: Boolean(aff),
    };
  }
  return {
    affiliation: brand || "",
    affiliationClass: "AFFILIATION_UNKNOWN",
    isIndependent: false,
    isBranded: false,
  };
}

/**
 * Apply Scout affiliation flags from optional Affiliation Status field on a mapped row.
 * Safe for Legacy rows (no status → infer from Affiliation string only; never invent Independent).
 */
export function applyScoutAffiliationFlags(row) {
  const status = exactMatchKey(row.affiliationStatus);
  if (status) {
    const mapped = mapHpcAffiliationToScout(status, row.affiliation, row.parentCompany);
    return {
      ...row,
      affiliation: mapped.affiliation || row.affiliation || "",
      affiliationClass: mapped.affiliationClass,
      isIndependent: mapped.isIndependent,
      isBranded: mapped.isBranded,
    };
  }
  const affiliation = exactMatchKey(row.affiliation);
  const isIndependent = affiliation === CENSUS_INDEPENDENT_AFFILIATION;
  const isBranded = Boolean(affiliation) && !isIndependent;
  return {
    ...row,
    affiliation: affiliation || "",
    affiliationClass: isIndependent
      ? "INDEPENDENT_CONFIRMED"
      : isBranded
        ? "BRANDED_CONFIRMED"
        : "AFFILIATION_UNKNOWN",
    isIndependent,
    isBranded,
  };
}

function escapeFormulaString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function buildHpcGeographyFormula(options = {}) {
  const parts = [];
  const country = String(options.country || "").trim();
  const city = String(options.city || "").trim();
  if (country) {
    parts.push(`{Country}='${escapeFormulaString(country)}'`);
  }
  if (city) {
    parts.push(`{City}='${escapeFormulaString(city)}'`);
  }
  // Do NOT filter Airtable by Legacy Market — Dealality market is nullable / in-memory only.
  if (!parts.length) return null;
  return parts.length === 1 ? parts[0] : `AND(${parts.join(",")})`;
}

/**
 * Format HPC Airtable record into Legacy-shaped fields so existing Scout mappers work,
 * plus dhl_ / affiliationStatus / roomsKnown for HPC semantics.
 */
export function formatHpcRecordForScout(hotel) {
  const dto = formatHpcHotelRecord(hotel);
  const aff = mapHpcAffiliationToScout(
    dto.affiliationStatus,
    dto.brand === "Unknown Brand" ? null : dto.brand,
    dto.parentCompany === "Unknown" ? null : dto.parentCompany
  );
  const dhl = dto.dhl_ || null;
  const roomsKnown = dto.rooms != null && Number.isFinite(Number(dto.rooms));
  const rooms = roomsKnown ? Number(dto.rooms) : null;

  const fields = {
    [CENSUS_FIELDS.name]: dto.name,
    [CENSUS_FIELDS.affiliation]: aff.affiliation,
    [CENSUS_FIELDS.parentCompany]:
      dto.parentCompany && dto.parentCompany !== "Unknown" ? dto.parentCompany : "",
    [CENSUS_FIELDS.status]: dto.status,
    [CENSUS_FIELDS.rooms]: rooms,
    [CENSUS_FIELDS.country]: dto.country && dto.country !== "Unknown Country" ? dto.country : "",
    [CENSUS_FIELDS.city]: dto.city && dto.city !== "Unknown City" ? dto.city : "",
    // Dealality geo shim (nullable) — not Legacy STR Market/Submarket
    [CENSUS_FIELDS.market]: dto.dealality_market || dto.market || "",
    [CENSUS_FIELDS.submarket]: dto.dealality_submarket || dto.submarket || "",
    [CENSUS_FIELDS.chainScale]: null,
    [CENSUS_FIELDS.location]: null,
    [CENSUS_FIELDS.operationType]: null,
    [CENSUS_FIELDS.managementCompany]: dto.managementCompany || "",
    [CENSUS_FIELDS.projectPhase]: dto.projectPhase || "",
    Latitude: dto.lat || null,
    Longitude: dto.lng || null,
    "Affiliation Status": dto.affiliationStatus || "",
    "Dealality Hotel ID": dhl,
    "State / Region": dto.region || dto.state || "",
    "Rooms Confidence": roomsKnown ? "product" : "unknown",
    "Open Date": dto.openDate || "",
  };

  return {
    id: dhl || `hpc_${hotel.id}`,
    hpcRecordId: hotel.id,
    dhl_: dhl,
    fields,
    _source: "hotel_property_census",
    _adapter: SCOUT_HPC_ADAPTER,
    _affiliationClass: aff.affiliationClass,
    _roomsKnown: roomsKnown,
    _provenanceFlags: {
      ...(dto._provenanceFlags || {}),
      scout_hpc: true,
      chainScale_unknown_safe: true,
      no_legacy_fallback: true,
      no_str_identity: true,
    },
  };
}

/**
 * Fetch HPC records for Scout (read-only). Never reads Legacy Hotel Census.
 * @param {{ country?: string, city?: string, market?: string, forceRefresh?: boolean, ttlMs?: number }} [options]
 */
export async function fetchScoutHpcRecords(options = {}) {
  if (!isBrandPresenceHpcV2Enabled()) {
    throw new Error("BRAND_PRESENCE_HPC_V2 must be enabled for Scout HPC path");
  }
  const base = createBrandPresenceBase();
  if (!base) {
    throw new Error("Platform base not configured for Scout HPC");
  }

  const selectOptions = {
    fields: [...HPC_MAP_FIELDS],
    pageSize: 100,
  };
  const formula = buildHpcGeographyFormula(options);
  const warnings = [];
  if (formula) {
    selectOptions.filterByFormula = formula;
  } else {
    warnings.push(
      "SCOUT_HPC_FULL_TABLE: no country filter — loading full Hotel Property Census (slow)."
    );
  }
  if (options.market) {
    warnings.push(
      "SCOUT_HPC_MARKET_FILTER: Dealality market is nullable; market filter applied in-memory only (SAFE_DEGRADATION)."
    );
  }

  const raw = await base(HPC_TABLE).select(selectOptions).all();
  scoutCensusReadCounters.hpc += 1;

  const records = raw.map(formatHpcRecordForScout);
  return {
    records,
    fieldsLoaded: Object.keys(records[0]?.fields || {}),
    fetchedAt: Date.now(),
    warnings,
    recordCount: records.length,
    source: {
      base: "AIRTABLE_BASE_ID_ALT",
      table: SCOUT_HPC_TABLE,
      adapter: SCOUT_HPC_ADAPTER,
      readOnly: true,
      writes: false,
      censusSource: "hpc",
      hotelSource: "Hotel Property Census",
      noLegacyReads: true,
      noStrJoins: true,
      chainScale: "UNKNOWN_SAFE",
      geo: "dealality_shim",
    },
  };
}
