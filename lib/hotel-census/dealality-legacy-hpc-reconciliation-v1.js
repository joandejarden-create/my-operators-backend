/**
 * Full-table legacy Hotel Census → HPC reconciliation (read-only).
 *
 * Dispositions (founder Phase 2):
 * MATCHED_HPC_VERIFIED | MATCHED_HPC_ALIAS | HPC_CREATE_CANDIDATE |
 * AMBIGUOUS_MATCH | DUPLICATE_LEGACY_RECORD | HISTORICAL_OR_INACTIVE |
 * REQUIRES_EVIDENCE | NOT_APPLICABLE
 */

import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
} from "../hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import {
  resolveDealalityHotelIdentity,
  RESOLVER_MATCH,
} from "./dealality-hpc-identity-resolver-v1.js";
import { LEGACY_CENSUS_FIELDS } from "./fields.js";
import { HOTEL_PROPERTY_CENSUS_TABLE_ID } from "./platform-base.js";

export const DEALALITY_LEGACY_HPC_RECONCILIATION_V1 = "DEALALITY_LEGACY_HPC_RECONCILIATION_V1";

export const RECON_DISPOSITION = Object.freeze({
  MATCHED_HPC_VERIFIED: "MATCHED_HPC_VERIFIED",
  MATCHED_HPC_ALIAS: "MATCHED_HPC_ALIAS",
  HPC_CREATE_CANDIDATE: "HPC_CREATE_CANDIDATE",
  AMBIGUOUS_MATCH: "AMBIGUOUS_MATCH",
  DUPLICATE_LEGACY_RECORD: "DUPLICATE_LEGACY_RECORD",
  HISTORICAL_OR_INACTIVE: "HISTORICAL_OR_INACTIVE",
  REQUIRES_EVIDENCE: "REQUIRES_EVIDENCE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

function firstText(fields, keys) {
  for (const k of keys) {
    const v = fields?.[k];
    if (v == null || v === "") continue;
    if (typeof v === "string" && v.trim()) return v.trim();
    if (Array.isArray(v) && v.length) {
      const t = v.map((x) => (typeof x === "string" ? x : x?.name || "")).filter(Boolean).join("; ");
      if (t) return t;
    }
  }
  return null;
}

function firstNumber(fields, keys) {
  for (const k of keys) {
    const n = Number(fields?.[k]);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function normalizeStatus(raw) {
  const s = String(raw || "").trim().toLowerCase();
  if (!s) return null;
  if (s === "open") return "Open";
  if (s === "pipeline") return "Pipeline";
  if (/closed|inactive|historical|former|demolished|cancelled|canceled/.test(s)) {
    return "HISTORICAL_OR_INACTIVE";
  }
  return String(raw).trim();
}

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

/**
 * Page all legacy Hotel Census records (migration-read only).
 */
export async function fetchAllLegacyHotelCensusRecords(opts = {}) {
  const token = airtableToken();
  const baseId = platformBaseId();
  if (!token || !baseId) {
    const err = new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
    err.code = "HPC_CONFIG_MISSING";
    throw err;
  }

  assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
    method: "GET",
    workflow: opts.workflow || "legacy-hpc-reconciliation",
    allowLegacyHotelCensusRead: true,
  });

  const maxRecords =
    opts.maxRecords == null || opts.maxRecords === ""
      ? null
      : Number.isFinite(Number(opts.maxRecords))
        ? Number(opts.maxRecords)
        : null;
  const pageSize = Math.min(100, Number(opts.pageSize) || 100);
  const fields = opts.fields || [
    LEGACY_CENSUS_FIELDS.name,
    LEGACY_CENSUS_FIELDS.affiliation,
    LEGACY_CENSUS_FIELDS.parentCompany,
    LEGACY_CENSUS_FIELDS.status,
    LEGACY_CENSUS_FIELDS.rooms,
    LEGACY_CENSUS_FIELDS.country,
    LEGACY_CENSUS_FIELDS.city,
    LEGACY_CENSUS_FIELDS.market,
    LEGACY_CENSUS_FIELDS.submarket,
    LEGACY_CENSUS_FIELDS.managementCompany,
  ];

  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams();
    params.set("pageSize", String(pageSize));
    for (const f of fields) params.append("fields[]", f);
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
      LEGACY_HOTEL_CENSUS_TABLE_NAME
    )}?${params}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      const err = new Error(`Airtable ${res.status}: ${json?.error?.message || "legacy census read failed"}`);
      err.code = "LEGACY_CENSUS_READ_FAILED";
      throw err;
    }
    for (const rec of json.records || []) {
      out.push(rec);
      if (maxRecords != null && out.length >= maxRecords) {
        return out;
      }
    }
    offset = json.offset || null;
    if (opts.onPage) {
      await opts.onPage({ fetched: out.length, offset });
    }
  } while (offset);

  return out;
}

/**
 * Classify one legacy record against HPC using shared resolver (no name-only match).
 */
export async function classifyLegacyCensusRecord(record, opts = {}) {
  const f = record?.fields || {};
  const name = firstText(f, [LEGACY_CENSUS_FIELDS.name, "name", "Name"]);
  const brand = firstText(f, [LEGACY_CENSUS_FIELDS.affiliation, "Affiliation", "Current Brand"]);
  const country = firstText(f, [LEGACY_CENSUS_FIELDS.country, "country", "Country"]);
  const city = firstText(f, [LEGACY_CENSUS_FIELDS.city, "city", "City"]);
  const statusRaw = firstText(f, [LEGACY_CENSUS_FIELDS.status, "status", "Radar Status"]);
  const status = normalizeStatus(statusRaw);
  const rooms = firstNumber(f, [LEGACY_CENSUS_FIELDS.rooms, "rooms", "Rooms / Keys"]);

  if (!name) {
    return {
      legacyId: record.id,
      disposition: RECON_DISPOSITION.NOT_APPLICABLE,
      canonicalHpcId: null,
      reason: "missing_property_name",
      evidence: { status: statusRaw },
    };
  }

  if (status === "HISTORICAL_OR_INACTIVE") {
    return {
      legacyId: record.id,
      disposition: RECON_DISPOSITION.HISTORICAL_OR_INACTIVE,
      canonicalHpcId: null,
      reason: "inactive_or_historical_status",
      propertyName: name,
      evidence: { status: statusRaw, brand, country, city },
    };
  }

  // Never pass legacy Census record ids as hotelKey — resolveCanonicalHotelId
  // identity-passthrough would false-match every legacy id as EXACT_HPC_ID.
  // Match only via live name/geo/brand collision (+ optional non-id aliases).
  const resolved = await resolveDealalityHotelIdentity({
    hotelName: name,
    aliases: opts.aliases || [],
    city,
    country,
    brand,
    liveLookup: opts.liveLookup !== false,
    requireHpcRecord: true,
  });

  if (
    resolved.matchStatus === RESOLVER_MATCH.EXACT_HPC_ID ||
    resolved.matchStatus === RESOLVER_MATCH.LIVE_COLLISION_VERIFIED
  ) {
    return {
      legacyId: record.id,
      disposition: RECON_DISPOSITION.MATCHED_HPC_VERIFIED,
      canonicalHpcId: resolved.canonicalHotelId,
      propertyName: name,
      reason: resolved.matchStatus,
      evidence: {
        brand,
        country,
        city,
        rooms,
        status: statusRaw,
        resolver: resolved.matchStatus,
        confidence: resolved.confidence,
      },
    };
  }

  if (resolved.matchStatus === RESOLVER_MATCH.ALIAS_MAP && resolved.canonicalHotelId) {
    return {
      legacyId: record.id,
      disposition: RECON_DISPOSITION.MATCHED_HPC_ALIAS,
      canonicalHpcId: resolved.canonicalHotelId,
      propertyName: name,
      reason: "alias_map",
      evidence: {
        brand,
        country,
        city,
        rooms,
        status: statusRaw,
        confidence: resolved.confidence,
      },
    };
  }

  if (resolved.matchStatus === RESOLVER_MATCH.LIVE_COLLISION_AMBIGUOUS) {
    return {
      legacyId: record.id,
      disposition: RECON_DISPOSITION.AMBIGUOUS_MATCH,
      canonicalHpcId: null,
      propertyName: name,
      reason: "ambiguous_collision",
      candidates: (resolved.candidates || []).slice(0, 5),
      evidence: { brand, country, city, status: statusRaw },
    };
  }

  if (resolved.matchStatus === RESOLVER_MATCH.PROPOSED_NEW) {
    return {
      legacyId: record.id,
      disposition: RECON_DISPOSITION.HPC_CREATE_CANDIDATE,
      canonicalHpcId: null,
      propertyName: name,
      reason: "verified_absent_in_hpc",
      proposeCreate: {
        "Property Name": name,
        "Current Brand": brand,
        Country: country,
        City: city,
        "Rooms / Keys": rooms,
        "Radar Status": status === "Open" || status === "Pipeline" ? status : null,
        _legacyProvenanceId: record.id,
      },
      evidence: { brand, country, city, rooms, status: statusRaw },
    };
  }

  return {
    legacyId: record.id,
    disposition: RECON_DISPOSITION.REQUIRES_EVIDENCE,
    canonicalHpcId: null,
    propertyName: name,
    reason: resolved.matchStatus || "unresolved",
    evidence: { brand, country, city, rooms, status: statusRaw, resolver: resolved },
  };
}

/**
 * Mark duplicate legacy rows that share the same canonical HPC id.
 */
export function markDuplicateLegacyMatches(rows = []) {
  const byHpc = new Map();
  for (const row of rows) {
    if (!row.canonicalHpcId) continue;
    if (!byHpc.has(row.canonicalHpcId)) byHpc.set(row.canonicalHpcId, []);
    byHpc.get(row.canonicalHpcId).push(row);
  }
  const out = [];
  for (const row of rows) {
    const peers = row.canonicalHpcId ? byHpc.get(row.canonicalHpcId) || [] : [];
    if (peers.length > 1) {
      out.push({
        ...row,
        disposition:
          row.disposition === RECON_DISPOSITION.MATCHED_HPC_VERIFIED ||
          row.disposition === RECON_DISPOSITION.MATCHED_HPC_ALIAS
            ? RECON_DISPOSITION.DUPLICATE_LEGACY_RECORD
            : row.disposition,
        duplicateLegacyIds: peers.map((p) => p.legacyId).filter((id) => id !== row.legacyId),
      });
    } else {
      out.push(row);
    }
  }
  return out;
}

export function summarizeReconciliation(rows = []) {
  const byDisposition = {};
  for (const row of rows) {
    const d = row.disposition || "UNKNOWN";
    byDisposition[d] = (byDisposition[d] || 0) + 1;
  }
  return {
    contract: DEALALITY_LEGACY_HPC_RECONCILIATION_V1,
    legacyTableId: LEGACY_HOTEL_CENSUS_TABLE_ID,
    hpcTableId: HOTEL_PROPERTY_CENSUS_TABLE_ID,
    total: rows.length,
    byDisposition,
    matched:
      (byDisposition[RECON_DISPOSITION.MATCHED_HPC_VERIFIED] || 0) +
      (byDisposition[RECON_DISPOSITION.MATCHED_HPC_ALIAS] || 0),
    createCandidates: byDisposition[RECON_DISPOSITION.HPC_CREATE_CANDIDATE] || 0,
    ambiguous: byDisposition[RECON_DISPOSITION.AMBIGUOUS_MATCH] || 0,
    unresolved:
      (byDisposition[RECON_DISPOSITION.REQUIRES_EVIDENCE] || 0) +
      (byDisposition[RECON_DISPOSITION.AMBIGUOUS_MATCH] || 0),
  };
}
