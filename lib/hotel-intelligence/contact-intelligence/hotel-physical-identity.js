/**
 * Physical hotel identity for ownership/contact research.
 * Reuses Census join + resolveHotelIdentity — does not invent a new framework.
 *
 * Distinct statuses (never collapsed):
 * - input_valid
 * - hpc_record_loaded
 * - physical_identity_sufficiently_supported
 * - identity_ambiguous | identity_unresolved
 *
 * Loading an HPC record alone ≠ independent physical confirmation when
 * request hints conflict or census identity fields are insufficient.
 */

import { joinCensusContactByRecordId } from "./census-contact-join.js";
import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../map_hotel_intelligence_fields.js";
import { getPlatformBase } from "../../hotel-census/platform-base.js";
import {
  resolveHotelIdentity,
  MATCH_STATUS,
} from "../identity-resolve.js";
import { CI_FAILURE_CODE } from "./failure-codes.js";

export const HOTEL_PHYSICAL_IDENTITY_VERSION = "hotel-physical-identity-v1";

export const IDENTITY_STATUS = Object.freeze({
  INPUT_INVALID: "INPUT_INVALID",
  HPC_NOT_FOUND: "HPC_NOT_FOUND",
  HPC_LOADED: "HPC_LOADED",
  PHYSICAL_SUPPORTED: "PHYSICAL_IDENTITY_SUFFICIENTLY_SUPPORTED",
  AMBIGUOUS: "IDENTITY_AMBIGUOUS",
  UNRESOLVED: "IDENTITY_UNRESOLVED",
});

const AIRTABLE_ID_RE = /^rec[A-Za-z0-9]{14}$/;

const IDENTITY_LOAD_FIELDS = [
  MAP_CENSUS_FIELDS.propertyName,
  MAP_CENSUS_FIELDS.officialName,
  MAP_CENSUS_FIELDS.propertyIdentityKey,
  MAP_CENSUS_FIELDS.address,
  MAP_CENSUS_FIELDS.city,
  MAP_CENSUS_FIELDS.stateRegion,
  MAP_CENSUS_FIELDS.country,
  MAP_CENSUS_FIELDS.postalCode,
  MAP_CENSUS_FIELDS.latitude,
  MAP_CENSUS_FIELDS.longitude,
  MAP_CENSUS_FIELDS.website,
  MAP_CENSUS_FIELDS.phone,
  MAP_CENSUS_FIELDS.hbxHotelCode,
  MAP_CENSUS_FIELDS.identityConfidence,
  MAP_CENSUS_FIELDS.lastReviewedAt,
];

function blank(v) {
  return v == null || !String(v).trim();
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Load HPC record with geo fields for identity assessment.
 * Prefer fixture via opts.fixtureRecord / opts.loadRecord for offline tests.
 */
export async function loadHpcHotelByRecordId(airtableRecordId, opts = {}) {
  const id = String(airtableRecordId || "").trim();
  if (!AIRTABLE_ID_RE.test(id)) {
    return {
      ok: false,
      input_valid: false,
      hpc_record_loaded: false,
      code: CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE,
      detail: "airtable_record_id_shape_invalid",
      record: null,
    };
  }

  if (typeof opts.loadRecord === "function") {
    const rec = await opts.loadRecord(id);
    if (!rec) {
      return {
        ok: false,
        input_valid: true,
        hpc_record_loaded: false,
        code: CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE,
        detail: "hpc_record_not_found",
        record: null,
      };
    }
    return mapIdentityRecord(rec);
  }

  if (opts.fixtureRecord) {
    const rec = opts.fixtureRecord.id ? opts.fixtureRecord : { id, fields: opts.fixtureRecord };
    return mapIdentityRecord(rec);
  }

  // Wider field set than contact-only join
  const base = opts.base || getPlatformBase();
  if (!base) {
    // Fall back to contact join shape when platform unavailable (may lack address)
    const joined = await joinCensusContactByRecordId(id, opts);
    if (!joined.ok) {
      return {
        ok: false,
        input_valid: true,
        hpc_record_loaded: false,
        code: joined.code,
        detail: joined.detail,
        record: null,
      };
    }
    return {
      ok: true,
      input_valid: true,
      hpc_record_loaded: true,
      code: joined.code,
      record: {
        ...joined.record,
        address: null,
        state_region: null,
        postal_code: null,
        latitude: null,
        longitude: null,
      },
      detail: "contact_join_fallback_no_geo",
    };
  }

  try {
    const table = opts.tableName || MAP_HOTEL_PROPERTY_CENSUS.tableName;
    const rec = await base(table).find(id);
    return mapIdentityRecord(rec);
  } catch (err) {
    const msg = String(err?.message || err);
    const notFound = /NOT_FOUND|Could not find|404/i.test(msg);
    return {
      ok: false,
      input_valid: true,
      hpc_record_loaded: false,
      code: CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE,
      detail: notFound ? "hpc_record_not_found" : msg.slice(0, 200),
      record: null,
    };
  }
}

function mapIdentityRecord(rec) {
  const f = rec.fields || {};
  return {
    ok: true,
    input_valid: true,
    hpc_record_loaded: true,
    code: CI_FAILURE_CODE.CENSUS_JOIN_OK,
    record: {
      airtable_record_id: rec.id,
      property_name: f[MAP_CENSUS_FIELDS.propertyName] || null,
      official_name: f[MAP_CENSUS_FIELDS.officialName] || null,
      property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
      address: f[MAP_CENSUS_FIELDS.address] || null,
      city: f[MAP_CENSUS_FIELDS.city] || null,
      state_region: f[MAP_CENSUS_FIELDS.stateRegion] || null,
      country: f[MAP_CENSUS_FIELDS.country] || null,
      postal_code: f[MAP_CENSUS_FIELDS.postalCode] || null,
      latitude: f[MAP_CENSUS_FIELDS.latitude] ?? null,
      longitude: f[MAP_CENSUS_FIELDS.longitude] ?? null,
      website: f[MAP_CENSUS_FIELDS.website] || null,
      phone: f[MAP_CENSUS_FIELDS.phone] || null,
      hbx_hotel_code: f[MAP_CENSUS_FIELDS.hbxHotelCode] || null,
      identity_confidence: f[MAP_CENSUS_FIELDS.identityConfidence] || null,
      last_reviewed_at: f[MAP_CENSUS_FIELDS.lastReviewedAt] || null,
      verification_status: "CENSUS_CANDIDATE_NOT_AUTO_VERIFIED",
    },
  };
}

/**
 * Build authoritative hotel seed from HPC record. Body hints never replace census facts.
 */
export function hotelSeedFromHpcRecord(record, hints = {}) {
  if (!record) return null;
  const name = record.official_name || record.property_name;
  return {
    hotel_id: record.airtable_record_id,
    hotel_name: name,
    name,
    city: record.city,
    country: record.country,
    address: record.address,
    state_region: record.state_region,
    postal_code: record.postal_code,
    latitude: record.latitude,
    longitude: record.longitude,
    website: record.website,
    phone: record.phone,
    property_identity_key: record.property_identity_key,
    hbx_hotel_code: record.hbx_hotel_code,
    identity_confidence: record.identity_confidence,
    // Permitted hints only — never overwrite authoritative census fields
    language: hints.language || null,
    aliases: Array.isArray(hints.aliases) ? hints.aliases : [],
    hint_notes: {
      requested_name: hints.hotel_name || hints.name || null,
      requested_city: hints.city || null,
      requested_country: hints.country || null,
      requested_address: hints.address || null,
      substitution_rejected: true,
    },
    census_source: "Hotel Property Census",
    verification_status: record.verification_status,
  };
}

function detectHintConflicts(record, hints = {}) {
  const conflicts = [];
  if (!record) return conflicts;

  const hintName = hints.hotel_name || hints.name;
  if (!blank(hintName) && !blank(record.property_name || record.official_name)) {
    const a = norm(hintName);
    const b = norm(record.official_name || record.property_name);
    if (a && b && a !== b && !a.includes(b) && !b.includes(a)) {
      // Similar-name check: if tokens diverge substantially, conflict
      const aTok = new Set(a.split(" ").filter((t) => t.length > 2));
      const bTok = new Set(b.split(" ").filter((t) => t.length > 2));
      const overlap = [...aTok].filter((t) => bTok.has(t)).length;
      if (overlap === 0) {
        conflicts.push({
          field: "hotel_name",
          reason: "REQUEST_NAME_CONFLICTS_WITH_HPC",
          hint: hintName,
          census: record.official_name || record.property_name,
        });
      }
    }
  }

  if (!blank(hints.country) && !blank(record.country)) {
    if (norm(hints.country) !== norm(record.country)) {
      conflicts.push({
        field: "country",
        reason: "REQUEST_COUNTRY_CONFLICTS_WITH_HPC",
        hint: hints.country,
        census: record.country,
      });
    }
  }

  if (!blank(hints.city) && !blank(record.city)) {
    if (norm(hints.city) !== norm(record.city)) {
      conflicts.push({
        field: "city",
        reason: "REQUEST_CITY_CONFLICTS_WITH_HPC",
        hint: hints.city,
        census: record.city,
      });
    }
  }

  if (!blank(hints.address) && !blank(record.address)) {
    const ha = norm(hints.address);
    const ca = norm(record.address);
    if (ha && ca && !ha.includes(ca.slice(0, 12)) && !ca.includes(ha.slice(0, 12))) {
      conflicts.push({
        field: "address",
        reason: "REQUEST_ADDRESS_CONFLICTS_WITH_HPC",
        hint: hints.address,
        census: record.address,
      });
    }
  }

  // Attempt to substitute another hotel_id in body
  const requestedId = hints.requested_hotel_id || hints.hotel_id;
  if (requestedId && requestedId !== record.airtable_record_id) {
    conflicts.push({
      field: "hotel_id",
      reason: "REQUEST_ATTEMPTED_HOTEL_ID_SUBSTITUTION",
      hint: requestedId,
      census: record.airtable_record_id,
    });
  }

  return conflicts;
}

/**
 * Assess physical identity for a stable HPC ID + optional request hints.
 */
export async function assessHotelPhysicalIdentity({
  hotel_id,
  hints = {},
  deps = {},
} = {}) {
  const id = String(hotel_id || "").trim();
  const base = {
    version: HOTEL_PHYSICAL_IDENTITY_VERSION,
    input_valid: false,
    hpc_record_loaded: false,
    physical_identity_sufficiently_supported: false,
    identity_status: IDENTITY_STATUS.INPUT_INVALID,
    match_status: null,
    conflicts: [],
    hotel: null,
    hpc_record: null,
    matching_reasons: [],
    blocks_ownership_contact: true,
  };

  if (!AIRTABLE_ID_RE.test(id)) {
    return {
      ...base,
      detail: "airtable_record_id_shape_invalid",
      identity_status: IDENTITY_STATUS.INPUT_INVALID,
    };
  }

  const loaded = await loadHpcHotelByRecordId(id, {
    loadRecord: deps.loadRecord,
    fixtureRecord: deps.fixtureRecord,
    base: deps.base,
  });

  if (!loaded.input_valid) {
    return { ...base, detail: loaded.detail, identity_status: IDENTITY_STATUS.INPUT_INVALID };
  }

  if (!loaded.hpc_record_loaded || !loaded.record) {
    return {
      ...base,
      input_valid: true,
      hpc_record_loaded: false,
      detail: loaded.detail || "hpc_record_not_found",
      identity_status: IDENTITY_STATUS.HPC_NOT_FOUND,
    };
  }

  const record = loaded.record;
  const conflicts = detectHintConflicts(record, hints);
  const hotel = hotelSeedFromHpcRecord(record, hints);

  // Census pool of one — reuse resolveHotelIdentity for hint-vs-record scoring
  const censusPool = [
    {
      id: record.airtable_record_id,
      fields: {
        [MAP_CENSUS_FIELDS.propertyName]: record.property_name,
        [MAP_CENSUS_FIELDS.officialName]: record.official_name,
        [MAP_CENSUS_FIELDS.address]: record.address,
        [MAP_CENSUS_FIELDS.city]: record.city,
        [MAP_CENSUS_FIELDS.country]: record.country,
        [MAP_CENSUS_FIELDS.latitude]: record.latitude,
        [MAP_CENSUS_FIELDS.longitude]: record.longitude,
        [MAP_CENSUS_FIELDS.website]: record.website,
        [MAP_CENSUS_FIELDS.phone]: record.phone,
        [MAP_CENSUS_FIELDS.hbxHotelCode]: record.hbx_hotel_code,
      },
    },
  ];

  const hintInput = {
    name: hints.hotel_name || hints.name || hotel.hotel_name,
    address: hints.address || hotel.address,
    city: hints.city || hotel.city,
    country: hints.country || hotel.country,
    latitude: hints.latitude ?? hotel.latitude,
    longitude: hints.longitude ?? hotel.longitude,
    website: hints.website || hotel.website,
    phone: hints.phone || hotel.phone,
  };

  let match = null;
  let matcherException = null;
  const resolveFn =
    typeof deps.resolveHotelIdentity === "function" ? deps.resolveHotelIdentity : resolveHotelIdentity;
  try {
    match = resolveFn(hintInput, censusPool, { idRegistry: deps.idRegistry });
  } catch (err) {
    match = null;
    matcherException = {
      blocked: true,
      reason: "MATCHER_EXCEPTION",
      detail: String(err?.message || err).slice(0, 160),
    };
  }

  // Matcher exceptions explicitly block physical support (never default to EXACT / null-ok)
  if (matcherException) {
    return {
      ...base,
      input_valid: true,
      hpc_record_loaded: true,
      hpc_record_bound: true,
      hotel,
      hpc_record: record,
      conflicts,
      physical_identity_sufficiently_supported: false,
      identity_status: IDENTITY_STATUS.UNRESOLVED,
      match_status: null,
      matcher_exception: matcherException,
      blocks_ownership_contact: true,
      detail: "matcher_exception_blocks_physical_support",
      note: `Matcher exception blocked physical support: ${matcherException.detail}`,
    };
  }

  // Supported physical identity requires more than "record loaded + no hint conflict"
  // Direct ID bind ≠ physical confirmation. Use match quality + sufficient census fields.
  const matchStatus = match?.match_status || null;
  const matchOkForPhysical =
    matchStatus === MATCH_STATUS.EXACT ||
    matchStatus === MATCH_STATUS.STRONG ||
    // Direct record-id bind with no conflicting hints and no matcher run failure:
    // only when hints are absent/identical AND census has city or address (not name+country alone)
    (matchStatus == null && !conflicts.length && Boolean(record.city || record.address));

  const matchBlocksPhysical =
    matchStatus === MATCH_STATUS.AMBIGUOUS ||
    matchStatus === MATCH_STATUS.INSUFFICIENT ||
    matchStatus === MATCH_STATUS.NEW ||
    matchStatus === MATCH_STATUS.PROBABLE;

  const hasName = !blank(hotel.hotel_name);
  const hasCountry = !blank(hotel.country);
  const hasCityOrAddress = !blank(hotel.city) || !blank(hotel.address);
  const censusSufficientForPhysical = hasName && hasCountry && hasCityOrAddress;
  const hardConflict = conflicts.some((c) =>
    /COUNTRY|ADDRESS|HOTEL_ID_SUBSTITUTION|NAME_CONFLICTS/i.test(c.reason)
  );

  const recordBound = {
    ...base,
    input_valid: true,
    hpc_record_loaded: true,
    hpc_record_bound: true,
    hotel,
    hpc_record: record,
    conflicts,
    matching_reasons: match?.matching_reasons || [],
    match_status: matchStatus,
  };

  if (hardConflict || conflicts.length > 0) {
    return {
      ...recordBound,
      physical_identity_sufficiently_supported: false,
      identity_status: IDENTITY_STATUS.AMBIGUOUS,
      blocks_ownership_contact: true,
      detail: "request_hints_conflict_with_hpc",
      note: "HPC bound; physical identity not supported due to hint conflicts",
    };
  }

  if (matchBlocksPhysical || !matchOkForPhysical) {
    return {
      ...recordBound,
      physical_identity_sufficiently_supported: false,
      identity_status:
        matchStatus === MATCH_STATUS.AMBIGUOUS
          ? IDENTITY_STATUS.AMBIGUOUS
          : IDENTITY_STATUS.UNRESOLVED,
      blocks_ownership_contact: true,
      detail: "matcher_did_not_confirm_physical_identity",
      note: "HPC record bound by ID; matcher did not establish physical support (no EXACT default)",
    };
  }

  if (!censusSufficientForPhysical) {
    return {
      ...recordBound,
      physical_identity_sufficiently_supported: false,
      identity_status: IDENTITY_STATUS.UNRESOLVED,
      blocks_ownership_contact: true,
      detail: "hpc_record_insufficient_physical_fields",
      note: "Record bound but name+country+city/address required for physical support",
    };
  }

  return {
    ...recordBound,
    version: HOTEL_PHYSICAL_IDENTITY_VERSION,
    physical_identity_sufficiently_supported: true,
    identity_status: IDENTITY_STATUS.PHYSICAL_SUPPORTED,
    // Never invent EXACT — report actual matcher status (may be null when hints empty)
    match_status: matchStatus,
    matching_reasons: [
      "hpc_direct_find_by_record_id",
      ...(match?.matching_reasons || []),
      matchStatus ? `matcher:${matchStatus}` : "matcher_not_required_identical_or_absent_hints",
    ],
    blocks_ownership_contact: false,
    detail: null,
    note: "HPC bound and physical fields sufficient with non-blocking matcher status. Not a field survey.",
  };
}

export { IDENTITY_LOAD_FIELDS, AIRTABLE_ID_RE };
