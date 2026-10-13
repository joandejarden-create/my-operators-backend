/**
 * Contact Intelligence V1.2 — Census contact join by Airtable record ID.
 * Read-only. Does not upgrade Census values to officially verified.
 */

import "dotenv/config";
import { getPlatformBase } from "../../hotel-census/platform-base.js";
import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../map_hotel_intelligence_fields.js";
import { CI_FAILURE_CODE } from "./failure-codes.js";

const CONTACT_FIELDS = [
  MAP_CENSUS_FIELDS.propertyName,
  MAP_CENSUS_FIELDS.officialName,
  MAP_CENSUS_FIELDS.propertyIdentityKey,
  MAP_CENSUS_FIELDS.website,
  MAP_CENSUS_FIELDS.phone,
  MAP_CENSUS_FIELDS.city,
  MAP_CENSUS_FIELDS.country,
  MAP_CENSUS_FIELDS.hbxHotelCode,
  MAP_CENSUS_FIELDS.lastReviewedAt,
  MAP_CENSUS_FIELDS.identityConfidence,
];

/**
 * Direct find by record ID — no name join.
 * @param {string} airtableRecordId
 */
export async function joinCensusContactByRecordId(airtableRecordId, opts = {}) {
  const id = String(airtableRecordId || "").trim();
  if (!/^rec[A-Za-z0-9]{14}$/.test(id)) {
    return {
      ok: false,
      code: CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE,
      detail: "airtable_record_id_shape_invalid",
      record: null,
    };
  }

  if (opts.fixtureRecord) {
    return mapRecord(opts.fixtureRecord);
  }

  const base = opts.base || getPlatformBase();
  if (!base) {
    return {
      ok: false,
      code: CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE,
      detail: "platform_base_unavailable_AIRTABLE_BASE_ID_ALT_or_token",
      record: null,
    };
  }

  try {
    const table = opts.tableName || MAP_HOTEL_PROPERTY_CENSUS.tableName;
    const rec = await base(table).find(id);
    return mapRecord(rec);
  } catch (err) {
    const msg = String(err?.message || err);
    return {
      ok: false,
      code: CI_FAILURE_CODE.IDENTITY_JOIN_FAILURE,
      detail: msg.slice(0, 200),
      record: null,
    };
  }
}

function mapRecord(rec) {
  const f = rec.fields || {};
  const phone = f[MAP_CENSUS_FIELDS.phone] || null;
  const website = f[MAP_CENSUS_FIELDS.website] || null;
  return {
    ok: true,
    code: CI_FAILURE_CODE.CENSUS_JOIN_OK,
    record: {
      airtable_record_id: rec.id,
      property_name: f[MAP_CENSUS_FIELDS.propertyName] || null,
      official_name: f[MAP_CENSUS_FIELDS.officialName] || null,
      property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
      city: f[MAP_CENSUS_FIELDS.city] || null,
      country: f[MAP_CENSUS_FIELDS.country] || null,
      hbx_hotel_code: f[MAP_CENSUS_FIELDS.hbxHotelCode] || null,
      website,
      phone,
      last_reviewed_at: f[MAP_CENSUS_FIELDS.lastReviewedAt] || null,
      identity_confidence: f[MAP_CENSUS_FIELDS.identityConfidence] || null,
      // Never auto-upgrade:
      verification_status: "CENSUS_CANDIDATE_NOT_AUTO_VERIFIED",
      email: null, // no email field on Census map
    },
    fields_present: {
      phone: Boolean(phone),
      website: Boolean(website),
      email: false,
    },
  };
}

export { CONTACT_FIELDS, MAP_CENSUS_FIELDS };
