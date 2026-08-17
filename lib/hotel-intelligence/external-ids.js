/**
 * External identifier model: hotel_id ↔ provider ↔ external_id.
 */

import { createLocalStore } from "./local-store.js";
import { generateDealalityHotelId, isDealalityHotelId } from "./canonical-hotel.js";
import { MAP_PROVIDER_IDS } from "./map_hotel_intelligence_fields.js";

export const EXTERNAL_IDS_VERSION = "hotel-intelligence-external-ids-v1";

function externalKey(provider, externalId) {
  return `${String(provider || "").trim().toLowerCase()}::${String(externalId || "").trim()}`;
}

/**
 * @param {ReturnType<typeof createLocalStore>} [store]
 */
export function createExternalIdRegistry(store = createLocalStore()) {
  function load() {
    return store.readHotelIdMap();
  }

  function save(map) {
    store.writeHotelIdMap(map);
  }

  /**
   * Ensure a hotel_id exists for an Airtable census record (or create mapping).
   */
  function ensureHotelIdForAirtable(airtableRecordId, extras = {}) {
    const recId = String(airtableRecordId || "").trim();
    if (!recId) throw new Error("airtable_record_id_required");
    const map = load();
    const existing = map.by_airtable_id[recId];
    if (existing && isDealalityHotelId(existing)) {
      const row = map.by_hotel_id[existing] || {};
      map.by_hotel_id[existing] = {
        ...row,
        hotel_id: existing,
        airtable_record_id: recId,
        property_identity_key:
          extras.property_identity_key ?? row.property_identity_key ?? null,
        updated_at: new Date().toISOString(),
      };
      save(map);
      return existing;
    }
    const hotelId = generateDealalityHotelId();
    const now = new Date().toISOString();
    map.by_hotel_id[hotelId] = {
      hotel_id: hotelId,
      airtable_record_id: recId,
      property_identity_key: extras.property_identity_key || null,
      external_ids: [],
      first_seen_at: now,
      updated_at: now,
    };
    map.by_airtable_id[recId] = hotelId;
    save(map);
    return hotelId;
  }

  function getByHotelId(hotelId) {
    const map = load();
    return map.by_hotel_id[String(hotelId || "").trim()] || null;
  }

  function getByAirtableId(airtableRecordId) {
    const map = load();
    const hotelId = map.by_airtable_id[String(airtableRecordId || "").trim()];
    return hotelId ? map.by_hotel_id[hotelId] || null : null;
  }

  function findByExternalId(provider, externalId) {
    const map = load();
    const key = externalKey(provider, externalId);
    const hotelId = map.by_external[key];
    return hotelId ? map.by_hotel_id[hotelId] || null : null;
  }

  /**
   * Link an external provider ID to a hotel_id.
   */
  function linkExternalId(hotelId, provider, externalId, opts = {}) {
    const hid = String(hotelId || "").trim();
    const prov = String(provider || "").trim().toLowerCase();
    const ext = String(externalId || "").trim();
    if (!isDealalityHotelId(hid)) throw new Error("invalid_hotel_id");
    if (!prov || !ext) throw new Error("provider_and_external_id_required");

    const map = load();
    const row = map.by_hotel_id[hid] || {
      hotel_id: hid,
      airtable_record_id: null,
      property_identity_key: null,
      external_ids: [],
      first_seen_at: new Date().toISOString(),
    };
    const now = new Date().toISOString();
    const existingIdx = (row.external_ids || []).findIndex(
      (e) => e.provider === prov && e.external_id === ext
    );
    const entry = {
      hotel_id: hid,
      provider: prov,
      external_id: ext,
      external_url: opts.external_url || null,
      first_seen_at:
        existingIdx >= 0 ? row.external_ids[existingIdx].first_seen_at : now,
      last_verified_at: now,
      is_current: opts.is_current !== false,
    };
    if (existingIdx >= 0) row.external_ids[existingIdx] = entry;
    else row.external_ids = [...(row.external_ids || []), entry];
    row.updated_at = now;
    map.by_hotel_id[hid] = row;
    map.by_external[externalKey(prov, ext)] = hid;
    if (row.airtable_record_id) {
      map.by_airtable_id[row.airtable_record_id] = hid;
    }
    save(map);
    return entry;
  }

  /**
   * Create a staged (not yet in census) hotel_id mapping.
   */
  function createStagedHotelId(extras = {}) {
    const hotelId = generateDealalityHotelId();
    const map = load();
    const now = new Date().toISOString();
    map.by_hotel_id[hotelId] = {
      hotel_id: hotelId,
      airtable_record_id: null,
      property_identity_key: extras.property_identity_key || null,
      external_ids: [],
      staged: true,
      first_seen_at: now,
      updated_at: now,
    };
    save(map);
    return hotelId;
  }

  return {
    version: EXTERNAL_IDS_VERSION,
    providers: MAP_PROVIDER_IDS,
    ensureHotelIdForAirtable,
    getByHotelId,
    getByAirtableId,
    findByExternalId,
    linkExternalId,
    createStagedHotelId,
  };
}
