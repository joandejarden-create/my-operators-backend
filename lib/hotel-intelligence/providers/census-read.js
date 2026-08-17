/**
 * Dealality Hotel Property Census as a read provider.
 * Never writes. Uses central field map.
 */

import { getPlatformBase } from "../../hotel-census/platform-base.js";
import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
  MAP_PROVIDER_IDS,
} from "../map_hotel_intelligence_fields.js";
import {
  createEmptyCanonicalHotel,
  toMvpHotelSummary,
} from "../canonical-hotel.js";
import { createExternalIdRegistry } from "../external-ids.js";
import { emptyCandidate, providerStatus } from "./types.js";
import {
  normalizeKey,
  nameSimilarity,
  citiesMatch,
  countriesMatch,
  parseCoords,
  distanceMeters,
} from "../../independent-census/match-current-census.js";

export const CENSUS_READ_PROVIDER_VERSION = "census-read-provider-v1";
export const PROVIDER_ID = MAP_PROVIDER_IDS.census;

const READ_FIELDS = Object.values(MAP_CENSUS_FIELDS).filter((name) => {
  // Fields mapped historically but absent on live Hotel Property Census.
  if (!name) return false;
  if (name === MAP_CENSUS_FIELDS.postalCode) return false;
  if (name === MAP_CENSUS_FIELDS.chainScale) return false;
  return true;
});

function blank(v) {
  return v == null || !String(v).trim();
}

/**
 * Map Airtable fields → NormalizedHotelCandidate
 */
export function normalizeCensusFields(fields, recordId) {
  const f = fields || {};
  const lat = f[MAP_CENSUS_FIELDS.latitude];
  const lng = f[MAP_CENSUS_FIELDS.longitude];
  const coords = parseCoords(lat, lng);
  return emptyCandidate(PROVIDER_ID, {
    external_id: recordId || null,
    name:
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      null,
    address: f[MAP_CENSUS_FIELDS.address] || null,
    city: f[MAP_CENSUS_FIELDS.city] || null,
    country: f[MAP_CENSUS_FIELDS.country] || null,
    country_code: null,
    latitude: coords?.lat ?? null,
    longitude: coords?.lng ?? null,
    room_count:
      f[MAP_CENSUS_FIELDS.roomCount] != null
        ? Number(f[MAP_CENSUS_FIELDS.roomCount])
        : null,
    brand_name: f[MAP_CENSUS_FIELDS.brandName] || null,
    parent_company_name: f[MAP_CENSUS_FIELDS.parentCompanyName] || null,
    website: f[MAP_CENSUS_FIELDS.website] || null,
    phone: f[MAP_CENSUS_FIELDS.phone] || null,
    status: f[MAP_CENSUS_FIELDS.status] || null,
    raw_safe: {
      airtable_record_id: recordId || null,
      property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
      hbx_hotel_code: f[MAP_CENSUS_FIELDS.hbxHotelCode] || null,
      market: f[MAP_CENSUS_FIELDS.market] || null,
      submarket: f[MAP_CENSUS_FIELDS.submarket] || null,
      chain_scale: f[MAP_CENSUS_FIELDS.chainScale] || null,
    },
  });
}

/**
 * Build canonical hotel from census record + hotel_id mapping.
 */
export function censusRecordToCanonical(record, hotelId) {
  const f = record.fields || {};
  const coords = parseCoords(
    f[MAP_CENSUS_FIELDS.latitude],
    f[MAP_CENSUS_FIELDS.longitude]
  );
  const rooms = f[MAP_CENSUS_FIELDS.roomCount];
  return createEmptyCanonicalHotel({
    hotel_id: hotelId,
    identity: {
      official_name:
        f[MAP_CENSUS_FIELDS.officialName] ||
        f[MAP_CENSUS_FIELDS.propertyName] ||
        null,
      display_name:
        f[MAP_CENSUS_FIELDS.propertyName] ||
        f[MAP_CENSUS_FIELDS.officialName] ||
        null,
    },
    location: {
      address_line_1: f[MAP_CENSUS_FIELDS.address] || null,
      city: f[MAP_CENSUS_FIELDS.city] || null,
      state_region: f[MAP_CENSUS_FIELDS.stateRegion] || null,
      postal_code: f[MAP_CENSUS_FIELDS.postalCode] || null,
      country: f[MAP_CENSUS_FIELDS.country] || null,
      latitude: coords?.lat ?? null,
      longitude: coords?.lng ?? null,
      market: f[MAP_CENSUS_FIELDS.market] || null,
      submarket: f[MAP_CENSUS_FIELDS.submarket] || null,
    },
    property: {
      room_count: rooms != null && Number.isFinite(Number(rooms)) ? Number(rooms) : null,
      property_type: f[MAP_CENSUS_FIELDS.propertyType] || null,
      chain_scale: f[MAP_CENSUS_FIELDS.chainScale] || null,
      status: f[MAP_CENSUS_FIELDS.status] || null,
    },
    brand: {
      brand_name: f[MAP_CENSUS_FIELDS.brandName] || null,
      parent_company_name: f[MAP_CENSUS_FIELDS.parentCompanyName] || null,
      independent:
        String(f[MAP_CENSUS_FIELDS.affiliationStatus] || "")
          .toLowerCase()
          .includes("independent") || null,
    },
    digital: {
      website: f[MAP_CENSUS_FIELDS.website] || null,
      phone: f[MAP_CENSUS_FIELDS.phone] || null,
    },
    verification: {
      record_confidence: f[MAP_CENSUS_FIELDS.identityConfidence] || null,
      last_verified_at: f[MAP_CENSUS_FIELDS.lastReviewedAt] || null,
      review_status: f[MAP_CENSUS_FIELDS.reviewStatus] || null,
    },
    linkages: {
      airtable_record_id: record.id,
      property_identity_key: f[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
      external_ids: f[MAP_CENSUS_FIELDS.hbxHotelCode]
        ? [
            {
              provider: "hotelbeds",
              external_id: String(f[MAP_CENSUS_FIELDS.hbxHotelCode]),
              is_current: true,
            },
          ]
        : [],
    },
  });
}

/**
 * @param {object} [opts]
 * @param {object[]} [opts.records] — inject records for tests (skip Airtable)
 */
export function createCensusReadProvider(opts = {}) {
  const idRegistry = opts.idRegistry || createExternalIdRegistry(opts.store);
  let cache = Array.isArray(opts.records) ? opts.records : null;
  let cacheAt = 0;
  const cacheTtlMs = Number(opts.cacheTtlMs || 5 * 60 * 1000);

  async function loadRecords(force = false) {
    if (Array.isArray(opts.records)) return opts.records;
    if (cache && !force && Date.now() - cacheAt < cacheTtlMs) return cache;
    const base = opts.base || getPlatformBase();
    if (!base) {
      throw new Error("platform_base_unavailable");
    }
    const table = opts.tableName || MAP_HOTEL_PROPERTY_CENSUS.tableName;
    const records = await base(table)
      .select({
        fields: READ_FIELDS.filter(Boolean),
        pageSize: 100,
      })
      .all();
    cache = records;
    cacheAt = Date.now();
    return records;
  }

  async function getAvailabilityStatus() {
    try {
      if (Array.isArray(opts.records)) {
        return providerStatus(PROVIDER_ID, "ok", {
          retryable: false,
          message: `fixture_records=${opts.records.length}`,
        });
      }
      const base = opts.base || getPlatformBase();
      if (!base) {
        return providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
          message: "AIRTABLE_BASE_ID_ALT / token missing",
        });
      }
      return providerStatus(PROVIDER_ID, "ok", { retryable: false });
    } catch (err) {
      return providerStatus(PROVIDER_ID, "unavailable", {
        retryable: true,
        message: String(err?.message || err).slice(0, 120),
      });
    }
  }

  async function searchHotels(query = {}) {
    try {
      const records = await loadRecords();
      const limit = Math.min(100, Math.max(1, Number(query.limit) || 20));
      const nameQ = String(query.name || "").trim();
      const cityQ = String(query.city || "").trim();
      const countryQ = String(query.country || "").trim();
      const brandQ = String(query.brand || "").trim();
      const lat = query.latitude != null ? Number(query.latitude) : null;
      const lng = query.longitude != null ? Number(query.longitude) : null;
      const radiusKm =
        query.radius != null ? Number(query.radius) : query.radius_km != null
          ? Number(query.radius_km)
          : null;

      const scored = [];
      for (const rec of records) {
        const cand = normalizeCensusFields(rec.fields, rec.id);
        if (nameQ) {
          const sim = nameSimilarity(nameQ, cand.name || "");
          if (sim < 0.35 && !normalizeKey(cand.name || "").includes(normalizeKey(nameQ))) {
            continue;
          }
          cand._score = sim;
        } else {
          cand._score = 0.5;
        }
        if (cityQ && citiesMatch(cityQ, cand.city) === false) continue;
        if (countryQ && !countriesMatch(countryQ, cand.country)) continue;
        if (brandQ) {
          const b = normalizeKey(brandQ);
          const hay = normalizeKey(
            `${cand.brand_name || ""} ${cand.parent_company_name || ""} ${cand.name || ""}`
          );
          if (!hay.includes(b) && nameSimilarity(brandQ, cand.brand_name || "") < 0.5) {
            continue;
          }
        }
        if (
          Number.isFinite(lat) &&
          Number.isFinite(lng) &&
          Number.isFinite(radiusKm) &&
          cand.latitude != null &&
          cand.longitude != null
        ) {
          const d = distanceMeters(
            { lat, lng },
            { lat: cand.latitude, lng: cand.longitude }
          );
          if (d == null || d > radiusKm * 1000) continue;
          cand._distance_m = Math.round(d);
        }
        scored.push({ record: rec, candidate: cand });
      }

      scored.sort(
        (a, b) =>
          (b.candidate._score || 0) - (a.candidate._score || 0) ||
          (a.candidate._distance_m || 0) - (b.candidate._distance_m || 0)
      );

      const hotels = scored.slice(0, limit).map(({ record, candidate }) => {
        const hotelId = idRegistry.ensureHotelIdForAirtable(record.id, {
          property_identity_key:
            record.fields?.[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
        });
        const hbx = record.fields?.[MAP_CENSUS_FIELDS.hbxHotelCode];
        if (hbx) {
          try {
            idRegistry.linkExternalId(hotelId, "hotelbeds", String(hbx));
          } catch {
            /* ignore link failures */
          }
        }
        return {
          ...candidate,
          hotel_id: hotelId,
          airtable_record_id: record.id,
          distance_m: candidate._distance_m ?? null,
          match_score: candidate._score ?? null,
        };
      });

      return {
        provider_status: providerStatus(PROVIDER_ID, "ok", { retryable: false }),
        hotels,
      };
    } catch (err) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          retryable: true,
          message: String(err?.message || err).slice(0, 120),
        }),
        hotels: [],
      };
    }
  }

  async function getHotelByAirtableId(airtableRecordId) {
    const records = await loadRecords();
    const rec = records.find((r) => r.id === airtableRecordId);
    if (!rec) return null;
    const hotelId = idRegistry.ensureHotelIdForAirtable(rec.id, {
      property_identity_key:
        rec.fields?.[MAP_CENSUS_FIELDS.propertyIdentityKey] || null,
    });
    return censusRecordToCanonical(rec, hotelId);
  }

  async function getHotelByHotelId(hotelId) {
    const mapping = idRegistry.getByHotelId(hotelId);
    if (!mapping?.airtable_record_id) return null;
    return getHotelByAirtableId(mapping.airtable_record_id);
  }

  async function getHotel(externalId) {
    // externalId treated as airtable record id for this provider
    const hotel = await getHotelByAirtableId(externalId);
    return {
      provider_status: providerStatus(PROVIDER_ID, hotel ? "ok" : "not_found", {
        retryable: false,
      }),
      hotel: hotel
        ? normalizeCensusFields(
            {
              [MAP_CENSUS_FIELDS.officialName]: hotel.identity.official_name,
              [MAP_CENSUS_FIELDS.propertyName]: hotel.identity.display_name,
              [MAP_CENSUS_FIELDS.address]: hotel.location.address_line_1,
              [MAP_CENSUS_FIELDS.city]: hotel.location.city,
              [MAP_CENSUS_FIELDS.country]: hotel.location.country,
              [MAP_CENSUS_FIELDS.latitude]: hotel.location.latitude,
              [MAP_CENSUS_FIELDS.longitude]: hotel.location.longitude,
              [MAP_CENSUS_FIELDS.roomCount]: hotel.property.room_count,
              [MAP_CENSUS_FIELDS.brandName]: hotel.brand.brand_name,
              [MAP_CENSUS_FIELDS.parentCompanyName]: hotel.brand.parent_company_name,
              [MAP_CENSUS_FIELDS.website]: hotel.digital.website,
              [MAP_CENSUS_FIELDS.phone]: hotel.digital.phone,
              [MAP_CENSUS_FIELDS.status]: hotel.property.status,
              [MAP_CENSUS_FIELDS.propertyIdentityKey]:
                hotel.linkages.property_identity_key,
              [MAP_CENSUS_FIELDS.hbxHotelCode]:
                hotel.linkages.external_ids?.[0]?.external_id || null,
            },
            hotel.linkages.airtable_record_id
          )
        : null,
      canonical: hotel,
    };
  }

  function normalizeHotel(raw) {
    if (raw?.fields) return normalizeCensusFields(raw.fields, raw.id);
    return normalizeCensusFields(raw, raw?.id || null);
  }

  return {
    id: PROVIDER_ID,
    version: CENSUS_READ_PROVIDER_VERSION,
    getAvailabilityStatus,
    searchHotels,
    getHotel,
    getHotelByHotelId,
    getHotelByAirtableId,
    loadRecords,
    normalizeHotel,
    toMvpHotelSummary,
    idRegistry,
  };
}

export { blank };
