/**
 * GIATA Drive Open Content — Hotel Intelligence provider adapter.
 *
 * Role: complementary identity + geo/brand enrichment (read-only).
 * NOT: primary CALA universe, room_count, MultiCodes supplier crosswalk.
 *
 * Enable: HOTEL_INTELLIGENCE_GIATA_DRIVE=1
 * Auth: GIATA_DRIVE_API_KEY (Bearer)
 */

import {
  createGiataDriveClient,
  GIATA_DRIVE_ROOMS_CAPABILITY,
  GIATA_DRIVE_SUPPLIER_MAPPING,
  normalizeGiataDriveProperty,
  giataIdFromUrl,
  safeErrorMessage,
} from "../../research-engine-v2/providers/giata-drive/index.js";
import { emptyCandidate, providerStatus } from "./types.js";
import { MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";

export const GIATA_DRIVE_PROVIDER_VERSION = "hotel-intelligence-giata-drive-v1";
export const PROVIDER_ID = MAP_PROVIDER_IDS.giata_drive;
export const GIATA_DRIVE_ROOMS_CAPABILITY_STATUS =
  GIATA_DRIVE_ROOMS_CAPABILITY.status;

/**
 * Convert normalized GIATA property → NormalizedHotelCandidate.
 * room_count is ALWAYS null (firewall).
 */
export function normalizeGiataDriveHotel(normalized, opts = {}) {
  if (!normalized) {
    return emptyCandidate(PROVIDER_ID, { raw_safe: { empty: true } });
  }
  return emptyCandidate(PROVIDER_ID, {
    external_id: normalized.giata_id || null,
    name: normalized.name,
    address: normalized.address,
    city: normalized.city,
    country: normalized.country,
    country_code: normalized.country_code,
    latitude: normalized.latitude,
    longitude: normalized.longitude,
    room_count: null, // FIREWALL — never roomTypes → keys
    brand_name: normalized.brand_name,
    parent_company_name: normalized.parent_company_name,
    website: normalized.website,
    phone: normalized.phone,
    status: null,
    raw_safe: {
      giata_id: normalized.giata_id,
      destination: normalized.destination,
      postal_code: normalized.postal_code,
      state_region: normalized.state_region,
      star_rating: normalized.star_rating,
      description: normalized.description
        ? String(normalized.description).slice(0, 400)
        : null,
      amenity_fact_keys: normalized.amenity_fact_keys || [],
      image_count: normalized.image_count || 0,
      room_types_count: normalized.room_types_count || 0,
      rooms_capability: GIATA_DRIVE_ROOMS_CAPABILITY.status,
      supplier_mapping: GIATA_DRIVE_SUPPLIER_MAPPING.status,
      maps_room_types_to_room_count: false,
      source: opts.source || "giata_drive",
      // Explicitly refuse invented supplier IDs
      hotelbeds_id: null,
      booking_id: null,
      expedia_id: null,
    },
  });
}

function classifyHttp(status, provider = PROVIDER_ID) {
  if (status === 401 || status === 403) {
    return providerStatus(provider, "auth_failure", {
      retryable: false,
      message: `http_${status}`,
      http_status: status,
    });
  }
  if (status === 404) {
    return providerStatus(provider, "not_found", {
      retryable: false,
      message: "http_404",
      http_status: status,
    });
  }
  if (status === 429) {
    return providerStatus(provider, "quota_exhausted", {
      retryable: true,
      message: "http_429",
      http_status: status,
    });
  }
  if (status >= 500) {
    return providerStatus(provider, "unavailable", {
      retryable: true,
      message: `http_${status}`,
      http_status: status,
    });
  }
  return providerStatus(provider, "unavailable", {
    retryable: true,
    message: `http_${status}`,
    http_status: status,
  });
}

/**
 * @param {object} [opts]
 */
export function createGiataDriveProvider(opts = {}) {
  const env = opts.env || process.env;
  const enabled =
    opts.enabled != null
      ? Boolean(opts.enabled)
      : String(env.HOTEL_INTELLIGENCE_GIATA_DRIVE || "0").trim() === "1" ||
        Boolean(opts.forceEnabled);
  const client =
    opts.client ||
    createGiataDriveClient({
      env,
      fetchImpl: opts.fetchImpl,
      baseUrl: opts.baseUrl,
    });

  /** In-memory call metrics for efficiency reporting (per process). */
  const metrics = {
    index_calls: 0,
    detail_calls: 0,
    records_returned: 0,
    successful_details: 0,
  };

  async function getAvailabilityStatus() {
    if (!client.hasCredentials()) {
      return providerStatus(PROVIDER_ID, "unavailable", {
        retryable: false,
        message: "GIATA_DRIVE_API_KEY_missing",
      });
    }
    if (!enabled) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: "HOTEL_INTELLIGENCE_GIATA_DRIVE required",
      });
    }
    return providerStatus(PROVIDER_ID, "ok", {
      retryable: false,
      message: `rooms=${GIATA_DRIVE_ROOMS_CAPABILITY.status};supplier=${GIATA_DRIVE_SUPPLIER_MAPPING.status}`,
    });
  }

  function normalizeHotel(raw) {
    const n = normalizeGiataDriveProperty(raw);
    return normalizeGiataDriveHotel(n, { source: "normalizeHotel" });
  }

  /**
   * Country / index listing. Prefer countryCode for targeted discovery.
   * Does NOT bulk-fetch all global properties by default.
   */
  async function searchHotels(query = {}) {
    if (!enabled) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
          message: "provider_disabled",
        }),
        hotels: [],
        metrics: { ...metrics },
      };
    }
    if (!client.hasCredentials()) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          retryable: false,
          message: "GIATA_DRIVE_API_KEY_missing",
        }),
        hotels: [],
        metrics: { ...metrics },
      };
    }

    const countryCode = String(
      query.countryCode || query.country_code || ""
    )
      .trim()
      .toUpperCase();
    const fetchDetails = query.fetch_details !== false;
    const limit = Math.min(
      Math.max(1, Number(query.limit) || 25),
      Number(query.max_limit) || 50
    );
    const after = query.after != null ? String(query.after) : undefined;

    try {
      metrics.index_calls += 1;
      const listed = await client.listPropertyUrls({
        countryCode: countryCode || undefined,
        after,
      });
      if (!listed.ok) {
        return {
          provider_status: classifyHttp(listed.status || 0),
          hotels: [],
          metrics: { ...metrics },
          index: null,
        };
      }
      const urls = Array.isArray(listed.json?.urls) ? listed.json.urls : [];
      const deletedUrls = Array.isArray(listed.json?.deletedUrls)
        ? listed.json.deletedUrls
        : [];
      const latestRevision =
        listed.json?.latestRevision != null
          ? String(listed.json.latestRevision)
          : null;

      const ids = [];
      for (const u of urls) {
        const id = giataIdFromUrl(u) || String(u).match(/(\d+)/)?.[1];
        if (id) ids.push(id);
        if (ids.length >= limit) break;
      }
      metrics.records_returned += urls.length;

      const hotels = [];
      if (fetchDetails) {
        for (const id of ids) {
          metrics.detail_calls += 1;
          const got = await client.getProperty(id);
          if (!got.ok || !got.json) continue;
          metrics.successful_details += 1;
          hotels.push(normalizeHotel(got.json));
        }
      } else {
        for (const id of ids) {
          hotels.push(
            emptyCandidate(PROVIDER_ID, {
              external_id: id,
              raw_safe: {
                giata_id: id,
                rooms_capability: GIATA_DRIVE_ROOMS_CAPABILITY.status,
                detail_pending: true,
              },
            })
          );
        }
      }

      return {
        provider_status: providerStatus(PROVIDER_ID, "ok", {
          retryable: false,
          message: `urls=${urls.length};returned=${hotels.length};country=${countryCode || "ALL"}`,
        }),
        hotels,
        index: {
          url_count: urls.length,
          deleted_url_count: deletedUrls.length,
          deleted_urls: deletedUrls.slice(0, 100),
          latest_revision: latestRevision,
          country_code: countryCode || null,
        },
        metrics: { ...metrics },
      };
    } catch (err) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          retryable: true,
          message: safeErrorMessage(err).slice(0, 120),
        }),
        hotels: [],
        metrics: { ...metrics },
      };
    }
  }

  async function getHotel(externalId, query = {}) {
    if (!enabled) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
          message: "provider_disabled",
        }),
        hotel: null,
      };
    }
    if (!client.hasCredentials()) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          retryable: false,
          message: "GIATA_DRIVE_API_KEY_missing",
        }),
        hotel: null,
      };
    }
    const id = String(externalId || query.giata_id || "").trim();
    if (!id) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "giata_id_required",
        }),
        hotel: null,
      };
    }
    try {
      metrics.detail_calls += 1;
      const got = await client.getProperty(id);
      if (!got.ok) {
        return {
          provider_status: classifyHttp(got.status || 0),
          hotel: null,
        };
      }
      if (!got.json) {
        return {
          provider_status: providerStatus(PROVIDER_ID, "malformed", {
            retryable: false,
            message: "empty_property_body",
          }),
          hotel: null,
        };
      }
      metrics.successful_details += 1;
      const hotel = normalizeHotel(got.json);
      // Assert firewall in adapter path
      if (hotel.room_count != null) {
        hotel.room_count = null;
      }
      return {
        provider_status: providerStatus(PROVIDER_ID, "ok"),
        hotel,
      };
    } catch (err) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          retryable: true,
          message: safeErrorMessage(err).slice(0, 120),
        }),
        hotel: null,
      };
    }
  }

  function getMetrics() {
    return { ...metrics };
  }

  function capabilities() {
    return {
      provider: PROVIDER_ID,
      role: "COMPLEMENTARY_IDENTITY_GEO_BRAND_ENRICHMENT",
      supported_fields: [
        "giata_id",
        "name",
        "city",
        "destination",
        "country",
        "country_code",
        "address",
        "postal_code",
        "latitude",
        "longitude",
        "brand_name",
        "star_rating",
        "website",
        "phone",
        "description",
        "amenities",
        "images",
        "incremental_update_feed",
      ],
      unsupported_fields: [
        "room_count",
        "hotelbeds_id",
        "booking_id",
        "expedia_id",
        "supplier_crosswalk",
        "primary_cala_universe_discovery",
      ],
      rooms_capability: GIATA_DRIVE_ROOMS_CAPABILITY,
      supplier_mapping: GIATA_DRIVE_SUPPLIER_MAPPING,
      production_roles: [
        "SECONDARY_UNIVERSE_DISCOVERY",
        "IDENTITY_VALIDATION",
        "EXTERNAL_ID_GRAPH",
        "GEO_ENRICHMENT",
        "BRAND_ENRICHMENT",
      ],
    };
  }

  return {
    id: PROVIDER_ID,
    version: GIATA_DRIVE_PROVIDER_VERSION,
    getAvailabilityStatus,
    searchHotels,
    getHotel,
    getHotelContent: getHotel,
    normalizeHotel,
    getMetrics,
    capabilities,
    client,
  };
}

export { giataIdFromUrl, normalizeGiataDriveProperty };
