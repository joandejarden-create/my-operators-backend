/**
 * Tripadvisor via Apify (maxcopell/tripadvisor) — read-only hotel enrichment provider.
 *
 * Does NOT write Airtable / census Rooms / Keys.
 * Prefer injecting a pre-fetched Actor dataset (MCP) via searchHotels({ pool }).
 * Live Apify HTTP calls only when APIFY_TOKEN is present (optional).
 *
 * Cost tracking: after any Actor run, record via
 *   createApifyUsageStore().recordRun({ use_case: 'ROOM_COUNT', apify_run, ... })
 * or recordTripadvisorRoomCountRun — see lib/hotel-intelligence/apify-usage/.
 * Prefer GET /v2/actor-runs/{runId} usageTotalUsd (MCP resource) over estimates.
 */

import { emptyCandidate, providerStatus } from "./types.js";
import { MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";
import {
  buildTripadvisorActorInput,
  buildTripadvisorResolutionPlan,
  matchTripadvisorHotel,
  usableTripadvisorRooms,
  isHotelItem,
} from "../tripadvisor-rooms/index.js";

const PROVIDER_ID = MAP_PROVIDER_IDS.tripadvisor_apify;

function normalizeTaItem(it) {
  return emptyCandidate(PROVIDER_ID, {
    external_id: it?.id != null ? String(it.id) : null,
    name: it?.name || null,
    address: it?.address || null,
    city: it?.addressObj?.city || null,
    country: it?.addressObj?.country || it?.locationString || null,
    latitude: it?.latitude ?? null,
    longitude: it?.longitude ?? null,
    room_count: usableTripadvisorRooms(it) ? Number(it.numberOfRooms) : null,
    website: it?.website || null,
    phone: it?.phone || null,
    status: null,
    raw_safe: {
      type: it?.type || null,
      category: it?.category || null,
      hotelClass: it?.hotelClass ?? null,
      email: it?.email || null,
      webUrl: it?.webUrl || null,
      amenities: Array.isArray(it?.amenities) ? it.amenities.slice(0, 20) : null,
      numberOfReviews: it?.numberOfReviews ?? null,
    },
  });
}

/**
 * @param {object} [opts]
 */
export function createTripadvisorApifyProvider(opts = {}) {
  const env = opts.env || process.env;
  const forceEnabled = opts.forceEnabled === true;
  const token = String(env.APIFY_TOKEN || env.APIFY_API_TOKEN || "").trim();

  async function getAvailabilityStatus() {
    if (forceEnabled || opts.pool?.length) {
      return providerStatus(PROVIDER_ID, "ok", {
        message: opts.pool?.length
          ? `pool_mode:${opts.pool.length}_items`
          : "forced",
      });
    }
    if (!token) {
      return providerStatus(PROVIDER_ID, "disabled", {
        message: "APIFY_TOKEN missing — use MCP pool injection or set token",
      });
    }
    return providerStatus(PROVIDER_ID, "ok", { message: "apify_token_present" });
  }

  function normalizeHotel(raw) {
    return normalizeTaItem(raw);
  }

  /**
   * @param {object} query
   * @param {object[]} [query.pool] Tripadvisor Actor items
   * @param {string} [query.name]
   */
  async function searchHotels(query = {}) {
    const pool = query.pool || opts.pool || [];
    if (!pool.length) {
      // Live Actor run only with token — not used in staged MCP path
      if (!token) {
        return {
          provider_status: providerStatus(PROVIDER_ID, "disabled", {
            message: "no_pool_and_no_apify_token",
          }),
          hotels: [],
        };
      }
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          message:
            "live_apify_http_not_wired_in_provider_use_mcp_or_inject_pool",
          retryable: false,
        }),
        hotels: [],
      };
    }

    if (query.name || query.hotel) {
      const hotel = query.hotel || {
        name: query.name,
        city: query.city,
        country: query.country,
        lat: query.latitude,
        lng: query.longitude,
        website: query.website,
        rooms: query.room_count,
      };
      const { match, rejection } = matchTripadvisorHotel(hotel, pool);
      if (!match) {
        return {
          provider_status: providerStatus(PROVIDER_ID, "not_found", {
            message: rejection?.reason || "no_match",
          }),
          hotels: [],
          rejection,
          resolution_plan: buildTripadvisorResolutionPlan(hotel),
        };
      }
      return {
        provider_status: providerStatus(PROVIDER_ID, "ok"),
        hotels: [normalizeHotel(match.item)],
        match_meta: {
          score: match.score,
          confidence: match.confidence,
          geo_km: match.geo_km,
        },
      };
    }

    const hotels = pool.filter(isHotelItem).map(normalizeHotel);
    return {
      provider_status: providerStatus(PROVIDER_ID, "ok"),
      hotels,
    };
  }

  async function getHotel(externalId) {
    const pool = opts.pool || [];
    const hit = pool.find((it) => String(it?.id) === String(externalId));
    if (!hit) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found"),
        hotel: null,
      };
    }
    return {
      provider_status: providerStatus(PROVIDER_ID, "ok"),
      hotel: normalizeHotel(hit),
    };
  }

  return {
    id: PROVIDER_ID,
    getAvailabilityStatus,
    searchHotels,
    getHotel,
    normalizeHotel,
    buildActorInput: buildTripadvisorActorInput,
    buildResolutionPlan: buildTripadvisorResolutionPlan,
  };
}

export { PROVIDER_ID as TRIPADVISOR_APIFY_PROVIDER_ID };
