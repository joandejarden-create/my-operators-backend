/**
 * StayingAPI provider adapter for Hotel Intelligence MCP.
 * Reuses lib/research-engine-v2/providers/staying-api/* — does not duplicate HTTP client.
 *
 * Rooms / Keys: NOT_SUPPORTED (bedrooms/occupancy/room types must never map to total keys).
 */

import {
  getAccount,
  searchProperties,
  getListing,
  normalizeProperty,
  STAYINGAPI_ROOMS_CAPABILITY,
  StayingCreditTracker,
  safeErrorMessage,
} from "../../research-engine-v2/providers/staying-api/index.js";
import { emptyCandidate, providerStatus } from "./types.js";
import { MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";

export const STAYINGAPI_PROVIDER_VERSION = "stayingapi-provider-adapter-v1";
export const PROVIDER_ID = MAP_PROVIDER_IDS.stayingapi || "stayingapi";

export { STAYINGAPI_ROOMS_CAPABILITY };

/**
 * Normalize StayingAPI candidate → Hotel Intelligence candidate shape.
 * room_count intentionally always null.
 */
export function normalizeStayingHotel(normalizedOrRaw) {
  const n =
    normalizedOrRaw?.firewall_version || normalizedOrRaw?.rooms_capability
      ? normalizedOrRaw
      : normalizeProperty(normalizedOrRaw);
  if (!n) {
    return emptyCandidate(PROVIDER_ID, { room_count: null });
  }

  const platform = String(n.platform || "").toLowerCase();
  const platformListingId = n.platform_listing_id || null;

  return emptyCandidate(PROVIDER_ID, {
    external_id: n.staying_id ? String(n.staying_id) : null,
    name: n.name || null,
    address: n.address || null,
    city: n.city || null,
    country: n.country || null,
    country_code: null,
    latitude:
      n.latitude != null && Number.isFinite(Number(n.latitude))
        ? Number(n.latitude)
        : null,
    longitude:
      n.longitude != null && Number.isFinite(Number(n.longitude))
        ? Number(n.longitude)
        : null,
    room_count: null, // STAYINGAPI_ROOMS_CAPABILITY = NOT_SUPPORTED
    brand_name: null, // OTA listing does not provide Dealality brand SoT
    parent_company_name: null,
    website: n.url || null,
    phone: null,
    status: null,
    raw_safe: {
      staying_id: n.staying_id || null,
      platform: n.platform || null,
      platform_listing_id: platformListingId,
      booking_com_id: platform === "booking" ? platformListingId : null,
      property_type: n.property_type_dealality || n.property_type_raw || null,
      postal_code: n.postal_code || null,
      state_region: n.state_region || null,
      rooms_capability: STAYINGAPI_ROOMS_CAPABILITY,
      image_count: Array.isArray(n.image_urls_reference_only)
        ? n.image_urls_reference_only.length
        : 0,
    },
  });
}

function classifyError(resOrErr) {
  const status = Number(resOrErr?.status || 0);
  const msg = String(
    resOrErr?.reason || resOrErr?.error?.message || resOrErr?.message || ""
  );
  if (/credit|ceiling|quota/i.test(msg) || resOrErr?.blocked) {
    return providerStatus(PROVIDER_ID, "quota_exhausted", {
      retryable: true,
      message: "credit_ceiling_or_unavailable",
      http_status: status || null,
    });
  }
  if (status === 429 || /rate limit/i.test(msg)) {
    return providerStatus(PROVIDER_ID, "unavailable", {
      retryable: true,
      message: "rate_limited",
      http_status: 429,
    });
  }
  if (status === 401 || status === 403) {
    return providerStatus(PROVIDER_ID, "auth_failure", {
      retryable: false,
      message: "auth_failure",
      http_status: status,
    });
  }
  if (/timeout|abort/i.test(msg)) {
    return providerStatus(PROVIDER_ID, "timeout", {
      retryable: true,
      message: msg.slice(0, 120),
    });
  }
  return providerStatus(PROVIDER_ID, "unavailable", {
    retryable: true,
    message: msg.slice(0, 120) || "provider_error",
    http_status: status || null,
  });
}

/**
 * @param {object} [opts]
 */
export function createStayingApiProvider(opts = {}) {
  const env = opts.env || process.env;
  const enabled =
    opts.enabled != null
      ? Boolean(opts.enabled)
      : String(env.HOTEL_INTELLIGENCE_STAYINGAPI || "0").trim() === "1" ||
        Boolean(opts.forceEnabled);
  const tracker =
    opts.tracker ||
    new StayingCreditTracker({
      ceiling: Number(opts.creditCeiling || env.STAYINGAPI_HI_CREDIT_CEILING || 120),
      startingAvailable: opts.startingCredits ?? null,
    });

  async function getAvailabilityStatus() {
    if (!String(env.STAYINGAPI_KEY || "").trim()) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: "STAYINGAPI_KEY_missing",
      });
    }
    if (!enabled) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: "HOTEL_INTELLIGENCE_STAYINGAPI required",
      });
    }
    try {
      const acct = await getAccount();
      if (!acct.ok) {
        return classifyError(acct);
      }
      const available = acct.credits?.available;
      if (available != null && Number(available) <= 0) {
        return providerStatus(PROVIDER_ID, "quota_exhausted", {
          retryable: true,
          message: "credits_available_zero",
        });
      }
      if (tracker.startingAvailable == null && available != null) {
        tracker.startingAvailable = Number(available);
      }
      return providerStatus(PROVIDER_ID, "ok", {
        retryable: false,
        message: `plan=${acct.plan?.code || "unknown"};credits_available=${available ?? "unknown"};rpm=${acct.rateLimit?.requestsPerMinute ?? "unknown"}`,
      });
    } catch (err) {
      return providerStatus(PROVIDER_ID, "unavailable", {
        retryable: true,
        message: safeErrorMessage(err).slice(0, 120),
      });
    }
  }

  /**
   * Search via location string. Supported query shape (existing client):
   * { location, name?, city?, country?, limit, platforms }
   * Free-text global hotel search is NOT a separate endpoint — location is required.
   */
  async function searchHotels(query = {}) {
    if (!enabled) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
          message: "provider_disabled",
        }),
        hotels: [],
      };
    }
    const location =
      String(query.location || "").trim() ||
      [query.name, query.city, query.country].filter(Boolean).join(", ").trim() ||
      [query.city, query.country].filter(Boolean).join(", ").trim();
    if (!location) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "location_required_for_stayingapi_search",
        }),
        hotels: [],
      };
    }

    try {
      const res = await searchProperties(
        {
          location,
          platforms: query.platforms || ["booking"],
          limit: Math.min(10, Math.max(1, Number(query.limit) || 5)),
          timeoutMs: query.timeoutMs,
        },
        { tracker, hotelId: query.hotel_id }
      );
      if (res.blocked) {
        return {
          provider_status: classifyError(res),
          hotels: [],
          credits_charged: 0,
        };
      }
      if (!res.ok) {
        return {
          provider_status: classifyError(res),
          hotels: [],
          credits_charged: res.creditsCharged || 0,
        };
      }
      let hotels = (res.candidates || []).map(normalizeStayingHotel);
      const nameQ = String(query.name || "").trim().toLowerCase();
      if (nameQ) {
        hotels = hotels.filter((h) =>
          String(h.name || "").toLowerCase().includes(nameQ.split(/\s+/)[0] || nameQ)
        );
      }
      return {
        provider_status: providerStatus(PROVIDER_ID, "ok", { retryable: false }),
        hotels,
        credits_charged: res.creditsCharged || 0,
      };
    } catch (err) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "unavailable", {
          retryable: true,
          message: safeErrorMessage(err).slice(0, 120),
        }),
        hotels: [],
      };
    }
  }

  /**
   * getHotel(externalId) — expects platform:listingId or staying id via listing.
   * Prefer { platform, platform_listing_id } in query for details.
   */
  async function getHotel(externalId, detailQuery = {}) {
    if (!enabled) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
        }),
        hotel: null,
      };
    }
    const platform =
      detailQuery.platform ||
      (String(externalId || "").includes(":")
        ? String(externalId).split(":")[0]
        : "booking");
    const id =
      detailQuery.platform_listing_id ||
      (String(externalId || "").includes(":")
        ? String(externalId).split(":").slice(1).join(":")
        : externalId);
    if (!id) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "listing_id_required",
        }),
        hotel: null,
      };
    }
    try {
      const res = await getListing(
        platform,
        id,
        { timeoutMs: detailQuery.timeoutMs },
        { tracker, hotelId: detailQuery.hotel_id, useful: true }
      );
      if (res.blocked) {
        return { provider_status: classifyError(res), hotel: null };
      }
      if (!res.ok || !res.candidate) {
        return {
          provider_status: classifyError(res),
          hotel: null,
          credits_charged: res.creditsCharged || 0,
        };
      }
      return {
        provider_status: providerStatus(PROVIDER_ID, "ok", { retryable: false }),
        hotel: normalizeStayingHotel(res.candidate),
        credits_charged: res.creditsCharged || 0,
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

  return {
    id: PROVIDER_ID,
    version: STAYINGAPI_PROVIDER_VERSION,
    rooms_capability: STAYINGAPI_ROOMS_CAPABILITY,
    getAvailabilityStatus,
    searchHotels,
    getHotel,
    getHotelContent: getHotel,
    normalizeHotel: normalizeStayingHotel,
    tracker,
  };
}
