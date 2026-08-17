/**
 * Hotelbeds provider adapter — wraps existing HBX Content API client.
 * Provider id for MCP: hotelbeds. Internal module remains HBX.
 *
 * Quota exhaustion → provider_status.quota_exhausted (retryable), never crash MCP.
 */

import {
  resolveHbxConfig,
  hbxFetchJson,
  contentUrl,
} from "../../research-engine-v2/hbx-content-api-client.js";
import { createHbxRequestRateLimiter } from "../../research-engine-v2/hbx-request-rate-limiter-v1.js";
import { emptyCandidate, providerStatus } from "./types.js";
import { MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";

function textContent(v) {
  if (v == null) return null;
  if (typeof v === "string") return v.trim() || null;
  if (typeof v === "object" && v.content != null) return String(v.content).trim() || null;
  return String(v).trim() || null;
}

function extractPhoneHotel(phones) {
  if (!Array.isArray(phones)) return null;
  for (const p of phones) {
    const typ = String(p?.phoneType || p?.phoneTypeCode || p?.type || "")
      .toUpperCase()
      .trim();
    const num = p?.phoneNumber || p?.number || null;
    if (!num) continue;
    if (typ.includes("PHONEBOOKING") || typ.includes("PHONEMANAGEMENT")) continue;
    if (typ.includes("PHONEHOTEL") || typ === "HOTEL" || !typ) {
      return String(num).trim();
    }
  }
  return null;
}

/** Minimal HBX hotel extract (aligned with wave1 extractHbxHotel; no rooms[] length as keys). */
function extractRawHotel(raw, countryHint) {
  const roomsNumber =
    raw?.roomsNumber ?? raw?.roomCount ?? raw?.numberOfRooms ?? raw?.totalRooms ?? null;
  return {
    hbx_hotel_code: raw?.code ?? raw?.hotelCode ?? null,
    name: textContent(raw?.name),
    address: textContent(raw?.address),
    city: textContent(raw?.city),
    country: countryHint || null,
    country_code: String(raw?.countryCode || raw?.country?.code || "").toUpperCase() || null,
    latitude: raw?.coordinates?.latitude ?? raw?.latitude ?? null,
    longitude: raw?.coordinates?.longitude ?? raw?.longitude ?? null,
    website: raw?.web || raw?.website || raw?.url || null,
    phonehotel: extractPhoneHotel(raw?.phones),
    rooms_total_supported: roomsNumber != null && Number(roomsNumber) > 0,
    rooms_total_value:
      roomsNumber != null && Number(roomsNumber) > 0 ? Number(roomsNumber) : null,
    chain_code: raw?.chainCode || raw?.chain?.code || null,
    category: raw?.categoryCode || raw?.category?.code || null,
  };
}

export const HOTELBEDS_PROVIDER_VERSION = "hotelbeds-provider-adapter-v1";
export const PROVIDER_ID = MAP_PROVIDER_IDS.hotelbeds;

function isQuotaResponse(res) {
  if (!res) return false;
  if (res.quota_exceeded) return true;
  const status = Number(res.status || 0);
  const msg = String(res.error_message || res.error_code || "");
  return status === 403 && /quota/i.test(msg);
}

function classifyFetch(res) {
  if (!res) {
    return providerStatus(PROVIDER_ID, "unavailable", {
      retryable: true,
      message: "no_response",
    });
  }
  if (isQuotaResponse(res)) {
    return providerStatus(PROVIDER_ID, "quota_exhausted", {
      retryable: true,
      message: "TEST_DAILY_QUOTA_EXHAUSTED",
      http_status: res.status || 403,
    });
  }
  if (res.budget_exceeded) {
    return providerStatus(PROVIDER_ID, "unavailable", {
      retryable: true,
      message: "REQUEST_BUDGET_EXCEEDED",
      http_status: res.status || 0,
    });
  }
  if (res.status === 401 || res.status === 403) {
    return providerStatus(PROVIDER_ID, "auth_failure", {
      retryable: false,
      message: String(res.error_message || "auth_failure").slice(0, 120),
      http_status: res.status,
    });
  }
  if (res.error_code === "AbortError" || /timeout|aborted/i.test(String(res.error_message || ""))) {
    return providerStatus(PROVIDER_ID, "timeout", {
      retryable: true,
      message: String(res.error_message || "timeout").slice(0, 120),
      http_status: res.status || 0,
    });
  }
  if (!res.ok) {
    return providerStatus(PROVIDER_ID, "unavailable", {
      retryable: res.status === 429 || res.status >= 500,
      message: String(res.error_message || res.error_code || "provider_error").slice(0, 120),
      http_status: res.status || 0,
    });
  }
  return providerStatus(PROVIDER_ID, "ok", { retryable: false, http_status: res.status });
}

export function normalizeHotelbedsHotel(rawOrExtracted, opts = {}) {
  const extracted =
    rawOrExtracted?.hbx_hotel_code != null || rawOrExtracted?.normalized_name != null
      ? rawOrExtracted
      : extractRawHotel(rawOrExtracted, opts.countryHint);

  const lat = extracted.latitude != null ? Number(extracted.latitude) : null;
  const lng = extracted.longitude != null ? Number(extracted.longitude) : null;

  return emptyCandidate(PROVIDER_ID, {
    external_id:
      extracted.hbx_hotel_code != null ? String(extracted.hbx_hotel_code) : null,
    name: extracted.name || null,
    address: extracted.address || null,
    city: extracted.city || null,
    country: extracted.country || null,
    country_code: extracted.country_code || null,
    latitude: Number.isFinite(lat) && !(lat === 0 && lng === 0) ? lat : null,
    longitude: Number.isFinite(lng) && !(lat === 0 && lng === 0) ? lng : null,
    room_count: extracted.rooms_total_value ?? null,
    brand_name: extracted.chain_code || null,
    parent_company_name: null,
    website: extracted.website || null,
    phone: extracted.phonehotel || null,
    status: null,
    raw_safe: {
      hbx_hotel_code: extracted.hbx_hotel_code ?? null,
      chain_code: extracted.chain_code ?? null,
      category: extracted.category ?? null,
      rooms_total_supported: extracted.rooms_total_supported ?? null,
    },
  });
}

/**
 * @param {object} [opts]
 */
export function createHotelbedsProvider(opts = {}) {
  const env = opts.env || process.env;
  const cfg = opts.cfg || resolveHbxConfig(env);
  const limiter =
    opts.limiter ||
    createHbxRequestRateLimiter({
      minIntervalMs: opts.minIntervalMs,
      maxRequestsPerRun: opts.maxRequestsPerRun ?? 50,
    });
  const enabled =
    opts.enabled != null
      ? Boolean(opts.enabled)
      : String(env.ENABLE_HBX_CONTENT_API || "0").trim() === "1" ||
        String(env.HOTEL_INTELLIGENCE_HOTELBEDS || "0").trim() === "1" ||
        Boolean(opts.forceEnabled);

  async function getAvailabilityStatus() {
    if (!cfg.ok) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: `missing_credentials:${(cfg.missing || []).join(",")}`,
      });
    }
    if (!enabled) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: "ENABLE_HBX_CONTENT_API or HOTEL_INTELLIGENCE_HOTELBEDS required",
      });
    }
    try {
      const res = await limiter.schedule(() =>
        hbxFetchJson(contentUrl(cfg, "hotels?from=1&to=1"), cfg)
      );
      return classifyFetch(res);
    } catch (err) {
      return providerStatus(PROVIDER_ID, "unavailable", {
        retryable: true,
        message: String(err?.message || err).slice(0, 120),
      });
    }
  }

  /**
   * Search by destination country codes / hotel codes — Content API hotels list.
   * Query: { country_code, from, to, hotel_codes[], name, city, limit }
   */
  async function searchHotels(query = {}) {
    if (!cfg.ok) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
          message: "missing_credentials",
        }),
        hotels: [],
      };
    }
    if (!enabled) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "disabled", {
          retryable: false,
          message: "provider_disabled",
        }),
        hotels: [],
      };
    }

    const limit = Math.min(100, Math.max(1, Number(query.limit) || 20));
    const from = Math.max(1, Number(query.from) || 1);
    const to = from + limit - 1;
    const params = new URLSearchParams();
    params.set("from", String(from));
    params.set("to", String(to));
    params.set("fields", "all");
    params.set("language", "ENG");
    if (query.country_code) params.set("countryCode", String(query.country_code).toUpperCase());
    if (query.destination_code) {
      params.set("destinationCode", String(query.destination_code).toUpperCase());
    }
    if (Array.isArray(query.hotel_codes) && query.hotel_codes.length) {
      params.set("codes", query.hotel_codes.map(String).join(","));
    }

    try {
      const res = await limiter.schedule(() =>
        hbxFetchJson(contentUrl(cfg, `hotels?${params}`), cfg)
      );
      const status = classifyFetch(res);
      if (status.status !== "ok") {
        return { provider_status: status, hotels: [] };
      }
      const hotelsRaw = res.body?.hotels || res.body?.hotel || [];
      const list = Array.isArray(hotelsRaw) ? hotelsRaw : hotelsRaw ? [hotelsRaw] : [];
      let hotels = list.map((h) =>
        normalizeHotelbedsHotel(h, { countryHint: query.country })
      );

      const nameQ = String(query.name || "").trim().toLowerCase();
      const cityQ = String(query.city || "").trim().toLowerCase();
      if (nameQ) {
        hotels = hotels.filter((h) => String(h.name || "").toLowerCase().includes(nameQ));
      }
      if (cityQ) {
        hotels = hotels.filter((h) => String(h.city || "").toLowerCase().includes(cityQ));
      }
      if (query.brand) {
        const b = String(query.brand).toLowerCase();
        hotels = hotels.filter(
          (h) =>
            String(h.brand_name || "").toLowerCase().includes(b) ||
            String(h.name || "").toLowerCase().includes(b)
        );
      }

      return { provider_status: status, hotels: hotels.slice(0, limit) };
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

  async function getHotel(externalId) {
    const code = String(externalId || "").trim();
    if (!code) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "missing_external_id",
        }),
        hotel: null,
      };
    }
    const result = await searchHotels({ hotel_codes: [code], limit: 1 });
    if (result.provider_status.status !== "ok") {
      return { provider_status: result.provider_status, hotel: null };
    }
    const hotel = result.hotels[0] || null;
    if (!hotel) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "hotel_not_found",
          http_status: 404,
        }),
        hotel: null,
      };
    }
    return { provider_status: result.provider_status, hotel };
  }

  return {
    id: PROVIDER_ID,
    version: HOTELBEDS_PROVIDER_VERSION,
    getAvailabilityStatus,
    searchHotels,
    getHotel,
    getHotelContent: getHotel,
    normalizeHotel: normalizeHotelbedsHotel,
    /** @internal test helpers */
    _classifyFetch: classifyFetch,
    _isQuotaResponse: isQuotaResponse,
  };
}
