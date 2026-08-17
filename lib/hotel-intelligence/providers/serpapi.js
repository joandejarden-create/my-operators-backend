/**
 * SerpApi provider adapter for Hotel Intelligence MCP.
 * Reuses lib/research-engine-v2/providers/serpapi-google-hotels/* — no duplicate HTTP client.
 *
 * Primary engine: google_hotels (search + property_token details).
 * Google Maps place_id / data_cid / data_id are documented on SerpApi Maps but are NOT
 * wired in the existing client — leave UNKNOWN until a Maps adapter is approved.
 *
 * Rooms / Keys: NOT_SUPPORTED (room types / VR bedrooms never map to total keys).
 */

import {
  getAccount,
  searchGoogleHotels,
  getGoogleHotelDetails,
  normalizeGoogleHotelProperty,
  matchCensusProperty,
  SERPAPI_ROOMS_CAPABILITY,
  SerpApiCreditTracker,
  safeErrorMessage,
} from "../../research-engine-v2/providers/serpapi-google-hotels/index.js";
import { emptyCandidate, providerStatus } from "./types.js";
import { MAP_PROVIDER_IDS } from "../map_hotel_intelligence_fields.js";

export const SERPAPI_PROVIDER_VERSION = "serpapi-provider-adapter-v1";
export const PROVIDER_ID = MAP_PROVIDER_IDS.serpapi || "serpapi";

export { SERPAPI_ROOMS_CAPABILITY, matchCensusProperty };

/** Country name / code → SerpApi `gl` (best-effort CALA). */
const COUNTRY_TO_GL = Object.freeze({
  mexico: "mx",
  mx: "mx",
  "dominican republic": "do",
  do: "do",
  "costa rica": "cr",
  cr: "cr",
  colombia: "co",
  co: "co",
  panama: "pa",
  pa: "pa",
  guatemala: "gt",
  gt: "gt",
  honduras: "hn",
  hn: "hn",
  "el salvador": "sv",
  sv: "sv",
  nicaragua: "ni",
  ni: "ni",
  belize: "bz",
  bz: "bz",
  jamaica: "jm",
  jm: "jm",
  "puerto rico": "pr",
  pr: "pr",
  cuba: "cu",
  cu: "cu",
  "trinidad and tobago": "tt",
  barbados: "bb",
  aruba: "aw",
  curacao: "cw",
  "united states": "us",
  usa: "us",
  us: "us",
});

export function resolveGl(country) {
  const key = String(country || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
  return COUNTRY_TO_GL[key] || "mx";
}

/**
 * Normalize SerpApi Google Hotels candidate → Hotel Intelligence candidate shape.
 * room_count intentionally always null.
 */
export function normalizeSerpApiHotel(normalizedOrRaw) {
  const n =
    normalizedOrRaw?.firewall_version || normalizedOrRaw?.rooms_capability
      ? normalizedOrRaw
      : normalizeGoogleHotelProperty(normalizedOrRaw);
  if (!n) {
    return emptyCandidate(PROVIDER_ID, { room_count: null });
  }

  return emptyCandidate(PROVIDER_ID, {
    external_id: n.property_token ? String(n.property_token) : null,
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
    room_count: null, // SERPAPI_ROOMS_CAPABILITY = NOT_SUPPORTED
    brand_name: null, // Google Hotels brand filter ≠ Dealality Current Brand SoT
    parent_company_name: null,
    website: n.website || null,
    phone: n.phone || null,
    status: null,
    raw_safe: {
      serpapi_property_token: n.property_token || null,
      google_property_url: n.google_property_url || null,
      google_place_id: null, // Maps engine not wired
      google_maps_data_id: null,
      google_cid: null,
      postal_code: n.postal_code || null,
      state_region: n.state_region || null,
      hotel_class_raw: n.hotel_class_raw ?? null,
      extracted_hotel_class: n.extracted_hotel_class ?? null,
      property_type_raw: n.property_type_raw || null,
      overall_rating: n.overall_rating ?? null,
      reviews: n.reviews ?? null,
      check_in_time: n.check_in_time || null,
      check_out_time: n.check_out_time || null,
      rooms_capability: SERPAPI_ROOMS_CAPABILITY,
      source_shape: n.source_shape || null,
      image_count: Array.isArray(n.image_urls_reference_only)
        ? n.image_urls_reference_only.length
        : 0,
      observed_bookable_room_type_count:
        n._observed_bookable_room_type_count ?? null,
    },
  });
}

function classifyError(resOrErr) {
  const status = Number(resOrErr?.status || 0);
  const msg = String(
    resOrErr?.reason || resOrErr?.error?.message || resOrErr?.message || ""
  );
  if (/ceiling|quota|searches?_left|ran out/i.test(msg) || resOrErr?.blocked) {
    return providerStatus(PROVIDER_ID, "quota_exhausted", {
      retryable: true,
      message: "search_ceiling_or_unavailable",
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

function buildQuery(query = {}) {
  const parts = [query.name, query.city, query.country].filter(Boolean);
  if (parts.length) return parts.join(", ");
  return String(query.q || query.location || "").trim();
}

/**
 * @param {object} [opts]
 */
export function createSerpApiProvider(opts = {}) {
  const env = opts.env || process.env;
  const enabled =
    opts.enabled != null
      ? Boolean(opts.enabled)
      : String(env.HOTEL_INTELLIGENCE_SERPAPI || "0").trim() === "1" ||
        Boolean(opts.forceEnabled);
  const tracker =
    opts.tracker ||
    new SerpApiCreditTracker({
      ceiling: Number(opts.creditCeiling || env.SERPAPI_HI_CREDIT_CEILING || 500),
      startingSearchesLeft: opts.startingSearchesLeft ?? null,
    });

  async function getAvailabilityStatus() {
    if (!String(env.SERPAPI_KEY || env.SERPAPI_API_KEY || "").trim()) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: "SERPAPI_KEY_missing",
      });
    }
    if (!enabled) {
      return providerStatus(PROVIDER_ID, "disabled", {
        retryable: false,
        message: "HOTEL_INTELLIGENCE_SERPAPI required",
      });
    }
    try {
      const acct = await getAccount();
      if (!acct.ok) {
        return classifyError(acct);
      }
      const left = acct.plan_searches_left ?? acct.total_searches_left;
      if (left != null && Number(left) <= 0) {
        return providerStatus(PROVIDER_ID, "quota_exhausted", {
          retryable: true,
          message: "plan_searches_left_zero",
        });
      }
      if (tracker.startingSearchesLeft == null && left != null) {
        tracker.startingSearchesLeft = Number(left);
      }
      return providerStatus(PROVIDER_ID, "ok", {
        retryable: false,
        message: `plan=${acct.plan_name || "unknown"};searches_left=${left ?? "unknown"};rpm_hour=${acct.account_rate_limit_per_hour ?? "unknown"}`,
      });
    } catch (err) {
      return providerStatus(PROVIDER_ID, "unavailable", {
        retryable: true,
        message: safeErrorMessage(err).slice(0, 120),
      });
    }
  }

  /**
   * Search Google Hotels. Supported query: name + city + country (preferred),
   * or free-text `q` / `location`. Optional property_token for details-only.
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
    const q = buildQuery(query);
    if (!q) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "q_or_name_city_required",
        }),
        hotels: [],
      };
    }

    try {
      const res = await searchGoogleHotels(
        {
          q,
          gl: query.gl || resolveGl(query.country),
          hl: query.hl || "en",
          currency: query.currency || "USD",
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
      const hotels = (res.candidates || []).map(normalizeSerpApiHotel);
      return {
        provider_status: providerStatus(PROVIDER_ID, "ok", { retryable: false }),
        hotels,
        credits_charged: res.creditsCharged || 0,
        response_shape: res.response_shape || null,
        search_id: res.search_id || null,
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
   * getHotel(property_token) — Google Hotels property details.
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
    const token = String(
      detailQuery.property_token || detailQuery.serpapi_property_token || externalId || ""
    ).trim();
    if (!token) {
      return {
        provider_status: providerStatus(PROVIDER_ID, "not_found", {
          retryable: false,
          message: "property_token_required",
        }),
        hotel: null,
      };
    }
    try {
      const res = await getGoogleHotelDetails(
        {
          property_token: token,
          q: detailQuery.q || detailQuery.name || "hotel",
          gl: detailQuery.gl || resolveGl(detailQuery.country),
          hl: detailQuery.hl || "en",
          currency: detailQuery.currency || "USD",
          timeoutMs: detailQuery.timeoutMs,
        },
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
        hotel: normalizeSerpApiHotel(res.candidate),
        credits_charged: res.creditsCharged || 0,
        search_id: res.search_id || null,
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
    version: SERPAPI_PROVIDER_VERSION,
    rooms_capability: SERPAPI_ROOMS_CAPABILITY,
    getAvailabilityStatus,
    searchHotels,
    getHotel,
    getHotelContent: getHotel,
    normalizeHotel: normalizeSerpApiHotel,
    matchCensusProperty,
    tracker,
  };
}
