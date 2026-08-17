/**
 * Marriott official metadata adapter — MARSHA / property URL → Level 2 candidates.
 *
 * Revalidates underlying official Marriott sources only.
 * Webhound findings must be passed as candidate URLs and re-fetched here.
 */

import {
  extractMarshaCode,
  fetchMarriottHqvCoordinates,
} from "./marriott-hqv-coordinate-client.js";
import {
  extractOfficialAddressFromHtml,
  extractOfficialRoomsFromHtml,
} from "./census-level-2-parent-extractors.js";
import {
  extractOfficialPhoneFromHtml,
  isForbiddenPhoneSourceUrl,
} from "./census-phone-number-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { isFalsePositiveRoomCount } from "./production-census-rooms-keys-extractor.js";

export const MARRIOTT_METADATA_ADAPTER_VERSION =
  "marriott-official-metadata-adapter-v1";

function isBlank(v) {
  return v == null || !String(v).trim();
}

function classifyFetchBlock(status, text) {
  if (status === 403 || status === 429) return "akamai_or_bot_blocked";
  if (/access denied|akamai|bot.?detect|forbidden/i.test(String(text || ""))) {
    return "akamai_or_bot_blocked";
  }
  return null;
}

/**
 * Alternate Marriott overview URL candidates (locale / trailing variants).
 * @param {string} officialUrl
 */
export function buildMarriottAlternatePropertyUrls(officialUrl) {
  const raw = String(officialUrl || "").trim();
  if (!/^https?:\/\//i.test(raw)) return [];
  if (isForbiddenPhoneSourceUrl(raw)) return [];
  /** @type {string[]} */
  const out = [];
  const base = raw.replace(/\/+$/, "").replace(/\/overview\/?$/i, "");
  const variants = [
    `${base}/overview/`,
    `${base}/overview`,
    base.replace("/en-us/", "/en/"),
    base.replace("/en-us/", "/es-mx/"),
    base.replace("/en-us/", "/es/"),
  ];
  for (const v of variants) {
    if (/marriott\.com/i.test(v) && !out.includes(v)) out.push(v);
  }
  return out;
}

/**
 * Fetch Marriott official HTML; classify blocks without fatal throw.
 * @param {string} url
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function fetchMarriottOfficialPage(url, opts = {}) {
  const target = String(url || "").trim();
  if (!target || isForbiddenPhoneSourceUrl(target)) {
    return { ok: false, reason: "forbidden_or_blank_url", blocked: false };
  }
  const timeoutMs = opts.timeoutMs || 20000;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(target, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "user-agent":
          "Mozilla/5.0 (compatible; DealalityCensusMarriottMetadata/1.0; +https://dealality.com)",
        accept: "text/html,application/xhtml+xml",
      },
    });
    const text = await res.text();
    const block = classifyFetchBlock(res.status, text);
    if (block) {
      return {
        ok: false,
        reason: block,
        blocked: true,
        status: res.status,
        url: target,
        html: null,
      };
    }
    if (!res.ok) {
      return {
        ok: false,
        reason: `http_${res.status}`,
        blocked: false,
        status: res.status,
        url: target,
      };
    }
    return { ok: true, html: text, url: target, status: res.status, blocked: false };
  } catch (err) {
    return {
      ok: false,
      reason: err?.name === "AbortError" ? "timeout" : "network_error",
      blocked: false,
      error: err?.message || String(err),
      url: target,
    };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Extract Level 2 fields from official Marriott page HTML + optional HQV coords.
 * @param {object} record
 * @param {object} [opts]
 */
export async function extractMarriottOfficialMetadata(record, opts = {}) {
  const f = record?.fields || {};
  const official =
    opts.pageUrl ||
    f["Official Property URL"] ||
    f["Source URL"] ||
    "";
  const marsha =
    opts.marsha ||
    extractMarshaCode(official) ||
    extractMarshaCode(f["Property Identity Key"]);

  const result = {
    version: MARRIOTT_METADATA_ADAPTER_VERSION,
    record_id: record?.id || null,
    marsha,
    webhound_as_sot: false,
    blocked: false,
    candidates: {},
    attempts: [],
    source_urls_tried: [],
  };

  const urls = [
    ...buildMarriottAlternatePropertyUrls(official),
    ...(opts.extraUrls || []),
  ].filter(Boolean);

  for (const url of urls.slice(0, opts.maxUrlAttempts || 4)) {
    result.source_urls_tried.push(url);
    const fetched = await fetchMarriottOfficialPage(url, opts);
    result.attempts.push({
      url,
      ok: fetched.ok,
      reason: fetched.reason,
      blocked: fetched.blocked,
    });
    if (fetched.blocked) {
      result.blocked = true;
      continue;
    }
    if (!fetched.ok || !fetched.html) continue;

    if (!result.candidates.address) {
      const addr = extractOfficialAddressFromHtml(fetched.html, url);
      if (addr.ok && isStreetLevelAddress(addr.address)) {
        result.candidates.address = {
          value: addr.address,
          state: addr.state || null,
          city: addr.city || null,
          source_url: url,
          source_type: "official_property_page",
          confidence: "High",
          evidence_tier: "official_marriott_html",
        };
      }
    }
    if (!result.candidates.phone) {
      const phone = extractOfficialPhoneFromHtml(fetched.html, url);
      if (phone.ok && phone.phone) {
        result.candidates.phone = {
          value: phone.phone,
          source_url: url,
          source_type: "official_property_page",
          confidence: "High",
          evidence_tier: "official_marriott_html",
        };
      }
    }
    if (!result.candidates.rooms) {
      const rooms = extractOfficialRoomsFromHtml(fetched.html, url);
      const roomCount = rooms.rooms ?? rooms.count;
      if (
        rooms.ok &&
        roomCount != null &&
        !isFalsePositiveRoomCount(
          rooms.note || fetched.html.slice(0, 400),
          roomCount,
          rooms.method || "official"
        )
      ) {
        result.candidates.rooms = {
          value: roomCount,
          source_url: url,
          source_type: "official_property_page",
          confidence: "High",
          evidence_tier: "official_marriott_html",
        };
      }
    }
    if (
      result.candidates.address &&
      result.candidates.phone &&
      result.candidates.rooms
    ) {
      break;
    }
  }

  if (marsha && opts.includeHqv !== false && !result.candidates.coordinates) {
    const hqv = await fetchMarriottHqvCoordinates(marsha, {
      signature: opts.hqvSignature,
    });
    result.attempts.push({
      url: "hqv_graphql",
      ok: hqv.ok,
      reason: hqv.reason,
      blocked: hqv.reason === "akamai_or_bot_blocked",
    });
    if (hqv.reason === "akamai_or_bot_blocked") result.blocked = true;
    if (hqv.ok) {
      result.candidates.coordinates = {
        latitude: hqv.lat,
        longitude: hqv.lng,
        source_url: hqv.source_url,
        source_type: "official_catalog_api",
        confidence: "High",
        evidence_tier: "official_marriott_hqv",
        coordinate_source_type: "official_api",
        geocode_provider: "Marriott HQV GraphQL",
        geocode_method: hqv.method,
      };
    }
  }

  return result;
}

/**
 * Build High Census patch from metadata candidates (field-completion only).
 * Requires source_url on every write.
 */
export function buildMarriottMetadataPatch(extraction, existingFields = {}) {
  /** @type {Record<string, unknown>} */
  const patch = {};
  const f = existingFields;
  const c = extraction?.candidates || {};
  const today = new Date().toISOString().slice(0, 10);

  if (c.address?.value && c.address.source_url && !isStreetLevelAddress(f.Address || "")) {
    patch.Address = c.address.value;
    patch["Address Confidence"] = "High";
    patch["Address Source URL"] = c.address.source_url;
    if (c.address.state && isBlank(f["State / Region"])) {
      patch["State / Region"] = c.address.state;
    }
  }
  if (c.phone?.value && c.phone.source_url && isBlank(f.Phone)) {
    patch.Phone = c.phone.value;
  }
  if (c.rooms?.value != null && c.rooms.source_url && isBlank(f["Rooms / Keys"])) {
    patch["Rooms / Keys"] = c.rooms.value;
    patch["Rooms Confidence"] = "High";
    patch["Rooms Source URL"] = c.rooms.source_url;
    patch["Rooms Source Type"] = c.rooms.source_type || "official_property_page";
    patch["Rooms Reviewed Date"] = today;
  }
  // Coordinates only when High Address already present or being written
  const willHaveHighAddress =
    isStreetLevelAddress(f.Address || "") || Boolean(patch.Address);
  const addrHigh =
    String(f["Address Confidence"] || "").toLowerCase() === "high" ||
    Boolean(patch["Address Confidence"]);
  if (
    c.coordinates &&
    willHaveHighAddress &&
    addrHigh &&
    (f.Latitude == null || f.Longitude == null)
  ) {
    patch.Latitude = c.coordinates.latitude;
    patch.Longitude = c.coordinates.longitude;
    patch["Coordinate Source Type"] = c.coordinates.coordinate_source_type;
    patch["Coordinate Confidence"] = "High";
    patch["Geocode Provider"] = c.coordinates.geocode_provider;
    patch["Geocode Method"] = c.coordinates.geocode_method;
    patch["Geocode Reviewed Date"] = today;
  }

  // Every field write must have an underlying source URL somewhere in patch provenance
  for (const key of Object.keys(patch)) {
    if (
      ["Address", "Phone", "Rooms / Keys", "Latitude"].includes(key) &&
      !patch["Address Source URL"] &&
      !patch["Rooms Source URL"] &&
      !c.coordinates?.source_url &&
      !c.phone?.source_url
    ) {
      // safety — drop if no provenance
      if (key === "Phone" && !c.phone?.source_url) delete patch.Phone;
    }
  }

  return {
    patch,
    has_writes: Object.keys(patch).length > 0,
    blocked: Boolean(extraction?.blocked),
    webhound_as_sot: false,
  };
}
