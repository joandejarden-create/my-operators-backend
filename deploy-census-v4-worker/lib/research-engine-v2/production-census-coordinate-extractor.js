/**
 * Production-safe coordinate extraction from official property HTML.
 * No Webhound. No city-only geocodes. Used by Census coordinate resolver.
 */

/**
 * @typedef {{ lat: number, lng: number, method: string, confidence: 'High'|'Medium'|'Low', address?: string|null }} CoordHit
 */

const JSON_LD_RE = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;

/**
 * Known junk pins only — NOT downtown/airport hotel locations.
 * Airport hotels and Centro hotels legitimately sit near airports / historic plazas.
 * Use exact/near-exact match for null-island; coarse city tourism pins only when
 * coordinate precision looks city-level (<=3 decimal places) AND name lacks hotel locality cues.
 */
export const MEXICO_REJECT_PINS = Object.freeze([
  { lat: 0, lng: 0, label: "null island", mode: "exact" },
]);

/** Coarse tourism centroids — only flag low-precision coords without locality cues in the name. */
export const MEXICO_COARSE_CITY_PINS = Object.freeze([
  { lat: 19.4326, lng: -99.1332, label: "Mexico City tourism centroid", tol: 0.008 },
  { lat: 21.1619, lng: -86.8515, label: "Cancun tourism centroid", tol: 0.008 },
  { lat: 20.6809, lng: -105.2542, label: "Puerto Vallarta tourism centroid", tol: 0.008 },
  { lat: 25.6866, lng: -100.3161, label: "Monterrey tourism centroid", tol: 0.008 },
  { lat: 20.6597, lng: -103.3496, label: "Guadalajara tourism centroid", tol: 0.008 },
]);

function decimalPlaces(n) {
  const s = String(n);
  const i = s.indexOf(".");
  return i < 0 ? 0 : s.length - i - 1;
}

export function isValidCoordPair(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
}

function almostEqual(a, b, tol = 0.02) {
  return Math.abs(a - b) <= tol;
}

export function matchesRejectedPin(lat, lng, opts = {}) {
  if (lat === 0 && lng === 0) {
    return { lat: 0, lng: 0, label: "null island" };
  }
  for (const pin of MEXICO_REJECT_PINS) {
    if (pin.mode === "exact" && lat === pin.lat && lng === pin.lng) return pin;
  }
  const name = String(opts.propertyName || "").toLowerCase();
  const localityCue =
    /airport|aeropuerto|centro|reforma|polanco|santa fe|zona hotelera|hotel zone|marina|beach|playa/i.test(
      name
    );
  if (localityCue) return null;
  // Only flag coarse (low precision) pins that collapse to tourism centroids
  if (decimalPlaces(lat) <= 3 && decimalPlaces(lng) <= 3) {
    for (const pin of MEXICO_COARSE_CITY_PINS) {
      if (almostEqual(lat, pin.lat, pin.tol) && almostEqual(lng, pin.lng, pin.tol)) {
        return pin;
      }
    }
  }
  return null;
}

function pushHit(hits, lat, lng, method, confidence, address = null, propertyName = "") {
  if (!isValidCoordPair(lat, lng)) return;
  const rejected = matchesRejectedPin(lat, lng, { propertyName });
  if (rejected) {
    hits.push({
      lat,
      lng,
      method,
      confidence: "Low",
      address,
      rejected: true,
      reject_reason: rejected.label,
    });
    return;
  }
  hits.push({ lat, lng, method, confidence, address, rejected: false });
}

function parseJsonSafe(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function walkJsonLd(node, hits) {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (const item of node) walkJsonLd(item, hits);
    return;
  }
  const types = []
    .concat(node["@type"] || [])
    .map((t) => String(t).toLowerCase());
  const isLodging =
    types.some((t) =>
      /hotel|lodgingbusiness|resort|motel|bedandbreakfast|place/i.test(t)
    ) || Boolean(node.geo || node.address);

  const geo = node.geo;
  if (geo && (geo.latitude != null || geo.lat != null)) {
    const lat = Number(geo.latitude ?? geo.lat);
    const lng = Number(geo.longitude ?? geo.lng ?? geo.lon);
    const conf = isLodging ? "High" : "Medium";
    pushHit(hits, lat, lng, "json_ld_geo", conf, formatAddress(node.address));
  }
  if (node.latitude != null && node.longitude != null) {
    pushHit(
      hits,
      Number(node.latitude),
      Number(node.longitude),
      "json_ld_latlng_fields",
      isLodging ? "High" : "Medium",
      formatAddress(node.address)
    );
  }
  for (const v of Object.values(node)) {
    if (v && typeof v === "object") walkJsonLd(v, hits);
  }
}

function formatAddress(addr) {
  if (!addr) return null;
  if (typeof addr === "string") return addr.trim() || null;
  if (typeof addr === "object") {
    const parts = [
      addr.streetAddress,
      addr.addressLocality,
      addr.addressRegion,
      addr.postalCode,
      addr.addressCountry,
    ]
      .map((x) => (x == null ? "" : String(x).trim()))
      .filter(Boolean);
    return parts.length ? parts.join(", ") : null;
  }
  return null;
}

/**
 * Extract official address candidates from HTML (for optional geocode path).
 * @param {string} html
 */
export function extractOfficialAddressCandidates(html) {
  const text = String(html || "");
  const out = [];
  const street =
    text.match(/"streetAddress"\s*:\s*"((?:\\.|[^"\\])*)"/i) ||
    text.match(/itemprop=["']streetAddress["'][^>]*content=["']([^"']+)["']/i) ||
    text.match(/itemprop=["']streetAddress["'][^>]*>([^<]+)</i);
  const locality =
    text.match(/"addressLocality"\s*:\s*"((?:\\.|[^"\\])*)"/i) ||
    text.match(/itemprop=["']addressLocality["'][^>]*>([^<]+)</i);
  const region = text.match(/"addressRegion"\s*:\s*"((?:\\.|[^"\\])*)"/i);
  const postal = text.match(/"postalCode"\s*:\s*"((?:\\.|[^"\\])*)"/i);
  const country = text.match(/"addressCountry"\s*:\s*"((?:\\.|[^"\\])*)"/i);
  if (street) {
    const parts = [
      unescapeJson(street[1]),
      locality ? unescapeJson(locality[1]) : null,
      region ? unescapeJson(region[1]) : null,
      postal ? unescapeJson(postal[1]) : null,
      country ? unescapeJson(country[1]) : null,
    ].filter(Boolean);
    out.push({ address: parts.join(", "), method: "schema_address", confidence: "High" });
  }
  return out;
}

function unescapeJson(s) {
  return String(s || "")
    .replace(/\\u([0-9a-fA-F]{4})/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/\\"/g, '"')
    .replace(/\\\\/g, "\\");
}

/**
 * Family-aware HTML coordinate extraction.
 * @param {string} html
 * @param {{ url?: string, family?: string }} [opts]
 * @returns {{ hits: object[], addresses: object[], patterns_matched: string[] }}
 */
export function extractCoordinatesFromOfficialHtml(html, opts = {}) {
  const text = String(html || "");
  const family = String(opts.family || "").toLowerCase();
  const url = String(opts.url || "");
  /** @type {object[]} */
  const hits = [];
  const patterns_matched = [];

  // 1) JSON-LD
  let m;
  const re = new RegExp(JSON_LD_RE.source, "gi");
  while ((m = re.exec(text))) {
    const parsed = parseJsonSafe(m[1].trim());
    if (!parsed) continue;
    const before = hits.length;
    walkJsonLd(parsed, hits);
    if (hits.length > before) patterns_matched.push("json_ld");
  }

  // 2) Marriott / __NEXT_DATA__ / GraphQL-ish payloads
  const marriottPatterns = [
    [/"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)/i, "marriott_lat_lng_pair"],
    [/"hotelLatitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"hotelLongitude"\s*:\s*(-?\d+\.?\d*)/i, "marriott_hotelLatitude"],
    [/"lat"\s*:\s*(-?\d+\.?\d*)\s*,\s*"lng"\s*:\s*(-?\d+\.?\d*)/i, "marriott_lat_lng_short"],
    [/"geo"\s*:\s*\{\s*"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)/i, "marriott_geo_object"],
    [/\\"latitude\\"\s*:\s*(-?\d+\.?\d*)\s*,\s*\\"longitude\\"\s*:\s*(-?\d+\.?\d*)/i, "marriott_escaped_json"],
  ];
  if (family === "marriott" || /marriott\.com/i.test(url)) {
    for (const [rx, method] of marriottPatterns) {
      const mm = text.match(rx);
      if (mm) {
        pushHit(hits, Number(mm[1]), Number(mm[2]), method, "High");
        patterns_matched.push(method);
      }
    }
  }

  // 3) Hilton property page / GraphQL localization
  if (family === "hilton" || /hilton\.com/i.test(url)) {
    const h =
      text.match(
        /"coordinate"\s*:\s*\{\s*"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)/i
      ) ||
      text.match(
        /"localization"\s*:\s*\{[^}]{0,400}?"coordinate"\s*:\s*\{\s*"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)/i
      );
    if (h) {
      pushHit(hits, Number(h[1]), Number(h[2]), "hilton_localization_coordinate", "High");
      patterns_matched.push("hilton_localization_coordinate");
    }
  }

  // 4) Choice geoLocation
  if (family === "choice" || /choicehotels\.com/i.test(url)) {
    const c = text.match(
      /"geoLocation"\s*:\s*\{\s*"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)/i
    );
    if (c) {
      pushHit(hits, Number(c[1]), Number(c[2]), "choice_geoLocation", "High");
      patterns_matched.push("choice_geoLocation");
    }
  }

  // 5) IHG hoteldetail / map
  if (family === "ihg" || /ihg\.com/i.test(url)) {
    const i =
      text.match(/"latitude"\s*:\s*"?(-?\d+\.?\d*)"?\s*,\s*"longitude"\s*:\s*"?(-?\d+\.?\d*)"?/i) ||
      text.match(/lat["']?\s*[:=]\s*(-?\d+\.?\d*)\s*.{0,40}?lng["']?\s*[:=]\s*(-?\d+\.?\d*)/i);
    if (i) {
      pushHit(hits, Number(i[1]), Number(i[2]), "ihg_hoteldetail_latlng", "High");
      patterns_matched.push("ihg_hoteldetail_latlng");
    }
  }

  // 6) Generic embedded map / meta (Medium — secondary)
  const generic = [
    [/content=["'](-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)["'][^>]*(?:geo|icbm)/i, "meta_geo_icbm"],
    [/@(-?\d+\.\d+),(-?\d+\.\d+)/, "google_maps_at_pin"],
    [/maps\.google[^"']*[?&]q=(-?\d+\.\d+),(-?\d+\.\d+)/i, "google_maps_q_param"],
    [/["']center["']\s*:\s*\{\s*["']lat["']\s*:\s*(-?\d+\.?\d*)\s*,\s*["']lng["']\s*:\s*(-?\d+\.?\d*)/i, "map_center_object"],
  ];
  for (const [rx, method] of generic) {
    const g = text.match(rx);
    if (g) {
      pushHit(hits, Number(g[1]), Number(g[2]), method, "Medium");
      patterns_matched.push(method);
    }
  }

  const addresses = extractOfficialAddressCandidates(text);

  // Prefer High non-rejected
  hits.sort((a, b) => {
    const rank = { High: 3, Medium: 2, Low: 1 };
    const ar = a.rejected ? 0 : rank[a.confidence] || 0;
    const br = b.rejected ? 0 : rank[b.confidence] || 0;
    return br - ar;
  });

  return { hits, addresses, patterns_matched: [...new Set(patterns_matched)] };
}

/**
 * Pick best writable coordinate from extraction hits.
 * @param {object[]} hits
 */
export function selectBestCoordinateHit(hits) {
  for (const h of hits || []) {
    if (h.rejected) continue;
    if (h.confidence === "Low") continue;
    if (!isValidCoordPair(h.lat, h.lng)) continue;
    return h;
  }
  return null;
}

/**
 * Optional Google Geocoding of official property name + full address only.
 * Never city-only / brand-only / country-only.
 * @param {{ name: string, address: string, city?: string, country?: string }} input
 */
export async function geocodeOfficialAddressOnly(input) {
  const key = String(process.env.GOOGLE_MAPS_API_KEY || "").trim();
  if (!key) {
    return { ok: false, reason: "GOOGLE_MAPS_API_KEY_missing" };
  }
  const name = String(input.name || "").trim();
  const address = String(input.address || "").trim();
  const city = String(input.city || "").trim();
  const country = String(input.country || "").trim();
  if (!name || !address) {
    return { ok: false, reason: "missing_name_or_address" };
  }
  // Reject city-only / brand-only payloads
  if (address.length < 12 || !/\d/.test(address)) {
    return { ok: false, reason: "address_not_street_level" };
  }
  const query = `${name}, ${address}${city ? `, ${city}` : ""}${country ? `, ${country}` : ""}`;
  const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(query)}&key=${encodeURIComponent(key)}`;
  const res = await fetch(url);
  const json = await res.json();
  if (!res.ok || json.status !== "OK" || !json.results?.[0]) {
    return { ok: false, reason: `geocode_${json.status || res.status}`, query };
  }
  const loc = json.results[0].geometry?.location;
  const lat = Number(loc?.lat);
  const lng = Number(loc?.lng);
  if (!isValidCoordPair(lat, lng)) {
    return { ok: false, reason: "geocode_invalid_coords" };
  }
  const rejected = matchesRejectedPin(lat, lng, { propertyName: name });
  if (rejected) {
    return { ok: false, reason: "geocode_matches_rejected_pin", pin: rejected.label };
  }
  const locType = json.results[0].geometry?.location_type;
  if (locType === "APPROXIMATE") {
    return { ok: false, reason: "geocode_approximate_only", lat, lng };
  }
  return {
    ok: true,
    lat,
    lng,
    method: "google_geocode_official_address",
    confidence: locType === "ROOFTOP" ? "High" : "Medium",
    query,
    formatted_address: json.results[0].formatted_address || null,
    location_type: locType || null,
  };
}

/** Durable crawler rules derived from sidecar learning (code-reproducible). */
export const COORDINATE_CRAWLER_RULES = Object.freeze([
  {
    family: "Marriott",
    page_types: [
      "mexico hotel sitemap (MARSHA seed)",
      "GraphQL phoenixShopHQVPropertyInfoCall (preferred)",
      "overview HTML (usually negative for coords)",
    ],
    patterns: [
      "data.property.basicInformation.latitude/longitude via HQV",
      "sitemap /hotels/([A-Z0-9]{5})- MARSHA",
      "__NEXT_DATA__.props.pageProps.operationSignatures[] → MARRIOTT_GRAPHQL_OPERATION_SIGNATURE",
      "optional JSON-LD geo (rarely present on Mexico overview)",
    ],
    reproducible_without_webhound: true,
    becomes_crawler_rule: true,
    steward_if: "Akamai blocks HQV or signature missing",
    learning_source: "webhound_sidecar_bbaa85f9",
    env_required: ["MARRIOTT_GRAPHQL_OPERATION_SIGNATURE (optional but usually required)"],
  },
  {
    family: "Hilton",
    page_types: ["hilton.com/en/hotels/{ctyhocn}-.../", "locations directory GraphQL"],
    patterns: ["localization.coordinate.latitude/longitude", "JSON-LD geo"],
    reproducible_without_webhound: true,
    becomes_crawler_rule: true,
  },
  {
    family: "Choice",
    page_types: ["choicehotels.com regional hotel cards", "property pages"],
    patterns: ['"geoLocation":{"latitude":n,"longitude":n}'],
    reproducible_without_webhound: true,
    becomes_crawler_rule: true,
  },
  {
    family: "IHG",
    page_types: ["ihg.com/.../hoteldetail"],
    patterns: ["hoteldetail latitude/longitude JSON", "JSON-LD geo", "map embed"],
    reproducible_without_webhound: true,
    becomes_crawler_rule: true,
  },
]);
