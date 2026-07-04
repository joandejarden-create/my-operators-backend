/**
 * OpenStreetMap / Overpass source for independent hotel census (Phase 2B+ / 2D).
 *
 * DRY-RUN ONLY: fetches and normalizes OSM elements. Does not write to Airtable.
 * License: OpenStreetMap data © OSM contributors — ODbL (https://www.openstreetmap.org/copyright)
 */

import { SOURCE_TYPES } from "../fields.js";
import {
  buildIndependentCandidate,
  normalizeText,
  normalizeKey,
  computeCandidateDedupeKey,
  analyzeMissingFields,
  computeQualityScore,
} from "../normalize-candidate.js";
import { getSourceProfile } from "../source-registry.js";

export const OSM_SOURCE_NAME = "OpenStreetMap";
export const OSM_SOURCE_LICENSE = "ODbL";
export const OSM_COPYRIGHT = "© OpenStreetMap contributors";

export { normalizeText, normalizeKey, computeCandidateDedupeKey, analyzeMissingFields };

/** Default safety cap when --max-elements is omitted (Overpass output size). */
export const DEFAULT_MAX_ELEMENTS = 10000;

/** Hotel-focused lodging tags (excludes apartment). */
export const OSM_HOTEL_FOCUSED_TOURISM = [
  "hotel",
  "resort",
  "guest_house",
  "hostel",
  "motel",
];

/** All lodging tags including apartments (broader discovery). */
export const OSM_TOURISM_TAGS = [...OSM_HOTEL_FOCUSED_TOURISM, "apartment"];

const DEFAULT_OVERPASS_URL =
  process.env.OVERPASS_API_URL || "https://overpass-api.de/api/interpreter";

const PAYLOAD_TAG_KEYS = new Set([
  "tourism",
  "name",
  "official_name",
  "brand",
  "operator",
  "network",
  "website",
  "contact:website",
  "url",
  "phone",
  "contact:phone",
  "contact:mobile",
  "email",
  "contact:email",
  "stars",
  "rooms",
  "opening_hours",
  "is_in",
  "is_in:city",
  "is_in:town",
  "place",
]);

const CITY_TAG_PRIORITY = [
  "addr:city",
  "addr:town",
  "addr:village",
  "addr:municipality",
  "is_in:city",
  "is_in:town",
  "addr:suburb",
  "addr:place",
  "addr:district",
  "addr:county",
];

const COUNTRY_ISO2 = {
  "dominican republic": "DO",
  mexico: "MX",
  jamaica: "JM",
  "costa rica": "CR",
  "puerto rico": "PR",
  "united states": "US",
  "united states of america": "US",
  canada: "CA",
  brazil: "BR",
  colombia: "CO",
  chile: "CL",
  panama: "PA",
  "trinidad and tobago": "TT",
  ecuador: "EC",
  argentina: "AR",
  bahamas: "BS",
  honduras: "HN",
  peru: "PE",
  belize: "BZ",
  guatemala: "GT",
  nicaragua: "NI",
  "el salvador": "SV",
  uruguay: "UY",
  paraguay: "PY",
  bolivia: "BO",
  venezuela: "VE",
  aruba: "AW",
  barbados: "BB",
};

function tourismRegexForTags(tags) {
  return tags.join("|");
}

function escapeOverpassString(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function countryIso2(countryName) {
  return COUNTRY_ISO2[normalizeKey(countryName)] || "";
}

/**
 * @param {string} bbox - south,west,north,east
 */
export function parseBbox(bbox) {
  const parts = String(bbox)
    .split(",")
    .map((p) => Number(p.trim()));
  if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n))) {
    throw new Error(`Invalid --bbox "${bbox}". Expected south,west,north,east`);
  }
  const [south, west, north, east] = parts;
  if (south >= north || west >= east) {
    throw new Error(`Invalid --bbox: south must be < north and west < east`);
  }
  return { south, west, north, east };
}

function overpassTimeoutSec(opts) {
  if (opts.bbox) return 180;
  if (opts.city) return 180;
  return opts.maxElements && opts.maxElements > 1500 ? 540 : 360;
}

/**
 * @param {{ country?: string, city?: string, bbox?: string, maxElements?: number|null, hotelFocused?: boolean, tourismTags?: string[] }} opts
 */
/**
 * Hotel-focused Overpass around a point (targeted recovery).
 */
export function parseOsmSourceRecordId(sourceRecordId) {
  const m = String(sourceRecordId || "").match(/^(node|way|relation)\/(\d+)$/i);
  if (!m) return null;
  return { type: m[1].toLowerCase(), id: m[2] };
}

export function buildOverpassFetchByIdQuery(sourceRecordId) {
  const parsed = parseOsmSourceRecordId(sourceRecordId);
  if (!parsed) {
    throw new Error(`Invalid OSM source record id: ${sourceRecordId}`);
  }
  const timeout = 45;
  if (parsed.type === "relation") {
    return `[out:json][timeout:${timeout}];
relation(${parsed.id});
out center tags;`;
  }
  return `[out:json][timeout:${timeout}];
${parsed.type}(${parsed.id});
out center tags;`;
}

export async function fetchOsmElementBySourceRecordId(sourceRecordId, opts = {}) {
  const query = buildOverpassFetchByIdQuery(sourceRecordId);
  const elements = await fetchOverpassElements(query, opts);
  if (!elements.length) return null;
  return elements[0];
}

export function buildOverpassAroundQuery(opts) {
  const lat = Number(opts.lat);
  const lng = Number(opts.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    throw new Error("buildOverpassAroundQuery requires finite lat/lng");
  }
  const radiusMeters = Math.min(Math.max(Number(opts.radiusMeters) || 500, 50), 2000);
  const maxElements = Math.min(Math.max(Number(opts.maxElements) || 40, 5), 100);
  const tags = opts.tourismTags || OSM_HOTEL_FOCUSED_TOURISM;
  const tourismFilter = `["tourism"~"^(${tourismRegexForTags(tags)})$"]`;
  const timeout = opts.timeoutSec || 90;
  return `[out:json][timeout:${timeout}];
(
  node${tourismFilter}(around:${radiusMeters},${lat},${lng});
  way${tourismFilter}(around:${radiusMeters},${lat},${lng});
);
out center ${maxElements} tags;`;
}

export function buildOverpassQuery(opts) {
  const tags =
    opts.tourismTags ||
    (opts.hotelFocused ? OSM_HOTEL_FOCUSED_TOURISM : OSM_TOURISM_TAGS);
  const tourismFilter = `["tourism"~"^(${tourismRegexForTags(tags)})$"]`;
  const timeout = overpassTimeoutSec(opts);

  let outClause = "out center tags;";
  if (opts.maxElements != null && opts.maxElements > 0) {
    outClause = `out center ${Math.floor(opts.maxElements)} tags;`;
  }

  const { country, city, bbox } = opts;

  if (bbox) {
    const { south, west, north, east } = parseBbox(bbox);
    return `[out:json][timeout:${timeout}];
(
  node${tourismFilter}(${south},${west},${north},${east});
  way${tourismFilter}(${south},${west},${north},${east});
  relation${tourismFilter}(${south},${west},${north},${east});
);
${outClause}`;
  }

  const countryEsc = escapeOverpassString(country || "Dominican Republic");
  const iso = countryIso2(country || "Dominican Republic");

  if (city) {
    const cityEsc = escapeOverpassString(city);
    const isoLine = iso
      ? `area["ISO3166-1"="${iso}"]["admin_level"="2"]->.country;`
      : `area["name"="${countryEsc}"]["admin_level"~"^[2-8]$"]->.country;`;
    return `[out:json][timeout:${timeout}];
${isoLine}
area["name"="${cityEsc}"](area.country)->.searchArea;
(
  node${tourismFilter}(area.searchArea);
  way${tourismFilter}(area.searchArea);
  relation${tourismFilter}(area.searchArea);
);
${outClause}`;
  }

  if (iso) {
    return `[out:json][timeout:${timeout}];
area["ISO3166-1"="${iso}"]["admin_level"="2"]->.searchArea;
(
  node${tourismFilter}(area.searchArea);
  way${tourismFilter}(area.searchArea);
  relation${tourismFilter}(area.searchArea);
);
${outClause}`;
  }

  return `[out:json][timeout:${timeout}];
area["name"="${countryEsc}"]["admin_level"~"^[2-8]$"]->.searchArea;
(
  node${tourismFilter}(area.searchArea);
  way${tourismFilter}(area.searchArea);
  relation${tourismFilter}(area.searchArea);
);
${outClause}`;
}

export function osmRecordId(type, id) {
  return `${type}/${id}`;
}

export function osmSourceUrl(type, id) {
  return `https://www.openstreetmap.org/${type}/${id}`;
}

export function elementCoordinates(el) {
  if (el.type === "node" && el.lat != null && el.lon != null) {
    return { lat: Number(el.lat), lng: Number(el.lon) };
  }
  if (el.center?.lat != null && el.center?.lon != null) {
    return { lat: Number(el.center.lat), lng: Number(el.center.lon) };
  }
  if (el.lat != null && el.lon != null) {
    return { lat: Number(el.lat), lng: Number(el.lon) };
  }
  return { lat: null, lng: null };
}

function pickTag(tags, keys) {
  for (const k of keys) {
    const v = tags?.[k];
    if (v != null && String(v).trim() !== "") return normalizeText(v);
  }
  return "";
}

function parseIsInCity(isIn) {
  if (!isIn) return "";
  return isIn.split(",")[0]?.trim() || "";
}

export function extractCity(tags) {
  const direct = pickTag(tags, CITY_TAG_PRIORITY);
  if (direct) return direct;
  return parseIsInCity(pickTag(tags, ["is_in"]));
}

export function buildAddress(tags) {
  const line1 = [
    pickTag(tags, ["addr:housenumber"]),
    pickTag(tags, ["addr:street", "addr:place"]),
  ]
    .filter(Boolean)
    .join(" ");

  const line2 = [
    pickTag(tags, ["addr:neighbourhood", "addr:suburb"]),
    extractCity(tags),
    pickTag(tags, ["addr:postcode"]),
    pickTag(tags, ["addr:country"]),
  ].filter(Boolean);

  const parts = [line1, line2.join(", ")].filter(Boolean);
  const joined = parts.join(", ");
  if (joined) return joined;
  return pickTag(tags, ["address"]);
}

function operatorLooksLikeBrand(operator) {
  const o = normalizeKey(operator);
  if (!o || o.length < 2) return false;
  if (["yes", "no", "true", "false", "private", "independent"].includes(o)) {
    return false;
  }
  return true;
}

export function extractNameAndBrand(tags) {
  const brand = pickTag(tags, ["brand"]);
  const operator = pickTag(tags, ["operator"]);
  const network = pickTag(tags, ["network"]);
  const tourism = pickTag(tags, ["tourism"]);

  let rawHotelName = pickTag(tags, [
    "name",
    "official_name",
    "name:en",
    "name:es",
    "name:fr",
  ]);

  if (!rawHotelName && brand && tourism) {
    rawHotelName = `${brand} (${tourism.replace(/_/g, " ")})`;
  } else if (!rawHotelName && brand) {
    rawHotelName = brand;
  } else if (!rawHotelName && operatorLooksLikeBrand(operator)) {
    rawHotelName = operator;
  }

  let rawBrand = brand;
  if (!rawBrand && operatorLooksLikeBrand(operator)) rawBrand = operator;
  if (!rawBrand && network) rawBrand = network;

  return { rawHotelName, rawBrand };
}

export function buildPayloadSnapshot(el, tags, coords) {
  const { type, id } = el;
  const payload = { type, id, tourism: tags.tourism ?? null };
  if (coords.lat != null && coords.lng != null) {
    payload.lat = coords.lat;
    payload.lng = coords.lng;
  }
  if (type !== "node" && el.center) {
    payload.center = { lat: el.center.lat, lon: el.center.lon };
  }
  for (const [key, value] of Object.entries(tags || {})) {
    if (key.startsWith("addr:") || PAYLOAD_TAG_KEYS.has(key)) {
      payload[key] = value;
    }
  }
  return payload;
}

/**
 * Normalize OSM element via shared buildIndependentCandidate.
 */
export function normalizeOsmElement(el, ctx) {
  const tags = el.tags || {};
  const coords = elementCoordinates(el);
  const { lat, lng } = coords;
  const type = el.type;
  const id = el.id;
  const tourism = pickTag(tags, ["tourism"]) || "unknown";
  const { rawHotelName, rawBrand } = extractNameAndBrand(tags);
  const profile = getSourceProfile(SOURCE_TYPES.OSM);

  return buildIndependentCandidate({
    sourceName: profile?.sourceName || OSM_SOURCE_NAME,
    sourceType: SOURCE_TYPES.OSM,
    sourceLicense: profile?.sourceLicense || OSM_SOURCE_LICENSE,
    sourceUrl: osmSourceUrl(type, id),
    sourceRecordId: osmRecordId(type, id),
    rawHotelName,
    rawAddress: buildAddress(tags),
    rawCity: extractCity(tags),
    rawCountry: pickTag(tags, ["addr:country"]) || normalizeText(ctx.defaultCountry || ""),
    rawLatitude: lat,
    rawLongitude: lng,
    rawWebsite: pickTag(tags, ["website", "contact:website", "url"]),
    rawPhone: pickTag(tags, ["phone", "contact:phone", "contact:mobile", "contact:fax"]),
    rawBrand,
    rawPayload: buildPayloadSnapshot(el, tags, { lat, lng }),
    importBatchId: ctx.batchId,
    importedAt: ctx.importedAt,
    internalMeta: {
      _osmElementType: type,
      _osmTourismTag: tourism,
      _hasRoomsTag: tags.rooms != null && String(tags.rooms).trim() !== "",
      _hasStarsTag: tags.stars != null && String(tags.stars).trim() !== "",
      _hasBrandOrOperator:
        !!(tags.brand || tags.operator || tags.network) &&
        !!(rawBrand || operatorLooksLikeBrand(tags.operator)),
    },
  });
}

/**
 * Post-normalize filters for hotel-focused runs (report-only).
 * @param {Array<object>} candidates
 * @param {{ hotelFocused?: boolean, includeApartments?: boolean, includeUnnamed?: boolean, minQuality?: number }} opts
 */
export function filterOsmCandidates(candidates, opts = {}) {
  const hotelFocused = !!opts.hotelFocused;
  const includeApartments = !!opts.includeApartments;
  const includeUnnamed = !!opts.includeUnnamed;
  const minQuality = Number(opts.minQuality) || 0;

  const kept = [];
  const excluded = {
    apartments: 0,
    unnamed: 0,
    lowQuality: 0,
    tourismExcluded: 0,
  };

  for (const c of candidates) {
    const tourism = c._osmTourismTag || "";

    if (hotelFocused && tourism === "apartment" && !includeApartments) {
      excluded.apartments++;
      continue;
    }

    if (hotelFocused && !OSM_HOTEL_FOCUSED_TOURISM.includes(tourism)) {
      excluded.tourismExcluded++;
      continue;
    }

    if (!includeUnnamed && !normalizeKey(c.rawHotelName)) {
      excluded.unnamed++;
      continue;
    }

    const q = c.qualityScore ?? computeQualityScore(c);
    if (minQuality > 0 && q < minQuality) {
      excluded.lowQuality++;
      continue;
    }

    kept.push(c);
  }

  return {
    candidates: kept,
    excluded,
    beforeCount: candidates.length,
    afterCount: kept.length,
  };
}

export async function fetchOverpassElements(query, options = {}) {
  const url = options.overpassUrl || DEFAULT_OVERPASS_URL;
  const fetchFn = options.fetchFn || globalThis.fetch;
  if (!fetchFn) throw new Error("fetch is not available in this runtime");

  const res = await fetchFn(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
      "User-Agent": "DealalityIndependentCensus/1.0 (dry-run; contact: ops@dealality.local)",
    },
    body: `data=${encodeURIComponent(query)}`,
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Overpass HTTP ${res.status}: ${text.slice(0, 500)}`);
  }

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Overpass returned non-JSON: ${text.slice(0, 300)}`);
  }

  if (json.remark && /runtime|error|timeout/i.test(json.remark)) {
    throw new Error(`Overpass remark: ${json.remark}`);
  }

  return json.elements || [];
}

/**
 * @param {{
 *   country?: string,
 *   city?: string,
 *   bbox?: string,
 *   maxElements?: number|null,
 *   limit?: number|null,
 *   batchId: string,
 *   hotelFocused?: boolean,
 *   includeApartments?: boolean,
 *   includeUnnamed?: boolean,
 *   minQuality?: number,
 * }} opts
 */
export async function fetchOsmHotelCandidates(opts) {
  const maxElements = opts.maxElements === undefined ? null : opts.maxElements;
  const hotelFocused = !!opts.hotelFocused;
  const tourismTags = hotelFocused && !opts.includeApartments
    ? OSM_HOTEL_FOCUSED_TOURISM
    : OSM_TOURISM_TAGS;

  const query = buildOverpassQuery({ ...opts, maxElements, hotelFocused, tourismTags });
  const rawElements = await fetchOverpassElements(query);
  const importedAt = new Date().toISOString();
  const defaultCountry = opts.country || "Dominican Republic";

  const overpassCapped =
    maxElements != null && maxElements > 0 && rawElements.length >= maxElements;

  const elements = rawElements.filter((el) => {
    const t = String(el.tags?.tourism || "").trim();
    return t && tourismTags.includes(t);
  });

  const allCandidates = elements.map((el) =>
    normalizeOsmElement(el, {
      batchId: opts.batchId,
      defaultCountry,
      importedAt,
    })
  );

  const filterResult = filterOsmCandidates(allCandidates, {
    hotelFocused,
    includeApartments: opts.includeApartments,
    includeUnnamed: opts.includeUnnamed,
    minQuality: opts.minQuality,
  });

  let candidates = filterResult.candidates;
  const beforeCandidateLimit = candidates.length;
  let candidateLimitCapped = false;
  if (opts.limit != null && opts.limit > 0 && candidates.length > opts.limit) {
    candidates = candidates.slice(0, opts.limit);
    candidateLimitCapped = true;
  }

  return {
    query,
    elements,
    rawElementCount: rawElements.length,
    allCandidates,
    candidates,
    filtering: filterResult,
    capping: {
      overpassMaxElements: maxElements,
      overpassReturned: rawElements.length,
      overpassCapped,
      candidateLimit: opts.limit ?? null,
      candidatesBeforeLimit: beforeCandidateLimit,
      candidateLimitCapped,
      uncappedOverpass: maxElements == null || maxElements === 0,
      uncappedCandidates: opts.limit == null || opts.limit === 0,
      hotelFocused,
      tourismTagsQueried: tourismTags,
    },
  };
}

function incrementCount(map, key) {
  const k = key || "(blank)";
  map.set(k, (map.get(k) || 0) + 1);
}

function countPresent(candidates, pred) {
  return candidates.filter(pred).length;
}

/**
 * Extended summary for Phase 2D reports.
 */
export function summarizeCandidates(candidates, extras = {}) {
  const byCountry = new Map();
  const byCity = new Map();
  const byTourismTag = new Map();
  const byElementType = new Map();
  const byMissingField = new Map();
  const byQualityTier = new Map();
  const dedupeKeyCounts = new Map();

  for (const c of candidates) {
    incrementCount(byCountry, c.rawCountry);
    incrementCount(byCity, c.rawCity);
    incrementCount(byTourismTag, c._osmTourismTag);
    incrementCount(byElementType, c._osmElementType);
    incrementCount(byQualityTier, c.qualityTier || "unknown");
    for (const m of c.missingFields || c._missingFields || []) {
      incrementCount(byMissingField, m);
    }
    const dk = c.candidateDedupeKey || "(blank)";
    dedupeKeyCounts.set(dk, (dedupeKeyCounts.get(dk) || 0) + 1);
  }

  const duplicateGroups = [...dedupeKeyCounts.entries()]
    .filter(([, n]) => n > 1)
    .sort((a, b) => b[1] - a[1]);

  const toSortedObj = (map) =>
    Object.fromEntries([...map.entries()].sort((a, b) => b[1] - a[1]));

  const total = candidates.length;

  return {
    byCountry: toSortedObj(byCountry),
    byCity: toSortedObj(byCity),
    byTourismTag: toSortedObj(byTourismTag),
    byElementType: toSortedObj(byElementType),
    byMissingField: toSortedObj(byMissingField),
    byQualityTier: toSortedObj(byQualityTier),
    coverage: {
      total,
      withCity: countPresent(candidates, (c) => normalizeKey(c.rawCity)),
      withWebsite: countPresent(candidates, (c) => normalizeKey(c.rawWebsite)),
      withPhone: countPresent(candidates, (c) => normalizeKey(c.rawPhone)),
      withCoordinates: countPresent(
        candidates,
        (c) =>
          Number.isFinite(c.rawLatitude) && Number.isFinite(c.rawLongitude)
      ),
      withRoomsTag: countPresent(candidates, (c) => c._hasRoomsTag),
      withStarsTag: countPresent(candidates, (c) => c._hasStarsTag),
      withBrandOrOperator: countPresent(candidates, (c) => c._hasBrandOrOperator),
      missingCity: countPresent(
        candidates,
        (c) => (c.missingFieldFlags || c._missingFieldFlags)?.missingCity
      ),
      missingWebsite: countPresent(
        candidates,
        (c) => (c.missingFieldFlags || c._missingFieldFlags)?.missingWebsite
      ),
      missingPhone: countPresent(
        candidates,
        (c) => (c.missingFieldFlags || c._missingFieldFlags)?.missingPhone
      ),
      missingName: countPresent(
        candidates,
        (c) => (c.missingFieldFlags || c._missingFieldFlags)?.missingName
      ),
    },
    dedupe: {
      uniqueKeys: dedupeKeyCounts.size,
      duplicateKeyGroups: duplicateGroups.length,
      duplicateRows: duplicateGroups.reduce((s, [, n]) => s + n, 0),
      topDuplicateKeys: duplicateGroups.slice(0, 10).map(([key, count]) => ({
        candidateDedupeKey: key,
        count,
      })),
    },
    filtering: extras.filtering || null,
    capping: extras.capping || {},
    sourcePolicy: extras.sourcePolicy || null,
    comparison: extras.comparison || null,
  };
}
