/**
 * Accor Catalog Properties API — destination hotel discovery.
 * Public client apikey is embedded in all.accor.com booking pages (read-only catalog).
 * @see https://developer.accor.com/api-portfolio/properties/how-use-it
 */

import { ACCOR_FETCH_HEADERS } from "./accor-brand-directory-extract.js";
import { ACCOR_COUNTRY_CODE_TO_LABEL } from "./brand-sitemap/cala-url-segments.js";

const CATALOG_BASE = "https://api.accor.com/catalog/v1";
const DEFAULT_API_KEY =
  process.env.ACCOR_CATALOG_API_KEY ||
  process.env.ACCOR_PUBLIC_API_KEY ||
  "l7xx5b9f4a053aaf43d8bc05bcc266dd8532";

const DEFAULT_FIELDS = [
  "results.hotel.id",
  "results.hotel.name",
  "results.hotel.brand",
  "results.hotel.localization",
  "results.hotel.contact",
].join(",");

/** @param {string} countryName census / catalog country label */
export function accorCountryNameToCode(countryName) {
  const target = String(countryName || "").trim().toLowerCase();
  if (!target) return "";
  for (const [code, label] of Object.entries(ACCOR_COUNTRY_CODE_TO_LABEL)) {
    if (String(label).toLowerCase() === target) return code;
  }
  return "";
}

/**
 * @param {object} hotel catalog hotel node
 */
export function mapAccorCatalogHotel(hotel) {
  const h = hotel?.hotel || hotel;
  if (!h?.id) return null;

  const addr = h.localization?.address || {};
  const gps = h.localization?.gps || {};
  const phone = String(h.contact?.phone || "").trim();
  const prefix = String(h.contact?.phonePrefix || "").trim();
  const telephone =
    phone && prefix && !phone.startsWith("+") ? `+${prefix} ${phone}` : phone;

  return {
    propertyId: String(h.id).toUpperCase(),
    name: String(h.name || "").trim(),
    brand: String(h.brand || "").trim(),
    city: String(addr.city || "").trim(),
    country: String(addr.country || "").trim(),
    countryCode: String(addr.countryCode || "").trim().toUpperCase(),
    address1: String(addr.street || addr.line1 || "").trim(),
    postalCode: String(addr.zipCode || "").trim(),
    telephone,
    email: String(h.contact?.mail || "").trim(),
    latitude: gps.lat != null ? Number(gps.lat) : null,
    longitude: gps.lng != null ? Number(gps.lng) : null,
    propertyUrl: `https://all.accor.com/hotel/${String(h.id).toUpperCase()}/index.en.shtml`,
    source: "accor_catalog_api",
  };
}

/**
 * @param {URLSearchParams} params
 * @param {object} [opts]
 */
async function fetchAccorCatalogParams(params, opts = {}) {
  const fields = opts.fields || DEFAULT_FIELDS;
  params.set("fields", fields);

  const url = `${CATALOG_BASE}/hotels?${params}`;
  const res = await fetch(url, {
    headers: {
      ...ACCOR_FETCH_HEADERS,
      apikey: opts.apiKey || DEFAULT_API_KEY,
      Accept: "application/json",
    },
  });

  if (!res.ok) {
    return { ok: false, hotels: [], error: `http_${res.status}`, status: res.status };
  }

  const data = await res.json();
  const hotels = (data.results || [])
    .map(mapAccorCatalogHotel)
    .filter(Boolean)
    .filter((h) => !opts.countryCode || h.countryCode === String(opts.countryCode).toUpperCase());

  return {
    ok: true,
    query: params.toString(),
    count: hotels.length,
    autoEnlarged: data.autoEnlarged ?? null,
    place: data.place ?? null,
    hotels,
  };
}

/**
 * @param {string} destinationQuery e.g. bogota, lavras
 * @param {object} [opts]
 */
export async function fetchAccorCatalogHotels(destinationQuery, opts = {}) {
  const q = String(destinationQuery || "").trim();
  if (!q) return { ok: false, hotels: [], error: "empty_query" };

  const params = new URLSearchParams({
    q,
    enlargementAllowed: String(opts.enlargementAllowed ?? false),
  });
  if (opts.brand) params.set("brand", opts.brand);
  if (opts.amenity) params.set("amenity", opts.amenity);

  const result = await fetchAccorCatalogParams(params, opts);
  if (!result.ok) return result;
  return { ...result, query: q };
}

/**
 * Radius search (km). @see Accor catalog API latLng + radius params.
 * @param {number} lat
 * @param {number} lng
 * @param {number} [radiusKm]
 * @param {object} [opts]
 */
export async function fetchAccorCatalogByRadius(lat, lng, radiusKm = 15, opts = {}) {
  const params = new URLSearchParams({
    latLng: `${lat},${lng}`,
    radius: String(radiusKm),
  });
  if (opts.brand) params.set("brand", opts.brand);
  return fetchAccorCatalogParams(params, opts);
}

/**
 * Bounding-box search. Params are "lat,lng" per Accor docs (bottomLeft / topRight).
 * @param {string} boxBottomLeft e.g. "-24.0,-47.5"
 * @param {string} boxTopRight e.g. "-23.0,-46.0"
 * @param {object} [opts]
 */
export async function fetchAccorCatalogByBbox(boxBottomLeft, boxTopRight, opts = {}) {
  const params = new URLSearchParams({
    boxBottomLeft,
    boxTopRight,
  });
  if (opts.brand) params.set("brand", opts.brand);
  return fetchAccorCatalogParams(params, opts);
}

/**
 * Batch lookup by property IDs.
 * @param {string[]} propertyIds
 * @param {object} [opts]
 */
export async function fetchAccorCatalogByIds(propertyIds, opts = {}) {
  const ids = [...new Set(propertyIds.map((id) => String(id || "").trim().toUpperCase()).filter(Boolean))];
  if (!ids.length) return { ok: false, hotels: [], error: "empty_ids" };

  const params = new URLSearchParams({
    id: ids.join(","),
  });
  return fetchAccorCatalogParams(params, opts);
}

/** Brazil macro-regions to beat the ~300 hotel country cap. */
export const BRAZIL_CATALOG_BBOXES = [
  { label: "SP metro", boxBottomLeft: "-24.2,-47.2", boxTopRight: "-23.3,-46.2" },
  { label: "SP interior", boxBottomLeft: "-24.0,-51.5", boxTopRight: "-20.0,-47.0" },
  { label: "RJ-ES", boxBottomLeft: "-23.5,-45.0", boxTopRight: "-19.5,-40.5" },
  { label: "South", boxBottomLeft: "-34.0,-58.0", boxTopRight: "-24.5,-48.0" },
  { label: "Northeast", boxBottomLeft: "-18.0,-45.0", boxTopRight: "-2.5,-34.5" },
  { label: "North-Central", boxBottomLeft: "-18.0,-58.0", boxTopRight: "-2.0,-42.0" },
];
