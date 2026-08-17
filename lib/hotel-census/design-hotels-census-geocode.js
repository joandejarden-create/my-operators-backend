/**
 * Verified WGS84 coordinates for Design Hotels CALA census rows missing lat/long.
 * Sources: Google Geocoding API (establishment queries), Jul 2026 steward pass.
 */
export const DESIGN_HOTELS_GEO_BY_RECORD_ID = {
  recxR0liKqOxWmYzR: {
    lat: 6.1956353,
    lng: -75.5592716,
    address1: "Calle 2 Sur #25-115, El Poblado",
    source: "google_places",
  },
  recM66GgH8KdBfV2M: {
    lat: 6.210572,
    lng: -75.564608,
    address1: "Carrera 35 #10B-115, El Poblado",
    source: "google_places",
  },
  recGL1epYhEle5SDM: {
    lat: 14.5557947,
    lng: -90.7286486,
    address1: "Calle del Hermano Pedro 12",
    source: "google_places",
  },
  recpjQt3RtrBGblEi: {
    lat: 15.8350732,
    lng: -97.0446643,
    address1: "Alejandro Cárdenas Peralta 610, Brisas de Zicatela",
    source: "google_places",
  },
  recl1pAQioQQyeWEM: {
    lat: 9.947451,
    lng: -85.6586983,
    address1: "Las Huacas, 25 m north of ICE Tower, Playa Guiones",
    source: "google_places",
  },
  rech091F6Lkpb62tH: {
    lat: 19.4329306,
    lng: -99.136213,
    address1: "Isabel la Católica 30, Centro Histórico",
    source: "google_geocode",
  },
  recnG8R0WC3Din6Gy: {
    lat: 23.1748193,
    lng: -109.4835411,
    address1: "Camino Cabo Este s/n",
    source: "google_places",
  },
};

export const F_LAT = "Latitude";
export const F_LNG = "Longitude";
export const F_ADDR = "Address 1";

export function hasCoords(fields) {
  const lat = Number(fields[F_LAT]);
  const lng = Number(fields[F_LNG]);
  return Number.isFinite(lat) && Number.isFinite(lng);
}

/**
 * @param {string} recordId
 * @param {import('airtable').FieldSet} fields
 * @param {{ fillAddress?: boolean }} [opts]
 */
export function buildGeocodePatch(recordId, fields, opts = {}) {
  const geo = DESIGN_HOTELS_GEO_BY_RECORD_ID[recordId];
  if (!geo) return null;
  if (hasCoords(fields)) return null;

  /** @type {Record<string, unknown>} */
  const patch = {
    [F_LAT]: geo.lat,
    [F_LNG]: geo.lng,
  };
  if (opts.fillAddress !== false && geo.address1 && !String(fields[F_ADDR] ?? "").trim()) {
    patch[F_ADDR] = geo.address1;
  }
  return patch;
}
