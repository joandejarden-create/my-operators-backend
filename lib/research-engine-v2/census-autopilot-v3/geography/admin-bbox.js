/**
 * Dealality-owned approximate admin bounding boxes (axis-aligned).
 * NOT copied from proprietary polygon vendors. Sufficient for cohort disambiguation.
 * Confidence = Medium when used as sole method; High when corroborates alias/address.
 *
 * Source registry: dealality_approx_admin_bbox_v1
 * Persistence/licensing: first-party Dealality geometry approximation.
 */

/** @typedef {{ minLat:number, maxLat:number, minLng:number, maxLng:number, admin:string }} BBox */

/** @type {Record<string, BBox[]>} */
export const ADMIN_BBOXES = Object.freeze({
  Brazil: [
    { admin: "São Paulo", minLat: -25.5, maxLat: -19.5, minLng: -53.5, maxLng: -44.0 },
    { admin: "Rio de Janeiro", minLat: -23.5, maxLat: -20.5, minLng: -45.0, maxLng: -40.5 },
    { admin: "Pará", minLat: -9.5, maxLat: 2.5, minLng: -59.0, maxLng: -46.0 },
    { admin: "Amazonas", minLat: -11.0, maxLat: 2.5, minLng: -74.0, maxLng: -56.0 },
    { admin: "Paraná", minLat: -27.0, maxLat: -22.5, minLng: -55.0, maxLng: -48.0 },
    { admin: "Rio Grande do Sul", minLat: -34.0, maxLat: -27.0, minLng: -58.0, maxLng: -49.5 },
    { admin: "Bahia", minLat: -18.5, maxLat: -8.5, minLng: -46.5, maxLng: -37.0 },
    { admin: "Minas Gerais", minLat: -22.9, maxLat: -14.0, minLng: -51.5, maxLng: -39.5 },
    { admin: "Distrito Federal", minLat: -16.1, maxLat: -15.4, minLng: -48.3, maxLng: -47.3 },
    { admin: "Santa Catarina", minLat: -29.5, maxLat: -25.9, minLng: -54.0, maxLng: -48.3 },
    { admin: "Pernambuco", minLat: -9.6, maxLat: -7.2, minLng: -41.5, maxLng: -34.8 },
    { admin: "Ceará", minLat: -7.9, maxLat: -2.8, minLng: -41.5, maxLng: -37.2 },
    { admin: "Goiás", minLat: -19.5, maxLat: -12.4, minLng: -53.3, maxLng: -45.9 },
  ],
  Argentina: [
    { admin: "Buenos Aires", minLat: -41.0, maxLat: -33.0, minLng: -63.5, maxLng: -56.5 },
    { admin: "Ciudad Autónoma de Buenos Aires", minLat: -34.72, maxLat: -34.52, minLng: -58.55, maxLng: -58.33 },
    { admin: "Neuquén", minLat: -41.2, maxLat: -36.5, minLng: -71.5, maxLng: -68.0 },
    { admin: "Córdoba", minLat: -35.0, maxLat: -29.5, minLng: -66.0, maxLng: -61.5 },
    { admin: "Mendoza", minLat: -37.5, maxLat: -32.0, minLng: -70.5, maxLng: -66.5 },
    { admin: "Santa Fe", minLat: -34.5, maxLat: -28.0, minLng: -62.5, maxLng: -58.5 },
    { admin: "Santiago del Estero", minLat: -30.5, maxLat: -25.5, minLng: -65.5, maxLng: -61.5 },
    { admin: "Tucumán", minLat: -28.0, maxLat: -26.0, minLng: -66.0, maxLng: -64.5 },
    { admin: "Salta", minLat: -26.5, maxLat: -22.0, minLng: -68.5, maxLng: -62.5 },
    { admin: "Corrientes", minLat: -30.5, maxLat: -27.0, minLng: -59.5, maxLng: -55.5 },
    { admin: "Río Negro", minLat: -42.0, maxLat: -37.5, minLng: -72.0, maxLng: -62.5 },
  ],
  "Costa Rica": [
    { admin: "Guanacaste", minLat: 9.8, maxLat: 11.3, minLng: -86.2, maxLng: -84.8 },
    { admin: "Puntarenas", minLat: 8.0, maxLat: 10.3, minLng: -85.8, maxLng: -83.0 },
    { admin: "San José", minLat: 9.5, maxLat: 10.2, minLng: -84.5, maxLng: -83.5 },
    { admin: "Alajuela", minLat: 9.8, maxLat: 10.9, minLng: -85.0, maxLng: -84.0 },
    { admin: "Heredia", minLat: 9.9, maxLat: 10.6, minLng: -84.3, maxLng: -83.7 },
    { admin: "Limón", minLat: 9.5, maxLat: 11.0, minLng: -83.8, maxLng: -82.5 },
    { admin: "Cartago", minLat: 9.5, maxLat: 10.0, minLng: -84.1, maxLng: -83.5 },
  ],
  Jamaica: [
    { admin: "St. James", minLat: 18.35, maxLat: 18.55, minLng: -78.05, maxLng: -77.65 },
    { admin: "Kingston", minLat: 17.92, maxLat: 18.08, minLng: -76.85, maxLng: -76.72 },
    { admin: "St. Andrew", minLat: 17.95, maxLat: 18.15, minLng: -76.9, maxLng: -76.7 },
    { admin: "Westmoreland", minLat: 18.05, maxLat: 18.35, minLng: -78.35, maxLng: -77.9 },
    { admin: "Portland", minLat: 18.05, maxLat: 18.3, minLng: -76.55, maxLng: -76.2 },
    { admin: "St. Ann", minLat: 18.25, maxLat: 18.5, minLng: -77.5, maxLng: -77.0 },
  ],
  Barbados: [
    { admin: "Christ Church", minLat: 13.04, maxLat: 13.12, minLng: -59.62, maxLng: -59.42 },
    { admin: "Saint Michael", minLat: 13.07, maxLat: 13.15, minLng: -59.64, maxLng: -59.55 },
    { admin: "Saint Philip", minLat: 13.08, maxLat: 13.2, minLng: -59.5, maxLng: -59.4 },
    { admin: "Saint James", minLat: 13.15, maxLat: 13.25, minLng: -59.65, maxLng: -59.55 },
    { admin: "Saint Peter", minLat: 13.22, maxLat: 13.32, minLng: -59.65, maxLng: -59.55 },
  ],
  Mexico: [
    { admin: "Quintana Roo", minLat: 17.8, maxLat: 21.7, minLng: -89.5, maxLng: -86.7 },
    { admin: "Baja California Sur", minLat: 22.8, maxLat: 28.0, minLng: -115.2, maxLng: -109.4 },
    { admin: "Jalisco", minLat: 18.9, maxLat: 22.8, minLng: -105.7, maxLng: -101.5 },
    { admin: "Ciudad de México", minLat: 19.15, maxLat: 19.6, minLng: -99.35, maxLng: -98.95 },
    { admin: "Nuevo León", minLat: 23.1, maxLat: 27.8, minLng: -101.2, maxLng: -98.8 },
    { admin: "Yucatán", minLat: 19.5, maxLat: 21.7, minLng: -90.5, maxLng: -87.5 },
  ],
  "Dominican Republic": [
    { admin: "La Altagracia", minLat: 18.4, maxLat: 18.8, minLng: -68.7, maxLng: -68.3 },
    { admin: "Distrito Nacional", minLat: 18.42, maxLat: 18.55, minLng: -70.0, maxLng: -69.85 },
    { admin: "Puerto Plata", minLat: 19.6, maxLat: 19.9, minLng: -71.1, maxLng: -70.4 },
  ],
});

export const ADMIN_BBOX_SOURCE = Object.freeze({
  id: "dealality_approx_admin_bbox_v1",
  licensing: "first_party_dealality_approximation",
  not_osm_copy: true,
  not_google: true,
  not_cvent: true,
  not_str: true,
  not_legacy_census: true,
});

/**
 * @param {string} country
 * @param {number} lat
 * @param {number} lng
 */
export function lookupAdminByBbox(country, lat, lng) {
  const boxes = ADMIN_BBOXES[country];
  if (!boxes || lat == null || lng == null || Number.isNaN(Number(lat)) || Number.isNaN(Number(lng))) {
    return null;
  }
  const la = Number(lat);
  const lo = Number(lng);
  const hits = boxes.filter(
    (b) => la >= b.minLat && la <= b.maxLat && lo >= b.minLng && lo <= b.maxLng
  );
  if (!hits.length) return null;
  // Prefer smallest area (most specific)
  hits.sort(
    (a, b) =>
      (a.maxLat - a.minLat) * (a.maxLng - a.minLng) - (b.maxLat - b.minLat) * (b.maxLng - b.minLng)
  );
  return {
    value: hits[0].admin,
    method: "dealality_admin_bbox",
    boundary_source: ADMIN_BBOX_SOURCE.id,
    confidence: hits.length === 1 ? "High" : "Medium",
    candidate_count: hits.length,
  };
}
