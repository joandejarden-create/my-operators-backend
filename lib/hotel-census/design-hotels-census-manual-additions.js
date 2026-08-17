/**
 * Design Hotels (CALA) properties missing from Hotel Census — curated creates.
 * Source: designhotels.com sitemap gap audit (reports/design-hotels-cala-census-gaps.csv).
 */
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { countryToDealalityRegion } from "./region.js";
import { countryToSubContinent } from "./geography-enrichment-contract.js";
import { normalizeNameKey } from "./choice-census-manual-additions.js";

export { HOTEL_CENSUS_TABLE, normalizeNameKey };

export const MAP_DESIGN_HOTELS_CENSUS_MANUAL = {
  name: CENSUS_FIELDS.name,
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: CENSUS_FIELDS.parentCompany,
  status: CENSUS_FIELDS.status,
  city: CENSUS_FIELDS.city,
  country: CENSUS_FIELDS.country,
  region: CENSUS_FIELDS.region,
  subContinent: CENSUS_FIELDS.subContinent,
  market: CENSUS_FIELDS.market,
  submarket: CENSUS_FIELDS.submarket,
  chainScale: CENSUS_FIELDS.chainScale,
  operationType: CENSUS_FIELDS.operationType,
  projectPhase: CENSUS_FIELDS.projectPhase,
  location: CENSUS_FIELDS.location,
  website: "Website",
  latitude: "Latitude",
  longitude: "Longitude",
  address1: "Address 1",
};

const DESIGN_HOTELS_PARENT = "Marriott International";
const DESIGN_HOTELS_AFFILIATION = "Design Hotels";

/**
 * @typedef {object} DesignHotelsCensusManualRow
 * @property {string} portfolioKey
 * @property {string} name
 * @property {string} city
 * @property {string} country
 * @property {string} website
 * @property {string} [market]
 * @property {string} [submarket]
 * @property {string} [location]
 * @property {number} [latitude]
 * @property {number} [longitude]
 * @property {string} [address1]
 * @property {string} [notes]
 */

/** @type {DesignHotelsCensusManualRow[]} */
export const DESIGN_HOTELS_CENSUS_MANUAL_PLAN = [
  {
    portfolioKey: "wake-biohotel",
    name: "Wake BioHotel",
    city: "Medellín",
    country: "Colombia",
    latitude: 6.1956353,
    longitude: -75.5592716,
    address1: "Calle 2 Sur #25-115, El Poblado",
    website: "https://www.designhotels.com/hotels/colombia/medellin/wake-biohotel/",
    market: "Medellín",
    submarket: "Medellín",
    location: "Urban",
    notes: "Design Hotels sitemap gap; distinct from census row Wake (recUUKbie4t0mxiQG).",
  },
  {
    portfolioKey: "wake-medellin",
    name: "Wake Medellín",
    city: "Medellín",
    country: "Colombia",
    latitude: 6.210572,
    longitude: -75.564608,
    address1: "Carrera 35 #10B-115, El Poblado",
    website: "https://www.designhotels.com/hotels/colombia/medellin/wake-medellin/",
    market: "Medellín",
    submarket: "Medellín",
    location: "Urban",
    notes: "Design Hotels sitemap gap; second Wake property in Medellín.",
  },
  {
    portfolioKey: "good-hotel-antigua",
    name: "Good Hotel Antigua",
    city: "Antigua Guatemala",
    country: "Guatemala",
    latitude: 14.5557947,
    longitude: -90.7286486,
    address1: "Calle del Hermano Pedro 12",
    website: "https://www.designhotels.com/hotels/guatemala/antigua-guatemala/good-hotel-antigua/",
    market: "Antigua Guatemala",
    submarket: "Antigua Guatemala",
    location: "Urban",
    notes: "Design Hotels sitemap gap; distinct from Selina Antigua false match.",
  },
  {
    portfolioKey: "hotel-humano",
    name: "Hotel Humano",
    city: "Puerto Escondido",
    country: "Mexico",
    latitude: 15.8350732,
    longitude: -97.0446643,
    address1: "Alejandro Cárdenas Peralta 610, Brisas de Zicatela",
    website:
      "https://www.designhotels.com/hotels/mexico/oaxaca/puerto-escondido/la-punta-zicatela/hotel-humano/",
    market: "Oaxaca",
    submarket: "Puerto Escondido",
    location: "Resort",
    notes: "Design Hotels sitemap gap; La Punta Zicatela beach property.",
  },
  {
    portfolioKey: "esh-hotel-spa",
    name: "Esh Hotel & Spa",
    city: "Nosara",
    country: "Costa Rica",
    latitude: 9.947451,
    longitude: -85.6586983,
    address1: "Las Huacas, 25 m north of ICE Tower, Playa Guiones",
    website: "https://www.designhotels.com/hotels/costa-rica/nosara/esh-hotel-spa/",
    market: "Guanacaste",
    submarket: "Nosara",
    location: "Resort",
    notes: "Design Hotels sitemap gap.",
  },
  {
    portfolioKey: "downtown-mexico",
    name: "Downtown Mexico",
    city: "Mexico City",
    country: "Mexico",
    latitude: 19.4329306,
    longitude: -99.136213,
    address1: "Isabel la Católica 30, Centro Histórico",
    website: "https://www.designhotels.com/hotels/mexico/mexico-city/downtown-mexico/",
    market: "Mexico City",
    submarket: "Mexico City",
    location: "Urban",
    notes: "Design Hotels sitemap gap; Grupo Habita heritage urban property.",
  },
  {
    portfolioKey: "nest-baja",
    name: "NEST Baja",
    city: "San José del Cabo",
    country: "Mexico",
    latitude: 23.1748193,
    longitude: -109.4835411,
    address1: "Camino Cabo Este s/n",
    website:
      "https://www.designhotels.com/hotels/mexico/baja-california-sur/san-jose-del-cabo/nest-baja/",
    market: "Los Cabos",
    submarket: "San José del Cabo",
    location: "Resort",
    notes: "Design Hotels sitemap gap; distinct from NEST Tulum (recrzJunQGjBCGc8f).",
  },
];

export function rowToAirtableFields(row) {
  const F = MAP_DESIGN_HOTELS_CENSUS_MANUAL;

  /** @type {Record<string, unknown>} */
  const fields = {
    [F.name]: row.name,
    [F.city]: row.city,
    [F.country]: row.country,
    [F.region]: countryToDealalityRegion(row.country),
    [F.subContinent]: countryToSubContinent(row.country),
    [F.affiliation]: DESIGN_HOTELS_AFFILIATION,
    [F.parentCompany]: DESIGN_HOTELS_PARENT,
    [F.status]: ["Open"],
    [F.projectPhase]: "Open",
    [F.website]: row.website,
    [F.operationType]: "Independent",
    [F.chainScale]: "Independent",
  };

  if (row.market) fields[F.market] = row.market;
  if (row.submarket) fields[F.submarket] = row.submarket;
  if (row.location) fields[F.location] = row.location;
  if (Number.isFinite(row.latitude)) fields[F.latitude] = row.latitude;
  if (Number.isFinite(row.longitude)) fields[F.longitude] = row.longitude;
  if (row.address1) fields[F.address1] = row.address1;

  return fields;
}

/**
 * @param {DesignHotelsCensusManualRow} row
 */
export function validateDesignHotelsCensusManualRow(row) {
  const errors = [];
  if (!row.portfolioKey?.trim()) errors.push("portfolioKey required");
  if (!row.name?.trim()) errors.push("name required");
  if (!row.city?.trim()) errors.push("city required");
  if (!row.country?.trim()) errors.push("country required");
  if (!row.website?.trim()) errors.push("website required");
  if (!row.website.includes("designhotels.com/hotels/")) {
    errors.push("website must be a designhotels.com property URL");
  }
  return { pass: errors.length === 0, errors };
}

function websiteSlug(url) {
  try {
    const path = new URL(url).pathname.replace(/\/+$/, "");
    return path.split("/").filter(Boolean).pop() || "";
  } catch {
    return "";
  }
}

/**
 * @param {import('airtable').Records<any>} records
 * @param {DesignHotelsCensusManualRow} row
 */
export function findDuplicateCandidates(records, row) {
  const nameKey = normalizeNameKey(row.name);
  const countryKey = normalizeNameKey(row.country);
  const slug = websiteSlug(row.website).toLowerCase();

  return records.filter((rec) => {
    const f = rec.fields;
    const recWebsite = String(f.Website ?? "").toLowerCase();
    if (slug && recWebsite.includes(slug)) return true;
    if (normalizeNameKey(f.name) === nameKey && normalizeNameKey(f.country) === countryKey) {
      return true;
    }
    return false;
  });
}
