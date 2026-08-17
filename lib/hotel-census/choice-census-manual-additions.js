/**
 * Curated Choice Hotels (CALA) properties missing from STR match → Hotel Census creates.
 */
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { countryToDealalityRegion } from "./region.js";
import { countryToSubContinent } from "./geography-enrichment-contract.js";

export { HOTEL_CENSUS_TABLE };

export const MAP_CHOICE_CENSUS_MANUAL = {
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
  rooms: CENSUS_FIELDS.rooms,
  chainScale: CENSUS_FIELDS.chainScale,
  operationType: CENSUS_FIELDS.operationType,
  projectPhase: CENSUS_FIELDS.projectPhase,
  location: CENSUS_FIELDS.location,
  hotelServiceModel: CENSUS_FIELDS.hotelServiceModel,
  strNumber: "STR Number",
  propertyId: "Property ID",
  website: "Website",
  latitude: "Latitude",
  longitude: "Longitude",
  address1: "Address 1",
  projectedOpenDate: "projected_open_date",
};

const CHOICE_PARENT = "Choice Hotels International, Inc.";

/**
 * @typedef {object} ChoiceCensusManualRow
 * @property {string} portfolioKey
 * @property {string} name
 * @property {string} city
 * @property {string} country
 * @property {string} affiliation
 * @property {string} [parentCompany]
 * @property {'Open'|'Pipeline'} lifecycle
 * @property {string} [operationType]
 * @property {string} [chainScale]
 * @property {number} [rooms]
 * @property {string} [market]
 * @property {string} [submarket]
 * @property {string} [location]
 * @property {string} [hotelServiceModel]
 * @property {string} [strNumber]
 * @property {string} [propertyId]
 * @property {string} [website]
 * @property {number} [latitude]
 * @property {number} [longitude]
 * @property {string} [address1]
 * @property {string} [projectedOpenDate] - YYYY-MM-DD
 * @property {string} [notes]
 */

/** @type {ChoiceCensusManualRow[]} */
export const CHOICE_CENSUS_MANUAL_PLAN = [
  {
    portfolioKey: "radisson-red-ibirapuera",
    name: "Radisson RED Ibirapuera",
    city: "Sao Paulo",
    country: "Brazil",
    affiliation: "Radisson RED by Choice",
    parentCompany: CHOICE_PARENT,
    lifecycle: "Open",
    operationType: "Franchise",
    chainScale: "Upper Upscale Chain",
    rooms: 181,
    market: "São Paulo",
    submarket: "São Paulo",
    location: "Urban",
    hotelServiceModel: "Select-Service",
    strNumber: "331301",
    propertyId: "br185",
    website: "https://www.choicehotels.com/sao-paulo/sao-paulo/radisson-red-hotels/br185",
    latitude: -23.6047475,
    longitude: -46.6613493,
    address1: "Alameda Iraé, 663, Moema",
    notes:
      "STR Existing CALA import no-match row; Choice sitemap br185; rooms from public OTA listings (181 keys).",
  },
  {
    portfolioKey: "radisson-red-funes",
    name: "Radisson RED Funes",
    city: "Funes",
    country: "Argentina",
    affiliation: "Radisson RED by Choice",
    parentCompany: CHOICE_PARENT,
    lifecycle: "Pipeline",
    operationType: "Franchise",
    chainScale: "Upper Upscale Chain",
    rooms: 80,
    market: "Argentina Provincial",
    submarket: "Rosario",
    location: "Suburban",
    hotelServiceModel: "Select-Service",
    propertyId: "aa024",
    website: "https://www.choicehotels.com/argentina/rosario/radisson-red-hotels/aa024",
    latitude: -32.9388,
    longitude: -60.8119,
    address1: "Ruta 9 Autopista Rosario-Córdoba, Km 307.5, Sol de Funes",
    projectedOpenDate: "2026-08-11",
    notes:
      "Not yet in STR CALA extract; Choice sitemap aa024 (Rosario path). Pipeline — local press cites Aug 11, 2026 opening (~50 keys phase 1, 80+ total). Coords approximate (Sol de Funes complex, near HJ Funes).",
  },
];

export function rowToAirtableFields(row) {
  const F = MAP_CHOICE_CENSUS_MANUAL;
  const statusVal = row.lifecycle === "Open" ? ["Open"] : ["Pipeline"];
  const projectPhase = row.lifecycle === "Open" ? "Open" : "In Construction";

  /** @type {Record<string, unknown>} */
  const fields = {
    [F.name]: row.name,
    [F.city]: row.city,
    [F.country]: row.country,
    [F.region]: countryToDealalityRegion(row.country),
    [F.subContinent]: countryToSubContinent(row.country),
    [F.affiliation]: row.affiliation,
    [F.status]: statusVal,
    [F.projectPhase]: projectPhase,
  };

  if (row.parentCompany) fields[F.parentCompany] = row.parentCompany;
  if (row.operationType) fields[F.operationType] = row.operationType;
  if (row.chainScale) fields[F.chainScale] = row.chainScale;
  if (Number.isFinite(row.rooms) && row.rooms > 0) fields[F.rooms] = row.rooms;
  if (row.market) fields[F.market] = row.market;
  if (row.submarket) fields[F.submarket] = row.submarket;
  if (row.location) fields[F.location] = row.location;
  if (row.hotelServiceModel) fields[F.hotelServiceModel] = row.hotelServiceModel;
  if (row.strNumber) fields[F.strNumber] = row.strNumber;
  if (row.propertyId) fields[F.propertyId] = row.propertyId;
  if (row.website) fields[F.website] = row.website;
  if (Number.isFinite(row.latitude)) fields[F.latitude] = row.latitude;
  if (Number.isFinite(row.longitude)) fields[F.longitude] = row.longitude;
  if (row.address1) fields[F.address1] = row.address1;
  if (row.projectedOpenDate) fields[F.projectedOpenDate] = row.projectedOpenDate;

  return fields;
}

/**
 * @param {ChoiceCensusManualRow} row
 */
export function validateChoiceCensusManualRow(row) {
  const errors = [];
  if (!row.portfolioKey?.trim()) errors.push("portfolioKey required");
  if (!row.name?.trim()) errors.push("name required");
  if (!row.city?.trim()) errors.push("city required");
  if (!row.country?.trim()) errors.push("country required");
  if (!row.affiliation?.trim()) errors.push("affiliation required");
  if (!["Open", "Pipeline"].includes(row.lifecycle)) errors.push("invalid lifecycle");
  const allowedOp = ["Franchise", "Chain Management", "Independent"];
  if (row.operationType && !allowedOp.includes(row.operationType)) {
    errors.push(`operationType must be one of: ${allowedOp.join(", ")}`);
  }
  return { pass: errors.length === 0, errors };
}

export function normalizeNameKey(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * @param {import('airtable').Records<any>} records
 * @param {ChoiceCensusManualRow} row
 */
export function findDuplicateCandidates(records, row) {
  const nameKey = normalizeNameKey(row.name);
  const str = String(row.strNumber || "").trim();
  const propId = String(row.propertyId || "").trim().toLowerCase();

  return records.filter((rec) => {
    const f = rec.fields;
    if (str && String(f["STR Number"] ?? "") === str) return true;
    if (propId && String(f["Property ID"] ?? "").trim().toLowerCase() === propId) return true;
    if (normalizeNameKey(f.name) === nameKey && normalizeNameKey(f.country) === normalizeNameKey(row.country)) {
      return true;
    }
    return false;
  });
}
