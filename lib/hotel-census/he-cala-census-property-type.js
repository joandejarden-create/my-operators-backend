/**
 * HE CALA Hotel Census — Property Type assignments.
 * Field: `Property Type` (multilineText on Hotel Census; no Airtable select constraint).
 * Vocabulary aligned with HE operator setup: Full Service, Select Service, Resort, Boutique, Lifestyle.
 */

import { HE_MGMT } from "./he-cala-census-apply.js";

export const MAP_HE_CALA_PROPERTY_TYPE = {
  propertyType: "Property Type",
  name: "name",
  mgmt: "Management Company",
};

/** Allowed Property Type labels for validation. */
export const HE_CALA_PROPERTY_TYPE_OPTIONS = [
  "Full Service",
  "Select Service",
  "Resort",
  "Boutique",
  "Lifestyle",
  "All-inclusive",
];

/**
 * @typedef {object} HeCalaPropertyTypeRow
 * @property {string} recordId
 * @property {string} portfolioKey
 * @property {string} name
 * @property {string} propertyType
 * @property {string} rationale
 */

/** @type {HeCalaPropertyTypeRow[]} */
export const HE_CALA_PROPERTY_TYPE_PLAN = [
  {
    recordId: "recUF12aRBJxaDdIU",
    portfolioKey: "terra-nova",
    name: "Terra Nova BW Premier Collection",
    propertyType: "Full Service",
    rationale: "All-suite urban hotel with restaurant, bar, and meeting space.",
  },
  {
    recordId: "recq7PdwcUkBDDRbU",
    portfolioKey: "claritas-jaco",
    name: "Clarita's Beach Bar & Resort",
    propertyType: "Resort",
    rationale: "Beachfront resort positioning; WorldHotels Elite Costa Rica.",
  },
  {
    recordId: "recETlBd8ctQnHx4L",
    portfolioKey: "hgi-averanda",
    name: "Hilton Garden Inn Averanda",
    propertyType: "Select Service",
    rationale: "Hilton Garden Inn — select-service brand.",
  },
  {
    recordId: "recnV6adKBfss0VfR",
    portfolioKey: "elephant-tree-tobago",
    name: "Elephant Tree Resort and Villas Tobago, Tapestry Collection by Hilton",
    propertyType: "Resort",
    rationale: "Beachfront resort with hotel keys and branded residences.",
  },
  {
    recordId: "recgeikvPO8zuKTyJ",
    portfolioKey: "one-true-blue",
    name: "ONE True Blue Beach Hotel & Residences, Tapestry Collection by Hilton",
    propertyType: "Resort",
    rationale: "Beach hotel and residences; Tapestry resort product.",
  },
  {
    recordId: "rec8shi9qzHM6fjsf",
    portfolioKey: "ceora-curacao",
    name: "Ceòra, a Luxury Collection Resort, Curaçao",
    propertyType: "Lifestyle",
    rationale: "Marriott Luxury Collection lifestyle resort (pipeline).",
  },
  {
    recordId: "recT41S0j01asWiK8",
    portfolioKey: "sanctuary-rainforest",
    name: "Sanctuary Rainforest Eco Resort and Spa",
    propertyType: "Resort",
    rationale: "Eco resort and spa with villa inventory.",
  },
  {
    recordId: "recDtetmqOGsJGcxK",
    portfolioKey: "xiwara-las-terrenas",
    name: "Xiwara Las Terrenas Residential & Resort",
    propertyType: "Resort",
    rationale: "Mixed-use resort and residential; hotel-suite component.",
  },
  {
    recordId: "recUfg2NwXX3DvDWF",
    portfolioKey: "hgi-san-fernando",
    name: "Hilton Garden Inn San Fernando South Park",
    propertyType: "Select Service",
    rationale: "Hilton Garden Inn — select-service brand.",
  },
  {
    recordId: "recdeNV2tBUjf4zr2",
    portfolioKey: "grenada-national-resort",
    name: "Grenada National Resort (Hotel)",
    propertyType: "Resort",
    rationale: "Large-format destination resort (500 hotel suites).",
  },
  {
    recordId: "recFsXj5d1l3VwxTz",
    portfolioKey: "donoma-las-terrenas",
    name: "Donoma Las Terrenas Beach Hotel & Spa Autograph Collection",
    propertyType: "Lifestyle",
    rationale: "Autograph Collection lifestyle beach hotel.",
  },
  {
    recordId: "recGKuT86TJI05QAA",
    portfolioKey: "amaris-grace-bay",
    name: "AMARIS Grace Bay, LXR Hotels & Resorts",
    propertyType: "Lifestyle",
    rationale: "LXR luxury lifestyle beach resort.",
  },
  {
    recordId: "recHBTqnDeXJOc7FZ",
    portfolioKey: "casas-del-xvi",
    name: "Casas Del XVI",
    propertyType: "Boutique",
    rationale: "Small-format boutique hotel in Zona Colonial.",
  },
  {
    recordId: "recr3DXLHdh09J8mi",
    portfolioKey: "hampton-st-thomas",
    name: "Hampton by Hilton St. Thomas",
    propertyType: "Select Service",
    rationale: "Hampton by Hilton — select-service brand.",
  },
  {
    recordId: "recscC7og2NEHYfbr",
    portfolioKey: "hacienda-tres-rios",
    name: "Hacienda Tres Rios Resort, Spa & Nature Park",
    propertyType: "Resort",
    rationale: "Riviera Maya nature-park resort (all-inclusive resort product).",
  },
];

/**
 * @param {HeCalaPropertyTypeRow} row
 * @returns {{ pass: boolean, errors: string[] }}
 */
export function validatePropertyTypeRow(row) {
  const errors = [];
  if (!row.recordId?.trim()) errors.push("recordId required");
  if (!row.name?.trim()) errors.push("name required");
  if (!row.propertyType?.trim()) errors.push("propertyType required");
  if (!HE_CALA_PROPERTY_TYPE_OPTIONS.includes(row.propertyType)) {
    errors.push(
      `propertyType must be one of: ${HE_CALA_PROPERTY_TYPE_OPTIONS.join(", ")}`
    );
  }
  return { pass: errors.length === 0, errors };
}

/**
 * @param {HeCalaPropertyTypeRow} row
 * @returns {Record<string, string>}
 */
export function propertyTypeToAirtableFields(row) {
  return { [MAP_HE_CALA_PROPERTY_TYPE.propertyType]: row.propertyType };
}

export { HE_MGMT };
