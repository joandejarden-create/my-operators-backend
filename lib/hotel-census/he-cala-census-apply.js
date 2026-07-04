/**
 * Hotel Equities (CALA) portfolio → Hotel Census create/update payloads.
 * Central field mapping per workspace rules.
 */

import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { countryToDealalityRegion } from "./region.js";

export { HOTEL_CENSUS_TABLE };

export const MAP_HE_CALA_CENSUS = {
  mgmt: CENSUS_FIELDS.managementCompany,
  name: CENSUS_FIELDS.name,
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: "Parent Company",
  status: CENSUS_FIELDS.status,
  city: CENSUS_FIELDS.city,
  country: CENSUS_FIELDS.country,
  region: CENSUS_FIELDS.region,
  rooms: CENSUS_FIELDS.rooms,
  chainScale: CENSUS_FIELDS.chainScale,
  operationType: CENSUS_FIELDS.operationType,
  projectPhase: CENSUS_FIELDS.projectPhase,
  projectedOpenDate: "projected_open_date",
  website: "Website",
};

export const HE_MGMT = "Hotel Equities (CALA)";

const PARENT_BY_BRAND_FAMILY = {
  hilton: "Hilton Worldwide",
  marriott: "Marriott International",
  bestWestern: "Best Western Hotels & Resorts",
  worldhotels: "Wyndham Hotels & Resorts",
};

/** @typedef {'create'|'update'} CensusAction */

/**
 * @typedef {object} HeCalaCensusRow
 * @property {CensusAction} action
 * @property {string} [recordId] - required for update
 * @property {string} portfolioKey
 * @property {string} name
 * @property {string} city
 * @property {string} country
 * @property {string} affiliation
 * @property {string} [parentCompany]
 * @property {'Open'|'Pipeline'} lifecycle
 * @property {string} [operationType] - Franchise | Chain Management | Independent
 * @property {string} [chainScale]
 * @property {number} [rooms]
 * @property {string} [projectedOpenDate] - YYYY-MM-DD
 * @property {string} [website]
 * @property {string} [notes]
 * @property {boolean} [setMgmt]
 */

/** @type {HeCalaCensusRow[]} */
export const HE_CALA_CENSUS_PLAN = [
  {
    action: "update",
    recordId: "recUF12aRBJxaDdIU",
    portfolioKey: "terra-nova",
    name: "Terra Nova BW Premier Collection",
    city: "Kingston",
    country: "Jamaica",
    affiliation: "Best Western Premier Collection",
    parentCompany: PARENT_BY_BRAND_FAMILY.bestWestern,
    lifecycle: "Open",
    operationType: "Franchise",
    chainScale: "Upper Midscale Chain",
    rooms: 41,
    setMgmt: true,
    notes: "HE CALA advisory/BW conversion support per Jamaica Gleaner Jan 2026; Hussey family ownership.",
    website: "https://www.terranovajamaica.com/",
  },
  {
    action: "create",
    portfolioKey: "claritas-jaco",
    name: "Clarita's Beach Bar & Resort",
    city: "Jacó",
    country: "Costa Rica",
    affiliation: "WorldHotels Elite",
    parentCompany: PARENT_BY_BRAND_FAMILY.worldhotels,
    lifecycle: "Open",
    operationType: "Chain Management",
    chainScale: "Upscale Chain",
    rooms: 63,
    projectedOpenDate: "2026-07-01",
    setMgmt: true,
    website: "https://www.hotelequities.com/cala.htm",
    notes: "HE CALA first Costa Rica property; WorldHotels Elite Mar 2026.",
  },
  {
    action: "create",
    portfolioKey: "hgi-averanda",
    name: "Hilton Garden Inn Averanda",
    city: "Cuernavaca",
    country: "Mexico",
    affiliation: "Hilton Garden Inn",
    parentCompany: PARENT_BY_BRAND_FAMILY.hilton,
    lifecycle: "Pipeline",
    operationType: "Franchise",
    chainScale: "Upscale Chain",
    rooms: 144,
    projectedOpenDate: "2028-01-01",
    setMgmt: true,
    notes: "HE CALA operator; Averanda Encuentro Cuernavaca; opening ~2028.",
  },
  {
    action: "create",
    portfolioKey: "elephant-tree-tobago",
    name: "Elephant Tree Resort and Villas Tobago, Tapestry Collection by Hilton",
    city: "Roxborough",
    country: "Trinidad and Tobago",
    affiliation: "Tapestry Collection by Hilton",
    parentCompany: PARENT_BY_BRAND_FAMILY.hilton,
    lifecycle: "Pipeline",
    operationType: "Franchise",
    chainScale: "Upscale Chain",
    rooms: 40,
    projectedOpenDate: "2028-01-01",
    setMgmt: true,
    website: "https://www.elephanttreebeachclub.com/",
    notes: "Hilton signing; operated by Hotel Equities per Hilton release.",
  },
  {
    action: "create",
    portfolioKey: "one-true-blue",
    name: "ONE True Blue Beach Hotel & Residences, Tapestry Collection by Hilton",
    city: "St George's",
    country: "Grenada",
    affiliation: "Tapestry Collection by Hilton",
    parentCompany: PARENT_BY_BRAND_FAMILY.hilton,
    lifecycle: "Pipeline",
    operationType: "Franchise",
    chainScale: "Upscale Chain",
    rooms: 70,
    projectedOpenDate: "2027-01-01",
    setMgmt: true,
    website: "https://1truebluebeach.com/",
    notes: "CBI project Golden Coast Ltd; distinct from True Blue Bay Boutique Resort.",
  },
  {
    action: "create",
    portfolioKey: "ceora-curacao",
    name: "Ceòra, a Luxury Collection Resort, Curaçao",
    city: "Willemstad",
    country: "Curaçao",
    affiliation: "Luxury Collection",
    parentCompany: PARENT_BY_BRAND_FAMILY.marriott,
    lifecycle: "Pipeline",
    operationType: "Franchise",
    chainScale: "Luxury Chain",
    setMgmt: true,
    website: "https://www.hotelequities.com/cala.htm",
    notes: "HE CALA portfolio pipeline; verify opening date.",
  },
  {
    action: "create",
    portfolioKey: "sanctuary-rainforest",
    name: "Sanctuary Rainforest Eco Resort and Spa",
    city: "Laudat",
    country: "Dominica",
    affiliation: "Independent",
    lifecycle: "Pipeline",
    operationType: "Chain Management",
    chainScale: "Independent",
    rooms: 72,
    setMgmt: true,
    website: "https://www.vitaldevelopers.com/sanctuary-rainforest-eco-resort-and-spa/",
    notes: "HE CALA Rainforest Eco Resort listing; Vital Developers CBI project.",
  },
  {
    action: "create",
    portfolioKey: "xiwara-las-terrenas",
    name: "Xiwara Las Terrenas Residential & Resort",
    city: "El Portillo",
    country: "Dominican Republic",
    affiliation: "Independent",
    lifecycle: "Pipeline",
    operationType: "Chain Management",
    chainScale: "Independent",
    rooms: 84,
    projectedOpenDate: "2028-01-01",
    setMgmt: true,
    website: "https://www.hotelequities.com/cala.htm",
    notes: "HE CALA portfolio; Bigentik Group developer El Portillo.",
  },
  {
    action: "create",
    portfolioKey: "hgi-san-fernando",
    name: "Hilton Garden Inn San Fernando South Park",
    city: "San Fernando",
    country: "Trinidad and Tobago",
    affiliation: "Hilton Garden Inn",
    parentCompany: PARENT_BY_BRAND_FAMILY.hilton,
    lifecycle: "Pipeline",
    operationType: "Franchise",
    chainScale: "Upscale Chain",
    rooms: 120,
    projectedOpenDate: "2028-01-01",
    setMgmt: true,
    website: "https://www.ttt.live/tt220m-hotel-project-breaks-ground-in-san-fernando-pm-welcomes-major-investment-at-south-park/",
    notes: "HE CALA site; developer Superior Hotels; operator not in press—confirm.",
  },
  {
    action: "create",
    portfolioKey: "grenada-national-resort",
    name: "Grenada National Resort (Hotel)",
    city: "Levera",
    country: "Grenada",
    affiliation: "Independent",
    lifecycle: "Pipeline",
    operationType: "Chain Management",
    chainScale: "Luxury Chain",
    rooms: 500,
    projectedOpenDate: "2027-01-01",
    setMgmt: true,
    website: "https://www.hsgrenadaresort.com/grenada-national-resort/",
    notes: "HE CALA Hotel Grenada listing; HSG International developer—confirm HE operator.",
  },
];

export function normalizeNameKey(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function rowToAirtableFields(row) {
  const F = MAP_HE_CALA_CENSUS;
  const statusVal = row.lifecycle === "Open" ? ["Open"] : ["Pipeline"];
  const projectPhase = row.lifecycle === "Open" ? "Open" : "In Construction";

  /** @type {Record<string, unknown>} */
  const fields = {
    [F.name]: row.name,
    [F.city]: row.city,
    [F.country]: row.country,
    [F.region]: countryToDealalityRegion(row.country),
    [F.affiliation]: row.affiliation,
    [F.status]: statusVal,
    [F.projectPhase]: projectPhase,
  };

  if (row.parentCompany) fields[F.parentCompany] = row.parentCompany;
  if (row.setMgmt) fields[F.mgmt] = HE_MGMT;
  if (row.operationType) fields[F.operationType] = row.operationType;
  if (row.chainScale) fields[F.chainScale] = row.chainScale;
  if (Number.isFinite(row.rooms) && row.rooms > 0) fields[F.rooms] = row.rooms;
  if (row.projectedOpenDate) fields[F.projectedOpenDate] = row.projectedOpenDate;
  if (row.website) fields[F.website] = row.website;

  return fields;
}

/**
 * @param {HeCalaCensusRow} row
 * @returns {{ pass: boolean, errors: string[] }}
 */
export function validateHeCalaRow(row) {
  const errors = [];
  if (!row.name?.trim()) errors.push("name required");
  if (!row.city?.trim()) errors.push("city required");
  if (!row.country?.trim()) errors.push("country required");
  if (!row.affiliation?.trim()) errors.push("affiliation required");
  if (!["Open", "Pipeline"].includes(row.lifecycle)) errors.push("invalid lifecycle");
  if (row.action === "update" && !row.recordId) errors.push("recordId required for update");
  const allowedOp = ["Franchise", "Chain Management", "Independent"];
  if (row.operationType && !allowedOp.includes(row.operationType)) {
    errors.push(`operationType must be one of: ${allowedOp.join(", ")}`);
  }
  return { pass: errors.length === 0, errors };
}

/**
 * Find likely duplicate census records.
 * @param {import('airtable').Records<any>} records
 * @param {HeCalaCensusRow} row
 */
/** Distinctive tokens per portfolio row for fuzzy match (min length 5). */
const FUZZY_NAME_TOKENS = {
  "claritas-jaco": ["clarita"],
  "hgi-averanda": ["averanda"],
  "elephant-tree-tobago": ["elephant tree"],
  "one-true-blue": ["one true blue"],
  "ceora-curacao": ["ceora"],
  "sanctuary-rainforest": ["sanctuary rainforest"],
  "xiwara-las-terrenas": ["xiwara"],
  "hgi-san-fernando": ["san fernando south park", "hilton garden inn san fernando"],
  "grenada-national-resort": ["grenada national resort"],
};

export function findDuplicateCandidates(records, row) {
  const nameKey = normalizeNameKey(row.name);
  const countryNorm = normalizeNameKey(row.country);
  const fuzzyTokens = FUZZY_NAME_TOKENS[row.portfolioKey] || [];

  return records.filter((rec) => {
    const f = rec.fields || {};
    const recCountry = normalizeNameKey(f[MAP_HE_CALA_CENSUS.country]);
    const recName = normalizeNameKey(f[MAP_HE_CALA_CENSUS.name]);
    if (!recName) return false;

    if (countryNorm && recCountry && recCountry !== countryNorm) return false;

    if (recName === nameKey) return true;

    const tokenHit = fuzzyTokens.some(
      (tok) => recName.includes(normalizeNameKey(tok)) && tok.length >= 5
    );
    if (!tokenHit) return false;

    if (countryNorm && recCountry) return recCountry === countryNorm;
    return true;
  });
}
