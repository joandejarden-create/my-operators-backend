/**
 * Hotel Census Location (STR location type) for Curio Collection by Hilton — CALA + global.
 * Field: Hotel Census → Location (multilineText).
 *
 * Vocabulary in census today: Urban, Resort, Suburban, Small Metro/Town, Airport, Interstate.
 */
import { CENSUS_FIELDS } from "./fields.js";
import { LOCATION_TYPE_SET } from "./location-enrichment-contract.js";

/** @typedef {{ recordId: string, name: string, location: string, rationale: string }} CurioLocationPlanRow */

/** @type {CurioLocationPlanRow[]} */
export const CURIO_LOCATION_TYPE_PLAN = [
  {
    recordId: "rec1X6pphTLTJMsHw",
    name: "Atolon Hotel Cartagena Tierra Bomba, Curio Collection by Hilton",
    location: "Resort",
    rationale: "Island beach resort on Tierra Bomba — leisure destination, not urban core.",
  },
  {
    recordId: "rec5jtMpYE4mJAnHc",
    name: "Zacatecas Centro Historico, Curio Collection by Hilton",
    location: "Urban",
    rationale: "Historic-city-center adaptive reuse in Zacatecas UNESCO core.",
  },
  {
    recordId: "recAaTLAMer14ZEuG",
    name: "Mahogany Bay Resort & Beach Club, Curio Collection by Hilton (pipeline)",
    location: "Resort",
    rationale: "Beach-club resort product on Ambergris Caye — matches open Mahogany Bay rows.",
  },
  {
    recordId: "recBYT2thdlopl8Wr",
    name: "Tranquility Beach Resort Dominica",
    location: "Resort",
    rationale: "Beachfront resort on Dominica's northwest coast.",
  },
  {
    recordId: "recJVheMdCGoKM0Eg",
    name: "Hotel Koti Gramado, Curio Collection by Hilton",
    location: "Resort",
    rationale: "Leisure mountain-resort market in Gramado — destination resort town, not metro urban.",
  },
  {
    recordId: "recKLvUHJCL77N3RJ",
    name: "Garcha Palace Santiago, Curio Collection by Hilton",
    location: "Urban",
    rationale: "Santiago de Chile urban boutique — Vitacura / city lifestyle product.",
  },
  {
    recordId: "recNRwEkGwEFm2Fyu",
    name: "Jean Hotel Polanco Curio Collection by Hilton",
    location: "Urban",
    rationale: "Mexico City Polanco urban lifestyle boutique.",
  },
  {
    recordId: "recYdFCf6D1LfvkLh",
    name: "Hotel Santiago Curio Collection by Hilton",
    location: "Urban",
    rationale: "Urban hotel in Santiago de los Caballeros, Dominican Republic.",
  },
  {
    recordId: "recZDon2IJp4CUuGi",
    name: "EOS Tulum, Curio Collection by Hilton",
    location: "Resort",
    rationale: "Tulum beach resort — leisure destination product.",
  },
  {
    recordId: "recgcOVunMPjXkJIH",
    name: "Kailani Grand Cayman, Curio Collection by Hilton",
    location: "Resort",
    rationale: "Pipeline beach resort on Grand Cayman — leisure coastal.",
  },
];

export const MAP_CURIO_CENSUS_LOCATION = {
  name: CENSUS_FIELDS.name,
  affiliation: CENSUS_FIELDS.affiliation,
  location: CENSUS_FIELDS.location,
};

const ALLOWED = LOCATION_TYPE_SET;

/**
 * @param {CurioLocationPlanRow} row
 */
export function validateLocationTypeRow(row) {
  const errors = [];
  if (!row.recordId) errors.push("missing recordId");
  if (!row.location || !ALLOWED.has(row.location)) {
    errors.push(`invalid location: ${row.location}`);
  }
  return { pass: errors.length === 0, errors };
}

/**
 * @param {CurioLocationPlanRow} row
 */
export function locationTypeToAirtableFields(row) {
  return { [CENSUS_FIELDS.location]: row.location };
}
