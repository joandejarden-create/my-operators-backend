/**
 * HE CALA Hotel Census — curated room count corrections.
 * Central mapping per workspace rules.
 */

import { CENSUS_FIELDS } from "./fields.js";
import { HE_MGMT } from "./he-cala-census-apply.js";

export const MAP_HE_CALA_ROOM_CORRECTIONS = {
  rooms: CENSUS_FIELDS.rooms,
  name: CENSUS_FIELDS.name,
  mgmt: CENSUS_FIELDS.managementCompany,
};

/**
 * @typedef {object} HeCalaRoomCorrection
 * @property {string} recordId
 * @property {string} portfolioKey
 * @property {string} name
 * @property {number} rooms - target room count
 * @property {number} [expectedPreviousRooms] - for audit log only
 * @property {string} sourceUrl
 * @property {string} rationale
 */

/** @type {HeCalaRoomCorrection[]} */
export const HE_CALA_ROOM_CORRECTIONS = [
  {
    recordId: "recUF12aRBJxaDdIU",
    portfolioKey: "terra-nova",
    name: "Terra Nova BW Premier Collection",
    expectedPreviousRooms: 47,
    rooms: 41,
    sourceUrl: "https://www.terranovajamaica.com/suites",
    rationale: "Property website lists 41 suites; census had 47.",
  },
  {
    recordId: "recscC7og2NEHYfbr",
    portfolioKey: "hacienda-tres-rios",
    name: "Hacienda Tres Rios Resort, Spa & Nature Park",
    expectedPreviousRooms: 255,
    rooms: 273,
    sourceUrl: "https://www.oyster.com/playa-del-carmen/hotels/hacienda-tres-rios/",
    rationale: "Published suite inventory 273 keys (Kimpton/Hacienda era); census had 255.",
  },
];

/**
 * @param {HeCalaRoomCorrection} row
 * @returns {{ pass: boolean, errors: string[] }}
 */
export function validateRoomCorrection(row) {
  const errors = [];
  if (!row.recordId?.trim()) errors.push("recordId required");
  if (!row.name?.trim()) errors.push("name required");
  if (!Number.isFinite(row.rooms) || row.rooms <= 0) errors.push("rooms must be a positive number");
  if (row.expectedPreviousRooms != null && (!Number.isFinite(row.expectedPreviousRooms) || row.expectedPreviousRooms <= 0)) {
    errors.push("expectedPreviousRooms must be positive when set");
  }
  return { pass: errors.length === 0, errors };
}

/**
 * @param {HeCalaRoomCorrection} row
 * @returns {Record<string, number>}
 */
export function correctionToAirtableFields(row) {
  return { [MAP_HE_CALA_ROOM_CORRECTIONS.rooms]: row.rooms };
}

export { HE_MGMT };
