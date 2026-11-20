/**
 * Hotel Census identity search — PERMANENTLY DISABLED for data access.
 *
 * Founder correction 2026-11-20: Hotel Census is not a trusted data source.
 * Any call throws LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED before network I/O.
 */

import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
} from "./legacy-hotel-census-access-guard-v1.js";

export const HOTEL_CENSUS_IDENTITY_SEARCH_V1 = "HOTEL_CENSUS_IDENTITY_SEARCH_V1_DISABLED";
export const HOTEL_CENSUS_TABLE_NAME = LEGACY_HOTEL_CENSUS_TABLE_NAME;
export const HOTEL_CENSUS_TABLE_ID = LEGACY_HOTEL_CENSUS_TABLE_ID;

/**
 * @returns {never}
 */
export async function searchHotelCensusIdentityCandidates(_opts = {}) {
  assertLegacyHotelCensusAccess(HOTEL_CENSUS_TABLE_ID, {
    method: "GET",
    workflow: "hotel-census-identity-search-v1-prohibited",
  });
  const err = new Error("LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED");
  err.code = "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED";
  throw err;
}
