import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE } from "./fields.js";
import {
  assertLegacyHotelCensusAccess,
  isLegacyHotelCensusTableRef,
} from "../hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";

/** Canonical hotel property table for Dealality product runtime. */
export const HOTEL_PROPERTY_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_PROPERTY_CENSUS_TABLE || "Hotel Property Census";
export const HOTEL_PROPERTY_CENSUS_TABLE_ID = "tbl9aY5ijiuIzzWam";

let cachedBase = null;

/**
 * Platform Airtable base — zero Hotel Census data access (read or write).
 * Founder correction 2026-11-20: no migration-read exception for record contents.
 */
export function getPlatformBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    return null;
  }
  if (!cachedBase) {
    const rawBase = new Airtable({ apiKey }).base(baseId);
    cachedBase = function guardedPlatformBase(tableName) {
      const table = rawBase(tableName);
      if (!isLegacyHotelCensusTableRef(tableName)) {
        return table;
      }
      return new Proxy(table, {
        get(target, prop, receiver) {
          const value = Reflect.get(target, prop, receiver);
          if (typeof value !== "function") return value;
          const method =
            prop === "select" || prop === "find"
              ? "GET"
              : prop === "create"
                ? "POST"
                : prop === "destroy"
                  ? "DELETE"
                  : "PATCH";
          if (
            prop === "select" ||
            prop === "find" ||
            prop === "create" ||
            prop === "update" ||
            prop === "destroy" ||
            prop === "replace"
          ) {
            return function blockedLegacyAccess() {
              assertLegacyHotelCensusAccess(tableName, {
                method,
                workflow: "platform-base-airtable",
              });
            };
          }
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    };
  }
  return cachedBase;
}

export function ensurePlatformConfig(res) {
  if (getPlatformBase()) return true;
  res.status(500).json({
    success: false,
    error:
      "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT for Platform / Hotel Property Census",
  });
  return false;
}

/**
 * @deprecated Name retained for import compatibility. Value resolves to HPC.
 * Never use for Hotel Census access.
 */
export { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE };
