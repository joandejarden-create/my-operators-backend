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

function migrationReadEnvEnabled() {
  const a = String(process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ || "")
    .trim()
    .toLowerCase();
  const b = String(process.env.HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ || "")
    .trim()
    .toLowerCase();
  return a === "1" || a === "true" || b === "1" || b === "true";
}

/**
 * Platform Airtable base with legacy Hotel Census write prohibition.
 * Table accessors are wrapped so ordinary CLI/runtime paths cannot mutate
 * `tblgj2qEwxjTcg6q0` without an explicit migration-read GET opt-in.
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
          if (prop === "select" || prop === "find") {
            return function guardedRead(...args) {
              assertLegacyHotelCensusAccess(tableName, {
                method: "GET",
                workflow: "platform-base-airtable",
                allowLegacyHotelCensusRead: migrationReadEnvEnabled(),
              });
              return value.apply(target, args);
            };
          }
          if (prop === "create" || prop === "update" || prop === "destroy" || prop === "replace") {
            return function blockedWrite() {
              assertLegacyHotelCensusAccess(tableName, {
                method: prop === "create" ? "POST" : prop === "destroy" ? "DELETE" : "PATCH",
                workflow: "platform-base-airtable",
                forceBlock: true,
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
 * @deprecated Legacy Hotel Census — product runtime must use HOTEL_PROPERTY_CENSUS_TABLE.
 * Retained for migration tooling imports only. HOTEL_CENSUS_TABLE now resolves to HPC.
 */
export { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE };
