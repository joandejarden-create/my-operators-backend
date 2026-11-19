import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE } from "./fields.js";

/** Canonical hotel property table for Dealality product runtime. */
export const HOTEL_PROPERTY_CENSUS_TABLE =
  process.env.AIRTABLE_HOTEL_PROPERTY_CENSUS_TABLE || "Hotel Property Census";
export const HOTEL_PROPERTY_CENSUS_TABLE_ID = "tbl9aY5ijiuIzzWam";

let cachedBase = null;

export function getPlatformBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    return null;
  }
  if (!cachedBase) {
    cachedBase = new Airtable({ apiKey }).base(baseId);
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
 * Retained for migration tooling imports only.
 */
export { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE };
