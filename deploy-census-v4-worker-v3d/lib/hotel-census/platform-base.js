import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE } from "./fields.js";

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
    error: "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT for Platform / Hotel Census",
  });
  return false;
}

export { HOTEL_CENSUS_TABLE, BRAND_ALIAS_TABLE };
