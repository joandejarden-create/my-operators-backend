/**
 * Airtable base routing for Market Demand Intelligence.
 *
 * Market Demand tables belong on Deal Capture Platform (AIRTABLE_BASE_ID_ALT),
 * not Deal Capture MVP (AIRTABLE_BASE_ID). Deals remain on MVP; cross-base
 * links use Deal Record ID text fields.
 */

import Airtable from "airtable";
import { DEALS_TABLE } from "../../api/schemas/deal-setup-fields.js";

/**
 * Platform base for Markets, Demand Centers, Demand Categories, Nearby Hotel Supply, Snapshots.
 * @returns {string | null}
 */
export function getMarketDemandBaseId() {
  return (
    process.env.AIRTABLE_MARKET_DEMAND_BASE_ID ||
    process.env.AIRTABLE_BASE_ID_ALT ||
    null
  );
}

/**
 * MVP base for Deals (auth, optional summary mirror fields).
 * @returns {string | null}
 */
export function getDealsBaseId() {
  return process.env.AIRTABLE_BASE_ID || null;
}

/**
 * @returns {{ baseId: string, apiKey: string, base: import('airtable').Base } | null}
 */
export function getMarketDemandAirtableConfig() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = getMarketDemandBaseId();
  if (!apiKey || !baseId) return null;
  return { baseId, apiKey, base: new Airtable({ apiKey }).base(baseId) };
}

/**
 * @returns {{ baseId: string, apiKey: string, base: import('airtable').Base } | null}
 */
export function getDealsAirtableConfig() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = getDealsBaseId();
  if (!apiKey || !baseId) return null;
  return { baseId, apiKey, base: new Airtable({ apiKey }).base(baseId) };
}

export { DEALS_TABLE };
