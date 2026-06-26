/**
 * Cross-base deal linking helpers (Platform tables ↔ MVP Deals record ids).
 */

import {
  DEMAND_CENTER_FIELDS,
  NEARBY_HOTEL_SUPPLY_FIELDS,
  MARKET_DEMAND_SNAPSHOT_FIELDS,
  MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
} from "./airtable-market-demand-fields.js";

/**
 * @param {Record<string, unknown>} fields
 * @param {string[]} [linkFieldNames]
 */
export function extractDealRecordIdsFromFields(fields, linkFieldNames = []) {
  const f = fields || {};
  const ids = [];
  const textKey = MARKET_DEMAND_DEAL_RECORD_ID_FIELD;
  const textVal = f[textKey];
  if (typeof textVal === "string" && textVal.trim().startsWith("rec")) {
    ids.push(textVal.trim());
  }
  for (const name of linkFieldNames) {
    const raw = f[name];
    const arr = Array.isArray(raw) ? raw : raw ? [raw] : [];
    for (const id of arr) {
      if (typeof id === "string" && id.startsWith("rec")) ids.push(id);
    }
  }
  return [...new Set(ids)];
}

/**
 * @param {Record<string, unknown>} fields
 * @param {string} dealId
 */
export function recordMatchesDealId(fields, dealId) {
  return extractDealRecordIdsFromFields(fields, [
    DEMAND_CENTER_FIELDS.linkedDeals,
    DEMAND_CENTER_FIELDS.linkedDeal,
    NEARBY_HOTEL_SUPPLY_FIELDS.linkedDeal,
    MARKET_DEMAND_SNAPSHOT_FIELDS.linkedDeal,
    "Linked Deals",
    "Linked Deal",
  ]).includes(dealId);
}

/**
 * Build write payload for deal association (text id and/or legacy link if column exists).
 * @param {string} dealId
 * @param {Set<string> | null} schemaNameSet
 */
export function buildDealAssociationFields(dealId, schemaNameSet) {
  const out = {};
  if (!dealId) return out;

  if (!schemaNameSet || schemaNameSet.has(MARKET_DEMAND_DEAL_RECORD_ID_FIELD)) {
    out[MARKET_DEMAND_DEAL_RECORD_ID_FIELD] = dealId;
  }

  const legacyLinkNames = [
    DEMAND_CENTER_FIELDS.linkedDeals,
    NEARBY_HOTEL_SUPPLY_FIELDS.linkedDeal,
    MARKET_DEMAND_SNAPSHOT_FIELDS.linkedDeal,
  ];
  for (const name of legacyLinkNames) {
    if (!schemaNameSet || schemaNameSet.has(name)) {
      out[name] = [dealId];
    }
  }

  return out;
}
