/**
 * Airtable read/write helpers for Market Demand tables.
 * Tables: Deal Capture Platform (AIRTABLE_BASE_ID_ALT).
 * Deals auth/mirror: Deal Capture MVP (AIRTABLE_BASE_ID).
 */

import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../third-party-operator-basics-airtable-column-aliases.js";
import {
  getMarketDemandAirtableConfig,
  getDealsAirtableConfig,
  DEALS_TABLE,
} from "./market-demand-base.js";
import {
  DEMAND_CENTERS_TABLE,
  NEARBY_HOTEL_SUPPLY_TABLE,
  MARKET_DEMAND_SNAPSHOTS_TABLE,
  MARKET_DEMAND_TABLES,
  DEMAND_CENTER_FIELDS,
  NEARBY_HOTEL_SUPPLY_FIELDS,
  MARKET_DEMAND_SNAPSHOT_FIELDS,
  DEALS_MARKET_DEMAND_FIELDS,
} from "./airtable-market-demand-fields.js";
import {
  normalizeDemandCenterRecord,
  normalizeNearbyHotelSupplyRecord,
  normalizeMarketDemandSnapshotRecord,
} from "./normalize-market-demand.js";
import { buildDealAssociationFields, recordMatchesDealId } from "./deal-link-helpers.js";

const isDev = process.env.NODE_ENV !== "production";

function marketDemandConfigError() {
  if (!process.env.AIRTABLE_API_KEY) return "airtable_config_missing";
  if (!getMarketDemandAirtableConfig()) return "market_demand_base_missing";
  return null;
}

/**
 * @returns {Promise<{ ok: true } | { ok: false, missing: string[] }>}
 */
export async function verifyMarketDemandTables(baseId, apiKey) {
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) {
      if (isDev) console.warn("[market-demand] meta tables fetch failed", res.status);
      return { ok: false, missing: [...MARKET_DEMAND_TABLES] };
    }
    const data = await res.json();
    const names = new Set((data.tables || []).map((t) => t.name));
    const missing = MARKET_DEMAND_TABLES.filter((t) => !names.has(t));
    if (missing.length) return { ok: false, missing };
    return { ok: true };
  } catch (err) {
    if (isDev) console.warn("[market-demand] verifyMarketDemandTables error", err?.message || err);
    return { ok: false, missing: [...MARKET_DEMAND_TABLES] };
  }
}

/**
 * @param {string} dealId
 */
export async function fetchDealRecord(dealId) {
  const cfg = getDealsAirtableConfig();
  if (!cfg) return { error: "airtable_config_missing" };
  try {
    const rec = await cfg.base(DEALS_TABLE).find(dealId);
    return { record: rec };
  } catch (err) {
    if (String(err?.statusCode) === "404" || /NOT_FOUND/i.test(String(err?.message))) {
      return { error: "deal_not_found" };
    }
    throw err;
  }
}

/**
 * @param {string} dealId
 */
export async function fetchDemandCentersForDeal(dealId) {
  const cfgErr = marketDemandConfigError();
  if (cfgErr) return { error: cfgErr };

  const cfg = getMarketDemandAirtableConfig();
  const tables = await verifyMarketDemandTables(cfg.baseId, cfg.apiKey);
  if (!tables.ok) return { error: "market_demand_tables_missing", missing: tables.missing };

  try {
    const records = await cfg.base(DEMAND_CENTERS_TABLE).select().all();
    const matched = records.filter((r) => recordMatchesDealId(r.fields || {}, dealId));
    return { demandCenters: matched.map(normalizeDemandCenterRecord) };
  } catch (err) {
    if (isDev) console.error("[market-demand] fetchDemandCentersForDeal", err);
    if (/Could not find table|NOT_FOUND|INVALID_PERMISSIONS/i.test(String(err?.message))) {
      return { error: "market_demand_tables_missing" };
    }
    throw err;
  }
}

/**
 * @param {string} dealId
 */
export async function fetchNearbyHotelSupplyForDeal(dealId) {
  const cfgErr = marketDemandConfigError();
  if (cfgErr) return { error: cfgErr };

  const cfg = getMarketDemandAirtableConfig();
  const tables = await verifyMarketDemandTables(cfg.baseId, cfg.apiKey);
  if (!tables.ok) return { error: "market_demand_tables_missing", missing: tables.missing };

  try {
    const records = await cfg.base(NEARBY_HOTEL_SUPPLY_TABLE).select().all();
    const matched = records.filter((r) => recordMatchesDealId(r.fields || {}, dealId));
    return { nearbyHotelSupply: matched.map(normalizeNearbyHotelSupplyRecord) };
  } catch (err) {
    if (isDev) console.error("[market-demand] fetchNearbyHotelSupplyForDeal", err);
    if (/Could not find table|NOT_FOUND/i.test(String(err?.message))) {
      return { error: "market_demand_tables_missing" };
    }
    throw err;
  }
}

/**
 * @param {string} dealId
 */
export async function fetchLatestSnapshotForDeal(dealId) {
  const cfgErr = marketDemandConfigError();
  if (cfgErr) return { error: cfgErr };

  const cfg = getMarketDemandAirtableConfig();
  const tables = await verifyMarketDemandTables(cfg.baseId, cfg.apiKey);
  if (!tables.ok) return { error: "market_demand_tables_missing", missing: tables.missing };

  try {
    const records = await cfg.base(MARKET_DEMAND_SNAPSHOTS_TABLE).select().all();
    const matched = records
      .filter((r) => recordMatchesDealId(r.fields || {}, dealId))
      .sort((a, b) => {
        const da = String(a.fields?.[MARKET_DEMAND_SNAPSHOT_FIELDS.lastGenerated] || "");
        const db = String(b.fields?.[MARKET_DEMAND_SNAPSHOT_FIELDS.lastGenerated] || "");
        return db.localeCompare(da);
      });
    if (!matched.length) return { snapshot: null, hasSnapshot: false };
    return {
      snapshot: normalizeMarketDemandSnapshotRecord(matched[0]),
      hasSnapshot: true,
      recordId: matched[0].id,
    };
  } catch (err) {
    if (isDev) console.error("[market-demand] fetchLatestSnapshotForDeal", err);
    if (/Could not find table|NOT_FOUND/i.test(String(err?.message))) {
      return { error: "market_demand_tables_missing" };
    }
    throw err;
  }
}

/**
 * @param {string} dealId
 * @param {Record<string, unknown>} airtableFields
 * @param {string} [existingRecordId]
 */
export async function upsertMarketDemandSnapshot(dealId, airtableFields, existingRecordId) {
  const cfgErr = marketDemandConfigError();
  if (cfgErr) return { error: cfgErr };

  const cfg = getMarketDemandAirtableConfig();
  const schema = await fetchAirtableTableFieldNameSet(
    cfg.baseId,
    cfg.apiKey,
    MARKET_DEMAND_SNAPSHOTS_TABLE
  );
  const withDeal = {
    ...airtableFields,
    ...buildDealAssociationFields(dealId, schema),
  };
  const safeFields = filterFieldsToAirtableSchema(withDeal, schema);

  if (existingRecordId) {
    const updated = await cfg
      .base(MARKET_DEMAND_SNAPSHOTS_TABLE)
      .update(existingRecordId, safeFields, { typecast: true });
    return { snapshot: normalizeMarketDemandSnapshotRecord(updated) };
  }

  const created = await cfg
    .base(MARKET_DEMAND_SNAPSHOTS_TABLE)
    .create(safeFields, { typecast: true });
  return { snapshot: normalizeMarketDemandSnapshotRecord(created) };
}

/**
 * Optionally mirror summary fields onto Deals (MVP base) when columns exist.
 * @param {string} dealId
 * @param {Record<string, unknown>} patch
 */
export async function patchDealsMarketDemandFields(dealId, patch) {
  const cfg = getDealsAirtableConfig();
  if (!cfg || !patch || !Object.keys(patch).length) return;

  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, DEALS_TABLE);
  const safe = filterFieldsToAirtableSchema(patch, schema);
  if (!Object.keys(safe).length) return;

  try {
    await cfg.base(DEALS_TABLE).update(dealId, safe, { typecast: true });
  } catch (err) {
    if (isDev) console.warn("[market-demand] patchDealsMarketDemandFields skipped", err?.message);
  }
}

/**
 * @param {string} dealId
 * @param {object[]} items
 */
export async function createDemandCenterRecords(dealId, items) {
  const cfgErr = marketDemandConfigError();
  if (cfgErr) return { error: cfgErr };

  const cfg = getMarketDemandAirtableConfig();
  const tables = await verifyMarketDemandTables(cfg.baseId, cfg.apiKey);
  if (!tables.ok) return { error: "market_demand_tables_missing", missing: tables.missing };

  const schema = await fetchAirtableTableFieldNameSet(
    cfg.baseId,
    cfg.apiKey,
    DEMAND_CENTERS_TABLE
  );

  const created = [];
  for (const item of items) {
    const fields = {
      ...buildDealAssociationFields(dealId, schema),
      [DEMAND_CENTER_FIELDS.name]: item.name,
      [DEMAND_CENTER_FIELDS.demandCategory]: item.category,
      [DEMAND_CENTER_FIELDS.demandSubcategory]: item.subcategory || undefined,
      [DEMAND_CENTER_FIELDS.distanceFromDeal]: item.distanceFromDeal ?? undefined,
      [DEMAND_CENTER_FIELDS.estimatedDriveTime]: item.estimatedDriveTime ?? undefined,
      [DEMAND_CENTER_FIELDS.demandStrength]: item.demandStrength || undefined,
      [DEMAND_CENTER_FIELDS.relevanceToHotelDemand]: item.relevanceToHotelDemand || undefined,
      [DEMAND_CENTER_FIELDS.demandPattern]: item.demandPattern || undefined,
      [DEMAND_CENTER_FIELDS.relevantHotelTypes]: item.relevantHotelTypes || undefined,
      [DEMAND_CENTER_FIELDS.source]: item.source || undefined,
      [DEMAND_CENTER_FIELDS.dataConfidence]: item.dataConfidence || undefined,
      [DEMAND_CENTER_FIELDS.relevanceScore]: item.relevanceScore ?? undefined,
      [DEMAND_CENTER_FIELDS.notes]: item.notes || undefined,
      [DEMAND_CENTER_FIELDS.address]: item.address || undefined,
      [DEMAND_CENTER_FIELDS.latitude]: item.latitude ?? undefined,
      [DEMAND_CENTER_FIELDS.longitude]: item.longitude ?? undefined,
      [DEMAND_CENTER_FIELDS.sourcePlaceId]: item.sourcePlaceId || undefined,
      [DEMAND_CENTER_FIELDS.sourceReference]: item.sourceReference || undefined,
    };
    const safe = filterFieldsToAirtableSchema(fields, schema);
    const rec = await cfg.base(DEMAND_CENTERS_TABLE).create(safe, { typecast: true });
    created.push(normalizeDemandCenterRecord(rec));
  }
  return { demandCenters: created };
}
