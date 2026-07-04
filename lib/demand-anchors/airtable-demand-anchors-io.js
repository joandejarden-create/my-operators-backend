/**
 * Airtable read/write helpers for Demand Anchors.
 */

import { DEMAND_ANCHORS_FIELDS as F } from "./airtable-demand-anchors-fields.js";
import { fetchAirtableTableFieldNameSet } from "../third-party-operator-basics-airtable-column-aliases.js";
import {
  getDemandAnchorsAirtableConfig,
  resolveDemandAnchorsTableName,
} from "./demand-anchors-base.js";
import {
  normalizeDemandAnchorToRadarPoint,
  toLegacyDemandAnchorItem,
} from "./normalize-demand-anchor.js";
import {
  calculateDemandAnchorsStatistics,
  getDemandAnchorLayerFilters,
  summarizeDemandAnchors,
} from "./radar-map-layers.js";
import { isDemandAnchorMapDisplayReady } from "./coordinate-verification.js";

const isDev = process.env.NODE_ENV !== "production";

export const DEMAND_ANCHORS_SELECT_FIELDS = [
  F.name,
  F.radarCategory,
  F.pointType,
  F.pointSubtype,
  F.linkedMarket,
  F.dealRecordId,
  F.linkedDeals,
  F.city,
  F.country,
  F.region,
  F.submarket,
  F.address,
  F.lat,
  F.lng,
  F.distanceFromDeal,
  F.estimatedDriveTime,
  F.demandRelevance,
  F.demandSegment,
  F.demandPattern,
  F.relevantHotelTypes,
  F.hotelDemandRationale,
  F.source,
  F.sourceReference,
  F.dataConfidence,
  F.includeOnRadarMap,
  F.mapLayer,
  F.mapIconType,
  F.visibility,
  F.notes,
  F.lastVerified,
];

function escFormula(s) {
  return String(s).replace(/'/g, "\\'");
}

/**
 * @param {object} [query]
 */
export async function listDemandAnchors(query = {}) {
  return fetchDemandAnchorRecords(query);
}

/**
 * @param {object} [query]
 */
export async function listDemandAnchorsForRadar(query = {}) {
  const result = await fetchDemandAnchorRecords(query);
  if (result.error) return result;
  return {
    ...result,
    layerFilters: getDemandAnchorLayerFilters(result.allPoints || result.points || []),
    statistics: calculateDemandAnchorsStatistics(result.allPoints || result.points || []),
  };
}

/**
 * @param {object} [query]
 */
export async function fetchDemandAnchorRecords(query = {}) {
  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg) {
    return { error: "airtable_config_missing" };
  }

  const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const requestedFields = schema
    ? DEMAND_ANCHORS_SELECT_FIELDS.filter((name) => schema.has(name))
    : DEMAND_ANCHORS_SELECT_FIELDS.filter((name) =>
        [F.name, F.pointType, F.lat, F.lng, F.city, F.country, F.region].includes(name)
      );

  const conditions = [];
  const typeFilter = query.pointTypeFilter || query.pointType;
  if (typeFilter && typeFilter !== "all") {
    conditions.push(`{${F.pointType}} = '${escFormula(typeFilter)}'`);
  }
  if (query.country) {
    conditions.push(`{${F.country}} = '${escFormula(query.country)}'`);
  }
  if (query.region) {
    conditions.push(`{${F.region}} = '${escFormula(query.region)}'`);
  }
  const dealId = query.dealId || query.dealRecordId;
  if (dealId) {
    conditions.push(`{${F.dealRecordId}} = '${escFormula(dealId)}'`);
  }

  const selectOptions = {
    fields: requestedFields.length ? requestedFields : [F.name, F.pointType, F.lat, F.lng],
    maxRecords: Number(process.env.DEMAND_ANCHORS_MAX_RECORDS) || 1000,
  };
  if (conditions.length) {
    selectOptions.filterByFormula = `AND(${conditions.join(", ")})`;
  }

  try {
    const records = await cfg.base(tableName).select(selectOptions).all();
    let points = records.map((r) =>
      normalizeDemandAnchorToRadarPoint(r, { applyDefaults: true })
    );

    if (query.market) {
      const market = String(query.market).trim();
      if (market.startsWith("rec")) {
        points = points.filter((p) => p.linkedMarketIds.includes(market));
      } else {
        const m = market.toLowerCase();
        points = points.filter(
          (p) =>
            String(p.region || "").toLowerCase() === m ||
            String(p.city || "").toLowerCase().includes(m)
        );
      }
    }

    const visiblePoints = query.includeHidden
      ? points
      : points.filter((p) => isDemandAnchorMapDisplayReady(p));

    return {
      tableName,
      points: visiblePoints,
      allPoints: points,
      anchors: visiblePoints.map(toLegacyDemandAnchorItem),
    };
  } catch (err) {
    if (isDev) console.error("[demand-anchors] fetch error", err?.message || err);
    if (/Could not find table|NOT_FOUND|INVALID_PERMISSIONS/i.test(String(err?.message))) {
      return { error: "demand_anchors_table_missing", tableName };
    }
    throw err;
  }
}

/**
 * @param {Array<{ fields: Record<string, unknown> }>} records
 */
export async function createDemandAnchors(records) {
  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg) return { error: "airtable_config_missing" };
  const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
  const created = [];
  for (const row of records || []) {
    const rec = await cfg.base(tableName).create(row.fields || row, { typecast: true });
    created.push(rec);
  }
  return { tableName, created };
}

/**
 * @param {string} baseId
 * @param {string} apiKey
 */
export async function verifyDemandAnchorsTable(baseId, apiKey) {
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return { ok: false };
    const data = await res.json();
    const hit = (data.tables || []).find((t) => /demand anchors/i.test(t.name));
    return { ok: Boolean(hit), tableName: hit?.name || null, table: hit || null };
  } catch {
    return { ok: false };
  }
}

export { summarizeDemandAnchors, getDemandAnchorLayerFilters };
