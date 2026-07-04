/**
 * Airtable read helpers for Travel Infrastructure Data.
 */

import { TRAVEL_INFRASTRUCTURE_FIELDS as F } from "./airtable-travel-infrastructure-fields.js";
import { fetchAirtableTableFieldNameSet } from "../third-party-operator-basics-airtable-column-aliases.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "./travel-infrastructure-base.js";
import {
  normalizeTravelInfrastructureToRadarPoint,
  toLegacyInfrastructureItem,
} from "./normalize-radar-map-point.js";

const isDev = process.env.NODE_ENV !== "production";

/** Fields requested from Airtable (legacy + extensions). */
export const TRAVEL_INFRASTRUCTURE_SELECT_FIELDS = [
  F.name,
  F.type,
  F.lat,
  F.lng,
  F.city,
  F.country,
  F.region,
  F.submarket,
  F.iataCode,
  F.icaoCode,
  F.unLocode,
  F.scaleTier,
  F.infrastructureRole,
  F.airportType,
  F.sourceUrl,
  F.lastVerified,
  F.radarCategory,
  F.pointType,
  F.pointSubtype,
  F.linkedMarket,
  F.dealRecordId,
  F.address,
  F.distanceFromDeal,
  F.estimatedDriveTime,
  F.demandRelevance,
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
];

/**
 * @param {object} [query]
 * @param {string} [query.type]
 * @param {string} [query.pointType]
 * @param {string} [query.country]
 * @param {string} [query.region]
 * @param {boolean} [query.includeHidden]
 */
export async function fetchTravelInfrastructureRecords(query = {}) {
  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg) {
    return { error: "airtable_config_missing" };
  }

  const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const requestedFields = schema
    ? TRAVEL_INFRASTRUCTURE_SELECT_FIELDS.filter((name) => schema.has(name))
    : TRAVEL_INFRASTRUCTURE_SELECT_FIELDS.filter((name) =>
        [F.name, F.type, F.lat, F.lng, F.city, F.country, F.region].includes(name)
      );

  const conditions = [];
  const typeFilter = query.pointType || query.type;
  if (typeFilter) {
    conditions.push(
      `OR({${F.pointType}} = '${typeFilter.replace(/'/g, "\\'")}', {${F.type}} = '${typeFilter.replace(/'/g, "\\'")}')`
    );
  }
  if (query.country) {
    conditions.push(`{${F.country}} = '${String(query.country).replace(/'/g, "\\'")}'`);
  }
  if (query.region) {
    conditions.push(`{${F.region}} = '${String(query.region).replace(/'/g, "\\'")}'`);
  }

  const selectOptions = {
    fields: requestedFields.length ? requestedFields : [F.name, F.type, F.lat, F.lng],
    maxRecords: Number(process.env.TRAVEL_INFRASTRUCTURE_MAX_RECORDS) || 1000,
  };
  if (conditions.length) {
    selectOptions.filterByFormula = `AND(${conditions.join(", ")})`;
  }

  try {
    const records = await cfg.base(tableName).select(selectOptions).all();
    const points = records.map((r) =>
      normalizeTravelInfrastructureToRadarPoint(r, { applyDefaults: true })
    );
    const visiblePoints = query.includeHidden
      ? points
      : points.filter((p) => p.includeOnRadarMap !== false);

    return {
      tableName,
      points: visiblePoints,
      allPoints: points,
      infrastructure: visiblePoints.map(toLegacyInfrastructureItem),
    };
  } catch (err) {
    if (isDev) console.error("[travel-infrastructure] fetch error", err?.message || err);
    if (/Could not find table|NOT_FOUND|INVALID_PERMISSIONS/i.test(String(err?.message))) {
      return { error: "travel_infrastructure_table_missing", tableName };
    }
    throw err;
  }
}

/**
 * @param {string} baseId
 * @param {string} apiKey
 */
export async function verifyTravelInfrastructureTable(baseId, apiKey) {
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return { ok: false };
    const data = await res.json();
    const hit = (data.tables || []).find((t) => /travel infrastructure/i.test(t.name));
    return { ok: Boolean(hit), tableName: hit?.name || null, table: hit || null };
  } catch {
    return { ok: false };
  }
}
