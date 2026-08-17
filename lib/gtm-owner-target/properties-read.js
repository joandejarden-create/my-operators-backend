/**
 * Read CoStar property rows from the GTM Properties table.
 */
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_PROPERTIES,
} from "./field-map.js";
import { getGtmAirtableBase } from "./platform-base.js";
import { buildSourceRowKey, normalizeOwnerKey } from "./normalize.js";

function fieldValue(fields, key) {
  const v = fields[key];
  if (v == null || v === "") return "";
  return v;
}

function fieldNumber(fields, key) {
  const v = fields[key];
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * @param {object} fields Airtable record fields
 */
export function mapAirtablePropertyRecord(fields) {
  const trueOwner = String(fieldValue(fields, MAP_GTM_PROPERTIES.trueOwner)).trim();
  const buildingName = String(fieldValue(fields, MAP_GTM_PROPERTIES.buildingName)).trim();
  const row = {
    trueOwner,
    buildingName,
    costarPropertyId: String(fieldValue(fields, MAP_GTM_PROPERTIES.propertyId) || "").trim(),
    submarket: String(fieldValue(fields, MAP_GTM_PROPERTIES.submarket)).trim(),
    market: String(fieldValue(fields, MAP_GTM_PROPERTIES.market)).trim(),
    country: String(fieldValue(fields, MAP_GTM_PROPERTIES.country)).trim(),
    city: String(fieldValue(fields, MAP_GTM_PROPERTIES.city)).trim(),
    zipCode: String(fieldValue(fields, MAP_GTM_PROPERTIES.zipCode)).trim(),
    starRating: fieldNumber(fields, MAP_GTM_PROPERTIES.starRating),
    rbaGlaSf: fieldNumber(fields, MAP_GTM_PROPERTIES.rbaGla),
    yearBuilt: fieldNumber(fields, MAP_GTM_PROPERTIES.yearBuilt),
    yearRenovated: fieldNumber(fields, MAP_GTM_PROPERTIES.yearRenov),
    brandAffiliation: String(fieldValue(fields, MAP_GTM_PROPERTIES.brand)).trim(),
    parentCompany: String(fieldValue(fields, MAP_GTM_PROPERTIES.parentCompany)).trim(),
    hotelOperator: String(fieldValue(fields, MAP_GTM_PROPERTIES.hotelOperator)).trim(),
    rooms: fieldNumber(fields, MAP_GTM_PROPERTIES.rooms),
    propertyType: String(fieldValue(fields, MAP_GTM_PROPERTIES.propertyType) || "Hospitality").trim(),
    sourceFile: "airtable_properties",
  };
  row.sourceRowKey = buildSourceRowKey(row);
  return row;
}

/**
 * @returns {Promise<{ records: { id: string, fields: object, row: object }[], tableName: string }>}
 */
export async function fetchAllGtmProperties() {
  const base = getGtmAirtableBase();
  const tableName = GTM_OWNER_TARGET_TABLES.properties;
  const selectFields = [
    MAP_GTM_PROPERTIES.buildingName,
    MAP_GTM_PROPERTIES.trueOwner,
    MAP_GTM_PROPERTIES.propertyId,
    MAP_GTM_PROPERTIES.submarket,
    MAP_GTM_PROPERTIES.market,
    MAP_GTM_PROPERTIES.country,
    MAP_GTM_PROPERTIES.city,
    MAP_GTM_PROPERTIES.zipCode,
    MAP_GTM_PROPERTIES.starRating,
    MAP_GTM_PROPERTIES.rbaGla,
    MAP_GTM_PROPERTIES.yearBuilt,
    MAP_GTM_PROPERTIES.yearRenov,
    MAP_GTM_PROPERTIES.brand,
    MAP_GTM_PROPERTIES.parentCompany,
    MAP_GTM_PROPERTIES.hotelOperator,
    MAP_GTM_PROPERTIES.rooms,
    MAP_GTM_PROPERTIES.propertyType,
  ];

  const records = await base(tableName)
    .select({ fields: selectFields })
    .all();

  return {
    tableName,
    records: records.map((rec) => ({
      id: rec.id,
      fields: rec.fields,
      row: mapAirtablePropertyRecord(rec.fields),
    })),
  };
}

/**
 * @param {object[]} propertyRecords from fetchAllGtmProperties
 */
export function groupAirtablePropertiesByOwner(propertyRecords) {
  const groups = new Map();
  for (const rec of propertyRecords) {
    const ownerName = rec.row.trueOwner;
    if (!ownerName) continue;
    const key = normalizeOwnerKey(ownerName);
    if (!groups.has(key)) {
      groups.set(key, { ownerName, ownerKey: key, properties: [], propertyRecordIds: [] });
    }
    const g = groups.get(key);
    g.properties.push(rec.row);
    g.propertyRecordIds.push(rec.id);
  }
  return [...groups.values()];
}
