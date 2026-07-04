/**
 * Map normalized demand anchor import items → Airtable field payloads.
 */

import { DEMAND_ANCHORS_FIELDS as F } from "./airtable-demand-anchors-fields.js";
import { applyPointTypeDefaults } from "./point-type-defaults.js";
import { filterFieldsToAirtableSchema } from "../third-party-operator-basics-airtable-column-aliases.js";
import { resolveSubmarketForImport } from "../radar-submarket.js";
import { MAP_REQUIRE_VERIFIED_COUNTRIES } from "./coordinate-verification.js";

/**
 * @param {object} item — normalized import item
 * @param {object} [opts]
 * @param {string} [opts.dealRecordId]
 * @param {string} [opts.linkedMarketId]
 * @param {Set<string>|null} [opts.schema]
 */
export function buildDemandAnchorAirtableFields(item, opts = {}) {
  const withDefaults = applyPointTypeDefaults({
    ...item,
    name: item.name,
    pointType: item.pointType,
    latitude: item.latitude ?? item.lat,
    longitude: item.longitude ?? item.lng,
  });

  const sourceVal = item.source;
  const sourceArr = Array.isArray(sourceVal)
    ? sourceVal
    : sourceVal
      ? [String(sourceVal)]
      : ["Manual Research"];

  const country = String(withDefaults.country || item.country || "").trim();
  const requiresCoordinateGate = MAP_REQUIRE_VERIFIED_COUNTRIES.has(country);
  const mapReady =
    !requiresCoordinateGate ||
    (item.coordinateVerified === true && item.includeOnRadarMap !== false);

  const fields = {
    [F.name]: withDefaults.name,
    [F.pointType]: withDefaults.pointType,
    [F.pointSubtype]: withDefaults.pointSubtype,
    [F.lat]: withDefaults.latitude ?? withDefaults.lat,
    [F.lng]: withDefaults.longitude ?? withDefaults.lng,
    [F.city]: withDefaults.city || item.city,
    [F.country]: withDefaults.country || item.country,
    [F.region]: withDefaults.region || item.region,
    ...(item.submarket
      ? { [F.submarket]: resolveSubmarketForImport(item) }
      : {}),
    [F.address]: withDefaults.address || item.address,
    [F.radarCategory]: withDefaults.radarCategory,
    [F.mapLayer]: withDefaults.mapLayer,
    [F.mapIconType]: withDefaults.mapIconType,
    [F.demandSegment]: withDefaults.demandSegment || item.demandSegment,
    [F.demandRelevance]: withDefaults.demandRelevance || item.demandRelevance,
    [F.demandPattern]: withDefaults.demandPattern || item.demandPattern,
    [F.relevantHotelTypes]: withDefaults.relevantHotelTypes || item.relevantHotelTypes,
    [F.hotelDemandRationale]: withDefaults.hotelDemandRationale || item.hotelDemandRationale,
    [F.dataConfidence]: item.dataConfidence || withDefaults.dataConfidence || "Medium",
    [F.includeOnRadarMap]: mapReady,
    [F.visibility]: item.visibility || "Internal Only",
    [F.notes]: item.notes,
    [F.source]: sourceArr,
    [F.sourceReference]: item.sourceReference || item.sourceUrl,
  };

  if (mapReady && (item.coordinateVerified === true || item.lastVerified)) {
    fields[F.lastVerified] = item.lastVerified || new Date().toISOString().slice(0, 10);
  }

  if (opts.dealRecordId) fields[F.dealRecordId] = opts.dealRecordId;
  if (opts.linkedMarketId) fields[F.linkedMarket] = [opts.linkedMarketId];

  if (opts.schema) return filterFieldsToAirtableSchema(fields, opts.schema);
  return fields;
}
