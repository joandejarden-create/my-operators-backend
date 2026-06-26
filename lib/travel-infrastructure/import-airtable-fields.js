/**
 * Travel Infrastructure — Airtable field builder for additional-type imports.
 */

import { TRAVEL_INFRASTRUCTURE_FIELDS as F } from "./airtable-travel-infrastructure-fields.js";
import { applyPointTypeDefaults } from "./point-type-defaults.js";
import { filterFieldsToAirtableSchema } from "../third-party-operator-basics-airtable-column-aliases.js";
import { resolveSubmarketForImport } from "../radar-submarket.js";

export function buildTravelInfraAirtableFields(item, opts = {}) {
  const withDefaults = applyPointTypeDefaults({
    ...item,
    name: item.name,
    pointType: item.pointType,
    latitude: item.latitude ?? item.lat,
    longitude: item.longitude ?? item.lng,
  });

  const fields = {
    [F.name]: withDefaults.name,
    [F.type]: withDefaults.pointType,
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
    [F.radarCategory]: withDefaults.radarCategory,
    [F.mapLayer]: withDefaults.mapLayer,
    [F.mapIconType]: withDefaults.mapIconType,
    [F.demandRelevance]: withDefaults.demandRelevance,
    [F.demandPattern]: withDefaults.demandPattern,
    [F.relevantHotelTypes]: withDefaults.relevantHotelTypes,
    [F.hotelDemandRationale]: withDefaults.hotelDemandRationale,
    [F.dataConfidence]: item.dataConfidence || "Medium",
    [F.includeOnRadarMap]: item.includeOnRadarMap !== false,
    [F.source]: ["Manual Research"],
    [F.sourceReference]: item.sourceReference,
    [F.notes]: item.notes,
  };

  if (opts.schema) return filterFieldsToAirtableSchema(fields, opts.schema);
  return fields;
}
