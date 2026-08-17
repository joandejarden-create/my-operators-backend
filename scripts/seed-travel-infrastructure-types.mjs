#!/usr/bin/env node
/**
 * Travel Infrastructure type reference + optional deal-scoped sample points.
 *
 *   node scripts/seed-travel-infrastructure-types.mjs
 *   node scripts/seed-travel-infrastructure-types.mjs --dry-run
 *   node scripts/seed-travel-infrastructure-types.mjs --apply [dealId]
 *   node scripts/seed-travel-infrastructure-types.mjs --apply [dealId] --market-id rec...
 */
import "../load-env.js";
import {
  TRAVEL_INFRASTRUCTURE_FIELDS as F,
  POINT_TYPES,
  POINT_SUBTYPES,
  MAP_ICON_TYPES,
  POINT_TYPE_TO_MAP_ICON,
} from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import {
  getTravelInfrastructureAirtableConfig,
  resolveTravelInfrastructureTableName,
} from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { applyPointTypeDefaults } from "../lib/travel-infrastructure/point-type-defaults.js";
import { fetchAirtableTableFieldNameSet } from "../lib/third-party-operator-basics-airtable-column-aliases.js";
import { filterFieldsToAirtableSchema } from "../lib/third-party-operator-basics-airtable-column-aliases.js";
import samplePayload from "../fixtures/travel-infrastructure-sample-import.json" assert { type: "json" };

const DRY = process.argv.includes("--dry-run");
const APPLY = process.argv.includes("--apply");

/** Reference catalog — not written to Airtable unless --apply with dealId. */
export const TRAVEL_INFRASTRUCTURE_TYPE_CATALOG = POINT_TYPES.flatMap((pointType) => {
  const subtypes = POINT_SUBTYPES.filter((sub) => {
    if (pointType === "Airport") return sub.includes("Airport");
    if (pointType === "Cruise Port") return sub === "Cruise Terminal";
    if (pointType === "Train Station") return sub === "Rail Hub" || sub === "Metro Hub";
    if (pointType === "Highway Access") return sub === "Highway Exit" || sub === "Major Corridor";
    if (pointType === "Bus Terminal") return sub === "Intercity Bus Terminal";
    if (pointType === "Ferry Terminal") return sub === "Ferry Terminal" || sub === "Island Access";
    if (pointType === "Port / Maritime") {
      return sub === "Cargo Port" || sub === "Marina" || sub === "Ferry Port";
    }
    return false;
  });
  return subtypes.map((pointSubtype) => ({
    pointType,
    pointSubtype,
    mapIconType: POINT_TYPE_TO_MAP_ICON[pointType],
  }));
});

function buildAirtableFields(item, dealId, marketId, schema) {
  const withDefaults = applyPointTypeDefaults(item);
  const fields = {
    [F.name]: withDefaults.name,
    [F.type]: withDefaults.pointType || withDefaults.type,
    [F.pointType]: withDefaults.pointType || withDefaults.type,
    [F.pointSubtype]: withDefaults.pointSubtype,
    [F.lat]: withDefaults.latitude ?? withDefaults.lat,
    [F.lng]: withDefaults.longitude ?? withDefaults.lng,
    [F.city]: withDefaults.city,
    [F.country]: withDefaults.country,
    [F.region]: withDefaults.region,
    [F.radarCategory]: withDefaults.radarCategory,
    [F.mapLayer]: withDefaults.mapLayer,
    [F.mapIconType]: withDefaults.mapIconType,
    [F.demandRelevance]: withDefaults.demandRelevance,
    [F.demandPattern]: withDefaults.demandPattern,
    [F.relevantHotelTypes]: withDefaults.relevantHotelTypes,
    [F.hotelDemandRationale]: withDefaults.hotelDemandRationale,
    [F.dataConfidence]: withDefaults.dataConfidence || "Medium",
    [F.includeOnRadarMap]: withDefaults.includeOnRadarMap !== false,
    [F.visibility]: withDefaults.visibility || "Demo",
    [F.notes]: withDefaults.notes || "Seeded travel infrastructure sample point",
  };
  if (dealId) fields[F.dealRecordId] = dealId;
  if (marketId) fields[F.linkedMarket] = [marketId];
  return schema ? filterFieldsToAirtableSchema(fields, schema) : fields;
}

async function main() {
  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg && APPLY) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT required for --apply");

  console.log("Travel Infrastructure type catalog (" + TRAVEL_INFRASTRUCTURE_TYPE_CATALOG.length + " type/subtype pairs):");
  for (const row of TRAVEL_INFRASTRUCTURE_TYPE_CATALOG) {
    console.log(" -", row.pointType, "→", row.pointSubtype, "(" + row.mapIconType + ")");
  }
  console.log("\nMap icon types:", MAP_ICON_TYPES.join(", "));

  const dealArg = process.argv.find((a) => a.startsWith("rec") && a.length > 10);
  const marketFlagIdx = process.argv.indexOf("--market-id");
  const marketId = marketFlagIdx >= 0 ? process.argv[marketFlagIdx + 1] : null;

  if (!APPLY) {
    console.log("\nDry run / catalog only. Pass --apply [dealId] to create sample deal points.");
    if (samplePayload.points?.length) {
      console.log("\nSample import payload preview:");
      for (const p of samplePayload.points) {
        const d = applyPointTypeDefaults(p);
        console.log(" ", d.name, "—", d.pointType, "/", d.pointSubtype);
      }
    }
    return;
  }

  if (DRY) console.log("\n[dry-run] would create sample points");

  const tableName = await resolveTravelInfrastructureTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);

  const points = samplePayload.points || [];
  if (!points.length) {
    console.log("No sample points in fixture.");
    return;
  }

  for (const item of points) {
    const fields = buildAirtableFields(item, dealArg, marketId, schema);
    if (DRY) {
      console.log("WOULD CREATE", fields[F.name] || item.name);
      continue;
    }
    const rec = await cfg.base(tableName).create(fields, { typecast: true });
    console.log("CREATED", rec.id, fields[F.name] || item.name);
  }

  console.log("\nDone.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
