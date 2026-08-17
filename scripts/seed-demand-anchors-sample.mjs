#!/usr/bin/env node
/**
 * Demand Anchors sample seed (San Juan market).
 *
 *   node scripts/seed-demand-anchors-sample.mjs
 *   node scripts/seed-demand-anchors-sample.mjs --dry-run
 *   node scripts/seed-demand-anchors-sample.mjs --apply [dealId]
 *   node scripts/seed-demand-anchors-sample.mjs --apply [dealId] --market-id rec...
 */
import "../load-env.js";
import {
  DEMAND_ANCHORS_FIELDS as F,
  POINT_TYPES,
  MAP_ICON_TYPES,
} from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import {
  getDemandAnchorsAirtableConfig,
  resolveDemandAnchorsTableName,
} from "../lib/demand-anchors/demand-anchors-base.js";
import { applyPointTypeDefaults } from "../lib/demand-anchors/point-type-defaults.js";
import {
  fetchAirtableTableFieldNameSet,
  filterFieldsToAirtableSchema,
} from "../lib/third-party-operator-basics-airtable-column-aliases.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const samplePayload = JSON.parse(
  readFileSync(join(__dirname, "../fixtures/demand-anchors-sample-import.json"), "utf8")
);

const DRY = process.argv.includes("--dry-run");
const APPLY = process.argv.includes("--apply");

function buildAirtableFields(item, dealId, marketId, schema) {
  const withDefaults = applyPointTypeDefaults(item);
  const fields = {
    [F.name]: withDefaults.name,
    [F.pointType]: withDefaults.pointType,
    [F.pointSubtype]: withDefaults.pointSubtype,
    [F.lat]: withDefaults.latitude ?? withDefaults.lat,
    [F.lng]: withDefaults.longitude ?? withDefaults.lng,
    [F.city]: withDefaults.city,
    [F.country]: withDefaults.country,
    [F.region]: withDefaults.region,
    [F.address]: withDefaults.address,
    [F.radarCategory]: withDefaults.radarCategory,
    [F.mapLayer]: withDefaults.mapLayer,
    [F.mapIconType]: withDefaults.mapIconType,
    [F.demandSegment]: withDefaults.demandSegment,
    [F.demandRelevance]: withDefaults.demandRelevance,
    [F.demandPattern]: withDefaults.demandPattern,
    [F.relevantHotelTypes]: withDefaults.relevantHotelTypes,
    [F.hotelDemandRationale]: withDefaults.hotelDemandRationale,
    [F.dataConfidence]: withDefaults.dataConfidence || item.dataConfidence || "Medium",
    [F.includeOnRadarMap]: withDefaults.includeOnRadarMap !== false,
    [F.visibility]: withDefaults.visibility || item.visibility || "Demo",
    [F.notes]: withDefaults.notes || item.notes || "Seeded demand anchor sample point",
    [F.source]: ["Manual Research"],
  };
  if (dealId) fields[F.dealRecordId] = dealId;
  if (marketId) fields[F.linkedMarket] = [marketId];
  return schema ? filterFieldsToAirtableSchema(fields, schema) : fields;
}

async function main() {
  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg && APPLY) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT required for --apply");

  console.log("Demand Anchors point types:", POINT_TYPES.join(", "));
  console.log("Map icon types:", MAP_ICON_TYPES.join(", "));

  const dealArg = process.argv.find((a) => a.startsWith("rec") && a.length > 10);
  const marketFlagIdx = process.argv.indexOf("--market-id");
  const marketId = marketFlagIdx >= 0 ? process.argv[marketFlagIdx + 1] : null;

  const points = samplePayload.points || [];
  console.log("\nSample market:", samplePayload.market, "—", points.length, "anchors");

  if (!APPLY) {
    for (const p of points) {
      const d = applyPointTypeDefaults(p);
      console.log(" ", d.name, "—", d.pointType);
    }
    console.log("\nPass --apply [dealId] to create records in Airtable.");
    return;
  }

  if (DRY) console.log("\n[dry-run] would create sample demand anchors");

  const tableName = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);

  let created = 0;
  for (const item of points) {
    const fields = buildAirtableFields(item, dealArg, marketId, schema);
    if (DRY) {
      console.log("WOULD CREATE", fields[F.name] || item.name);
      continue;
    }
    const rec = await cfg.base(tableName).create(fields, { typecast: true });
    created += 1;
    console.log("CREATED", rec.id, fields[F.name] || item.name);
  }

  console.log("\nDone.", created, "record(s) created.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
