#!/usr/bin/env node
/**
 * Seed Market Demand sample records for one deal (Platform base + MVP deal id).
 *
 *   node scripts/seed-market-demand-sample.mjs [dealId]
 */
import "../load-env.js";
import {
  DEMAND_CENTERS_TABLE,
  NEARBY_HOTEL_SUPPLY_TABLE,
  MARKETS_TABLE,
  DEMAND_CENTER_FIELDS,
  NEARBY_HOTEL_SUPPLY_FIELDS,
  MARKET_FIELDS,
  MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
  DEALS_TABLE,
} from "../lib/market-demand/airtable-market-demand-fields.js";
import {
  getMarketDemandAirtableConfig,
  getDealsAirtableConfig,
} from "../lib/market-demand/market-demand-base.js";
import { SAMPLE_IMPORT_PAYLOAD } from "../lib/market-demand/sample-import-payload.js";

async function main() {
  const mdCfg = getMarketDemandAirtableConfig();
  const dealsCfg = getDealsAirtableConfig();
  if (!mdCfg) {
    throw new Error("AIRTABLE_BASE_ID_ALT (or AIRTABLE_MARKET_DEMAND_BASE_ID) and AIRTABLE_API_KEY required");
  }
  if (!dealsCfg) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  let dealId = process.argv[2];
  if (!dealId) {
    const deals = await dealsCfg.base(DEALS_TABLE).select({ maxRecords: 1 }).firstPage();
    dealId = deals[0]?.id;
  }
  if (!dealId?.startsWith("rec")) throw new Error("Valid dealId required");

  await dealsCfg.base(DEALS_TABLE).find(dealId);
  console.log("Seeding for deal:", dealId, "(Platform base:", mdCfg.baseId + ")");

  const base = mdCfg.base;
  const market = await base(MARKETS_TABLE).create(
    {
      [MARKET_FIELDS.name]: "Sample Validation Market",
      [MARKET_FIELDS.country]: "United States",
      [MARKET_FIELDS.region]: "Southeast",
      [MARKET_FIELDS.marketType]: "Urban",
      [MARKET_FIELDS.dataConfidence]: "Medium",
    },
    { typecast: true }
  );
  console.log("Created market:", market.id);

  const createdCenters = [];
  for (const item of SAMPLE_IMPORT_PAYLOAD.demandCenters) {
    const rec = await base(DEMAND_CENTERS_TABLE).create(
      {
        [MARKET_DEMAND_DEAL_RECORD_ID_FIELD]: dealId,
        [DEMAND_CENTER_FIELDS.name]: item.name,
        [DEMAND_CENTER_FIELDS.linkedMarket]: [market.id],
        [DEMAND_CENTER_FIELDS.demandCategory]: item.category,
        [DEMAND_CENTER_FIELDS.demandSubcategory]: item.subcategory,
        [DEMAND_CENTER_FIELDS.distanceFromDeal]: item.distanceFromDeal,
        [DEMAND_CENTER_FIELDS.estimatedDriveTime]: item.estimatedDriveTime,
        [DEMAND_CENTER_FIELDS.demandStrength]: item.demandStrength,
        [DEMAND_CENTER_FIELDS.relevanceToHotelDemand]: item.relevanceToHotelDemand,
        [DEMAND_CENTER_FIELDS.demandPattern]: item.demandPattern,
        [DEMAND_CENTER_FIELDS.relevantHotelTypes]: item.relevantHotelTypes,
        [DEMAND_CENTER_FIELDS.source]: item.source,
        [DEMAND_CENTER_FIELDS.dataConfidence]: item.dataConfidence,
        [DEMAND_CENTER_FIELDS.relevanceScore]: item.relevanceScore,
        [DEMAND_CENTER_FIELDS.notes]: item.notes,
      },
      { typecast: true }
    );
    createdCenters.push(rec.id);
    console.log("Created demand center:", rec.id, item.name);
  }

  const hotel = await base(NEARBY_HOTEL_SUPPLY_TABLE).create(
    {
      [NEARBY_HOTEL_SUPPLY_FIELDS.hotelName]: "Sample Comp Hotel — Airport Corridor",
      [MARKET_DEMAND_DEAL_RECORD_ID_FIELD]: dealId,
      [NEARBY_HOTEL_SUPPLY_FIELDS.linkedMarket]: [market.id],
      [NEARBY_HOTEL_SUPPLY_FIELDS.brand]: "Sample Brand",
      [NEARBY_HOTEL_SUPPLY_FIELDS.parentCompany]: "Sample Parent Co",
      [NEARBY_HOTEL_SUPPLY_FIELDS.chainScale]: "Upper Midscale",
      [NEARBY_HOTEL_SUPPLY_FIELDS.hotelType]: "Select-Service",
      [NEARBY_HOTEL_SUPPLY_FIELDS.rooms]: 142,
      [NEARBY_HOTEL_SUPPLY_FIELDS.distanceFromDeal]: 2.4,
      [NEARBY_HOTEL_SUPPLY_FIELDS.estimatedDriveTime]: 8,
      [NEARBY_HOTEL_SUPPLY_FIELDS.competitiveRelevance]: "Primary",
      [NEARBY_HOTEL_SUPPLY_FIELDS.source]: "Manual Research",
      [NEARBY_HOTEL_SUPPLY_FIELDS.dataConfidence]: "Medium",
    },
    { typecast: true }
  );
  console.log("Created nearby hotel:", hotel.id);

  console.log("\nDone. Demand centers:", createdCenters.join(", "));
  console.log("Run: node scripts/validate-market-demand-live.mjs", dealId);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
