#!/usr/bin/env node
/**
 * Seed Demand Categories reference table.
 *   node scripts/seed-demand-categories.mjs
 *   node scripts/seed-demand-categories.mjs --dry-run
 */
import "../load-env.js";
import {
  DEMAND_CATEGORIES_TABLE,
  DEMAND_CATEGORY_FIELDS,
} from "../lib/market-demand/airtable-market-demand-fields.js";
import { getMarketDemandAirtableConfig } from "../lib/market-demand/market-demand-base.js";
import { DEMAND_CATEGORIES_SEED_ROWS } from "../lib/market-demand/demand-categories-seed-data.js";

const DRY = process.argv.includes("--dry-run");

async function main() {
  const mdCfg = getMarketDemandAirtableConfig();
  if (!mdCfg) {
    throw new Error("AIRTABLE_BASE_ID_ALT (or AIRTABLE_MARKET_DEMAND_BASE_ID) and AIRTABLE_API_KEY required");
  }

  const base = mdCfg.base;
  const existing = await base(DEMAND_CATEGORIES_TABLE).select().all();
  const byCategory = new Map(
    existing.map((r) => [String(r.fields[DEMAND_CATEGORY_FIELDS.category] || "").trim(), r])
  );

  let created = 0;
  let updated = 0;
  let skipped = 0;

  for (const row of DEMAND_CATEGORIES_SEED_ROWS) {
    const fields = {
      [DEMAND_CATEGORY_FIELDS.category]: row.category,
      [DEMAND_CATEGORY_FIELDS.description]: row.description,
      [DEMAND_CATEGORY_FIELDS.typicalDemandPattern]: row.typicalDemandPattern,
      [DEMAND_CATEGORY_FIELDS.mostRelevantHotelTypes]: row.mostRelevantHotelTypes,
      [DEMAND_CATEGORY_FIELDS.brandFitImplications]: row.brandFitImplications,
      [DEMAND_CATEGORY_FIELDS.operatorFitImplications]: row.operatorFitImplications,
      [DEMAND_CATEGORY_FIELDS.scoringWeight]: row.scoringWeight,
    };

    const hit = byCategory.get(row.category);
    if (hit) {
      if (DRY) {
        console.log("WOULD UPDATE", row.category);
        updated += 1;
        continue;
      }
      await base(DEMAND_CATEGORIES_TABLE).update(hit.id, fields, { typecast: true });
      console.log("UPDATED", row.category, hit.id);
      updated += 1;
    } else {
      if (DRY) {
        console.log("WOULD CREATE", row.category);
        created += 1;
        continue;
      }
      const rec = await base(DEMAND_CATEGORIES_TABLE).create(fields, { typecast: true });
      console.log("CREATED", row.category, rec.id);
      created += 1;
    }
  }

  console.log("\nDone.", { created, updated, skipped, totalSeedRows: DEMAND_CATEGORIES_SEED_ROWS.length });
  if (DRY) console.log("Dry run only — re-run without --dry-run to apply.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
