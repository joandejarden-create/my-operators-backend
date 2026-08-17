#!/usr/bin/env node
/**
 * Ensure Scout Opportunity Signals table on Deal Capture Platform (AIRTABLE_BASE_ID_ALT).
 *
 *   node scripts/ensure-scout-opportunity-signals-table.mjs --dry-run
 *   node scripts/ensure-scout-opportunity-signals-table.mjs --apply
 *
 * Idempotent: creates table if missing; adds missing fields only.
 * Does not modify Hotel Census, Radar, or Brand Explorer tables.
 *
 * Requires AIRTABLE_API_KEY with schema.bases:read + schema.bases:write
 */
import "../load-env.js";
import { ensureScoutOpportunitySignalsSchema } from "../lib/scout/ensure-scout-opportunity-signals-schema.js";
import { SCOUT_OPPORTUNITY_SIGNALS_TABLE } from "../lib/scout/scout-signal-watchlist-fields.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

async function main() {
  console.log(`=== Ensure "${SCOUT_OPPORTUNITY_SIGNALS_TABLE}" (${DRY ? "DRY RUN" : "APPLY"}) ===\n`);

  const result = await ensureScoutOpportunitySignalsSchema({ apply: APPLY });

  if (result.tableCreated) {
    console.log(APPLY ? "Table created." : "[dry-run] Would create table.");
  } else if (result.tableId) {
    console.log(`Table exists (${result.tableId}).`);
  }

  if (result.fieldsCreated?.length) {
    console.log(`\nFields ${DRY ? "to create" : "created"} (${result.fieldsCreated.length}):`);
    result.fieldsCreated.forEach((f) => console.log(`  + ${f}`));
  }

  if (result.fieldsExisting?.length) {
    console.log(`\nFields already present (${result.fieldsExisting.length}).`);
  }

  if (result.errors?.length) {
    console.error("\nErrors:");
    result.errors.forEach((e) => console.error(`  • ${e}`));
  }

  if (!result.ok) {
    process.exit(1);
  }

  console.log("\nDone. Hotel Census was not modified.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
