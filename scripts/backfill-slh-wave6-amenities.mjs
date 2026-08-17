#!/usr/bin/env node
/**
 * Wave 6: fill-blank SLH Amenities from official SLH catalog.
 *
 *   node scripts/backfill-slh-wave6-amenities.mjs
 *   node scripts/backfill-slh-wave6-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { planSlhCensusAmenitiesBackfill } from "../lib/hotel-census/plan-slh-census-amenities-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const APPLY = process.argv.includes("--apply");

async function main() {
  mkdirSync("reports", { recursive: true });
  const plan = await planSlhCensusAmenitiesBackfill({
    onProgress: (m) => console.log(" ", m),
  });
  writeFileSync(
    "reports/slh-wave6-amenities-plan.json",
    JSON.stringify(
      { generatedAt: new Date().toISOString(), mode: APPLY ? "apply" : "dry-run", ...plan },
      null,
      2
    )
  );
  console.log("Ready:", plan.readyToApply, "Skipped:", plan.skipped?.length);
  for (const r of (plan.planRows || []).slice(0, 10)) {
    console.log(" ", r.censusName || r.name, Object.keys(r.applyFields || {}).join("+"));
  }
  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of plan.planRows || []) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/slh-wave6-amenities-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows: plan.planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
