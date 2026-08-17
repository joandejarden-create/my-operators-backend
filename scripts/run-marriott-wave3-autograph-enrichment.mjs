#!/usr/bin/env node
/**
 * Wave 3 Marriott: fill-blank Website + Property ID for Autograph Collection only.
 *
 *   node scripts/run-marriott-wave3-autograph-enrichment.mjs
 *   node scripts/run-marriott-wave3-autograph-enrichment.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { planMarriottCensusEnrichment } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const APPLY = process.argv.includes("--apply");
const AFFILIATION = "Autograph Collection";
const BATCH = 10;

async function main() {
  mkdirSync("reports", { recursive: true });
  console.log("=== Marriott Wave3 Autograph Website/ID ===\n");

  const plan = await planMarriottCensusEnrichment({
    minConfidence: "medium",
    onProgress: (m) => console.log(" ", m),
  });

  const filtered = (plan.planRows || []).filter(
    (r) => String(r.affiliation || "").trim() === AFFILIATION && Object.keys(r.applyFields || {}).length
  );

  writeFileSync(
    "reports/marriott-wave3-autograph-enrichment-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        affiliation: AFFILIATION,
        readyToApply: filtered.length,
        planRows: filtered,
        fieldMapping: plan.fieldMapping,
      },
      null,
      2
    )
  );

  console.log("Autograph ready:", filtered.length);
  for (const r of filtered.slice(0, 15)) {
    console.log(" ", r.censusName, Object.keys(r.applyFields), r.matchConfidence);
  }

  if (!APPLY) {
    console.log("\nDRY-RUN — re-run with --apply after review.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let updated = 0;
  let batch = [];
  const log = [];
  async function flush() {
    if (!batch.length) return;
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of filtered) {
    // Require Website when blank was being filled — trust plan applyFields
    log.push(row);
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) await flush();
  }
  await flush();
  writeFileSync(
    "reports/marriott-wave3-autograph-enrichment-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, rows: log }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
