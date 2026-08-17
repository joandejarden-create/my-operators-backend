#!/usr/bin/env node
/**
 * Backfill Hilton website URLs (+ status/description where blank) for census rows missing hilton.com links.
 *
 *   node scripts/backfill-hilton-census-website.mjs
 *   node scripts/backfill-hilton-census-website.mjs --open-only
 *   node scripts/backfill-hilton-census-website.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  loadHiltonRowsMissingWebsite,
  planHiltonWebsiteBackfill,
} from "../lib/hotel-census/plan-hilton-website-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    openOnly: args.includes("--open-only"),
    fetchDelayMs: (() => {
      const i = args.indexOf("--fetch-delay-ms");
      return i >= 0 ? Number(args[i + 1] || 200) : 200;
    })(),
  };
}

async function main() {
  const opts = parseArgs();
  console.log("=== Plan Hilton website backfill ===\n");

  const rows = await loadHiltonRowsMissingWebsite({ openOnly: opts.openOnly });
  console.log("Rows missing hilton.com website:", rows.length, opts.openOnly ? "(open only)" : "");

  const plan = await planHiltonWebsiteBackfill(rows, {
    fetchDelayMs: opts.fetchDelayMs,
    pageDelayMs: 150,
    minNameSim: 0.5,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "hilton-census-website-backfill-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  console.log("\n=== Summary ===");
  console.log("  Ready:", plan.planRows.length);
  console.log("  Skipped:", plan.skipped.length);
  console.log("Report:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write to Airtable.");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const base = new Airtable({ apiKey }).base(baseId);

  let updated = 0;
  let batch = [];
  for (const row of plan.planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated} records`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
