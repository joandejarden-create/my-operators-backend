#!/usr/bin/env node
/**
 * Inherit Hotel Service Model on branded Hotel Census rows from Brand Setup - Brand Basics.
 * Only rows whose Affiliation maps to a Brand Setup brand with Hotel Service Model populated.
 *
 *   node scripts/sync-census-brand-service-model.mjs
 *   node scripts/sync-census-brand-service-model.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planCensusBrandServiceModelBackfill } from "../lib/hotel-census/plan-census-brand-service-model-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();

  console.log("=== Plan Hotel Census Service Model (Brand Setup inheritance) ===\n");
  console.log("Scope: branded rows mapped to Brand Setup with Hotel Service Model set.");
  console.log("Mode: fill-blank only (existing census values are not overwritten).\n");

  const plan = await planCensusBrandServiceModelBackfill({ fillBlankOnly: true });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "census-brand-service-model-backfill-plan.json");
  writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2)
  );

  const skipReasons = {};
  for (const s of plan.skipped) {
    skipReasons[s.reason] = (skipReasons[s.reason] || 0) + 1;
  }

  const byBrand = new Map();
  for (const row of plan.planRows) {
    const k = `${row.canonicalBrand} → ${row.expectedServiceModel}`;
    byBrand.set(k, (byBrand.get(k) || 0) + 1);
  }

  console.log("=== Summary ===");
  console.log("  Brand Setup rows scanned:", plan.brandSetupRowsScanned);
  console.log("  Brand Setup brands with service model:", plan.brandSetupMappedBrands);
  console.log("  Alias / affiliation mappings:", plan.aliasMappings);
  console.log("  Census rows scanned:", plan.censusRowsScanned);
  console.log("  Ready to apply (blank census field):", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length, skipReasons);

  if (plan.invalidBrandSetupValues.length) {
    console.log("\nBrand Setup values not in census schema (skipped at source):");
    plan.invalidBrandSetupValues.slice(0, 15).forEach(({ brand, raw }) => {
      console.log(`  ${brand}: ${JSON.stringify(raw)}`);
    });
    if (plan.invalidBrandSetupValues.length > 15) {
      console.log(`  … and ${plan.invalidBrandSetupValues.length - 15} more`);
    }
  }

  console.log("\nTop planned inheritances:");
  [...byBrand.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .forEach(([k, n]) => console.log(`  ${n}x ${k}`));

  console.log("\nReport:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write Hotel Service Model to Hotel Census.");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const base = new Airtable({ apiKey }).base(baseId);

  let updated = 0;
  let errors = 0;
  let batch = [];
  const log = [];

  for (const row of plan.planRows) {
    log.push(row);
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!opts.dryRun) {
        try {
          await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
          updated += batch.length;
        } catch (err) {
          errors += batch.length;
          console.error("Batch failed:", err?.message || err);
        }
      } else {
        updated += batch.length;
      }
      batch = [];
    }
  }

  if (batch.length) {
    if (!opts.dryRun) {
      try {
        await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
        updated += batch.length;
      } catch (err) {
        errors += batch.length;
        console.error("Batch failed:", err?.message || err);
      }
    } else {
      updated += batch.length;
    }
  }

  const logPath = join(REPORTS, "census-brand-service-model-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,affiliation,canonicalBrand,currentServiceModel,expectedServiceModel,matchSource\n${log
      .map((r) =>
        [
          r.censusRecordId,
          csvEscape(r.censusName),
          csvEscape(r.affiliation),
          csvEscape(r.canonicalBrand),
          csvEscape(r.currentServiceModel),
          csvEscape(r.expectedServiceModel),
          r.matchSource,
        ].join(",")
      )
      .join("\n")}\n`
  );

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
  console.log("Errors:", errors);
  console.log("Log:", logPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
