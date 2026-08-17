#!/usr/bin/env node
/**
 * Sync Hotel Census Parent Company for non-Hilton brands from Brand Setup + Alias + manual overrides.
 *
 *   node scripts/sync-census-parent-company.mjs
 *   node scripts/sync-census-parent-company.mjs --apply
 *   node scripts/sync-census-parent-company.mjs --apply --hilton-misclassified-only
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planCensusParentCompanyCleanup } from "../lib/hotel-census/plan-census-parent-company-cleanup.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    hiltonMisclassifiedOnly: args.includes("--hilton-misclassified-only"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();

  console.log("=== Plan Census Parent Company cleanup (non-Hilton brands) ===\n");
  if (opts.hiltonMisclassifiedOnly) {
    console.log("(Scope: rows with Hilton Worldwide parent on non-Hilton brands only)\n");
  }

  const plan = await planCensusParentCompanyCleanup({
    hiltonMisclassifiedOnly: opts.hiltonMisclassifiedOnly,
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "census-parent-company-cleanup-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  const skipReasons = {};
  for (const s of plan.skipped) {
    skipReasons[s.reason] = (skipReasons[s.reason] || 0) + 1;
  }

  const byChange = new Map();
  for (const row of plan.planRows) {
    const k = `${row.affiliation} | ${row.currentParent || "(blank)"} → ${row.expectedParent || "(blank)"}`;
    byChange.set(k, (byChange.get(k) || 0) + 1);
  }

  console.log("=== Summary ===");
  console.log("  Brand Setup brands:", plan.brandSetupBrands);
  console.log("  Alias mappings:", plan.aliasMappings);
  console.log("  Hilton brands (skipped):", plan.hiltonBrandCount);
  console.log("  Census rows scanned:", plan.censusRowsScanned);
  console.log("  Ready to apply:", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length, skipReasons);
  console.log("\nTop planned changes:");
  [...byChange.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 25)
    .forEach(([k, n]) => console.log(`  ${n}x ${k}`));
  console.log("\nReport:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write Parent Company to Hotel Census.");
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
      } else updated += batch.length;
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
    } else updated += batch.length;
  }

  const logPath = join(REPORTS, "census-parent-company-cleanup-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,affiliation,currentParent,expectedParent,source\n${log
      .map((r) =>
        [
          r.censusRecordId,
          csvEscape(r.censusName),
          csvEscape(r.affiliation),
          csvEscape(r.currentParent),
          csvEscape(r.expectedParent),
          r.source,
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
