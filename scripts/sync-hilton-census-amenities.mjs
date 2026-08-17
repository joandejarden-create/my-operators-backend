#!/usr/bin/env node
/**
 * Sync Hotel Census Amenities for Hilton-parent rows from hilton.com directory data.
 *
 *   node scripts/sync-hilton-census-amenities.mjs
 *   node scripts/sync-hilton-census-amenities.mjs --apply
 *   node scripts/sync-hilton-census-amenities.mjs --apply --refresh-index
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planHiltonCensusAmenitiesSync } from "../lib/hotel-census/plan-hilton-census-amenities-sync.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    refreshIndex: args.includes("--refresh-index"),
    fillBlankOnly: args.includes("--fill-blank-only"),
    crawlDelayMs: Number(get("--crawl-delay-ms") || 200),
    brandCodes: get("--brand-codes")
      ? get("--brand-codes")
          .split(",")
          .map((s) => s.trim().toUpperCase())
      : null,
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();

  console.log("=== Plan Hilton Census Amenities sync ===\n");
  if (opts.refreshIndex) console.log("(Refreshing directory index from hilton.com — may take several minutes)\n");

  const plan = await planHiltonCensusAmenitiesSync({
    refreshCrawl: opts.refreshIndex,
    crawlDelayMs: opts.crawlDelayMs,
    brandCodes: opts.brandCodes,
    fillBlankOnly: opts.fillBlankOnly,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "hilton-census-amenities-sync-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  const skipReasons = {};
  for (const s of plan.skipped) {
    skipReasons[s.reason] = (skipReasons[s.reason] || 0) + 1;
  }

  console.log("\n=== Summary ===");
  console.log("  Directory index size:", plan.indexSize);
  console.log("  Census rows scanned:", plan.censusRowsScanned);
  console.log("  Ready to apply:", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length, skipReasons);
  console.log("Report:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write Amenities to Hotel Census.");
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
    log.push({
      recordId: row.censusRecordId,
      name: row.censusName,
      ctyhocn: row.ctyhocn,
      source: row.source,
    });
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

  const logPath = join(REPORTS, "hilton-census-amenities-sync-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,ctyhocn,source\n${log.map((r) => [r.recordId, csvEscape(r.name), r.ctyhocn, r.source].join(",")).join("\n")}\n`
  );

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
  console.log("Errors:", errors);
  console.log("Log:", logPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
