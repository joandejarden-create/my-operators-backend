#!/usr/bin/env node
/**
 * Sync Hotel Census fields for Marriott-parent rows from marriott.com country sitemaps.
 *
 * Phase 1 (works from server): Website, Property ID (MARSHA), names via sitemap match.
 * Descriptions/amenities: marriott.com overview pages are Akamai-blocked for server fetch.
 * Use import-marriott-overview-export.mjs with browser-saved overview HTML, or
 * backfill-marriott-census-descriptions-amenities.mjs --file=overview.html --apply
 *
 *   node scripts/sync-marriott-census-from-sitemap.mjs --audit
 *   node scripts/sync-marriott-census-from-sitemap.mjs
 *   node scripts/sync-marriott-census-from-sitemap.mjs --apply
 *   node scripts/sync-marriott-census-from-sitemap.mjs --apply --countries=dominican-republic,mexico
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  auditMarriottCensusFieldBlanks,
  planMarriottCensusEnrichment,
} from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    audit: args.includes("--audit"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    crawlDelayMs: Number(get("--crawl-delay-ms") || 300),
    countries: get("--countries")
      ? get("--countries")
          .split(",")
          .map((s) => s.trim().toLowerCase())
      : null,
    minConfidence: get("--min-confidence") || "low",
    skipGlobalRescue: args.includes("--skip-global-rescue"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();

  if (opts.audit || (!opts.apply && !opts.audit)) {
    const audit = await auditMarriottCensusFieldBlanks();
    console.log("=== Marriott Census Field Blanks ===\n");
    console.log("Total Marriott rows:", audit.total);
    console.log("Writable fields:", audit.writable.join(", "));
    console.log("Country sitemaps derivable:", audit.countrySlugsAvailable.join(", "));
    console.log("\nBlank counts:");
    for (const [k, v] of Object.entries(audit.blankCounts)) {
      console.log(`  ${k}: ${v}`);
    }
    if (!opts.apply) {
      console.log("\nRun with --apply to fill blanks from marriott.com country sitemaps.");
      console.log(
        "Note: Hotel Description / Amenities need overview data — use import-marriott-search-export.mjs after saving browser JSON."
      );
    }
    if (opts.audit && !opts.apply) return;
  }

  if (!opts.apply) return;

  console.log("\n=== Plan Marriott census enrichment ===\n");
  const plan = await planMarriottCensusEnrichment({
    countrySlugs: opts.countries,
    crawlDelayMs: opts.crawlDelayMs,
    minConfidence: opts.minConfidence,
    skipGlobalRescue: opts.skipGlobalRescue,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "marriott-census-enrichment-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  console.log("\n=== Summary ===");
  console.log("  Country sitemaps:", plan.countrySlugs.join(", "));
  if (plan.crawlSummary) {
    console.log("  Sitemap pages fetched:", plan.crawlSummary.countryPagesFetched);
    console.log("  Directory hotels:", plan.crawlSummary.hotelsFound);
    if (plan.crawlSummary.fetchErrors?.length) {
      console.log("  Crawl errors:", plan.crawlSummary.fetchErrors.length);
    }
  }
  console.log("  Census rows scanned:", plan.censusRowsScanned);
  console.log("  Ready to apply:", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length);
  console.log("Report:", jsonPath);

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

  const logPath = join(REPORTS, "marriott-census-enrichment-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,marsha,matchConfidence,fields\n${log
      .map((r) =>
        [
          r.censusRecordId,
          csvEscape(r.censusName),
          r.marshaCode,
          r.matchConfidence,
          Object.keys(r.applyFields).join(";"),
        ].join(",")
      )
      .join("\n")}\n`
  );

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
  console.log("Errors:", errors);
  console.log("Log:", logPath);

  const after = await auditMarriottCensusFieldBlanks();
  console.log("\n=== Blank counts (after) ===");
  for (const [k, v] of Object.entries(after.blankCounts)) {
    console.log(`  ${k}: ${v}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
