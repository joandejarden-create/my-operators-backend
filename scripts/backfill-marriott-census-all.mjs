#!/usr/bin/env node
/**
 * Backfill all Marriott Hotel Census rows:
 *   1) Bazaarvoice descriptions (bulk, server-accessible)
 *   2) Overview amenities via saved HTML in data/marriott-overview-harvest/ (optional)
 *
 *   node scripts/backfill-marriott-census-all.mjs --audit
 *   node scripts/backfill-marriott-census-all.mjs --apply
 *   node scripts/backfill-marriott-census-all.mjs --apply --descriptions-only
 *   node scripts/backfill-marriott-census-all.mjs --apply --amenities-dir=data/marriott-overview-harvest
 */
import "../load-env.js";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planMarriottCensusBazaarvoiceBackfill } from "../lib/hotel-census/plan-marriott-census-bazaarvoice-backfill.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { auditMarriottCensusFieldBlanks } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const DEFAULT_AMENITIES_DIR = join(__dirname, "..", "data", "marriott-overview-harvest");
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
    descriptionsOnly: args.includes("--descriptions-only"),
    amenitiesDir: get("--amenities-dir") || DEFAULT_AMENITIES_DIR,
    batchSize: Number(get("--batch-size") || 10),
    delayMs: Number(get("--delay-ms") || 200),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function loadAmenitiesFromDir(dirPath) {
  if (!existsSync(dirPath)) return [];
  return readdirSync(dirPath)
    .filter((f) => /\.html?$/i.test(f))
    .map((f) => {
      const html = readFileSync(join(dirPath, f), "utf8");
      const parsed = parseMarriottOverviewHtml(html);
      const marsha =
        f.match(/(?:^|[-_])([a-z]{4,6})(?:[-_.]|$)/i)?.[1]?.toUpperCase() ||
        marshaFromMarriottWebsite(html);
      if (!parsed.amenitiesText) return null;
      return {
        marshaCode: marsha,
        description: parsed.description,
        amenitiesText: parsed.amenitiesText,
        website: html.match(/https:\/\/www\.marriott\.com\/en-us\/hotels\/[a-z0-9-]+\/?/i)?.[0] || "",
        sourceFile: join(dirPath, f),
      };
    })
    .filter(Boolean);
}

async function applyPlan(base, planRows, dryRun) {
  let updated = 0;
  let errors = 0;
  let batch = [];
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!dryRun) {
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
    if (!dryRun) {
      try {
        await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
        updated += batch.length;
      } catch (err) {
        errors += batch.length;
        console.error("Batch failed:", err?.message || err);
      }
    } else updated += batch.length;
  }
  return { updated, errors };
}

async function main() {
  const opts = parseArgs();

  if (opts.audit || !opts.apply) {
    const before = await auditMarriottCensusFieldBlanks();
    console.log("=== Marriott Census (before) ===");
    console.log("Total:", before.total);
    console.log("Blank Hotel Description:", before.blankCounts["Hotel Description"]);
    console.log("Blank Amenities:", before.blankCounts.Amenities);
    if (opts.audit && !opts.apply) return;
  }

  if (!opts.apply) return;

  mkdirSync(REPORTS, { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const base = new Airtable({ apiKey }).base(baseId);

  console.log("\n=== Phase 1: Bazaarvoice descriptions ===\n");
  const descPlan = await planMarriottCensusBazaarvoiceBackfill({
    batchSize: opts.batchSize,
    delayMs: opts.delayMs,
    onProgress: (msg) => console.log(" ", msg),
  });
  writeFileSync(
    join(REPORTS, "marriott-census-bazaarvoice-plan.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), ...descPlan }, null, 2)
  );
  console.log("Bazaarvoice hits:", descPlan.bazaarvoiceHits);
  console.log("Ready (descriptions):", descPlan.readyToApply);
  console.log("Skipped:", descPlan.skipped.length);

  const descApply = await applyPlan(base, descPlan.planRows, opts.dryRun);
  console.log(`${opts.dryRun ? "Would update" : "Updated"} descriptions:`, descApply.updated);
  console.log("Description errors:", descApply.errors);

  if (opts.descriptionsOnly) {
    const after = await auditMarriottCensusFieldBlanks();
    console.log("\n=== After ===");
    console.log("Blank Hotel Description:", after.blankCounts["Hotel Description"]);
    console.log("Blank Amenities:", after.blankCounts.Amenities);
    return;
  }

  console.log("\n=== Phase 2: Overview amenities ===\n");
  const amenityExports = loadAmenitiesFromDir(opts.amenitiesDir);
  console.log("Overview HTML files with amenities:", amenityExports.length, "in", opts.amenitiesDir);

  if (!amenityExports.length) {
    console.log(
      "No overview HTML harvest found. Amenities require saved overview pages.\n" +
        "Save HTML to data/marriott-overview-harvest/{MARSHA}.html then re-run.\n" +
        "Descriptions were still applied from Bazaarvoice where available."
    );
  } else {
    const amenPlan = await planMarriottCensusContentBackfill({ contentRows: amenityExports });
    writeFileSync(
      join(REPORTS, "marriott-census-amenities-plan.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), ...amenPlan }, null, 2)
    );
    console.log("Ready (amenities):", amenPlan.readyToApply);
    const amenApply = await applyPlan(base, amenPlan.planRows, opts.dryRun);
    console.log(`${opts.dryRun ? "Would update" : "Updated"} amenities rows:`, amenApply.updated);
  }

  const after = await auditMarriottCensusFieldBlanks();
  console.log("\n=== After ===");
  console.log("Blank Hotel Description:", after.blankCounts["Hotel Description"]);
  console.log("Blank Amenities:", after.blankCounts.Amenities);

  writeFileSync(
    join(REPORTS, "marriott-census-all-backfill-log.csv"),
    `phase,recordId,name,marsha,fields\n${descPlan.planRows
      .map((r) =>
        ["description", r.censusRecordId, csvEscape(r.censusName), r.marshaCode, Object.keys(r.applyFields).join(";")].join(
          ","
        )
      )
      .join("\n")}\n`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
