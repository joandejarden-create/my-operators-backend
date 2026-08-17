#!/usr/bin/env node
/**
 * Scale Marriott Hotel Census enrichment (server-side).
 *
 *   Phase 0: Sitemap → Website + Property ID (MARSHA)
 *   Phase 1: Bazaarvoice → Hotel Description (fill-blank)
 *   Phase 2: Overview HTML harvest dir → exact Description + Amenities
 *   Phase 3: Subpage fallback → Description + Amenities from /experiences/ etc.
 *   Phase 4: Optional puppeteer overview harvest (often blocked server-side)
 *
 *   node scripts/run-marriott-census-scale-backfill.mjs --audit
 *   node scripts/run-marriott-census-scale-backfill.mjs --apply
 *   node scripts/run-marriott-census-scale-backfill.mjs --apply --skip-harvest
 */
import "../load-env.js";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import Airtable from "airtable";
import { planMarriottCensusEnrichment, auditMarriottCensusFieldBlanks } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { planMarriottCensusBazaarvoiceBackfill } from "../lib/hotel-census/plan-marriott-census-bazaarvoice-backfill.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
import { planMarriottCensusSubpageBackfill } from "../lib/hotel-census/plan-marriott-census-subpage-backfill.js";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { parseMarriottAmenitiesText } from "../lib/marriott-amenity-format.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports");
const DEFAULT_HARVEST_DIR = join(ROOT, "data", "marriott-overview-harvest");
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
    skipSitemap: args.includes("--skip-sitemap"),
    skipHarvest: args.includes("--skip-harvest"),
    skipSubpage: args.includes("--skip-subpage"),
    subpageLimit: Number(get("--subpage-limit") || 0),
    harvestLimit: Number(get("--harvest-limit") || 0),
    subpageDelayMs: Number(get("--subpage-delay-ms") || 150),
    bazaarvoiceDelayMs: Number(get("--bazaarvoice-delay-ms") || 200),
    amenitiesDir: get("--amenities-dir") || DEFAULT_HARVEST_DIR,
  };
}

function logSection(title) {
  console.log(`\n=== ${title} ===\n`);
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

function loadOverviewHarvestRows(dirPath) {
  if (!existsSync(dirPath)) return [];
  return readdirSync(dirPath)
    .filter((f) => /\.html?$/i.test(f))
    .map((f) => {
      const filePath = join(dirPath, f);
      const html = readFileSync(filePath, "utf8");
      const parsed = parseMarriottOverviewHtml(html);
      if (/access denied/i.test(html) || (!parsed.description && !parsed.amenitiesText)) return null;
      const marsha =
        f.replace(/\.html?$/i, "").toUpperCase() ||
        marshaFromMarriottWebsite(html);
      return {
        marshaCode: marsha,
        description: parsed.description,
        amenitiesText: parsed.amenitiesText,
        website: html.match(/https:\/\/www\.marriott\.com\/en-us\/hotels\/[a-z0-9-]+\/?/i)?.[0] || "",
        sourceFile: filePath,
      };
    })
    .filter(Boolean);
}

function runNodeScript(script, extraArgs = []) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [join(__dirname, script), ...extraArgs], {
      cwd: ROOT,
      stdio: "inherit",
      env: process.env,
    });
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`${script} exited ${code}`))));
  });
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  const before = await auditMarriottCensusFieldBlanks();
  logSection("Before");
  console.log("Total Marriott rows:", before.total);
  console.log("Blank Hotel Description:", before.blankCounts["Hotel Description"]);
  console.log("Blank Amenities:", before.blankCounts.Amenities);
  console.log("Blank Website:", before.blankCounts.Website);
  console.log("Blank Property ID:", before.blankCounts["Property ID"]);

  if (opts.audit && !opts.apply) return;

  if (!opts.apply) {
    console.log("\nRun with --apply to execute all phases.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  /** @type {object[]} */
  const phaseLog = [];

  if (!opts.skipSitemap) {
    logSection("Phase 0: Sitemap (Website + Property ID)");
    const sitemapPlan = await planMarriottCensusEnrichment({ minConfidence: "low" });
    writeFileSync(
      join(REPORTS, "marriott-census-sitemap-scale-plan.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), ...sitemapPlan }, null, 2)
    );
    console.log("Ready:", sitemapPlan.readyToApply, "Skipped:", sitemapPlan.skipped.length);
    const sitemapApply = await applyPlan(base, sitemapPlan.planRows, opts.dryRun);
    console.log(`${opts.dryRun ? "Would update" : "Updated"}:`, sitemapApply.updated);
    phaseLog.push({ phase: "sitemap", ...sitemapApply, ready: sitemapPlan.readyToApply });
  }

  logSection("Phase 1: Bazaarvoice descriptions");
  const descPlan = await planMarriottCensusBazaarvoiceBackfill({
    delayMs: opts.bazaarvoiceDelayMs,
    onProgress: (msg) => console.log(" ", msg),
  });
  writeFileSync(
    join(REPORTS, "marriott-census-bazaarvoice-scale-plan.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), ...descPlan }, null, 2)
  );
  console.log("Bazaarvoice hits:", descPlan.bazaarvoiceHits);
  console.log("Ready:", descPlan.readyToApply);
  const descApply = await applyPlan(base, descPlan.planRows, opts.dryRun);
  console.log(`${opts.dryRun ? "Would update" : "Updated"} descriptions:`, descApply.updated);
  phaseLog.push({ phase: "bazaarvoice", ...descApply, ready: descPlan.readyToApply });

  logSection("Phase 2: Overview HTML harvest import");
  const harvestRows = loadOverviewHarvestRows(opts.amenitiesDir);
  console.log("Valid overview HTML files:", harvestRows.length, "in", opts.amenitiesDir);
  for (const row of harvestRows.slice(0, 5)) {
    console.log(
      `  ${row.marshaCode} desc=${row.description.length} amen=${parseMarriottAmenitiesText(row.amenitiesText).length}`
    );
  }
  if (harvestRows.length) {
    const overviewPlan = await planMarriottCensusContentBackfill({ contentRows: harvestRows });
    writeFileSync(
      join(REPORTS, "marriott-census-overview-import-scale-plan.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), ...overviewPlan }, null, 2)
    );
    console.log("Ready:", overviewPlan.readyToApply, "Skipped:", overviewPlan.skipped.length);
    const overviewApply = await applyPlan(base, overviewPlan.planRows, opts.dryRun);
    console.log(`${opts.dryRun ? "Would update" : "Updated"} overview imports:`, overviewApply.updated);
    phaseLog.push({ phase: "overview_html", ...overviewApply, ready: overviewPlan.readyToApply });
  }

  if (!opts.skipSubpage) {
    logSection("Phase 3: Subpage fallback (marriott.com subpages)");
    console.log("Note: subpage amenities are not identical to /overview/ chip lists.");
    const subPlan = await planMarriottCensusSubpageBackfill({
      limit: opts.subpageLimit,
      fetchDelayMs: opts.subpageDelayMs,
      amenitiesOnly: false,
      onProgress: (msg) => console.log(" ", msg),
    });
    writeFileSync(
      join(REPORTS, "marriott-census-subpage-scale-plan.json"),
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          readyToApply: subPlan.readyToApply,
          skippedSample: subPlan.skipped.slice(0, 50),
          skippedCount: subPlan.skipped.length,
          fetchErrors: subPlan.fetchErrors.slice(0, 20),
          planRowsSample: subPlan.planRows.slice(0, 10).map((r) => ({
            censusRecordId: r.censusRecordId,
            censusName: r.censusName,
            marsha: r.marshaCode,
            fields: Object.keys(r.applyFields),
            amenitiesPreview: r.amenitiesTextSuggested?.slice(0, 120),
          })),
        },
        null,
        2
      )
    );
    console.log("Ready:", subPlan.readyToApply, "Skipped:", subPlan.skipped.length);
    const subApply = await applyPlan(base, subPlan.planRows, opts.dryRun);
    console.log(`${opts.dryRun ? "Would update" : "Updated"} subpage rows:`, subApply.updated);
    phaseLog.push({ phase: "subpage", ...subApply, ready: subPlan.readyToApply });
  }

  if (!opts.skipHarvest) {
    logSection("Phase 4: Puppeteer overview harvest");
    console.log("Attempting headless harvest (often Access Denied server-side)…");
    const harvestArgs = ["--headless", "--apply-import"];
    if (opts.harvestLimit > 0) harvestArgs.push(`--limit=${opts.harvestLimit}`);
    try {
      await runNodeScript("harvest-marriott-overview-pages.mjs", harvestArgs);
      phaseLog.push({ phase: "harvest", status: "completed" });
    } catch (err) {
      console.warn("Harvest phase warning:", err?.message || err);
      phaseLog.push({ phase: "harvest", status: "failed", error: String(err?.message || err) });
    }
  }

  const after = await auditMarriottCensusFieldBlanks();
  logSection("After");
  console.log("Blank Hotel Description:", after.blankCounts["Hotel Description"]);
  console.log("Blank Amenities:", after.blankCounts.Amenities);
  console.log("Blank Website:", after.blankCounts.Website);
  console.log("Blank Property ID:", after.blankCounts["Property ID"]);

  writeFileSync(
    join(REPORTS, "marriott-census-scale-backfill-summary.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        before: before.blankCounts,
        after: after.blankCounts,
        phases: phaseLog,
      },
      null,
      2
    )
  );
  console.log("\nSummary:", join(REPORTS, "marriott-census-scale-backfill-summary.json"));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
