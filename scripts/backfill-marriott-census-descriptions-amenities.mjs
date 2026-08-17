#!/usr/bin/env node
/**
 * Backfill Marriott Hotel Census description + amenities from marriott.com overview pages.
 *
 * Overview is Akamai-blocked for plain fetch — defaults to puppeteer with plain-fetch fallback chain.
 *
 *   node scripts/backfill-marriott-census-descriptions-amenities.mjs --pilot=POPLC
 *   node scripts/backfill-marriott-census-descriptions-amenities.mjs --audit
 *   node scripts/backfill-marriott-census-descriptions-amenities.mjs --apply --limit=25
 *   node scripts/backfill-marriott-census-descriptions-amenities.mjs --file=exports/poplc-overview.html --apply
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  fetchMarriottHotelContent,
  normalizeMarriottContentExport,
  parseMarriottOverviewHtml,
} from "../lib/marriott-hotel-content-fetch.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
import { auditMarriottCensusFieldBlanks } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
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
    pilot: get("--pilot"),
    file: get("--file"),
    limit: Number(get("--limit") || 0),
    fetchDelayMs: Number(get("--fetch-delay-ms") || 1500),
    noPuppeteer: args.includes("--no-puppeteer"),
    plainOnly: args.includes("--plain-only"),
    marsha: get("--marsha"),
    url: get("--url"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function runPilot(pilot) {
  const code = String(pilot).trim().toUpperCase();
  const url =
    parseArgs().url ||
    `https://www.marriott.com/en-us/hotels/${code.toLowerCase()}/overview/`;
  console.log(`=== Pilot fetch: ${code} ===\n`, url);
  const result = await fetchMarriottHotelContent(url, {
    marshaCode: code,
    usePuppeteer: !parseArgs().plainOnly,
    fallbackPuppeteer: !parseArgs().noPuppeteer,
  });
  console.log(JSON.stringify(result, null, 2));
  if (result.description) {
    console.log("\n--- Description ---\n", result.description);
  }
  if (result.amenities?.length) {
    console.log("\n--- Amenities ---\n", result.amenities.join("\n"));
  }
}

async function runFileParse(file) {
  const raw = readFileSync(file, "utf8");
  let payload = raw;
  if (file.endsWith(".json")) {
    payload = JSON.parse(raw);
  }
  const rows = normalizeMarriottContentExport(payload);
  if (!rows.length) {
    const parsed = parseMarriottOverviewHtml(raw);
    console.log("Parsed HTML:", parsed);
    return parsed;
  }
  console.log("Parsed export rows:", rows.length);
  console.log(JSON.stringify(rows.slice(0, 3), null, 2));
  return rows;
}

async function main() {
  const opts = parseArgs();

  if (opts.pilot) {
    await runPilot(opts.pilot);
    return;
  }

  if (opts.file && !opts.apply) {
    await runFileParse(opts.file);
    return;
  }

  if (opts.audit || (!opts.apply && !opts.file)) {
    const audit = await auditMarriottCensusFieldBlanks();
    console.log("=== Marriott Census Content Blanks ===\n");
    console.log("Total Marriott rows:", audit.total);
    console.log("Blank Hotel Description:", audit.blankCounts["Hotel Description"]);
    console.log("Blank Amenities:", audit.blankCounts.Amenities);
    if (!opts.apply) {
      console.log("\nRun --pilot=POPLC to test one hotel.");
      console.log("Run --apply --limit=N to backfill (uses puppeteer by default).");
      console.log("Or save overview HTML/JSON and run --file=path --apply.");
    }
    if (opts.audit && !opts.apply) return;
  }

  if (!opts.apply) return;

  /** @type {ReturnType<typeof normalizeMarriottContentExport>} */
  let contentRows = [];
  if (opts.file) {
    const raw = readFileSync(opts.file, "utf8");
    contentRows = normalizeMarriottContentExport(opts.file.endsWith(".json") ? JSON.parse(raw) : raw);
  }

  const plan = await planMarriottCensusContentBackfill({
    contentRows: contentRows.length ? contentRows : undefined,
    limit: opts.limit,
    fetchDelayMs: opts.fetchDelayMs,
    usePuppeteer: !opts.plainOnly,
    fallbackPuppeteer: !opts.noPuppeteer,
    marshaCodes: opts.marsha ? [opts.marsha] : undefined,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "marriott-census-content-backfill-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  console.log("\n=== Summary ===");
  console.log("  Scanned:", plan.censusRowsScanned);
  console.log("  Ready to apply:", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length);
  console.log("  Fetch errors:", plan.fetchErrors.length);
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

  const logPath = join(REPORTS, "marriott-census-content-backfill-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,marsha,fields,descriptionLen,amenitiesLen\n${log
      .map((r) =>
        [
          r.censusRecordId,
          csvEscape(r.censusName),
          r.marshaCode,
          Object.keys(r.applyFields).join(";"),
          (r.descriptionSuggested || "").length,
          (r.amenitiesTextSuggested || "").length,
        ].join(",")
      )
      .join("\n")}\n`
  );

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
  console.log("Errors:", errors);
  console.log("Log:", logPath);

  const after = await auditMarriottCensusFieldBlanks();
  console.log("\n=== Blank counts (after) ===");
  console.log("  Hotel Description:", after.blankCounts["Hotel Description"]);
  console.log("  Amenities:", after.blankCounts.Amenities);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
