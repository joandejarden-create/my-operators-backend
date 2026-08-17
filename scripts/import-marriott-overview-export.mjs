#!/usr/bin/env node
/**
 * Import browser-saved Marriott overview HTML files into census description + amenities.
 *
 * File naming (any works if content includes marsha in URL/meta):
 *   POPLC.html
 *   poplc-the-ocean-club-overview.html
 *   manifest.json  [{ "marsha": "POPLC", "file": "poplc.html", "website": "..." }]
 *
 * Save HTML from Chrome: open overview tab → DevTools → Elements → <html> → Copy outerHTML
 * or File → Save Page As → "Webpage, HTML only".
 *
 *   node scripts/import-marriott-overview-export.mjs --dir exports/marriott-overviews
 *   node scripts/import-marriott-overview-export.mjs --file exports/POPLC.html --apply
 *   node scripts/import-marriott-overview-export.mjs --dir exports/marriott-overviews --apply --dry-run
 */
import "../load-env.js";
import { readdirSync, readFileSync, statSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, basename } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  parseMarriottOverviewHtml,
} from "../lib/marriott-hotel-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { planMarriottCensusContentBackfill } from "../lib/hotel-census/plan-marriott-census-content-backfill.js";
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
    file: get("--file"),
    dir: get("--dir"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
  };
}

function marshaFromFileName(name) {
  const base = basename(name).replace(/\.(html?|json)$/i, "");
  const upper = base.match(/(?:^|[-_])([a-z]{4,6})(?:[-_.]|$)/i);
  if (upper) return upper[1].toUpperCase();
  const lead = base.match(/^([a-z]{4,6})\b/i);
  return lead ? lead[1].toUpperCase() : "";
}

/**
 * @param {string} filePath
 */
function parseOverviewFile(filePath) {
  const html = readFileSync(filePath, "utf8");
  const parsed = parseMarriottOverviewHtml(html);
  const marsha =
    marshaFromFileName(filePath) ||
    marshaFromMarriottWebsite(html.match(/https:\/\/www\.marriott\.com[^"'\\]+/i)?.[0] || "");
  const websiteMatch = html.match(/https:\/\/www\.marriott\.com\/en-us\/hotels\/[a-z0-9-]+\/?/i);
  return {
    marshaCode: marsha,
    description: parsed.description,
    amenitiesText: parsed.amenitiesText,
    website: websiteMatch ? websiteMatch[0] : "",
    sourceFile: filePath,
    parseErrors: parsed.parseErrors,
  };
}

/**
 * @param {string} dirPath
 */
function loadOverviewDir(dirPath) {
  const manifestPath = join(dirPath, "manifest.json");
  if (statSync(manifestPath, { throwIfNoEntry: false })?.isFile()) {
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    const rows = Array.isArray(manifest) ? manifest : manifest.hotels || [];
    return rows.map((row) => {
      const filePath = join(dirPath, row.file || row.path);
      const parsed = parseOverviewFile(filePath);
      return {
        ...parsed,
        marshaCode: String(row.marsha || row.marshaCode || parsed.marshaCode).toUpperCase(),
        website: row.website || parsed.website,
      };
    });
  }

  return readdirSync(dirPath)
    .filter((f) => /\.html?$/i.test(f))
    .map((f) => parseOverviewFile(join(dirPath, f)))
    .filter((r) => r.description || r.amenitiesText);
}

async function main() {
  const opts = parseArgs();
  if (!opts.file && !opts.dir) {
    console.error("Usage: --file path/to/overview.html | --dir path/to/folder [--apply]");
    process.exit(1);
  }

  const contentRows = opts.dir ? loadOverviewDir(opts.dir) : [parseOverviewFile(opts.file)];
  const usable = contentRows.filter((r) => r.description || r.amenitiesText);
  console.log("Parsed overview files:", usable.length);
  for (const row of usable.slice(0, 5)) {
    console.log(
      `  ${row.marshaCode || "?"} desc=${row.description.length} chars amenities=${row.amenitiesText.split(",").filter(Boolean).length}`
    );
  }
  if (!usable.length) {
    console.error("No description/amenities parsed from export files.");
    process.exit(1);
  }

  const plan = await planMarriottCensusContentBackfill({ contentRows: usable });
  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "marriott-overview-import-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  console.log("\nReady to apply:", plan.readyToApply);
  console.log("Skipped:", plan.skipped.length);
  console.log("Report:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write Hotel Description + Amenities (fill-blank only).");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
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
  console.log(`${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
