/**
 * Phase 2F — Brand directory discovery dry-run (REPORT ONLY).
 *
 * Modes: manual-file | sitemap | search-list
 * Rejects --apply. No Airtable writes. No Google Places. No deep scraping.
 */
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  parseManualCsv,
  parseManualJson,
  processSearchListSeeds,
  processSitemapMode,
  buildBrandDirectoryCandidate,
  summarizeBrandDirectoryReport,
} from "../lib/independent-census/sources/brand-directory.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      "--apply is not supported. Brand directory discovery is dry-run / report-only."
    );
  }

  let mode = "";
  let input = "";
  let batchId = "";
  let brand = "";
  let parentCompany = "";
  let sourceUrl = "";
  let country = "";
  let maxPages = 500;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--mode" && argv[i + 1]) mode = argv[++i];
    else if (a.startsWith("--mode=")) mode = a.slice("--mode=".length);
    else if (a === "--input" && argv[i + 1]) input = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--input=")) input = a.slice("--input=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--brand" && argv[i + 1]) brand = argv[++i];
    else if (a.startsWith("--brand=")) brand = a.slice("--brand=".length);
    else if (a === "--parent-company" && argv[i + 1]) parentCompany = argv[++i];
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length);
    else if (a === "--source-url" && argv[i + 1]) sourceUrl = argv[++i];
    else if (a.startsWith("--source-url=")) sourceUrl = a.slice("--source-url=".length);
    else if (a === "--country" && argv[i + 1]) country = argv[++i];
    else if (a.startsWith("--country=")) country = a.slice("--country=".length);
    else if (a === "--max-pages" && argv[i + 1]) maxPages = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-pages="))
      maxPages = parseInt(a.slice("--max-pages=".length), 10);
  }

  if (!mode) throw new Error("Missing --mode (manual-file | sitemap | search-list)");
  if (!batchId) {
    const date = new Date().toISOString().slice(0, 10);
    batchId = `brand-directory-${mode}-${date}`;
  }

  return { mode, input, batchId, brand, parentCompany, sourceUrl, country, maxPages };
}

async function runManualFile(inputPath, batchId) {
  if (!existsSync(inputPath)) throw new Error(`Input not found: ${inputPath}`);
  const lower = inputPath.toLowerCase();
  const rows =
    lower.endsWith(".csv") || lower.endsWith(".tsv")
      ? parseManualCsv(inputPath)
      : parseManualJson(inputPath);

  const ctx = { batchId, mode: "manual-file", importedAt: new Date().toISOString() };
  const candidates = [];
  const discoveryLeads = [];
  let seq = 0;
  for (const row of rows) {
    seq++;
    const hasName = String(row.hotelName || "").trim();
    if (hasName) {
      candidates.push(buildBrandDirectoryCandidate(row, { ...ctx, seq }));
    } else {
      const lead = buildBrandDirectoryCandidate(
        { ...row, extraPayload: { recordKind: "discovery_lead" } },
        { ...ctx, seq }
      );
      discoveryLeads.push({ ...lead, recordKind: "discovery_lead" });
    }
  }
  return { candidates, discoveryLeads, urlsInspected: rows.length, brand: rows[0]?.brand, parentCompany: rows[0]?.parentCompany };
}

async function main() {
  const args = parseArgs();
  const jsonPath = join(REPORTS_DIR, `independent-census-brand-directory-dry-run-${args.batchId}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-brand-directory-dry-run-${args.batchId}.csv`);

  console.log("=== Brand directory discovery (Phase 2F, DRY-RUN) ===\n");
  console.log("Report-only — no Airtable writes, no scraping of rates/reviews/photos.\n");
  console.log(`Mode:     ${args.mode}`);
  console.log(`Batch ID: ${args.batchId}\n`);

  let candidates = [];
  let discoveryLeads = [];
  let urlsInspected = 0;
  let brandLabel = args.brand;
  let parentLabel = args.parentCompany;

  if (args.mode === "manual-file") {
    const inputPath = join(process.cwd(), args.input);
    if (!args.input) throw new Error("manual-file mode requires --input");
    const result = await runManualFile(inputPath, args.batchId);
    candidates = result.candidates;
    discoveryLeads = result.discoveryLeads;
    urlsInspected = result.urlsInspected;
    brandLabel = result.brand || brandLabel;
    parentLabel = result.parentCompany || parentLabel;
  } else if (args.mode === "search-list") {
    const inputPath = join(process.cwd(), args.input);
    if (!args.input) throw new Error("search-list mode requires --input");
    const data = JSON.parse(readFileSync(inputPath, "utf8"));
    const seeds = Array.isArray(data) ? data : data.seeds || [];
    const result = processSearchListSeeds(seeds, {
      batchId: args.batchId,
      importedAt: new Date().toISOString(),
    });
    candidates = result.candidates;
    discoveryLeads = result.discoveryLeads;
    urlsInspected = result.urlsInspected;
    brandLabel = seeds[0]?.brand || brandLabel;
    parentLabel = seeds[0]?.parentCompany || parentLabel;
  } else if (args.mode === "sitemap") {
    if (!args.brand || !args.sourceUrl) {
      throw new Error("sitemap mode requires --brand and --source-url");
    }
    const result = await processSitemapMode({
      brand: args.brand,
      parentCompany: args.parentCompany,
      sourceUrl: args.sourceUrl,
      country: args.country,
      batchId: args.batchId,
      maxPages: args.maxPages,
    });
    candidates = result.candidates;
    discoveryLeads = result.discoveryLeads;
    urlsInspected = result.urlsInspected;
    brandLabel = args.brand;
    parentLabel = args.parentCompany;
  } else {
    throw new Error(`Unknown mode: ${args.mode}`);
  }

  const summary = summarizeBrandDirectoryReport(candidates, discoveryLeads, {
    brand: brandLabel,
    parentCompany: parentLabel,
    mode: args.mode,
    urlsInspected,
  });

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "2F-brand-directory-dry-run",
    batchId: args.batchId,
    mode: args.mode,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    summary,
    candidates,
    discoveryLeads,
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  const csvRows = [...candidates, ...discoveryLeads].map((c) => ({
    recordKind: c.recordKind || "candidate",
    sourceRecordId: c.sourceRecordId,
    rawHotelName: c.rawHotelName,
    rawBrand: c.rawBrand,
    rawCity: c.rawCity,
    rawCountry: c.rawCountry,
    rawWebsite: c.rawWebsite || c.sourceUrl,
    sourceUrl: c.sourceUrl,
    recommendedAction: c.recommendedAction,
    qualityTier: c.qualityTier,
  }));

  writeJson(jsonPath, report);
  writeCsv(csvPath, csvRows, [
    "recordKind",
    "sourceRecordId",
    "rawHotelName",
    "rawBrand",
    "rawCity",
    "rawCountry",
    "rawWebsite",
    "sourceUrl",
    "recommendedAction",
    "qualityTier",
  ]);

  console.log("--- Summary ---");
  console.log(JSON.stringify(summary, null, 2));
  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log("\n✓ Dry-run complete. No Airtable. Review required before product use.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
