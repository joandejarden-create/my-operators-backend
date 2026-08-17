#!/usr/bin/env node
/**
 * Plan Hilton website descriptions for matched Hotel Census rows (GraphQL).
 *
 *   node scripts/plan-hilton-census-descriptions.mjs --brand "Curio Collection by Hilton"
 *   node scripts/plan-hilton-census-descriptions.mjs --brand-codes QQ,GI,HP
 *   node scripts/plan-hilton-census-descriptions.mjs --ctyhocn SJOCUQQ
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadHiltonBrandDirectoryConfigs } from "../lib/hilton-brand-registry.js";
import {
  planHiltonBrandDescriptions,
  planHiltonCensusDescriptionsFromCensus,
} from "../lib/hotel-census/plan-hilton-census-descriptions.js";
import { fetchHiltonHotelDescription, pickPrimaryHiltonDescription } from "../lib/hilton-hotel-description-fetch.js";
import { readFileSync, existsSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    brand: get("--brand"),
    brandCodes: get("--brand-codes")
      ? get("--brand-codes").split(",").map((s) => s.trim().toUpperCase())
      : null,
    ctyhocn: get("--ctyhocn"),
    minConfidence: get("--min-confidence") || "medium",
    crawlDelayMs: Number(get("--crawl-delay-ms") || 200),
    fetchDelayMs: Number(get("--fetch-delay-ms") || 300),
    fromCensus: args.includes("--from-census"),
    enrichmentPlan: get("--enrichment-plan"),
  };
}

function loadEnrichmentCtyhocnMap(planPath) {
  if (!planPath || !existsSync(planPath)) return new Map();
  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  const rows = plan.planRows || [];
  /** @type {Map<string, string>} */
  const map = new Map();
  for (const row of rows) {
    const id = String(row.censusRecordId || "").trim();
    const code = String(row.directoryBrandPropertyCode || "").trim().toUpperCase();
    if (!id || !code) continue;
    if (row.matchConfidence === "none") continue;
    map.set(id, code);
  }
  return map;
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function main() {
  const opts = parseArgs();

  if (opts.ctyhocn) {
    const row = await fetchHiltonHotelDescription(opts.ctyhocn);
    console.log(JSON.stringify(row, null, 2));
    console.log("\nPrimary description:\n", pickPrimaryHiltonDescription(row));
    return;
  }

  if (opts.fromCensus) {
    const enrichmentPath =
      opts.enrichmentPlan ||
      join(REPORTS, "hilton-census-enrichment-plan-all-brands.json");
    const enrichmentCtyhocnByCensusId = loadEnrichmentCtyhocnMap(enrichmentPath);
    console.log("=== Plan Hilton Census Descriptions (census-direct) ===\n");
    console.log("Enrichment plan codes:", enrichmentCtyhocnByCensusId.size, "from", enrichmentPath);

    const plan = await planHiltonCensusDescriptionsFromCensus({
      fetchDelayMs: opts.fetchDelayMs,
      enrichmentCtyhocnByCensusId,
      onProgress: (msg) => console.log(" ", msg),
    });

    mkdirSync(REPORTS, { recursive: true });
    const jsonPath = join(REPORTS, "hilton-census-descriptions-plan-from-census.json");
    writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

    console.log("\n=== Summary ===");
    console.log("  Census rows:", plan.censusRowsLoaded);
    console.log("  With ctyhocn:", plan.withCtyhocn);
    console.log("  Skipped (no code):", plan.skippedNoCode);
    console.log("  Descriptions fetched:", plan.descriptionsFetched);
    console.log("  Ready to apply:", plan.readyToApply);
    console.log("  Fetch errors:", plan.fetchErrors.length);
    console.log("\nReport:", jsonPath);
    return;
  }

  const configs = await loadHiltonBrandDirectoryConfigs();
  let brands = configs;
  if (opts.brand) {
    brands = configs.filter((c) => c.canonicalBrandName === opts.brand);
    if (!brands.length) throw new Error(`Brand not found: ${opts.brand}`);
  }
  if (opts.brandCodes?.length) {
    brands = brands.filter((c) => opts.brandCodes.includes(c.brandCode));
  }

  console.log("=== Plan Hilton Census Descriptions ===\n");
  console.log("Brands:", brands.map((b) => `${b.brandCode} ${b.canonicalBrandName}`).join("; "));

  const allPlans = [];
  const summaries = [];

  for (const brandConfig of brands) {
    console.log(`\n--- ${brandConfig.canonicalBrandName} (${brandConfig.brandCode}) ---`);
    const plan = await planHiltonBrandDescriptions({
      brandConfig,
      minConfidence: opts.minConfidence,
      crawlDelayMs: opts.crawlDelayMs,
      fetchDelayMs: opts.fetchDelayMs,
      onProgress: (msg) => console.log(" ", msg),
    });
    summaries.push({
      brand: plan.brand,
      brandCode: brandConfig.brandCode,
      matched: plan.matched,
      descriptionsFetched: plan.descriptionsFetched,
      readyToApply: plan.readyToApply,
      fetchErrors: plan.fetchErrors.length,
    });
    allPlans.push(...plan.planRows);

    const stamp = slugify(brandConfig.canonicalBrandName);
    const jsonPath = join(REPORTS, `hilton-census-descriptions-plan-${stamp}.json`);
    mkdirSync(REPORTS, { recursive: true });
    writeFileSync(jsonPath, JSON.stringify(plan, null, 2));
    console.log("  Wrote", jsonPath);
  }

  const combinedJson = join(REPORTS, "hilton-census-descriptions-plan-all-brands.json");
  const combinedCsv = join(REPORTS, "hilton-census-descriptions-plan-all-brands.csv");
  writeFileSync(
    combinedJson,
    JSON.stringify({ generatedAt: new Date().toISOString(), summaries, planRows: allPlans }, null, 2)
  );

  const headers = [
    "brand",
    "brandCode",
    "censusRecordId",
    "censusName",
    "directoryName",
    "ctyhocn",
    "status",
    "headline",
    "primaryDescription",
  ];
  const csvLines = [
    headers.join(","),
    ...allPlans.map((r) =>
      [
        csvEscape(r.brand),
        r.brandCode,
        r.censusRecordId,
        csvEscape(r.censusName),
        csvEscape(r.directoryName),
        r.ctyhocn,
        r.status,
        csvEscape(r.headline),
        csvEscape(r.primaryDescription || r.shortDesc),
      ].join(",")
    ),
  ];
  writeFileSync(combinedCsv, csvLines.join("\n"));

  console.log("\n=== Summary ===");
  for (const s of summaries) {
    console.log(
      `  ${s.brandCode}: matched=${s.matched} descriptions=${s.descriptionsFetched} ready=${s.readyToApply} errors=${s.fetchErrors}`
    );
  }
  console.log("\nReports:", combinedJson, combinedCsv);
  console.log(
    "\nNote: Hotel Census needs a `Hotel Description` column (multilineText) before --apply. See docs/hilton-census-description-enrichment.md"
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
