/**
 * Extract public hotel identity metadata from official brand property pages (report-only default).
 */
import "../load-env.js";
import { join } from "path";
import {
  runBrandPortfolioMetadataExtract,
  extractRowToCsv,
  EXTRACT_CSV_COLUMNS,
} from "../lib/independent-census/brand-portfolio-metadata-extract.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let propertyUrlReport = "";
  let parentCompany = "Choice Hotels International";
  let batchId = "";
  let maxPages = null;
  let sourcePolicyApproved = false;
  let apply = false;
  let requestDelayMs = 600;
  let userAgent = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--property-url-report" && argv[i + 1])
      propertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-url-report="))
      propertyUrlReport = a.slice("--property-url-report=".length).replace(/^"|"$/g, "");
    else if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--max-pages" && argv[i + 1])
      maxPages = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-pages="))
      maxPages = parseInt(a.slice("--max-pages=".length), 10);
    else if (a === "--request-delay-ms" && argv[i + 1])
      requestDelayMs = parseInt(argv[++i], 10);
    else if (a === "--user-agent" && argv[i + 1])
      userAgent = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--user-agent="))
      userAgent = a.slice("--user-agent=".length).replace(/^"|"$/g, "");
    else if (a === "--source-policy-approved") sourcePolicyApproved = true;
    else if (a === "--apply") apply = true;
  }

  if (!propertyUrlReport) throw new Error("Required: --property-url-report");
  if (!batchId) throw new Error("Required: --batch-id");
  if (apply && !sourcePolicyApproved) {
    throw new Error("--apply requires --source-policy-approved");
  }

  return {
    propertyUrlReportPath: join(process.cwd(), propertyUrlReport),
    parentCompany,
    batchId,
    maxPages,
    sourcePolicyApproved,
    apply,
    requestDelayMs,
    userAgent: userAgent || undefined,
  };
}

async function main() {
  const args = parseArgs();
  console.log("Brand portfolio metadata extract (read-only by default)\n");

  const result = await runBrandPortfolioMetadataExtract(args);

  const base = `independent-census-brand-portfolio-metadata-${args.batchId}`;
  const jsonPath = join(REPORTS_DIR, `${base}.json`);
  const csvPath = join(REPORTS_DIR, `${base}.csv`);

  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "brand-portfolio-metadata-extract",
    ...result,
  });
  await writeCsv(
    csvPath,
    result.results.map(extractRowToCsv),
    EXTRACT_CSV_COLUMNS
  );

  console.log("--- Extract summary ---");
  console.log(`CALA included in report:  ${result.calaIncludedInReport}`);
  console.log(`Pages attempted:          ${result.pagesAttempted}`);
  console.log(`Extracted OK:             ${result.pagesExtractedOk}`);
  console.log(`Robots blocked:           ${result.robotsBlocked}`);
  console.log(`Fetch failed:             ${result.fetchFailed}`);
  console.log("Field coverage:", result.fieldCoverage);
  console.log("Source policy:", result.sourcePolicyWarnings);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
  console.log(`\nAirtable writes: ${result.airtableWrites}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
