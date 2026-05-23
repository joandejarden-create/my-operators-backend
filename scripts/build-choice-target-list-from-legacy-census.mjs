/**
 * Phase Choice-A — Build read-only Choice target list from legacy Hotel Census.
 */
import "../load-env.js";
import { join } from "path";
import {
  buildChoiceTargetList,
  targetRowToReport,
  CHOICE_TARGET_CSV_COLUMNS,
} from "../lib/independent-census/choice-target-list.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let parentCompany = "Choice Hotels International";
  let countries = "";
  let brandFilter = "";
  let batchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--countries" && argv[i + 1]) countries = argv[++i];
    else if (a.startsWith("--countries="))
      countries = a.slice("--countries=".length);
    else if (a === "--brand-filter" && argv[i + 1])
      brandFilter = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--brand-filter="))
      brandFilter = a.slice("--brand-filter=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  if (!batchId) throw new Error("Required: --batch-id");

  return { parentCompany, countriesStr: countries, brandFilter, batchId };
}

function printSummary(result, jsonPath, csvPath) {
  console.log("\n--- Choice target list (legacy read-only) ---");
  console.log(`Batch ID:                  ${result.batchId}`);
  console.log(`Parent company filter:     ${result.parentCompanyFilter}`);
  console.log(`Legacy census scanned:     ${result.legacyCensusRecordsScanned}`);
  console.log(`Choice targets:            ${result.choiceTargetCount}`);
  console.log(`Brand Setup brands loaded: ${result.brandSetupBrandsLoaded}`);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
  console.log("\nSafety:");
  console.log(`  Hotel Census writes:       ${result.hotelCensusWrites}`);
  console.log(`  STR fields used:           ${result.strFieldsUsed}`);
  console.log(`  Google API:                ${result.googleApiUsed}`);
}

async function main() {
  const args = parseArgs();
  const result = await buildChoiceTargetList(args);

  const base = `independent-census-choice-target-list-${args.batchId}`;
  const jsonPath = join(REPORTS_DIR, `${base}.json`);
  const csvPath = join(REPORTS_DIR, `${base}.csv`);

  const reportRows = result.targets.map(targetRowToReport);
  await writeJson(jsonPath, {
    generatedAt: new Date().toISOString(),
    phase: "Choice-A-target-list",
    ...result,
    reportRows,
  });
  await writeCsv(csvPath, reportRows, CHOICE_TARGET_CSV_COLUMNS);

  printSummary(result, jsonPath, csvPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
