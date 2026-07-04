/**
 * Phase 4G — Generate brand-directory search-list seeds from Brand Setup (read-only).
 */
import "../load-env.js";
import { join } from "path";
import { writeFileSync, mkdirSync } from "fs";
import { generateBrandDirectorySeedsFromBrandSetup } from "../lib/independent-census/brand-directory-seeds-from-brand-setup.js";
import { writeJson } from "../lib/str-census-import/report-utils.mjs";

const FIXTURES_DIR = join(process.cwd(), "fixtures", "independent-census");

function parseArgs() {
  let parentCompany = "";
  let normalizedParentCompany = "";
  let activeOnly = false;
  let batchId = "";
  let output = "";
  let calaRegionOnly = true;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--normalized-parent-company" && argv[i + 1])
      normalizedParentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--normalized-parent-company="))
      normalizedParentCompany = a
        .slice("--normalized-parent-company=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--activeOnly=true") activeOnly = true;
    else if (a === "--activeOnly=false") activeOnly = false;
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--output" && argv[i + 1]) output = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--output="))
      output = a.slice("--output=".length).replace(/^"|"$/g, "");
    else if (a === "--calaRegionOnly=false") calaRegionOnly = false;
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Seed generation is read-only.");
    }
  }

  if (!parentCompany && !normalizedParentCompany) {
    throw new Error("Provide --parent-company and/or --normalized-parent-company");
  }

  if (!batchId) {
    const slug = (normalizedParentCompany || parentCompany)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    batchId = `brand-directory-seeds-${slug}-from-brand-setup`;
  }

  if (!output) {
    const slug = (normalizedParentCompany || parentCompany)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    output = join(
      FIXTURES_DIR,
      `brand-directory-seeds-${slug}-from-brand-setup.json`
    );
  } else {
    output = join(process.cwd(), output);
  }

  return { parentCompany, normalizedParentCompany, activeOnly, batchId, output, calaRegionOnly };
}

async function main() {
  const args = parseArgs();

  console.log("=== Brand directory seeds from Brand Setup (Phase 4G, read-only) ===\n");
  console.log(`Parent filter: ${args.parentCompany || "(normalized only)"}`);
  if (args.normalizedParentCompany) {
    console.log(`Normalized parent: ${args.normalizedParentCompany}`);
  }
  console.log(`Active only: ${args.activeOnly}`);
  console.log(`CALA region filter: ${args.calaRegionOnly}\n`);

  const payload = await generateBrandDirectorySeedsFromBrandSetup({
    parentCompany: args.parentCompany,
    normalizedParentCompany: args.normalizedParentCompany,
    activeOnly: args.activeOnly,
    calaRegionOnly: args.calaRegionOnly,
    batchId: args.batchId,
  });

  mkdirSync(join(process.cwd(), "fixtures", "independent-census"), { recursive: true });
  writeJson(args.output, payload);

  console.log(`Brand Setup rows read: ${payload.brandSetupRecordsRead}`);
  console.log(`Seeds generated: ${payload.brandsMatched}`);
  console.log(`  Active/Live (priority 1): ${payload.activeLiveCount}`);
  console.log(`  Other status (priority 2): ${payload.otherStatusCount}`);
  console.log(`  Missing source URL: ${payload.missingSourceUrlCount}`);
  console.log(`\nSeed file: ${args.output}`);
  console.log("\n✓ No Airtable writes. Brand Setup untouched.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
