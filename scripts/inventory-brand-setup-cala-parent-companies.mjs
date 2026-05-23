/**
 * Brand Setup CALA parent-company inventory (READ-ONLY).
 *
 * Source of truth: Brand Setup - Brand Basics on AIRTABLE_BASE_ID.
 * Compares to Brand Alias Mapping on AIRTABLE_BASE_ID_ALT (read-only).
 *
 * No writes to Hotel Census, Brand Setup, Brand Alias, or independent census tables.
 */
import "../load-env.js";
import { join } from "path";
import {
  loadBrandSetupBasics,
  loadBrandAliasMappingReadOnly,
  buildBrandSetupCalaInventory,
  inventoryToCsvRow,
  gapsToCsvRow,
  INVENTORY_CSV_COLUMNS,
  GAPS_CSV_COLUMNS,
} from "../lib/independent-census/brand-setup-cala-inventory.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let activeOnly = false;
  const argv = process.argv.slice(2);
  for (const a of argv) {
    if (a === "--activeOnly=true") activeOnly = true;
    if (a === "--includeInactive=true") activeOnly = false;
    if (a === "--activeOnly=false") activeOnly = false;
    if (a === "--apply") {
      throw new Error("--apply is not supported. This inventory is read-only.");
    }
  }
  return { activeOnly };
}

async function main() {
  const { activeOnly } = parseArgs();

  const jsonPath = join(REPORTS_DIR, "brand-setup-cala-parent-company-inventory.json");
  const csvPath = join(REPORTS_DIR, "brand-setup-cala-parent-company-inventory.csv");
  const gapsPath = join(REPORTS_DIR, "brand-setup-cala-brand-gaps.csv");

  console.log("=== Brand Setup CALA parent-company inventory (read-only) ===\n");

  console.log(
    `Loading Brand Setup - Brand Basics (${activeOnly ? "active brands only" : "all records / all statuses"})…`
  );
  const brandData = await loadBrandSetupBasics({ activeOnly });
  console.log(`  Records read: ${brandData.totalLoaded}; in scope: ${brandData.brandsInScope}`);

  console.log("Loading Brand Alias Mapping (read-only compare)…");
  const aliasData = await loadBrandAliasMappingReadOnly();
  console.log(`  Alias rows read: ${aliasData.totalLoaded}`);

  const inventory = buildBrandSetupCalaInventory(brandData, aliasData);

  const gapRows = inventory.brands
    .filter(
      (b) =>
        b.missingKeyFields.length > 0 ||
        !b.aliasMappingPresent ||
        (!b.calaRelevant && !b.regionOffered.length)
    )
    .map(gapsToCsvRow);

  writeJson(jsonPath, inventory);
  writeCsv(
    csvPath,
    inventory.brands.map(inventoryToCsvRow),
    INVENTORY_CSV_COLUMNS
  );
  writeCsv(gapsPath, gapRows, GAPS_CSV_COLUMNS);

  console.log("\n--- Parent companies (by recommended priority) ---");
  for (const p of inventory.parentCompanies.slice(0, 20)) {
    console.log(
      `  ${p.recommendedPriorityRank}. ${p.rawParentCompanyLabels.join(" | ") || p.normalizedParentCompany}: ${p.brandCount} brands (${p.calaBrandCount} CALA-flagged)`
    );
  }
  if (inventory.parentCompanies.length > 20) {
    console.log(`  … and ${inventory.parentCompanies.length - 20} more`);
  }

  console.log("\n--- Summary ---");
  console.log(`  Parent companies: ${inventory.totals.parentCompanyCount}`);
  console.log(`  Brands in scope: ${inventory.totals.brandsInScope}`);
  console.log(`  Active / Live: ${inventory.totals.activeBrandCount}`);
  console.log(`  Inactive / Draft / other: ${inventory.totals.inactiveOrDraftBrandCount}`);
  console.log(`  CALA-relevant (Region Offered): ${inventory.totals.calaRelevantBrandCount}`);
  console.log(`  Not CALA-flagged: ${inventory.totals.nonCalaFlaggedBrandCount}`);
  console.log(`  Missing parent company: ${inventory.dataQuality.missingParentCompany}`);
  console.log(`  Missing brand family: ${inventory.dataQuality.missingBrandFamily}`);
  console.log(`  Missing website/directory URL: ${inventory.dataQuality.missingWebsiteOrDirectoryUrl}`);
  console.log(`  Inconsistent parent spellings: ${inventory.totals.inconsistentParentCompanyGroups}`);
  console.log(`  Duplicate brand name groups: ${inventory.totals.duplicateBrandNameGroups}`);
  console.log(`  Alias in mapping but not Brand Setup: ${inventory.totals.aliasOnlyNotInSetupCount}`);
  console.log(`  Brand Setup without alias: ${inventory.totals.setupWithoutAliasCount}`);

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(`  ${gapsPath}`);
  console.log(
    "\n✓ No Airtable writes. Hotel Census, Brand Setup, Brand Alias Mapping, and independent census tables untouched."
  );
  console.log("✓ No STR/CoStar fields used.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
