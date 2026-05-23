/**
 * PATCH Brand Setup - Sustainability & ESG for Choice CHI brands (empty fields only).
 *
 *   node scripts/apply-choice-sustainability-esg-batch.mjs --dry-run
 *   node scripts/apply-choice-sustainability-esg-batch.mjs
 *   node scripts/apply-choice-sustainability-esg-batch.mjs --brand "Cambria Hotels"
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  TARGET_BRANDS,
  buildEsgFieldsForBrand,
} from "./lib/choice-sustainability-esg-fixtures.mjs";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const ESG_TABLE = "Brand Setup - Sustainability & ESG";

function parseArgs(argv) {
  const args = argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const overwriteText = args.includes("--overwrite-text");
  const bi = args.indexOf("--brand");
  const brandFilter = bi >= 0 ? String(args[bi + 1] || "").trim() : "";
  return { dryRun, brandFilter, overwriteText };
}

const TEXT_FIELDS = ["Energy Efficiency Initiatives", "Waste Reduction Programs"];

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function patchMissing(record, fields, { overwriteText = false } = {}) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value == null || value === "") continue;
    if (!isEmpty(record.get(key)) && !(overwriteText && TEXT_FIELDS.includes(key))) {
      skipped.push(key);
      continue;
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

async function findBasics(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  return rows.find((r) =>
    String(r.get("Parent Company") || "").includes("Choice Hotels International")
  );
}

async function findEsgByBrandName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(ESG_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  return rows[0] || null;
}

async function main() {
  const { dryRun, brandFilter, overwriteText } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );

  let brands = TARGET_BRANDS;
  if (brandFilter) {
    if (!brands.includes(brandFilter)) throw new Error(`Unknown brand: ${brandFilter}`);
    brands = [brandFilter];
  }

  let totalFields = 0;
  let brandsUpdated = 0;

  for (const brandName of brands) {
    const fields = buildEsgFieldsForBrand(brandName);
    if (!fields) {
      console.warn(`Skip ${brandName}: no fixture`);
      continue;
    }

    const basics = await findBasics(base, brandName);
    if (!basics) {
      console.warn(`Skip ${brandName}: no CHI Brand Basics row`);
      continue;
    }

    let esg = await findEsgByBrandName(base, brandName);
    if (!esg) {
      if (dryRun) {
        console.log(`[dry-run] Would CREATE ESG row for ${brandName} with ${Object.keys(fields).length} fields`);
        brandsUpdated++;
        totalFields += Object.keys(fields).length;
        continue;
      }
      const createFields = { ...fields, "Brand Name": brandName, Brand: [basics.id] };
      esg = await base(ESG_TABLE).create(createFields);
      console.log(`Created ESG ${esg.id} for ${brandName} (${Object.keys(fields).length} fields)`);
      brandsUpdated++;
      totalFields += Object.keys(fields).length;
      continue;
    }

    const { patch, skipped } = patchMissing(esg, fields, { overwriteText });
    if (!Object.keys(patch).length) {
      console.log(`Skip ${brandName}: all fields populated (${skipped.length} skipped)`);
      continue;
    }

    if (dryRun) {
      console.log(`[dry-run] ${brandName} (${esg.id}) would patch: ${Object.keys(patch).join(", ")}`);
      if (skipped.length) console.log(`  skip existing: ${skipped.join(", ")}`);
      brandsUpdated++;
      totalFields += Object.keys(patch).length;
      continue;
    }

    await base(ESG_TABLE).update(esg.id, patch);
    console.log(`Updated ${brandName}: ${Object.keys(patch).join(", ")}`);
    if (skipped.length) console.log(`  skipped: ${skipped.join(", ")}`);
    brandsUpdated++;
    totalFields += Object.keys(patch).length;
  }

  console.log(
    `\n${dryRun ? "[dry-run] " : ""}Done: ${brandsUpdated} brands, ${totalFields} fields ${dryRun ? "would be " : ""}written`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
