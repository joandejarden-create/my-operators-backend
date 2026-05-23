/**
 * PATCH Brand Setup - Loyalty & Commercial for Choice CHI brands (empty fields only).
 *
 *   node scripts/apply-choice-loyalty-commercial-batch.mjs --dry-run
 *   node scripts/apply-choice-loyalty-commercial-batch.mjs
 *   node scripts/apply-choice-loyalty-commercial-batch.mjs --overwrite --brand "Cambria Hotels"
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  TARGET_BRANDS,
  buildLoyaltyFieldsForBrand,
} from "./lib/choice-loyalty-commercial-fixtures.mjs";
import { FDD_ITEM19 } from "./lib/choice-fdd-item19.mjs";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const LC_TABLE = "Brand Setup - Loyalty & Commercial";

const PERCENT_COLS = new Set([
  "Typical % of Rooms from Loyalty (est.)",
  "Typical Direct Booking % (est.)",
  "Typical OTA Reliance % (est.)",
  "OTA Commission (Typical % of Reservation)",
  "CRS Usage (% of bookings flowing through)",
  "Website/App Conv. Rates (%)",
]);

function parseArgs(argv) {
  const args = argv.slice(2);
  return {
    dryRun: args.includes("--dry-run"),
    overwrite: args.includes("--overwrite"),
    brandFilter: (() => {
      const i = args.indexOf("--brand");
      return i >= 0 ? String(args[i + 1] || "").trim() : "";
    })(),
  };
}

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function toAirtableValue(key, value) {
  if (value == null || value === "") return null;
  if (PERCENT_COLS.has(key)) {
    const num = typeof value === "number" ? value : parseFloat(String(value));
    if (Number.isNaN(num)) return null;
    return num >= 0 && num <= 100 ? num / 100 : num;
  }
  if (typeof value === "number") return value;
  return String(value).trim();
}

/** When overwrite, only these keys are replaced (FDD-backed metrics). */
const FDD_OVERWRITE_KEYS = new Set([
  "Typical % of Rooms from Loyalty (est.)",
  "CRS Usage (% of bookings flowing through)",
]);

function patchMissing(record, fields, { overwrite = false } = {}) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    const v = toAirtableValue(key, value);
    if (v == null || v === "") continue;
    const mayOverwrite = overwrite && FDD_OVERWRITE_KEYS.has(key);
    if (!mayOverwrite && !isEmpty(record.get(key))) {
      skipped.push(key);
      continue;
    }
    if (mayOverwrite && record.get(key) === v) {
      skipped.push(key);
      continue;
    }
    patch[key] = v;
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

async function findLc(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(LC_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  return rows[0] || null;
}

async function main() {
  const { dryRun, brandFilter, overwrite } = parseArgs(process.argv);
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
    if (overwrite && !FDD_ITEM19[brandName]?.loyaltyPct && !FDD_ITEM19[brandName]?.enterprisePct && !FDD_ITEM19[brandName]?.proprietaryPct) {
      continue;
    }
    const fields = buildLoyaltyFieldsForBrand(brandName);
    if (!fields) continue;

    const basics = await findBasics(base, brandName);
    if (!basics) {
      console.warn(`Skip ${brandName}: no CHI Brand Basics row`);
      continue;
    }

    let lc = await findLc(base, brandName);
    if (!lc) {
      if (dryRun) {
        console.log(`[dry-run] CREATE Loyalty & Commercial for ${brandName}`);
        brandsUpdated++;
        totalFields += Object.keys(fields).length;
        continue;
      }
      const createFields = { "Brand Name": brandName, Brand: [basics.id] };
      for (const [k, v] of Object.entries(fields)) {
        const av = toAirtableValue(k, v);
        if (av != null && av !== "") createFields[k] = av;
      }
      lc = await base(LC_TABLE).create(createFields);
      console.log(`Created ${brandName} (${lc.id})`);
      brandsUpdated++;
      totalFields += Object.keys(fields).length;
      continue;
    }

    const { patch, skipped } = patchMissing(lc, fields, { overwrite });
    if (!Object.keys(patch).length) {
      console.log(`Skip ${brandName}: all populated`);
      continue;
    }

    if (dryRun) {
      console.log(`[dry-run] ${brandName}: ${Object.keys(patch).join(", ")}`);
      brandsUpdated++;
      totalFields += Object.keys(patch).length;
      continue;
    }

    await base(LC_TABLE).update(lc.id, patch);
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
