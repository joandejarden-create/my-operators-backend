import "../load-env.js";
import Airtable from "airtable";
import { buildCompletePresentationRows } from "./lib/choice-explorer-complete-rows.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const REGION_SLOTS = new Set([
  "footprint.region.am",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
]);

function esc(v) {
  return String(v || "").replace(/"/g, '\\"');
}

function parseArgs(argv) {
  const i = argv.indexOf("--brand");
  return {
    dryRun: argv.includes("--dry-run"),
    brandFilter: i >= 0 ? String(argv[i + 1] || "").trim() : "",
  };
}

async function listRegionRows(base, brandFilter) {
  const slotFormula = `OR(${[...REGION_SLOTS].map((s) => `{Slot Key} = "${esc(s)}"`).join(",")})`;
  const formula = brandFilter
    ? `AND(${slotFormula}, {Brand Name} = "${esc(brandFilter)}")`
    : slotFormula;
  return base(TABLE).select({ filterByFormula: formula, maxRecords: 3000 }).all();
}

function groupedByBrand(rows) {
  /** @type {Map<string, any[]>} */
  const byBrand = new Map();
  for (const r of rows) {
    const brand = String(r.get("Brand Name") || "").trim();
    if (!brand) continue;
    if (!byBrand.has(brand)) byBrand.set(brand, []);
    byBrand.get(brand).push(r);
  }
  return byBrand;
}

async function main() {
  const { dryRun, brandFilter } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await listRegionRows(base, brandFilter);
  const byBrand = groupedByBrand(rows);

  let updated = 0;
  for (const [brand, brandRows] of byBrand.entries()) {
    const expectedBySlot = new Map(
      buildCompletePresentationRows(brand)
        .filter((r) => REGION_SLOTS.has(String(r.slotKey || "").trim()))
        .map((r) => [String(r.slotKey).trim(), String(r.body || "").trim()])
    );

    for (const rec of brandRows) {
      const slotKey = String(rec.get("Slot Key") || "").trim();
      const nextBody = expectedBySlot.get(slotKey);
      if (!nextBody) continue;
      const current = String(rec.get("Body") || "").trim();
      if (current === nextBody) continue;
      console.log(`- ${brand}: ${slotKey}`);
      if (!dryRun) await base(TABLE).update(rec.id, { Body: nextBody });
      updated += 1;
    }
  }

  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

