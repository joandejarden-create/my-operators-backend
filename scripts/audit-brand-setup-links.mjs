/**
 * Audit linked Brand Setup records per brand (Basics → child tables).
 * Usage: node scripts/audit-brand-setup-links.mjs --name "Radisson Blu (Choice)"
 */
import "../load-env.js";
import Airtable from "airtable";

const BASICS = "Brand Setup - Brand Basics";
const TABLES = [
  "Brand Setup - Sustainability & ESG",
  "Brand Setup - Brand Footprint",
  "Brand Setup - Project Fit",
  "Brand Setup - Portfolio & Performance",
  "Brand Setup - Brand Standards",
  "Brand Setup - Fee Structure",
  "Brand Setup - Deal Terms",
  "Brand Setup - Operational Support",
  "Brand Setup - Legal Terms",
  "Brand Setup - Loyalty & Commercial",
  "Brand Setup - Brand Explorer Presentation",
];
const LINK_FIELDS = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];
const BASICS_LINK_FIELDS = [
  "Brand Setup - Fee Structure",
  "Brand Setup - Brand Footprint",
  "Brand Setup - Deal Terms",
  "Brand Setup - Operational Support",
  "Brand Setup - Legal Terms",
  "Brand Setup - Loyalty & Commercial",
  "Brand Setup - Brand Standards",
  "Brand Setup - Project Fit",
  "Brand Setup - Portfolio & Performance",
  "Brand Setup - Sustainability & ESG",
];

function parseName() {
  const i = process.argv.indexOf("--name");
  return i >= 0 ? String(process.argv[i + 1] || "").trim() : "";
}

async function findBasics(base, name) {
  const esc = name.replace(/"/g, '\\"');
  const rows = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .all();
  return rows[0] || null;
}

async function countByBrandName(base, table, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  let n = 0;
  try {
    n += (
      await base(table).select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 }).all()
    ).length;
  } catch {
    /* */
  }
  try {
    const byLink = await base(table)
      .select({ filterByFormula: `{Brand} = "${esc}"`, maxRecords: 500 })
      .all();
    const seen = new Set();
    for (const r of byLink) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        n = Math.max(n, byLink.length);
      }
    }
  } catch {
    /* */
  }
  return n;
}

async function main() {
  const name = parseName();
  if (!name) throw new Error("Require --name");
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const basics = await findBasics(base, name);
  if (!basics) {
    console.log("No Basics row for", name);
    return;
  }
  console.log(`\n=== ${name} (${basics.id}) ===\nBasics link fields on row:`);
  for (const f of BASICS_LINK_FIELDS) {
    const v = basics.get(f);
    if (v != null && (Array.isArray(v) ? v.length : String(v).trim())) {
      const ids = Array.isArray(v) ? v.join(", ") : String(v);
      console.log(`  ${f}: ${ids}`);
    }
  }
  console.log("\nChild table row counts (Brand Name / Brand link):");
  for (const t of TABLES) {
    const n = await countByBrandName(base, t, name);
    console.log(`  ${n ? "✓" : "✗"} ${t}: ${n}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
