/**
 * Delete all Brand Explorer Presentation rows for a brand.
 * Usage: node scripts/purge-brand-explorer-presentation.mjs --brand-name "Radisson Blu" [--dry-run]
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";

function parseArgs() {
  const dryRun = process.argv.includes("--dry-run");
  const i = process.argv.indexOf("--brand-name");
  const brandName = i >= 0 ? String(process.argv[i + 1] || "").trim() : "";
  if (!brandName) throw new Error("Require --brand-name");
  return { dryRun, brandName };
}

async function findBasics(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  return records[0] || null;
}

async function selectPresentation(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  for (const formula of [`{Brand Name} = "${esc}"`, `{Brand} = "${esc}"`]) {
    try {
      const recs = await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all();
      for (const r of recs) {
        if (!seen.has(r.id)) {
          seen.add(r.id);
          merged.push(r);
        }
      }
    } catch {
      /* column may not exist */
    }
  }
  return merged;
}

async function main() {
  const { dryRun, brandName } = parseArgs();
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const basics = await findBasics(base, brandName);
  if (!basics) throw new Error(`No basics for ${brandName}`);
  const rows = await selectPresentation(base, brandName);
  console.log(`${brandName} (${basics.id}): ${rows.length} presentation row(s) to delete`);
  if (dryRun || !rows.length) return;
  for (let i = 0; i < rows.length; i += 10) {
    await base(TABLE).destroy(rows.slice(i, i + 10).map((r) => r.id));
  }
  console.log("Deleted.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
