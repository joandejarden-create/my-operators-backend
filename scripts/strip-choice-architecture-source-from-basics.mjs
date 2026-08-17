/**
 * Remove "Source: CHI Brands Architecture (Oct 2025) — …" from Brand Positioning in Airtable.
 *
 *   node scripts/strip-choice-architecture-source-from-basics.mjs --dry-run
 *   node scripts/strip-choice-architecture-source-from-basics.mjs --apply
 */
import "../load-env.js";
import Airtable from "airtable";
import { stripArchitectureSourceFromPositioning } from "../lib/choice-brand-architecture-oct2025.js";
import { clearBrandDetailCache } from "../api/brand-library.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PARENT = "Choice Hotels International";
const FIELD = "Brand Positioning";

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") || !argv.includes("--apply") };
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const records = await base(BASICS_TABLE)
    .select({
      filterByFormula: `FIND("${PARENT}", {Parent Company})`,
      fields: ["Brand Name", "Parent Company", FIELD],
    })
    .all();

  let updated = 0;
  let skipped = 0;

  console.log(dryRun ? "DRY RUN\n" : "APPLY\n");

  for (const rec of records) {
    const brandName = String(rec.get("Brand Name") || "").trim();
    const current = String(rec.get(FIELD) || "");
    const next = stripArchitectureSourceFromPositioning(current);
    if (!current || current === next) {
      skipped++;
      continue;
    }
    console.log(`${brandName}:`);
    console.log(`  was: ${current.slice(-120)}`);
    console.log(`  →   ${next.slice(-120)}`);
    if (!dryRun) {
      await base(BASICS_TABLE).update(rec.id, { [FIELD]: next });
      clearBrandDetailCache(rec.id);
      clearBrandDetailCache(brandName);
    }
    updated++;
  }

  console.log(`\n${dryRun ? "Would update" : "Updated"} ${updated}; skipped ${skipped}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
