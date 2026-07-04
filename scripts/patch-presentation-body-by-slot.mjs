/**
 * Update Body (and optional Case Summary fields) on existing presentation rows by Slot Key.
 *
 *   node scripts/patch-presentation-body-by-slot.mjs --brand-name "Radisson Blu (Choice)" --fixture fixtures/brand-explorer-presentation-radisson-blu-footprint.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";

function parseArgs(argv) {
  const dryRun = argv.includes("--dry-run");
  let brandName = "";
  let brandRecordId = "";
  let fixturePath = "";
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--brand-name" && argv[i + 1]) brandName = argv[++i];
    else if (argv[i] === "--brand-record-id" && argv[i + 1]) brandRecordId = argv[++i];
    else if (argv[i] === "--fixture" && argv[i + 1]) {
      const rel = argv[++i];
      fixturePath = path.isAbsolute(rel) ? rel : path.resolve(ROOT, rel);
    }
  }
  if ((!brandName && !brandRecordId) || !fixturePath) {
    throw new Error("Require --fixture and --brand-name or --brand-record-id");
  }
  return { dryRun, brandName: brandName.trim(), brandRecordId: brandRecordId.trim(), fixturePath };
}

function buildPatchFields(r) {
  const fields = { Body: r.body ?? "" };
  if (r.caseSummaryOverview != null) fields["Case Summary Overview"] = r.caseSummaryOverview;
  if (r.caseSummaryOwnerObjective != null) fields["Case Summary Owner Objective"] = r.caseSummaryOwnerObjective;
  if (r.caseSummaryBrandRelevance != null) fields["Case Summary Brand Relevance"] = r.caseSummaryBrandRelevance;
  if (r.caseSummaryInterpretation != null) fields["Case Summary Interpretation"] = r.caseSummaryInterpretation;
  if (r.caseSummaryTags != null) fields["Case Summary Tags"] = r.caseSummaryTags;
  return fields;
}

async function main() {
  const { dryRun, brandName, brandRecordId, fixturePath } = parseArgs(process.argv);
  const data = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const bySlot = new Map(
    data.rows.map((r) => [String(r.slotKey || "").trim(), r]).filter(([k]) => k)
  );
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  let records;
  if (brandRecordId) {
    const escId = brandRecordId.replace(/"/g, '\\"');
    records = await base(TABLE)
      .select({ filterByFormula: `FIND("${escId}", ARRAYJOIN({Brand})) > 0`, maxRecords: 500 })
      .all();
    if (!records.length && brandName) {
      const esc = brandName.replace(/"/g, '\\"');
      records = await base(TABLE)
        .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
        .all();
    }
  } else {
    const esc = brandName.replace(/"/g, '\\"');
    records = await base(TABLE)
      .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
      .all();
  }
  let updated = 0;
  for (const rec of records) {
    const sk = String(rec.get("Slot Key") || "").trim();
    const row = bySlot.get(sk);
    if (!row) continue;
    const fields = buildPatchFields(row);
    console.log(`${dryRun ? "Would update" : "Updating"} ${sk} (${rec.id})`);
    if (!dryRun) await base(TABLE).update(rec.id, fields);
    updated++;
  }
  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
