/**
 * Apply three Radisson Individuals materials.caseStudy rows (sort 1–3).
 * Updates empty sort=1 row if present; creates sort 2–3.
 *
 *   node scripts/apply-radisson-individuals-case-studies-three.mjs --dry-run
 *   node scripts/apply-radisson-individuals-case-studies-three.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import { sanitizeExternalCopy } from "../lib/external-owner-copy.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(ROOT, "fixtures", "brand-explorer-presentation-radisson-individuals-case-studies.json");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const BRAND = "Radisson Individuals by Choice";
const LINK_FIELDS = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];
const TARGET_SORTS = [1, 2, 3];

const dryRun = process.argv.includes("--dry-run");

function sanitizeRowText(value) {
  return sanitizeExternalCopy(String(value ?? "").trim());
}

function buildFields(brandRecordId, linkField, row, brandName) {
  const fields = {
    [linkField]: [brandRecordId],
    "Slot Key": row.slotKey,
    Title: sanitizeRowText(row.title),
    Body: sanitizeRowText(row.body),
    "Sort Order": row.sort,
    Active: true,
    "Brand Name": brandName,
  };
  if (row.caseSummaryOverview) fields["Case Summary Overview"] = sanitizeRowText(row.caseSummaryOverview);
  if (row.caseSummaryOwnerObjective) fields["Case Summary Owner Objective"] = sanitizeRowText(row.caseSummaryOwnerObjective);
  if (row.caseSummaryBrandRelevance) fields["Case Summary Brand Relevance"] = sanitizeRowText(row.caseSummaryBrandRelevance);
  if (row.caseSummaryInterpretation) fields["Case Summary Interpretation"] = sanitizeRowText(row.caseSummaryInterpretation);
  if (row.caseSummaryTags) fields["Case Summary Tags"] = sanitizeRowText(row.caseSummaryTags);
  return fields;
}

async function findBasics(base) {
  const esc = BRAND.replace(/"/g, '\\"');
  const records = await base(BASICS).select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 }).all();
  if (records.length !== 1) throw new Error(`Expected 1 Brand Basics row for ${BRAND}, got ${records.length}`);
  return records[0];
}

async function listCaseStudies(base) {
  const esc = BRAND.replace(/"/g, '\\"');
  const recs = await base(TABLE)
    .select({
      filterByFormula: `{Brand Name} = "${esc}"`,
      fields: ["Slot Key", "Title", "Body", "Sort Order"],
      maxRecords: 100,
    })
    .all();
  return recs
    .filter((r) => r.get("Slot Key") === "materials.caseStudy")
    .sort((a, b) => (a.get("Sort Order") || 0) - (b.get("Sort Order") || 0));
}

async function updateRecord(base, recordId, fields) {
  await base(TABLE).update(recordId, fields);
}

async function createRecord(base, brandRecordId, row, brandName) {
  let lastErr;
  for (const linkField of LINK_FIELDS) {
    try {
      const fields = buildFields(brandRecordId, linkField, row, brandName);
      const [created] = await base(TABLE).create([{ fields }]);
      return { id: created.id, linkField };
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("Could not create presentation row");
}

async function main() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const data = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const rows = data.rows.filter(
    (r) => r.slotKey === "materials.caseStudy" && TARGET_SORTS.includes(Number(r.sort))
  );
  if (rows.length !== 3) throw new Error(`Expected 3 fixture rows for sort 1–3, got ${rows.length}`);

  const base = new Airtable({ apiKey: key }).base(baseId);
  const basics = await findBasics(base);
  const brandId = basics.id;
  const existing = await listCaseStudies(base);

  console.log(dryRun ? "DRY RUN\n" : "APPLY\n");
  console.log(`Brand: ${BRAND} (${brandId})`);
  console.log(`Existing caseStudy rows: ${existing.length}`);

  for (const row of rows.sort((a, b) => a.sort - b.sort)) {
    const sort = row.sort;
    const match = existing.find((r) => Number(r.get("Sort Order")) === sort);
    const payloadFields = {
      Title: sanitizeRowText(row.title),
      Body: sanitizeRowText(row.body),
      "Sort Order": sort,
      Active: true,
    };
    if (row.caseSummaryOverview) payloadFields["Case Summary Overview"] = sanitizeRowText(row.caseSummaryOverview);
    if (row.caseSummaryOwnerObjective) payloadFields["Case Summary Owner Objective"] = sanitizeRowText(row.caseSummaryOwnerObjective);
    if (row.caseSummaryBrandRelevance) payloadFields["Case Summary Brand Relevance"] = sanitizeRowText(row.caseSummaryBrandRelevance);
    if (row.caseSummaryInterpretation) payloadFields["Case Summary Interpretation"] = sanitizeRowText(row.caseSummaryInterpretation);
    if (row.caseSummaryTags) payloadFields["Case Summary Tags"] = sanitizeRowText(row.caseSummaryTags);

    if (match) {
      const empty = !String(match.get("Title") || "").trim() && !String(match.get("Body") || "").trim();
      if (!empty) {
        console.log(`\nSort ${sort}: skip ${match.id} — row already has content (${match.get("Title")})`);
        continue;
      }
      console.log(`\nSort ${sort}: update ${match.id} → ${row.title}`);
      if (!dryRun) {
        await updateRecord(base, match.id, payloadFields);
        console.log("  ✓ updated");
      }
    } else {
      console.log(`\nSort ${sort}: create → ${row.title}`);
      if (!dryRun) {
        const { id, linkField } = await createRecord(base, brandId, row, BRAND);
        console.log(`  ✓ created ${id} (link: ${linkField})`);
      }
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
