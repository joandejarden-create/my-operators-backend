/**
 * Import CoStar CompaniesDataExport files into CoStar Companies table.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname, resolve } from "path";
import { fileURLToPath } from "url";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
import { getGtmAirtableBase, assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";
import { parseCostarCompanyFiles } from "../lib/gtm-owner-target/costar-company-parse.js";
import { dedupeCompanyRows } from "../lib/gtm-owner-target/company-to-airtable.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-costar-companies-import.json");
const APPLY = process.argv.includes("--apply");

const DEFAULT_FILES = [9, 10, 11, 12].map((n) => `C:/Users/joand/Downloads/Costar_Export (${n}).xlsx`);

function parseArgs() {
  let files = [...DEFAULT_FILES];
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--files=")) {
      files = arg.slice("--files=".length).replace(/^"|"$/g, "").split(",").map((f) => f.trim()).filter(Boolean);
    }
  }
  return { files: files.map((f) => resolve(f)) };
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function loadExisting(base) {
  const records = await base(GTM_COMPANY_TABLE).select({ fields: [MAP_GTM_COMPANY.companyDedupeKey] }).all();
  const byKey = new Map();
  for (const rec of records) {
    const key = rec.fields[MAP_GTM_COMPANY.companyDedupeKey];
    if (key) byKey.set(String(key), rec);
  }
  return { records, byKey };
}

async function main() {
  const { files } = parseArgs();
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { fileReports, allRows } = parseCostarCompanyFiles(files);
  const deduped = dedupeCompanyRows(allRows);

  let existing = { records: [], byKey: new Map() };
  try { existing = await loadExisting(getGtmAirtableBase()); } catch (e) { if (APPLY) throw e; }

  const toCreate = [];
  const toUpdate = [];
  for (const row of deduped.rows) {
    const hit = existing.byKey.get(row.key);
    if (hit) toUpdate.push({ id: hit.id, fields: row.fields });
    else toCreate.push(row.fields);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    table: GTM_COMPANY_TABLE,
    files: fileReports,
    parseSummary: { rawRows: allRows.length, uniqueRows: deduped.rows.length, duplicateRowsRemoved: deduped.duplicateRowsRemoved },
    plan: { existingInAirtable: existing.records.length, wouldCreate: toCreate.length, wouldUpdate: toUpdate.length },
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  console.log("CoStar companies import:");
  console.log(`  Table: ${GTM_COMPANY_TABLE}`);
  console.log(`  Files: ${files.length}`);
  console.log(`  Raw rows: ${allRows.length}`);
  console.log(`  Unique (deduped): ${deduped.rows.length}`);
  console.log(`  Duplicates removed: ${deduped.duplicateRowsRemoved}`);
  console.log(`  Create: ${toCreate.length}`);
  console.log(`  Update: ${toUpdate.length}`);

  if (!APPLY) { console.log("\nDry-run. Re-run with --apply."); return; }

  const base = getGtmAirtableBase();
  let created = 0, updated = 0;
  for (const batch of chunk(toCreate, 10)) {
    await base(GTM_COMPANY_TABLE).create(batch.map((fields) => ({ fields })), { typecast: true });
    created += batch.length;
  }
  for (const batch of chunk(toUpdate, 10)) {
    await base(GTM_COMPANY_TABLE).update(batch.map(({ id, fields }) => ({ id, fields })), { typecast: true });
    updated += batch.length;
  }
  report.applySummary = { created, updated };
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  console.log(`\nApply complete: Created ${created}, Updated ${updated}`);
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
