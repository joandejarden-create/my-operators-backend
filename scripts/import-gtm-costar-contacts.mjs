/**
 * Import CoStar ContactDataExport files into GTM CoStar Contacts table.
 *
 *   node scripts/import-gtm-costar-contacts.mjs --files="..."
 *   node scripts/import-gtm-costar-contacts.mjs --apply --files="..."
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname, resolve } from "path";
import { fileURLToPath } from "url";
import { GTM_CONTACT_TABLE, MAP_GTM_CONTACT } from "../lib/gtm-owner-target/contact-field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import { parseCostarContactFiles } from "../lib/gtm-owner-target/costar-contact-parse.js";
import { dedupeContactRows } from "../lib/gtm-owner-target/contact-to-airtable.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-costar-contacts-import.json");
const APPLY = process.argv.includes("--apply");

const DEFAULT_FILES = [1, 2, 3, 4, 5, 6, 7, 8].map(
  (n) => `C:/Users/joand/Downloads/Costar_Export (${n}).xlsx`
);

function parseArgs() {
  let files = [...DEFAULT_FILES];
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--files=")) {
      files = arg
        .slice("--files=".length)
        .replace(/^"|"$/g, "")
        .split(",")
        .map((f) => f.trim())
        .filter(Boolean);
    }
  }
  return { files: files.map((f) => resolve(f)) };
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function loadExistingContacts(base) {
  const records = await base(GTM_CONTACT_TABLE)
    .select({
      fields: [MAP_GTM_CONTACT.contactDedupeKey, MAP_GTM_CONTACT.email, MAP_GTM_CONTACT.name],
    })
    .all();

  const byKey = new Map();
  for (const rec of records) {
    const key = rec.fields[MAP_GTM_CONTACT.contactDedupeKey];
    if (key) byKey.set(String(key), rec);
  }
  return { records, byKey };
}

async function main() {
  const { files } = parseArgs();
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { fileReports, allRows } = parseCostarContactFiles(files);
  const deduped = dedupeContactRows(allRows);

  let existing = { records: [], byKey: new Map() };
  try {
    existing = await loadExistingContacts(getGtmAirtableBase());
  } catch (err) {
    if (APPLY) throw err;
    console.warn("Could not load existing contacts:", err.message || err);
  }

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
    table: GTM_CONTACT_TABLE,
    files: fileReports,
    parseSummary: {
      rawRows: allRows.length,
      uniqueRows: deduped.rows.length,
      duplicateRowsRemoved: deduped.duplicateRowsRemoved,
      skippedNoKey: deduped.skippedNoKey,
    },
    plan: {
      existingInAirtable: existing.records.length,
      wouldCreate: toCreate.length,
      wouldUpdate: toUpdate.length,
    },
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  console.log("CoStar contacts import:");
  console.log(`  Table: ${GTM_CONTACT_TABLE}`);
  console.log(`  Files: ${files.length}`);
  console.log(`  Raw rows: ${allRows.length}`);
  console.log(`  Unique (deduped): ${deduped.rows.length}`);
  console.log(`  Duplicates removed: ${deduped.duplicateRowsRemoved}`);
  console.log(`  Existing: ${existing.records.length}`);
  console.log(`  Create: ${toCreate.length}`);
  console.log(`  Update: ${toUpdate.length}`);
  console.log("Wrote", REPORT_JSON);

  if (!APPLY) {
    console.log("\nDry-run. Re-run with --apply to import.");
    return;
  }

  const base = getGtmAirtableBase();
  let created = 0;
  let updated = 0;

  for (const batch of chunk(toCreate, 10)) {
    await base(GTM_CONTACT_TABLE).create(
      batch.map((fields) => ({ fields })),
      { typecast: true }
    );
    created += batch.length;
  }
  for (const batch of chunk(toUpdate, 10)) {
    await base(GTM_CONTACT_TABLE).update(
      batch.map(({ id, fields }) => ({ id, fields })),
      { typecast: true }
    );
    updated += batch.length;
  }

  report.applySummary = { created, updated };
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  console.log("\nApply complete:");
  console.log(`  Created: ${created}`);
  console.log(`  Updated: ${updated}`);
}

main().catch((err) => {
  console.error(err.message || err);
  if (err.error) console.error(JSON.stringify(err.error));
  process.exit(1);
});
