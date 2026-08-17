/**
 * Import CoStar Properties exports into GTM Airtable Properties table.
 *
 * Default: dry-run. Use --apply to write.
 *
 * Usage:
 *   node scripts/import-gtm-properties-costar.mjs --files="C:/path/a.xlsx,C:/path/b.xlsx"
 *   node scripts/import-gtm-properties-costar.mjs --apply --files="..."
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname, resolve } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_PROPERTIES,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import { parseCostarPropertiesFiles } from "../lib/gtm-owner-target/costar-full-parse.js";
import { dedupeCostarRows, propertyDedupeKey } from "../lib/gtm-owner-target/costar-to-airtable.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_JSON = join(__dirname, "..", "reports", "gtm-properties-costar-import.json");
const REPORT_CSV = join(__dirname, "..", "reports", "gtm-properties-costar-import-summary.csv");
const APPLY = process.argv.includes("--apply");

const DEFAULT_FILES = [
  "C:/Users/joand/Downloads/CostarExport (2).xlsx",
  "C:/Users/joand/Downloads/CostarExport (3).xlsx",
  "C:/Users/joand/Downloads/CostarExport (5).xlsx",
  "C:/Users/joand/Downloads/CostarExport (6).xlsx",
  "C:/Users/joand/Downloads/CostarExport (7).xlsx",
  "C:/Users/joand/Downloads/CostarExport (8).xlsx",
  "C:/Users/joand/Downloads/CostarExport (9).xlsx",
  "C:/Users/joand/Downloads/CostarExport (10).xlsx",
  "C:/Users/joand/Downloads/CostarExport (11).xlsx",
  "C:/Users/joand/Downloads/CostarExport (12).xlsx",
  "C:/Users/joand/Downloads/CostarExport (13).xlsx",
  "C:/Users/joand/Downloads/CostarExport (14).xlsx",
  "C:/Users/joand/Downloads/CostarExport (15).xlsx",
];

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

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function loadExistingProperties(base) {
  const records = await base(GTM_OWNER_TARGET_TABLES.properties)
    .select({
      fields: [
        MAP_GTM_PROPERTIES.buildingName,
        MAP_GTM_PROPERTIES.propertyId,
        MAP_GTM_PROPERTIES.city,
        MAP_GTM_PROPERTIES.trueOwner,
      ],
    })
    .all();

  const byKey = new Map();
  for (const rec of records) {
    const fields = {
      "Building Name": rec.fields[MAP_GTM_PROPERTIES.buildingName],
      "Property ID": rec.fields[MAP_GTM_PROPERTIES.propertyId],
      City: rec.fields[MAP_GTM_PROPERTIES.city],
      "True Owner": rec.fields[MAP_GTM_PROPERTIES.trueOwner],
    };
    const key = propertyDedupeKey(fields);
    if (key) byKey.set(key, rec);
  }
  return { records, byKey };
}

async function createRecords(base, payloads) {
  let created = 0;
  for (const batch of chunk(payloads, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.properties).create(
      batch.map((fields) => ({ fields })),
      { typecast: true }
    );
    created += batch.length;
  }
  return created;
}

async function updateRecords(base, payloads) {
  let updated = 0;
  for (const batch of chunk(payloads, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.properties).update(
      batch.map(({ id, fields }) => ({ id, fields })),
      { typecast: true }
    );
    updated += batch.length;
  }
  return updated;
}

async function main() {
  const { files } = parseArgs();
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { fileReports, allRows } = parseCostarPropertiesFiles(files);
  const deduped = dedupeCostarRows(allRows);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    files: fileReports,
    parseSummary: {
      rawRows: allRows.length,
      uniqueRows: deduped.rows.length,
      duplicateRowsRemoved: deduped.duplicateRowsRemoved,
      skippedNoKey: deduped.skippedNoKey,
    },
    applySummary: null,
  };

  let existing = { records: [], byKey: new Map() };
  try {
    const base = getGtmAirtableBase();
    existing = await loadExistingProperties(base);
  } catch (err) {
    if (APPLY) throw err;
    console.warn("Could not load existing Airtable rows (dry-run):", err.message || err);
  }

  const toCreate = [];
  const toUpdate = [];
  let unchanged = 0;

  for (const row of deduped.rows) {
    const existingRec = existing.byKey.get(row.key);
    if (existingRec) {
      toUpdate.push({ id: existingRec.id, fields: row.fields });
    } else {
      toCreate.push(row.fields);
    }
  }

  report.plan = {
    existingInAirtable: existing.records.length,
    wouldCreate: toCreate.length,
    wouldUpdate: toUpdate.length,
    unchanged,
  };

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  const csvLines = [
    "Metric,Count",
    `Raw rows parsed,${allRows.length}`,
    `Unique after file dedupe,${deduped.rows.length}`,
    `Duplicates removed across files,${deduped.duplicateRowsRemoved}`,
    `Existing in Airtable,${existing.records.length}`,
    `Would create,${toCreate.length}`,
    `Would update,${toUpdate.length}`,
  ];
  writeFileSync(REPORT_CSV, csvLines.join("\n") + "\n");

  console.log("CoStar import plan:");
  console.log(`  Files: ${files.length}`);
  console.log(`  Raw rows: ${allRows.length}`);
  console.log(`  Unique (deduped): ${deduped.rows.length}`);
  console.log(`  Duplicates removed: ${deduped.duplicateRowsRemoved}`);
  console.log(`  Existing in Airtable: ${existing.records.length}`);
  console.log(`  Create: ${toCreate.length}`);
  console.log(`  Update: ${toUpdate.length}`);
  console.log("Wrote", REPORT_JSON);

  if (!APPLY) {
    console.log("\nDry-run only. Re-run with --apply to import into Properties.");
    return;
  }

  const base = getGtmAirtableBase();
  let created = 0;
  let updated = 0;
  let failed = 0;
  const errors = [];

  try {
    created = await createRecords(base, toCreate);
  } catch (err) {
    errors.push({ phase: "create", message: err.message || String(err) });
    throw err;
  }

  try {
    updated = await updateRecords(base, toUpdate);
  } catch (err) {
    errors.push({ phase: "update", message: err.message || String(err) });
    throw err;
  }

  report.applySummary = { created, updated, failed, errors };
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
