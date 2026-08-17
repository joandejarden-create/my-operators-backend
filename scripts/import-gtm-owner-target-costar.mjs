/**
 * Import CoStar Properties export into the internal GTM Airtable base.
 *
 * Default: dry-run (preview only, no Airtable writes).
 * Use --apply to create/update owner targets and property rows.
 *
 * Prerequisites:
 *   - AIRTABLE_GTM_BASE_ID set (separate from product bases)
 *   - node scripts/ensure-gtm-owner-target-base.mjs --apply
 *
 * Usage:
 *   node scripts/import-gtm-owner-target-costar.mjs
 *   node scripts/import-gtm-owner-target-costar.mjs --file="data/internal/gtm-costar-imports/properties.csv"
 *   node scripts/import-gtm-owner-target-costar.mjs --dir="data/internal/gtm-costar-imports" --apply
 *
 * Reports:
 *   reports/gtm-owner-target-import-preview.json
 *   reports/gtm-owner-target-import-preview.csv
 */
import "../load-env.js";
import { existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname, resolve } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  MAP_GTM_TARGET_PROPERTY,
  MAP_GTM_IMPORT_BATCH,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  parseCostarExportFile,
  parseCostarExportDirectory,
  groupRowsByOwner,
} from "../lib/gtm-owner-target/costar-parse.js";
import { computeOwnerRollups } from "../lib/gtm-owner-target/rollup.js";
import {
  validateOwnerTargetWrite,
  validatePropertyWrite,
  buildOwnerTargetFieldsFromRollup,
  buildOwnerTargetUpdateFieldsFromRollup,
  buildPropertyFieldsFromRow,
} from "../lib/gtm-owner-target/validate.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const DEFAULT_DIR = join(ROOT, "data", "internal", "gtm-costar-imports");
const PREVIEW_JSON = join(ROOT, "reports", "gtm-owner-target-import-preview.json");
const PREVIEW_CSV = join(ROOT, "reports", "gtm-owner-target-import-preview.csv");

function parseArgs() {
  let file = "";
  let dir = DEFAULT_DIR;
  let apply = process.argv.includes("--apply");
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--file=")) file = arg.slice("--file=".length).replace(/^"|"$/g, "");
    if (arg.startsWith("--dir=")) dir = arg.slice("--dir=".length).replace(/^"|"$/g, "");
  }
  return { file, dir, apply };
}

function csvEscape(value) {
  const s = value == null ? "" : String(value);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function loadExistingOwners(base) {
  const records = await base(GTM_OWNER_TARGET_TABLES.ownerTargets)
    .select({
      fields: [
        MAP_GTM_OWNER_TARGET.ownerName,
        MAP_GTM_OWNER_TARGET.ownerNameNormalized,
        MAP_GTM_OWNER_TARGET.outreachStatus,
        MAP_GTM_OWNER_TARGET.pitchStatus,
      ],
    })
    .all();

  const byNormalized = new Map();
  for (const rec of records) {
    const normalized =
      rec.fields[MAP_GTM_OWNER_TARGET.ownerNameNormalized] ||
      rec.fields[MAP_GTM_OWNER_TARGET.ownerName];
    if (normalized) byNormalized.set(String(normalized).trim().toLowerCase(), rec);
  }
  return { records, byNormalized };
}

async function loadExistingProperties(base) {
  const records = await base(GTM_OWNER_TARGET_TABLES.properties)
    .select({
      fields: [
        MAP_GTM_TARGET_PROPERTY.sourceRowKey,
        MAP_GTM_TARGET_PROPERTY.costarPropertyId,
        MAP_GTM_TARGET_PROPERTY.buildingName,
        MAP_GTM_TARGET_PROPERTY.ownerTarget,
      ],
    })
    .all();

  const bySourceRowKey = new Map();
  const byPropertyId = new Map();
  for (const rec of records) {
    const key = rec.fields[MAP_GTM_TARGET_PROPERTY.sourceRowKey];
    const pid = rec.fields[MAP_GTM_TARGET_PROPERTY.costarPropertyId];
    if (key) bySourceRowKey.set(String(key), rec);
    if (pid) byPropertyId.set(String(pid), rec);
  }
  return { records, bySourceRowKey, byPropertyId };
}

function findExistingProperty(existing, row) {
  if (row.sourceRowKey && existing.bySourceRowKey.has(row.sourceRowKey)) {
    return existing.bySourceRowKey.get(row.sourceRowKey);
  }
  if (row.costarPropertyId && existing.byPropertyId.has(String(row.costarPropertyId))) {
    return existing.byPropertyId.get(String(row.costarPropertyId));
  }
  return null;
}

async function main() {
  const { file, dir, apply } = parseArgs();
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  let parsed;
  if (file) {
    const single = parseCostarExportFile(resolve(file));
    parsed = { rows: single.rows, fileReports: [single] };
  } else {
    parsed = parseCostarExportDirectory(resolve(dir));
    if (!parsed.rows.length) {
      throw new Error(
        `No CoStar export files found. Place CSV/XLS/XLSX in ${dir} or pass --file=...`
      );
    }
  }

  const warnings = parsed.fileReports.flatMap((r) => r.warnings || []);
  if (warnings.length) {
    console.warn("Parser warnings:");
    warnings.forEach((w) => console.warn(" -", w));
  }

  const groups = groupRowsByOwner(parsed.rows);
  const rollups = computeOwnerRollups(groups);

  const preview = {
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    baseId,
    source: file ? { file: resolve(file) } : { dir: resolve(dir) },
    fileReports: parsed.fileReports,
    summary: {
      propertyRows: parsed.rows.length,
      ownerGroups: groups.length,
      warnings,
    },
    owners: rollups.map((r) => ({
      ownerName: r.ownerName,
      ownerType: r.ownerType,
      priorityTier: r.priorityTier,
      propertyCount: r.propertyCount,
      totalRbaSf: r.totalRbaSf,
      marketsSummary: r.marketsSummary,
      countriesSummary: r.countriesSummary,
    })),
  };

  mkdirSync(dirname(PREVIEW_JSON), { recursive: true });
  writeFileSync(PREVIEW_JSON, JSON.stringify(preview, null, 2));

  const csvHeaders = [
    "Owner Name",
    "Owner Type",
    "Priority Tier",
    "Property Count",
    "Total RBA SF",
    "Countries",
    "Markets",
  ];
  const csvLines = [
    csvHeaders.join(","),
    ...preview.owners.map((o) =>
      [
        csvEscape(o.ownerName),
        csvEscape(o.ownerType),
        csvEscape(o.priorityTier),
        o.propertyCount,
        o.totalRbaSf ?? "",
        csvEscape(o.countriesSummary),
        csvEscape(o.marketsSummary),
      ].join(",")
    ),
  ];
  writeFileSync(PREVIEW_CSV, csvLines.join("\n") + "\n");

  console.log(`Parsed ${parsed.rows.length} properties → ${groups.length} owner groups`);
  console.log("Wrote", PREVIEW_JSON);
  console.log("Wrote", PREVIEW_CSV);

  if (!apply) {
    console.log("\nDry-run only. Re-run with --apply to write to the GTM Airtable base.");
    return;
  }

  const base = getGtmAirtableBase();
  const existingOwners = await loadExistingOwners(base);
  const existingProperties = await loadExistingProperties(base);

  const batchLabel = `costar-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}`;
  const batchFields = {
    [MAP_GTM_IMPORT_BATCH.batchLabel]: batchLabel,
    [MAP_GTM_IMPORT_BATCH.sourceFileName]: file
      ? file
      : (parsed.fileReports || []).map((r) => r.fileName).join(", "),
    [MAP_GTM_IMPORT_BATCH.sourceFilePath]: file || dir,
    [MAP_GTM_IMPORT_BATCH.rowCount]: parsed.rows.length,
    [MAP_GTM_IMPORT_BATCH.ownerCount]: groups.length,
    [MAP_GTM_IMPORT_BATCH.status]: "applied",
    [MAP_GTM_IMPORT_BATCH.appliedAt]: new Date().toISOString(),
    [MAP_GTM_IMPORT_BATCH.previewReportPath]: PREVIEW_JSON,
    [MAP_GTM_IMPORT_BATCH.notes]: "CoStar licensed internal import. Not for Dealality product.",
  };

  const batchRec = await base(GTM_OWNER_TARGET_TABLES.importBatches).create([{ fields: batchFields }]);
  const batchId = batchRec[0].id;
  console.log("Created import batch", batchId, batchLabel);

  const ownerIdByKey = new Map();
  let ownerCreates = 0;
  let ownerUpdates = 0;

  for (const rollup of rollups) {
    const existing = existingOwners.byNormalized.get(rollup.ownerNameNormalized);
    if (existing) {
      const updateFields = {
        ...buildOwnerTargetUpdateFieldsFromRollup(rollup),
        [MAP_GTM_OWNER_TARGET.importBatch]: [batchId],
      };
      const validation = validateOwnerTargetWrite({
        ...updateFields,
        [MAP_GTM_OWNER_TARGET.ownerName]: rollup.ownerName,
      });
      if (!validation.ok) {
        throw new Error(`Owner update validation failed for ${rollup.ownerName}: ${validation.failures.join("; ")}`);
      }
      await base(GTM_OWNER_TARGET_TABLES.ownerTargets).update([
        { id: existing.id, fields: updateFields },
      ]);
      ownerIdByKey.set(rollup.ownerNameNormalized, existing.id);
      ownerUpdates++;
    } else {
      const createFields = buildOwnerTargetFieldsFromRollup(rollup, batchId);
      const validation = validateOwnerTargetWrite(createFields);
      if (!validation.ok) {
        throw new Error(`Owner create validation failed for ${rollup.ownerName}: ${validation.failures.join("; ")}`);
      }
      const created = await base(GTM_OWNER_TARGET_TABLES.ownerTargets).create([{ fields: createFields }]);
      ownerIdByKey.set(rollup.ownerNameNormalized, created[0].id);
      ownerCreates++;
    }
  }

  let propertyCreates = 0;
  let propertyUpdates = 0;
  const propertyCreatesPayload = [];
  const propertyUpdatesPayload = [];

  for (const group of groups) {
    const ownerId = ownerIdByKey.get(group.ownerKey);
    if (!ownerId) continue;

    for (const row of group.properties) {
      const fields = buildPropertyFieldsFromRow(row, ownerId, batchId);
      const validation = validatePropertyWrite(fields);
      if (!validation.ok) {
        console.warn(`Skipping property ${row.buildingName}: ${validation.failures.join("; ")}`);
        continue;
      }

      const existing = findExistingProperty(existingProperties, row);
      if (existing) {
        propertyUpdatesPayload.push({ id: existing.id, fields });
      } else {
        propertyCreatesPayload.push({ fields });
      }
    }
  }

  for (const batch of chunk(propertyCreatesPayload, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.properties).create(batch);
    propertyCreates += batch.length;
  }
  for (const batch of chunk(propertyUpdatesPayload, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.properties).update(batch);
    propertyUpdates += batch.length;
  }

  await base(GTM_OWNER_TARGET_TABLES.importBatches).update([
    {
      id: batchId,
      fields: {
        [MAP_GTM_IMPORT_BATCH.propertyCreateCount]: propertyCreates,
        [MAP_GTM_IMPORT_BATCH.propertyUpdateCount]: propertyUpdates,
      },
    },
  ]);

  console.log("\nApply complete:");
  console.log(`  Owners created: ${ownerCreates}`);
  console.log(`  Owners updated: ${ownerUpdates}`);
  console.log(`  Properties created: ${propertyCreates}`);
  console.log(`  Properties updated: ${propertyUpdates}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
