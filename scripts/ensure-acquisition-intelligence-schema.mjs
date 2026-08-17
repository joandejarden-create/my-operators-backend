/**
 * Ensure Acquisition Intelligence tables on GTM base.
 *
 * Tables:
 *   - Acquisition Network Relationships
 *   - Acquisition Import Batches
 * Links to existing Contacts table (person identity).
 *
 * Usage:
 *   node scripts/ensure-acquisition-intelligence-schema.mjs
 *   node scripts/ensure-acquisition-intelligence-schema.mjs --dry-run
 *   node scripts/ensure-acquisition-intelligence-schema.mjs --apply
 *
 * Report: reports/ensure-acquisition-intelligence-schema.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_ACQUISITION_RELATIONSHIPS_TABLE,
  GTM_ACQUISITION_IMPORT_BATCHES_TABLE,
  GTM_ACQUISITION_LINKED_TABLES,
  MAP_ACQUISITION_RELATIONSHIP,
  MAP_ACQUISITION_IMPORT_BATCH,
} from "../lib/acquisition-intelligence/field-map.js";
import {
  buildAcquisitionRelationshipCoreFields,
  buildAcquisitionRelationshipLinkFields,
  buildAcquisitionImportBatchCoreFields,
  classifyFieldEnsureAction,
  getAcquisitionIntelligenceSchemaSummary,
} from "../lib/acquisition-intelligence/schema-spec.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import { GTM_OWNER_TARGET_TABLES } from "../lib/gtm-owner-target/field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name) || null;
}

function fieldByName(table, name) {
  return (table?.fields || []).find((f) => f.name === name) || null;
}

async function ensureFieldsOnTable(reportEntry, table, fieldSpecs, ctx) {
  for (const spec of fieldSpecs) {
    const existing = fieldByName(table, spec.name);
    const classification = classifyFieldEnsureAction(existing, spec);

    if (classification.action === "conflict") {
      reportEntry.fieldsConflict.push({
        name: spec.name,
        reason: classification.reason,
        existingType: existing?.type,
        desiredType: spec.type,
      });
      console.error("CONFLICT", table.name, spec.name, classification.reason);
      continue;
    }

    if (classification.action === "skip") {
      reportEntry.fieldsSkipped.push(spec.name);
      console.log("SKIP", table.name, spec.name);
      continue;
    }

    if (DRY_RUN) {
      reportEntry.fieldsWouldCreate.push(spec.name);
      console.log("WOULD CREATE FIELD", table.name, spec.name, spec.type);
      continue;
    }

    const { res, json } = await metaFetch(ctx.baseId, ctx.apiKey, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(spec),
    });
    if (!res.ok) {
      reportEntry.fieldsFailed.push({ name: spec.name, error: json });
      console.error("FAIL FIELD", table.name, spec.name, JSON.stringify(json));
      continue;
    }
    reportEntry.fieldsCreated.push(spec.name);
    table.fields = [...(table.fields || []), json];
    console.log("CREATED FIELD", table.name, spec.name);
  }
}

async function ensureTable(report, tables, tableName, primaryField, coreFields, ctx) {
  let table = findTable(tables, tableName);
  const entry = {
    tableName,
    tableId: table?.id || null,
    createdTable: false,
    wouldCreateTable: false,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
    fieldsConflict: [],
  };
  report.tables.push(entry);

  if (!table) {
    if (DRY_RUN) {
      entry.wouldCreateTable = true;
      entry.fieldsWouldCreate = [primaryField.name, ...coreFields.map((f) => f.name)];
      console.log("WOULD CREATE TABLE", tableName);
      return null;
    }
    const { res, json } = await metaFetch(ctx.baseId, ctx.apiKey, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: tableName,
        description: "Acquisition Intelligence — internal founder network (user-scoped).",
        fields: [primaryField, ...coreFields.filter((f) => f.name !== primaryField.name)],
      }),
    });
    if (!res.ok) throw new Error(`Create table ${tableName} failed: ${JSON.stringify(json)}`);
    table = json;
    entry.createdTable = true;
    entry.tableId = json.id;
    entry.fieldsCreated = (json.fields || []).map((f) => f.name);
    tables.push(table);
    console.log("CREATED TABLE", tableName, json.id);
    return table;
  }

  console.log("TABLE EXISTS", table.name, table.id);
  await ensureFieldsOnTable(entry, table, coreFields, ctx);
  return table;
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  const contacts = findTable(tables, GTM_ACQUISITION_LINKED_TABLES.contacts);
  const ownerTargets = findTable(tables, GTM_OWNER_TARGET_TABLES.ownerTargets);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    schemaSummary: getAcquisitionIntelligenceSchemaSummary(),
    contactsTableFound: Boolean(contacts),
    contactsTableId: contacts?.id || null,
    ownerTargetsTableFound: Boolean(ownerTargets),
    ownerTargetsTableId: ownerTargets?.id || null,
    tables: [],
    warnings: [],
  };

  if (!contacts) {
    report.warnings.push(
      `Contacts table "${GTM_ACQUISITION_LINKED_TABLES.contacts}" not found — Contact link fields will be skipped.`
    );
    console.warn(report.warnings[0]);
  }

  const batchPrimary = {
    name: MAP_ACQUISITION_IMPORT_BATCH.batchLabel,
    type: "singleLineText",
  };
  const batchesTable = await ensureTable(
    report,
    tables,
    GTM_ACQUISITION_IMPORT_BATCHES_TABLE,
    batchPrimary,
    buildAcquisitionImportBatchCoreFields(),
    { apiKey, baseId }
  );

  const relPrimary = {
    name: MAP_ACQUISITION_RELATIONSHIP.relationshipName,
    type: "singleLineText",
  };
  const relTable = await ensureTable(
    report,
    tables,
    GTM_ACQUISITION_RELATIONSHIPS_TABLE,
    relPrimary,
    buildAcquisitionRelationshipCoreFields(),
    { apiKey, baseId }
  );

  // Link fields after both tables exist
  if (relTable && (contacts || batchesTable || ownerTargets)) {
    const relEntry = report.tables.find((t) => t.tableName === GTM_ACQUISITION_RELATIONSHIPS_TABLE);
    const linkIds = {
      contactsTableId: contacts?.id,
      importBatchesTableId: batchesTable?.id || findTable(tables, GTM_ACQUISITION_IMPORT_BATCHES_TABLE)?.id,
      ownerTargetsTableId: ownerTargets?.id,
    };

    if (relEntry) {
      const realLinks = buildAcquisitionRelationshipLinkFields({
        contactsTableId: linkIds.contactsTableId,
        importBatchesTableId: linkIds.importBatchesTableId,
        ownerTargetsTableId: linkIds.ownerTargetsTableId,
      }).filter((f) => {
        if (f.name === MAP_ACQUISITION_RELATIONSHIP.contact) return Boolean(linkIds.contactsTableId);
        if (f.name === MAP_ACQUISITION_RELATIONSHIP.importBatch) {
          return Boolean(linkIds.importBatchesTableId);
        }
        if (f.name === MAP_ACQUISITION_RELATIONSHIP.existingOwnerTarget) {
          return Boolean(linkIds.ownerTargetsTableId);
        }
        return true;
      });
      await ensureFieldsOnTable(relEntry, relTable, realLinks, { apiKey, baseId });
    }
  }

  const outPath = path.join(ROOT, "reports", "ensure-acquisition-intelligence-schema.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  console.log("\nReport:", outPath);
  console.log(DRY_RUN ? "Dry-run complete (no schema writes)." : "Apply complete.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
