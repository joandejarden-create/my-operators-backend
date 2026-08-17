/**
 * GTM Owner Target List — Airtable schema ensure (separate internal base).
 *
 * Prerequisites:
 *   1. Create a new Airtable base manually (e.g. "Dealality GTM — Owner Targets").
 *   2. Set AIRTABLE_GTM_BASE_ID in .env (must NOT be AIRTABLE_BASE_ID or AIRTABLE_BASE_ID_ALT).
 *   3. PAT with schema.bases:read + schema.bases:write on that base.
 *
 * Usage:
 *   node scripts/ensure-gtm-owner-target-base.mjs
 *   node scripts/ensure-gtm-owner-target-base.mjs --apply
 *
 * Report: reports/ensure-gtm-owner-target-base.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  MAP_GTM_PROPERTIES,
  MAP_GTM_IMPORT_BATCH,
  VAL_GTM_OWNER_TYPE,
  VAL_GTM_PRIORITY_TIER,
  VAL_GTM_OUTREACH_STATUS,
  VAL_GTM_PITCH_STATUS,
  VAL_GTM_CONTACT_PATH,
  VAL_GTM_DATA_SOURCE,
  VAL_GTM_DATA_LICENSE,
  VAL_GTM_VISIBILITY,
  VAL_GTM_IMPORT_BATCH_STATUS,
} from "../lib/gtm-owner-target/field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}

function dateField(name, description) {
  const field = {
    name,
    type: "date",
    options: { dateFormat: { name: "iso" } },
  };
  if (description) field.description = description;
  return field;
}

function dateTimeField(name, description) {
  const field = {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
  if (description) field.description = description;
  return field;
}

function numberField(name, precision = 0, description) {
  const field = { name, type: "number", options: { precision } };
  if (description) field.description = description;
  return field;
}

function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
}

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

function buildOwnerTargetFields(importBatchTableId) {
  const fields = [
    { name: MAP_GTM_OWNER_TARGET.ownerName, type: "singleLineText" },
    {
      name: MAP_GTM_OWNER_TARGET.ownerNameNormalized,
      type: "singleLineText",
      description: "Lowercase normalized key for dedupe and re-import matching.",
    },
    singleSelect(MAP_GTM_OWNER_TARGET.ownerType, VAL_GTM_OWNER_TYPE),
    singleSelect(MAP_GTM_OWNER_TARGET.priorityTier, VAL_GTM_PRIORITY_TIER),
    singleSelect(
      MAP_GTM_OWNER_TARGET.outreachStatus,
      VAL_GTM_OUTREACH_STATUS,
      "Internal GTM outreach pipeline status."
    ),
    singleSelect(
      MAP_GTM_OWNER_TARGET.pitchStatus,
      VAL_GTM_PITCH_STATUS,
      "Dealality product pitch status (internal only)."
    ),
    { name: MAP_GTM_OWNER_TARGET.pitchAngle, type: "multilineText" },
    numberField(MAP_GTM_OWNER_TARGET.propertyCount, 0, "Maintained by CoStar import script."),
    numberField(MAP_GTM_OWNER_TARGET.totalRbaSf, 0, "Sum of linked property RBA GLA SF."),
    { name: MAP_GTM_OWNER_TARGET.marketsSummary, type: "multilineText" },
    { name: MAP_GTM_OWNER_TARGET.countriesSummary, type: "singleLineText" },
    { name: MAP_GTM_OWNER_TARGET.sampleProperties, type: "multilineText" },
    singleSelect(MAP_GTM_OWNER_TARGET.contactPath, VAL_GTM_CONTACT_PATH),
    { name: MAP_GTM_OWNER_TARGET.primaryContactName, type: "singleLineText" },
    { name: MAP_GTM_OWNER_TARGET.primaryContactEmail, type: "email" },
    { name: MAP_GTM_OWNER_TARGET.primaryContactPhone, type: "phoneNumber" },
    { name: MAP_GTM_OWNER_TARGET.nextAction, type: "singleLineText" },
    dateField(MAP_GTM_OWNER_TARGET.nextActionDate),
    { name: MAP_GTM_OWNER_TARGET.assignedTo, type: "singleCollaborator" },
    { name: MAP_GTM_OWNER_TARGET.internalNotes, type: "multilineText" },
    singleSelect(
      MAP_GTM_OWNER_TARGET.dataSource,
      VAL_GTM_DATA_SOURCE,
      "Must remain costar_internal or manual for licensed internal use."
    ),
    singleSelect(
      MAP_GTM_OWNER_TARGET.dataLicense,
      VAL_GTM_DATA_LICENSE,
      "CoStar data — internal GTM only; never publish in Dealality product."
    ),
    singleSelect(
      MAP_GTM_OWNER_TARGET.visibility,
      VAL_GTM_VISIBILITY,
      "Always internal_only. Do not expose via product APIs."
    ),
    dateTimeField(
      MAP_GTM_OWNER_TARGET.lastCostarSyncAt,
      "Last time CoStar import refreshed portfolio metrics for this owner."
    ),
  ];
  if (importBatchTableId) {
    fields.push(
      linkField(
        MAP_GTM_OWNER_TARGET.importBatch,
        importBatchTableId,
        "Most recent CoStar import batch affecting this owner."
      )
    );
  }
  return fields;
}

function buildImportBatchFields() {
  return [
    { name: MAP_GTM_IMPORT_BATCH.batchLabel, type: "singleLineText" },
    { name: MAP_GTM_IMPORT_BATCH.sourceFileName, type: "singleLineText" },
    {
      name: MAP_GTM_IMPORT_BATCH.sourceFilePath,
      type: "singleLineText",
      description: "Local path reference only; CoStar files stay gitignored.",
    },
    numberField(MAP_GTM_IMPORT_BATCH.rowCount, 0),
    numberField(MAP_GTM_IMPORT_BATCH.ownerCount, 0),
    numberField(MAP_GTM_IMPORT_BATCH.propertyCreateCount, 0),
    numberField(MAP_GTM_IMPORT_BATCH.propertyUpdateCount, 0),
    dateTimeField(MAP_GTM_IMPORT_BATCH.appliedAt),
    { name: MAP_GTM_IMPORT_BATCH.appliedBy, type: "singleLineText" },
    singleSelect(MAP_GTM_IMPORT_BATCH.status, VAL_GTM_IMPORT_BATCH_STATUS),
    { name: MAP_GTM_IMPORT_BATCH.previewReportPath, type: "singleLineText" },
    { name: MAP_GTM_IMPORT_BATCH.notes, type: "multilineText" },
  ];
}

function buildPropertyFields(ownerTargetTableId, importBatchTableId) {
  return [
    { name: MAP_GTM_TARGET_PROPERTY.buildingName, type: "singleLineText" },
    linkField(MAP_GTM_TARGET_PROPERTY.ownerTarget, ownerTargetTableId),
    { name: MAP_GTM_TARGET_PROPERTY.trueOwnerRaw, type: "singleLineText" },
    {
      name: MAP_GTM_TARGET_PROPERTY.costarPropertyId,
      type: "singleLineText",
      description: "CoStar Property ID — internal reference only.",
    },
    { name: MAP_GTM_TARGET_PROPERTY.submarket, type: "singleLineText" },
    { name: MAP_GTM_TARGET_PROPERTY.market, type: "singleLineText" },
    { name: MAP_GTM_TARGET_PROPERTY.country, type: "singleLineText" },
    { name: MAP_GTM_TARGET_PROPERTY.city, type: "singleLineText" },
    { name: MAP_GTM_TARGET_PROPERTY.zipCode, type: "singleLineText" },
    numberField(MAP_GTM_TARGET_PROPERTY.starRating, 0),
    numberField(MAP_GTM_TARGET_PROPERTY.rbaGlaSf, 0),
    numberField(MAP_GTM_TARGET_PROPERTY.yearBuilt, 0),
    numberField(MAP_GTM_TARGET_PROPERTY.yearRenovated, 0),
    { name: MAP_GTM_TARGET_PROPERTY.brandAffiliation, type: "singleLineText" },
    { name: MAP_GTM_TARGET_PROPERTY.propertyType, type: "singleLineText" },
    { name: MAP_GTM_TARGET_PROPERTY.builtRenovText, type: "singleLineText" },
    linkField(MAP_GTM_TARGET_PROPERTY.importBatch, importBatchTableId),
    { name: MAP_GTM_TARGET_PROPERTY.internalNotes, type: "multilineText" },
    {
      name: MAP_GTM_TARGET_PROPERTY.sourceRowKey,
      type: "singleLineText",
      description: "Stable dedupe key for re-imports.",
    },
  ];
}

async function ensureTable(baseId, apiKey, tables, report, spec) {
  const { tableName, description, buildFields, dependsOn } = spec;
  const entry = {
    tableName,
    tableId: null,
    createdTable: false,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  let table = tables.find((t) => t.name === tableName);
  const deps = typeof dependsOn === "function" ? dependsOn(report) : dependsOn || {};
  const fieldSpecs = buildFields(deps);

  if (!table) {
    if (!APPLY) {
      entry.wouldCreateTable = true;
      entry.fieldsWouldCreate = fieldSpecs.map((f) => f.name);
      console.log("WOULD CREATE TABLE", tableName, `(${fieldSpecs.length} fields)`);
    } else {
      const primaryName =
        fieldSpecs.find((f) => f.type === "singleLineText")?.name || fieldSpecs[0]?.name;
      const primary = fieldSpecs.find((f) => f.name === primaryName) || {
        name: primaryName || "Name",
        type: "singleLineText",
      };
      const rest = fieldSpecs.filter((f) => f.name !== primary.name);
      const { res, json } = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: tableName,
          description,
          fields: [primary, ...rest],
        }),
      });
      if (!res.ok) throw new Error(`Create table ${tableName} failed: ${JSON.stringify(json)}`);
      table = json;
      entry.createdTable = true;
      entry.tableId = json.id;
      entry.fieldsCreated = (json.fields || []).map((f) => f.name);
      console.log("CREATED TABLE", json.name, json.id);
      tables.push(json);
    }
  } else {
    entry.tableId = table.id;
    console.log("TABLE EXISTS", table.name, table.id);
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const fieldSpec of fieldSpecs) {
      if (existing.has(fieldSpec.name)) {
        entry.fieldsSkipped.push(fieldSpec.name);
        continue;
      }
      if (!APPLY) {
        entry.fieldsWouldCreate.push(fieldSpec.name);
        continue;
      }
      const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(fieldSpec),
      });
      if (!res.ok) {
        entry.fieldsFailed.push({ name: fieldSpec.name, error: json });
        console.error("FIELD FAILED", tableName, fieldSpec.name, JSON.stringify(json));
      } else {
        entry.fieldsCreated.push(fieldSpec.name);
        console.log("CREATED FIELD", tableName, fieldSpec.name);
      }
    }
  }

  report.tables.push(entry);
  return table;
}

async function ensureFieldOnTable(baseId, apiKey, table, fieldSpec, reportEntry) {
  const existing = new Set((table.fields || []).map((f) => f.name));
  if (existing.has(fieldSpec.name)) {
    reportEntry?.fieldsSkipped?.push(fieldSpec.name);
    return;
  }
  if (!APPLY) {
    reportEntry?.fieldsWouldCreate?.push(fieldSpec.name);
    console.log("WOULD CREATE FIELD", table.name, fieldSpec.name);
    return;
  }
  const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldSpec),
  });
  if (!res.ok) {
    reportEntry?.fieldsFailed?.push({ name: fieldSpec.name, error: json });
    console.error("FIELD FAILED", table.name, fieldSpec.name, JSON.stringify(json));
  } else {
    reportEntry?.fieldsCreated?.push(fieldSpec.name);
    console.log("CREATED FIELD", table.name, fieldSpec.name);
  }
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  const propertiesTable = tables.find((t) => t.name === GTM_OWNER_TARGET_TABLES.properties);
  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    existingPropertiesTable: propertiesTable
      ? { id: propertiesTable.id, name: propertiesTable.name, fieldCount: (propertiesTable.fields || []).length }
      : null,
    guardrails: {
      internalOnly: true,
      neverSyncToProductBases: true,
      costarLicensedInternalUseOnly: true,
    },
    tables: [],
  };

  const ownerTargetTable = await ensureTable(baseId, apiKey, tables, report, {
    tableName: GTM_OWNER_TARGET_TABLES.ownerTargets,
    description:
      "Internal Dealality GTM owner target rollups. CoStar-derived; never publish in product.",
    buildFields: () => buildOwnerTargetFields(null),
    dependsOn: () => ({}),
  });

  const ownerTargetTableId =
    ownerTargetTable?.id ||
    tables.find((t) => t.name === GTM_OWNER_TARGET_TABLES.ownerTargets)?.id;

  if (propertiesTable && ownerTargetTableId) {
    const linkEntry = {
      tableName: propertiesTable.name,
      tableId: propertiesTable.id,
      createdTable: false,
      fieldsWouldCreate: [],
      fieldsCreated: [],
      fieldsSkipped: [],
      fieldsFailed: [],
    };
    await ensureFieldOnTable(
      baseId,
      apiKey,
      propertiesTable,
      linkField(
        MAP_GTM_PROPERTIES.ownerTargetLink,
        ownerTargetTableId,
        "Rollup owner target for Dealality GTM outreach."
      ),
      linkEntry
    );
    report.tables.push(linkEntry);
  } else if (!propertiesTable) {
    console.log(
      `Note: CoStar table "${GTM_OWNER_TARGET_TABLES.properties}" not found. Import properties before linking.`
    );
  }

  const outPath = path.join(ROOT, "reports", "ensure-gtm-owner-target-base.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
  console.log(
    APPLY
      ? "Apply complete. Run: node scripts/sync-gtm-owner-target-rollups.mjs --apply"
      : "Dry-run complete. Re-run with --apply after setting AIRTABLE_GTM_BASE_ID."
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
