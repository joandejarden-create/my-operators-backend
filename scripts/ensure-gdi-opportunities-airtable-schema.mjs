/**
 * Ensure "Group Demand Opportunities" table/fields for GDI Airtable store.
 *
 * Field map: lib/group-demand-intelligence/opportunity-field-map.js
 *
 * Prerequisites:
 *   AIRTABLE_API_KEY or AIRTABLE_PAT with schema.bases:read + schema.bases:write
 *   GDI_OPPORTUNITIES_AIRTABLE_BASE_ID || DECISION_OUTCOMES_AIRTABLE_BASE_ID || AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-gdi-opportunities-airtable-schema.mjs
 *   node scripts/ensure-gdi-opportunities-airtable-schema.mjs --apply
 *
 * Report: reports/group-demand-intelligence/ensure-gdi-opportunities-airtable-schema.json
 *
 * Guardrails:
 *   - Never create "Group Demand Opportunities 2" / duplicate table names
 *   - If table exists with incompatible primary field name → STOP and report
 *   - Existing tables: add missing fields only
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY as F,
  VAL_GDI_PRIORITY,
  VAL_GDI_OPPORTUNITY_TYPE,
  VAL_GDI_VENUE_STATUS,
  VAL_GDI_ROOM_DEMAND,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { OPPORTUNITY_QUALIFICATION } from "../lib/group-demand-intelligence/claim-types.js";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const REPORT_PATH = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "ensure-gdi-opportunities-airtable-schema.json"
);

/** Qualification options (no VAL_GDI_QUALIFICATION export yet — mirror claim-types). */
const VAL_GDI_QUALIFICATION = Object.freeze(Object.values(OPPORTUNITY_QUALIFICATION));

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

function singleLine(name, description) {
  const field = { name, type: "singleLineText" };
  if (description) field.description = description;
  return field;
}

function multiline(name, description) {
  const field = { name, type: "multilineText" };
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

function findTable(tables, nameOrId) {
  return (tables || []).find((t) => t.name === nameOrId || t.id === nameOrId) || null;
}

function existingFieldNames(table) {
  return new Set((table.fields || []).map((f) => f.name));
}

function getPrimaryField(table) {
  if (!table) return null;
  const byId = (table.fields || []).find((f) => f.id === table.primaryFieldId);
  if (byId) return byId;
  return (table.fields || [])[0] || null;
}

/**
 * If table exists and primary field name !== expected → incompatible (do not create alt table).
 */
function assertCompatiblePrimary(table, expectedPrimaryName, report) {
  const primary = getPrimaryField(table);
  const actualName = primary?.name || null;
  if (actualName === expectedPrimaryName) {
    return { ok: true, primary };
  }
  const conflict = {
    tableName: table.name,
    tableId: table.id,
    expectedPrimaryField: expectedPrimaryName,
    actualPrimaryField: actualName,
    reason: "incompatible_primary_field_name",
  };
  report.primaryFieldConflicts.push(conflict);
  report.blocked = true;
  report.blockReasons.push(
    `Table "${table.name}" (${table.id}) primary field is "${actualName}", expected "${expectedPrimaryName}". Refusing to create a duplicate table.`
  );
  return { ok: false, primary, conflict };
}

function buildPrimaryField() {
  return {
    name: F.opportunityId,
    type: "singleLineText",
    description: "Canonical GDI opportunity id (primary)",
  };
}

/** Non-primary fields (primary opportunityId created with the table). */
function buildNonPrimaryFields() {
  return [
    singleLine(F.hotelId),
    singleLine(F.organizationId),
    singleLine(F.opportunityName),
    singleLine(F.organizationName),
    singleLine(F.organizationEntityId),
    singleSelect(F.opportunityType, VAL_GDI_OPPORTUNITY_TYPE),
    singleSelect(F.priority, VAL_GDI_PRIORITY),
    singleLine(F.actionStatus),
    dateField(F.eventStartDate),
    dateField(F.eventEndDate),
    singleLine(F.segment),
    singleLine(F.territory),
    singleSelect(F.venueStatus, VAL_GDI_VENUE_STATUS),
    // sourcingStatus often mirrors venue sourcing codes
    singleSelect(F.sourcingStatus, VAL_GDI_VENUE_STATUS),
    numberField(F.attendance, 0),
    singleLine(F.attendanceStatus),
    numberField(F.peakRooms, 0),
    singleSelect(F.roomDemandStatus, VAL_GDI_ROOM_DEMAND),
    numberField(F.hotelFit, 2),
    singleSelect(F.qualification, VAL_GDI_QUALIFICATION),
    numberField(F.evidenceConfidence, 2),
    multiline(F.whyNow),
    multiline(F.whyThisMatters),
    multiline(F.hotelWinThesis),
    multiline(F.recommendedAction),
    singleLine(F.primaryContactId),
    singleLine(F.primaryContactName),
    singleLine(F.primaryContactRole),
    singleLine(F.primaryContactEmail),
    singleLine(F.primaryContactPhone),
    singleLine(F.contactQuality),
    numberField(F.sourceCount, 0),
    singleLine(F.sourceSummary),
    dateTimeField(F.lastResearchedAt),
    singleLine(F.researchVersion),
    singleLine(F.decisionId),
    dateTimeField(F.createdAt),
    dateTimeField(F.updatedAt),
    singleLine(F.schemaVersion),
    multiline(F.opportunityPayloadJson, "Lossless customer-facing opportunity JSON"),
    singleLine(F.hotelIdentityStatus),
    singleLine(F.runId),
  ];
}

function buildAllFields() {
  return [buildPrimaryField(), ...buildNonPrimaryFields()];
}

async function createField(baseId, token, tableId, fieldSpec, tableEntry) {
  if (!APPLY) {
    tableEntry.wouldCreateFields.push(fieldSpec.name);
    return { ok: true, dryRun: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldSpec),
  });
  if (!res.ok) {
    tableEntry.errors.push({ field: fieldSpec.name, status: res.status, error: json });
    return { ok: false, json };
  }
  tableEntry.createdFields.push({ name: fieldSpec.name, id: json.id });
  return { ok: true, json };
}

async function ensureTable({
  baseId,
  token,
  tables,
  tableName,
  primaryFieldName,
  allFields,
  nonPrimaryFields,
  description,
  report,
}) {
  const entry = {
    tableName,
    tableId: null,
    createdTable: false,
    wouldCreateTable: false,
    wouldCreateFields: [],
    createdFields: [],
    skippedExisting: [],
    errors: [],
  };
  report.tables.push(entry);

  let table = findTable(tables, tableName);

  if (table) {
    entry.tableId = table.id;
    const compat = assertCompatiblePrimary(table, primaryFieldName, report);
    if (!compat.ok) {
      console.error(
        `STOP: incompatible primary on "${tableName}" — expected "${primaryFieldName}", got "${compat.primary?.name || null}"`
      );
      return entry;
    }
    console.log(`Table already exists: ${tableName} (${table.id})`);
  } else {
    if (!APPLY) {
      entry.wouldCreateTable = true;
      entry.wouldCreateFields = allFields.map((f) => f.name);
      console.log(
        `[dry-run] Would create table "${tableName}" with ${allFields.length} fields`
      );
      return entry;
    }

    const { res, json } = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: tableName,
        description,
        fields: allFields,
      }),
    });
    if (!res.ok) {
      entry.errors.push({ createTable: true, status: res.status, error: json });
      report.errors.push({ tableName, status: res.status, error: json });
      console.error(`Create table "${tableName}" failed:`, JSON.stringify(json));
      return entry;
    }
    table = json;
    tables.push(table);
    entry.createdTable = true;
    entry.tableId = table.id;
    entry.createdFields = (table.fields || []).map((f) => ({ name: f.name, id: f.id }));
    console.log(`Created table "${tableName}" (${table.id}).`);
    return entry;
  }

  // Table exists and primary is compatible — add missing fields only
  const existing = existingFieldNames(table);
  const pending = [];
  for (const field of nonPrimaryFields) {
    if (existing.has(field.name)) {
      entry.skippedExisting.push(field.name);
    } else {
      pending.push(field);
    }
  }
  if (existing.has(primaryFieldName)) {
    entry.skippedExisting.push(primaryFieldName);
  }

  for (const field of pending) {
    console.log(`${APPLY ? "Creating" : "[dry-run] Would create"} field: ${tableName}.${field.name}`);
    await createField(baseId, token, table.id, field, entry);
  }

  entry.skippedExisting = [...new Set(entry.skippedExisting)];
  return entry;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId();

  if (!token || !baseId) {
    throw new Error(
      `Set AIRTABLE_API_KEY (or AIRTABLE_PAT) and AIRTABLE_GDI_BASE_ID or AIRTABLE_INTELLIGENCE_BASE_ID (target ${CANONICAL_INTELLIGENCE_BASE_ID})`
    );
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "ensure_gdi_opportunities_schema" });

  const report = {
    mode: APPLY ? "apply" : "dry-run",
    generatedAt: new Date().toISOString(),
    baseId,
    tableName: GDI_OPPORTUNITIES_TABLE_NAME,
    tables: [],
    primaryFieldConflicts: [],
    blocked: false,
    blockReasons: [],
    errors: [],
    guardrails: {
      neverDuplicateTableNames: true,
      neverCreateGroupDemandOpportunities2: true,
      addMissingFieldsOnly: true,
      stopOnIncompatiblePrimary: true,
    },
  };

  console.log(`Mode: ${report.mode}`);
  console.log(`Base: ${baseId}`);
  console.log(`Table: ${GDI_OPPORTUNITIES_TABLE_NAME}`);

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  const tables = listJson.tables || [];

  await ensureTable({
    baseId,
    token,
    tables,
    tableName: GDI_OPPORTUNITIES_TABLE_NAME,
    primaryFieldName: F.opportunityId,
    allFields: buildAllFields(),
    nonPrimaryFields: buildNonPrimaryFields(),
    description:
      "Group Demand Intelligence — live opportunities (GDI-specific). Spec: lib/group-demand-intelligence/opportunity-field-map.js",
    report,
  });

  if (report.blocked) {
    console.error("Blocked by primary-field conflict; refusing duplicate table creation.");
  }

  report.summary = {
    blocked: report.blocked,
    tableId: report.tables.find((t) => t.tableName === GDI_OPPORTUNITIES_TABLE_NAME)?.tableId || null,
    createdTables: report.tables.filter((t) => t.createdTable).map((t) => t.tableName),
    wouldCreateTables: report.tables.filter((t) => t.wouldCreateTable).map((t) => t.tableName),
    createdFieldCount: report.tables.reduce((n, t) => n + (t.createdFields?.length || 0), 0),
    wouldCreateFieldCount: report.tables.reduce(
      (n, t) => n + (t.wouldCreateFields?.length || 0),
      0
    ),
    skippedExistingCount: report.tables.reduce(
      (n, t) => n + (t.skippedExisting?.length || 0),
      0
    ),
    errorCount:
      report.errors.length +
      report.tables.reduce((n, t) => n + (t.errors?.length || 0), 0) +
      report.primaryFieldConflicts.length,
  };

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2) + "\n");

  console.log("\n--- Summary ---");
  console.log(`Mode: ${report.mode}`);
  console.log(`Blocked: ${report.blocked}`);
  for (const t of report.tables) {
    console.log(
      `  ${t.tableName}: id=${t.tableId || "(none)"} created=${t.createdTable} wouldCreate=${t.wouldCreateTable} fields+${t.createdFields.length} would+${t.wouldCreateFields.length} skipped=${t.skippedExisting.length} errors=${t.errors.length}`
    );
  }
  if (report.summary.tableId) {
    console.log(`Group Demand Opportunities table id: ${report.summary.tableId}`);
  }
  console.log(`Report: ${REPORT_PATH}`);

  if (!APPLY) {
    console.log("\nRe-run with --apply to write schema to Airtable.");
  }

  if (report.blocked || report.summary.errorCount > 0) {
    process.exitCode = 1;
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
