/**
 * Ensure "Decisions" + "Decision Events" tables/fields for Decision & Outcome.
 *
 * Field map: lib/decision-outcomes/field-map.js
 *
 * Prerequisites:
 *   AIRTABLE_API_KEY or AIRTABLE_PAT with schema.bases:read + schema.bases:write
 *   AIRTABLE_INTELLIGENCE_BASE_ID || AIRTABLE_DECISION_OUTCOME_BASE_ID
 *   || DECISION_OUTCOMES_AIRTABLE_BASE_ID || ADP_AIRTABLE_BASE_ID
 *   (target: appa2cE7FTRmIbB32 — not Deal Capture MVP)
 *
 * Usage:
 *   node scripts/ensure-decision-outcomes-airtable-schema.mjs
 *   node scripts/ensure-decision-outcomes-airtable-schema.mjs --apply
 *
 * Report: reports/decision-outcomes/ensure-decision-outcomes-airtable-schema.json
 *
 * Guardrails:
 *   - Never create "Decisions 2" / duplicate table names
 *   - If table exists with incompatible primary field name → STOP and report
 *   - Existing tables: add missing fields only
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  DECISIONS_TABLE_NAME,
  DECISION_EVENTS_TABLE_NAME,
  MAP_DECISION as D,
  MAP_DECISION_EVENT as E,
  VAL_PRODUCT_MODULE,
  VAL_DECISION_TYPE,
  VAL_SUBJECT_TYPE,
  VAL_DECISION_STATUS,
  VAL_EVENT_TYPE,
} from "../lib/decision-outcomes/field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const REPORT_PATH = path.join(
  ROOT,
  "reports",
  "decision-outcomes",
  "ensure-decision-outcomes-airtable-schema.json"
);

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
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

function currencyField(name, description) {
  const field = {
    name,
    type: "currency",
    options: { precision: 2, symbol: "USD" },
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

/** Non-primary Decision fields (primary decisionId created with the table). */
function buildDecisionNonPrimaryFields() {
  return [
    singleLine(D.idempotencyKey),
    singleLine(D.hotelId),
    singleLine(D.organizationId),
    singleLine(D.entityId),
    singleSelect(D.productModule, VAL_PRODUCT_MODULE),
    singleSelect(D.decisionType, VAL_DECISION_TYPE),
    singleSelect(D.subjectType, VAL_SUBJECT_TYPE),
    singleLine(D.subjectId),
    multiline(D.recommendation),
    multiline(D.recommendationSummary),
    dateTimeField(D.recommendationDate),
    numberField(D.recommendationVersion, 0),
    singleSelect(D.decisionStatus, VAL_DECISION_STATUS),
    numberField(D.confidenceScore, 2),
    singleLine(D.confidenceBand),
    singleLine(D.confidenceMethodologyVersion),
    multiline(D.evidenceSnapshotSummary),
    multiline(D.evidenceReferenceIds),
    singleLine(D.sourceSystem),
    singleLine(D.schemaVersion),
    dateTimeField(D.createdAt),
    dateTimeField(D.updatedAt),
    singleLine(D.createdBy),
    singleLine(D.createdByRole),
    singleLine(D.supersedesDecisionId),
  ];
}

function buildDecisionPrimaryField() {
  return {
    name: D.decisionId,
    type: "singleLineText",
    description: "Canonical decision id (primary)",
  };
}

/** All Decision fields including primary (for create-table body). */
function buildDecisionAllFields() {
  return [buildDecisionPrimaryField(), ...buildDecisionNonPrimaryFields()];
}

function buildEventPrimaryField() {
  return {
    name: E.eventId,
    type: "singleLineText",
    description: "Canonical event id (primary)",
  };
}

function buildEventNonPrimaryFields() {
  return [
    singleLine(E.decisionId),
    singleLine(E.hotelId),
    singleLine(E.organizationId),
    singleSelect(E.productModule, VAL_PRODUCT_MODULE),
    singleLine(E.subjectId),
    singleSelect(E.eventType, VAL_EVENT_TYPE),
    singleLine(E.eventSubtype),
    singleLine(E.eventValue),
    singleLine(E.reason),
    multiline(E.note),
    singleLine(E.userId),
    singleLine(E.userRole),
    dateTimeField(E.eventDate),
    dateTimeField(E.reportedAt),
    dateTimeField(E.createdAt),
    currencyField(E.financialValue),
    singleLine(E.currency),
    numberField(E.roomNights, 0),
    currencyField(E.revenueValue),
    singleLine(E.sourceSurface),
    singleLine(E.schemaVersion),
    singleLine(E.causalConfidence),
    multiline(E.measurementChange),
    multiline(E.temporalAssociation),
    multiline(E.rawEventJson, "Lossless extras / migration forward-compat JSON"),
  ];
}

function buildEventAllFields() {
  return [buildEventPrimaryField(), ...buildEventNonPrimaryFields()];
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
  // Primary should already exist; record skip
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

import {
  getDecisionOutcomesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getDecisionOutcomesAirtableBaseId();

  if (!token || !baseId) {
    throw new Error(
      `Set AIRTABLE_API_KEY (or AIRTABLE_PAT) and AIRTABLE_DECISION_OUTCOME_BASE_ID or AIRTABLE_INTELLIGENCE_BASE_ID (target ${CANONICAL_INTELLIGENCE_BASE_ID})`
    );
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "ensure_decision_outcomes_schema" });

  const report = {
    mode: APPLY ? "apply" : "dry-run",
    generatedAt: new Date().toISOString(),
    baseId,
    tables: [],
    primaryFieldConflicts: [],
    blocked: false,
    blockReasons: [],
    errors: [],
    guardrails: {
      neverDuplicateTableNames: true,
      neverCreateDecisions2: true,
      addMissingFieldsOnly: true,
      stopOnIncompatiblePrimary: true,
    },
  };

  console.log(`Mode: ${report.mode}`);
  console.log(`Base: ${baseId}`);
  console.log(`Tables: ${DECISIONS_TABLE_NAME}, ${DECISION_EVENTS_TABLE_NAME}`);

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  const tables = listJson.tables || [];

  await ensureTable({
    baseId,
    token,
    tables,
    tableName: DECISIONS_TABLE_NAME,
    primaryFieldName: D.decisionId,
    allFields: buildDecisionAllFields(),
    nonPrimaryFields: buildDecisionNonPrimaryFields(),
    description:
      "Canonical Decision & Outcome — decision records (module-neutral). Spec: lib/decision-outcomes/field-map.js",
    report,
  });

  if (!report.blocked) {
    await ensureTable({
      baseId,
      token,
      tables,
      tableName: DECISION_EVENTS_TABLE_NAME,
      primaryFieldName: E.eventId,
      allFields: buildEventAllFields(),
      nonPrimaryFields: buildEventNonPrimaryFields(),
      description:
        "Canonical Decision & Outcome — append-only validation/action/outcome events. Spec: lib/decision-outcomes/field-map.js",
      report,
    });
  } else {
    console.error("Blocked by primary-field conflict; skipping further schema writes.");
  }

  // Flatten top-level summary helpers
  report.summary = {
    blocked: report.blocked,
    decisionsTableId: report.tables.find((t) => t.tableName === DECISIONS_TABLE_NAME)?.tableId || null,
    decisionEventsTableId:
      report.tables.find((t) => t.tableName === DECISION_EVENTS_TABLE_NAME)?.tableId || null,
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
  if (report.summary.decisionsTableId) {
    console.log(`Decisions table id: ${report.summary.decisionsTableId}`);
  }
  if (report.summary.decisionEventsTableId) {
    console.log(`Decision Events table id: ${report.summary.decisionEventsTableId}`);
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
