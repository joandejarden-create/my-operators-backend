#!/usr/bin/env node
/**
 * Ensure Capital Setup Airtable schema for Financing Hub / Capital Provider Explorer (non-destructive).
 *
 *   node scripts/ensure-capital-setup-schema.mjs --dry-run
 *   node scripts/ensure-capital-setup-schema.mjs --apply
 *   node scripts/ensure-capital-setup-schema.mjs --apply --seed
 *
 * Requires AIRTABLE_API_KEY with schema.bases:read + schema.bases:write
 * Uses AIRTABLE_BASE_ID (Deal Capture MVP base).
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  CAPITAL_SETUP_TABLES,
  TABLE_CAPITAL_PROVIDERS,
  TABLE_CONTACTS,
  TABLE_FINANCING_NEEDS,
  TABLE_DOCUMENT_CATEGORIES,
  buildTableFieldSpecs,
} from "../lib/capital-setup/airtable-capital-setup-fields.js";
import {
  FINANCING_DOCUMENT_CATEGORY_SEED_ROWS,
  SAMPLE_REQUIRED_DOCUMENT_NAMES,
  buildCategorySeedFields,
} from "../lib/capital-setup/capital-setup-seed-data.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;
const SEED = process.argv.includes("--seed");

const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";

const TABLE_CREATE_ORDER = [
  TABLE_DOCUMENT_CATEGORIES,
  TABLE_CAPITAL_PROVIDERS,
  "Capital Setup - Capital Provider Criteria",
  "Capital Setup - Capital Provider Required Documents",
  TABLE_CONTACTS,
  "Capital Setup - Representative Financings",
  "Capital Setup - Deal Financing Needs",
  "Capital Setup - Capital Provider Visibility Rules",
  "Capital Setup - Capital Provider Source References",
  "Capital Setup - Capital Provider Activity",
  "Capital Setup - Deal Capital Provider List",
];

async function metaFetch(baseId, token, pathSuffix, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${pathSuffix}`;
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

function hasField(table, name) {
  return (table?.fields || []).some((f) => f.name === name);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function buildCtx(tables, dealsTable) {
  const USERS_TABLE = process.env.USERS_TABLE_ID || "tbl6shiyz2wdUqE5F";
  const providers = findTable(tables, TABLE_CAPITAL_PROVIDERS);
  const contacts = findTable(tables, TABLE_CONTACTS);
  const financingNeeds = findTable(tables, TABLE_FINANCING_NEEDS);
  const users =
    findTable(tables, "Users") || (tables || []).find((t) => t.id === USERS_TABLE) || null;
  return {
    providersId: providers?.id || null,
    contactsId: contacts?.id || null,
    financingNeedsId: financingNeeds?.id || null,
    dealsId: dealsTable?.id || null,
    usersId: users?.id || null,
  };
}

async function createField(baseId, token, tableId, spec) {
  if (DRY) {
    console.log(`  [dry-run] would create field: ${spec.name}`);
    return { ok: true, dry: true };
  }
  const body = {
    name: spec.name,
    type: spec.type,
    ...(spec.options ? { options: spec.options } : {}),
    ...(spec.description ? { description: spec.description } : {}),
  };
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

async function createTable(baseId, token, body) {
  if (DRY) {
    console.log(`[dry-run] would create table: ${body.name} (${body.fields?.length || 0} fields)`);
    return { ok: true, dry: true, json: { id: "dry_run", name: body.name } };
  }
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

async function ensureField(baseId, token, table, spec, failures) {
  if (!table) return { skipped: true, reason: "no table" };
  if (hasField(table, spec.name)) {
    console.log(`  skip (exists): ${spec.name}`);
    return { skipped: true };
  }
  const r = await createField(baseId, token, table.id, spec);
  if (!r.ok) {
    console.error(`  FAIL ${spec.name}: ${r.status}`, JSON.stringify(r.json));
    failures.push({ table: table.name, field: spec.name, status: r.status, error: r.json });
    return { failed: true };
  }
  console.log(`  created: ${spec.name}`);
  if (!DRY && table.fields) table.fields.push({ name: spec.name, type: spec.type });
  await sleep(220);
  return { created: true };
}

async function refreshTables(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed ${res.status}: ${JSON.stringify(json)}`);
  return json.tables || [];
}

async function ensureTable(baseId, token, tables, dealsTable, tableName, report, failures) {
  const ctx = buildCtx(tables, dealsTable);
  const fieldSpecs = buildTableFieldSpecs(tableName, ctx);
  let table = findTable(tables, tableName);
  const existedBefore = !!table;

  if (!table) {
    console.log(`\nCreate table: ${tableName}`);
    const cr = await createTable(baseId, token, {
      name: tableName,
      description: "Capital Setup — Financing Hub / Capital Provider Explorer (Dealality MVP).",
      fields: fieldSpecs,
    });
    if (!cr.ok) {
      failures.push({ table: tableName, action: "create_table", status: cr.status, error: cr.json });
      throw new Error(`Create ${tableName} failed ${cr.status}: ${JSON.stringify(cr.json)}`);
    }
    console.log(`  created table: ${tableName}${cr.json?.id ? ` (${cr.json.id})` : ""}`);
    report.tablesCreated.push(tableName);
    if (!DRY) {
      tables = await refreshTables(baseId, token);
      table = findTable(tables, tableName);
    } else {
      table = { id: "dry_run", name: tableName, fields: fieldSpecs.map((f) => ({ name: f.name })) };
    }
  } else {
    console.log(`\n${tableName} — ensure missing fields`);
    report.tablesExisting.push(tableName);
    let createdCount = 0;
    for (const spec of fieldSpecs) {
      const r = await ensureField(baseId, token, table, spec, failures);
      if (r.created) createdCount += 1;
    }
    if (createdCount === 0) console.log("  (all fields present)");
  }

  report.tableDetails[tableName] = {
    existedBefore,
    created: !existedBefore,
    fieldCount: (table?.fields || fieldSpecs).length,
    linkedTables: fieldSpecs
      .filter((f) => f.type === "multipleRecordLinks")
      .map((f) => ({ field: f.name, linkedTableId: f.options?.linkedTableId || null })),
    todoTextFields: fieldSpecs
      .filter((f) => f.description && String(f.description).includes("TODO"))
      .map((f) => f.name),
  };

  return tables;
}

async function seedDocumentCategories(baseId, apiKey, report) {
  console.log("\nSeed: Financing Document Categories");
  if (DRY) {
    for (const row of FINANCING_DOCUMENT_CATEGORY_SEED_ROWS) {
      console.log(`  [dry-run] would upsert category: ${row.categoryName}`);
    }
    console.log(
      `\nNote: ${SAMPLE_REQUIRED_DOCUMENT_NAMES.length} sample required document names are defined for future provider-linked seeding.`
    );
    report.seed = { dryRun: true, categories: FINANCING_DOCUMENT_CATEGORY_SEED_ROWS.length };
    return;
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const existing = await base(TABLE_DOCUMENT_CATEGORIES).select().all();
  const byName = new Map(
    existing.map((r) => [String(r.fields["Category Name"] || "").trim(), r])
  );

  let created = 0;
  let updated = 0;
  for (const row of FINANCING_DOCUMENT_CATEGORY_SEED_ROWS) {
    const fields = buildCategorySeedFields(row);
    const hit = byName.get(row.categoryName);
    if (hit) {
      await base(TABLE_DOCUMENT_CATEGORIES).update(hit.id, fields, { typecast: true });
      console.log(`  updated: ${row.categoryName}`);
      updated += 1;
    } else {
      const rec = await base(TABLE_DOCUMENT_CATEGORIES).create(fields, { typecast: true });
      console.log(`  created: ${row.categoryName} (${rec.id})`);
      created += 1;
    }
    await sleep(120);
  }

  report.seed = {
    categoriesCreated: created,
    categoriesUpdated: updated,
    sampleDocumentNamesDeferred: SAMPLE_REQUIRED_DOCUMENT_NAMES.length,
  };
  console.log(
    `\nSample required documents (${SAMPLE_REQUIRED_DOCUMENT_NAMES.length} names) deferred — link to Capital Providers after provider records exist.`
  );
}

function printValidationReport(report) {
  console.log("\n=== VALIDATION REPORT ===");
  for (const tableName of CAPITAL_SETUP_TABLES) {
    const d = report.tableDetails[tableName] || {};
    const links = (d.linkedTables || [])
      .filter((l) => l.linkedTableId)
      .map((l) => l.field)
      .join(", ");
    const todos = (d.todoTextFields || []).join(", ");
    console.log(`\n${tableName}`);
    console.log(`  table created: ${d.created ? "yes" : "no (already existed)"}`);
    console.log(`  field count: ${d.fieldCount ?? "n/a"}`);
    console.log(`  linked fields: ${links || "none"}`);
    if (todos) console.log(`  TODO text fields (needs manual link upgrade): ${todos}`);
  }
  if (report.failures.length) {
    console.log(`\nIssues (${report.failures.length}):`);
    for (const f of report.failures) {
      console.log(`  - ${f.table || ""} ${f.field || f.action || ""}: ${f.status || ""}`);
    }
  } else {
    console.log("\nNo field creation failures.");
  }
  if (report.dealsLinked) {
    console.log(`\nDeals table resolved: ${DEALS_TABLE} — Related Deal fields use linked records.`);
  } else {
    console.log(
      `\nDeals table not found — Related Deal fields created as single line text (TODO upgrade).`
    );
  }
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Deal Capture MVP base:", baseId);
  console.log("Capital Setup tables:", CAPITAL_SETUP_TABLES.length);

  let tables = await refreshTables(baseId, token);
  let dealsTable = findTable(tables, DEALS_TABLE);

  const report = {
    tablesCreated: [],
    tablesExisting: [],
    tableDetails: {},
    failures: [],
    dealsLinked: !!dealsTable,
    dealsTableName: dealsTable?.name || null,
  };

  if (!dealsTable) {
    console.warn(`\nWARN: Deals table "${DEALS_TABLE}" not found — Related Deal fields will use single line text.`);
  } else {
    console.log(`\nDeals table found: ${dealsTable.name} (${dealsTable.id})`);
  }

  for (const tableName of TABLE_CREATE_ORDER) {
    tables = await ensureTable(baseId, token, tables, dealsTable, tableName, report, report.failures);
    if (!DRY) {
      tables = await refreshTables(baseId, token);
      dealsTable = findTable(tables, DEALS_TABLE) || dealsTable;
    }
  }

  if (SEED) {
    await seedDocumentCategories(baseId, token, report);
  } else if (!DRY) {
    console.log("\nSeed skipped — re-run with --seed to upsert Financing Document Categories.");
  }

  printValidationReport(report);

  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = path.join(
    REPORTS,
    `capital-setup-schema-${DRY ? "dry-run" : "apply"}-${stamp}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(`\nWrote report: ${outPath}`);

  console.log(
    "\nDone.",
    DRY ? "Re-run with --apply to create schema. Add --seed to upsert category reference rows." : "Schema apply complete."
  );

  if (report.failures.length) process.exit(2);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
