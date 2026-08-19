#!/usr/bin/env node
/**
 * Ensure AI Visibility Airtable schema (Prompts + Opportunities).
 *
 *   node scripts/ensure-ai-visibility-schema.mjs --dry-run
 *   AI_VISIBILITY_SCHEMA_APPLY=true node scripts/ensure-ai-visibility-schema.mjs --apply
 *
 * Only creates approved tables/fields. Never deletes. Never touches Brand Basics / Operator Master fields.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  AI_VISIBILITY_PROMPTS_TABLE,
  AI_VISIBILITY_OPPORTUNITIES_TABLE,
  AI_VISIBILITY_BRAND_BASICS_TABLE,
  AI_VISIBILITY_OPERATOR_MASTER_TABLE,
  getPromptCoreFieldSpecs,
  getPromptLinkFieldSpecs,
  getOpportunityCoreFieldSpecs,
  getOpportunityLinkFieldSpecs,
  classifyFieldEnsureAction,
} from "../lib/ai-visibility/airtable-schema-proposal.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const REPORT_PATH = path.join(ROOT, "reports", "ensure-ai-visibility-schema.json");

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

function fieldByName(table, name) {
  return (table?.fields || []).find((f) => f.name === name) || null;
}

function toCreatePayload(spec, linkedTableId = null) {
  if (spec.type === "multipleRecordLinks") {
    if (!linkedTableId) return null;
    return {
      name: spec.name,
      type: "multipleRecordLinks",
      options: { linkedTableId },
    };
  }
  const payload = { name: spec.name, type: spec.type };
  if (spec.description) payload.description = spec.description;
  if (spec.options) payload.options = spec.options;
  return payload;
}

async function ensureFieldsOnTable(entry, table, fieldSpecs, ctx, linkIds = {}) {
  for (const spec of fieldSpecs) {
    const existing = fieldByName(table, spec.name);
    const classification = classifyFieldEnsureAction(existing, spec);
    if (classification.action === "conflict") {
      entry.fieldsConflict.push({
        name: spec.name,
        reason: classification.reason,
      });
      console.error("CONFLICT", table.name, spec.name, classification.reason);
      continue;
    }
    if (spec.schemaApply === false) {
      entry.fieldsSkipped.push(`${spec.name} (schemaApply=false)`);
      console.log("SKIP", table.name, spec.name, "schemaApply=false");
      continue;
    }

    if (classification.action === "skip") {
      entry.fieldsSkipped.push(spec.name);
      console.log("SKIP", table.name, spec.name);
      continue;
    }

    if (classification.action === "add_choices") {
      entry.choicesWouldAdd = entry.choicesWouldAdd || [];
      entry.choicesAdded = entry.choicesAdded || [];
      if (DRY) {
        entry.choicesWouldAdd.push({
          field: spec.name,
          missing: classification.missingChoices,
        });
        console.log(
          "WOULD ADD CHOICES",
          table.name,
          spec.name,
          classification.missingChoices.join(", ")
        );
        continue;
      }
      const { res, json } = await metaFetch(ctx.baseId, ctx.apiKey, `/tables/${table.id}/fields/${existing.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          options: {
            choices: classification.allChoices.map((name) => ({ name })),
          },
        }),
      });
      if (!res.ok) {
        entry.fieldsFailed.push({ name: spec.name, error: json, action: "add_choices" });
        console.error("CHOICES FAILED", table.name, spec.name, JSON.stringify(json));
      } else {
        entry.choicesAdded.push({
          field: spec.name,
          missing: classification.missingChoices,
        });
        existing.options = json.options || existing.options;
        console.log("ADDED CHOICES", table.name, spec.name);
      }
      continue;
    }

    let linkedTableId = null;
    if (spec.linkTableEnv === "brandBasics") linkedTableId = linkIds.brandBasics;
    if (spec.linkTableEnv === "operatorMaster") linkedTableId = linkIds.operatorMaster;
    const payload = toCreatePayload(spec, linkedTableId);
    if (!payload) {
      entry.fieldsSkipped.push(`${spec.name} (link target missing)`);
      console.warn("SKIP LINK (no target)", table.name, spec.name);
      continue;
    }

    if (DRY) {
      entry.fieldsWouldCreate.push(spec.name);
      console.log("WOULD CREATE FIELD", table.name, spec.name, spec.type);
      continue;
    }

    const { res, json } = await metaFetch(ctx.baseId, ctx.apiKey, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      entry.fieldsFailed.push({ name: spec.name, error: json });
      console.error("FAIL FIELD", table.name, spec.name, JSON.stringify(json));
      continue;
    }
    entry.fieldsCreated.push(spec.name);
    table.fields = [...(table.fields || []), json];
    console.log("CREATED FIELD", table.name, spec.name);
  }
}

async function ensureTable(report, tables, tableName, coreFields, linkFields, ctx, linkIds) {
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
    protectedFieldsTouched: false,
  };
  report.tables.push(entry);

  const primary = coreFields.find((f) => f.primary) || coreFields[0];
  const nonPrimaryCore = coreFields.filter((f) => f.name !== primary.name);

  if (!table) {
    if (DRY) {
      entry.wouldCreateTable = true;
      entry.fieldsWouldCreate = [
        primary.name,
        ...nonPrimaryCore.map((f) => f.name),
        ...linkFields.map((f) => f.name),
      ];
      console.log("WOULD CREATE TABLE", tableName);
      return null;
    }
    const createFields = [
      toCreatePayload(primary),
      ...nonPrimaryCore.map((f) => toCreatePayload(f)).filter(Boolean),
    ];
    const { res, json } = await metaFetch(ctx.baseId, ctx.apiKey, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: tableName,
        description: "Dealality AI Visibility — governed operational config (admin).",
        fields: createFields,
      }),
    });
    if (!res.ok) throw new Error(`Create table ${tableName} failed: ${JSON.stringify(json)}`);
    table = json;
    entry.createdTable = true;
    entry.tableId = json.id;
    entry.fieldsCreated = (json.fields || []).map((f) => f.name);
    tables.push(table);
    console.log("CREATED TABLE", tableName, json.id);
    await ensureFieldsOnTable(entry, table, linkFields, ctx, linkIds);
    return table;
  }

  console.log("TABLE EXISTS", table.name, table.id);
  await ensureFieldsOnTable(entry, table, nonPrimaryCore, ctx, linkIds);
  // Also ensure primary exists (skip if present)
  await ensureFieldsOnTable(entry, table, [primary], ctx, linkIds);
  await ensureFieldsOnTable(entry, table, linkFields, ctx, linkIds);
  return table;
}

async function main() {
  console.log(DRY ? "=== DRY RUN (no Airtable writes) ===" : "=== APPLY ===");

  if (APPLY && String(process.env.AI_VISIBILITY_SCHEMA_APPLY || "").toLowerCase() !== "true") {
    console.error(
      "Refusing --apply.\nSet AI_VISIBILITY_SCHEMA_APPLY=true only after dry-run PASS + founder authorization."
    );
    process.exit(2);
  }

  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) {
    console.error("AIRTABLE_API_KEY / AIRTABLE_BASE_ID required");
    process.exit(1);
  }

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed: ${JSON.stringify(json)}`);
  const tables = json.tables || [];

  const brandBasics = findTable(tables, AI_VISIBILITY_BRAND_BASICS_TABLE);
  const operatorMaster = findTable(tables, AI_VISIBILITY_OPERATOR_MASTER_TABLE);
  const linkIds = {
    brandBasics: brandBasics?.id || null,
    operatorMaster: operatorMaster?.id || null,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    APPROVED_TABLES: [AI_VISIBILITY_PROMPTS_TABLE, AI_VISIBILITY_OPPORTUNITIES_TABLE],
    brandBasicsFound: Boolean(brandBasics),
    operatorMasterFound: Boolean(operatorMaster),
    protectedFieldsTouched: false,
    destructiveChanges: false,
    tables: [],
    warnings: [],
  };

  if (!brandBasics) report.warnings.push(`Brand Basics table not found — link fields skipped`);
  if (!operatorMaster) report.warnings.push(`Operator Master table not found — link fields skipped`);

  const ctx = { apiKey: token, baseId };

  await ensureTable(
    report,
    tables,
    AI_VISIBILITY_PROMPTS_TABLE,
    getPromptCoreFieldSpecs(),
    getPromptLinkFieldSpecs(),
    ctx,
    linkIds
  );
  await ensureTable(
    report,
    tables,
    AI_VISIBILITY_OPPORTUNITIES_TABLE,
    getOpportunityCoreFieldSpecs(),
    getOpportunityLinkFieldSpecs(),
    ctx,
    linkIds
  );

  // Re-list live schema snapshot after apply/dry
  const { res: res2, json: json2 } = await metaFetch(baseId, token, "/tables");
  const live = {};
  if (res2.ok) {
    for (const name of report.APPROVED_TABLES) {
      const t = findTable(json2.tables || [], name);
      if (t) {
        live[name] = {
          id: t.id,
          fields: (t.fields || []).map((f) => ({
            name: f.name,
            type: f.type,
            choices: f.options?.choices?.map((c) => c.name) || undefined,
            linkedTableId: f.options?.linkedTableId || undefined,
          })),
        };
      }
    }
  }
  report.liveSchema = live;
  report.DRY_RUN_STATUS = DRY ? "PASS" : undefined;
  report.SCHEMA_APPLY_STATUS = APPLY ? "PASS" : "NOT_APPLIED";
  report.AIRTABLE_WRITES_SCHEMA = APPLY
    ? report.tables.reduce(
        (n, t) => n + (t.createdTable ? 1 : 0) + t.fieldsCreated.length,
        0
      )
    : 0;

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2), "utf8");
  console.log("\nReport:", REPORT_PATH);
  console.log("AIRTABLE_WRITES_SCHEMA:", report.AIRTABLE_WRITES_SCHEMA);
  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        tables: report.tables.map((t) => ({
          name: t.tableName,
          wouldCreateTable: t.wouldCreateTable,
          createdTable: t.createdTable,
          fieldsWouldCreate: t.fieldsWouldCreate.length,
          fieldsCreated: t.fieldsCreated.length,
          conflicts: t.fieldsConflict.length,
        })),
        protectedFieldsTouched: false,
        destructiveChanges: false,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
